<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

use Semitexa\Core\Support\Row;
use Semitexa\Ledger\Domain\Model\ClusterConfig;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Consumer\DeliverPolicy;
use Basis\Nats\Message\Msg;
use Basis\Nats\Message\Payload;

/**
 * Thin wrapper around basis-company/nats that exposes the operations
 * needed by LedgerPublisher, LedgerReplayer, and CommandBus.
 *
 * One instance per cluster connection (managed by ClusterRegistry).
 *
 * Swoole compatibility: basis-company/nats uses PHP socket streams.
 * SWOOLE_HOOK_ALL (enabled in SwooleBootstrap) converts these to
 * coroutine-friendly I/O automatically.
 */
final class NatsClient
{
    /**
     * Rebuilt after a failure, see {@see reconnect()}. Nullable only between a
     * reconnect and the next call.
     */
    private ?Client $client;

    /** @var array<string, mixed> */
    private readonly array $options;

    /** Seconds of empty pulls after which the connection is proven alive. */
    private const LIVENESS_AFTER = 10.0;

    /** When this connection last showed it was alive (a delivery or a PONG). */
    private float $aliveAt;

    public function __construct(ClusterConfig $config)
    {
        $parsed = parse_url($config->url);
        $host   = $parsed['host'] ?? 'localhost';
        $port   = $parsed['port'] ?? 4222;

        // The library's own reconnect is OFF. It retries forever, 1 ms apart,
        // inside whatever call hit the failure — so during a partition the
        // publisher or replayer coroutine never came back out, and after the
        // network healed it stayed stuck (two-node harness, 2026-09-28).
        // Failing fast and reconnecting here ({@see reconnect()}) keeps the
        // callers' own retry and health tracking in charge.
        $options = ['host' => $host, 'port' => $port, 'reconnect' => false];

        if ($config->credentialsPath !== null) {
            $options['nkey'] = $config->credentialsPath;
        }

        // Security: enable TLS with peer verification when CA file is configured (VULN-009)
        if ($config->tlsCaFile !== null) {
            $tlsCaFile = $config->tlsCaFile;
            if (!is_file($tlsCaFile) || !is_readable($tlsCaFile)) {
                throw new \InvalidArgumentException("NATS TLS CA file is not readable: {$tlsCaFile}");
            }

            /** @phpstan-ignore argument.type */
            $options['tls'] = [
                'verify_peer' => true,
                'cafile' => $tlsCaFile,
            ];
        }

        $this->options = $options;
        $this->client = $this->newClient();
        $this->aliveAt = microtime(true);
    }

    /**
     * Drop the connection so the next call opens a fresh one.
     *
     * A network partition leaves the TCP connection half-open: nothing tells
     * the client it is dead, every later request on it times out, and the
     * library never reconnects on its own. Measured in the two-node harness —
     * after the network healed, both nodes kept failing with "Processing
     * timeout" until restarted. Every operation below therefore reconnects on
     * failure; the caller's retry then runs on a new socket.
     */
    public function reconnect(): void
    {
        $stale = $this->client;
        $this->client = null;

        try {
            $stale?->disconnect();
        } catch (\Throwable) {
            // A dead socket cannot be closed cleanly; it is dropped either way.
        }
    }

    private function newClient(): Client
    {
        $this->aliveAt = microtime(true);

        // 'tls' is an array of stream-context options, which Configuration
        // accepts at runtime though its docblock types only scalars.
        /** @phpstan-ignore argument.type */
        return new Client(new Configuration($this->options));
    }

    private function client(): Client
    {
        return $this->client ??= $this->newClient();
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @return T
     */
    private function reconnectingOnFailure(callable $operation): mixed
    {
        try {
            return $operation();
        } catch (\Throwable $e) {
            $this->reconnect();
            throw $e;
        }
    }

    /**
     * Establish the TCP connection to the NATS server.
     */
    public function connect(): void
    {
        $this->reconnectingOnFailure(fn () => $this->client()->ping());
    }

    /**
     * Publish to a JetStream-captured subject and wait for the stream's PubAck.
     *
     * Returns the stream sequence the message was stored at. Throws when no
     * stream captured the subject or the server refused it, so a caller never
     * marks as published an event nothing stored. A PubAck flagged duplicate
     * (same Nats-Msg-Id inside the stream's duplicate window) still returns the
     * original sequence: the event IS in the stream.
     *
     * Headers ride on the Payload object — Client::publish() takes three
     * arguments and silently dropped the fourth, so Nats-Msg-Id never reached
     * the server and dedup never happened.
     *
     * @param array<string, string> $headers  e.g. ['Nats-Msg-Id' => $eventId]
     */
    public function jetStreamPublish(string $subject, string $payload, array $headers = [], float $timeoutSeconds = 5.0): int
    {
        $reply = $this->reconnectingOnFailure(
            fn () => $this->client()->dispatch($subject, new Payload($payload, $headers), $timeoutSeconds),
        );

        $body = $reply instanceof Payload ? $reply->body : (is_string($reply) ? $reply : '');
        $ack  = json_decode($body, true);

        if (!is_array($ack) || isset($ack['error']) || !is_int($ack['seq'] ?? null)) {
            $error  = is_array($ack) && is_array($ack['error'] ?? null) ? $ack['error'] : [];
            $reason = is_string($error['description'] ?? null)
                ? $error['description']
                : ($body === '' ? 'no stream captured the subject' : $body);
            throw new \RuntimeException("JetStream refused publish to '{$subject}': {$reason}");
        }

        return $ack['seq'];
    }

    /**
     * Core NATS publish (no JetStream, no dedup). Used for ephemeral reply subjects.
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): void
    {
        $this->client()->publish($subject, $payload, $replyTo);
    }

    /**
     * NATS request-reply (synchronous). Used by CommandBus::sendAndWait().
     *
     * @throws \RuntimeException on timeout
     */
    public function request(string $subject, string $payload, float $timeoutSeconds = 5.0): string
    {
        $response = null;

        // Basis\Nats\Client::request() waits via process($this->configuration->timeout) —
        // a single blocking read bounded by the CONFIGURATION timeout, not by the stream
        // timeout that setTimeout()/Connection::setTimeout() adjusts. That stream timeout
        // only governs how long a single fread() blocks; the actual wait budget for
        // request()'s reply loop comes straight from $client->configuration->timeout,
        // which is fixed to 1.0s at construction (Configuration::$timeout). Without also
        // overriding it here, a caller-specified $timeoutSeconds (e.g. 5.0) is silently
        // capped at ~1s and the RuntimeException below lies about how long it waited.
        //
        // setTimeout() lazily opens the connection (Connection::init()), which can
        // throw. The configuration change is therefore made inside the protected
        // region so a failed setup still restores it — otherwise a later connect
        // attempt would run with this request's timeout. The stream timeout is only
        // restored when setup succeeded: restoring it would re-enter init().
        $previousTimeout = $this->client()->configuration->timeout;
        $timeoutConfigured = false;

        try {
            $this->client()->configuration->timeout = $timeoutSeconds;
            $this->client()->setTimeout($timeoutSeconds);
            $timeoutConfigured = true;

            $this->client()->request($subject, $payload, function (string $body) use (&$response): void {
                $response = $body;
            });
        } finally {
            $this->client()->configuration->timeout = $previousTimeout;
            if ($timeoutConfigured) {
                $this->client()->setTimeout($previousTimeout);
            }
        }

        if ($response === null) {
            throw new \RuntimeException("NATS request to '{$subject}' timed out after {$timeoutSeconds}s");
        }

        return $response;
    }

    /**
     * Ensure a JetStream stream exists with the given configuration.
     * Creates it if absent; updates subjects if the stream already exists.
     *
     * @param array<string, mixed> $config  Stream configuration map
     */
    public function ensureStream(string $streamName, array $config): void
    {
        $api    = $this->client()->getApi();
        $stream = $api->getStream($streamName);

        $streamConfig = $stream->getConfiguration();
        $streamConfig->setSubjects($config['subjects'] ?? []);

        if (isset($config['max_age'])) {
            $streamConfig->setMaxAge($config['max_age']);
        }
        if (isset($config['max_bytes'])) {
            $streamConfig->setMaxBytesPerSubject($config['max_bytes']);
        }
        if (isset($config['storage'])) {
            $streamConfig->setStorageBackend($config['storage']);
        }
        if (isset($config['duplicate_window'])) {
            $streamConfig->setDuplicateWindow($config['duplicate_window']);
        }

        if (!$stream->exists()) {
            $stream->create();
        }
    }

    /**
     * Create or resume a durable pull consumer on an existing stream.
     *
     * An existing durable consumer keeps the configuration it was created
     * with; these settings apply on first creation only.
     *
     * @param int        $startSequence   JetStream sequence to start from (0 = from beginning).
     * @param float|null $ackWaitSeconds  How long a delivery may stay unacked before redelivery.
     * @param int|null   $maxDeliver      Deliveries after which JetStream stops redelivering.
     */
    public function ensurePullConsumer(
        string $streamName,
        string $consumerName,
        string $filterSubject,
        int $startSequence = 0,
        ?float $ackWaitSeconds = null,
        ?int $maxDeliver = null,
    ): void {
        $api      = $this->client()->getApi();
        $stream   = $api->getStream($streamName);
        $consumer = $stream->getConsumer($consumerName);

        $cfg = $consumer->getConfiguration();
        $cfg->setSubjectFilter($filterSubject);
        $cfg->setAckPolicy('explicit');

        if ($ackWaitSeconds !== null) {
            $cfg->setAckWait((int) ($ackWaitSeconds * 1_000_000_000));
        }
        if ($maxDeliver !== null) {
            $cfg->setMaxDeliver($maxDeliver);
        }

        if ($startSequence > 0) {
            $cfg->setDeliverPolicy(DeliverPolicy::BY_START_SEQUENCE);
            $cfg->setStartSequence($startSequence);
        } else {
            $cfg->setDeliverPolicy(DeliverPolicy::ALL);
        }

        $consumer->create();
    }

    /**
     * Pull up to $batchSize messages from a durable pull consumer WITHOUT
     * acknowledging them — the caller acks or naks each one.
     *
     * Consumer::handle() acks on receipt and hands its callback a bare Payload,
     * which has no ack()/nak(); this reads the delivery queue directly so the
     * reply subject survives.
     *
     * @return list<PulledMessage>
     */
    public function pullMessages(
        string $streamName,
        string $consumerName,
        int $batchSize = 50,
        float $waitSeconds = 1.0,
    ): array {
        return $this->reconnectingOnFailure(fn (): array => $this->pull($streamName, $consumerName, $batchSize, $waitSeconds));
    }

    /** @return list<PulledMessage> */
    private function pull(string $streamName, string $consumerName, int $batchSize, float $waitSeconds): array
    {
        $consumer = $this->client()->getApi()->getStream($streamName)->getConsumer($consumerName);
        $consumer->setBatching($batchSize)->setExpires($waitSeconds);

        // Read a little past the server-side expiry: a batch the server sends
        // just before it expires must not land on an inbox already abandoned
        // (it would sit unacked until ack_wait and arrive late, out of order).
        $queue = $consumer->getQueue();
        $queue->setTimeout($waitSeconds + 0.5);

        try {
            $raw = $queue->fetchAll($batchSize);
        } finally {
            $this->client()->unsubscribe($queue);
        }

        $messages = [];
        foreach ($raw as $msg) {
            // Status frames (404 no messages, 408 request timeout) are told
            // apart by their Status-Code header, not by an empty body: a job
            // published with an empty body is a real delivery that must reach
            // the callback and be acked.
            if (!$msg instanceof Msg || ($msg->payload->isEmpty() && $msg->payload->getHeader('Status-Code') !== null)) {
                continue;
            }

            $messages[] = new PulledMessage(
                body:           $msg->payload->body,
                streamSequence: PulledMessage::streamSequenceFromReplyTo($msg->replyTo),
                ack:            static fn () => $msg->ack(),
                nak:            static fn (float $delay) => $msg->nack($delay),
                deliveryCount:  PulledMessage::deliveryCountFromReplyTo($msg->replyTo),
            );
        }

        $this->assertAlive($messages !== []);

        return $messages;
    }

    /**
     * A half-open connection — what a network partition leaves behind — does
     * not fail a pull: the request goes into a dead socket and the pull simply
     * returns nothing, forever, looking exactly like a quiet stream. Measured
     * in the two-node harness: after the network healed, both replayers kept
     * "pulling" empty batches and never received another event. So after a
     * stretch of empty pulls the connection must answer a PING, or it is
     * dropped and the caller's next pull opens a new one.
     */
    private function assertAlive(bool $delivered): void
    {
        $now = microtime(true);
        if ($delivered) {
            $this->aliveAt = $now;
            return;
        }
        if ($now - $this->aliveAt < self::LIVENESS_AFTER) {
            return;
        }

        if (!$this->client()->ping()) {
            throw new \RuntimeException('NATS connection did not answer PING; reconnecting.');
        }
        $this->aliveAt = $now;
    }

    /** How many messages the stream currently holds. */
    public function streamMessageCount(string $streamName): int
    {
        return self::apiInt($this->client()->getApi()->getStream($streamName)->info(), 'state.messages');
    }

    /** Whether the server runs JetStream (a plain NATS server answers with an error). */
    public function hasJetStream(): bool
    {
        try {
            $this->client()->getApi()->getInfo();

            return true;
        } catch (\Throwable) {
            $this->reconnect();

            return false;
        }
    }

    /** Deliveries of a consumer still waiting for an ack. */
    public function pendingAcks(string $streamName, string $consumerName): int
    {
        return self::apiInt($this->client()->getApi()->getStream($streamName)->getConsumer($consumerName)->info(), 'num_ack_pending');
    }

    /**
     * A number from a JetStream API reply. The reply is a Payload whose fields
     * are read through a magic __get with no __isset — so `isset($reply->x)`
     * and `$reply->x ?? 0` are always "missing", and every count read that
     * way was 0. getValue() walks the decoded body by a dotted path.
     */
    private static function apiInt(mixed $reply, string $path): int
    {
        return $reply instanceof Payload ? Row::asInt($reply->getValue($path)) : 0;
    }

    /** Remove a durable consumer. Test and operator cleanup. */
    public function deleteConsumer(string $streamName, string $consumerName): void
    {
        $consumer = $this->client()->getApi()->getStream($streamName)->getConsumer($consumerName);
        if ($consumer->exists()) {
            $consumer->delete();
        }
    }

    /** Remove a stream and every message in it. Test and operator cleanup. */
    public function deleteStream(string $streamName): void
    {
        $stream = $this->client()->getApi()->getStream($streamName);
        if ($stream->exists()) {
            $stream->delete();
        }
    }

    /**
     * Subscribe to a subject (core NATS, not JetStream). Used by CommandProcessor
     * to receive commands routed to this node.
     */
    public function subscribe(string $subject, callable $callback): void
    {
        $this->client()->subscribe($subject, function (Payload $payload) use ($callback): void {
            $callback($payload->body, $payload->replyTo, $payload);
        });
    }

    /**
     * Process any pending incoming messages (non-blocking tick).
     */
    public function process(float $timeoutSeconds = 0.0): void
    {
        $this->client()->process($timeoutSeconds);
    }
}
