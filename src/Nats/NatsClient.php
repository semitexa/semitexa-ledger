<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Nats;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Consumer\DeliverPolicy;
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
    private readonly Client $client;

    public function __construct(ClusterConfig $config)
    {
        $parsed = parse_url($config->url);
        $host   = $parsed['host'] ?? 'localhost';
        $port   = $parsed['port'] ?? 4222;

        $options = ['host' => $host, 'port' => $port];

        if ($config->credentialsPath !== null) {
            $options['nkey'] = $config->credentialsPath;
        }

        // Security: enable TLS with peer verification when CA file is configured (VULN-009)
        if ($config->tlsCaFile !== null) {
            /** @phpstan-ignore argument.type */
            $options['tls'] = [
                'verify_peer' => true,
                'cafile' => $config->tlsCaFile,
            ];
        }

        $this->client = new Client(new Configuration($options));
    }

    /**
     * Establish the TCP connection to the NATS server.
     */
    public function connect(): void
    {
        $this->client->ping();
    }

    /**
     * Publish a message to a JetStream-captured subject with deduplication headers.
     *
     * JetStream captures the message if the subject matches a configured stream.
     * The Nats-Msg-Id header enables server-side deduplication within DuplicateWindow.
     *
     * @param array<string, string> $headers  e.g. ['Nats-Msg-Id' => $eventId]
     */
    public function jetStreamPublish(string $subject, string $payload, array $headers = []): void
    {
        // basis-company/nats sends headers via the Payload object when supported.
        // The Nats-Msg-Id header is set as a NATS header (NATS 2.x HPUB protocol).
        $this->client->publish($subject, $payload, null, $headers);
    }

    /**
     * Core NATS publish (no JetStream, no dedup). Used for ephemeral reply subjects.
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): void
    {
        $this->client->publish($subject, $payload, $replyTo);
    }

    /**
     * NATS request-reply (synchronous). Used by CommandBus::sendAndWait().
     *
     * @throws \RuntimeException on timeout
     */
    public function request(string $subject, string $payload, float $timeoutSeconds = 5.0): string
    {
        $response = null;
        $previousTimeout = $this->client->configuration->timeout;
        $this->client->setTimeout($timeoutSeconds);

        $this->client->request($subject, $payload, function (string $body) use (&$response): void {
            $response = $body;
        });

        $this->client->setTimeout($previousTimeout);

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
        $api    = $this->client->getApi();
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
     * @param int $startSequence  JetStream sequence to start from (0 = from beginning).
     */
    public function ensurePullConsumer(
        string $streamName,
        string $consumerName,
        string $filterSubject,
        int $startSequence = 0,
    ): void {
        $api      = $this->client->getApi();
        $stream   = $api->getStream($streamName);
        $consumer = $stream->getConsumer($consumerName);

        $cfg = $consumer->getConfiguration();
        $cfg->setSubjectFilter($filterSubject);
        $cfg->setAckPolicy('explicit');

        if ($startSequence > 0) {
            $cfg->setDeliverPolicy(DeliverPolicy::BY_START_SEQUENCE);
            $cfg->setStartSequence($startSequence);
        } else {
            $cfg->setDeliverPolicy(DeliverPolicy::ALL);
        }

        $consumer->create();
    }

    /**
     * Pull a batch of messages from a durable pull consumer.
     *
     * Each message in the returned array has:
     *   ->body   — raw string payload
     *   ->ack()  — acknowledge delivery
     *   ->nak()  — negative-acknowledge (triggers redelivery)
     *
     * @return list<Payload>
     */
    public function pullMessages(
        string $streamName,
        string $consumerName,
        int $batchSize = 50,
    ): array {
        $api      = $this->client->getApi();
        $stream   = $api->getStream($streamName);
        $consumer = $stream->getConsumer($consumerName);

        $messages = [];

        $consumer
            ->setIterations($batchSize)
            ->handle(function (Payload $payload) use (&$messages): void {
                $messages[] = $payload;
            });

        return $messages;
    }

    /**
     * Subscribe to a subject (core NATS, not JetStream). Used by CommandProcessor
     * to receive commands routed to this node.
     */
    public function subscribe(string $subject, callable $callback): void
    {
        $this->client->subscribe($subject, function (Payload $payload) use ($callback): void {
            $callback($payload->body, $payload->replyTo, $payload);
        });
    }

    /**
     * Process any pending incoming messages (non-blocking tick).
     */
    public function process(float $timeoutSeconds = 0.0): void
    {
        $this->client->process($timeoutSeconds);
    }
}
