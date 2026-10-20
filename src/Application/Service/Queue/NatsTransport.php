<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Queue;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;

/**
 * NATS JetStream implementation of the existing QueueTransportInterface.
 *
 * NATS-based async queue transport. Set EVENTS_TRANSPORT=nats to
 * activate. Queue names become NATS subjects under the semitexa.queue.> prefix.
 *
 * Messages are published to JetStream (stream must be configured to capture
 * `semitexa.queue.>` subjects). Consumers are created per queue name.
 */
final class NatsTransport implements QueueTransportInterface
{
    private const SUBJECT_PREFIX = 'semitexa.queue.';
    private const STREAM_NAME    = 'QUEUE';
    /** Seconds a delivery may stay unacked before JetStream redelivers it — the database transport's lease. */
    private const ACK_WAIT    = 300.0;
    /** Deliveries before a failing message stops being redelivered — the database transport's max_attempts. */
    private const MAX_DELIVER = 5;
    private const RETRY_DELAY = 5.0;

    private bool $streamReady = false;

    /** Cleared by stop() to end consume() after the current batch. */
    private bool $running = true;

    /** @var array<string, string> queue name => the consumer it is read through */
    private array $consumerNames = [];

    public function __construct(
        private readonly ClusterRegistry $clusters,
        private readonly float $retryDelaySeconds = self::RETRY_DELAY,
    ) {}

    public function publish(string $queueName, string $payload): void
    {
        $subject = self::SUBJECT_PREFIX . $queueName;

        // One id for both attempts: JetStream drops a second copy with the
        // same Nats-Msg-Id inside its duplicate window, so a retry after a
        // send the server did store (an ack that timed out) cannot deliver a
        // job twice — a second email, say.
        $headers = ['Nats-Msg-Id' => 'q-' . bin2hex(random_bytes(16))];

        $attempt = function () use ($subject, $payload, $headers): void {
            $this->ensureQueueStream();
            $this->primaryClient()->jetStreamPublish($subject, $payload, $headers);
        };

        try {
            $attempt();
        } catch (\Throwable) {
            // A socket that died with a NATS restart fails the first call —
            // the stream setup or the send ('Error sending data'). Publishers
            // do not retry: media ingest logged the failure and left the
            // variant queued forever. So the transport drops the socket and
            // retries once, setup included, on a fresh connection.
            $this->primaryClient()->reconnect();
            $attempt();
        }
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function consume(string $queueName, callable $callback): void
    {
        // Blocking consume loop for queue:work command.
        while ($this->running) {
            if ($this->consumeBatch($queueName, $callback) === 0) {
                sleep(1);
            }
        }
    }

    /**
     * Pull one batch and hand each message to $callback. Returns how many
     * arrived. The consume loop is this in a loop; tests drive it directly.
     *
     * At-least-once, like the database transport: a message is acked only
     * after the callback returned. A callback that throws gets it back after
     * the retry delay; a worker that dies mid-job gets it back after ACK_WAIT; one
     * that keeps failing stops being delivered after MAX_DELIVER. Messages
     * used to be acked on receipt, so a crash lost the job.
     *
     * @param callable(string):void $callback
     */
    public function consumeBatch(string $queueName, callable $callback): int
    {
        $this->ensureQueueStream();
        $subject      = self::SUBJECT_PREFIX . $queueName;
        $client       = $this->primaryClient();
        $consumerName = $this->consumerFor($client, $queueName, $subject);

        $messages = $client->pullMessages(
            streamName:   self::STREAM_NAME,
            consumerName: $consumerName,
            batchSize:    10,
        );

        foreach ($messages as $msg) {
            try {
                $callback($msg->body);
            } catch (\Throwable $e) {
                $context = [
                    'queue'    => $queueName,
                    'attempt'  => $msg->deliveryCount,
                    'error'    => $e->getMessage(),
                    'class'    => get_class($e),
                    'trace'    => $e->getTraceAsString(),
                ];
                if ($msg->deliveryCount >= self::MAX_DELIVER) {
                    // JetStream will not deliver it again: the job is gone.
                    StaticLoggerBridge::error('queue', sprintf('NATS queue job dropped after %d attempts', self::MAX_DELIVER), $context);
                } else {
                    StaticLoggerBridge::error('queue', 'NATS queue job failed, will be redelivered', $context);
                }
                $msg->nak($this->retryDelaySeconds);
                continue;
            }

            $msg->ack();
        }

        return count($messages);
    }

    /**
     * The name a queue's durable consumer is looked for under first: the queue
     * name mapped onto [a-z0-9-], as every queue's consumer has always been
     * named. The mapping can join two queues (Mail and mail, media.thumbs and
     * media-thumbs); {@see consumerFor()} then gives the second one
     * {@see ownConsumerName()}.
     */
    public static function consumerName(string $queueName): string
    {
        return 'queue-' . preg_replace('/[^a-z0-9-]/', '-', strtolower($queueName));
    }

    /**
     * A name only this queue maps to: the mapped name, then a hash of the
     * queue name after a character the mapping never produces.
     */
    public static function ownConsumerName(string $queueName): string
    {
        return self::consumerName($queueName) . '_' . substr(hash('sha256', $queueName), 0, 12);
    }

    /**
     * The consumer this queue is read through. A consumer keeps the subject it
     * was made with, so when the mapped name holds another queue's subject,
     * reading through it took that queue's jobs and left this one's unread:
     * the queue then uses its own name instead. A consumer that is this
     * queue's own keeps its name and its place, whatever other queues do, and
     * nothing is ever moved or deleted here. A name that is free is created
     * only if it still is ({@see NatsClient::createPullConsumerIfAbsent()}), so
     * two queues' workers that reach it at once cannot share it.
     *
     * The choice holds as long as the consumers do: delete the one under the
     * mapped name, and the next queue to start takes it from the stream's
     * first message, while the queue that had it moves to its own name and
     * starts there too. Both then run their jobs again.
     */
    private function consumerFor(\Semitexa\Ledger\Application\Service\Nats\NatsClient $client, string $queueName, string $subject): string
    {
        if (isset($this->consumerNames[$queueName])) {
            return $this->consumerNames[$queueName];
        }

        foreach ([self::consumerName($queueName), self::ownConsumerName($queueName)] as $candidate) {
            $state = $client->consumerState(self::STREAM_NAME, $candidate);
            if ($state === null) {
                if (!$client->createPullConsumerIfAbsent(self::STREAM_NAME, $candidate, $subject, self::ACK_WAIT, self::MAX_DELIVER)) {
                    continue;
                }
            } elseif ($state['filterSubject'] !== $subject) {
                continue;
            } else {
                // Ours already: same subject, so this only brings its settings up to date.
                $client->ensurePullConsumer(
                    streamName:     self::STREAM_NAME,
                    consumerName:   $candidate,
                    filterSubject:  $subject,
                    ackWaitSeconds: self::ACK_WAIT,
                    maxDeliver:     self::MAX_DELIVER,
                );
            }
            if ($candidate !== self::consumerName($queueName)) {
                StaticLoggerBridge::info('queue', 'NATS queue reads through a consumer of its own: its mapped name belongs to another queue', [
                    'queue'    => $queueName,
                    'consumer' => $candidate,
                ]);
            }

            return $this->consumerNames[$queueName] = $candidate;
        }

        throw new \RuntimeException(sprintf(
            "NATS consumers '%s' and '%s' both read another subject than '%s': delete or rename the one that is not a queue's.",
            self::consumerName($queueName),
            self::ownConsumerName($queueName),
            $subject,
        ));
    }

    private function primaryClient(): \Semitexa\Ledger\Application\Service\Nats\NatsClient
    {
        $clusters = $this->clusters->getOrderedByPriority();
        if ($clusters === []) {
            throw new \RuntimeException('No NATS clusters configured.');
        }

        return $clusters[0]['client'];
    }

    private function ensureQueueStream(): void
    {
        if ($this->streamReady) {
            return;
        }

        $this->primaryClient()->ensureStream(self::STREAM_NAME, [
            'subjects' => [self::SUBJECT_PREFIX . '>'],
        ]);

        $this->streamReady = true;
    }
}
