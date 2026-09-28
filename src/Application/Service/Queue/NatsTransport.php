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

    /** @var array<string, true> */
    private array $consumerReady = [];

    public function __construct(
        private readonly ClusterRegistry $clusters,
        private readonly float $retryDelaySeconds = self::RETRY_DELAY,
    ) {}

    public function publish(string $queueName, string $payload): void
    {
        $this->ensureQueueStream();
        $subject = self::SUBJECT_PREFIX . $queueName;
        $client  = $this->primaryClient();
        $client->jetStreamPublish($subject, $payload);
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
        $consumerName = self::consumerName($queueName);
        $client       = $this->primaryClient();

        if (!isset($this->consumerReady[$consumerName])) {
            $client->ensurePullConsumer(
                streamName:     self::STREAM_NAME,
                consumerName:   $consumerName,
                filterSubject:  $subject,
                ackWaitSeconds: self::ACK_WAIT,
                maxDeliver:     self::MAX_DELIVER,
            );
            $this->consumerReady[$consumerName] = true;
        }

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

    public static function consumerName(string $queueName): string
    {
        return 'queue-' . preg_replace('/[^a-z0-9-]/', '-', strtolower($queueName));
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
