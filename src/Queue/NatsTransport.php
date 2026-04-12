<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Queue;

use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Ledger\Nats\ClusterRegistry;

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
    private bool $streamReady = false;

    public function __construct(
        private readonly ClusterRegistry $clusters,
    ) {}

    public function publish(string $queueName, string $payload): void
    {
        $this->ensureQueueStream();
        $subject = self::SUBJECT_PREFIX . $queueName;
        $client  = $this->primaryClient();
        $client->jetStreamPublish($subject, $payload);
    }

    public function consume(string $queueName, callable $callback): void
    {
        $this->ensureQueueStream();
        $subject      = self::SUBJECT_PREFIX . $queueName;
        $consumerName = 'queue-' . preg_replace('/[^a-z0-9-]/', '-', strtolower($queueName));
        $client       = $this->primaryClient();

        $client->ensurePullConsumer(
            streamName:    self::STREAM_NAME,
            consumerName:  $consumerName,
            filterSubject: $subject,
        );

        // Blocking consume loop for queue:work command.
        while (true) {
            $messages = $client->pullMessages(
                streamName:   self::STREAM_NAME,
                consumerName: $consumerName,
                batchSize:    10,
            );

            foreach ($messages as $msg) {
                $callback((string) $msg->body);
                $msg->ack();
            }

            if (empty($messages)) {
                sleep(1);
            }
        }
    }

    private function primaryClient(): \Semitexa\Ledger\Nats\NatsClient
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
