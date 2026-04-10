<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Queue;

use Semitexa\Core\Queue\QueueTransportInterface;

/**
 * Dual-write transport for the RabbitMQ → NATS migration Phase 1.
 *
 * Publishes to both primary and secondary transports. Consumers read
 * from the primary only. If secondary publish fails it is logged and
 * silently skipped — NATS is non-critical during this migration phase.
 *
 * Activation:
 *   EVENTS_TRANSPORT=dual
 *   EVENTS_DUAL_PRIMARY=rabbitmq
 *   EVENTS_DUAL_SECONDARY=nats
 */
final class DualWriteTransport implements QueueTransportInterface
{
    public function __construct(
        private readonly QueueTransportInterface $primary,
        private readonly QueueTransportInterface $secondary,
    ) {}

    public function publish(string $queueName, string $payload): void
    {
        $this->primary->publish($queueName, $payload);

        try {
            $this->secondary->publish($queueName, $payload);
        } catch (\Throwable $e) {
            error_log(sprintf(
                '[semitexa-ledger] DualWriteTransport secondary publish failed (queue=%s): %s',
                $queueName,
                $e->getMessage(),
            ));
        }
    }

    public function consume(string $queueName, callable $callback): void
    {
        // Consume from primary only during dual-write phase.
        $this->primary->consume($queueName, $callback);
    }
}
