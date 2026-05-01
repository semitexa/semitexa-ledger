<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Queue;

use Semitexa\Core\Queue\QueueTransportInterface;

/**
 * Dual-write transport for zero-downtime queue migrations.
 *
 * Publishes to both primary and secondary transports. Consumers read
 * from the primary only. If secondary publish fails it is logged and
 * silently skipped.
 *
 * Activation:
 *   EVENTS_TRANSPORT=dual
 *   EVENTS_DUAL_PRIMARY=nats
 *   EVENTS_DUAL_SECONDARY=<other-transport>
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
