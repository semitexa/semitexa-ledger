<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Queue;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Queue\DelayedQueueTransportInterface;
use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Core\Queue\StoppableQueueTransportInterface;

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
final class DualWriteTransport implements StoppableQueueTransportInterface
{
    public function __construct(
        private readonly QueueTransportInterface $primary,
        private readonly QueueTransportInterface $secondary,
    ) {}

    /**
     * One that holds a message back when the primary can. Without it a worker
     * slept a retry's delay inside the consumer (every message behind it
     * waited), and mail dropped a message whose row was not visible yet.
     */
    public static function of(QueueTransportInterface $primary, QueueTransportInterface $secondary): StoppableQueueTransportInterface
    {
        return $primary instanceof DelayedQueueTransportInterface
            ? new DelayedDualWriteTransport(new self($primary, $secondary), $primary, $secondary)
            : new self($primary, $secondary);
    }

    public function publish(string $queueName, string $payload): void
    {
        $this->primary->publish($queueName, $payload);

        try {
            $this->secondary->publish($queueName, $payload);
        } catch (\Throwable $e) {
            StaticLoggerBridge::warning('queue', 'DualWriteTransport secondary publish failed', [
                'queue' => $queueName,
                'error' => $e->getMessage(),
            ]);
        }
    }

    public function consume(string $queueName, callable $callback): void
    {
        // Consume from primary only during dual-write phase.
        $this->primary->consume($queueName, $callback);
    }

    /** consume() reads the primary: stopping it stops this one. A primary that cannot stop, nothing. */
    public function stop(): void
    {
        if ($this->primary instanceof StoppableQueueTransportInterface) {
            $this->primary->stop();
        }
    }
}
