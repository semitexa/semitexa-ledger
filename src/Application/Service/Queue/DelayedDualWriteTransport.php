<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Queue;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Queue\DelayedQueueTransportInterface;
use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Core\Queue\StoppableQueueTransportInterface;

/** {@see DualWriteTransport} over a primary that can hold a message back. */
final class DelayedDualWriteTransport implements DelayedQueueTransportInterface, StoppableQueueTransportInterface
{
    public function __construct(
        private readonly DualWriteTransport $dual,
        private readonly DelayedQueueTransportInterface $primary,
        private readonly QueueTransportInterface $secondary,
    ) {}

    public function publish(string $queueName, string $payload): void
    {
        $this->dual->publish($queueName, $payload);
    }

    public function publishDelayed(string $queueName, string $payload, int $delaySeconds): void
    {
        $this->primary->publishDelayed($queueName, $payload, $delaySeconds);

        // Nobody consumes the secondary yet: one that cannot wait gets the
        // message now, so it is there after the switch.
        try {
            $this->secondary instanceof DelayedQueueTransportInterface
                ? $this->secondary->publishDelayed($queueName, $payload, $delaySeconds)
                : $this->secondary->publish($queueName, $payload);
        } catch (\Throwable $e) {
            StaticLoggerBridge::warning('queue', 'DualWriteTransport secondary publish failed', [
                'queue' => $queueName,
                'error' => $e->getMessage(),
            ]);
        }
    }

    public function consume(string $queueName, callable $callback): void
    {
        $this->dual->consume($queueName, $callback);
    }

    public function stop(): void
    {
        $this->dual->stop();
    }
}
