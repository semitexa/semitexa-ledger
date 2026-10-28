<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Queue;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Queue\QueueTransportFactoryInterface;
use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Core\Queue\QueueTransportRegistry;

/**
 * EVENTS_TRANSPORT=dual. The secondary was always NATS, whatever
 * EVENTS_DUAL_SECONDARY said: with the documented PRIMARY=nats every message
 * went to the same stream twice and every job ran twice.
 */
final class DualWriteTransportFactory implements QueueTransportFactoryInterface
{
    /**
     * A name core registers a second time for one transport. The registry
     * makes an instance per name, so database and db were two writers into the
     * same queue_messages table: every job queued twice and run twice.
     */
    private const ALIASES = ['db' => 'database'];

    public function __construct(
        private readonly string $primaryName,
        private readonly string $secondaryName,
    ) {}

    public function create(): QueueTransportInterface
    {
        // 'dual' on either side called itself until the stack ran out.
        $primaryKey = self::key($this->primaryName);
        $secondaryKey = self::key($this->secondaryName);
        if ($primaryKey === 'dual') {
            throw new \InvalidArgumentException("EVENTS_DUAL_PRIMARY cannot be 'dual'.");
        }
        $primary = QueueTransportRegistry::create($this->primaryName);

        // The same transport twice ran every job twice; one the registry does
        // not know failed every publish. A secondary is never worth either.
        if ($secondaryKey === $primaryKey || $secondaryKey === 'dual') {
            StaticLoggerBridge::warning('queue', "EVENTS_DUAL_SECONDARY '{$this->secondaryName}' cannot be a second copy of EVENTS_DUAL_PRIMARY '{$this->primaryName}': publishing to the primary only.");

            return $primary;
        }
        try {
            $secondary = QueueTransportRegistry::create($this->secondaryName);
        } catch (\Throwable $e) {
            StaticLoggerBridge::warning('queue', "EVENTS_DUAL_SECONDARY '{$this->secondaryName}' is not usable ({$e->getMessage()}): publishing to the primary only.");

            return $primary;
        }

        return DualWriteTransport::of($primary, $secondary);
    }

    private static function key(string $name): string
    {
        $key = strtolower($name);

        return self::ALIASES[$key] ?? $key;
    }
}
