<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration\Fixture;

use Semitexa\Ledger\Attribute\AsReplayHandler;
use Semitexa\Ledger\Domain\Contract\ReplayHandlerInterface;
use Semitexa\Ledger\Domain\Model\LedgerEvent;

/**
 * Stands in for a projection into the main database: records what it was
 * asked to apply, and can be told to fail the next N applies.
 */
#[AsReplayHandler(domain: 'ledgerit', eventType: 'replication_probe_recorded')]
final class RecordingReplayHandler implements ReplayHandlerInterface
{
    /** @var list<string> probe ids, in apply order */
    public array $applied = [];

    public int $failNext = 0;

    public function apply(LedgerEvent $event): void
    {
        if ($this->failNext > 0) {
            $this->failNext--;
            throw new \RuntimeException('projection unavailable');
        }

        $this->applied[] = (string) $event->payload['probeId'];
    }
}
