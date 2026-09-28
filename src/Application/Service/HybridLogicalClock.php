<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Exception\ClockDriftException;

/**
 * Hybrid logical clock (Kulkarni et al., 2014): readings follow wall-clock
 * time, never go backwards, and are always later than every reading this clock
 * has observed — so a change is never ordered before a change it has seen.
 *
 * One instance per worker, with no shared or persisted state, on purpose. The
 * ordering that matters is per field, and it is enforced by the data: before a
 * write stamps a row, it observes that row's existing field clocks (read under
 * the row lock), so a later write to a row always carries a greater reading,
 * whichever worker makes it and whatever happened to the wall clock since.
 */
final class HybridLogicalClock
{
    public const DEFAULT_MAX_DRIFT_MS = 60_000;

    private int $lastWallMs = 0;
    private int $lastCounter = 0;

    /** @var \Closure(): int */
    private readonly \Closure $physicalMs;

    /**
     * @param (\Closure(): int)|null $physicalMs wall-clock milliseconds; tests inject their own
     */
    public function __construct(
        ?\Closure $physicalMs = null,
        private readonly int $maxDriftMs = self::DEFAULT_MAX_DRIFT_MS,
    ) {
        $this->physicalMs = $physicalMs ?? static fn (): int => (int) floor(microtime(true) * 1000);
    }

    /** A new reading for a local write, later than every reading so far. */
    public function now(): HlcTimestamp
    {
        $physical = ($this->physicalMs)();

        if ($physical > $this->lastWallMs) {
            $this->lastWallMs = $physical;
            $this->lastCounter = 0;
        } else {
            $this->advanceCounter();
        }

        return new HlcTimestamp($this->lastWallMs, $this->lastCounter);
    }

    /**
     * Take in a reading from elsewhere — a remote change, or the clock already
     * stored on a row about to be written — so that later readings exceed it.
     *
     * @throws ClockDriftException when $seen is further ahead than the allowed drift
     */
    public function observe(HlcTimestamp $seen): void
    {
        $physical = ($this->physicalMs)();

        if ($seen->wallMs - $physical > $this->maxDriftMs) {
            throw ClockDriftException::ahead($seen, $physical, $this->maxDriftMs);
        }

        $wall = max($this->lastWallMs, $seen->wallMs, $physical);

        if ($wall === $this->lastWallMs && $wall === $seen->wallMs) {
            $this->lastCounter = max($this->lastCounter, $seen->counter);
        } elseif ($wall === $seen->wallMs) {
            $this->lastCounter = $seen->counter;
        } elseif ($wall !== $this->lastWallMs) {
            $this->lastCounter = 0;
        }
        $this->lastWallMs = $wall;
    }

    /**
     * The reading for a write that must come after $seen: observe, then tick.
     *
     * @throws ClockDriftException
     */
    public function after(HlcTimestamp $seen): HlcTimestamp
    {
        $this->observe($seen);

        return $this->now();
    }

    private function advanceCounter(): void
    {
        if ($this->lastCounter < HlcTimestamp::MAX_COUNTER) {
            $this->lastCounter++;
            return;
        }

        // A million readings inside one millisecond: borrow the next one.
        $this->lastWallMs++;
        $this->lastCounter = 0;
    }
}
