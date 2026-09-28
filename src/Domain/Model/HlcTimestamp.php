<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Model;

/**
 * A hybrid logical clock reading: wall-clock milliseconds plus a counter that
 * orders readings within the same millisecond (or while the wall clock lags a
 * time already seen).
 *
 * The string form is fixed-width, so comparing two strings compares the
 * readings — it can be stored and indexed as-is. Ties between nodes are broken
 * by node id (see {@see self::wins()}); two readings of one node never tie for
 * the same field, because a write to a row first observes that row's clocks.
 */
final class HlcTimestamp
{
    public const MAX_COUNTER = 999_999;
    private const FORMAT = '%013d.%06d';
    private const PATTERN = '/^(\d{13})\.(\d{6})$/';

    public function __construct(
        public readonly int $wallMs,
        public readonly int $counter,
    ) {
        if ($wallMs < 0 || $wallMs > 9_999_999_999_999) {
            throw new \InvalidArgumentException("HLC wall time out of range: {$wallMs}");
        }
        if ($counter < 0 || $counter > self::MAX_COUNTER) {
            throw new \InvalidArgumentException("HLC counter out of range: {$counter}");
        }
    }

    public static function fromString(string $value): self
    {
        if (preg_match(self::PATTERN, $value, $m) !== 1) {
            throw new \InvalidArgumentException("Not an HLC timestamp: '{$value}'");
        }

        return new self((int) $m[1], (int) $m[2]);
    }

    public function toString(): string
    {
        return sprintf(self::FORMAT, $this->wallMs, $this->counter);
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    /** <0, 0 or >0, like strcmp — the clock order alone, no node tiebreak. */
    public function compareTo(self $other): int
    {
        return [$this->wallMs, $this->counter] <=> [$other->wallMs, $other->counter];
    }

    /**
     * Whether a write stamped ($this, $node) beats one stamped ($other,
     * $otherNode). Total and identical on every node, so every node keeps the
     * same winner.
     */
    public function wins(string $node, self $other, string $otherNode): bool
    {
        $byClock = $this->compareTo($other);

        return $byClock !== 0 ? $byClock > 0 : strcmp($node, $otherNode) > 0;
    }
}
