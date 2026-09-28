<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Exception;

use Semitexa\Ledger\Domain\Model\HlcTimestamp;

/**
 * A remote clock reading lies further in the future than this node allows.
 *
 * Accepting it would let one node with a fast clock win every conflict for as
 * long as its lead lasts, and drag every clock that observes it along. The
 * change is not lost: the caller retries it later, and it applies once this
 * node's own time has come within the allowed drift.
 */
final class ClockDriftException extends \RuntimeException
{
    public static function ahead(HlcTimestamp $remote, int $nowMs, int $maxDriftMs): self
    {
        return new self(sprintf(
            'Remote clock %s is %d ms ahead of this node (allowed: %d ms). Check NTP on the sending node.',
            $remote->toString(),
            $remote->wallMs - $nowMs,
            $maxDriftMs,
        ));
    }
}
