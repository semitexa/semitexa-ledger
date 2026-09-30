<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Payload\Event;

use Semitexa\Core\Attribute\AsEvent;
use Semitexa\Ledger\Domain\Model\ReplicationConflict;

/**
 * A remote change broke a constraint on this node and was applied only in part
 * (ADR 0001 §5). Local by design — never #[Propagated]: every node that hits
 * the conflict reports its own, and the resolution a listener writes is an
 * ordinary replicated write that reaches the others anyway.
 *
 * Dispatched once per conflict, after the partial apply has committed. The
 * same conflict stays in `replication_conflict`, so a listener that was down
 * can still find it there.
 */
#[AsEvent]
final class ReplicationConflictDetected
{
    private ?ReplicationConflict $conflict = null;

    public static function of(ReplicationConflict $conflict): self
    {
        $event = new self();
        $event->conflict = $conflict;

        return $event;
    }

    public function getConflict(): ?ReplicationConflict { return $this->conflict; }
    public function setConflict(?ReplicationConflict $conflict): void { $this->conflict = $conflict; }
}
