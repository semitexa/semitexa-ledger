<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Payload\Event;

use Semitexa\Ledger\Attribute\Propagated;

/**
 * A marker event with no business meaning, written by `ledger:probe` to prove
 * that events leave this node and reach its peers. Peers store it like any
 * other event; no replay handler projects it anywhere.
 */
#[Propagated(domain: 'ledger')]
final class LedgerProbeRecorded
{
    private string $probeId = '';
    private string $note = '';

    public static function of(string $probeId, string $note): self
    {
        $event = new self();
        $event->probeId = $probeId;
        $event->note = $note;

        return $event;
    }

    public function getProbeId(): string { return $this->probeId; }
    public function setProbeId(string $probeId): void { $this->probeId = $probeId; }

    public function getNote(): string { return $this->note; }
    public function setNote(string $note): void { $this->note = $note; }
}
