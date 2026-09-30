<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Model;

/**
 * A remote change this node could not take whole because it broke a
 * constraint here — a unique value, a CHECK bound (ADR 0001 §5).
 *
 * Both versions travel with it, each with its clocks, so application code can
 * decide what the row should be; its decision is an ordinary replicated write.
 * The fields named in {@see $columns} were NOT applied; every other field the
 * change won was.
 */
final readonly class ReplicationConflict
{
    /**
     * @param list<string> $columns the fields left unapplied; empty when the row could not be created at all
     * @param array<string, mixed> $incoming column => value the remote change carried
     * @param array<string, array{0: string, 1: string}> $incomingClocks column => [hlc, node]
     * @param array<string, mixed>|null $local column => value of this row here; null when it does not exist here
     * @param array<string, array{0: string, 1: string}> $localClocks column => [hlc, node]
     */
    public function __construct(
        public string $table,
        public string $rowKey,
        public array $columns,
        public array $incoming,
        public array $incomingClocks,
        public ?array $local,
        public array $localClocks,
        public string $originNode,
        public string $reason,
    ) {}

    /**
     * Identity of the conflict, not of the event: every later change of the row
     * re-sends the same unapplied stamps, and must not record it again.
     */
    public function key(): string
    {
        $stamps = [];
        foreach ($this->columns === [] ? array_keys($this->incomingClocks) : $this->columns as $column) {
            [$hlc, $node] = $this->incomingClocks[$column] ?? ['', ''];
            $stamps[] = $column . '=' . $hlc . '/' . $node;
        }
        sort($stamps);

        return sha1($this->table . "\0" . $this->rowKey . "\0" . implode(',', $stamps));
    }
}
