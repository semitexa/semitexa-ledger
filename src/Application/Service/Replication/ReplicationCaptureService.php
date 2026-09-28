<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Application\Service\UuidV7;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Domain\Contract\ReplicationCaptureInterface;
use Semitexa\Orm\Domain\Enum\ResourceChangeOperation;
use Semitexa\Orm\Domain\Model\RowChange;

/**
 * Turns a write to a #[Replicated] row into field clocks and one outbox row,
 * inside the write's own transaction (ADR 0001).
 *
 * The event carries the WHOLE row with the clock of every field — the changed
 * fields stamped with this write's reading, the others with the clocks they
 * already had. A node receiving it can build the row correctly whatever else it
 * has or has not seen yet: delivery is ordered per origin only, so a node may
 * well get B's edit of a row before A's insert of it.
 */
final class ReplicationCaptureService implements ReplicationCaptureInterface
{
    public const EXISTS = '__exists';
    public const EVENT_DOMAIN = 'replication';
    public const EVENT_TYPE = 'row_changed';

    /** The clock of a field no replicated write has set yet: loses to any real one. */
    public const ZERO_HLC = '0000000000000.000000';

    public function __construct(
        private readonly string $nodeId,
        private readonly HybridLogicalClock $clock,
    ) {}

    public function capture(RowChange $change, DatabaseAdapterInterface $transaction): void
    {
        $changed = $this->changedColumns($change);
        if ($changed === []) {
            return; // an update that stored what was already there
        }

        $key    = RowCodec::encodeKey($change->primaryKeyValue);
        $clocks = FieldClocks::lockAndRead($transaction, $change->tableName, $key);

        // After every clock the row already carries: this is what makes a later
        // write to a row always win over an earlier one, whichever worker wrote.
        $latest = $clocks->latest();
        $stamp  = $latest !== null ? $this->clock->after($latest) : $this->clock->now();

        $image  = $change->after ?? $change->before ?? [];
        $fields = [];
        foreach ($image as $column => $value) {
            $fields[$column] = $this->field(RowCodec::encodeValue($value), $column, $changed, $stamp, $clocks);
        }
        // Never an empty object: {} decodes to [] and would no longer hash the same.
        $fields[$change->primaryKeyColumn] ??= $this->field(RowCodec::encodeValue($change->primaryKeyValue), $change->primaryKeyColumn, $changed, $stamp, $clocks);

        $payload = [
            'table'     => $change->tableName,
            'pk_column' => $change->primaryKeyColumn,
            'pk'        => $key,
            'hlc'       => $stamp->toString(),
            'node'      => $this->nodeId,
            'exists'    => $this->field($change->after !== null, self::EXISTS, $changed, $stamp, $clocks),
            'fields'    => $fields,
        ];

        FieldClocks::write($transaction, $change->tableName, $key, $changed, $stamp, $this->nodeId);

        // A hard delete keeps the row's last values: if a later write from
        // another node brings the row back, it is rebuilt from them.
        if ($change->after === null && $change->before !== null) {
            Tombstones::bury($transaction, $change->tableName, $key, $change->before);
        }

        $transaction->execute(
            'INSERT INTO replication_outbox (event_id, payload, created_at) VALUES (:event_id, :payload, :created_at)',
            [
                'event_id'   => UuidV7::generate(),
                'payload'    => json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
                'created_at' => gmdate('Y-m-d H:i:s'),
            ],
        );
    }

    /**
     * @param list<string> $changed
     * @return array{v: mixed, t: string, n: string}
     */
    private function field(mixed $value, string $column, array $changed, HlcTimestamp $stamp, FieldClocks $clocks): array
    {
        if (in_array($column, $changed, true)) {
            return ['v' => $value, 't' => $stamp->toString(), 'n' => $this->nodeId];
        }

        [$hlc, $node] = $clocks->of($column) ?? [self::ZERO_HLC, ''];

        return ['v' => $value, 't' => $hlc, 'n' => $node];
    }

    /** @return list<string> */
    private function changedColumns(RowChange $change): array
    {
        if ($change->after === null) {
            return [self::EXISTS]; // removed: only its existence changed
        }

        if ($change->before === null) {
            return [...array_keys($change->after), self::EXISTS]; // created
        }

        $changed = [];
        foreach ($change->after as $column => $value) {
            if (!array_key_exists($column, $change->before) || !RowCodec::same($change->before[$column], $value)) {
                $changed[] = $column;
            }
        }

        // A write says the row exists: a delete elsewhere that is older than
        // this edit must not win over it (ADR 0001: a later write resurrects).
        return $changed === [] ? [] : [...$changed, self::EXISTS];
    }
}
