<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Support\Row;
use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Domain\Model\FieldStamp;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Domain\Model\RowChangePayload;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Adapter\ServerCapability;
use Semitexa\Orm\Adapter\SqlIdentifier;

/**
 * Merges a replicated row change from another node into this one (ADR 0001).
 *
 * Every field — and the row's existence, `__exists` — is decided on its own:
 * the incoming value is taken only if its (clock, node) beats the clock stored
 * here. The merge is idempotent, commutative and associative, so nodes that
 * received the same changes hold the same rows in whatever order they came.
 *
 * Writes go straight to the table on the caller's transaction, never through
 * the ORM engine: a merged change must not be captured again and echoed back.
 */
final class RowChangeApplier
{
    /** @var array<string, list<string>> table => its columns; only tables that exist */
    private array $columnsByTable = [];

    /**
     * @param array<string, string> $replicatedTables table => primary-key column, for the
     *        resources this node's code marks #[Replicated] (see ReplicatedTables). The table
     *        and key in an event come off the wire; nothing outside this map is ever written.
     */
    public function __construct(
        private readonly array $replicatedTables,
        private readonly HybridLogicalClock $clock = new HybridLogicalClock(),
    ) {}

    /**
     * @param array<mixed> $payload a ReplicationCaptureService event payload, as received
     */
    public function apply(array $payload, DatabaseAdapterInterface $db): void
    {
        try {
            $change = RowChangePayload::fromArray($payload);
            // Decode the key and every value before anything is read or
            // written: a bad binary value found halfway would abort the write
            // and be retried forever, holding back the origin's later events.
            $pk      = RowCodec::decodeKey($change->rowKey);
            $decoded = [];
            foreach ($change->fields as $column => $field) {
                $decoded[$column] = RowCodec::decodeValue($field->value);
            }
        } catch (\UnexpectedValueException|\InvalidArgumentException $e) {
            // Signed by a peer but not a row change: retrying cannot fix it, and
            // half of it is not applied. Refused here, and the event stays in
            // the ledger for inspection.
            StaticLoggerBridge::error('ledger', 'Replicated change refused: malformed payload', ['error' => $e->getMessage()]);
            return;
        }

        $table    = $change->table;
        $pkColumn = $change->pkColumn;
        $rowKey   = $change->rowKey;
        /** @var array<string, FieldStamp> $incoming */
        $incoming = $change->fields + [ReplicationCaptureService::EXISTS => $change->exists];

        // A node whose clock runs far ahead would win every conflict for as
        // long as its lead lasted; its changes wait (the caller retries)
        // until this node's own time is within the allowed drift. Every clock
        // in the change counts — one future field stamp would otherwise win
        // that field against every ordinary write after it.
        $this->clock->observe($change->latestClock());

        // The table and key come off the wire. Only a table this node's own
        // code marks #[Replicated], keyed by its declared primary key, is
        // written — a change naming any other table (the clock table, a
        // permissions table) is refused, whoever signed it.
        if (($this->replicatedTables[$table] ?? null) !== $pkColumn) {
            StaticLoggerBridge::error('ledger', 'Replicated change refused: not a #[Replicated] table here', [
                'table'     => $table,
                'pk_column' => $pkColumn,
                'origin'    => $change->node,
            ]);
            return;
        }

        $columns = $this->columnsOf($db, $table);
        if ($columns === null) {
            // Declared but not created yet (the migration has not run). Nothing
            // is stored for it and nothing is cached, so `ledger:replay` after
            // the migration applies it.
            StaticLoggerBridge::warning('ledger', 'Replicated change for a table not created yet; kept in the ledger only', ['table' => $table]);
            return;
        }

        $clocks = FieldClocks::lockAndRead($db, $table, $rowKey);
        $row    = $this->lockRow($db, $table, $pkColumn, $pk);

        $won = [];
        foreach ($incoming as $column => $field) {
            if ($column !== ReplicationCaptureService::EXISTS && !in_array($column, $columns, true)) {
                continue; // not here yet; its clock is not stored, so a later replay still applies it
            }
            [$hlc, $node] = $clocks->of($column) ?? [ReplicationCaptureService::ZERO_HLC, ''];
            if ($field->beats(HlcTimestamp::fromString($hlc), $node)) {
                $won[$column] = $field;
            }
        }

        if ($won === []) {
            return; // nothing newer than what is here — including a repeat of this very change
        }

        $exists = isset($won[ReplicationCaptureService::EXISTS])
            ? (bool) $won[ReplicationCaptureService::EXISTS]->value
            : $row !== null;

        $values = [];
        foreach ($won as $column => $field) {
            if ($column !== ReplicationCaptureService::EXISTS) {
                $values[$column] = $decoded[$column];
            }
        }

        if (!$exists) {
            if ($row !== null) {
                Tombstones::bury($db, $table, $rowKey, array_replace($row, $values));
                $this->delete($db, $table, $pkColumn, $pk);
            } elseif ($values !== []) {
                // Already gone here: keep the newer values for a later return.
                $base = Tombstones::exhume($db, $table, $rowKey) ?? [];
                Tombstones::bury($db, $table, $rowKey, array_replace($base, $values));
            }
        } elseif ($row !== null) {
            $this->update($db, $table, $pkColumn, $pk, $values);
        } else {
            // Created, or back from a delete. Fields this change did not win
            // come from what this node last had (the tombstone), else from the
            // change itself — whose clocks for them are then the right ones.
            $base = Tombstones::exhume($db, $table, $rowKey);
            $full = [];
            foreach ($change->fields as $column => $field) {
                if (in_array($column, $columns, true)) {
                    $full[$column] = array_key_exists($column, $values)
                        ? $values[$column]
                        : ($base !== null && array_key_exists($column, $base) ? $base[$column] : $decoded[$column]);
                    if ($base === null && !isset($won[$column]) && $clocks->of($column) === null) {
                        $won[$column] = $field;
                    }
                }
            }
            $full[$pkColumn] = $pk;
            $this->insert($db, $table, $full);
            Tombstones::remove($db, $table, $rowKey);
        }

        FieldClocks::writeEach(
            $db,
            $table,
            $rowKey,
            array_map(static fn (FieldStamp $field): array => [$field->clock->toString(), $field->node], $won),
        );
    }

    /** @return list<string>|null */
    private function columnsOf(DatabaseAdapterInterface $db, string $table): ?array
    {
        if (isset($this->columnsByTable[$table])) {
            return $this->columnsByTable[$table];
        }

        $rows = $db->execute(
            'SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t',
            ['t' => $table],
        )->rows;
        if ($rows === []) {
            return null; // not cached: the migration may create it later
        }

        return $this->columnsByTable[$table] = array_values(array_map(static fn (array $r): string => Row::of($r)->string('name'), $rows));
    }

    /** @return array<string, mixed>|null */
    private function lockRow(DatabaseAdapterInterface $db, string $table, string $pkColumn, string $pk): ?array
    {
        return $db->execute(
            sprintf(
                'SELECT * FROM %s WHERE %s = :pk LIMIT 1%s',
                SqlIdentifier::quote($table),
                SqlIdentifier::quote($pkColumn),
                $db->supports(ServerCapability::LockingReads) ? ' FOR UPDATE' : '',
            ),
            ['pk' => $pk],
        )->rows[0] ?? null;
    }

    /** @param array<string, mixed> $values */
    private function update(DatabaseAdapterInterface $db, string $table, string $pkColumn, string $pk, array $values): void
    {
        unset($values[$pkColumn]);
        if ($values === []) {
            return;
        }

        $params = ['__pk' => $pk];
        $sets   = [];
        $i      = 0;
        foreach ($values as $column => $value) {
            $sets[] = sprintf('%s = :v%d', SqlIdentifier::quote($column), $i);
            $params["v{$i}"] = $value;
            $i++;
        }

        $db->execute(
            sprintf('UPDATE %s SET %s WHERE %s = :__pk', SqlIdentifier::quote($table), implode(', ', $sets), SqlIdentifier::quote($pkColumn)),
            $params,
        );
    }

    /** @param array<string, mixed> $values */
    private function insert(DatabaseAdapterInterface $db, string $table, array $values): void
    {
        $columns = [];
        $holders = [];
        $params  = [];
        $i       = 0;
        foreach ($values as $column => $value) {
            $columns[] = SqlIdentifier::quote($column);
            $holders[] = ":v{$i}";
            $params["v{$i}"] = $value;
            $i++;
        }

        $db->execute(
            sprintf('INSERT INTO %s (%s) VALUES (%s)', SqlIdentifier::quote($table), implode(', ', $columns), implode(', ', $holders)),
            $params,
        );
    }

    private function delete(DatabaseAdapterInterface $db, string $table, string $pkColumn, string $pk): void
    {
        $db->execute(
            sprintf('DELETE FROM %s WHERE %s = :pk', SqlIdentifier::quote($table), SqlIdentifier::quote($pkColumn)),
            ['pk' => $pk],
        );
    }
}
