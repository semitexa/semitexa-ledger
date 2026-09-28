<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
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
    /** @var array<string, list<string>|null> table => its columns, or null when it does not exist here */
    private array $columnsByTable = [];

    public function __construct(
        private readonly HybridLogicalClock $clock = new HybridLogicalClock(),
    ) {}

    /**
     * @param array<string, mixed> $payload a ReplicationCaptureService event payload
     */
    public function apply(array $payload, DatabaseAdapterInterface $db): void
    {
        $table    = (string) $payload['table'];
        $pkColumn = (string) $payload['pk_column'];
        $rowKey   = (string) $payload['pk'];
        /** @var array<string, array{v: mixed, t: string, n: string}> $fields */
        $fields   = $payload['fields'];
        $incoming = $fields + [ReplicationCaptureService::EXISTS => $payload['exists']];

        // A node whose clock runs far ahead would win every conflict for as
        // long as its lead lasted; its changes wait (the caller retries)
        // until this node's own time is within the allowed drift.
        $this->clock->observe(HlcTimestamp::fromString((string) $payload['hlc']));

        $columns = $this->columnsOf($db, $table);
        if ($columns === null) {
            // A resource this node's code does not have yet. Nothing is stored
            // for it, so once the node is upgraded a ledger replay applies it.
            StaticLoggerBridge::warning('ledger', 'Replicated change for a table this node lacks; kept in the ledger only', ['table' => $table]);
            return;
        }

        $clocks = FieldClocks::lockAndRead($db, $table, $rowKey);
        $pk     = RowCodec::decodeKey($rowKey);
        $row    = $this->lockRow($db, $table, $pkColumn, $pk);

        $won = [];
        foreach ($incoming as $column => $field) {
            if ($column !== ReplicationCaptureService::EXISTS && !in_array($column, $columns, true)) {
                continue; // not here yet; its clock is not stored, so a later replay still applies it
            }
            [$hlc, $node] = $clocks->of($column) ?? [ReplicationCaptureService::ZERO_HLC, ''];
            if (HlcTimestamp::fromString($field['t'])->wins($field['n'], HlcTimestamp::fromString($hlc), $node)) {
                $won[$column] = $field;
            }
        }

        if ($won === []) {
            return; // nothing newer than what is here — including a repeat of this very change
        }

        $exists = isset($won[ReplicationCaptureService::EXISTS])
            ? (bool) $won[ReplicationCaptureService::EXISTS]['v']
            : $row !== null;

        $values = [];
        foreach ($won as $column => $field) {
            if ($column !== ReplicationCaptureService::EXISTS) {
                $values[$column] = RowCodec::decodeValue($field['v']);
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
            foreach ($fields as $column => $field) {
                if (in_array($column, $columns, true)) {
                    $full[$column] = array_key_exists($column, $values)
                        ? $values[$column]
                        : ($base !== null && array_key_exists($column, $base) ? $base[$column] : RowCodec::decodeValue($field['v']));
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
            array_map(static fn (array $field): array => [(string) $field['t'], (string) $field['n']], $won),
        );
    }

    /** @return list<string>|null */
    private function columnsOf(DatabaseAdapterInterface $db, string $table): ?array
    {
        if (!array_key_exists($table, $this->columnsByTable)) {
            $rows = $db->execute(
                'SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t',
                ['t' => $table],
            )->rows;
            $this->columnsByTable[$table] = $rows === [] ? null : array_map(static fn (array $r): string => (string) $r['name'], $rows);
        }

        return $this->columnsByTable[$table];
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
