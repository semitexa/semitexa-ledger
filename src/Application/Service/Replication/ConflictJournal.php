<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Support\Row;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Domain\Model\ReplicationConflict;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;

/** Records replication conflicts (see ReplicationConflictResourceModel). */
final class ConflictJournal
{
    /**
     * @param array<mixed> $payload the change as received, kept for a retry
     * @return bool true the first time this conflict is recorded; false when a
     *         replay, or a later change still carrying the same stamps, finds it
     *         already journaled — the application has been told once
     */
    public static function record(DatabaseAdapterInterface $db, ReplicationConflict $conflict, array $payload): bool
    {
        $key = $conflict->key();

        // The replayer is one per node and holds the row lock, so read-then-insert
        // does not race; the upsert only keeps a surprise from aborting the apply.
        $known = $db->execute('SELECT 1 FROM replication_conflict WHERE conflict_key = :k', ['k' => $key])->rows !== [];
        if ($known) {
            return false;
        }

        $db->execute(
            'INSERT INTO replication_conflict (conflict_key, table_name, row_pk, columns, incoming, local, origin_node, reason, detected_at, payload)'
                . ' VALUES (:k, :t, :pk, :columns, :incoming, :local, :origin, :reason, :at, :payload)'
                . ' AS incoming_row ON DUPLICATE KEY UPDATE conflict_key = incoming_row.conflict_key',
            [
                'k'        => $key,
                't'        => $conflict->table,
                'pk'       => $conflict->rowKey,
                'columns'  => self::json($conflict->columns),
                'incoming' => self::json(self::image($conflict->incoming, $conflict->incomingClocks)),
                'local'    => $conflict->local === null ? null : self::json(self::image($conflict->local, $conflict->localClocks)),
                'origin'   => $conflict->originNode,
                'reason'   => mb_substr($conflict->reason, 0, 500),
                'at'       => gmdate('Y-m-d H:i:s'),
                'payload'  => self::json($payload),
            ],
        );

        return true;
    }

    /**
     * Close the row's open conflicts that no longer hold: a row kept out now
     * exists here, or its existence clock here is at least as new as the one
     * the kept-out change carried (a newer delete took its place); or every
     * field left unapplied now carries a clock at least as new as the one it
     * was journaled with — it landed, or a newer write took its place. Called
     * by the applier after each change of the row, on its transaction and
     * under its locks.
     */
    public static function settle(DatabaseAdapterInterface $db, string $table, string $rowKey, bool $rowExists): void
    {
        $open = $db->execute(
            'SELECT id, columns, incoming, payload FROM replication_conflict WHERE table_name = :t AND row_pk = :pk AND resolved_at IS NULL',
            ['t' => $table, 'pk' => $rowKey],
        )->rows;
        if ($open === []) {
            return;
        }

        $clocks = null;
        $settled = [];
        foreach ($open as $row) {
            $r = Row::of($row);
            /** @var list<string> $columns */
            $columns = json_decode($r->string('columns'), true, 8, JSON_THROW_ON_ERROR);
            if ($columns === []) {
                $clocks ??= FieldClocks::lockAndRead($db, $table, $rowKey);
                if ($rowExists || self::existenceOvertaken($r->string('payload'), $clocks)) {
                    $settled[] = $r->int('id');
                }
                continue;
            }

            /** @var array<string, array{v: mixed, t: string|null, n: string|null}> $incoming */
            $incoming = json_decode($r->string('incoming'), true, 64, JSON_THROW_ON_ERROR);
            $clocks ??= FieldClocks::lockAndRead($db, $table, $rowKey);
            if (self::allCaughtUp($columns, $incoming, $clocks)) {
                $settled[] = $r->int('id');
            }
        }

        foreach ($settled as $id) {
            $db->execute(
                'UPDATE replication_conflict SET resolved_at = :at WHERE id = :id',
                ['at' => gmdate('Y-m-d H:i:s'), 'id' => $id],
            );
        }
    }

    /**
     * @return array{payload: array<mixed>|null, resolved: bool}|null null when no conflict has this key
     */
    public static function find(DatabaseAdapterInterface $db, string $key): ?array
    {
        $row = $db->execute(
            'SELECT payload, resolved_at FROM replication_conflict WHERE conflict_key = :k',
            ['k' => $key],
        )->rows[0] ?? null;
        if ($row === null) {
            return null;
        }

        $payload = $row['payload'] ?? null;

        return [
            'payload'  => is_string($payload) ? (array) json_decode($payload, true, 512, JSON_THROW_ON_ERROR) : null,
            'resolved' => ($row['resolved_at'] ?? null) !== null,
        ];
    }

    /**
     * Whether this node's existence clock for the row is at least as new as
     * the one the kept-out change carried. The incoming image holds values
     * only; the existence stamp is in the change itself. A conflict journaled
     * before the change was kept ('' here) cannot tell, and stays open.
     */
    private static function existenceOvertaken(string $payload, FieldClocks $clocks): bool
    {
        $here = $clocks->of(ReplicationCaptureService::EXISTS);
        if ($payload === '' || $here === null) {
            return false;
        }

        $exists = json_decode($payload, true, 512, JSON_THROW_ON_ERROR)['exists'] ?? null;
        if (!is_array($exists) || !is_string($exists['t'] ?? null)) {
            return false;
        }

        return !HlcTimestamp::fromString($exists['t'])->wins((string) ($exists['n'] ?? ''), HlcTimestamp::fromString($here[0]), $here[1]);
    }

    /**
     * @param list<string> $columns
     * @param array<string, array{v: mixed, t: string|null, n: string|null}> $incoming
     */
    private static function allCaughtUp(array $columns, array $incoming, FieldClocks $clocks): bool
    {
        foreach ($columns as $column) {
            $here = $clocks->of($column);
            $hlc  = $incoming[$column]['t'] ?? null;
            if ($here === null || $hlc === null) {
                return false;
            }
            $journaled = HlcTimestamp::fromString($hlc);
            if ($journaled->wins((string) ($incoming[$column]['n'] ?? ''), HlcTimestamp::fromString($here[0]), $here[1])) {
                return false; // what is stored here is still older than the unapplied stamp
            }
        }

        return true;
    }

    /**
     * @param array<string, mixed> $values
     * @param array<string, array{0: string, 1: string}> $clocks
     * @return array<string, array{v: mixed, t: string|null, n: string|null}>
     */
    private static function image(array $values, array $clocks): array
    {
        $image = [];
        foreach ($values as $column => $value) {
            $image[$column] = [
                'v' => RowCodec::encodeValue($value),
                't' => $clocks[$column][0] ?? null,
                'n' => $clocks[$column][1] ?? null,
            ];
        }

        return $image;
    }

    private static function json(mixed $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }
}
