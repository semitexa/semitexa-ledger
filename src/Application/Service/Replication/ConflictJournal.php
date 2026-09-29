<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Ledger\Domain\Model\ReplicationConflict;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;

/** Records replication conflicts (see ReplicationConflictResourceModel). */
final class ConflictJournal
{
    /**
     * @return bool true the first time this conflict is recorded; false when a
     *         replay, or a later change still carrying the same stamps, finds it
     *         already journaled — the application has been told once
     */
    public static function record(DatabaseAdapterInterface $db, ReplicationConflict $conflict): bool
    {
        $key = $conflict->key();

        // The replayer is one per node and holds the row lock, so read-then-insert
        // does not race; the upsert only keeps a surprise from aborting the apply.
        $known = $db->execute('SELECT 1 FROM replication_conflict WHERE conflict_key = :k', ['k' => $key])->rows !== [];
        if ($known) {
            return false;
        }

        $db->execute(
            'INSERT INTO replication_conflict (conflict_key, table_name, row_pk, columns, incoming, local, origin_node, reason, detected_at)'
                . ' VALUES (:k, :t, :pk, :columns, :incoming, :local, :origin, :reason, :at)'
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
            ],
        );

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
