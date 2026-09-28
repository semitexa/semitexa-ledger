<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Support\Row;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;

/** The last image of hard-deleted replicated rows (see ReplicationTombstoneResourceModel). */
final class Tombstones
{
    /**
     * @param array<string, mixed> $row column => raw value
     */
    public static function bury(DatabaseAdapterInterface $db, string $table, string $rowKey, array $row): void
    {
        $image = array_map(RowCodec::encodeValue(...), $row);

        $db->execute(
            'INSERT INTO replication_tombstone (table_name, row_pk, image) VALUES (:t, :pk, :image)'
                . ' AS incoming ON DUPLICATE KEY UPDATE image = incoming.image',
            ['t' => $table, 'pk' => $rowKey, 'image' => json_encode($image, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE)],
        );
    }

    /** @return array<string, mixed>|null column => raw value */
    public static function exhume(DatabaseAdapterInterface $db, string $table, string $rowKey): ?array
    {
        $row = $db->execute(
            'SELECT image FROM replication_tombstone WHERE table_name = :t AND row_pk = :pk',
            ['t' => $table, 'pk' => $rowKey],
        )->rows[0] ?? null;

        if ($row === null) {
            return null;
        }

        $image = json_decode(Row::of($row)->string('image'), true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($image)) {
            return null;
        }

        $values = [];
        foreach ($image as $column => $value) {
            $values[(string) $column] = RowCodec::decodeValue($value);
        }

        return $values;
    }

    public static function remove(DatabaseAdapterInterface $db, string $table, string $rowKey): void
    {
        $db->execute('DELETE FROM replication_tombstone WHERE table_name = :t AND row_pk = :pk', ['t' => $table, 'pk' => $rowKey]);
    }
}
