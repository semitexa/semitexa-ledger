<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Support\Row;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Adapter\ServerCapability;

/**
 * The stored clocks of one replicated row — read under lock, written in the
 * caller's transaction. Shared by the capture (local writes) and the applier
 * (remote ones), so both see and leave the same thing.
 */
final class FieldClocks
{
    /**
     * @param array<string, array{0: string, 1: string}> $byColumn column => [hlc, node]
     */
    private function __construct(private readonly array $byColumn) {}

    public static function lockAndRead(DatabaseAdapterInterface $db, string $table, string $rowKey): self
    {
        $rows = $db->execute(
            'SELECT column_name, hlc, node FROM replication_field_clock WHERE table_name = :t AND row_pk = :pk'
                . ($db->supports(ServerCapability::LockingReads) ? ' FOR UPDATE' : ''),
            ['t' => $table, 'pk' => $rowKey],
        )->rows;

        $byColumn = [];
        foreach ($rows as $row) {
            $r = Row::of($row);
            $byColumn[$r->string('column_name')] = [$r->string('hlc'), $r->string('node')];
        }

        return new self($byColumn);
    }

    /** @return array{0: string, 1: string}|null [hlc, node] */
    public function of(string $column): ?array
    {
        return $this->byColumn[$column] ?? null;
    }

    public function latest(): ?HlcTimestamp
    {
        if ($this->byColumn === []) {
            return null;
        }

        return HlcTimestamp::fromString(max(array_column($this->byColumn, 0)));
    }

    /**
     * @param list<string> $columns
     */
    public static function write(
        DatabaseAdapterInterface $db,
        string $table,
        string $rowKey,
        array $columns,
        HlcTimestamp $stamp,
        string $node,
    ): void {
        self::writeEach($db, $table, $rowKey, array_fill_keys($columns, [$stamp->toString(), $node]));
    }

    /**
     * @param array<string, array{0: string, 1: string}> $clocks column => [hlc, node]
     */
    public static function writeEach(DatabaseAdapterInterface $db, string $table, string $rowKey, array $clocks): void
    {
        if ($clocks === []) {
            return;
        }

        // One set of placeholders per row: the transaction's single-connection
        // adapter binds natively, and a name repeated across rows is an
        // "invalid parameter number" there.
        $values = [];
        $params = [];
        $i = 0;
        foreach ($clocks as $column => [$hlc, $node]) {
            $values[] = "(:t{$i}, :pk{$i}, :c{$i}, :h{$i}, :n{$i})";
            $params["t{$i}"] = $table;
            $params["pk{$i}"] = $rowKey;
            $params["c{$i}"] = $column;
            $params["h{$i}"] = $hlc;
            $params["n{$i}"] = $node;
            $i++;
        }

        $db->execute(
            'INSERT INTO replication_field_clock (table_name, row_pk, column_name, hlc, node) VALUES '
                . implode(', ', $values)
                . ' AS incoming ON DUPLICATE KEY UPDATE hlc = incoming.hlc, node = incoming.node',
            $params,
        );
    }
}
