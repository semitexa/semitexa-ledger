<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ledger;

use Semitexa\Ledger\Dto\LedgerEvent;

/**
 * Thin SQLite3 wrapper with WAL mode, optimized pragmas, and typed query helpers.
 *
 * One instance per worker process. Swoole's SWOOLE_HOOK_ALL makes SQLite3
 * I/O non-blocking inside coroutines — no additional handling required.
 */
final class LedgerConnection
{
    private \SQLite3 $db;

    public function __construct(string $dbPath)
    {
        $this->db = new \SQLite3($dbPath);
        $this->db->enableExceptions(true);

        // WAL for concurrent reads alongside writes.
        $this->db->exec('PRAGMA journal_mode = WAL');
        // Balance durability vs. throughput (fsync on checkpoints, not every write).
        $this->db->exec('PRAGMA synchronous = NORMAL');
        // 5 s wait before returning SQLITE_BUSY.
        $this->db->exec('PRAGMA busy_timeout = 5000');
        // 64 MB in-process page cache.
        $this->db->exec('PRAGMA cache_size = -64000');
        // WAL checkpoint every 1000 pages.
        $this->db->exec('PRAGMA wal_autocheckpoint = 1000');
        // Enable foreign key enforcement.
        $this->db->exec('PRAGMA foreign_keys = ON');
    }

    public function getRaw(): \SQLite3
    {
        return $this->db;
    }

    /**
     * Execute a statement with named parameters. Returns the number of rows changed.
     *
     * @param array<string, mixed> $params
     */
    public function execute(string $sql, array $params = []): int
    {
        $stmt = $this->prepare($sql, $params);
        $stmt->execute();

        return $this->db->changes();
    }

    /**
     * Execute a statement and return all rows as associative arrays.
     *
     * @param array<string, mixed> $params
     * @return list<array<string, mixed>>
     */
    public function fetchAll(string $sql, array $params = []): array
    {
        $stmt = $this->prepare($sql, $params);
        $result = $stmt->execute();

        $rows = [];
        while ($row = $result->fetchArray(SQLITE3_ASSOC)) {
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * Execute a statement and return a single row, or null if not found.
     *
     * @param array<string, mixed> $params
     * @return array<string, mixed>|null
     */
    public function fetchOne(string $sql, array $params = []): ?array
    {
        $stmt = $this->prepare($sql, $params);
        $result = $stmt->execute();
        $row = $result->fetchArray(SQLITE3_ASSOC);

        return $row !== false ? $row : null;
    }

    /**
     * Return a single scalar value, or null.
     *
     * @param array<string, mixed> $params
     */
    public function fetchScalar(string $sql, array $params = []): mixed
    {
        $row = $this->fetchOne($sql, $params);

        return $row !== null ? array_values($row)[0] : null;
    }

    /**
     * Execute a callable inside an exclusive SQLite transaction.
     */
    public function transaction(callable $fn): void
    {
        $this->db->exec('BEGIN EXCLUSIVE');
        try {
            $fn($this);
            $this->db->exec('COMMIT');
        } catch (\Throwable $e) {
            $this->db->exec('ROLLBACK');
            throw $e;
        }
    }

    public function lastInsertRowId(): int
    {
        return $this->db->lastInsertRowID();
    }

    /**
     * @param array<string, mixed> $params
     */
    private function prepare(string $sql, array $params): \SQLite3Stmt
    {
        $stmt = $this->db->prepare($sql);
        if ($stmt === false) {
            throw new \RuntimeException("Failed to prepare SQL: {$sql}. Error: " . $this->db->lastErrorMsg());
        }

        foreach ($params as $key => $value) {
            $param = str_starts_with($key, ':') ? $key : ":{$key}";

            match (true) {
                is_int($value)   => $stmt->bindValue($param, $value, SQLITE3_INTEGER),
                is_float($value) => $stmt->bindValue($param, $value, SQLITE3_FLOAT),
                is_null($value)  => $stmt->bindValue($param, null, SQLITE3_NULL),
                default          => $stmt->bindValue($param, (string) $value, SQLITE3_TEXT),
            };
        }

        return $stmt;
    }
}
