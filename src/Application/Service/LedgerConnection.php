<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Ledger\Domain\Model\LedgerEvent;

/**
 * Thin SQLite3 wrapper with WAL mode, optimized pragmas, and typed query helpers.
 *
 * One instance per worker process. Swoole's SWOOLE_HOOK_ALL makes SQLite3 I/O
 * non-blocking inside coroutines — which is exactly why {@see transaction()}
 * needs an intra-worker mutex: a yielding statement inside one coroutine's
 * `BEGIN EXCLUSIVE` transaction would otherwise let another coroutine open a
 * SECOND transaction on the SAME shared connection ("cannot start a
 * transaction within a transaction", or interleaved SELECT/INSERT that shares
 * a sequence number / breaks the prev-hash chain). BEGIN EXCLUSIVE only
 * serialises writers across PROCESSES; the mutex serialises coroutines WITHIN
 * a worker.
 */
final class LedgerConnection
{
    private \SQLite3 $db;

    /**
     * Intra-worker transaction mutex: a capacity-1 coroutine Channel used as a
     * one-token lock. Created lazily inside a coroutine (Channel methods are
     * illegal outside one), so it is null on the CLI/single-threaded path where
     * no coroutine concurrency exists and no serialisation is needed.
     */
    private ?\Swoole\Coroutine\Channel $txMutex = null;

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
     * Returns the value returned by the callable.
     *
     * Serialised per worker: the callable's statements yield under
     * SWOOLE_HOOK_ALL, so without the mutex a concurrent coroutine could open a
     * second BEGIN EXCLUSIVE on the shared connection mid-transaction. The mutex
     * holds one coroutine's whole BEGIN..COMMIT before the next starts.
     */
    public function transaction(callable $fn): mixed
    {
        $mutex = $this->acquireTxMutex();
        try {
            $this->db->exec('BEGIN EXCLUSIVE');
            try {
                $result = $fn($this);
                $this->db->exec('COMMIT');
                return $result;
            } catch (\Throwable $e) {
                $this->db->exec('ROLLBACK');
                throw $e;
            }
        } finally {
            $mutex?->push(true);
        }
    }

    /**
     * Acquire the intra-worker transaction lock, returning the Channel to
     * release on (or null when not in a coroutine — the CLI path, where no
     * concurrent coroutine can interleave). Lazy-created inside the coroutine:
     * `new Channel(1)` + the seeding `push` do not yield, so two coroutines
     * cannot both initialise it, and the loser blocks on `pop()` until the
     * holder pushes back.
     */
    private function acquireTxMutex(): ?\Swoole\Coroutine\Channel
    {
        if (!self::inCoroutine()) {
            return null;
        }
        if ($this->txMutex === null) {
            $this->txMutex = new \Swoole\Coroutine\Channel(1);
            $this->txMutex->push(true);
        }
        $this->txMutex->pop();

        return $this->txMutex;
    }

    private static function inCoroutine(): bool
    {
        return class_exists(\Swoole\Coroutine::class, false) && \Swoole\Coroutine::getCid() > 0;
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
