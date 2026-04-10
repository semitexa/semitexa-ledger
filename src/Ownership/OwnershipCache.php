<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ownership;

/**
 * In-process LRU cache for aggregate ownership lookups.
 *
 * Worker-scoped (one instance per Swoole worker). TTL-based expiry
 * ensures stale entries are re-validated from the DB after ~60 seconds.
 * Capacity-bounded (default 10,000 entries) to prevent unbounded growth.
 */
final class OwnershipCache
{
    /** @var array<string, array{owner: string|null, expires: float}> */
    private array $entries = [];

    /** @var list<string> Insertion-order key list for LRU eviction. */
    private array $order = [];

    public function __construct(
        private readonly int $ttlSeconds = 60,
        private readonly int $capacity = 10_000,
    ) {}

    /**
     * Return the cached owner node ID, or call $loader and cache its result.
     */
    public function remember(string $cacheKey, callable $loader): ?string
    {
        $entry = $this->entries[$cacheKey] ?? null;

        if ($entry !== null && $entry['expires'] > microtime(true)) {
            return $entry['owner'];
        }

        $value = $loader();
        $this->set($cacheKey, $value);

        return $value;
    }

    /**
     * Store or overwrite an owner for a given key.
     */
    public function set(string $cacheKey, ?string $ownerNodeId): void
    {
        if (isset($this->entries[$cacheKey])) {
            // Re-insert to refresh LRU order.
            $this->order = array_values(array_filter(
                $this->order,
                static fn (string $k): bool => $k !== $cacheKey,
            ));
        } elseif (count($this->entries) >= $this->capacity) {
            // Evict the oldest entry (LRU).
            $oldest = array_shift($this->order);
            if ($oldest !== null) {
                unset($this->entries[$oldest]);
            }
        }

        $this->entries[$cacheKey] = [
            'owner'   => $ownerNodeId,
            'expires' => microtime(true) + $this->ttlSeconds,
        ];
        $this->order[] = $cacheKey;
    }

    /**
     * Invalidate a specific entry (e.g. after ownership transfer).
     */
    public function forget(string $cacheKey): void
    {
        unset($this->entries[$cacheKey]);
        $this->order = array_values(array_filter(
            $this->order,
            static fn (string $k): bool => $k !== $cacheKey,
        ));
    }
}
