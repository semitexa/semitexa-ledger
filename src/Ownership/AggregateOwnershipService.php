<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ownership;

use Semitexa\Orm\Adapter\DatabaseAdapterInterface;

/**
 * Resolves and manages ownership of aggregates across nodes.
 *
 * Ownership is stored in `aggregate_ownership` in the MAIN database
 * (not the SQLite ledger), so it participates in domain transactions
 * and is queryable during request handling.
 *
 * Worker-scoped service (one instance per Swoole worker). The in-process
 * cache avoids repeated DB hits for hot aggregates.
 */
final class AggregateOwnershipService
{
    public function __construct(
        private readonly string $nodeId,
        private readonly DatabaseAdapterInterface $db,
        private readonly OwnershipCache $cache,
    ) {}

    /**
     * Return the owner node ID for an aggregate, or null if unknown.
     */
    public function resolveOwner(string $aggregateType, string $aggregateId): ?string
    {
        $cacheKey = "{$aggregateType}:{$aggregateId}";

        return $this->cache->remember($cacheKey, function () use ($aggregateType, $aggregateId): ?string {
            $result = $this->db->execute(
                'SELECT owner_node_id FROM aggregate_ownership
                 WHERE aggregate_type = :type AND aggregate_id = :id
                 LIMIT 1',
                ['type' => $aggregateType, 'id' => $aggregateId],
            );

            $row = $result->rows[0] ?? null;

            return $row !== null ? (string) $row['owner_node_id'] : null;
        });
    }

    /**
     * Return true if the current node owns this aggregate.
     */
    public function isLocallyOwned(string $aggregateType, string $aggregateId): bool
    {
        return $this->resolveOwner($aggregateType, $aggregateId) === $this->nodeId;
    }

    /**
     * Claim ownership of a newly created aggregate (first-writer-wins via INSERT IGNORE).
     * Called as part of the aggregate creation transaction.
     */
    public function claimOwnership(string $aggregateType, string $aggregateId): void
    {
        $this->db->execute(
            'INSERT IGNORE INTO aggregate_ownership
             (aggregate_type, aggregate_id, owner_node_id, acquired_at)
             VALUES (:type, :id, :node, NOW(6))',
            ['type' => $aggregateType, 'id' => $aggregateId, 'node' => $this->nodeId],
        );

        $this->cache->set("{$aggregateType}:{$aggregateId}", $this->nodeId);
    }

    /**
     * Record that a remote node owns an aggregate (called from LedgerReplayer).
     *
     * Uses INSERT ... ON DUPLICATE KEY UPDATE so that:
     * - Ownership is set on first sight of the aggregate.
     * - Ownership is updated only if a transfer is in progress (transfer_token IS NOT NULL).
     * - A completed transfer clears the token.
     */
    public function recordRemoteOwnership(
        string $aggregateType,
        string $aggregateId,
        string $ownerNodeId,
    ): void {
        $this->db->execute(
            'INSERT INTO aggregate_ownership
             (aggregate_type, aggregate_id, owner_node_id, acquired_at)
             VALUES (:type, :id, :owner, NOW(6))
             ON DUPLICATE KEY UPDATE
               owner_node_id     = IF(transfer_token IS NOT NULL, VALUES(owner_node_id), owner_node_id),
               acquired_at       = IF(transfer_token IS NOT NULL, NOW(6), acquired_at),
               transfer_token    = NULL,
               transfer_expires_at = NULL',
            ['type' => $aggregateType, 'id' => $aggregateId, 'owner' => $ownerNodeId],
        );

        $this->cache->forget("{$aggregateType}:{$aggregateId}");
    }

    /**
     * Initiate an ownership transfer to another node.
     *
     * Sets a transfer_token and expiry. The new owner completes the transfer
     * by calling recordRemoteOwnership() — which clears the token upon receipt
     * of the first event from the new owner.
     *
     * @return string The transfer token (must be shared with the target node out-of-band)
     */
    public function initiateTransfer(
        string $aggregateType,
        string $aggregateId,
        string $targetNodeId,
        int $ttlSeconds = 300,
    ): string {
        $token = bin2hex(random_bytes(16));
        $expires = gmdate('Y-m-d H:i:s', time() + $ttlSeconds);

        $this->db->execute(
            'UPDATE aggregate_ownership
             SET transfer_token = :token, transfer_expires_at = :expires
             WHERE aggregate_type = :type AND aggregate_id = :id AND owner_node_id = :node',
            [
                'token'   => $token,
                'expires' => $expires,
                'type'    => $aggregateType,
                'id'      => $aggregateId,
                'node'    => $this->nodeId,
            ],
        );

        $this->cache->forget("{$aggregateType}:{$aggregateId}");

        return $token;
    }
}
