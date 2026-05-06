<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

/**
 * Tracks publish success/failure per cluster.
 *
 * After 3 consecutive failures a cluster is considered unhealthy.
 * It is re-probed every 30 seconds to allow automatic recovery.
 */
final class ClusterHealthTracker
{
    private const UNHEALTHY_THRESHOLD = 3;
    private const RECHECK_INTERVAL_SECONDS = 30.0;

    /** @var array<string, array{failures: int, last_success: float}> */
    private array $state = [];

    public function recordSuccess(string $clusterId): void
    {
        $this->state[$clusterId] = [
            'failures'     => 0,
            'last_success' => microtime(true),
        ];
    }

    public function recordFailure(string $clusterId): void
    {
        if (!isset($this->state[$clusterId])) {
            $this->state[$clusterId] = ['failures' => 0, 'last_success' => 0.0];
        }

        $this->state[$clusterId]['failures']++;
    }

    /**
     * Returns true if the cluster should be attempted.
     *
     * Unknown clusters are assumed healthy (first use). After 3 consecutive
     * failures the cluster is skipped for 30 seconds, then probed again.
     */
    public function isHealthy(string $clusterId): bool
    {
        $s = $this->state[$clusterId] ?? null;

        if ($s === null) {
            return true; // First attempt — assume healthy.
        }

        if ($s['failures'] < self::UNHEALTHY_THRESHOLD) {
            return true;
        }

        // Re-probe after the recheck interval.
        $elapsed = microtime(true) - $s['last_success'];
        return $elapsed >= self::RECHECK_INTERVAL_SECONDS;
    }

    public function getFailureCount(string $clusterId): int
    {
        return $this->state[$clusterId]['failures'] ?? 0;
    }
}
