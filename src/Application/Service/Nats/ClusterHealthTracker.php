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

    /** @var array<string, array{failures: int, last_success: float, opened_at: ?float}> */
    private array $state = [];

    public function recordSuccess(string $clusterId): void
    {
        $this->state[$clusterId] = [
            'failures'     => 0,
            'last_success' => microtime(true),
            'opened_at'    => null,
        ];
    }

    public function recordFailure(string $clusterId): void
    {
        if (!isset($this->state[$clusterId])) {
            $this->state[$clusterId] = ['failures' => 0, 'last_success' => 0.0, 'opened_at' => null];
        }

        $this->state[$clusterId]['failures']++;

        // The cooldown must be measured from the moment the breaker TRIPPED
        // (the failure that reached the threshold), not from 'last_success'.
        // A cluster that has never once succeeded defaults last_success to
        // 0.0, so anchoring the cooldown there made isHealthy() compute an
        // elapsed time of "now minus the Unix epoch" — always >= 30s — and the
        // breaker never actually opened for a cluster with no prior success.
        if ($this->state[$clusterId]['failures'] === self::UNHEALTHY_THRESHOLD) {
            $this->state[$clusterId]['opened_at'] = microtime(true);
        }
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

        // Re-probe after the recheck interval, counted from when the breaker
        // opened (see recordFailure()), not from the (possibly never-set)
        // last success.
        $elapsed = microtime(true) - ($s['opened_at'] ?? microtime(true));
        return $elapsed >= self::RECHECK_INTERVAL_SECONDS;
    }

    public function getFailureCount(string $clusterId): int
    {
        return $this->state[$clusterId]['failures'] ?? 0;
    }
}
