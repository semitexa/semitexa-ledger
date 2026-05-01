<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

use Semitexa\Ledger\Domain\Model\ClusterConfig;

/**
 * Manages NatsClient connections to one or more JetStream clusters.
 *
 * Worker-scoped: connections are established once per worker and reused
 * across requests and background coroutines.
 *
 * Usage:
 *
 *   $registry = ClusterRegistry::fromEnv();
 *   $registry->connect();               // establish all connections
 *
 *   foreach ($registry->getOrderedByPriority() as $entry) {
 *       $entry->client->jetStreamPublish(...);
 *   }
 */
final class ClusterRegistry
{
    /**
     * @var list<array{config: ClusterConfig, client: NatsClient}>
     */
    private array $clusters = [];

    private bool $connected = false;

    /**
     * Build from environment variables.
     *
     * Reads NATS_PRIMARY_URL / NATS_PRIMARY_CLUSTER_ID for the primary cluster.
     * If NATS_SECONDARY_URL is set, a secondary cluster is added automatically.
     */
    public static function fromEnv(): self
    {
        $registry = new self();

        $registry->add(new ClusterConfig(
            id:              (string) (getenv('NATS_PRIMARY_CLUSTER_ID') ?: 'primary'),
            url:             (string) (getenv('NATS_PRIMARY_URL') ?: getenv('NATS_URL') ?: 'nats://localhost:4222'),
            credentialsPath: (getenv('NATS_PRIMARY_CREDENTIALS') ?: null) ?: null,
            priority:        0,
            tlsCaFile:       (getenv('NATS_PRIMARY_TLS_CA') ?: null) ?: null,
        ));

        $secondaryUrl = getenv('NATS_SECONDARY_URL');
        if ($secondaryUrl !== false && $secondaryUrl !== '') {
            $registry->add(new ClusterConfig(
                id:              (string) (getenv('NATS_SECONDARY_CLUSTER_ID') ?: 'secondary'),
                url:             $secondaryUrl,
                credentialsPath: (getenv('NATS_SECONDARY_CREDENTIALS') ?: null) ?: null,
                priority:        1,
                tlsCaFile:       (getenv('NATS_SECONDARY_TLS_CA') ?: null) ?: null,
            ));
        }

        return $registry;
    }

    public function add(ClusterConfig $config): void
    {
        $this->clusters[] = [
            'config' => $config,
            'client' => new NatsClient($config),
        ];
    }

    /**
     * Connect all clusters. Failures are logged but do not abort startup —
     * LedgerPublisher will retry when it runs.
     */
    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        foreach ($this->clusters as $entry) {
            try {
                $entry['client']->connect();
            } catch (\Throwable $e) {
                // Logged upstream; continue to next cluster.
                error_log(sprintf(
                    '[semitexa-ledger] Failed to connect to NATS cluster "%s" (%s): %s',
                    $entry['config']->id,
                    $entry['config']->url,
                    $e->getMessage(),
                ));
            }
        }

        $this->connected = true;
    }

    /**
     * Return cluster entries sorted by priority (ascending = preferred first).
     *
     * @return list<array{config: ClusterConfig, client: NatsClient}>
     */
    public function getOrderedByPriority(): array
    {
        $clusters = $this->clusters;
        usort($clusters, static fn (array $a, array $b): int => $a['config']->priority <=> $b['config']->priority);
        return $clusters;
    }

    /**
     * @return list<array{config: ClusterConfig, client: NatsClient}>
     */
    public function getAll(): array
    {
        return $this->clusters;
    }

    public function getById(string $clusterId): ?NatsClient
    {
        foreach ($this->clusters as $entry) {
            if ($entry['config']->id === $clusterId) {
                return $entry['client'];
            }
        }

        return null;
    }

    public function isEmpty(): bool
    {
        return $this->clusters === [];
    }
}
