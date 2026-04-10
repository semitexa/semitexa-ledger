<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Nats;

/**
 * Immutable configuration for one JetStream cluster (primary or secondary).
 */
final readonly class ClusterConfig
{
    public function __construct(
        /** Unique identifier used in logs and publish_log table (e.g. "cluster-a"). */
        public string $id,

        /** NATS server URL, e.g. "nats://hub-a-1.internal:4222". */
        public string $url,

        /** Optional credentials file path (.creds format). */
        public ?string $credentialsPath = null,

        /**
         * Priority: lower = preferred. Primary should be 0, secondary 1.
         * ClusterRegistry::getOrderedByPriority() sorts ascending.
         */
        public int $priority = 0,
    ) {}

    public static function fromEnv(string $prefix = 'NATS'): self
    {
        $id  = self::env("{$prefix}_CLUSTER_ID", 'primary');
        $url = self::env("{$prefix}_URL", 'nats://localhost:4222');

        return new self(
            id:              $id,
            url:             $url,
            credentialsPath: self::env("{$prefix}_CREDENTIALS") ?: null,
            priority:        (int) self::env("{$prefix}_PRIORITY", '0'),
        );
    }

    private static function env(string $key, string $default = ''): string
    {
        $val = getenv($key);
        return $val !== false ? $val : $default;
    }
}
