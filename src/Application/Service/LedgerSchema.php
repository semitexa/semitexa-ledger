<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

/**
 * Creates and migrates the SQLite ledger schema.
 *
 * Versioned via the `ledger_meta` key `schema_version`. Each version adds
 * idempotent migrations — safe to run on every startup.
 */
final class LedgerSchema
{
    private const CURRENT_VERSION = 1;

    public function __construct(
        private readonly LedgerConnection $db,
        private readonly string $nodeId,
    ) {}

    /**
     * Ensure the schema is up to date. Called once per worker on boot.
     */
    public function migrate(): void
    {
        $this->createMetaTable();
        $currentVersion = (int) ($this->db->fetchScalar(
            "SELECT value FROM ledger_meta WHERE key = 'schema_version'"
        ) ?? 0);

        for ($v = $currentVersion + 1; $v <= self::CURRENT_VERSION; $v++) {
            $this->applyVersion($v);
        }
    }

    private function createMetaTable(): void
    {
        $this->db->getRaw()->exec(<<<'SQL'
            CREATE TABLE IF NOT EXISTS ledger_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        SQL);
    }

    private function applyVersion(int $version): void
    {
        $this->db->transaction(function (LedgerConnection $db) use ($version): void {
            match ($version) {
                1 => $this->applyV1($db),
                default => throw new \LogicException("Unknown schema version: {$version}"),
            };

            $db->execute(
                "INSERT INTO ledger_meta (key, value) VALUES ('schema_version', :v)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                ['v' => (string) $version]
            );
        });
    }

    private function applyV1(LedgerConnection $db): void
    {
        $raw = $db->getRaw();

        // Primary event store — append-only.
        $raw->exec(<<<'SQL'
            CREATE TABLE IF NOT EXISTS events (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id       TEXT    NOT NULL UNIQUE,
                origin_node    TEXT    NOT NULL,
                sequence       INTEGER NOT NULL,
                domain         TEXT    NOT NULL,
                event_type     TEXT    NOT NULL,
                event_version  INTEGER NOT NULL DEFAULT 1,
                aggregate_type TEXT,
                aggregate_id   TEXT,
                payload        TEXT    NOT NULL,
                metadata       TEXT,
                hash           TEXT    NOT NULL,
                prev_hash      TEXT    NOT NULL,
                hmac           TEXT    NOT NULL,
                source         TEXT    NOT NULL DEFAULT 'local',
                publish_status TEXT    NOT NULL DEFAULT 'pending',
                created_at     TEXT    NOT NULL,
                applied_at     TEXT,

                UNIQUE(origin_node, sequence)
            )
        SQL);

        $raw->exec('CREATE INDEX IF NOT EXISTS idx_events_origin_seq    ON events(origin_node, sequence)');
        $raw->exec('CREATE INDEX IF NOT EXISTS idx_events_domain_type   ON events(domain, event_type)');
        $raw->exec('CREATE INDEX IF NOT EXISTS idx_events_created       ON events(created_at)');
        $raw->exec('CREATE INDEX IF NOT EXISTS idx_events_source        ON events(source)');
        $raw->exec('CREATE INDEX IF NOT EXISTS idx_events_aggregate     ON events(aggregate_type, aggregate_id)');
        $raw->exec('CREATE INDEX IF NOT EXISTS idx_events_agg_seq       ON events(aggregate_type, aggregate_id, sequence)');
        $raw->exec(
            'CREATE INDEX IF NOT EXISTS idx_events_unpublished ON events(publish_status) ' .
            "WHERE publish_status = 'pending' AND source = 'local'"
        );
        $raw->exec(
            'CREATE INDEX IF NOT EXISTS idx_events_unapplied ON events(applied_at) ' .
            'WHERE applied_at IS NULL'
        );

        // Per-cluster consumer position tracking.
        $raw->exec(<<<'SQL'
            CREATE TABLE IF NOT EXISTS consumer_state (
                consumer_id         TEXT    NOT NULL,
                cluster_id          TEXT    NOT NULL,
                last_nats_sequence  INTEGER NOT NULL DEFAULT 0,
                last_event_id       TEXT,
                updated_at          TEXT    NOT NULL,

                PRIMARY KEY (consumer_id, cluster_id)
            )
        SQL);

        // Per-origin hash-chain state: last known sequence + last hash.
        $raw->exec(<<<'SQL'
            CREATE TABLE IF NOT EXISTS origin_state (
                origin_node    TEXT PRIMARY KEY,
                last_sequence  INTEGER NOT NULL DEFAULT 0,
                last_hash      TEXT    NOT NULL DEFAULT '',
                updated_at     TEXT    NOT NULL
            )
        SQL);

        // Per-cluster publish log — tracks which clusters received each local event.
        $raw->exec(<<<'SQL'
            CREATE TABLE IF NOT EXISTS publish_log (
                event_id     TEXT    NOT NULL,
                cluster_id   TEXT    NOT NULL,
                nats_sequence INTEGER,
                published_at TEXT    NOT NULL,

                PRIMARY KEY (event_id, cluster_id)
            )
        SQL);

        // Events that failed hash or HMAC verification — held for operator review.
        $raw->exec(<<<'SQL'
            CREATE TABLE IF NOT EXISTS quarantined_events (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id         TEXT    NOT NULL,
                origin_node      TEXT    NOT NULL,
                expected_hash    TEXT    NOT NULL,
                received_hash    TEXT    NOT NULL,
                received_payload TEXT    NOT NULL,
                source_cluster   TEXT    NOT NULL,
                quarantined_at   TEXT    NOT NULL,
                resolved_at      TEXT,
                resolution       TEXT
            )
        SQL);
        $raw->exec(
            'CREATE INDEX IF NOT EXISTS idx_quarantine_unresolved ON quarantined_events(resolved_at) ' .
            'WHERE resolved_at IS NULL'
        );

        // Seed static metadata if not already present.
        $db->execute(
            "INSERT OR IGNORE INTO ledger_meta (key, value) VALUES ('node_id', :node)",
            ['node' => $this->nodeId]
        );
        $db->execute(
            "INSERT OR IGNORE INTO ledger_meta (key, value) VALUES ('hmac_key_id', 'key-initial')"
        );
    }
}
