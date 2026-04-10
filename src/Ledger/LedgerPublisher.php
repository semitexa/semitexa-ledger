<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ledger;

use Semitexa\Ledger\Dto\LedgerEvent;
use Semitexa\Ledger\Nats\ClusterHealthTracker;
use Semitexa\Ledger\Nats\ClusterRegistry;

/**
 * Background Swoole coroutine that publishes pending local events to NATS.
 *
 * Decouples the write path (LedgerWriter) from NATS availability: events are
 * always written to the SQLite ledger first, then this loop picks them up and
 * publishes to whichever cluster is available.
 *
 * Retry strategy:
 *  - 0.5 s poll when there are pending events (aggressive — propagation matters).
 *  - 5.0 s poll when the ledger is empty.
 *  - No exponential backoff: the ledger is durable; retrying fast is correct.
 *
 * Cluster selection: primary-first. Falls back to secondary on failure.
 * No parallel publish — one successful delivery per event is enough.
 *
 * Lifecycle:
 *   Coroutine::create(fn() => $publisher->runRetryLoop())
 *   during server worker boot (registered via LedgerBootstrap).
 */
final class LedgerPublisher
{
    private const BATCH_SIZE = 100;
    private const IDLE_SLEEP = 5.0;
    private const BUSY_SLEEP = 0.5;

    public function __construct(
        private readonly LedgerConnection $db,
        private readonly string $nodeId,
        private readonly ClusterRegistry $clusters,
        private readonly ClusterHealthTracker $health,
    ) {}

    /**
     * Run indefinitely. Call from a dedicated Swoole coroutine.
     */
    public function runRetryLoop(): void
    {
        while (true) {
            try {
                $published = $this->publishBatch();
            } catch (\Throwable $e) {
                error_log('[semitexa-ledger] LedgerPublisher error: ' . $e->getMessage());
                $published = 0;
            }

            \Swoole\Coroutine::sleep($published > 0 ? self::BUSY_SLEEP : self::IDLE_SLEEP);
        }
    }

    /**
     * Publish one batch of pending events. Returns the number published.
     */
    public function publishBatch(): int
    {
        $rows = $this->db->fetchAll(
            "SELECT * FROM events
             WHERE source = 'local' AND publish_status = 'pending'
             ORDER BY sequence ASC
             LIMIT " . self::BATCH_SIZE
        );

        foreach ($rows as $row) {
            $this->publishToAnyCluster(LedgerEvent::fromRow($row));
        }

        return count($rows);
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private function publishToAnyCluster(LedgerEvent $event): void
    {
        $subject = sprintf(
            'semitexa.events.%s.%s.%s',
            $event->originNode,
            $event->domain,
            $event->eventType,
        );

        $payload = $event->toJson();
        $headers = ['Nats-Msg-Id' => $event->eventId];

        foreach ($this->clusters->getOrderedByPriority() as $entry) {
            $clusterId = $entry['config']->id;
            $client    = $entry['client'];

            if (!$this->health->isHealthy($clusterId)) {
                continue;
            }

            try {
                $client->jetStreamPublish($subject, $payload, $headers);

                $this->markPublished($event->eventId, $clusterId);
                $this->health->recordSuccess($clusterId);
                return; // Done — single successful delivery is sufficient.

            } catch (\Throwable $e) {
                $this->health->recordFailure($clusterId);
                error_log(sprintf(
                    '[semitexa-ledger] Publish failed for event %s on cluster %s: %s',
                    $event->eventId,
                    $clusterId,
                    $e->getMessage(),
                ));
                // Fall through to next cluster.
            }
        }

        // All clusters failed — event remains 'pending' and will be retried.
    }

    private function markPublished(string $eventId, string $clusterId): void
    {
        $now = gmdate('Y-m-d\TH:i:s\Z');

        $this->db->execute(
            "UPDATE events SET publish_status = 'published' WHERE event_id = :id",
            ['id' => $eventId]
        );

        $this->db->execute(
            'INSERT OR IGNORE INTO publish_log (event_id, cluster_id, published_at)
             VALUES (:event_id, :cluster_id, :now)',
            ['event_id' => $eventId, 'cluster_id' => $clusterId, 'now' => $now]
        );
    }
}
