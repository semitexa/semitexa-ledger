<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ledger;

use Semitexa\Core\Container\ContainerFactory;
use Semitexa\Ledger\Dto\LedgerEvent;
use Semitexa\Ledger\Nats\ClusterRegistry;
use Semitexa\Ledger\Ownership\AggregateOwnershipService;

/**
 * Background Swoole coroutine that consumes events from NATS and applies them
 * to the local ledger + main database.
 *
 * Runs one consumer loop per cluster (each in its own coroutine).
 * Both loops feed into the same deduplication + apply pipeline.
 *
 * Deduplication layers:
 *  1. origin_node == self    → skip (own events already in ledger).
 *  2. SQLite INSERT OR IGNORE on event_id → duplicate delivery from either cluster.
 *  3. Sequence gap detection → NAK and wait (origin's publisher will fill the gap).
 *
 * Hash chain and HMAC verification are performed before DB write.
 * Mismatches go to the quarantine table and are never applied.
 */
final class LedgerReplayer
{
    private const STREAM_NAME = 'EVENTS';
    private const PULL_BATCH  = 50;
    private const PULL_SLEEP  = 1.0;  // seconds between empty polls

    public function __construct(
        private readonly LedgerConnection $db,
        private readonly string $nodeId,
        private readonly string $hmacKey,
        private readonly ClusterRegistry $clusters,
        private readonly ReplayHandlerRegistry $handlerRegistry,
        private readonly AggregateOwnershipService $ownership,
    ) {}

    /**
     * Start one consumer coroutine per cluster.
     * Call from the server's worker-start lifecycle hook.
     */
    public function start(): void
    {
        foreach ($this->clusters->getAll() as $entry) {
            $clusterId = $entry['config']->id;
            $client    = $entry['client'];

            \Swoole\Coroutine::create(function () use ($clusterId, $client): void {
                $consumerName = "node-{$this->nodeId}";
                $lastSeq      = $this->getLastNatsSequence($consumerName, $clusterId);

                $client->ensurePullConsumer(
                    streamName:     self::STREAM_NAME,
                    consumerName:   $consumerName,
                    filterSubject:  'semitexa.events.>',
                    startSequence:  $lastSeq,
                );

                $this->runConsumeLoop($client, $consumerName, $clusterId);
            });
        }
    }

    /**
     * Single-cluster consume loop. Runs indefinitely.
     */
    private function runConsumeLoop(
        \Semitexa\Ledger\Nats\NatsClient $client,
        string $consumerName,
        string $clusterId,
    ): void {
        while (true) {
            try {
                $messages = $client->pullMessages(
                    streamName:   self::STREAM_NAME,
                    consumerName: $consumerName,
                    batchSize:    self::PULL_BATCH,
                );

                foreach ($messages as $msg) {
                    $this->processMessage((string) $msg->body, $clusterId, $consumerName, $msg);
                }

                if (empty($messages)) {
                    \Swoole\Coroutine::sleep(self::PULL_SLEEP);
                }
            } catch (\Throwable $e) {
                error_log("[semitexa-ledger] LedgerReplayer error (cluster={$clusterId}): " . $e->getMessage());
                \Swoole\Coroutine::sleep(self::PULL_SLEEP);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Per-message pipeline
    // -------------------------------------------------------------------------

    private function processMessage(
        string $rawPayload,
        string $sourceCluster,
        string $consumerName,
        object $msg,
    ): void {
        try {
            $event = LedgerEvent::fromJson($rawPayload);
        } catch (\Throwable) {
            // Malformed envelope — cannot process; ACK to prevent redelivery loop.
            $msg->ack();
            error_log("[semitexa-ledger] Malformed event payload from cluster={$sourceCluster}");
            return;
        }

        // Skip own events (already in ledger from LedgerWriter).
        if ($event->originNode === $this->nodeId) {
            $msg->ack();
            return;
        }

        // Duplicate check by event_id.
        $existing = $this->db->fetchOne(
            'SELECT hash FROM events WHERE event_id = :id',
            ['id' => $event->eventId]
        );

        if ($existing !== null) {
            if ($existing['hash'] !== $event->hash) {
                $this->quarantine($event, (string) $existing['hash'], $sourceCluster);
            }
            $msg->ack();
            return;
        }

        // Sequence continuity check.
        $originState = $this->db->fetchOne(
            'SELECT last_sequence, last_hash FROM origin_state WHERE origin_node = :node',
            ['node' => $event->originNode]
        );

        $expectedSeq  = $originState !== null ? (int) $originState['last_sequence'] + 1 : 1;
        $expectedHash = $originState !== null
            ? (string) $originState['last_hash']
            : hash('sha256', "genesis:{$event->originNode}");

        if ($event->sequence < $expectedSeq) {
            // Old event — duplicate delivery path we missed above; safe to ACK.
            $msg->ack();
            return;
        }

        if ($event->sequence > $expectedSeq) {
            // Gap detected — NAK so JetStream redelivers later.
            $msg->nak();
            error_log(sprintf(
                '[semitexa-ledger] Sequence gap for origin=%s: expected=%d got=%d',
                $event->originNode, $expectedSeq, $event->sequence,
            ));
            return;
        }

        // Hash chain verification.
        $expectedChainHash = hash('sha256', $expectedHash . $event->eventId . json_encode($event->payload, JSON_THROW_ON_ERROR));
        if ($event->hash !== $expectedChainHash) {
            $this->quarantine($event, $expectedChainHash, $sourceCluster);
            $msg->ack();
            return;
        }

        // HMAC verification.
        $expectedHmac = hash_hmac('sha256', $event->hash, $this->hmacKey);
        if (!hash_equals($expectedHmac, $event->hmac)) {
            $this->quarantine($event, $expectedChainHash, $sourceCluster);
            $msg->ack();
            return;
        }

        // Persist to ledger + apply to main DB in one atomic step.
        $now = gmdate('Y-m-d\TH:i:s\Z');

        $this->db->transaction(function (LedgerConnection $db) use ($event, $now, $consumerName, $sourceCluster): void {
            // INSERT OR IGNORE as a safety net against concurrent coroutines.
            $affected = $db->execute(
                'INSERT OR IGNORE INTO events
                 (event_id, origin_node, sequence, domain, event_type, event_version,
                  aggregate_type, aggregate_id, payload, metadata, hash, prev_hash, hmac,
                  source, publish_status, created_at)
                 VALUES
                 (:event_id, :origin_node, :sequence, :domain, :event_type, :event_version,
                  :aggregate_type, :aggregate_id, :payload, :metadata, :hash, :prev_hash, :hmac,
                  :source, :publish_status, :created_at)',
                [
                    'event_id'       => $event->eventId,
                    'origin_node'    => $event->originNode,
                    'sequence'       => $event->sequence,
                    'domain'         => $event->domain,
                    'event_type'     => $event->eventType,
                    'event_version'  => $event->eventVersion,
                    'aggregate_type' => $event->aggregateType,
                    'aggregate_id'   => $event->aggregateId,
                    'payload'        => json_encode($event->payload, JSON_THROW_ON_ERROR),
                    'metadata'       => json_encode($event->metadata, JSON_THROW_ON_ERROR),
                    'hash'           => $event->hash,
                    'prev_hash'      => $event->prevHash,
                    'hmac'           => $event->hmac,
                    'source'         => 'remote',
                    'publish_status' => 'published',
                    'created_at'     => $event->createdAt,
                ]
            );

            if ($affected === 0) {
                // Concurrent insert won — this coroutine's work is done.
                return;
            }

            $db->execute(
                'INSERT INTO origin_state (origin_node, last_sequence, last_hash, updated_at)
                 VALUES (:node, :seq, :hash, :now)
                 ON CONFLICT(origin_node) DO UPDATE SET
                   last_sequence = excluded.last_sequence,
                   last_hash     = excluded.last_hash,
                   updated_at    = excluded.updated_at',
                [
                    'node' => $event->originNode,
                    'seq'  => $event->sequence,
                    'hash' => $event->hash,
                    'now'  => $now,
                ]
            );
        });

        // Update ownership registry if aggregate info is present.
        if ($event->aggregateType !== null && $event->aggregateId !== null) {
            $this->ownership->recordRemoteOwnership(
                $event->aggregateType,
                $event->aggregateId,
                $event->originNode,
            );
        }

        // Apply to main database via registered replay handler (idempotent).
        $this->handlerRegistry->apply(
            $event,
            fn (string $class): object => ContainerFactory::get()->resolve($class),
        );

        // Mark event as applied.
        $this->db->execute(
            'UPDATE events SET applied_at = :now WHERE event_id = :id',
            ['now' => $now, 'id' => $event->eventId]
        );

        // Advance cluster consumer position.
        $this->updateConsumerState($consumerName, $sourceCluster, $event->eventId);

        $msg->ack();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private function getLastNatsSequence(string $consumerName, string $clusterId): int
    {
        $row = $this->db->fetchOne(
            'SELECT last_nats_sequence FROM consumer_state
             WHERE consumer_id = :consumer AND cluster_id = :cluster',
            ['consumer' => $consumerName, 'cluster' => $clusterId]
        );

        return $row !== null ? (int) $row['last_nats_sequence'] : 0;
    }

    private function updateConsumerState(string $consumerName, string $clusterId, string $lastEventId): void
    {
        // Retrieve nats_sequence from publish_log for this cluster if available.
        $row = $this->db->fetchOne(
            'SELECT nats_sequence FROM publish_log
             WHERE event_id = :id AND cluster_id = :cluster',
            ['id' => $lastEventId, 'cluster' => $clusterId]
        );

        $natsSeq = $row !== null ? (int) $row['nats_sequence'] : 0;
        $now     = gmdate('Y-m-d\TH:i:s\Z');

        $this->db->execute(
            'INSERT INTO consumer_state (consumer_id, cluster_id, last_nats_sequence, last_event_id, updated_at)
             VALUES (:consumer, :cluster, :seq, :event_id, :now)
             ON CONFLICT(consumer_id, cluster_id) DO UPDATE SET
               last_nats_sequence = CASE WHEN :seq > last_nats_sequence THEN :seq ELSE last_nats_sequence END,
               last_event_id      = :event_id,
               updated_at         = :now',
            [
                'consumer' => $consumerName,
                'cluster'  => $clusterId,
                'seq'      => $natsSeq,
                'event_id' => $lastEventId,
                'now'      => $now,
            ]
        );
    }

    private function quarantine(LedgerEvent $event, string $expectedHash, string $sourceCluster): void
    {
        $now = gmdate('Y-m-d\TH:i:s\Z');

        $this->db->execute(
            'INSERT OR IGNORE INTO quarantined_events
             (event_id, origin_node, expected_hash, received_hash, received_payload, source_cluster, quarantined_at)
             VALUES (:event_id, :origin_node, :expected_hash, :received_hash, :received_payload, :source_cluster, :quarantined_at)',
            [
                'event_id'         => $event->eventId,
                'origin_node'      => $event->originNode,
                'expected_hash'    => $expectedHash,
                'received_hash'    => $event->hash,
                'received_payload' => $event->toJson(),
                'source_cluster'   => $sourceCluster,
                'quarantined_at'   => $now,
            ]
        );

        error_log(sprintf(
            '[semitexa-ledger] EVENT QUARANTINED event_id=%s origin=%s cluster=%s',
            $event->eventId,
            $event->originNode,
            $sourceCluster,
        ));
    }
}
