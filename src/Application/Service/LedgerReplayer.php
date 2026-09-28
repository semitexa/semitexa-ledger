<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Support\StandingCoroutines;
use Semitexa\Ledger\Domain\Model\LedgerEvent;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;
use Semitexa\Ledger\Application\Service\Nats\EventStream;
use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Application\Service\Nats\PulledMessage;
use Semitexa\Ledger\Application\Service\AggregateOwnershipService;

/**
 * Consumes events other nodes published and applies them to this node's
 * ledger + main database.
 *
 * Runs one consumer loop per cluster (each in its own coroutine), in ONE worker
 * per node — see LedgerBootstrap. Both loops feed the same deduplication +
 * apply pipeline.
 *
 * Every message is acked or naked explicitly, and only after its outcome is
 * durable:
 *  1. origin_node == self        → ack (own events already in ledger).
 *  2. event_id already in ledger → ack, but first re-apply it if it was stored
 *                                  and never applied (a handler failed).
 *  3. Sequence gap               → nak with a delay; the missing predecessor is
 *                                  in the stream and arrives first on redelivery.
 *  4. Hash chain / HMAC mismatch → quarantine, ack. Never applied.
 *  5. Apply failure              → nak with a delay; the stored row carries no
 *                                  applied_at, so step 2 retries it.
 */
final class LedgerReplayer
{
    private const PULL_BATCH  = 50;
    private const PULL_WAIT   = 1.0;  // seconds the server holds an empty pull open
    private const ERROR_SLEEP = 1.0;  // seconds after a failed pull
    private const RETRY_DELAY = 1.0;  // seconds before a naked message returns

    /** @var array<string, true> clusters whose stream + consumer are known to exist */
    private array $consumerReady = [];

    /**
     * @param \Closure(class-string): object $resolveHandler builds a replay handler instance
     */
    public function __construct(
        private readonly LedgerConnection $db,
        private readonly string $nodeId,
        private readonly string $hmacKey,
        private readonly ClusterRegistry $clusters,
        private readonly ReplayHandlerRegistry $handlerRegistry,
        private readonly AggregateOwnershipService $ownership,
        private readonly \Closure $resolveHandler,
        private readonly EventStream $stream = new EventStream(),
    ) {}

    /**
     * Start one consumer coroutine per cluster.
     * Call once per node, from the worker that owns the background loops.
     */
    public function start(): void
    {
        foreach ($this->clusters->getAll() as $entry) {
            $clusterId = $entry['config']->id;
            $client    = $entry['client'];

            \Swoole\Coroutine::create(function () use ($clusterId, $client): void {
                $this->runConsumeLoop($client, $clusterId);
            });
        }
    }

    /**
     * Pull one batch from one cluster and process it. Returns how many messages
     * arrived. The consume loop is this in a loop; tests drive it directly.
     */
    public function pullAndProcess(NatsClient $client, string $clusterId): int
    {
        $consumerName = $this->consumerName();

        if (!isset($this->consumerReady[$clusterId])) {
            // Inside the caller's error handling: an unreachable cluster at boot
            // used to kill the coroutine for good, because this ran before the
            // loop's try/catch.
            $this->stream->ensure($client);

            $lastSeq = $this->getLastNatsSequence($consumerName, $clusterId);
            $client->ensurePullConsumer(
                streamName:    $this->stream->name,
                consumerName:  $consumerName,
                filterSubject: $this->stream->filterSubject(),
                startSequence: $lastSeq > 0 ? $lastSeq + 1 : 0,
            );
            $this->consumerReady[$clusterId] = true;
        }

        $messages = $client->pullMessages(
            streamName:   $this->stream->name,
            consumerName: $consumerName,
            batchSize:    self::PULL_BATCH,
            waitSeconds:  self::PULL_WAIT,
        );

        foreach ($messages as $msg) {
            $this->processMessage($msg, $clusterId);
        }

        return count($messages);
    }

    private function runConsumeLoop(NatsClient $client, string $clusterId): void
    {
        StandingCoroutines::declare(
            'ledger replayer',
            'pulling from cluster ' . $clusterId . ' — parks between pulls, by design',
        );

        while (true) {
            try {
                // The pull holds the server-side wait (PULL_WAIT) — that is the
                // park the label describes. Handling what it returned is not,
                // and a message that wedges the replayer must not read as
                // standing by design. Raised in review of core#135.
                StandingCoroutines::busy(fn (): int => $this->pullAndProcess($client, $clusterId));
            } catch (\Throwable $e) {
                unset($this->consumerReady[$clusterId]);
                StaticLoggerBridge::error('ledger', 'Replayer pull failed', ['cluster' => $clusterId, 'error' => $e->getMessage()]);
                \Swoole\Coroutine::sleep(self::ERROR_SLEEP);
            }
        }
    }

    private function consumerName(): string
    {
        return "node-{$this->nodeId}";
    }

    // -------------------------------------------------------------------------
    // Per-message pipeline
    // -------------------------------------------------------------------------

    private function processMessage(PulledMessage $msg, string $sourceCluster): void
    {
        try {
            $event = LedgerEvent::fromJson($msg->body);
        } catch (\Throwable) {
            // Malformed envelope — cannot process; ACK to prevent redelivery loop.
            $this->ack($msg, $sourceCluster, null);
            StaticLoggerBridge::error('ledger', 'Malformed event payload dropped', ['cluster' => $sourceCluster]);
            return;
        }

        // Skip own events (already in ledger from LedgerWriter).
        if ($event->originNode === $this->nodeId) {
            $this->ack($msg, $sourceCluster, $event->eventId);
            return;
        }

        // Duplicate check by event_id.
        $existing = $this->db->fetchOne(
            'SELECT hash, applied_at FROM events WHERE event_id = :id',
            ['id' => $event->eventId]
        );

        if ($existing !== null) {
            if ($existing['hash'] !== $event->hash) {
                $this->quarantine($event, (string) $existing['hash'], $sourceCluster);
            } elseif ($existing['applied_at'] === null && !$this->applyAndMark($event, $msg)) {
                return; // naked — the next delivery retries the apply
            }
            $this->ack($msg, $sourceCluster, $event->eventId);
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
            $this->ack($msg, $sourceCluster, $event->eventId);
            return;
        }

        if ($event->sequence > $expectedSeq) {
            // Gap detected — NAK so JetStream redelivers once the predecessor landed.
            $msg->nak(self::RETRY_DELAY);
            StaticLoggerBridge::warning('ledger', 'Sequence gap, waiting for predecessor', [
                'origin'   => $event->originNode,
                'expected' => $expectedSeq,
                'got'      => $event->sequence,
            ]);
            return;
        }

        // Hash chain verification.
        $expectedChainHash = hash('sha256', $expectedHash . $event->eventId . json_encode($event->payload, JSON_THROW_ON_ERROR));
        if ($event->hash !== $expectedChainHash) {
            $this->quarantine($event, $expectedChainHash, $sourceCluster);
            $this->ack($msg, $sourceCluster, $event->eventId);
            return;
        }

        // HMAC verification.
        $expectedHmac = hash_hmac('sha256', $event->hash, $this->hmacKey);
        if (!hash_equals($expectedHmac, $event->hmac)) {
            $this->quarantine($event, $expectedChainHash, $sourceCluster);
            $this->ack($msg, $sourceCluster, $event->eventId);
            return;
        }

        $now = gmdate('Y-m-d\TH:i:s\Z');

        $this->db->transaction(function (LedgerConnection $db) use ($event, $now): void {
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

        // The event is durable in the ledger now; applying it is a separate
        // step so a failed handler leaves a row without applied_at, which the
        // duplicate path above retries on the next delivery.
        if (!$this->applyAndMark($event, $msg)) {
            return;
        }

        $this->ack($msg, $sourceCluster, $event->eventId);
    }

    /**
     * Apply a stored event to the main database and stamp applied_at. On
     * failure the message is naked and false returned.
     */
    private function applyAndMark(LedgerEvent $event, PulledMessage $msg): bool
    {
        try {
            if ($event->aggregateType !== null && $event->aggregateId !== null) {
                $this->ownership->recordRemoteOwnership(
                    $event->aggregateType,
                    $event->aggregateId,
                    $event->originNode,
                );
            }

            // Registered replay handlers are idempotent by contract.
            $this->handlerRegistry->apply($event, $this->resolveHandler);
        } catch (\Throwable $e) {
            StaticLoggerBridge::error('ledger', 'Apply failed, will retry', [
                'event_id' => $event->eventId,
                'event'    => "{$event->domain}.{$event->eventType}",
                'origin'   => $event->originNode,
                'error'    => $e->getMessage(),
            ]);
            $msg->nak(self::RETRY_DELAY);
            return false;
        }

        $this->db->execute(
            'UPDATE events SET applied_at = :now WHERE event_id = :id',
            ['now' => gmdate('Y-m-d\TH:i:s\Z'), 'id' => $event->eventId]
        );

        return true;
    }

    /** Ack, and remember how far into the stream this node has consumed. */
    private function ack(PulledMessage $msg, string $clusterId, ?string $eventId): void
    {
        $this->updateConsumerState($clusterId, $eventId, $msg->streamSequence);
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

    /**
     * The stream sequence comes from the delivery itself. It used to be read
     * from publish_log, which only holds this node's OWN publishes, so the
     * position stayed 0 for every remote event.
     */
    private function updateConsumerState(string $clusterId, ?string $lastEventId, int $natsSeq): void
    {
        $this->db->execute(
            'INSERT INTO consumer_state (consumer_id, cluster_id, last_nats_sequence, last_event_id, updated_at)
             VALUES (:consumer, :cluster, :seq, :event_id, :now)
             ON CONFLICT(consumer_id, cluster_id) DO UPDATE SET
               last_nats_sequence = CASE WHEN :seq > last_nats_sequence THEN :seq ELSE last_nats_sequence END,
               last_event_id      = COALESCE(:event_id, last_event_id),
               updated_at         = :now',
            [
                'consumer' => $this->consumerName(),
                'cluster'  => $clusterId,
                'seq'      => $natsSeq,
                'event_id' => $lastEventId,
                'now'      => gmdate('Y-m-d\TH:i:s\Z'),
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

        StaticLoggerBridge::error('ledger', 'Event quarantined — hash or HMAC mismatch, never applied', [
            'event_id' => $event->eventId,
            'origin'   => $event->originNode,
            'cluster'  => $sourceCluster,
        ]);
    }
}
