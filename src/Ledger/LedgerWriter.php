<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ledger;

use Semitexa\Core\Support\PayloadSerializer;
use Semitexa\Ledger\Attribute\OwnedAggregate;
use Semitexa\Ledger\Attribute\Propagated;
use Semitexa\Ledger\Dto\LedgerEvent;
use Semitexa\Ledger\Exception\AggregateNotFoundException;
use Semitexa\Ledger\Exception\OwnershipViolationException;
use Semitexa\Ledger\Ownership\AggregateOwnershipService;
use Semitexa\Ledger\Support\UuidV7;

/**
 * Appends domain events to the local SQLite ledger.
 *
 * Called from EventDispatcher's post-dispatch hook for every event annotated
 * with #[Propagated]. Performs:
 *
 *   1. Ownership gate check (if #[OwnedAggregate] is present).
 *   2. Sequence increment + hash-chain computation.
 *   3. HMAC signing.
 *   4. Atomic SQLite transaction (events + origin_state).
 *
 * NATS publishing is intentionally NOT done here. LedgerPublisher (a background
 * Swoole coroutine) reads the ledger and publishes asynchronously. This decouples
 * the write path from NATS availability.
 */
final class LedgerWriter
{
    /** @var array<string, Propagated|null> */
    private array $propagatedCache = [];

    /** @var array<string, OwnedAggregate|null> */
    private array $ownedAggregateCache = [];

    public function __construct(
        private readonly LedgerConnection $db,
        private readonly string $nodeId,
        private readonly string $hmacKey,
        private readonly AggregateOwnershipService $ownership,
    ) {}

    /**
     * Append an event to the ledger.
     *
     * Returns the persisted LedgerEvent, or null if the event class is not
     * annotated with #[Propagated] (i.e. the event is local-only).
     */
    public function append(object $event): ?LedgerEvent
    {
        $eventClass = get_class($event);
        $payload = PayloadSerializer::toArray($event);

        $propagated = $this->readPropagated($eventClass);
        if ($propagated === null) {
            return null;
        }

        $domain    = $propagated->domain ?? $this->deriveDomain($eventClass);
        $eventType = $this->deriveEventType($eventClass);

        $now       = $this->nowMicros();
        $eventId   = UuidV7::generate();
        $metadata  = ['timestamp' => $now, 'node_id' => $this->nodeId];

        $ledgerEvent = $this->db->transaction(function (LedgerConnection $db) use (
            $event, $eventClass, $eventId, $domain, $eventType, $payload, $metadata, $now
        ): LedgerEvent {
            // Ownership gate is checked inside the exclusive transaction to prevent
            // TOCTOU races between concurrent Swoole workers (VULN-005).
            [$aggregateType, $aggregateId] = $this->resolveAggregate($event, $eventClass, $payload);
            // Increment per-origin sequence and fetch previous hash.
            [$sequence, $prevHash] = $this->nextSequenceAndHash($db);

            // Compute hash chain: SHA-256(prevHash || eventId || json(payload))
            $hash = hash('sha256', $prevHash . $eventId . json_encode($payload, JSON_THROW_ON_ERROR));
            $hmac = hash_hmac('sha256', $hash, $this->hmacKey);

            $record = new LedgerEvent(
                eventId:       $eventId,
                originNode:    $this->nodeId,
                sequence:      $sequence,
                domain:        $domain,
                eventType:     $eventType,
                eventVersion:  1,
                payload:       $payload,
                metadata:      $metadata,
                hash:          $hash,
                prevHash:      $prevHash,
                hmac:          $hmac,
                source:        'local',
                createdAt:     $now,
                appliedAt:     null,
                aggregateType: $aggregateType,
                aggregateId:   $aggregateId,
                publishStatus: 'pending',
            );

            $db->execute(
                'INSERT INTO events
                 (event_id, origin_node, sequence, domain, event_type, event_version,
                  aggregate_type, aggregate_id, payload, metadata, hash, prev_hash, hmac,
                  source, publish_status, created_at)
                 VALUES
                 (:event_id, :origin_node, :sequence, :domain, :event_type, :event_version,
                  :aggregate_type, :aggregate_id, :payload, :metadata, :hash, :prev_hash, :hmac,
                  :source, :publish_status, :created_at)',
                [
                    'event_id'       => $record->eventId,
                    'origin_node'    => $record->originNode,
                    'sequence'       => $record->sequence,
                    'domain'         => $record->domain,
                    'event_type'     => $record->eventType,
                    'event_version'  => $record->eventVersion,
                    'aggregate_type' => $record->aggregateType,
                    'aggregate_id'   => $record->aggregateId,
                    'payload'        => json_encode($record->payload, JSON_THROW_ON_ERROR),
                    'metadata'       => json_encode($record->metadata, JSON_THROW_ON_ERROR),
                    'hash'           => $record->hash,
                    'prev_hash'      => $record->prevHash,
                    'hmac'           => $record->hmac,
                    'source'         => $record->source,
                    'publish_status' => $record->publishStatus,
                    'created_at'     => $record->createdAt,
                ]
            );

            $db->execute(
                'INSERT INTO origin_state (origin_node, last_sequence, last_hash, updated_at)
                 VALUES (:node, :seq, :hash, :now)
                 ON CONFLICT(origin_node) DO UPDATE SET
                   last_sequence = excluded.last_sequence,
                   last_hash     = excluded.last_hash,
                   updated_at    = excluded.updated_at',
                [
                    'node' => $this->nodeId,
                    'seq'  => $sequence,
                    'hash' => $hash,
                    'now'  => $now,
                ]
            );

            return $record;
        });

        return $ledgerEvent;
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    /**
     * @return array{0: int, 1: string} [$nextSequence, $prevHash]
     */
    private function nextSequenceAndHash(LedgerConnection $db): array
    {
        $row = $db->fetchOne(
            'SELECT last_sequence, last_hash FROM origin_state WHERE origin_node = :node',
            ['node' => $this->nodeId]
        );

        if ($row === null) {
            // First event for this origin.
            $prevHash = hash('sha256', "genesis:{$this->nodeId}");
            return [1, $prevHash];
        }

        return [(int) $row['last_sequence'] + 1, (string) $row['last_hash']];
    }

    /**
     * @return array{0: string|null, 1: string|null} [$aggregateType, $aggregateId]
     */
    private function resolveAggregate(object $event, string $eventClass, array $payload): array
    {
        $ownedAttr = $this->readOwnedAggregate($eventClass);
        if ($ownedAttr === null) {
            return [null, null];
        }

        $idField = $ownedAttr->idField;
        $aggregateId = $this->readProperty($event, $idField, $payload);

        if ($aggregateId === null) {
            throw new \InvalidArgumentException(
                "Event {$eventClass} declares #[OwnedAggregate(idField: '{$idField}')] " .
                "but property '{$idField}' is null or missing."
            );
        }

        if ($ownedAttr->creates) {
            $this->ownership->claimOwnership($ownedAttr->type, $aggregateId);
        } else {
            $ownerNodeId = $this->ownership->resolveOwner($ownedAttr->type, $aggregateId);

            if ($ownerNodeId === null) {
                throw new AggregateNotFoundException($ownedAttr->type, $aggregateId);
            }

            if ($ownerNodeId !== $this->nodeId) {
                throw new OwnershipViolationException(
                    $ownedAttr->type,
                    $aggregateId,
                    $ownerNodeId,
                    $this->nodeId,
                );
            }
        }

        return [$ownedAttr->type, $aggregateId];
    }

    private function readPropagated(string $eventClass): ?Propagated
    {
        if (!array_key_exists($eventClass, $this->propagatedCache)) {
            $ref  = new \ReflectionClass($eventClass);
            $attrs = $ref->getAttributes(Propagated::class);
            $this->propagatedCache[$eventClass] = $attrs !== []
                ? $attrs[0]->newInstance()
                : null;
        }

        return $this->propagatedCache[$eventClass];
    }

    private function readOwnedAggregate(string $eventClass): ?OwnedAggregate
    {
        if (!array_key_exists($eventClass, $this->ownedAggregateCache)) {
            $ref  = new \ReflectionClass($eventClass);
            $attrs = $ref->getAttributes(OwnedAggregate::class);
            $this->ownedAggregateCache[$eventClass] = $attrs !== []
                ? $attrs[0]->newInstance()
                : null;
        }

        return $this->ownedAggregateCache[$eventClass];
    }

    private function readProperty(object $event, string $property, array $payload): ?string
    {
        $candidates = [$property];

        $camel = lcfirst(str_replace('_', '', ucwords($property, '_')));
        if (!in_array($camel, $candidates, true)) {
            $candidates[] = $camel;
        }

        foreach ($candidates as $candidate) {
            if (array_key_exists($candidate, $payload)) {
                $value = $payload[$candidate];
                return $value !== null ? (string) $value : null;
            }
        }

        foreach ($candidates as $candidate) {
            if (!property_exists($event, $candidate)) {
                continue;
            }

            $reflectionProperty = new \ReflectionProperty($event, $candidate);
            $reflectionProperty->setAccessible(true);
            $value = $reflectionProperty->getValue($event);

            return $value !== null ? (string) $value : null;
        }

        return null;
    }

    /**
     * Derive the domain from the event class namespace.
     *
     * e.g. App\Modules\Inventory\Event\StockAdjusted → "inventory"
     */
    private function deriveDomain(string $eventClass): string
    {
        $parts = explode('\\', $eventClass);
        // Walk backwards; skip the class name and "Event/Events" segments.
        $skip = ['event', 'events'];
        for ($i = count($parts) - 2; $i >= 0; $i--) {
            $segment = strtolower($parts[$i]);
            if (!in_array($segment, $skip, true)) {
                return $segment;
            }
        }

        return 'default';
    }

    /**
     * Derive a snake_case event type from the class short name.
     *
     * e.g. StockAdjusted → "stock_adjusted"
     */
    private function deriveEventType(string $eventClass): string
    {
        $shortName = substr($eventClass, strrpos($eventClass, '\\') + 1);
        return strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $shortName) ?? $shortName);
    }

    private function nowMicros(): string
    {
        return gmdate('Y-m-d\TH:i:s') . '.' . sprintf('%06d', (int) ((microtime(true) - floor(microtime(true))) * 1_000_000)) . 'Z';
    }
}
