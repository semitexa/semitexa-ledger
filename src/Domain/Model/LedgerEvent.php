<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Model;

/**
 * Immutable value object representing one entry in the event ledger.
 *
 * All fields map 1:1 to the `events` SQLite table. Created by LedgerWriter
 * (local events) and LedgerReplayer (remote events received from NATS).
 */
final readonly class LedgerEvent
{
    public function __construct(
        /** UUIDv7 — globally unique, time-sortable. */
        public string $eventId,

        /** Slug of the originating node (e.g. "store-a"). */
        public string $originNode,

        /** Per-origin monotonically increasing integer. */
        public int $sequence,

        /** Business domain matching the Semitexa module name (lowercased). */
        public string $domain,

        /** Snake_case event name (e.g. "stock_adjusted"). */
        public string $eventType,

        /** Schema version; bump on breaking changes. */
        public int $eventVersion,

        /** Domain-specific event data. */
        public array $payload,

        /** Optional: correlation_id, causation_id, actor, timestamp, command_id. */
        public ?array $metadata,

        /** SHA-256 over what $hashVersion covers — see EnvelopeSeal. */
        public string $hash,

        /** Hash of the previous event from the same origin_node. */
        public string $prevHash,

        /** HMAC-SHA256 of hash (labelled from hash version 2 on), keyed by the node's HMAC key. */
        public string $hmac,

        /** 'local' for events produced here, 'remote' for events received via NATS. */
        public string $source,

        /** ISO 8601 with microseconds. */
        public string $createdAt,

        /** Set after the event is applied to the main database. Null until then. */
        public ?string $appliedAt,

        /** Aggregate type string (e.g. "product"). Null for non-owned events. */
        public ?string $aggregateType,

        /** UUID of the aggregate root. Null for non-owned events. */
        public ?string $aggregateId,

        /**
         * Publish tracking: 'pending' | 'published' | 'failed'
         * Managed by LedgerPublisher. Only meaningful for source='local'.
         */
        public string $publishStatus = 'pending',

        /**
         * Which formula produced hash and hmac: 1 = payload only (the first
         * format), 2 = the whole envelope. See EnvelopeSeal.
         */
        public int $hashVersion = 1,
    ) {}

    /**
     * Serialize to the wire format sent over NATS.
     */
    public function toJson(): string
    {
        return json_encode([
            'event_id'       => $this->eventId,
            'origin_node'    => $this->originNode,
            'sequence'       => $this->sequence,
            'domain'         => $this->domain,
            'event_type'     => $this->eventType,
            'event_version'  => $this->eventVersion,
            'aggregate_type' => $this->aggregateType,
            'aggregate_id'   => $this->aggregateId,
            'payload'        => $this->payload,
            'metadata'       => $this->metadata,
            'hash'           => $this->hash,
            'prev_hash'      => $this->prevHash,
            'hmac'           => $this->hmac,
            'hash_version'   => $this->hashVersion,
            'created_at'     => $this->createdAt,
        ], JSON_THROW_ON_ERROR);
    }

    /**
     * Deserialize from NATS wire format (remote events).
     */
    public static function fromJson(string $json): self
    {
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        return new self(
            eventId:       $data['event_id'],
            originNode:    $data['origin_node'],
            sequence:      (int) $data['sequence'],
            domain:        $data['domain'],
            eventType:     $data['event_type'],
            eventVersion:  (int) ($data['event_version'] ?? 1),
            payload:       $data['payload'] ?? [],
            metadata:      $data['metadata'] ?? null,
            hash:          $data['hash'],
            prevHash:      $data['prev_hash'],
            hmac:          $data['hmac'],
            source:        'remote',
            createdAt:     $data['created_at'],
            appliedAt:     null,
            aggregateType: $data['aggregate_type'] ?? null,
            aggregateId:   $data['aggregate_id'] ?? null,
            publishStatus: 'published', // remote events are already published
            // Absent on events from a node that predates versions.
            hashVersion:   self::hashVersionOf(is_array($data) ? ($data['hash_version'] ?? null) : null),
        );
    }

    /**
     * Reconstruct from a SQLite row (associative array).
     *
     * @param array<string, mixed> $row
     */
    public static function fromRow(array $row): self
    {
        return new self(
            eventId:       (string) $row['event_id'],
            originNode:    (string) $row['origin_node'],
            sequence:      (int) $row['sequence'],
            domain:        (string) $row['domain'],
            eventType:     (string) $row['event_type'],
            eventVersion:  (int) ($row['event_version'] ?? 1),
            payload:       json_decode((string) $row['payload'], true, 512, JSON_THROW_ON_ERROR),
            metadata:      isset($row['metadata']) && $row['metadata'] !== null
                               ? json_decode((string) $row['metadata'], true, 512, JSON_THROW_ON_ERROR)
                               : null,
            hash:          (string) $row['hash'],
            prevHash:      (string) $row['prev_hash'],
            hmac:          (string) $row['hmac'],
            source:        (string) ($row['source'] ?? 'local'),
            createdAt:     (string) $row['created_at'],
            appliedAt:     isset($row['applied_at']) ? (string) $row['applied_at'] : null,
            aggregateType: isset($row['aggregate_type']) ? (string) $row['aggregate_type'] : null,
            aggregateId:   isset($row['aggregate_id']) ? (string) $row['aggregate_id'] : null,
            publishStatus: (string) ($row['publish_status'] ?? 'pending'),
            hashVersion:   self::hashVersionOf($row['hash_version'] ?? null),
        );
    }

    /**
     * Absent means 1: the first format carried no version. Anything that is
     * not a whole number reads as 0, a version nothing verifies under.
     */
    private static function hashVersionOf(mixed $value): int
    {
        return match (true) {
            $value === null                            => 1,
            is_int($value)                             => $value,
            is_string($value) && ctype_digit($value)   => (int) $value,
            default                                    => 0,
        };
    }
}
