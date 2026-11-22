<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Ledger\Domain\Model\LedgerEvent;

/**
 * What an event's hash and HMAC cover, by hash version.
 *
 * Version 1 (the first format) hashed `prevHash . eventId . json(payload)` and
 * signed that hash. Domain, event type, aggregate, metadata and the rest of
 * the envelope were unsigned: whoever could write to NATS could rewrite them
 * on a signed event and both checks still passed.
 *
 * Version 2 hashes the whole envelope, and signs the hash under a label of its
 * own. The label is what stops a downgrade: a version 2 signature is not a
 * valid version 1 signature of any hash, so relabelling a new event as an old
 * one fails the HMAC instead of falling back to the weaker formula.
 *
 * The version travels in the envelope (`hash_version`, absent = 1) and is
 * stored with the row (`events.hash_version`), so stored version 1 history and
 * events from nodes still on version 1 keep verifying under their own formula.
 * Version 1 events stay as weak as they were written; only an origin that
 * writes version 2 is protected.
 */
final class EnvelopeSeal
{
    public const LEGACY   = 1;
    public const ENVELOPE = 2;
    public const CURRENT  = self::ENVELOPE;

    /** Prefixed to what version 2 hashes and to what it signs. */
    private const ENVELOPE_LABEL = "semitexa-ledger-v2\n";

    public static function supports(int $version): bool
    {
        return $version === self::LEGACY || $version === self::ENVELOPE;
    }

    /**
     * The version this node writes: LEDGER_HASH_VERSION, or the current one.
     * Setting it to 1 is for a rolling upgrade only — see the README.
     */
    public static function writeVersionFromEnv(): int
    {
        $value = getenv('LEDGER_HASH_VERSION');
        if ($value === false || $value === '') {
            return self::CURRENT;
        }

        if (!ctype_digit($value) || !self::supports((int) $value)) {
            throw new \RuntimeException(
                "semitexa-ledger: LEDGER_HASH_VERSION must be 1 or 2, got '{$value}'."
            );
        }

        return (int) $value;
    }

    /**
     * @param array<mixed>      $payload
     * @param array<mixed>|null $metadata
     */
    public static function hash(
        int $version,
        string $prevHash,
        string $eventId,
        string $originNode,
        int $sequence,
        string $domain,
        string $eventType,
        int $eventVersion,
        ?string $aggregateType,
        ?string $aggregateId,
        array $payload,
        ?array $metadata,
        string $createdAt,
    ): string {
        if ($version === self::LEGACY) {
            return hash('sha256', $prevHash . $eventId . json_encode($payload, JSON_THROW_ON_ERROR));
        }

        if ($version !== self::ENVELOPE) {
            throw new \InvalidArgumentException("Unknown ledger hash version: {$version}");
        }

        // A JSON list: every field has its own position and its own quoting,
        // so no value can spill into its neighbour. Same encoder flags as the
        // payload always had: a receiver re-encodes what it decoded, and the
        // two must come out byte for byte the same.
        return hash('sha256', self::ENVELOPE_LABEL . json_encode([
            $prevHash,
            $eventId,
            $originNode,
            $sequence,
            $domain,
            $eventType,
            $eventVersion,
            $aggregateType,
            $aggregateId,
            $payload,
            $metadata,
            $createdAt,
        ], JSON_THROW_ON_ERROR));
    }

    /** The hash $event must carry if it follows $prevHash in its origin's chain. */
    public static function hashOf(LedgerEvent $event, string $prevHash): string
    {
        return self::hash(
            $event->hashVersion,
            $prevHash,
            $event->eventId,
            $event->originNode,
            $event->sequence,
            $event->domain,
            $event->eventType,
            $event->eventVersion,
            $event->aggregateType,
            $event->aggregateId,
            $event->payload,
            $event->metadata,
            $event->createdAt,
        );
    }

    public static function hmac(int $version, string $hash, string $key): string
    {
        return match ($version) {
            self::LEGACY   => hash_hmac('sha256', $hash, $key),
            self::ENVELOPE => hash_hmac('sha256', self::ENVELOPE_LABEL . $hash, $key),
            default        => throw new \InvalidArgumentException("Unknown ledger hash version: {$version}"),
        };
    }

    /**
     * True only when the event's version is known, it names $prevHash as its
     * predecessor, its hash is the one its version computes, and its HMAC is
     * the one $key signs for that version.
     */
    public static function verifies(LedgerEvent $event, string $prevHash, string $key): bool
    {
        return self::supports($event->hashVersion)
            && hash_equals($prevHash, $event->prevHash)
            && hash_equals(self::hashOf($event, $prevHash), $event->hash)
            && hash_equals(self::hmac($event->hashVersion, $event->hash, $key), $event->hmac);
    }
}
