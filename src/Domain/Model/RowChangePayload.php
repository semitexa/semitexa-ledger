<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Model;

/**
 * A replicated row change as ReplicationCaptureService writes it, read back
 * off the wire with its shape checked once, here.
 *
 * Everything in it came from another node, so nothing is trusted: a payload
 * of the wrong shape is refused as a whole rather than half-applied.
 */
final readonly class RowChangePayload
{
    /**
     * @param array<string, FieldStamp> $fields column => stamp, the whole row
     */
    public function __construct(
        public string $table,
        public string $pkColumn,
        public string $rowKey,
        public HlcTimestamp $clock,
        public string $node,
        public FieldStamp $exists,
        public array $fields,
    ) {}

    /**
     * @throws \UnexpectedValueException when the payload is not a row change
     */
    public static function fromArray(mixed $payload): self
    {
        if (!is_array($payload)) {
            throw new \UnexpectedValueException('A row change must be an object.');
        }
        if (!is_array($payload['fields'] ?? null) || $payload['fields'] === []) {
            throw new \UnexpectedValueException("A row change needs its 'fields'.");
        }

        $fields = [];
        foreach ($payload['fields'] as $column => $field) {
            $fields[(string) $column] = FieldStamp::fromArray($field, (string) $column);
        }

        try {
            $clock = HlcTimestamp::fromString(self::string($payload, 'hlc'));
        } catch (\InvalidArgumentException $e) {
            throw new \UnexpectedValueException('A row change has no valid clock: ' . $e->getMessage(), 0, $e);
        }

        return new self(
            table: self::string($payload, 'table'),
            pkColumn: self::string($payload, 'pk_column'),
            rowKey: self::string($payload, 'pk'),
            clock: $clock,
            node: self::string($payload, 'node', allowEmpty: true),
            exists: FieldStamp::fromArray($payload['exists'] ?? null, 'exists'),
            fields: $fields,
        );
    }

    /** @param array<mixed> $payload */
    private static function string(array $payload, string $key, bool $allowEmpty = false): string
    {
        $value = $payload[$key] ?? null;
        if (!is_string($value) || (!$allowEmpty && $value === '')) {
            throw new \UnexpectedValueException("A row change needs a string '{$key}'.");
        }

        return $value;
    }
}
