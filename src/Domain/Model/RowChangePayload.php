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
    /** The pseudo column that carries the row's existence; never a real field. */
    public const EXISTS = '__exists';

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
            if ((string) $column === self::EXISTS) {
                // It would shadow the separate existence stamp in the merge.
                throw new \UnexpectedValueException("'" . self::EXISTS . "' is reserved and cannot be a field.");
            }
            $fields[(string) $column] = FieldStamp::fromArray($field, (string) $column);
        }

        $exists = FieldStamp::fromArray($payload['exists'] ?? null, 'exists');
        if (!is_bool($exists->value)) {
            // "false" as a string would read as true.
            throw new \UnexpectedValueException("A row change's 'exists' must be a boolean.");
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
            exists: $exists,
            fields: $fields,
        );
    }

    /** The latest clock anywhere in the change — the event's own, or any field's. */
    public function latestClock(): HlcTimestamp
    {
        $latest = $this->clock;
        foreach ([$this->exists, ...array_values($this->fields)] as $stamp) {
            if ($stamp->clock->compareTo($latest) > 0) {
                $latest = $stamp->clock;
            }
        }

        return $latest;
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
