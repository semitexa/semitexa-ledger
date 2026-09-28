<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Model;

/**
 * One field of a replicated row as it travels: the value (as RowCodec encodes
 * it) and the clock of the write that set it.
 */
final readonly class FieldStamp
{
    public function __construct(
        public mixed $value,
        public HlcTimestamp $clock,
        public string $node,
    ) {}

    /**
     * @throws \UnexpectedValueException on anything but {v, t, n} with a valid clock
     */
    public static function fromArray(mixed $field, string $name): self
    {
        if (!is_array($field) || !array_key_exists('v', $field) || !is_string($field['t'] ?? null) || !is_string($field['n'] ?? null)) {
            throw new \UnexpectedValueException("Field '{$name}' is not {v, t, n}.");
        }

        try {
            $clock = HlcTimestamp::fromString($field['t']);
        } catch (\InvalidArgumentException $e) {
            throw new \UnexpectedValueException("Field '{$name}' has no valid clock: " . $e->getMessage(), 0, $e);
        }

        return new self($field['v'], $clock, $field['n']);
    }

    public function beats(HlcTimestamp $clock, string $node): bool
    {
        return $this->clock->wins($this->node, $clock, $node);
    }
}
