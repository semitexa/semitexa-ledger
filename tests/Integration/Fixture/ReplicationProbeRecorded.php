<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration\Fixture;

use Semitexa\Ledger\Attribute\Propagated;

#[Propagated(domain: 'ledgerit')]
final class ReplicationProbeRecorded
{
    private string $probeId = '';
    private string $label = '';
    /** @var array<string, mixed> */
    private array $attributes = [];

    /**
     * @param array<string, mixed> $attributes
     */
    public static function of(string $probeId, string $label, array $attributes = []): self
    {
        $event = new self();
        $event->probeId = $probeId;
        $event->label = $label;
        $event->attributes = $attributes;

        return $event;
    }

    public function getProbeId(): string { return $this->probeId; }
    public function setProbeId(string $probeId): void { $this->probeId = $probeId; }

    public function getLabel(): string { return $this->label; }
    public function setLabel(string $label): void { $this->label = $label; }

    /** @return array<string, mixed> */
    public function getAttributes(): array { return $this->attributes; }
    /** @param array<string, mixed> $attributes */
    public function setAttributes(array $attributes): void { $this->attributes = $attributes; }
}
