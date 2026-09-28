<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration\Fixture;

use Semitexa\Ledger\Attribute\Propagated;

/** Data in public properties, no getters — serialises to an empty payload. */
#[Propagated(domain: 'ledgerit')]
final class PropertyOnlyEvent
{
    public function __construct(public readonly string $id) {}
}
