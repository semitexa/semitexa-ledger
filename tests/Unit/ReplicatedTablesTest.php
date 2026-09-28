<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Ledger\Application\Service\Replication\ReplicatedTables;

final class ReplicatedTablesTest extends TestCase
{
    #[Test]
    public function a_listed_replicated_class_that_cannot_load_fails_discovery_instead_of_vanishing(): void
    {
        $discovery = $this->createStub(ClassDiscovery::class);
        $discovery->method('findClassesWithAttribute')->willReturn(['App\\Gone\\MissingResource']);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('App\\Gone\\MissingResource is listed by discovery but cannot be loaded');

        ReplicatedTables::discover($discovery);
    }
}
