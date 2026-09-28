<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration\Fixture;

final readonly class ReplicatedArticle
{
    public function __construct(
        public string $id,
        public string $title,
        public string $body,
    ) {}
}
