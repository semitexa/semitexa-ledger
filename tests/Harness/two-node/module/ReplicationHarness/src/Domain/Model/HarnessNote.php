<?php

declare(strict_types=1);

namespace Semitexa\Modules\ReplicationHarness\Domain\Model;

final readonly class HarnessNote
{
    public function __construct(
        public string $id,
        public string $title,
        public string $body,
    ) {}
}
