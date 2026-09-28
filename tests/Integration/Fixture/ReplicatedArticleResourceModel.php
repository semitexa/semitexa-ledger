<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration\Fixture;

use Semitexa\Orm\Adapter\MySqlType;
use Semitexa\Orm\Attribute\Column;
use Semitexa\Orm\Attribute\FromTable;
use Semitexa\Orm\Attribute\PrimaryKey;
use Semitexa\Orm\Attribute\Replicated;

#[FromTable(name: 'ledger_it_articles')]
#[Replicated]
final readonly class ReplicatedArticleResourceModel
{
    public function __construct(
        #[PrimaryKey(strategy: 'uuid')]
        #[Column(type: MySqlType::Varchar, length: 36)]
        public string $id,

        #[Column(type: MySqlType::Varchar, length: 255)]
        public string $title,

        #[Column(type: MySqlType::Varchar, length: 255)]
        public string $body,
    ) {}
}
