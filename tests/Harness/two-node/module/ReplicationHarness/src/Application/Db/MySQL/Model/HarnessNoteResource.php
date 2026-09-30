<?php

declare(strict_types=1);

namespace Semitexa\Modules\ReplicationHarness\Application\Db\MySQL\Model;

use Semitexa\Orm\Adapter\MySqlType;
use Semitexa\Orm\Attribute\Column;
use Semitexa\Orm\Attribute\FromTable;
use Semitexa\Orm\Attribute\Index;
use Semitexa\Orm\Attribute\PrimaryKey;
use Semitexa\Orm\Attribute\Replicated;
use Semitexa\Orm\Metadata\HasColumnReferences;
use Semitexa\Orm\Metadata\HasRelationReferences;

/**
 * A replicated row for the two-node harness. Mounted only into harness nodes.
 * The title is unique so that two nodes can each legitimately take the same one
 * while apart — the replication conflict of ADR 0001 §5.
 */
#[FromTable(name: 'harness_notes')]
#[Index(columns: ['title'], unique: true, name: 'uniq_harness_notes_title')]
#[Replicated]
final readonly class HarnessNoteResource
{
    use HasColumnReferences;
    use HasRelationReferences;

    public function __construct(
        #[PrimaryKey(strategy: 'uuid')]
        #[Column(type: MySqlType::Varchar, length: 36)]
        public string $id = '',

        #[Column(type: MySqlType::Varchar, length: 255)]
        public string $title = '',

        #[Column(type: MySqlType::Varchar, length: 255)]
        public string $body = '',
    ) {}
}
