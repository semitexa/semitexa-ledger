<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Db\MySQL\Model;

use Semitexa\Orm\Adapter\MySqlType;
use Semitexa\Orm\Attribute\Column;
use Semitexa\Orm\Attribute\Connection;
use Semitexa\Orm\Attribute\FromTable;
use Semitexa\Orm\Attribute\Index;
use Semitexa\Orm\Attribute\PrimaryKey;

/**
 * The last values of a hard-deleted #[Replicated] row (ADR 0001).
 *
 * A delete can lose to a later write from another node, which brings the row
 * back. Its field clocks survive the delete in replication_field_clock, but
 * its values would not — and a row rebuilt without them would carry older
 * values than the clocks say it has, a silent divergence. The image here is
 * what the row is rebuilt from; it is removed when the row returns.
 */
#[FromTable(name: 'replication_tombstone')]
#[Connection('default')]
#[Index(columns: ['table_name', 'row_pk'], unique: true, name: 'uniq_replication_tombstone')]
final readonly class ReplicationTombstoneResourceModel
{
    public function __construct(
        #[PrimaryKey(strategy: 'auto')]
        #[Column(type: MySqlType::Bigint)]
        public int $id,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $table_name,

        #[Column(type: MySqlType::Varchar, length: 191)]
        public string $row_pk,

        /** column => value, as RowCodec encodes it */
        #[Column(type: MySqlType::LongText)]
        public string $image,
    ) {}
}
