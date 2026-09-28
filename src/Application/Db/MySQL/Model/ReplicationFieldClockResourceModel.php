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
 * The clock of every field of every #[Replicated] row: the hybrid logical
 * clock reading and node of the write that last set it (ADR 0001). The pseudo
 * column `__exists` carries the row's own existence, so a delete is a
 * tombstone that a later write can override.
 *
 * Written in the same transaction as the data, by the capture on a local write
 * and by the applier on a remote one. The effective key is
 * (table_name, row_pk, column_name); `id` is a surrogate because schema sync
 * supports single-column primary keys only.
 */
#[FromTable(name: 'replication_field_clock')]
#[Connection('default')]
#[Index(columns: ['table_name', 'row_pk', 'column_name'], unique: true, name: 'uniq_replication_field_clock')]
final readonly class ReplicationFieldClockResourceModel
{
    public function __construct(
        #[PrimaryKey(strategy: 'auto')]
        #[Column(type: MySqlType::Bigint)]
        public int $id,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $table_name,

        /** The row's primary key, as the codec encodes it. */
        #[Column(type: MySqlType::Varchar, length: 191)]
        public string $row_pk,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $column_name,

        #[Column(type: MySqlType::Char, length: 20)]
        public string $hlc,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $node,
    ) {}
}
