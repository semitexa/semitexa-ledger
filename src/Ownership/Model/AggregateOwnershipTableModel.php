<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Ownership\Model;

use Semitexa\Orm\Adapter\MySqlType;
use Semitexa\Orm\Attribute\Column;
use Semitexa\Orm\Attribute\Connection;
use Semitexa\Orm\Attribute\FromTable;
use Semitexa\Orm\Attribute\Index;
use Semitexa\Orm\Attribute\PrimaryKey;

/**
 * ORM table model for aggregate_ownership.
 *
 * Defines the schema for the main-DB table that tracks which node
 * owns each aggregate. Used by orm:sync to create/migrate the table.
 *
 * The effective primary key is (aggregate_type, aggregate_id) enforced
 * via a unique index. The `id` column is a surrogate auto-increment key
 * because the ORM schema sync only supports single-column primary keys.
 *
 * By default this table lives in the main ('default') database connection.
 * Override at runtime via LEDGER_DB_CONNECTION environment variable in
 * LedgerBootstrap — the attribute here documents the default.
 */
#[FromTable(name: 'aggregate_ownership')]
#[Connection('default')]
#[Index(columns: ['aggregate_type', 'aggregate_id'], unique: true, name: 'uniq_aggregate_ownership')]
#[Index(columns: ['owner_node_id'], unique: false, name: 'idx_aggregate_ownership_owner')]
final readonly class AggregateOwnershipTableModel
{
    public function __construct(
        #[PrimaryKey(strategy: 'auto')]
        #[Column(type: MySqlType::Bigint)]
        public int $id,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $aggregate_type,

        #[Column(type: MySqlType::Varchar, length: 128)]
        public string $aggregate_id,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $owner_node_id,

        #[Column(type: MySqlType::Datetime)]
        public \DateTimeImmutable $acquired_at,

        #[Column(type: MySqlType::Varchar, length: 64, nullable: true)]
        public ?string $transfer_token,

        #[Column(type: MySqlType::Datetime, nullable: true)]
        public ?\DateTimeImmutable $transfer_expires_at,
    ) {}
}
