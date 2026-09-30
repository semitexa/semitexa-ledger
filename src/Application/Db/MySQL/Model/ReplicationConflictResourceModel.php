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
 * Remote changes that broke a constraint here, kept for application code to
 * resolve (ADR 0001 §5; see ReplicationConflict).
 *
 * Node-local, like the tombstones and field clocks: each node journals what IT
 * could not apply. One row per conflict, not per event — `conflict_key` is the
 * unapplied fields' stamps, which every later change of the row re-sends.
 */
#[FromTable(name: 'replication_conflict')]
#[Connection('default')]
#[Index(columns: ['conflict_key'], unique: true, name: 'uniq_replication_conflict')]
#[Index(columns: ['table_name', 'row_pk'], name: 'idx_replication_conflict_row')]
final readonly class ReplicationConflictResourceModel
{
    public function __construct(
        #[PrimaryKey(strategy: 'auto')]
        #[Column(type: MySqlType::Bigint)]
        public int $id,

        #[Column(type: MySqlType::Char, length: 40)]
        public string $conflict_key,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $table_name,

        #[Column(type: MySqlType::Varchar, length: 191)]
        public string $row_pk,

        /** JSON list of the fields left unapplied; [] when the row could not be created */
        #[Column(type: MySqlType::Text)]
        public string $columns,

        /** JSON {column: {v, t, n}}, values as RowCodec encodes them */
        #[Column(type: MySqlType::LongText)]
        public string $incoming,

        /** JSON {column: {v, t, n}}, or null when the row does not exist here */
        #[Column(type: MySqlType::LongText, nullable: true)]
        public ?string $local,

        #[Column(type: MySqlType::Varchar, length: 64)]
        public string $origin_node,

        /** The driver's message: it names the key and the value */
        #[Column(type: MySqlType::Varchar, length: 500)]
        public string $reason,

        #[Column(type: MySqlType::Datetime)]
        public \DateTimeImmutable $detected_at,

        /**
         * The change as it arrived, so ReplicationConflicts::retry() can apply
         * it again; null for a conflict journaled before it was kept
         */
        #[Column(type: MySqlType::LongText, nullable: true)]
        public ?string $payload = null,

        /** When its fields landed here or a newer write superseded them; null while open */
        #[Column(type: MySqlType::Datetime, nullable: true)]
        public ?\DateTimeImmutable $resolved_at = null,
    ) {}
}
