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
 * Row changes waiting to enter the ledger (ADR 0001, capture).
 *
 * Written in the same transaction as the data, so a committed change always
 * has its outbox row and a rolled-back one never does. The relay moves rows
 * into the SQLite ledger in `id` order and deletes them; `event_id` becomes the
 * ledger event id, so a relay that dies between the two steps repeats the
 * append harmlessly.
 */
#[FromTable(name: 'replication_outbox')]
#[Connection('default')]
#[Index(columns: ['event_id'], unique: true, name: 'uniq_replication_outbox_event')]
final readonly class ReplicationOutboxResourceModel
{
    public function __construct(
        #[PrimaryKey(strategy: 'auto')]
        #[Column(type: MySqlType::Bigint)]
        public int $id,

        #[Column(type: MySqlType::Char, length: 36)]
        public string $event_id,

        #[Column(type: MySqlType::LongText)]
        public string $payload,

        #[Column(type: MySqlType::Datetime)]
        public \DateTimeImmutable $created_at,
    ) {}
}
