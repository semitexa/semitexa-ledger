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
 * Outbox rows the relay could never move: a payload that does not decode or is
 * not an event. Retrying cannot fix them, and left at the head of the outbox
 * they would hold back every later change of this node. Kept here, with the
 * reason, for an operator to inspect.
 */
#[FromTable(name: 'replication_outbox_dead')]
#[Connection('default')]
#[Index(columns: ['event_id'], unique: true, name: 'uniq_replication_outbox_dead_event')]
final readonly class ReplicationOutboxDeadResourceModel
{
    public function __construct(
        #[PrimaryKey(strategy: 'auto')]
        #[Column(type: MySqlType::Bigint)]
        public int $id,

        #[Column(type: MySqlType::Varchar, length: 36)]
        public string $event_id,

        #[Column(type: MySqlType::LongText)]
        public string $payload,

        #[Column(type: MySqlType::Varchar, length: 500)]
        public string $error,

        #[Column(type: MySqlType::Datetime)]
        public \DateTimeImmutable $failed_at,
    ) {}
}
