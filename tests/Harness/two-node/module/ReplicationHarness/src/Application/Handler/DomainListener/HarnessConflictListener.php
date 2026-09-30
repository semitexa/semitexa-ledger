<?php

declare(strict_types=1);

namespace Semitexa\Modules\ReplicationHarness\Application\Handler\DomainListener;

use Semitexa\Core\Attribute\AsEventListener;
use Semitexa\Core\Event\EventExecution;
use Semitexa\Ledger\Application\Payload\Event\ReplicationConflictDetected;

/**
 * Proves the replay handler announces a conflict: one line per event, in the
 * node's own /tmp, which `harness:note conflicts` reads back.
 */
#[AsEventListener(event: ReplicationConflictDetected::class, execution: EventExecution::Sync)]
final class HarnessConflictListener
{
    public const LOG = '/tmp/harness-conflicts.log';

    public function handle(ReplicationConflictDetected $event): void
    {
        $conflict = $event->getConflict();
        if ($conflict === null) {
            return;
        }

        file_put_contents(
            self::LOG,
            json_encode(['table' => $conflict->table, 'row' => $conflict->rowKey, 'columns' => $conflict->columns], JSON_THROW_ON_ERROR) . "\n",
            FILE_APPEND | LOCK_EX,
        );
    }
}
