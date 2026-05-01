<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Contract;

use Semitexa\Ledger\Domain\Model\LedgerEvent;

/**
 * Applies a remote event to the local main database.
 *
 * Implementations MUST be idempotent. The event_id should be used as the
 * idempotency key to guard against double-application during failure recovery.
 *
 * Example pattern (upsert with event_id guard):
 *
 *   $this->db->execute(
 *       'UPDATE stock
 *        SET quantity = quantity + :delta, last_event_id = :eid
 *        WHERE product_id = :pid
 *          AND (last_event_id IS NULL OR last_event_id != :eid)',
 *       ['delta' => $payload['qty'], 'eid' => $event->eventId, 'pid' => $payload['product_id']]
 *   );
 */
interface ReplayHandlerInterface
{
    public function apply(LedgerEvent $event): void;
}
