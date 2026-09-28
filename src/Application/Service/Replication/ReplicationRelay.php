<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Support\StandingCoroutines;
use Semitexa\Ledger\Application\Service\LedgerWriter;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;

/**
 * Moves captured row changes from the MySQL outbox into the SQLite ledger,
 * from where the publisher sends them to the other nodes (ADR 0001).
 *
 * Append first, delete second. A relay that dies in between appends the same
 * event id again on the next pass, which the ledger ignores — so nothing is
 * lost and nothing doubles.
 */
final class ReplicationRelay
{
    private const BATCH      = 100;
    private const BUSY_SLEEP = 0.2;
    private const IDLE_SLEEP = 1.0;

    /**
     * @param \Closure(): DatabaseAdapterInterface $adapter the application database
     */
    public function __construct(
        private readonly LedgerWriter $writer,
        private readonly \Closure $adapter,
    ) {}

    /** Relay up to one batch, oldest first. Returns how many were moved. */
    public function relayBatch(): int
    {
        $db   = ($this->adapter)();
        $rows = $db->execute(
            'SELECT id, event_id, payload FROM replication_outbox ORDER BY id LIMIT ' . self::BATCH,
        )->rows;

        foreach ($rows as $row) {
            $payload = json_decode((string) $row['payload'], true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($payload)) {
                throw new \UnexpectedValueException("Outbox row {$row['id']} holds no event payload.");
            }

            $this->writer->appendRecord(
                (string) $row['event_id'],
                ReplicationCaptureService::EVENT_DOMAIN,
                ReplicationCaptureService::EVENT_TYPE,
                $payload,
            );
            $db->execute('DELETE FROM replication_outbox WHERE id = :id', ['id' => $row['id']]);
        }

        return count($rows);
    }

    public function run(): void
    {
        StandingCoroutines::declare(
            'ledger replication relay',
            'moving captured row changes into the ledger — sleeps between batches, by design',
        );

        while (true) {
            try {
                $moved = StandingCoroutines::busy(fn (): int => $this->relayBatch());
            } catch (\Throwable $e) {
                StaticLoggerBridge::error('ledger', 'Replication relay batch failed', ['error' => $e->getMessage()]);
                $moved = 0;
            }

            \Swoole\Coroutine::sleep($moved > 0 ? self::BUSY_SLEEP : self::IDLE_SLEEP);
        }
    }
}
