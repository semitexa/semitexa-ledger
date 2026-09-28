<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Ledger\Domain\Model\RowChangePayload;
use Semitexa\Core\Support\Row;
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

    /**
     * Relay up to one batch, oldest first. Returns how many rows left the
     * outbox (moved to the ledger, or dead-lettered).
     *
     * A row that can never be relayed — its payload does not decode, or is not
     * an event — goes to replication_outbox_dead, so it cannot hold back the
     * changes behind it. Any other failure (the ledger file busy, say) stops
     * the batch at that row and leaves it in place: order is kept and the next
     * pass retries it.
     */
    public function relayBatch(): int
    {
        $db   = ($this->adapter)();
        $rows = $db->execute(
            'SELECT id, event_id, payload FROM replication_outbox ORDER BY id LIMIT ' . self::BATCH,
        )->rows;

        $moved = 0;
        foreach ($rows as $raw) {
            $row = Row::of($raw);
            $id  = $row->int('id');

            try {
                $payload = json_decode($row->string('payload'), true, 512, JSON_THROW_ON_ERROR);
                RowChangePayload::fromArray($payload); // the shape the peers will need
            } catch (\JsonException|\UnexpectedValueException $e) {
                $this->deadLetter($db, $id, $row->string('event_id'), $row->string('payload'), $e);
                $moved++;
                continue;
            }
            /** @var array<string, mixed> $payload checked by RowChangePayload::fromArray() */

            try {
                $this->writer->appendRecord(
                    $row->string('event_id'),
                    ReplicationCaptureService::EVENT_DOMAIN,
                    ReplicationCaptureService::EVENT_TYPE,
                    $payload,
                );
            } catch (\Throwable $e) {
                throw new \RuntimeException(
                    sprintf('Relaying outbox row %d (event %s) failed: %s', $id, $row->string('event_id'), $e->getMessage()),
                    0,
                    $e,
                );
            }
            $db->execute('DELETE FROM replication_outbox WHERE id = :id', ['id' => $id]);
            $moved++;
        }

        return $moved;
    }

    private function deadLetter(DatabaseAdapterInterface $db, int $id, string $eventId, string $payload, \Throwable $reason): void
    {
        $db->execute(
            'INSERT IGNORE INTO replication_outbox_dead (event_id, payload, error, failed_at) VALUES (:e, :p, :r, :at)',
            ['e' => $eventId, 'p' => $payload, 'r' => mb_substr($reason->getMessage(), 0, 500), 'at' => gmdate('Y-m-d H:i:s')],
        );
        $db->execute('DELETE FROM replication_outbox WHERE id = :id', ['id' => $id]);

        StaticLoggerBridge::error('ledger', 'Outbox row can never be relayed; moved to replication_outbox_dead', [
            'outbox_id' => $id,
            'event_id'  => $eventId,
            'error'     => $reason->getMessage(),
        ]);
    }

    private bool $running = true;

    /** End run() after the current pass. */
    public function stop(): void
    {
        $this->running = false;
    }

    public function run(): void
    {
        StandingCoroutines::declare(
            'ledger replication relay',
            'moving captured row changes into the ledger — sleeps between batches, by design',
        );

        while ($this->running) {
            try {
                $moved = StandingCoroutines::busy(fn (): int => $this->relayBatch());
            } catch (\Throwable $e) {
                // The failing row stays at the head of the outbox and is retried
                // every pass; while it fails, none of this node's later changes
                // leave it. The message names the row — a failure that repeats
                // here is replication from this node standing still.
                StaticLoggerBridge::error('ledger', 'Replication relay stalled on an outbox row', [
                    'error' => $e->getMessage(),
                    'class' => get_class($e->getPrevious() ?? $e),
                ]);
                $moved = 0;
            }

            \Swoole\Coroutine::sleep($moved > 0 ? self::BUSY_SLEEP : self::IDLE_SLEEP);
        }
    }
}
