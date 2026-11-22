<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Ledger\Application\Service\AggregateOwnershipService;
use Semitexa\Ledger\Application\Service\EnvelopeSeal;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Semitexa\Ledger\Application\Service\LedgerReplayer;
use Semitexa\Ledger\Application\Service\LedgerSchema;
use Semitexa\Ledger\Application\Service\LedgerWriter;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;
use Semitexa\Ledger\Application\Service\Nats\PulledMessage;
use Semitexa\Ledger\Application\Service\OwnershipCache;
use Semitexa\Ledger\Application\Service\ReplayHandlerRegistry;
use Semitexa\Ledger\Domain\Model\LedgerEvent;
use Semitexa\Ledger\Tests\Integration\Fixture\RecordingReplayHandler;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicationProbeRecorded;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;

/**
 * The hash and the HMAC used to cover the event id and the payload only, so a
 * signed event's domain, event type, aggregate and metadata could be rewritten
 * in transit and still pass both checks; and a redelivery of a stored,
 * not-yet-applied event was applied as delivered, unverified.
 *
 * Two ledgers in temporary SQLite files; deliveries are handed to the replayer
 * directly, as NATS would hand them. No NATS, no main database.
 */
final class LedgerEnvelopeTamperTest extends TestCase
{
    private const KEY    = 'unit-shared-secret';
    private const ORIGIN = 'unit-origin';

    /** @var list<string> */
    private array $files = [];

    private LedgerConnection $originDb;
    private LedgerConnection $db;
    private RecordingReplayHandler $handler;
    /** @var list<string> */
    private array $settled = [];

    protected function setUp(): void
    {
        $this->originDb = $this->ledger(self::ORIGIN);
        $this->db       = $this->ledger('unit-receiver');
        $this->handler  = new RecordingReplayHandler();
        $this->settled  = [];
    }

    protected function tearDown(): void
    {
        unset($this->originDb, $this->db);

        foreach ($this->files as $file) {
            foreach ([$file, "{$file}-wal", "{$file}-shm"] as $path) {
                if (is_file($path)) {
                    unlink($path);
                }
            }
        }
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function rewrittenHeaders(): iterable
    {
        yield 'domain'         => [['domain' => 'billing']];
        yield 'event_type'     => [['event_type' => 'grant_admin']];
        yield 'event_version'  => [['event_version' => 7]];
        yield 'aggregate_id'   => [['aggregate_type' => 'account', 'aggregate_id' => 'victim-1']];
        yield 'metadata'       => [['metadata' => ['tenant_id' => 'victim']]];
        yield 'created_at'     => [['created_at' => '2001-01-01T00:00:00.000000Z']];
    }

    /** @param array<string, mixed> $rewrite */
    #[Test]
    #[DataProvider('rewrittenHeaders')]
    public function a_signed_event_with_a_rewritten_header_is_quarantined_and_never_applied(array $rewrite): void
    {
        $signed = $this->writer()->append(ReplicationProbeRecorded::of('p1', 'genuine'));
        self::assertNotNull($signed);

        $this->deliver($this->replayer(), $this->wire($signed, $rewrite));

        self::assertSame(1, $this->rows('quarantined_events'), 'the rewritten envelope is held for review');
        self::assertSame(0, $this->rows('events'), 'and never enters the ledger');
        self::assertSame([], $this->handler->applied);
        self::assertSame(['ack'], $this->settled);
    }

    #[Test]
    public function the_untouched_event_is_stored_and_applied(): void
    {
        $signed = $this->writer()->append(ReplicationProbeRecorded::of('p1', 'genuine'));
        self::assertNotNull($signed);

        $this->deliver($this->replayer(), $signed->toJson());

        self::assertSame(0, $this->rows('quarantined_events'));
        self::assertSame(['p1'], $this->handler->applied);
        self::assertSame(EnvelopeSeal::CURRENT, $this->db->fetchScalar('SELECT hash_version FROM events'));
    }

    #[Test]
    public function a_tampered_redelivery_of_a_stored_unapplied_event_applies_the_stored_event(): void
    {
        $signed = $this->writer()->append(ReplicationProbeRecorded::of('p1', 'genuine'));
        self::assertNotNull($signed);

        // The genuine event has no aggregate: nothing may reach the ownership
        // table, least of all the aggregate the tampered copy names.
        $mainDb = $this->createMock(DatabaseAdapterInterface::class);
        $mainDb->expects(self::never())->method('execute');
        $replayer = $this->replayer($mainDb);

        // First delivery: verified and stored, but its projection fails.
        $this->handler->failNext = 1;
        $this->deliver($replayer, $signed->toJson());
        self::assertSame(['nak'], $this->settled);
        self::assertNull($this->db->fetchScalar('SELECT applied_at FROM events'));

        // The retry arrives rewritten: same id and hash, everything else forged.
        $this->deliver($replayer, $this->wire($signed, [
            'payload'        => ['probeId' => 'evil', 'label' => 'forged', 'attributes' => []],
            'aggregate_type' => 'account',
            'aggregate_id'   => 'victim-1',
            'hmac'           => 'not-even-a-signature',
        ]));

        self::assertSame(['p1'], $this->handler->applied, 'the stored, verified event is what applies');
        self::assertNotNull($this->db->fetchScalar('SELECT applied_at FROM events'));
        self::assertSame(['nak', 'ack'], $this->settled);
    }

    #[Test]
    public function a_new_event_relabelled_as_the_old_format_is_quarantined(): void
    {
        $signed = $this->writer()->append(ReplicationProbeRecorded::of('p1', 'genuine'));
        self::assertNotNull($signed);

        // As an old node would have sent it: no version field at all.
        $unlabelled = json_decode($signed->toJson(), true, 512, JSON_THROW_ON_ERROR);
        self::assertIsArray($unlabelled);
        unset($unlabelled['hash_version']);
        $this->deliver($this->replayer(), json_encode($unlabelled, JSON_THROW_ON_ERROR));

        self::assertSame(1, $this->rows('quarantined_events'));
        self::assertSame(0, $this->rows('events'));
        self::assertSame([], $this->handler->applied);
    }

    #[Test]
    public function an_old_format_signature_does_not_pass_as_a_new_one_nor_an_unknown_version_as_any(): void
    {
        $old = $this->legacyEvent(1, hash('sha256', 'genesis:' . self::ORIGIN), 'p1');

        $replayer = $this->replayer();
        $this->deliver($replayer, $this->wire($old, ['hash_version' => 2]));
        $this->deliver($replayer, $this->wire($old, ['hash_version' => 3]));

        self::assertSame(2, $this->rows('quarantined_events'));
        self::assertSame([], $this->handler->applied);
    }

    #[Test]
    public function old_format_events_still_verify_and_apply_and_a_chain_may_continue_in_the_new_format(): void
    {
        // Signed with the first format's formulas, written out here by hand,
        // and sent without a version field — what a node that predates
        // versions puts on the wire.
        $first  = $this->legacyEvent(1, hash('sha256', 'genesis:' . self::ORIGIN), 'p1');
        $second = $this->legacyEvent(2, $first->hash, 'p2');

        $replayer = $this->replayer();
        foreach ([$first, $second] as $event) {
            $wire = json_decode($event->toJson(), true, 512, JSON_THROW_ON_ERROR);
            self::assertIsArray($wire);
            unset($wire['hash_version']);
            $this->deliver($replayer, json_encode($wire, JSON_THROW_ON_ERROR));
        }

        // The origin upgrades: its own stored history is old-format, its next
        // event is new-format, on the same chain.
        $this->store($this->originDb, $first);
        $this->store($this->originDb, $second);
        $third = $this->writer()->append(ReplicationProbeRecorded::of('p3', 'after the upgrade'));
        self::assertNotNull($third);
        self::assertSame(3, $third->sequence);
        self::assertSame($second->hash, $third->prevHash);
        $this->deliver($replayer, $third->toJson());

        self::assertSame(0, $this->rows('quarantined_events'));
        self::assertSame(['p1', 'p2', 'p3'], $this->handler->applied);

        // And the stored chain verifies row by row, each under its own
        // version — what ledger:verify walks.
        $prev = hash('sha256', 'genesis:' . self::ORIGIN);
        $versions = [];
        foreach ($this->db->fetchAll('SELECT * FROM events ORDER BY sequence') as $row) {
            $stored = LedgerEvent::fromRow($row);
            self::assertTrue(EnvelopeSeal::verifies($stored, $prev, self::KEY), "seq {$stored->sequence} verifies");
            $versions[] = $stored->hashVersion;
            $prev = $stored->hash;
        }
        self::assertSame([1, 1, 2], $versions);
    }

    #[Test]
    public function a_writer_held_at_the_old_version_writes_the_first_format_byte_for_byte(): void
    {
        $event = (new LedgerWriter($this->originDb, self::ORIGIN, self::KEY, $this->ownership(), EnvelopeSeal::LEGACY))
            ->append(ReplicationProbeRecorded::of('p1', 'rolling upgrade'));
        self::assertNotNull($event);

        $genesis = hash('sha256', 'genesis:' . self::ORIGIN);
        self::assertSame(1, $event->hashVersion);
        self::assertSame(hash('sha256', $genesis . $event->eventId . json_encode($event->payload, JSON_THROW_ON_ERROR)), $event->hash);
        self::assertSame(hash_hmac('sha256', $event->hash, self::KEY), $event->hmac);
    }

    #[Test]
    public function a_ledger_created_before_hash_versions_gains_the_column_and_its_rows_read_as_version_one(): void
    {
        $this->store($this->db, $this->legacyEvent(1, hash('sha256', 'genesis:' . self::ORIGIN), 'p1'));
        $this->db->getRaw()->exec('ALTER TABLE events DROP COLUMN hash_version');
        $this->db->execute("UPDATE ledger_meta SET value = '1' WHERE key = 'schema_version'");

        (new LedgerSchema($this->db, 'unit-receiver'))->migrate();

        self::assertSame(1, $this->db->fetchScalar('SELECT hash_version FROM events'));
        self::assertSame('2', $this->db->fetchScalar("SELECT value FROM ledger_meta WHERE key = 'schema_version'"));

        // A second worker that read the old version before the first one
        // migrated runs the same step again.
        $this->db->execute("UPDATE ledger_meta SET value = '1' WHERE key = 'schema_version'");
        (new LedgerSchema($this->db, 'unit-receiver'))->migrate();
        self::assertSame(1, $this->rows('events'));
    }

    // -------------------------------------------------------------------------

    private function ledger(string $nodeId): LedgerConnection
    {
        $file = sys_get_temp_dir() . '/ledger-unit-' . $nodeId . '-' . bin2hex(random_bytes(4)) . '.sqlite';
        $this->files[] = $file;

        $db = new LedgerConnection($file);
        (new LedgerSchema($db, $nodeId))->migrate();

        return $db;
    }

    private function ownership(?DatabaseAdapterInterface $mainDb = null): AggregateOwnershipService
    {
        return new AggregateOwnershipService(
            self::ORIGIN,
            $mainDb ?? $this->createStub(DatabaseAdapterInterface::class),
            new OwnershipCache(),
        );
    }

    private function writer(): LedgerWriter
    {
        return new LedgerWriter($this->originDb, self::ORIGIN, self::KEY, $this->ownership());
    }

    private function replayer(?DatabaseAdapterInterface $mainDb = null): LedgerReplayer
    {
        $discovery = $this->createStub(ClassDiscovery::class);
        $discovery->method('findClassesWithAttribute')->willReturn([RecordingReplayHandler::class]);
        $handler = $this->handler;

        return new LedgerReplayer(
            $this->db,
            'unit-receiver',
            self::KEY,
            new ClusterRegistry(),
            new ReplayHandlerRegistry($discovery),
            $this->ownership($mainDb),
            static fn (string $class): object => $handler,
        );
    }

    private function deliver(LedgerReplayer $replayer, string $body): void
    {
        $message = new PulledMessage(
            $body,
            count($this->settled) + 1,
            function (): void { $this->settled[] = 'ack'; },
            function (float $delay): void { $this->settled[] = 'nak'; },
        );

        (new \ReflectionMethod(LedgerReplayer::class, 'processMessage'))->invoke($replayer, $message, 'primary');
    }

    /**
     * The event as it travels, with some fields rewritten after signing.
     *
     * @param array<string, mixed> $rewrite
     */
    private function wire(LedgerEvent $event, array $rewrite): string
    {
        $envelope = json_decode($event->toJson(), true, 512, JSON_THROW_ON_ERROR);
        self::assertIsArray($envelope);

        return json_encode(array_replace($envelope, $rewrite), JSON_THROW_ON_ERROR);
    }

    /** An event exactly as the first format signed it: id and payload hashed, the hash signed bare. */
    private function legacyEvent(int $sequence, string $prevHash, string $probeId): LedgerEvent
    {
        $eventId = sprintf('00000000-0000-7000-8000-%012d', $sequence);
        $payload = ['probeId' => $probeId, 'label' => 'old format', 'attributes' => ['price' => 12.5]];
        $hash    = hash('sha256', $prevHash . $eventId . json_encode($payload, JSON_THROW_ON_ERROR));

        return new LedgerEvent(
            eventId:       $eventId,
            originNode:    self::ORIGIN,
            sequence:      $sequence,
            domain:        'ledgerit',
            eventType:     'replication_probe_recorded',
            eventVersion:  1,
            payload:       $payload,
            metadata:      ['timestamp' => '2026-01-01T00:00:00.000000Z', 'node_id' => self::ORIGIN],
            hash:          $hash,
            prevHash:      $prevHash,
            hmac:          hash_hmac('sha256', $hash, self::KEY),
            source:        'local',
            createdAt:     '2026-01-01T00:00:00.000000Z',
            appliedAt:     null,
            aggregateType: null,
            aggregateId:   null,
        );
    }

    /** Put an old-format event into a ledger as its own writer once did. */
    private function store(LedgerConnection $db, LedgerEvent $event): void
    {
        $db->execute(
            'INSERT INTO events
             (event_id, origin_node, sequence, domain, event_type, event_version, payload, metadata,
              hash, prev_hash, hmac, source, publish_status, created_at)
             VALUES
             (:event_id, :origin_node, :sequence, :domain, :event_type, :event_version, :payload, :metadata,
              :hash, :prev_hash, :hmac, :source, :publish_status, :created_at)',
            [
                'event_id'       => $event->eventId,
                'origin_node'    => $event->originNode,
                'sequence'       => $event->sequence,
                'domain'         => $event->domain,
                'event_type'     => $event->eventType,
                'event_version'  => $event->eventVersion,
                'payload'        => json_encode($event->payload, JSON_THROW_ON_ERROR),
                'metadata'       => json_encode($event->metadata, JSON_THROW_ON_ERROR),
                'hash'           => $event->hash,
                'prev_hash'      => $event->prevHash,
                'hmac'           => $event->hmac,
                'source'         => 'local',
                'publish_status' => 'published',
                'created_at'     => $event->createdAt,
            ],
        );
        $db->execute(
            'INSERT INTO origin_state (origin_node, last_sequence, last_hash, updated_at)
             VALUES (:node, :seq, :hash, :now)
             ON CONFLICT(origin_node) DO UPDATE SET last_sequence = excluded.last_sequence, last_hash = excluded.last_hash',
            ['node' => $event->originNode, 'seq' => $event->sequence, 'hash' => $event->hash, 'now' => $event->createdAt],
        );
    }

    private function rows(string $table): int
    {
        $rows = $this->db->fetchScalar("SELECT COUNT(*) FROM {$table}");
        self::assertIsInt($rows);

        return $rows;
    }
}
