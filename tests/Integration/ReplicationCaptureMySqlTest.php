<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\AggregateOwnershipService;
use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Semitexa\Ledger\Application\Service\LedgerSchema;
use Semitexa\Ledger\Application\Service\LedgerWriter;
use Semitexa\Ledger\Application\Service\OwnershipCache;
use Semitexa\Ledger\Application\Service\Replication\ReplicationCaptureService;
use Semitexa\Ledger\Application\Service\Replication\ReplicationRelay;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicatedArticle;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicatedArticleMapper;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicatedArticleResourceModel;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Application\Service\Mapping\MapperRegistry;
use Semitexa\Orm\Application\Service\Persistence\ReplicationCapture;

/**
 * A write to a #[Replicated] resource, through the real ORM engine on MySQL,
 * leaves field clocks and an outbox row in the same transaction; the relay
 * moves the outbox row into the ledger exactly once.
 */
final class ReplicationCaptureMySqlTest extends TestCase
{
    use RequiresReplicationTables;

    private const TABLE = self::ARTICLES;

    private string $ledgerFile;

    /** The resolver in place before the test — a process-global, put back after. */
    private ?\Closure $previousResolver;

    protected function setUp(): void
    {
        $this->previousResolver = ReplicationCapture::resolver();
        $this->connectWithReplicationTables();

        ReplicationCapture::setResolver(static fn (): ReplicationCaptureService => new ReplicationCaptureService('node-a', new HybridLogicalClock()));
        $this->ledgerFile = sys_get_temp_dir() . '/ledger-capture-' . bin2hex(random_bytes(4)) . '.sqlite';
    }

    protected function tearDown(): void
    {
        ReplicationCapture::setResolver($this->previousResolver);
        $this->dropReplicationFixture();
        foreach (isset($this->ledgerFile) ? [$this->ledgerFile, "{$this->ledgerFile}-wal", "{$this->ledgerFile}-shm"] : [] as $f) {
            if (is_file($f)) {
                unlink($f);
            }
        }
    }

    #[Test]
    public function each_write_stamps_the_fields_it_changed_and_carries_the_rest_with_their_clocks(): void
    {
        $engine = $this->orm->getAggregateWriteEngine();
        $article = $engine->insert(new ReplicatedArticle('', 'Title', 'first body'), ReplicatedArticleResourceModel::class, $this->mappers());
        self::assertInstanceOf(ReplicatedArticle::class, $article);
        $engine->update(new ReplicatedArticle($article->id, 'Title', 'second body'), ReplicatedArticleResourceModel::class, $this->mappers());
        $engine->delete(new ReplicatedArticle($article->id, 'Title', 'second body'), ReplicatedArticleResourceModel::class, $this->mappers());

        [$insert, $update, $delete] = $this->outbox();

        // Insert: every field and the row's existence carry the insert's reading.
        self::assertSame($article->id, $insert['pk']);
        self::assertSame('node-a', $insert['node']);
        self::assertTrue($insert['exists']['v']);
        foreach ($insert['fields'] as $field) {
            self::assertSame($insert['hlc'], $field['t']);
        }

        // Update: only the changed field is re-stamped, later than the insert.
        self::assertGreaterThan(0, strcmp($update['hlc'], $insert['hlc']));
        self::assertSame('second body', $update['fields']['body']['v']);
        self::assertSame($update['hlc'], $update['fields']['body']['t']);
        self::assertSame($insert['hlc'], $update['fields']['title']['t'], 'an unchanged field keeps its clock');
        self::assertSame($update['hlc'], $update['exists']['t'], 'a write re-asserts that the row exists');

        // Delete: a tombstone, stamped later still, carrying the last state.
        self::assertFalse($delete['exists']['v']);
        self::assertGreaterThan(0, strcmp($delete['exists']['t'], $update['hlc']));
        self::assertSame('second body', $delete['fields']['body']['v']);

        $clocks = $this->db->execute(
            'SELECT column_name, hlc FROM replication_field_clock WHERE table_name = :t ORDER BY column_name',
            ['t' => self::TABLE],
        )->rows;
        self::assertSame(
            ['__exists' => $delete['exists']['t'], 'body' => $update['hlc'], 'id' => $insert['hlc'], 'title' => $insert['hlc']],
            array_column($clocks, 'hlc', 'column_name'),
        );
    }

    #[Test]
    public function an_update_that_changes_nothing_is_not_an_event(): void
    {
        $engine = $this->orm->getAggregateWriteEngine();
        $article = $engine->insert(new ReplicatedArticle('', 'T', 'B'), ReplicatedArticleResourceModel::class, $this->mappers());
        $engine->update(new ReplicatedArticle($article->id, 'T', 'B'), ReplicatedArticleResourceModel::class, $this->mappers());

        self::assertCount(1, $this->outbox());
    }

    #[Test]
    public function a_rolled_back_write_leaves_neither_clocks_nor_an_outbox_row(): void
    {
        try {
            $this->orm->getTransactionManager()->run(function (): void {
                $this->orm->getAggregateWriteEngine()->insert(new ReplicatedArticle('', 'T', 'B'), ReplicatedArticleResourceModel::class, $this->mappers());
                throw new \RuntimeException('the caller changes its mind');
            });
        } catch (\RuntimeException) {
        }

        self::assertSame([], $this->outbox());
        self::assertSame(0, (int) $this->db->execute('SELECT COUNT(*) AS c FROM replication_field_clock WHERE table_name = :t', ['t' => self::TABLE])->rows[0]['c']);
    }

    #[Test]
    public function the_relay_moves_each_outbox_row_into_the_ledger_exactly_once(): void
    {
        $engine = $this->orm->getAggregateWriteEngine();
        $engine->insert(new ReplicatedArticle('', 'A', '1'), ReplicatedArticleResourceModel::class, $this->mappers());
        $engine->insert(new ReplicatedArticle('', 'B', '2'), ReplicatedArticleResourceModel::class, $this->mappers());
        $pending = $this->db->execute('SELECT event_id, payload FROM replication_outbox ORDER BY id')->rows;

        $ledger = new LedgerConnection($this->ledgerFile);
        (new LedgerSchema($ledger, 'node-a'))->migrate();
        $relay = new ReplicationRelay($this->writer($ledger), fn (): DatabaseAdapterInterface => $this->db);

        self::assertSame(2, $relay->relayBatch());
        self::assertSame([], $this->outbox());

        // A relay that died after appending but before deleting sees the row again.
        $this->db->execute(
            'INSERT INTO replication_outbox (event_id, payload, created_at) VALUES (:e, :p, UTC_TIMESTAMP())',
            ['e' => $pending[0]['event_id'], 'p' => $pending[0]['payload']],
        );
        self::assertSame(1, $relay->relayBatch());

        $events = $ledger->fetchAll("SELECT event_id, domain, event_type, sequence FROM events ORDER BY sequence");
        self::assertCount(2, $events, 'the repeated append must not create a second event');
        self::assertSame(array_column($pending, 'event_id'), array_column($events, 'event_id'));
        self::assertSame(['replication', 'row_changed'], [$events[0]['domain'], $events[0]['event_type']]);
    }

    #[Test]
    public function a_row_that_can_never_be_relayed_is_set_aside_and_the_rows_behind_it_move_on(): void
    {
        $engine = $this->orm->getAggregateWriteEngine();
        $engine->insert(new ReplicatedArticle('', 'A', '1'), ReplicatedArticleResourceModel::class, $this->mappers());
        $this->db->execute(
            "INSERT INTO replication_outbox (event_id, payload, created_at) VALUES ('01a0e900-0000-7000-8000-00000000dead', '{not json', UTC_TIMESTAMP())",
        );
        $engine->insert(new ReplicatedArticle('', 'B', '2'), ReplicatedArticleResourceModel::class, $this->mappers());

        $ledger = new LedgerConnection($this->ledgerFile);
        (new LedgerSchema($ledger, 'node-a'))->migrate();
        $relay = new ReplicationRelay($this->writer($ledger), fn (): DatabaseAdapterInterface => $this->db);

        self::assertSame(3, $relay->relayBatch());
        self::assertSame([], $this->outbox(), 'nothing may stay stuck behind the bad row');
        self::assertSame(2, (int) $ledger->fetchScalar('SELECT COUNT(*) FROM events'));
        self::assertSame(
            [['event_id' => '01a0e900-0000-7000-8000-00000000dead', 'payload' => '{not json']],
            $this->db->execute('SELECT event_id, payload FROM replication_outbox_dead')->rows,
        );
    }

    /** @return list<array<string, mixed>> */
    private function outbox(): array
    {
        return array_map(
            static fn (array $row): array => json_decode((string) $row['payload'], true, 512, JSON_THROW_ON_ERROR),
            $this->db->execute('SELECT payload FROM replication_outbox ORDER BY id')->rows,
        );
    }

    private function writer(LedgerConnection $ledger): LedgerWriter
    {
        return new LedgerWriter($ledger, 'node-a', 'it-key', new AggregateOwnershipService('node-a', $this->db, new OwnershipCache()));
    }

    private function mappers(): MapperRegistry
    {
        $registry = new MapperRegistry();
        $registry->build(mapperClasses: [ReplicatedArticleMapper::class], domainModelClasses: [ReplicatedArticle::class]);

        return $registry;
    }
}
