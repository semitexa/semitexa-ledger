<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Ledger\Application\Service\AggregateOwnershipService;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Semitexa\Ledger\Application\Service\LedgerPublisher;
use Semitexa\Ledger\Application\Service\LedgerReplayer;
use Semitexa\Ledger\Application\Service\LedgerSchema;
use Semitexa\Ledger\Application\Service\LedgerWriter;
use Semitexa\Ledger\Application\Service\Nats\ClusterHealthTracker;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;
use Semitexa\Ledger\Application\Service\Nats\EventStream;
use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Application\Service\OwnershipCache;
use Semitexa\Ledger\Application\Service\ReplayHandlerRegistry;
use Semitexa\Ledger\Domain\Model\ClusterConfig;
use Semitexa\Ledger\Tests\Integration\Fixture\PropertyOnlyEvent;
use Semitexa\Ledger\Tests\Integration\Fixture\RecordingReplayHandler;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicationProbeRecorded;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Swoole\Coroutine;

/**
 * Two ledger nodes — separate SQLite ledgers, separate NATS connections — and
 * one real JetStream server between them. An event written on node A must be
 * stored AND applied on node B, in order, exactly once.
 *
 * Needs a reachable NATS with JetStream (the dev stack's `nats` service).
 * Every run uses its own stream and subject namespace and deletes the stream
 * afterwards, so it never touches a stream a running app uses.
 */
final class TwoNodeReplicationTest extends TestCase
{
    use RequiresNats;

    private const HMAC_KEY = 'it-shared-secret';

    private string $natsUrl;
    private EventStream $stream;
    /** @var list<string> */
    private array $files = [];

    protected function setUp(): void
    {
        $this->natsUrl = self::reachableNatsUrl();

        $run = bin2hex(random_bytes(4));
        $this->stream = new EventStream("LEDGER_IT_{$run}", "semitexa.it{$run}.events");
    }

    protected function tearDown(): void
    {
        if (isset($this->stream)) {
            Coroutine\run(function (): void {
                $this->client()->deleteStream($this->stream->name);
            });
        }

        foreach ($this->files as $file) {
            foreach ([$file, "{$file}-wal", "{$file}-shm"] as $path) {
                if (is_file($path)) {
                    unlink($path);
                }
            }
        }
    }

    #[Test]
    public function events_written_on_one_node_are_stored_and_applied_on_the_other_in_order(): void
    {
        $this->inCoroutine(function (): void {
            $a = $this->node('a');
            $b = $this->node('b');

            $a->writer->append(ReplicationProbeRecorded::of('p1', 'first'));
            $a->writer->append(ReplicationProbeRecorded::of('p2', 'ünïcode / slash', ['price' => 12.5, 'tags' => ['x', 'y']]));
            $a->writer->append(ReplicationProbeRecorded::of('p3', 'third', ['nested' => ['deep' => true]]));

            self::assertSame(3, $a->publisher->publishBatch());
            self::assertSame(
                [1, 2, 3],
                array_map('intval', array_column($a->db->fetchAll('SELECT nats_sequence FROM publish_log ORDER BY nats_sequence'), 'nats_sequence')),
                'every publish must record the stream sequence JetStream acknowledged',
            );

            $this->drain($b, until: fn (): bool => count($b->handler->applied) >= 3);

            self::assertSame(['p1', 'p2', 'p3'], $b->handler->applied);
            self::assertSame(
                3,
                (int) $b->db->fetchScalar("SELECT COUNT(*) FROM events WHERE origin_node = :o AND source = 'remote' AND applied_at IS NOT NULL", ['o' => $a->nodeId]),
            );
            self::assertSame(0, (int) $b->db->fetchScalar('SELECT COUNT(*) FROM quarantined_events'), 'payloads must survive the round trip hash-intact');
            self::assertSame(3, (int) $b->db->fetchScalar('SELECT last_nats_sequence FROM consumer_state'));

            // A's own replayer sees its own events and skips them.
            $this->drain($a, until: fn (): bool => (int) $a->db->fetchScalar('SELECT COALESCE(MAX(last_nats_sequence), 0) FROM consumer_state') >= 3);
            self::assertSame([], $a->handler->applied);
        });
    }

    #[Test]
    public function a_republished_event_is_deduplicated_by_the_stream(): void
    {
        $this->inCoroutine(function (): void {
            $a = $this->node('a');
            $a->writer->append(ReplicationProbeRecorded::of('p1', 'once'));
            self::assertSame(1, $a->publisher->publishBatch());

            // Simulate a publisher that crashed after the send but before it
            // recorded the publish: the same event goes out a second time.
            $a->db->execute("UPDATE events SET publish_status = 'pending'");
            $a->db->execute('DELETE FROM publish_log');
            self::assertSame(1, $a->publisher->publishBatch());

            $info = $this->client()->streamMessageCount($this->stream->name);
            self::assertSame(1, $info, 'Nats-Msg-Id must reach the server so the stream drops the repeat');
        });
    }

    #[Test]
    public function an_event_whose_apply_fails_is_retried_until_it_applies(): void
    {
        $this->inCoroutine(function (): void {
            $a = $this->node('a');
            $b = $this->node('b');
            $b->handler->failNext = 1;

            $a->writer->append(ReplicationProbeRecorded::of('p1', 'flaky projection'));
            $a->publisher->publishBatch();

            $this->drain($b, until: fn (): bool => $b->handler->applied !== [], seconds: 8.0);

            self::assertSame(['p1'], $b->handler->applied);
            self::assertNotNull($b->db->fetchScalar('SELECT applied_at FROM events WHERE origin_node = :o', ['o' => $a->nodeId]));
        });
    }

    #[Test]
    public function an_event_that_would_propagate_without_its_data_is_refused(): void
    {
        $this->inCoroutine(function (): void {
            $a = $this->node('a');

            try {
                $a->writer->append(new PropertyOnlyEvent('x1'));
                self::fail('an event whose data sits in public properties must not propagate empty');
            } catch (\LogicException $e) {
                self::assertStringContainsString('empty payload', $e->getMessage());
            }

            self::assertSame(0, (int) $a->db->fetchScalar('SELECT COUNT(*) FROM events'));
        });
    }

    // -------------------------------------------------------------------------

    private function node(string $name): object
    {
        $nodeId = "it-{$name}-" . bin2hex(random_bytes(3));
        $file   = sys_get_temp_dir() . "/ledger-it-{$nodeId}.sqlite";
        $this->files[] = $file;

        $db = new LedgerConnection($file);
        (new LedgerSchema($db, $nodeId))->migrate();

        $clusters = new ClusterRegistry();
        $clusters->add(new ClusterConfig(id: 'primary', url: $this->natsUrl));
        $clusters->connect();

        $ownership = new AggregateOwnershipService($nodeId, $this->createStub(DatabaseAdapterInterface::class), new OwnershipCache());

        $discovery = $this->createStub(ClassDiscovery::class);
        $discovery->method('findClassesWithAttribute')->willReturn([RecordingReplayHandler::class]);
        $handler = new RecordingReplayHandler();

        return new class (
            $nodeId,
            $db,
            $handler,
            $clusters->getAll()[0]['client'],
            new LedgerWriter($db, $nodeId, self::HMAC_KEY, $ownership),
            new LedgerPublisher($db, $nodeId, $clusters, new ClusterHealthTracker(), $this->stream),
            new LedgerReplayer(
                $db,
                $nodeId,
                self::HMAC_KEY,
                $clusters,
                new ReplayHandlerRegistry($discovery),
                $ownership,
                static fn (string $class): object => $handler,
                $this->stream,
            ),
        ) {
            public function __construct(
                public readonly string $nodeId,
                public readonly LedgerConnection $db,
                public readonly RecordingReplayHandler $handler,
                public readonly NatsClient $client,
                public readonly LedgerWriter $writer,
                public readonly LedgerPublisher $publisher,
                public readonly LedgerReplayer $replayer,
            ) {}
        };
    }

    private function drain(object $node, \Closure $until, float $seconds = 5.0): void
    {
        $deadline = microtime(true) + $seconds;
        while (!$until() && microtime(true) < $deadline) {
            $node->replayer->pullAndProcess($node->client, 'primary');
        }
    }

    /**
     * Run inside a coroutine and rethrow outside it: an assertion failure
     * thrown inside Coroutine\run() is a fatal error, not a test failure.
     */
    private function inCoroutine(\Closure $body): void
    {
        $failure = null;
        Coroutine\run(static function () use ($body, &$failure): void {
            try {
                $body();
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }
    }

    private function client(): NatsClient
    {
        $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
        $client->connect();

        return $client;
    }
}
