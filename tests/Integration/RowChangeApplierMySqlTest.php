<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Application\Service\Replication\ReplicationCaptureService;
use Semitexa\Ledger\Application\Service\Replication\RowChangeApplier;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Exception\ClockDriftException;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicatedArticle;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicatedArticleMapper;
use Semitexa\Ledger\Tests\Integration\Fixture\ReplicatedArticleResourceModel;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Application\Service\Mapping\MapperRegistry;
use Semitexa\Orm\Application\Service\Persistence\ReplicationCapture;

/**
 * Field-level merge of replicated row changes (ADR 0001), on MySQL.
 *
 * One database plays one node at a time: a scenario captures changes as they
 * happen on "node A", then wipes the data and replays them as the change
 * stream would deliver them to a node that has not seen them.
 */
final class RowChangeApplierMySqlTest extends TestCase
{
    use RequiresReplicationTables;

    private const TABLE = self::ARTICLES;
    private const ID = '01a0e7a0-0000-7000-8000-000000000001';

    protected function setUp(): void
    {
        $this->connectWithReplicationTables();
    }

    protected function tearDown(): void
    {
        ReplicationCapture::setResolver(null);
        $this->dropReplicationFixture();
    }

    #[Test]
    public function a_node_that_missed_everything_rebuilds_the_row_and_a_repeat_changes_nothing(): void
    {
        [$insert, $update] = $this->captureOn('node-a', function ($engine, $m): void {
            $a = $engine->insert(new ReplicatedArticle(self::ID, 'Title', 'v1'), ReplicatedArticleResourceModel::class, $m);
            $engine->update(new ReplicatedArticle($a->id, 'Title', 'v2'), ReplicatedArticleResourceModel::class, $m);
        });

        $this->applyAll([$insert, $update, $insert, $update]);

        self::assertSame(['id' => self::ID, 'title' => 'Title', 'body' => 'v2'], $this->row());
    }

    #[Test]
    public function an_edit_that_arrives_before_the_insert_still_ends_in_the_right_row(): void
    {
        [$insert, $update] = $this->captureOn('node-a', function ($engine, $m): void {
            $a = $engine->insert(new ReplicatedArticle(self::ID, 'Title', 'v1'), ReplicatedArticleResourceModel::class, $m);
            $engine->update(new ReplicatedArticle($a->id, 'Title', 'v2'), ReplicatedArticleResourceModel::class, $m);
        });

        $this->applyAll([$update, $insert]);

        self::assertSame(['id' => self::ID, 'title' => 'Title', 'body' => 'v2'], $this->row());
    }

    #[Test]
    public function concurrent_edits_of_different_fields_both_survive(): void
    {
        $base = $this->event(['title' => ['T0', 100, 'node-a'], 'body' => ['B0', 100, 'node-a']], [true, 100, 'node-a']);
        $titleOnA = $this->event(['title' => ['T-from-A', 200, 'node-a'], 'body' => ['B0', 100, 'node-a']], [true, 200, 'node-a']);
        $bodyOnB  = $this->event(['title' => ['T0', 100, 'node-a'], 'body' => ['B-from-B', 210, 'node-b']], [true, 210, 'node-b']);

        foreach ([[$base, $titleOnA, $bodyOnB], [$bodyOnB, $titleOnA, $base]] as $order) {
            $this->wipe();
            $this->applyAll($order);
            self::assertSame(['id' => self::ID, 'title' => 'T-from-A', 'body' => 'B-from-B'], $this->row());
        }
    }

    #[Test]
    public function for_the_same_field_the_later_clock_wins_and_a_tie_goes_to_the_higher_node(): void
    {
        $early = $this->event(['title' => ['early', 300, 'node-z'], 'body' => ['b', 100, 'node-a']], [true, 300, 'node-z']);
        $late  = $this->event(['title' => ['late', 301, 'node-a'], 'body' => ['b', 100, 'node-a']], [true, 301, 'node-a']);
        $tieA  = $this->event(['title' => ['tie-a', 400, 'node-a'], 'body' => ['b', 100, 'node-a']], [true, 400, 'node-a']);
        $tieB  = $this->event(['title' => ['tie-b', 400, 'node-b'], 'body' => ['b', 100, 'node-a']], [true, 400, 'node-b']);

        $this->applyAll([$late, $early]);
        self::assertSame('late', $this->row()['title'] ?? null);

        $this->applyAll([$tieB, $tieA]);
        self::assertSame('tie-b', $this->row()['title'] ?? null);
    }

    #[Test]
    public function a_delete_beats_an_older_edit_and_a_newer_edit_brings_the_row_back_with_its_last_values(): void
    {
        $create = $this->event(['title' => ['T', 100, 'node-a'], 'body' => ['B', 100, 'node-a']], [true, 100, 'node-a']);
        $delete = $this->event(['title' => ['T', 100, 'node-a'], 'body' => ['B', 100, 'node-a']], [false, 300, 'node-a']);
        $olderEdit = $this->event(['title' => ['T-old', 200, 'node-b'], 'body' => ['B', 100, 'node-a']], [true, 200, 'node-b']);
        $newerEdit = $this->event(['title' => ['T', 100, 'node-a'], 'body' => ['B-new', 400, 'node-c']], [true, 400, 'node-c']);

        $this->applyAll([$create, $delete, $olderEdit]);
        self::assertNull($this->row(), 'an edit older than the delete does not bring the row back');

        $this->applyAll([$newerEdit]);
        self::assertSame(
            ['id' => self::ID, 'title' => 'T-old', 'body' => 'B-new'],
            $this->row(),
            'back, with the newest value of every field — including the one written while it was deleted',
        );
    }

    #[Test]
    public function any_delivery_order_of_the_same_changes_ends_in_the_same_state(): void
    {
        mt_srand(28092026);
        for ($round = 0; $round < 25; $round++) {
            $events = [];
            for ($i = 0, $n = mt_rand(2, 7); $i < $n; $i++) {
                $events[] = $this->event(
                    [
                        'title' => ['t' . mt_rand(0, 9), mt_rand(1, 50), 'node-' . mt_rand(1, 3)],
                        'body'  => ['b' . mt_rand(0, 9), mt_rand(1, 50), 'node-' . mt_rand(1, 3)],
                    ],
                    [mt_rand(0, 3) > 0, mt_rand(1, 50), 'node-' . mt_rand(1, 3)],
                );
            }

            $states = [];
            foreach ([$events, array_reverse($events), $this->shuffled($events)] as $order) {
                $this->wipe();
                $this->applyAll($order);
                $states[] = $this->logicalState();
            }

            self::assertSame($states[0], $states[1], "round {$round}: reversed order diverged");
            self::assertSame($states[0], $states[2], "round {$round}: shuffled order diverged");
        }
    }

    #[Test]
    public function a_column_this_node_lacks_is_skipped_and_its_clock_not_kept_so_a_later_replay_applies_it(): void
    {
        $event = $this->event(
            ['title' => ['T', 100, 'node-a'], 'body' => ['B', 100, 'node-a'], 'subtitle' => ['S', 100, 'node-a']],
            [true, 100, 'node-a'],
        );

        $this->applyAll([$event]);

        self::assertSame(['id' => self::ID, 'title' => 'T', 'body' => 'B'], $this->row());
        self::assertSame(0, (int) $this->db->execute("SELECT COUNT(*) AS c FROM replication_field_clock WHERE column_name = 'subtitle'")->rows[0]['c']);
    }

    #[Test]
    public function a_change_from_a_clock_far_ahead_is_refused_until_time_catches_up(): void
    {
        $future = (int) floor(microtime(true) * 1000) + 3_600_000;
        $event = $this->event(['title' => ['T', 1, 'node-a'], 'body' => ['B', 1, 'node-a']], [true, 1, 'node-a']);
        $event['hlc'] = (new HlcTimestamp($future, 0))->toString();

        $this->expectException(ClockDriftException::class);
        (new RowChangeApplier())->apply($event, $this->db);
    }

    // -------------------------------------------------------------------------

    /**
     * Run $scenario with node $node capturing, and return its outbox payloads.
     *
     * @return list<array<string, mixed>>
     */
    private function captureOn(string $node, \Closure $scenario): array
    {
        ReplicationCapture::setResolver(static fn (): ReplicationCaptureService => new ReplicationCaptureService($node, new HybridLogicalClock()));
        $mappers = new MapperRegistry();
        $mappers->build(mapperClasses: [ReplicatedArticleMapper::class], domainModelClasses: [ReplicatedArticle::class]);
        $scenario($this->orm->getAggregateWriteEngine(), $mappers);
        ReplicationCapture::setResolver(null);

        $payloads = array_map(
            static fn (array $r): array => json_decode((string) $r['payload'], true, 512, JSON_THROW_ON_ERROR),
            $this->db->execute('SELECT payload FROM replication_outbox ORDER BY id')->rows,
        );
        $this->db->execute('DELETE FROM replication_outbox');
        $this->wipe();

        return $payloads;
    }

    /**
     * @param array<string, array{0: string, 1: int, 2: string}> $fields column => [value, clock ms, node]
     * @param array{0: bool, 1: int, 2: string}                  $exists
     * @return array<string, mixed>
     */
    private function event(array $fields, array $exists): array
    {
        $stamp = static fn (int $ms): string => (new HlcTimestamp($ms, 0))->toString();
        $encoded = ['id' => ['v' => self::ID, 't' => $stamp(1), 'n' => 'node-a']];
        foreach ($fields as $column => [$value, $ms, $node]) {
            $encoded[$column] = ['v' => $value, 't' => $stamp($ms), 'n' => $node];
        }

        return [
            'table'     => self::TABLE,
            'pk_column' => 'id',
            'pk'        => self::ID,
            'hlc'       => $stamp(max(array_merge(array_column($fields, 1), [$exists[1]]))),
            'node'      => $exists[2],
            'exists'    => ['v' => $exists[0], 't' => $stamp($exists[1]), 'n' => $exists[2]],
            'fields'    => $encoded,
        ];
    }

    /** @param list<array<string, mixed>> $events */
    private function applyAll(array $events): void
    {
        $applier = new RowChangeApplier();
        foreach ($events as $event) {
            $this->orm->getTransactionManager()->run(static fn (DatabaseAdapterInterface $db) => $applier->apply($event, $db));
        }
    }

    /** @return array<string, mixed>|null */
    private function row(): ?array
    {
        return $this->db->execute('SELECT id, title, body FROM ' . self::TABLE . ' WHERE id = :id', ['id' => self::ID])->rows[0] ?? null;
    }

    /**
     * What a node "knows" about the row, however it is stored: whether it
     * exists, every field's winning value (from the row, or the tombstone
     * while deleted) and every field's clock.
     *
     * @return array<string, mixed>
     */
    private function logicalState(): array
    {
        $row = $this->row();
        $tomb = $this->db->execute('SELECT image FROM replication_tombstone WHERE table_name = :t', ['t' => self::TABLE])->rows[0]['image'] ?? null;
        $values = $row ?? ($tomb !== null ? json_decode((string) $tomb, true) : []);
        ksort($values);
        $clocks = $this->db->execute(
            'SELECT column_name, CONCAT(hlc, "@", node) AS c FROM replication_field_clock WHERE table_name = :t ORDER BY column_name',
            ['t' => self::TABLE],
        )->rows;

        return ['exists' => $row !== null, 'values' => $values, 'clocks' => array_column($clocks, 'c', 'column_name')];
    }

    /** @param list<array<string, mixed>> $events @return list<array<string, mixed>> */
    private function shuffled(array $events): array
    {
        for ($i = count($events) - 1; $i > 0; $i--) {
            $j = mt_rand(0, $i);
            [$events[$i], $events[$j]] = [$events[$j], $events[$i]];
        }

        return $events;
    }

    private function wipe(): void
    {
        $this->wipeReplicationState();
    }
}
