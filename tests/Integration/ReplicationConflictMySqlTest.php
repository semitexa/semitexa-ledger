<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Replication\ReplicationConflicts;
use Semitexa\Ledger\Application\Service\Replication\RowChangeApplier;
use Semitexa\Ledger\Domain\Model\ConflictRetryOutcome;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Domain\Model\ReplicationConflict;
use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\Exception\ConstraintViolationException;

/**
 * A remote change that breaks a constraint here (ADR 0001 §5): the fields that
 * break it stay as they are and are journaled, everything else still converges,
 * and the change stream is not held up by it.
 */
final class ReplicationConflictMySqlTest extends TestCase
{
    use RequiresReplicationTables;

    private const ACCOUNTS = 'ledger_it_accounts';
    private const ANNA = '01a0e7a0-0000-7000-8000-00000000000a';
    private const BORYS = '01a0e7a0-0000-7000-8000-00000000000b';
    private const CARL = '01a0e7a0-0000-7000-8000-00000000000c';

    protected function setUp(): void
    {
        $this->connectWithReplicationTables();
        $this->db->execute('DROP TABLE IF EXISTS ' . self::ACCOUNTS);
        $this->db->execute(
            'CREATE TABLE ' . self::ACCOUNTS . ' (id CHAR(36) PRIMARY KEY, email VARCHAR(191) NOT NULL, name VARCHAR(191) NOT NULL, '
            . 'stock INT NOT NULL, UNIQUE KEY uniq_it_accounts_email (email), CONSTRAINT chk_it_accounts_stock CHECK (stock >= 0))'
        );
        $this->db->execute("DELETE FROM replication_field_clock WHERE table_name = '" . self::ACCOUNTS . "'");
        $this->db->execute("DELETE FROM replication_tombstone WHERE table_name = '" . self::ACCOUNTS . "'");
    }

    protected function tearDown(): void
    {
        if (isset($this->db)) {
            $this->db->execute("DELETE FROM replication_field_clock WHERE table_name = '" . self::ACCOUNTS . "'");
            $this->db->execute("DELETE FROM replication_tombstone WHERE table_name = '" . self::ACCOUNTS . "'");
            $this->db->execute('DROP TABLE IF EXISTS ' . self::ACCOUNTS);
        }
        $this->dropReplicationFixture();
    }

    #[Test]
    public function an_edit_taking_a_value_that_is_unique_here_applies_every_other_field(): void
    {
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);
        $this->given(self::BORYS, 'borys@x', 'Borys', 5);

        // On the other node Borys took the address Anna holds here, and renamed.
        $conflicts = $this->apply($this->account(self::BORYS, ['shared@x', 200], ['Borys B.', 200], [5, 100]));

        self::assertSame(['email' => 'borys@x', 'name' => 'Borys B.'], $this->emailAndName(self::BORYS), 'the name converged, the address did not');
        self::assertCount(1, $conflicts);
        self::assertSame(['email'], $conflicts[0]->columns);
        self::assertSame('shared@x', $conflicts[0]->incoming['email'] ?? null);
        self::assertSame('borys@x', $conflicts[0]->local['email'] ?? null);
        self::assertSame('node-b', $conflicts[0]->originNode);
        self::assertNull($this->clockOf(self::BORYS, 'email'), 'an unapplied field keeps no clock, so it is tried again later');
        self::assertSame(1, $this->journaled());
    }

    #[Test]
    public function a_conflict_is_journaled_and_announced_once_however_often_it_is_met(): void
    {
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);
        $this->given(self::BORYS, 'borys@x', 'Borys', 5);
        $change = $this->account(self::BORYS, ['shared@x', 200], ['Borys B.', 200], [5, 100]);

        self::assertCount(1, $this->apply($change));
        self::assertSame([], $this->apply($change), 'a replay of the same change');
        self::assertSame(
            [],
            $this->apply($this->account(self::BORYS, ['shared@x', 200], ['Borys C.', 300], [5, 100])),
            'a later change of the row still carrying the same unapplied stamp',
        );
        self::assertSame(1, $this->journaled());
        self::assertSame('Borys C.', $this->emailAndName(self::BORYS)['name'] ?? null);
    }

    #[Test]
    public function once_the_value_is_free_here_the_next_change_of_the_row_applies_it(): void
    {
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);
        $this->given(self::BORYS, 'borys@x', 'Borys', 5);
        $change = $this->account(self::BORYS, ['shared@x', 200], ['Borys B.', 200], [5, 100]);
        $this->apply($change);

        // The resolution: Anna's address is changed, an ordinary write.
        $this->db->execute('UPDATE ' . self::ACCOUNTS . " SET email = 'anna@x' WHERE id = :id", ['id' => self::ANNA]);
        $this->apply($change);

        self::assertSame('shared@x', $this->emailAndName(self::BORYS)['email'] ?? null);
        self::assertNotNull($this->clockOf(self::BORYS, 'email'));
        self::assertSame(0, $this->open(), 'the journal closes the conflict once its field lands');
    }

    #[Test]
    public function a_new_row_taking_a_unique_value_is_not_created_and_the_stream_goes_on(): void
    {
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);

        $conflicts = $this->apply($this->account(self::CARL, ['shared@x', 200], ['Carl', 200], [1, 200]));

        self::assertNull($this->emailAndName(self::CARL), 'a row cannot exist in part');
        self::assertCount(1, $conflicts);
        self::assertSame([], $conflicts[0]->columns, 'the whole row is unapplied');
        self::assertNull($conflicts[0]->local);
        self::assertNull($this->clockOf(self::CARL, '__exists'), 'no clocks, so a later change tries the insert again');

        $this->db->execute('UPDATE ' . self::ACCOUNTS . " SET email = 'anna@x' WHERE id = :id", ['id' => self::ANNA]);
        $this->apply($this->account(self::CARL, ['shared@x', 200], ['Carl', 200], [1, 200]));

        self::assertSame(['email' => 'shared@x', 'name' => 'Carl'], $this->emailAndName(self::CARL));
    }

    #[Test]
    public function a_value_outside_a_check_bound_here_is_left_out_and_the_rest_applied(): void
    {
        $this->given(self::ANNA, 'anna@x', 'Anna', 5);

        $conflicts = $this->apply($this->account(self::ANNA, ['anna@x', 100], ['Anna K.', 200], [-1, 200]));

        self::assertSame(['stock'], $conflicts[0]->columns ?? null);
        self::assertSame('Anna K.', $this->emailAndName(self::ANNA)['name'] ?? null);
        self::assertSame(5, (int) ($this->db->execute('SELECT stock FROM ' . self::ACCOUNTS . ' WHERE id = :id', ['id' => self::ANNA])->rows[0]['stock'] ?? -99));
    }

    #[Test]
    public function a_null_in_a_not_null_column_is_not_a_conflict_and_still_fails_the_apply(): void
    {
        // Not something two nodes can each write legitimately: a schema gap.
        // Journaling it would hide it; failing keeps it retried and visible.
        $this->given(self::ANNA, 'anna@x', 'Anna', 5);

        $this->expectException(ConstraintViolationException::class);
        $this->apply($this->account(self::ANNA, ['anna@x', 100], [null, 200], [5, 100]));
    }

    #[Test]
    public function a_new_row_kept_out_is_created_by_a_retry_once_its_value_is_free(): void
    {
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);
        $conflicts = $this->apply($this->account(self::CARL, ['shared@x', 200], ['Carl', 200], [1, 200]));
        $key = $conflicts[0]->key();

        // Resolved by writing ANOTHER row: nothing about it reaches Carl's change.
        $this->db->execute('UPDATE ' . self::ACCOUNTS . " SET email = 'anna@x' WHERE id = :id", ['id' => self::ANNA]);
        self::assertSame(1, $this->open(), 'a write elsewhere does not bring the row back by itself');

        [$outcome, $new] = $this->retry($key);

        self::assertSame(ConflictRetryOutcome::Resolved, $outcome);
        self::assertSame([], $new);
        self::assertSame(['email' => 'shared@x', 'name' => 'Carl'], $this->emailAndName(self::CARL));
        self::assertNotNull($this->clockOf(self::CARL, '__exists'));
        self::assertSame(0, $this->open());
        self::assertSame(1, $this->journaled(), 'resolved, not forgotten');
        self::assertSame([ConflictRetryOutcome::AlreadyResolved, []], $this->retry($key));
    }

    #[Test]
    public function a_retry_while_the_value_is_still_taken_keeps_it_open_and_quiet(): void
    {
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);
        $this->given(self::BORYS, 'borys@x', 'Borys', 5);
        $key = $this->apply($this->account(self::BORYS, ['shared@x', 200], ['Borys B.', 200], [5, 100]))[0]->key();

        self::assertSame([ConflictRetryOutcome::StillOpen, []], $this->retry($key), 'not announced a second time');
        self::assertSame('borys@x', $this->emailAndName(self::BORYS)['email'] ?? null);
        self::assertSame(1, $this->open());
    }

    #[Test]
    public function a_retry_does_not_undo_a_newer_write_of_the_unapplied_field(): void
    {
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);
        $this->given(self::BORYS, 'borys@x', 'Borys', 5);
        $key = $this->apply($this->account(self::BORYS, ['shared@x', 200], ['Borys B.', 200], [5, 100]))[0]->key();

        // Anna frees the address, and Borys's own address is written again here, later.
        $this->db->execute('UPDATE ' . self::ACCOUNTS . " SET email = 'anna@x' WHERE id = :id", ['id' => self::ANNA]);
        $this->db->execute('UPDATE ' . self::ACCOUNTS . " SET email = 'borys@new' WHERE id = :id", ['id' => self::BORYS]);
        $this->db->execute(
            'INSERT INTO replication_field_clock (table_name, row_pk, column_name, hlc, node) VALUES (:t, :pk, :c, :h, :n)',
            ['t' => self::ACCOUNTS, 'pk' => self::BORYS, 'c' => 'email', 'h' => (new HlcTimestamp(900, 0))->toString(), 'n' => 'node-a'],
        );

        self::assertSame([ConflictRetryOutcome::Resolved, []], $this->retry($key), 'overtaken counts as resolved');
        self::assertSame('borys@new', $this->emailAndName(self::BORYS)['email'] ?? null, 'the newer write stands');
        self::assertSame(0, $this->open());
    }

    #[Test]
    public function a_retry_of_a_key_that_is_not_journaled_or_kept_nothing_applies_nothing(): void
    {
        self::assertSame([ConflictRetryOutcome::Unknown, []], $this->retry(str_repeat('0', 40)));

        // Journaled before the change itself was kept.
        $this->given(self::ANNA, 'shared@x', 'Anna', 5);
        $key = $this->apply($this->account(self::CARL, ['shared@x', 200], ['Carl', 200], [1, 200]))[0]->key();
        $this->db->execute('UPDATE replication_conflict SET payload = NULL WHERE conflict_key = :k', ['k' => $key]);
        $this->db->execute('UPDATE ' . self::ACCOUNTS . " SET email = 'anna@x' WHERE id = :id", ['id' => self::ANNA]);

        self::assertSame([ConflictRetryOutcome::NotRetryable, []], $this->retry($key));
        self::assertNull($this->emailAndName(self::CARL));
    }

    /**
     * A row written on this node before replication knew of it: no clocks, so
     * any remote stamp beats its fields.
     */
    private function given(string $id, string $email, string $name, int $stock): void
    {
        $this->db->execute(
            'INSERT INTO ' . self::ACCOUNTS . ' (id, email, name, stock) VALUES (:id, :e, :n, :s)',
            ['id' => $id, 'e' => $email, 'n' => $name, 's' => $stock],
        );
    }

    /**
     * @param array{0: string, 1: int} $email [value, ms]
     * @param array{0: ?string, 1: int} $name
     * @param array{0: int, 1: int} $stock
     * @return array<string, mixed>
     */
    private function account(string $id, array $email, array $name, array $stock): array
    {
        $stamp = static fn (int $ms): string => (new HlcTimestamp($ms, 0))->toString();
        $latest = max($email[1], $name[1], $stock[1]);

        return [
            'table'     => self::ACCOUNTS,
            'pk_column' => 'id',
            'pk'        => $id,
            'hlc'       => $stamp($latest),
            'node'      => 'node-b',
            'exists'    => ['v' => true, 't' => $stamp($latest), 'n' => 'node-b'],
            'fields'    => [
                'id'    => ['v' => $id, 't' => $stamp(1), 'n' => 'node-b'],
                'email' => ['v' => $email[0], 't' => $stamp($email[1]), 'n' => 'node-b'],
                'name'  => ['v' => $name[0], 't' => $stamp($name[1]), 'n' => 'node-b'],
                'stock' => ['v' => $stock[0], 't' => $stamp($stock[1]), 'n' => 'node-b'],
            ],
        ];
    }

    /**
     * @param array<string, mixed> $change
     * @return list<ReplicationConflict>
     */
    private function apply(array $change): array
    {
        $applier = new RowChangeApplier([self::ACCOUNTS => 'id']);

        /** @var list<ReplicationConflict> */
        return $this->orm->getTransactionManager()->run(static fn (DatabaseAdapterInterface $db): array => $applier->apply($change, $db));
    }

    /** @return array{email: string, name: string}|null */
    private function emailAndName(string $id): ?array
    {
        /** @var array{email: string, name: string}|null */
        return $this->db->execute('SELECT email, name FROM ' . self::ACCOUNTS . ' WHERE id = :id', ['id' => $id])->rows[0] ?? null;
    }

    private function clockOf(string $id, string $column): ?string
    {
        $row = $this->db->execute(
            'SELECT hlc FROM replication_field_clock WHERE table_name = :t AND row_pk = :pk AND column_name = :c',
            ['t' => self::ACCOUNTS, 'pk' => $id, 'c' => $column],
        )->rows[0] ?? null;

        return $row === null ? null : (string) $row['hlc'];
    }

    /** @return array{0: ConflictRetryOutcome, 1: list<ReplicationConflict>} */
    private function retry(string $key): array
    {
        $applier = new RowChangeApplier([self::ACCOUNTS => 'id']);

        /** @var array{0: ConflictRetryOutcome, 1: list<ReplicationConflict>} */
        return $this->orm->getTransactionManager()->run(
            static fn (DatabaseAdapterInterface $db): array => ReplicationConflicts::retryOn($db, $applier, $key),
        );
    }

    private function open(): int
    {
        return (int) ($this->db->execute("SELECT COUNT(*) AS n FROM replication_conflict WHERE resolved_at IS NULL AND table_name = '" . self::ACCOUNTS . "'")->rows[0]['n'] ?? 0);
    }

    private function journaled(): int
    {
        return (int) ($this->db->execute("SELECT COUNT(*) AS n FROM replication_conflict WHERE table_name = '" . self::ACCOUNTS . "'")->rows[0]['n'] ?? 0);
    }
}
