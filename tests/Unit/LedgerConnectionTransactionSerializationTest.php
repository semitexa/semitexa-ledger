<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Swoole\Coroutine;
use Swoole\Coroutine\WaitGroup;

/**
 * One SQLite3 connection is shared by all coroutines in a worker, and its
 * statements yield under SWOOLE_HOOK_ALL. Without an intra-worker mutex, a
 * second coroutine entering transaction() mid-flight opens a nested
 * BEGIN EXCLUSIVE on the SAME connection — "cannot start a transaction within
 * a transaction" — or interleaves a read-modify-write, sharing a sequence
 * number / breaking the prev-hash chain. transaction() must serialise
 * coroutines so each BEGIN..COMMIT completes before the next starts.
 */
final class LedgerConnectionTransactionSerializationTest extends TestCase
{
    protected function setUp(): void
    {
        if (!class_exists(Coroutine::class)) {
            self::markTestSkipped('Swoole extension is required.');
        }
    }

    #[Test]
    public function concurrent_transactions_serialize_on_the_shared_connection(): void
    {
        $errors = [];
        $final = null;

        Coroutine\run(function () use (&$errors, &$final): void {
            $db = new LedgerConnection(':memory:');
            $db->execute('CREATE TABLE seq (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)');
            $db->execute('INSERT INTO seq (id, n) VALUES (1, 0)');

            // Each coroutine reads the counter, YIELDS mid-transaction (the point
            // a nested BEGIN or a lost update would happen), then writes n+1.
            $work = static function () use ($db, &$errors): void {
                try {
                    $db->transaction(static function (LedgerConnection $c): void {
                        $n = (int) $c->fetchScalar('SELECT n FROM seq WHERE id = 1');
                        Coroutine::sleep(0.01); // force a yield inside the transaction
                        $c->execute('UPDATE seq SET n = :n WHERE id = 1', ['n' => $n + 1]);
                    });
                } catch (\Throwable $e) {
                    $errors[] = $e->getMessage();
                }
            };

            $wg = new WaitGroup();
            for ($i = 0; $i < 5; $i++) {
                $wg->add();
                Coroutine::create(static function () use ($work, $wg): void {
                    $work();
                    $wg->done();
                });
            }
            $wg->wait();

            $final = (int) $db->fetchScalar('SELECT n FROM seq WHERE id = 1');
        });

        self::assertSame([], $errors, 'no nested-transaction error on the shared connection');
        self::assertSame(5, $final, 'every read-modify-write applied — none lost to interleaving');
    }
}
