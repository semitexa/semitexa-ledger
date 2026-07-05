<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Core\Server\Lifecycle\ServerLifecycleContext;
use Semitexa\Ledger\Application\Service\LedgerBootstrap;

/**
 * The WorkerStartAfterContainer lifecycle phase can re-run (the OS listeners
 * carry a re-arm guard for exactly this reason). LedgerBootstrap must boot the
 * ledger ONCE per worker: a second run would add a duplicate post-dispatch hook
 * to the long-lived EventDispatcher (double-appending every event, unbounded
 * hook growth) and spawn a second set of background coroutines. This pins the
 * static booted guard without standing up the real SQLite/NATS/coroutine boot.
 */
final class LedgerBootstrapIdempotencyTest extends TestCase
{
    protected function setUp(): void
    {
        LedgerBootstrap::reset();
    }

    protected function tearDown(): void
    {
        LedgerBootstrap::reset();
        putenv('LEDGER_ENABLED');
    }

    #[Test]
    public function boots_exactly_once_even_when_the_phase_reruns(): void
    {
        putenv('LEDGER_ENABLED=1');
        $bootstrap = new CountingLedgerBootstrap();
        $ctx = $this->context();

        $bootstrap->handle($ctx);
        $bootstrap->handle($ctx);
        $bootstrap->handle($ctx);

        self::assertSame(1, $bootstrap->bootCount, 'a phase re-run must not re-boot the ledger');
    }

    #[Test]
    public function reset_allows_a_fresh_boot(): void
    {
        putenv('LEDGER_ENABLED=1');
        $bootstrap = new CountingLedgerBootstrap();
        $ctx = $this->context();

        $bootstrap->handle($ctx);
        LedgerBootstrap::reset();
        $bootstrap->handle($ctx);

        self::assertSame(2, $bootstrap->bootCount);
    }

    #[Test]
    public function does_not_boot_when_the_ledger_is_disabled(): void
    {
        putenv('LEDGER_ENABLED=0');
        $bootstrap = new CountingLedgerBootstrap();

        $bootstrap->handle($this->context());

        self::assertSame(0, $bootstrap->bootCount);
    }

    private function context(): ServerLifecycleContext
    {
        // boot() is overridden to ignore the context, and the guard never reads
        // it — so we don't need to stand up a real Swoole\Http\Server.
        return (new \ReflectionClass(ServerLifecycleContext::class))->newInstanceWithoutConstructor();
    }
}

final class CountingLedgerBootstrap extends LedgerBootstrap
{
    public int $bootCount = 0;

    protected function boot(ServerLifecycleContext $context): void
    {
        $this->bootCount++;
    }
}
