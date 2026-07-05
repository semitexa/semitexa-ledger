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

    #[Test]
    public function the_real_boot_fails_fast_when_the_context_has_no_container(): void
    {
        // The composition root now reads the container from the lifecycle context
        // instead of the static ContainerFactory; a post-container phase always
        // carries it, but if it is somehow absent boot() must fail fast (before
        // touching any SQLite/NATS infrastructure) rather than run half-wired.
        putenv('LEDGER_ENABLED=1');
        $bootstrap = new LedgerBootstrap();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('requires the application container');
        $bootstrap->handle($this->context(container: null));
    }

    private function context(?object $container = null): ServerLifecycleContext
    {
        // boot() is overridden in the guard tests to ignore the context; for the
        // real-boot test we set `container` explicitly. Building via reflection
        // avoids standing up a real Swoole\Http\Server.
        $ctx = (new \ReflectionClass(ServerLifecycleContext::class))->newInstanceWithoutConstructor();
        $prop = new \ReflectionProperty(ServerLifecycleContext::class, 'container');
        $prop->setValue($ctx, $container);

        return $ctx;
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
