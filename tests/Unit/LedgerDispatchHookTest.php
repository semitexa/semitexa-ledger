<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Core\Log\LoggerInterface;
use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Ledger\Application\Service\LedgerDispatchHook;

/**
 * The ledger post-dispatch hook is the durable event-store write. On an append
 * failure the domain event is dropped (LedgerPublisher only retries persisted
 * events), so the loss must be surfaced LOUDLY and structured — the old inline
 * hook buried it in a bare error_log(). The hook must also stay NON-FATAL: the
 * ledger is decoupled from the dispatch path, so a ledger hiccup must not fail
 * the user's operation (the EventDispatcher catches per-hook, but the hook must
 * not depend on that to avoid losing the ledger-specific durability signal).
 */
final class LedgerDispatchHookTest extends TestCase
{
    private CapturingLogger $logger;

    protected function setUp(): void
    {
        $this->logger = new CapturingLogger();
        StaticLoggerBridge::set($this->logger);
    }

    protected function tearDown(): void
    {
        StaticLoggerBridge::reset();
    }

    #[Test]
    public function an_append_failure_is_logged_structured_and_does_not_propagate(): void
    {
        $hook = new LedgerDispatchHook(static function (object $event): void {
            throw new \RuntimeException('database is locked');
        });

        // Must NOT throw — the hook is non-fatal by design.
        $hook(new SampleDomainEvent());

        self::assertCount(1, $this->logger->errors, 'the durability loss must be logged, not swallowed');
        [$message, $context] = $this->logger->errors[0];
        self::assertStringContainsString('dropped from the event store', $message);
        self::assertSame(SampleDomainEvent::class, $context['event']);
        self::assertSame(\RuntimeException::class, $context['exception']);
        self::assertSame('database is locked', $context['message']);
        self::assertSame('ledger', $context['_channel']);
    }

    #[Test]
    public function a_successful_append_logs_nothing(): void
    {
        $seen = null;
        $hook = new LedgerDispatchHook(function (object $event) use (&$seen): void {
            $seen = $event;
        });

        $event = new SampleDomainEvent();
        $hook($event);

        self::assertSame($event, $seen, 'the append operation must receive the event');
        self::assertSame([], $this->logger->errors, 'a successful append must not log an error');
    }
}

final class SampleDomainEvent
{
}

final class CapturingLogger implements LoggerInterface
{
    /** @var list<array{0: string, 1: array<string, mixed>}> */
    public array $errors = [];

    public function error(string $message, array $context = []): void
    {
        $this->errors[] = [$message, $context];
    }

    public function critical(string $message, array $context = []): void
    {
    }

    public function warning(string $message, array $context = []): void
    {
    }

    public function info(string $message, array $context = []): void
    {
    }

    public function notice(string $message, array $context = []): void
    {
    }

    public function debug(string $message, array $context = []): void
    {
    }
}
