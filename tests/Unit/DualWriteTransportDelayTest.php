<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Core\Queue\DelayedQueueTransportInterface;
use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Core\Queue\QueueTransportFactoryInterface;
use Semitexa\Core\Queue\QueueTransportRegistry;
use Semitexa\Ledger\Application\Service\Queue\DualWriteTransport;
use Semitexa\Ledger\Application\Service\Queue\DualWriteTransportFactory;

/**
 * EVENTS_TRANSPORT=dual could not hold a message back: a worker slept a
 * retry's delay inside the consumer, and mail dropped a message whose row
 * was not visible yet.
 */
final class DualWriteTransportDelayTest extends TestCase
{
    #[Test]
    public function over_a_delaying_primary_it_holds_messages_back_on_both_sides(): void
    {
        $primary = self::delaying();
        $secondary = self::delaying();
        $dual = DualWriteTransport::of($primary, $secondary);

        self::assertInstanceOf(DelayedQueueTransportInterface::class, $dual);
        $dual->publishDelayed('mail', 'm', 60);
        $dual->publish('mail', 'n');

        self::assertSame([['mail', 'm', 60], ['mail', 'n', 0]], $primary->published);
        self::assertSame([['mail', 'm', 60], ['mail', 'n', 0]], $secondary->published);
    }

    #[Test]
    public function a_secondary_that_cannot_wait_gets_the_message_at_once(): void
    {
        $primary = self::delaying();
        $secondary = self::plain();
        DualWriteTransport::of($primary, $secondary)->publishDelayed('mail', 'm', 60);

        self::assertSame([['mail', 'm', 60]], $primary->published);
        self::assertSame([['mail', 'm']], $secondary->published);
    }

    #[Test]
    public function over_a_primary_that_cannot_wait_it_does_not_claim_to(): void
    {
        self::assertNotInstanceOf(DelayedQueueTransportInterface::class, DualWriteTransport::of(self::plain(), self::delaying()));
    }

    #[Test]
    public function a_failing_secondary_does_not_fail_the_delayed_publish(): void
    {
        $primary = self::delaying();
        $secondary = new class implements DelayedQueueTransportInterface {
            public function publish(string $queueName, string $payload): void { throw new \RuntimeException('down'); }
            public function publishDelayed(string $queueName, string $payload, int $delaySeconds): void { throw new \RuntimeException('down'); }
            public function consume(string $queueName, callable $callback): void {}
        };
        DualWriteTransport::of($primary, $secondary)->publishDelayed('mail', 'm', 5);

        self::assertSame([['mail', 'm', 5]], $primary->published);
    }

    #[Test]
    public function the_factory_takes_the_secondary_it_is_told_and_never_a_second_copy(): void
    {
        $nats = self::delaying();
        $database = self::delaying();
        QueueTransportRegistry::reset();
        try {
            QueueTransportRegistry::register('test-nats', self::factory($nats));
            QueueTransportRegistry::register('test-db', self::factory($database));

            (new DualWriteTransportFactory('test-nats', 'test-db'))->create()->publish('q', 'a');
            self::assertSame([['q', 'a', 0]], $nats->published);
            self::assertSame([['q', 'a', 0]], $database->published, 'EVENTS_DUAL_SECONDARY, not NATS');

            // The same transport twice ran every job twice; an unknown one failed every publish.
            self::assertSame($nats, (new DualWriteTransportFactory('test-nats', 'TEST-NATS'))->create());
            self::assertSame($nats, (new DualWriteTransportFactory('test-nats', 'dual'))->create());
            self::assertSame($nats, (new DualWriteTransportFactory('test-nats', 'no-such'))->create());

            $this->expectException(\InvalidArgumentException::class);
            (new DualWriteTransportFactory('dual', 'test-db'))->create();
        } finally {
            QueueTransportRegistry::reset();
        }
    }

    private static function factory(QueueTransportInterface $transport): QueueTransportFactoryInterface
    {
        return new class($transport) implements QueueTransportFactoryInterface {
            public function __construct(private readonly QueueTransportInterface $transport) {}
            public function create(): QueueTransportInterface { return $this->transport; }
        };
    }

    private static function delaying(): DelayedQueueTransportInterface
    {
        return new class implements DelayedQueueTransportInterface {
            /** @var list<array{string, string, int}> */
            public array $published = [];
            public function publish(string $queueName, string $payload): void { $this->published[] = [$queueName, $payload, 0]; }
            public function publishDelayed(string $queueName, string $payload, int $delaySeconds): void { $this->published[] = [$queueName, $payload, $delaySeconds]; }
            public function consume(string $queueName, callable $callback): void {}
        };
    }

    private static function plain(): QueueTransportInterface
    {
        return new class implements QueueTransportInterface {
            /** @var list<array{string, string}> */
            public array $published = [];
            public function publish(string $queueName, string $payload): void { $this->published[] = [$queueName, $payload]; }
            public function consume(string $queueName, callable $callback): void {}
        };
    }
}
