<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Queue\NatsTransport;

/** An ack that failed (the connection dropped) ended the batch and the worker, the rest of the batch unrun. */
final class NatsAckFailureTest extends TestCase
{
    #[Test]
    public function a_failed_ack_or_nak_is_logged_and_the_batch_goes_on(): void
    {
        $transport = (new \ReflectionClass(NatsTransport::class))->newInstanceWithoutConstructor();
        $settle = new \ReflectionMethod(NatsTransport::class, 'settle');
        $replied = [];

        $settle->invoke($transport, static function (): void {
            throw new \RuntimeException('connection reset');
        }, 'ack', 'jobs');
        $settle->invoke($transport, static function () use (&$replied): void {
            $replied[] = 'next';
        }, 'ack', 'jobs');

        self::assertSame(['next'], $replied, 'the next message is still answered');
    }
}
