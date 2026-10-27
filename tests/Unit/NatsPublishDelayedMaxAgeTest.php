<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Queue\NatsTransport;

/** The stream deletes a message NATS_QUEUE_MAX_AGE_DAYS after it is stored: one held back longer was lost. */
final class NatsPublishDelayedMaxAgeTest extends TestCase
{
    #[Test]
    public function a_delay_the_stream_would_outlive_is_refused_before_anything_is_sent(): void
    {
        $previous = getenv('NATS_QUEUE_MAX_AGE_DAYS');
        putenv('NATS_QUEUE_MAX_AGE_DAYS=1');
        try {
            $transport = (new \ReflectionClass(NatsTransport::class))->newInstanceWithoutConstructor();
            $this->expectException(\InvalidArgumentException::class);
            $this->expectExceptionMessage('NATS_QUEUE_MAX_AGE_DAYS');
            $transport->publishDelayed('jobs', '{}', 86_400);
        } finally {
            putenv($previous === false ? 'NATS_QUEUE_MAX_AGE_DAYS' : 'NATS_QUEUE_MAX_AGE_DAYS=' . $previous);
        }
    }
}
