<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Nats\PulledMessage;

/**
 * The replayer records how far into the stream it has consumed from the
 * delivery's own reply subject; every JetStream ack-subject shape must yield
 * the stream sequence, and anything else must yield 0 rather than a wrong one.
 */
final class PulledMessageStreamSequenceTest extends TestCase
{
    /** @return iterable<string, array{?string, int}> */
    public static function replySubjects(): iterable
    {
        yield 'legacy 9-token form'    => ['$JS.ACK.EVENTS.node-b.1.42.7.1727500000000000000.0', 42];
        yield 'domain + account form'  => ['$JS.ACK.hub.ACCHASH.EVENTS.node-b.1.42.7.1727500000000000000.0', 42];
        yield 'with trailing token'    => ['$JS.ACK.hub.ACCHASH.EVENTS.node-b.1.42.7.1727500000000000000.0.xyz', 42];
        yield 'not an ack subject'     => ['_INBOX.abc', 0];
        yield 'no reply subject'       => [null, 0];
        yield 'unexpected token count' => ['$JS.ACK.EVENTS.node-b.1.42', 0];
    }

    /** @return iterable<string, array{?string, int}> */
    public static function deliveryCounts(): iterable
    {
        yield 'legacy 9-token form, third delivery' => ['$JS.ACK.EVENTS.node-b.3.42.7.1727500000000000000.0', 3];
        yield 'domain + account form, fifth delivery' => ['$JS.ACK.hub.ACCHASH.QUEUE.queue-x.5.42.7.1727500000000000000.0.xyz', 5];
        yield 'not an ack subject' => ['_INBOX.abc', 0];
        yield 'no reply subject' => [null, 0];
    }

    #[Test]
    #[DataProvider('deliveryCounts')]
    public function reads_the_delivery_count_from_the_reply_subject(?string $replyTo, int $expected): void
    {
        self::assertSame($expected, PulledMessage::deliveryCountFromReplyTo($replyTo));
    }

    #[Test]
    #[DataProvider('replySubjects')]
    public function reads_the_stream_sequence_from_the_reply_subject(?string $replyTo, int $expected): void
    {
        self::assertSame($expected, PulledMessage::streamSequenceFromReplyTo($replyTo));
    }
}
