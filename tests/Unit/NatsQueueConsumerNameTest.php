<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Queue\NatsTransport;

/**
 * The names a queue's NATS consumer can have (tk-nats-consumer-name-collision).
 * The mapped name is the one every consumer has always had; a queue whose
 * mapped name holds another queue's subject reads through a name of its own,
 * which no other queue can map to.
 */
final class NatsQueueConsumerNameTest extends TestCase
{
    #[Test]
    public function the_first_name_is_the_one_consumers_always_had(): void
    {
        self::assertSame('queue-mail', NatsTransport::consumerName('mail'));
        self::assertSame('queue-mail', NatsTransport::consumerName('Mail'), 'the mapping joins these two');
        self::assertSame('queue-media-thumbs', NatsTransport::consumerName('media.thumbs'));
        self::assertSame('queue-media-thumbs', NatsTransport::consumerName('media-thumbs'));
    }

    #[Test]
    public function an_own_name_is_one_no_other_queue_maps_to(): void
    {
        $names = array_map(NatsTransport::ownConsumerName(...), ['mail', 'Mail', 'MAIL', 'media-thumbs', 'media.thumbs', 'media_thumbs']);

        self::assertSame($names, array_values(array_unique($names)));
        self::assertSame('queue-media-thumbs_' . substr(hash('sha256', 'media.thumbs'), 0, 12), NatsTransport::ownConsumerName('media.thumbs'));
        // The mapping turns '_' into '-', so an own name (with its '_') is never another queue's first name.
        self::assertSame('queue-a-b', NatsTransport::consumerName('a_b'));
    }

    /** NATS refuses a consumer name with '.', '*', '>', whitespace or a path separator. */
    #[Test]
    public function every_name_is_one_nats_accepts(): void
    {
        foreach (['demo.notifications', 'a b', 'x/y', 'w*>', 'Ünï'] as $queue) {
            self::assertMatchesRegularExpression('/^queue-[a-z0-9-]+$/', NatsTransport::consumerName($queue));
            self::assertMatchesRegularExpression('/^queue-[a-z0-9-]+_[0-9a-f]{12}$/', NatsTransport::ownConsumerName($queue));
        }
    }
}
