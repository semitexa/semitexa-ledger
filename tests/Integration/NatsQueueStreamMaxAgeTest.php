<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Application\Service\Nats\StreamMaxAge;
use Semitexa\Ledger\Application\Service\Queue\NatsTransport;
use Semitexa\Ledger\Domain\Model\ClusterConfig;

/**
 * The queue stream kept every message ever queued (no max age): a production
 * disk filled until it was full. A stream made before a max age existed is
 * given one, and nothing else about it changes.
 */
final class NatsQueueStreamMaxAgeTest extends TestCase
{
    use RequiresNats;

    private const STREAM = 'MAXAGE_PROBE';

    #[Test]
    public function an_existing_stream_gets_its_max_age_and_keeps_the_rest(): void
    {
        $client = new NatsClient(new ClusterConfig(id: 'admin', url: self::reachableNatsUrl()));
        $stream = $client->jetStream()->getStream(self::STREAM);
        if ($stream->exists()) {
            $stream->delete();
        }

        try {
            $client->ensureStream(self::STREAM, ['subjects' => ['maxage.probe.>'], 'duplicate_window' => 60]);
            self::assertSame(0, (int) $stream->info()->config->max_age, 'made without a max age, as the queue stream was');

            $fourteenDays = NatsTransport::maxAgeNanoSeconds(null);
            self::assertTrue(StreamMaxAge::ensure($client, self::STREAM, $fourteenDays));
            self::assertFalse(StreamMaxAge::ensure($client, self::STREAM, $fourteenDays), 'already set: nothing to change');

            $config = $stream->info()->config;
            self::assertSame($fourteenDays, (int) $config->max_age);
            self::assertSame(['maxage.probe.>'], $config->subjects);
            self::assertSame(60_000_000_000, (int) $config->duplicate_window, 'the rest of the configuration is kept');
        } finally {
            if ($stream->exists()) {
                $stream->delete();
            }
        }
    }

    /**
     * The library's own stream Configuration knows a part of the fields: an
     * update built from it reset max_msgs, compression and allow_direct to
     * their defaults. Every field but max_age must come back as it was.
     */
    #[Test]
    public function a_hand_tuned_stream_keeps_every_other_field(): void
    {
        $client = new NatsClient(new ClusterConfig(id: 'admin', url: self::reachableNatsUrl()));
        $stream = $client->jetStream()->getStream(self::STREAM);
        if ($stream->exists()) {
            $stream->delete();
        }

        try {
            $stream->client->api('STREAM.CREATE.' . self::STREAM, [
                'name' => self::STREAM,
                'subjects' => ['maxage.probe.>'],
                'max_msgs' => 1000,
                'compression' => 's2',
                'allow_direct' => true,
                'deny_delete' => true,
                'duplicate_window' => 120_000_000_000,
            ]);
            $before = (array) $stream->info()->config;

            $fourteenDays = NatsTransport::maxAgeNanoSeconds(null);
            self::assertTrue(StreamMaxAge::ensure($client, self::STREAM, $fourteenDays));
            $after = (array) $stream->info()->config;
            self::assertSame($fourteenDays, (int) $after['max_age']);
            unset($before['max_age'], $after['max_age']);
            self::assertEquals($before, $after, 'nothing but the max age changed');
            self::assertSame(1000, (int) $after['max_msgs']);
            self::assertTrue($after['deny_delete'], 'the queue relies on no message being deleted');

            self::assertTrue(StreamMaxAge::ensure($client, self::STREAM, 0), 'and back to keeping every message');
            self::assertSame(0, (int) $stream->info()->config->max_age);
        } finally {
            if ($stream->exists()) {
                $stream->delete();
            }
        }
    }

    /** A stream the server will not update this way: the refusal reaches the caller, which logs it. */
    #[Test]
    public function a_refused_update_throws(): void
    {
        $client = new NatsClient(new ClusterConfig(id: 'admin', url: self::reachableNatsUrl()));
        $stream = $client->jetStream()->getStream(self::STREAM);
        if ($stream->exists()) {
            $stream->delete();
        }

        try {
            $client->ensureStream(self::STREAM, ['subjects' => ['maxage.probe.>'], 'duplicate_window' => 120]);

            // The server will not keep messages for less than the window it
            // checks duplicates in (JSStreamMaxAgeDuplicateWindowErr).
            try {
                StreamMaxAge::ensure($client, self::STREAM, 60_000_000_000);
                $refusal = null;
            } catch (\Exception $e) {
                $refusal = [$e->getCode(), $e->getMessage()];
            }
            self::assertSame([10052, 'duplicates window can not be larger then max age'], $refusal);
            self::assertSame(0, (int) $stream->info()->config->max_age, 'a refused update changes nothing');
        } finally {
            if ($stream->exists()) {
                $stream->delete();
            }
        }
    }

    #[Test]
    public function the_max_age_is_fourteen_days_unless_set_to_whole_days(): void
    {
        $day = 86_400 * 1_000_000_000;
        self::assertSame(14 * $day, NatsTransport::maxAgeNanoSeconds(null));
        self::assertSame(14 * $day, NatsTransport::maxAgeNanoSeconds(' '));
        self::assertSame(3 * $day, NatsTransport::maxAgeNanoSeconds(' 3 '));
        self::assertSame(0, NatsTransport::maxAgeNanoSeconds('0'), '0 keeps every message');
        self::assertSame(14 * $day, NatsTransport::maxAgeNanoSeconds('-5'), 'not "forever": the opposite of what was meant');
        self::assertSame(14 * $day, NatsTransport::maxAgeNanoSeconds('two'));
        self::assertSame(14 * $day, NatsTransport::maxAgeNanoSeconds('1.5'));
        self::assertSame(36_500 * $day, NatsTransport::maxAgeNanoSeconds('9999999'), 'capped below an int overflow');
    }
}
