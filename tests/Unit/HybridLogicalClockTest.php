<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Domain\Model\HlcTimestamp;
use Semitexa\Ledger\Exception\ClockDriftException;

final class HybridLogicalClockTest extends TestCase
{
    private int $wall = 1_000_000;

    #[Test]
    public function readings_never_go_backwards_even_when_the_wall_clock_does(): void
    {
        $clock = $this->clock();

        $first = $clock->now();
        $this->wall -= 5_000; // NTP steps the clock back
        $second = $clock->now();
        $third = $clock->now();

        self::assertGreaterThan(0, $second->compareTo($first));
        self::assertGreaterThan(0, $third->compareTo($second));
    }

    #[Test]
    public function a_reading_after_an_observed_one_is_later_than_it(): void
    {
        $clock = $this->clock();
        $seen = new HlcTimestamp($this->wall + 30_000, 42); // a peer whose clock runs ahead

        self::assertGreaterThan(0, $clock->after($seen)->compareTo($seen));
        self::assertGreaterThan(0, $clock->now()->compareTo($seen), 'later local readings stay after it too');
    }

    #[Test]
    public function the_clock_follows_wall_time_once_it_catches_up(): void
    {
        $clock = $this->clock();
        $clock->observe(new HlcTimestamp($this->wall + 10, 7));

        $this->wall += 1_000;
        $reading = $clock->now();

        self::assertSame($this->wall, $reading->wallMs);
        self::assertSame(0, $reading->counter);
    }

    #[Test]
    public function a_reading_too_far_ahead_is_refused_and_leaves_the_clock_untouched(): void
    {
        $clock = $this->clock(maxDriftMs: 60_000);
        $before = $clock->now();

        try {
            $clock->observe(new HlcTimestamp($this->wall + 60_001, 0));
            self::fail('a clock more than the allowed drift ahead must be refused');
        } catch (ClockDriftException) {
        }

        $after = $clock->now();
        self::assertSame($before->wallMs, $after->wallMs, 'a refused reading must not drag the clock forward');
    }

    #[Test]
    public function a_million_readings_in_one_millisecond_borrow_the_next(): void
    {
        $clock = $this->clock();
        $clock->observe(new HlcTimestamp($this->wall, HlcTimestamp::MAX_COUNTER));

        $reading = $clock->now();

        self::assertSame($this->wall + 1, $reading->wallMs);
        self::assertSame(0, $reading->counter);
    }

    #[Test]
    public function the_string_form_sorts_exactly_like_the_readings(): void
    {
        mt_srand(20260928);
        for ($i = 0; $i < 2_000; $i++) {
            $a = new HlcTimestamp(mt_rand(0, 9_999_999_999), mt_rand(0, HlcTimestamp::MAX_COUNTER));
            $b = new HlcTimestamp(mt_rand(0, 9_999_999_999), mt_rand(0, HlcTimestamp::MAX_COUNTER));

            self::assertSame($a->compareTo($b) <=> 0, strcmp($a->toString(), $b->toString()) <=> 0);
            self::assertSame(0, HlcTimestamp::fromString($a->toString())->compareTo($a));
        }
    }

    #[Test]
    public function equal_readings_are_decided_by_node_id_the_same_way_on_every_node(): void
    {
        $t = new HlcTimestamp(5, 1);

        self::assertTrue($t->wins('node-b', $t, 'node-a'));
        self::assertFalse($t->wins('node-a', $t, 'node-b'));
        self::assertTrue((new HlcTimestamp(5, 2))->wins('node-a', $t, 'node-b'), 'the clock decides before the node id');
    }

    private function clock(int $maxDriftMs = HybridLogicalClock::DEFAULT_MAX_DRIFT_MS): HybridLogicalClock
    {
        return new HybridLogicalClock(fn (): int => $this->wall, $maxDriftMs);
    }
}
