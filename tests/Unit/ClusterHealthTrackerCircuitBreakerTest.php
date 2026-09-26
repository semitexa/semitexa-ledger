<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Nats\ClusterHealthTracker;

/**
 * isHealthy() used to compute its cooldown as "now minus last_success", but a
 * cluster that has NEVER had a recorded success defaults last_success to 0.0
 * (the Unix epoch). Once such a cluster hit UNHEALTHY_THRESHOLD failures, the
 * "elapsed" time was always decades, so `$elapsed >= RECHECK_INTERVAL_SECONDS`
 * was always true and the breaker never actually opened — every subsequent
 * call to isHealthy() reported healthy again, immediately.
 *
 * The fix tracks the moment the breaker tripped (the failure that reached the
 * threshold) and gates the cooldown on that instead.
 */
final class ClusterHealthTrackerCircuitBreakerTest extends TestCase
{
    #[Test]
    public function a_cluster_with_no_prior_success_opens_the_breaker_after_the_threshold(): void
    {
        $tracker = new ClusterHealthTracker();

        // Never a recordSuccess() for this cluster — last_success stays at its
        // 0.0 default, which is exactly the case the bug mishandled.
        $tracker->recordFailure('never-succeeded');
        $tracker->recordFailure('never-succeeded');
        self::assertTrue($tracker->isHealthy('never-succeeded'), 'below the threshold, still healthy');

        $tracker->recordFailure('never-succeeded');

        self::assertFalse(
            $tracker->isHealthy('never-succeeded'),
            'the 3rd consecutive failure must open the breaker even with no recorded success',
        );
        self::assertSame(3, $tracker->getFailureCount('never-succeeded'));
    }

    #[Test]
    public function the_breaker_stays_open_across_repeated_checks_until_the_cooldown_elapses(): void
    {
        $tracker = new ClusterHealthTracker();

        $tracker->recordFailure('flaky');
        $tracker->recordFailure('flaky');
        $tracker->recordFailure('flaky');

        // Immediately re-checking must not flip it back to healthy — that was
        // the observable symptom of the bug (breaker "opens" for one instant
        // and is already closed again on the very next call).
        self::assertFalse($tracker->isHealthy('flaky'));
        self::assertFalse($tracker->isHealthy('flaky'));
    }

    #[Test]
    public function a_cluster_with_a_prior_success_still_re_probes_after_the_cooldown(): void
    {
        // Guards against a regression that would anchor the cooldown on
        // opened_at unconditionally in a way that breaks the previously
        // correct "recovered" path once a success interleaves with failures.
        $now = 1_000.0;
        $tracker = new ClusterHealthTracker(static function () use (&$now): float {
            return $now;
        });

        $tracker->recordSuccess('recovering');
        $tracker->recordFailure('recovering');
        $tracker->recordFailure('recovering');
        $tracker->recordFailure('recovering');

        self::assertFalse($tracker->isHealthy('recovering'));

        $now += 29.0;
        self::assertFalse($tracker->isHealthy('recovering'), 'still inside the 30s cooldown');

        $now += 1.0;
        self::assertTrue($tracker->isHealthy('recovering'), 'the cooldown elapsed: the cluster is re-probed');
    }

    #[Test]
    public function a_failed_re_probe_starts_a_new_cooldown(): void
    {
        $now = 1_000.0;
        $tracker = new ClusterHealthTracker(static function () use (&$now): float {
            return $now;
        });

        $tracker->recordFailure('down');
        $tracker->recordFailure('down');
        $tracker->recordFailure('down');

        $now += 30.0;
        self::assertTrue($tracker->isHealthy('down'), 're-probe permitted');

        // The re-probe fails: the breaker must close the door for another
        // full cooldown instead of letting every later attempt through.
        $tracker->recordFailure('down');
        self::assertFalse($tracker->isHealthy('down'));

        $now += 29.0;
        self::assertFalse($tracker->isHealthy('down'));

        $now += 1.0;
        self::assertTrue($tracker->isHealthy('down'));
    }

    #[Test]
    public function a_failure_recorded_while_the_breaker_is_open_does_not_extend_the_cooldown(): void
    {
        $now = 1_000.0;
        $tracker = new ClusterHealthTracker(static function () use (&$now): float {
            return $now;
        });

        $tracker->recordFailure('busy');
        $tracker->recordFailure('busy');
        $tracker->recordFailure('busy');

        // An attempt that was already in flight when the breaker opened.
        $now += 10.0;
        $tracker->recordFailure('busy');

        $now += 20.0;
        self::assertTrue(
            $tracker->isHealthy('busy'),
            'the cooldown is counted from when the breaker opened, not from the in-flight failure',
        );
    }
}
