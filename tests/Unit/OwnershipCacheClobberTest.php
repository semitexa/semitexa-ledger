<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\OwnershipCache;

/**
 * remember()'s loader yields on its DB read, so a concurrent claimOwnership
 * (INSERT-IGNORE then cache->set(nodeId)) can cache the real owner while the
 * loader is suspended. The old code then blindly cached the loader's now-stale
 * result — a resolveOwner() that loaded null just before a concurrent claim
 * overwrote the real owner with null for the whole TTL, so the aggregate read
 * as unowned (AggregateNotFoundException) for ~60s despite the DB row.
 *
 * The interleave is reproduced deterministically here: the loader callback IS
 * the suspension point, so a set() performed inside it models the concurrent
 * claim landing mid-load.
 */
final class OwnershipCacheClobberTest extends TestCase
{
    #[Test]
    public function a_concurrent_claim_during_the_loader_is_not_clobbered_by_a_stale_null(): void
    {
        $cache = new OwnershipCache();

        $result = $cache->remember('agg-1', static function () use ($cache): ?string {
            // A concurrent claimOwnership lands while this loader is "yielded":
            $cache->set('agg-1', 'node-owner');

            // ...and THIS loader read the aggregate as still unowned (pre-claim).
            return null;
        });

        self::assertSame('node-owner', $result, 'the fresh concurrent owner must win, not the stale null');
        self::assertSame(
            'node-owner',
            $cache->remember('agg-1', static fn (): ?string => self::fail('must be a cache hit')),
            'the real owner must remain cached, not the null',
        );
    }

    #[Test]
    public function without_a_concurrent_write_the_loader_result_is_cached(): void
    {
        $cache = new OwnershipCache();

        $calls = 0;
        $first = $cache->remember('agg-2', static function () use (&$calls): ?string {
            $calls++;
            return 'owner-x';
        });
        $second = $cache->remember('agg-2', static function () use (&$calls): ?string {
            $calls++;
            return 'owner-y';
        });

        self::assertSame('owner-x', $first);
        self::assertSame('owner-x', $second, 'second call is a cache hit');
        self::assertSame(1, $calls, 'loader runs once');
    }

    #[Test]
    public function a_null_loader_result_is_cached_when_uncontended(): void
    {
        // A genuinely unowned aggregate stays cached as null (the common path
        // must be untouched by the clobber guard).
        $cache = new OwnershipCache();

        self::assertNull($cache->remember('agg-3', static fn (): ?string => null));
    }
}
