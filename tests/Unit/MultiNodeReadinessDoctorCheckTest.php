<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Core\Support\DoctorResult;
use Semitexa\Ledger\Application\Service\MultiNodeReadinessDoctorCheck;

final class MultiNodeReadinessDoctorCheckTest extends TestCase
{
    private const READY = [
        'LEDGER_NODE_ID'  => 'store-a',
        'LEDGER_HMAC_KEY' => 'k7Qx2vN9pL4mR8sT1wY6zA3bC5dE0fG2',
        'REDIS_HOST'      => 'redis',
        'CACHE_DRIVER'    => 'redis',
        'STORAGE_DRIVER'  => 's3',
    ];

    #[Test]
    public function a_node_outside_a_cluster_is_not_judged(): void
    {
        $result = MultiNodeReadinessDoctorCheck::assess(['LEDGER_NODE_ID' => null] + self::READY);

        self::assertSame('skip', $this->statusOf($result));
    }

    #[Test]
    public function the_framework_defaults_fail_a_clustered_node_and_name_every_reason(): void
    {
        $result = MultiNodeReadinessDoctorCheck::assess([
            'LEDGER_NODE_ID'  => 'store-a',
            'LEDGER_HMAC_KEY' => 'short',
            'REDIS_HOST'      => null,
            'CACHE_DRIVER'    => null,
            'STORAGE_DRIVER'  => null,
        ]);

        self::assertSame('fail', $this->statusOf($result));
        $message = $this->text($result);
        self::assertStringContainsString('Swoole Table', $message);
        self::assertStringContainsString('CACHE_DRIVER=array', $message);
        self::assertStringContainsString('LEDGER_HMAC_KEY', $message);
        self::assertStringContainsString('STORAGE_DRIVER=local', $message);
    }

    #[Test]
    public function local_storage_alone_is_a_warning_because_the_volume_may_be_shared(): void
    {
        $result = MultiNodeReadinessDoctorCheck::assess(['STORAGE_DRIVER' => 'local'] + self::READY);

        self::assertSame('warn', $this->statusOf($result));
    }

    #[Test]
    public function a_redis_on_this_machine_is_a_warning_not_a_pass(): void
    {
        $result = MultiNodeReadinessDoctorCheck::assess(['REDIS_HOST' => '127.0.0.1'] + self::READY);

        self::assertSame('warn', $this->statusOf($result));
        self::assertStringContainsString('REDIS_HOST=127.0.0.1 is this machine', $result->message);
    }

    #[Test]
    public function shared_sessions_cache_and_storage_pass(): void
    {
        self::assertSame('pass', $this->statusOf(MultiNodeReadinessDoctorCheck::assess(self::READY)));
    }

    private function statusOf(DoctorResult $result): string
    {
        return $result->status->value;
    }

    private function text(DoctorResult $result): string
    {
        return $result->message . ' ' . ($result->hint ?? '');
    }
}
