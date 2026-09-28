<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Domain\Model\ClusterConfig;

/**
 * Finds a NATS server with JetStream to run against, or skips the test.
 */
trait RequiresNats
{
    private static function reachableNatsUrl(): string
    {
        // First reachable wins. The dev .env points NATS_URL at 127.0.0.1 for
        // the host, while inside the compose network the server is `nats`.
        $candidates = array_filter([
            getenv('LEDGER_TEST_NATS_URL'),
            getenv('NATS_PRIMARY_URL'),
            'nats://nats:4222',
            getenv('NATS_URL'),
        ]);
        // Reachable AND running JetStream: a plain NATS server accepts the
        // connection and then fails every stream call in the test body.
        $url = null;
        foreach (array_unique($candidates) as $candidate) {
            $parts  = parse_url((string) $candidate);
            $socket = @fsockopen($parts['host'] ?? 'localhost', $parts['port'] ?? 4222, $errno, $errstr, 1.0);
            if ($socket === false) {
                continue;
            }
            fclose($socket);
            if (self::hasJetStream((string) $candidate)) {
                $url = (string) $candidate;
                break;
            }
        }
        if ($url === null) {
            self::markTestSkipped('No reachable NATS with JetStream (tried ' . implode(', ', array_unique($candidates)) . '); set LEDGER_TEST_NATS_URL.');
        }
        return $url;
    }

    private static function hasJetStream(string $url): bool
    {
        try {
            return (new NatsClient(new ClusterConfig(id: 'probe', url: $url)))->hasJetStream();
        } catch (\Throwable) {
            return false;
        }
    }
}
