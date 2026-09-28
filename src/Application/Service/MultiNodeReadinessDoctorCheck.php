<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Core\Attribute\AsDoctorCheck;
use Semitexa\Core\Contract\DoctorCheckInterface;
use Semitexa\Core\Environment;
use Semitexa\Core\Support\DoctorResult;

/**
 * A node that joins a ledger cluster (LEDGER_NODE_ID set) sits behind a load
 * balancer next to its peers. Several framework defaults keep state inside one
 * process or one host, and each fails quietly there: a login made on node A is
 * unknown to node B, a CSRF token issued by one worker is rejected by the next,
 * an uploaded file exists on one disk only.
 */
#[AsDoctorCheck(name: 'ledger.multi-node', package: 'semitexa/ledger')]
final class MultiNodeReadinessDoctorCheck implements DoctorCheckInterface
{
    private const ENV_KEYS = ['LEDGER_NODE_ID', 'LEDGER_HMAC_KEY', 'REDIS_HOST', 'CACHE_DRIVER', 'STORAGE_DRIVER'];

    /** An HMAC key shorter than this is guessable enough to forge ledger events. */
    private const MIN_HMAC_KEY_LENGTH = 32;

    public function run(): DoctorResult
    {
        $env = [];
        foreach (self::ENV_KEYS as $key) {
            $env[$key] = Environment::getEnvValue($key);
        }

        return self::assess($env);
    }

    /**
     * @param array<string, ?string> $env
     */
    public static function assess(array $env): DoctorResult
    {
        $nodeId = trim((string) ($env['LEDGER_NODE_ID'] ?? ''));
        if ($nodeId === '') {
            return DoctorResult::skip('Single node: LEDGER_NODE_ID is not set, so this node is not part of a ledger cluster.');
        }

        $blocking = [];
        $hints    = [];

        if (trim((string) ($env['REDIS_HOST'] ?? '')) === '') {
            $blocking[] = 'sessions live in a Swoole Table on this host (REDIS_HOST unset) — a login on one node is unknown to the others, and live updates stay on this node';
            $hints[]    = 'set REDIS_HOST';
        }

        $cache = strtolower(trim((string) ($env['CACHE_DRIVER'] ?? ''))) ?: 'array';
        if ($cache === 'array') {
            $blocking[] = "CACHE_DRIVER={$cache} keeps the cache inside each worker — form tokens and invalidations do not reach other nodes";
            $hints[]    = 'set CACHE_DRIVER=redis';
        }

        if (strlen((string) ($env['LEDGER_HMAC_KEY'] ?? '')) < self::MIN_HMAC_KEY_LENGTH) {
            $blocking[] = sprintf('LEDGER_HMAC_KEY is shorter than %d characters', self::MIN_HMAC_KEY_LENGTH);
            $hints[]    = 'use the same long random LEDGER_HMAC_KEY on every node';
        }

        $storage = strtolower(trim((string) ($env['STORAGE_DRIVER'] ?? ''))) ?: 'local';
        $storageNote = $storage === 'local'
            ? 'STORAGE_DRIVER=local keeps uploads on this disk — fine only if var/uploads is a volume every node shares'
            : null;

        if ($blocking !== []) {
            return DoctorResult::fail(
                "Node '{$nodeId}' is not ready to run beside other nodes: " . implode('; ', $blocking)
                    . ($storageNote !== null ? "; {$storageNote}" : '') . '.',
                ucfirst(implode(', ', $hints))
                    . ($storageNote !== null ? ', and STORAGE_DRIVER=s3 unless uploads are on a shared volume' : '') . '.',
            );
        }

        if ($storageNote !== null) {
            return DoctorResult::warn(
                "Node '{$nodeId}': {$storageNote}.",
                'Set STORAGE_DRIVER=s3, or mount var/uploads from shared storage on every node.',
            );
        }

        return DoctorResult::pass("Node '{$nodeId}': sessions, cache and storage are shared between nodes.");
    }
}
