<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;
use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Application\Service\Queue\NatsTransport;
use Semitexa\Ledger\Domain\Model\ClusterConfig;
use Swoole\Coroutine;

/**
 * Two apps on one NATS server each read only their own jobs
 * (tk-nats-subject-namespace).
 *
 * The queue stream and its subject prefix were fixed (QUEUE, semitexa.queue.),
 * so a second app on the same server (an E2E instance next to the developer's)
 * published into the stream the first one's workers read: its jobs ran against
 * the developer's database. NATS_QUEUE_STREAM and NATS_QUEUE_SUBJECT_PREFIX give
 * each app its own; the default is today's names.
 */
final class NatsQueueNamespaceTest extends TestCase
{
    use RequiresNats;

    private string $natsUrl;
    private string $queue;
    private string $stream;

    protected function setUp(): void
    {
        $this->natsUrl = self::reachableNatsUrl();
        $id = bin2hex(random_bytes(4));
        $this->queue = 'ns' . $id;
        $this->stream = 'QUEUE_NS_' . strtoupper($id);
    }

    protected function tearDown(): void
    {
        if (isset($this->natsUrl, $this->stream)) {
            Coroutine\run(function (): void {
                $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
                $client->deleteConsumer(NatsTransport::DEFAULT_STREAM, NatsTransport::consumerName($this->queue));
                $client->deleteStream($this->stream);
            });
        }
    }

    #[Test]
    public function a_job_from_one_app_is_not_read_by_the_other_apps_worker(): void
    {
        $seen = ['default' => [], 'e2e' => []];
        $failure = null;

        Coroutine\run(function () use (&$seen, &$failure): void {
            try {
                $default = $this->transport(null, null);
                $e2e = $this->transport($this->stream, 'e2e' . substr($this->stream, -8) . '.queue.');
                // The same queue name in both apps: only the namespace keeps them apart.
                $e2e->publish($this->queue, 'from-e2e');
                $default->publish($this->queue, 'from-default');

                $deadline = microtime(true) + 5.0;
                while (($seen['default'] === [] || $seen['e2e'] === []) && microtime(true) < $deadline) {
                    $default->consumeBatch($this->queue, static function (string $payload) use (&$seen): void {
                        $seen['default'][] = $payload;
                    });
                    $e2e->consumeBatch($this->queue, static function (string $payload) use (&$seen): void {
                        $seen['e2e'][] = $payload;
                    });
                }
                // One more round each: a job the other app published would show up now.
                $default->consumeBatch($this->queue, static function (string $payload) use (&$seen): void {
                    $seen['default'][] = $payload;
                });
                $e2e->consumeBatch($this->queue, static function (string $payload) use (&$seen): void {
                    $seen['e2e'][] = $payload;
                });
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['default' => ['from-default'], 'e2e' => ['from-e2e']], $seen);
    }

    private function transport(?string $stream, ?string $prefix): NatsTransport
    {
        $clusters = new ClusterRegistry();
        $clusters->add(new ClusterConfig(id: 'primary', url: $this->natsUrl));
        $clusters->connect();

        return new NatsTransport($clusters, retryDelaySeconds: 0.5, stream: $stream, subjectPrefix: $prefix);
    }
}
