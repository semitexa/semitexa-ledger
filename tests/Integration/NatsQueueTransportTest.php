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
 * The NATS queue must be at-least-once, like the database transport: a job is
 * acked only after its handler returned, so a handler that throws — or a
 * worker that dies — gets the job back instead of losing it.
 *
 * Each run uses its own queue name (subject + durable consumer) on the shared
 * QUEUE stream and deletes the consumer afterwards.
 */
final class NatsQueueTransportTest extends TestCase
{
    use RequiresNats;

    private string $natsUrl;
    private string $queue;

    protected function setUp(): void
    {
        $this->natsUrl = self::reachableNatsUrl();
        $this->queue   = 'it' . bin2hex(random_bytes(4));
    }

    protected function tearDown(): void
    {
        if (isset($this->queue, $this->natsUrl)) {
            Coroutine\run(function (): void {
                $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
                $client->deleteConsumer('QUEUE', NatsTransport::consumerName($this->queue));
            });
        }
    }

    #[Test]
    public function a_job_whose_handler_throws_is_delivered_again_and_a_handled_one_is_not(): void
    {
        $failure = null;
        $seen = [];

        Coroutine\run(function () use (&$failure, &$seen): void {
            try {
                $transport = $this->transport();
                $transport->publish($this->queue, 'job-1');
                $transport->publish($this->queue, 'job-2');

                $throwOnce = true;
                $handler = static function (string $payload) use (&$seen, &$throwOnce): void {
                    $seen[] = $payload;
                    if ($payload === 'job-1' && $throwOnce) {
                        $throwOnce = false;
                        throw new \RuntimeException('handler crashed');
                    }
                };

                $deadline = microtime(true) + 6.0;
                while (count($seen) < 3 && microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, $handler);
                }

                // Nothing is left: job-2 was acked on its first delivery and
                // job-1 on its second — an extra pull delivers nothing.
                $transport->consumeBatch($this->queue, $handler);
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['job-1', 'job-2', 'job-1'], $seen);
        self::assertSame(0, $this->pendingAcks(), 'every handled delivery must have been acked, not left to time out');
    }

    #[Test]
    public function a_job_with_an_empty_body_reaches_the_handler_and_is_acked(): void
    {
        $seen = null;
        $failure = null;

        Coroutine\run(function () use (&$seen, &$failure): void {
            try {
                $transport = $this->transport();
                $transport->publish($this->queue, '');

                $deadline = microtime(true) + 5.0;
                while ($seen === null && microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, static function (string $payload) use (&$seen): void {
                        $seen = $payload;
                    });
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame('', $seen, 'an empty job is a delivery, not a status frame');
        self::assertSame(0, $this->pendingAcks());
    }

    #[Test]
    public function a_queue_consumer_made_before_acks_moved_is_brought_to_the_new_settings(): void
    {
        $settings = null;
        $failure = null;

        Coroutine\run(function () use (&$settings, &$failure): void {
            try {
                $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
                $client->ensureStream('QUEUE', ['subjects' => ['semitexa.queue.>']]);
                // As an earlier release left it: server defaults (30 s, unlimited).
                $client->ensurePullConsumer('QUEUE', NatsTransport::consumerName($this->queue), 'semitexa.queue.' . $this->queue);
                $before = [
                    $client->consumerSetting('QUEUE', NatsTransport::consumerName($this->queue), 'ack_wait'),
                    $client->consumerSetting('QUEUE', NatsTransport::consumerName($this->queue), 'max_deliver'),
                ];

                $this->transport()->consumeBatch($this->queue, static function (): void {});

                $settings = [$before, [
                    $client->consumerSetting('QUEUE', NatsTransport::consumerName($this->queue), 'ack_wait'),
                    $client->consumerSetting('QUEUE', NatsTransport::consumerName($this->queue), 'max_deliver'),
                ]];
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame([30_000_000_000, -1], $settings[0], 'the starting point must be the old defaults');
        self::assertSame([300_000_000_000, 5], $settings[1]);
    }

    private function pendingAcks(): int
    {
        $pending = null;
        Coroutine\run(function () use (&$pending): void {
            $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
            $pending = $client->pendingAcks('QUEUE', NatsTransport::consumerName($this->queue));
        });

        return (int) $pending;
    }

    private function transport(): NatsTransport
    {
        $clusters = new ClusterRegistry();
        $clusters->add(new ClusterConfig(id: 'primary', url: $this->natsUrl));
        $clusters->connect();

        return new NatsTransport($clusters, retryDelaySeconds: 0.5);
    }
}
