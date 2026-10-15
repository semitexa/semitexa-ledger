<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Domain\Model\ClusterConfig;
use Swoole\Coroutine;

/**
 * Two NatsClient paths that could not have worked, against a real NATS.
 *
 * subscribe() read the reply subject from Payload, which has none, so a
 * command handler always saw null and never answered its requester; and the
 * library's own auto-reply would have raced an explicit answer with null.
 * ensureStream() called Configuration::setMaxBytesPerSubject(), a method the
 * library does not have, so any stream configured with max_bytes died.
 */
final class NatsClientCommandReplyTest extends TestCase
{
    use RequiresNats;

    private string $natsUrl;

    protected function setUp(): void
    {
        $this->natsUrl = self::reachableNatsUrl();
    }

    #[Test]
    public function a_subscriber_answers_its_requester_through_the_reply_subject(): void
    {
        $subject = 'semitexa.test.reply.' . bin2hex(random_bytes(4));
        $answer = null;
        $seenReplyTo = null;
        $failure = null;

        Coroutine\run(function () use ($subject, &$answer, &$seenReplyTo, &$failure): void {
            $responder = new NatsClient(new ClusterConfig(id: 'responder', url: $this->natsUrl));
            $responder->subscribe($subject, function (string $body, ?string $replyTo) use (&$seenReplyTo): string {
                $seenReplyTo = $replyTo;

                return 'pong:' . $body;
            });
            $done = false;
            Coroutine::create(function () use ($responder, &$done): void {
                while (!$done) {
                    $responder->process(0.05);
                }
            });

            try {
                $requester = new NatsClient(new ClusterConfig(id: 'requester', url: $this->natsUrl));
                $answer = $requester->request($subject, 'ping', 3.0);
                // Exactly one reply: a second, empty one would arrive with no
                // handler left for it and make this next read throw.
                Coroutine::sleep(0.3);
                $requester->process(0.3);
            } catch (\Throwable $e) {
                $failure = $e;
            } finally {
                $done = true;
            }
        });

        self::assertNull($failure, $failure?->getMessage() ?? '');
        self::assertNotNull($seenReplyTo, 'the handler must be told where to answer');
        self::assertSame('pong:ping', $answer);
    }

    #[Test]
    public function a_stream_can_be_capped_by_bytes(): void
    {
        $stream = 'IT_CAP_' . strtoupper(bin2hex(random_bytes(4)));
        $failure = null;
        $maxBytes = null;

        Coroutine\run(function () use ($stream, &$failure, &$maxBytes): void {
            $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
            try {
                $client->ensureStream($stream, ['subjects' => ['it.cap.' . strtolower($stream) . '.>'], 'max_bytes' => 1048576]);
                $raw = (new \ReflectionMethod(NatsClient::class, 'client'))->invoke($client);
                $maxBytes = $raw->getApi()->getStream($stream)->info()->config->max_bytes ?? null;
            } catch (\Throwable $e) {
                $failure = $e;
            } finally {
                try {
                    $client->deleteStream($stream);
                } catch (\Throwable) {
                }
            }
        });

        self::assertNull($failure, $failure?->getMessage() ?? '');
        self::assertSame(1048576, $maxBytes);
    }
}
