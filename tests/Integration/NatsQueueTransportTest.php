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

    /** @var list<string> further consumers a test made, deleted with the queue's own */
    private array $consumers = [];

    protected function tearDown(): void
    {
        if (isset($this->queue, $this->natsUrl)) {
            Coroutine\run(function (): void {
                $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
                foreach ([NatsTransport::consumerName($this->queue), ...$this->consumers] as $consumer) {
                    $client->deleteConsumer('QUEUE', $consumer);
                }
            });
        }
    }

    /**
     * REGRESSION (tk-nats-consumer-name-collision). The consumer name maps a
     * queue name onto [a-z0-9-], so media.thumbs and media-thumbs had one
     * consumer. It keeps the subject it was made with: the second queue's
     * worker read the first one's jobs, and its own were never read.
     */
    #[Test]
    public function two_queues_the_name_mapping_once_joined_each_read_only_their_own_jobs(): void
    {
        $dotted = $this->queue . '.thumbs';
        $dashed = $this->queue . '-thumbs';
        $this->consumers = [NatsTransport::consumerName($dotted), NatsTransport::ownConsumerName($dashed), NatsTransport::ownConsumerName($dotted)];
        $seen = ['dotted' => [], 'dashed' => []];
        $failure = null;

        Coroutine\run(function () use ($dotted, $dashed, &$seen, &$failure): void {
            try {
                $transport = $this->transport();
                $transport->publish($dotted, 'for-dotted');
                $transport->publish($dashed, 'for-dashed');

                $deadline = microtime(true) + 5.0;
                while (($seen['dotted'] === [] || $seen['dashed'] === []) && microtime(true) < $deadline) {
                    $transport->consumeBatch($dotted, static function (string $payload) use (&$seen): void {
                        $seen['dotted'][] = $payload;
                    });
                    $transport->consumeBatch($dashed, static function (string $payload) use (&$seen): void {
                        $seen['dashed'][] = $payload;
                    });
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['dotted' => ['for-dotted'], 'dashed' => ['for-dashed']], $seen);
    }

    /**
     * As the old mapping left it in production: the mapped name was made for
     * one queue (media.thumbs), and the other queue (media-thumbs) read the
     * first one's jobs through it. The second queue now reads through a name
     * of its own; the first keeps its consumer and its place, and runs no job
     * it already ran.
     */
    #[Test]
    public function a_queue_whose_mapped_name_holds_another_queues_subject_reads_through_its_own(): void
    {
        $dotted = $this->queue . '.jobs';
        $dashed = $this->queue . '-jobs';
        $this->consumers = [NatsTransport::consumerName($dotted), NatsTransport::ownConsumerName($dashed), NatsTransport::ownConsumerName($dotted)];
        $seen = ['dotted' => [], 'dashed' => []];
        $failure = null;

        Coroutine\run(function () use ($dotted, $dashed, &$seen, &$failure): void {
            try {
                $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
                $transport = $this->transport();
                $transport->publish($dotted, 'dotted-1');
                // Published while the mapped name read the other queue: never read by anyone.
                $transport->publish($dashed, 'dashed-0');
                // The release before: the mapped name made for the dotted queue, dotted-1 run and acked.
                $client->ensurePullConsumer('QUEUE', NatsTransport::consumerName($dotted), 'semitexa.queue.' . $dotted);
                $deadline = microtime(true) + 5.0;
                do {
                    $first = $client->pullMessages('QUEUE', NatsTransport::consumerName($dotted), 1);
                } while ($first === [] && microtime(true) < $deadline);
                self::assertSame(['dotted-1'], array_map(static fn ($m): string => $m->body, $first));
                $first[0]->ack();
                $transport->publish($dotted, 'dotted-2');
                $transport->publish($dashed, 'dashed-1');

                $deadline = microtime(true) + 5.0;
                while ((count($seen['dotted']) < 1 || count($seen['dashed']) < 2) && microtime(true) < $deadline) {
                    $transport->consumeBatch($dashed, static function (string $payload) use (&$seen): void {
                        $seen['dashed'][] = $payload;
                    });
                    $transport->consumeBatch($dotted, static function (string $payload) use (&$seen): void {
                        $seen['dotted'][] = $payload;
                    });
                }
                // One more pull each: nothing already run comes again.
                $transport->consumeBatch($dotted, static function (string $payload) use (&$seen): void {
                    $seen['dotted'][] = $payload;
                });
                $transport->consumeBatch($dashed, static function (string $payload) use (&$seen): void {
                    $seen['dashed'][] = $payload;
                });
                $seen['own'] = $client->consumerState('QUEUE', NatsTransport::ownConsumerName($dashed))['filterSubject'] ?? null;
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['dotted' => ['dotted-2'], 'dashed' => ['dashed-0', 'dashed-1'], 'own' => 'semitexa.queue.' . $dashed], $seen);
    }

    /**
     * Two queues' workers that create the mapped name at once: the second
     * create must leave the first one's consumer as it is. The legacy create
     * pointed it at the second queue's subject and answered ok, and both
     * queues ended on one consumer again (review, reproduced on NATS 2.12).
     */
    #[Test]
    public function a_create_that_meets_a_consumer_of_another_subject_leaves_it_as_it_was(): void
    {
        $name = NatsTransport::consumerName($this->queue . '.race');
        $this->consumers = [$name];
        $result = null;
        $failure = null;

        Coroutine\run(function () use ($name, &$result, &$failure): void {
            try {
                $client = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
                $client->ensureStream('QUEUE', ['subjects' => ['semitexa.queue.>']]);
                // The other worker looked first and saw no consumer (its library remembers that).
                $otherWorker = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
                $otherWorker->deleteConsumer('QUEUE', $name);
                $first = $client->createPullConsumerIfAbsent('QUEUE', $name, 'semitexa.queue.' . $this->queue . '.race', 300.0, 5);
                $again = $client->createPullConsumerIfAbsent('QUEUE', $name, 'semitexa.queue.' . $this->queue . '.race', 300.0, 5);
                $other = $otherWorker->createPullConsumerIfAbsent('QUEUE', $name, 'semitexa.queue.' . $this->queue . '-race', 300.0, 5);
                $result = [$first, $again, $other, $client->consumerState('QUEUE', $name)['filterSubject'] ?? null];
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame([true, true, false, 'semitexa.queue.' . $this->queue . '.race'], $result);
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
    public function a_publish_after_the_connection_died_still_reaches_the_queue(): void
    {
        $seen = [];
        $failure = null;

        Coroutine\run(function () use (&$seen, &$failure): void {
            try {
                $transport = $this->transport();
                $transport->publish($this->queue, 'before');
                // What a NATS restart leaves behind: the client still holds a
                // socket the server no longer has.
                $this->killSocket($transport);
                $transport->publish($this->queue, 'after');

                $deadline = microtime(true) + 5.0;
                while (count($seen) < 2 && microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, static function (string $payload) use (&$seen): void {
                        $seen[] = $payload;
                    });
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        sort($seen);
        self::assertSame(['after', 'before'], $seen);
    }

    #[Test]
    public function the_first_publish_on_a_dead_connection_still_reaches_the_queue(): void
    {
        // Stream setup (not yet done) is the first call on the dead socket.
        $seen = [];
        $failure = null;

        Coroutine\run(function () use (&$seen, &$failure): void {
            try {
                $transport = $this->transport();
                $this->killSocket($transport);
                $transport->publish($this->queue, 'first');

                $deadline = microtime(true) + 5.0;
                while ($seen === [] && microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, static function (string $payload) use (&$seen): void {
                        $seen[] = $payload;
                    });
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['first'], $seen);
    }

    private function killSocket(NatsTransport $transport): void
    {
        $natsClient = (new \ReflectionMethod(NatsTransport::class, 'primaryClient'))->invoke($transport);
        $client = (new \ReflectionMethod($natsClient, 'client'))->invoke($natsClient);
        $socket = (new \ReflectionProperty($client->connection, 'socket'))->getValue($client->connection);
        self::assertIsResource($socket);
        fclose($socket);
    }

    #[Test]
    public function a_delayed_job_is_handled_once_it_is_due_and_does_not_hold_up_the_rest(): void
    {
        // The worker used to sleep a retry's delay inside the consume loop.
        $seen = [];
        $failure = null;

        Coroutine\run(function () use (&$seen, &$failure): void {
            try {
                $transport = $this->transport();
                $published = microtime(true);
                $transport->publishDelayed($this->queue, 'retry', 3);
                $transport->publish($this->queue, 'fresh');

                $deadline = microtime(true) + 10.0;
                while (count($seen) < 2 && microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, static function (string $payload) use (&$seen, $published): void {
                        $seen[] = [$payload, microtime(true) - $published];
                    });
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['fresh', 'retry'], array_column($seen, 0));
        // A pull waits up to ~1.5 s for its batch; the fresh job is handled by
        // the first one, long before the delayed job is due.
        self::assertLessThan(2.5, $seen[0][1], 'the fresh job waited for the delayed one');
        self::assertGreaterThanOrEqual(2.75, $seen[1][1], 'the delayed job was handled before it was due');
        self::assertSame(0, $this->pendingAcks());
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

    /**
     * Acks travel to the server asynchronously, so a count read right after
     * the last ack can still include it. Wait briefly for the server to catch
     * up; a delivery that was never acked stays pending past the deadline.
     */
    private function pendingAcks(): int
    {
        $pending = null;
        Coroutine\run(function () use (&$pending): void {
            $client   = new NatsClient(new ClusterConfig(id: 'admin', url: $this->natsUrl));
            $deadline = microtime(true) + 2.0;
            do {
                $pending = $client->pendingAcks('QUEUE', NatsTransport::consumerName($this->queue));
                if ($pending === 0) {
                    return;
                }
                Coroutine::sleep(0.1);
            } while (microtime(true) < $deadline);
        });

        return (int) $pending;
    }

    /**
     * ep-queue-monitoring: QueueWorker dead-letters to `<queue>.failed`, which
     * nothing consumes. The inspector lists those messages (newest first, the
     * stream sequence as id), queues one again with attempts reset, and
     * forgets one that was never readable.
     */
    #[Test]
    public function dead_letters_are_listed_retried_into_the_queue_and_forgotten(): void
    {
        $result = [];
        $failure = null;

        Coroutine\run(function () use (&$result, &$failure): void {
            try {
                $transport = $this->transport();
                $transport->publish($this->queue . '.failed', (string) json_encode([
                    'type' => 'handler', 'handler' => 'App\\Jobs\\SendInvoice', 'attempts' => 3, 'error' => 'SMTP down', 'failed_at' => '2026-10-21T00:00:00+00:00',
                ]));
                $transport->publish($this->queue . '.failed', '{"raw":"x","error":"not a JSON object"}');

                $result['listed'] = $transport->deadLetters($this->queue);
                $readable = $result['listed'][1];
                // The consumer exists before the retry, so the depth read
                // below counts it, not a queue no consumer reads yet.
                $transport->consumeBatch($this->queue, static function (): void {});
                $result['retried'] = $transport->retryDeadLetter($this->queue, $readable->id);
                $result['retriedTwice'] = $transport->retryDeadLetter($this->queue, $readable->id);
                $result['forgot'] = $transport->forgetDeadLetter($this->queue, $result['listed'][0]->id);
                $result['left'] = $transport->deadLetters($this->queue);
                $result['notOurs'] = $transport->forgetDeadLetter('another-' . $this->queue, $readable->id);
                $result['depthBefore'] = $transport->depth($this->queue);

                // Drain for a while: a second copy of the retried job would arrive too.
                $result['requeued'] = [];
                $deadline = microtime(true) + 3.0;
                while (microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, static function (string $payload) use (&$result): void {
                        $result['requeued'][] = json_decode($payload, true);
                    });
                }

                // Letters on both sides of many other messages: the listing
                // widens its window back from the newest until it holds them.
                for ($i = 0; $i < 3; $i++) {
                    $transport->publish($this->queue . '.failed', (string) json_encode(['type' => 'handler', 'handler' => "Old{$i}", 'error' => 'e']));
                }
                for ($i = 0; $i < 300; $i++) {
                    $transport->publish($this->queue . '.noise', 'x');
                }
                $transport->publish($this->queue . '.failed', (string) json_encode(['type' => 'handler', 'handler' => 'Newest', 'error' => 'e']));
                $result['window'] = array_map(static fn ($l): string => $l->target, $transport->deadLetters($this->queue, 10));
                $result['newestOnly'] = array_map(static fn ($l): string => $l->target, $transport->deadLetters($this->queue, 1));
                $result['depth'] = $transport->depth($this->queue);
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertCount(2, $result['listed']);
        self::assertFalse($result['listed'][0]->retryable, 'newest first: the unreadable one was published last');
        self::assertSame('App\\Jobs\\SendInvoice', $result['listed'][1]->target);
        self::assertSame('SMTP down', $result['listed'][1]->error);
        self::assertTrue($result['retried']);
        self::assertFalse($result['retriedTwice'], 'retried once, then gone');
        self::assertTrue($result['forgot']);
        self::assertSame([], $result['left']);
        self::assertFalse($result['notOurs'], 'a dead letter of another queue is not touched');
        self::assertCount(1, $result['requeued'], 'the retried job arrives once');
        self::assertSame(0, $result['requeued'][0]['attempts'] ?? null, 'attempts start over');
        self::assertSame(['Newest', 'Old2', 'Old1', 'Old0'], $result['window']);
        self::assertSame(['Newest'], $result['newestOnly']);
        self::assertSame(1, $result['depthBefore'], 'the retried job waits in the backlog');
        self::assertArrayNotHasKey('error', $result['requeued'][0]);
        self::assertSame(0, $result['depth']);
    }

    /**
     * A walk stops at its cap from the oldest end. A wider window holding
     * more letters than that replaced the narrower one and lost the newest
     * letter; one with more tombstones after a letter than that missed its
     * tombstone, and a retry ran the job a second time.
     */
    #[Test]
    public function a_walk_cut_short_by_its_cap_neither_hides_the_newest_letter_nor_retries_a_handled_one(): void
    {
        $result = [];
        $failure = null;
        $old4 = '';

        Coroutine\run(function () use (&$result, &$failure, &$old4): void {
            try {
                $capped = $this->transport(walkLimit: 4);
                $full = $this->transport();
                for ($i = 0; $i < 5; $i++) {
                    $full->publish($this->queue . '.failed', (string) json_encode(['type' => 'handler', 'handler' => "Old{$i}", 'error' => 'e']));
                }
                for ($i = 0; $i < 300; $i++) {
                    $full->publish($this->queue . '.noise', 'x');
                }
                $full->publish($this->queue . '.failed', (string) json_encode(['type' => 'handler', 'handler' => 'Newest', 'error' => 'e']));

                $result['listed'] = array_map(static fn ($l): string => $l->target, $capped->deadLetters($this->queue, 10));

                $ids = [];
                foreach ($full->deadLetters($this->queue, 10) as $letter) {
                    $ids[$letter->target] = $letter->id;
                }
                // Four tombstones after Old4, none of them its own; then its own.
                foreach (['Old0', 'Old1', 'Old2', 'Old3', 'Old4'] as $target) {
                    $full->forgetDeadLetter($this->queue, $ids[$target]);
                }
                $old4 = $ids['Old4'];
                try {
                    $capped->retryDeadLetter($this->queue, $ids['Old4']);
                    $result['retry'] = 'retried';
                } catch (\RuntimeException $e) {
                    $result['retry'] = $e->getMessage();
                }
                $result['forgetOld0'] = $capped->forgetDeadLetter($this->queue, $ids['Old0']);
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['Newest'], $result['listed'], 'the narrow window stands; the wider one held more than a walk reads');
        self::assertSame("Dead letter {$old4}: 4 or more retries and forgets came after it, so whether it was handled is not known; it is left as it is.", $result['retry']);
        self::assertFalse($result['forgetOld0'], 'a tombstone the walk did read still counts');
    }

    /**
     * A worker that only inspects (queue:failed, depth) never publishes or
     * pulls: after a NATS restart its dead socket failed every inspection
     * until the process restarted. The call that fails drops the socket, so
     * the next one runs on a new connection.
     */
    #[Test]
    public function an_inspection_after_the_connection_died_works_the_next_time(): void
    {
        $result = [];
        $failure = null;

        Coroutine\run(function () use (&$result, &$failure): void {
            try {
                $transport = $this->transport();
                $transport->publish($this->queue . '.failed', (string) json_encode(['type' => 'handler', 'handler' => 'Dead', 'error' => 'e']));
                $inspections = [
                    'letters' => fn (): array => array_map(static fn ($l): string => $l->target, $transport->deadLetters($this->queue)),
                    'depth' => fn (): int => $transport->depth($this->queue),
                ];
                foreach ($inspections as $name => $inspect) {
                    $this->killSocket($transport);
                    try {
                        $inspect();
                    } catch (\Throwable) {
                        // The call that meets the dead socket may fail.
                    }
                    $result[$name] = $inspect();
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }

        self::assertSame(['Dead'], $result['letters']);
        self::assertSame(0, $result['depth']);
    }

    /**
     * Stopped (SIGTERM) on the first message of a batch: the rest of the batch
     * must not run here, past the stop, nor wait out ACK_WAIT (300 s) — they
     * go back to the stream at once, for the next worker.
     */
    #[Test]
    public function a_stop_mid_batch_hands_the_rest_back_at_once(): void
    {
        $ran = ['stopped' => [], 'next' => []];
        $failure = null;

        Coroutine\run(function () use (&$ran, &$failure): void {
            try {
                $transport = $this->transport();
                foreach (['a', 'b', 'c'] as $payload) {
                    $transport->publish($this->queue, $payload);
                }
                $deadline = microtime(true) + 5.0;
                while ($ran['stopped'] === [] && microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, static function (string $payload) use ($transport, &$ran): void {
                        $ran['stopped'][] = $payload;
                        $transport->stop();
                    });
                }

                $next = $this->transport();
                $deadline = microtime(true) + 5.0;
                while (count($ran['stopped']) + count($ran['next']) < 3 && microtime(true) < $deadline) {
                    $next->consumeBatch($this->queue, static function (string $payload) use (&$ran): void {
                        $ran['next'][] = $payload;
                    });
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }
        self::assertCount(1, $ran['stopped'], 'nothing ran after the stop');
        $all = [...$ran['stopped'], ...$ran['next']];
        sort($all);
        self::assertSame(['a', 'b', 'c'], $all, 'the rest reached the next worker within seconds, each once');
    }

    /**
     * A stop handed the rest of the batch back with a nak, which spends a
     * delivery: one already on its last delivery was dropped, without a word.
     * It runs before the worker stops instead.
     */
    #[Test]
    public function a_stop_does_not_hand_back_a_message_on_its_last_delivery(): void
    {
        $attempts = [];
        $finished = [];
        $failure = null;

        Coroutine\run(function () use (&$attempts, &$finished, &$failure): void {
            try {
                $transport = $this->transport();
                $transport->publish($this->queue, 'x');
                $transport->publish($this->queue, 'y');
                $deadline = microtime(true) + 20.0;
                while (count($finished) < 2 && microtime(true) < $deadline) {
                    $transport->consumeBatch($this->queue, static function (string $payload) use ($transport, &$attempts, &$finished): void {
                        $attempts[$payload] = ($attempts[$payload] ?? 0) + 1;
                        if ($attempts[$payload] < 5) {
                            throw new \RuntimeException('not yet');
                        }
                        $finished[] = $payload;
                        if ($payload === 'x') {
                            $transport->stop();
                        }
                    });
                    if (in_array('x', $finished, true)) {
                        break;
                    }
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }
        });

        if ($failure !== null) {
            throw $failure;
        }
        sort($finished);
        self::assertSame(['x', 'y'], $finished, 'y, on its fifth delivery, ran in the batch the stop came in');
    }

    private function transport(?int $walkLimit = null): NatsTransport
    {
        $clusters = new ClusterRegistry();
        $clusters->add(new ClusterConfig(id: 'primary', url: $this->natsUrl));
        $clusters->connect();

        return $walkLimit === null
            ? new NatsTransport($clusters, retryDelaySeconds: 0.5)
            : new NatsTransport($clusters, retryDelaySeconds: 0.5, walkLimit: $walkLimit);
    }
}
