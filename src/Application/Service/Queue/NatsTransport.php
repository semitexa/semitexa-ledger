<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Queue;

use Semitexa\Core\Environment;
use Semitexa\Core\Log\StaticLoggerBridge;
use Semitexa\Core\Queue\DelayedQueueTransportInterface;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;
use Semitexa\Ledger\Application\Service\Nats\PulledMessage;
use Semitexa\Ledger\Application\Service\Nats\StreamMaxAge;

/**
 * NATS JetStream implementation of the existing QueueTransportInterface.
 *
 * NATS-based async queue transport. Set EVENTS_TRANSPORT=nats to
 * activate. Queue names become NATS subjects under the semitexa.queue.> prefix.
 *
 * Messages are published to JetStream (stream must be configured to capture
 * `semitexa.queue.>` subjects). Consumers are created per queue name.
 */
final class NatsTransport implements DelayedQueueTransportInterface, \Semitexa\Core\Queue\QueueInspectorInterface, \Semitexa\Core\Queue\StoppableQueueTransportInterface
{
    private const SUBJECT_PREFIX = 'semitexa.queue.';
    private const STREAM_NAME    = 'QUEUE';
    /** Seconds a delivery may stay unacked before JetStream redelivers it — the database transport's lease. */
    private const ACK_WAIT    = 300.0;
    /** Deliveries before a failing message stops being redelivered — the database transport's max_attempts. */
    private const MAX_DELIVER = 5;

    /** A redelivery a little before its due time is due: a second nak would spend a delivery for nothing. */
    private const DUE_TOLERANCE_MS = 250;
    private const RETRY_DELAY = 5.0;

    /** Messages a walk reads, at most: each is one request to the server. */
    private const WALK_LIMIT = 10_000;

    private bool $streamReady = false;

    /** Cleared by stop() to end consume() after the current batch. */
    private bool $running = true;

    /** @var array<string, string> queue name => the consumer it is read through */
    private array $consumerNames = [];

    public function __construct(
        private readonly ClusterRegistry $clusters,
        private readonly float $retryDelaySeconds = self::RETRY_DELAY,
        private readonly int $walkLimit = self::WALK_LIMIT,
    ) {}

    public function publish(string $queueName, string $payload): void
    {
        $this->send($queueName, $payload, []);
    }

    /**
     * JetStream cannot hold a message back on publish. It is stored now with
     * the time it is due; the consumer that pulls it early hands it back
     * with a nak for the time that remains, and that costs one delivery.
     */
    public function publishDelayed(string $queueName, string $payload, int $delaySeconds): void
    {
        // The stream deletes a message NATS_QUEUE_MAX_AGE_DAYS after it is
        // stored, due or not: one held back longer was lost without a trace.
        $maxAge = self::maxAgeNanoSeconds(Environment::getEnvValue('NATS_QUEUE_MAX_AGE_DAYS'));
        if ($maxAge > 0 && $delaySeconds * 1_000_000_000 >= $maxAge) {
            throw new \InvalidArgumentException("A message held back {$delaySeconds} s would be deleted first: the queue stream keeps messages " . intdiv($maxAge, 86_400_000_000_000) . ' days (NATS_QUEUE_MAX_AGE_DAYS).');
        }
        $this->send($queueName, $payload, [
            PulledMessage::NOT_BEFORE_HEADER => (string) ((int) floor(microtime(true) * 1000) + max(0, $delaySeconds) * 1000),
        ]);
    }

    /** @param array<string, string> $extraHeaders */
    private function send(string $queueName, string $payload, array $extraHeaders): void
    {
        $subject = self::SUBJECT_PREFIX . $queueName;

        // One id for both attempts: JetStream drops a second copy with the
        // same Nats-Msg-Id inside its duplicate window, so a retry after a
        // send the server did store (an ack that timed out) cannot deliver a
        // job twice — a second email, say.
        $headers = ['Nats-Msg-Id' => 'q-' . bin2hex(random_bytes(16))] + $extraHeaders;

        $attempt = function () use ($subject, $payload, $headers): void {
            $this->ensureQueueStream();
            $this->primaryClient()->jetStreamPublish($subject, $payload, $headers);
        };

        try {
            $attempt();
        } catch (\Throwable) {
            // A socket that died with a NATS restart fails the first call —
            // the stream setup or the send ('Error sending data'). Publishers
            // do not retry: media ingest logged the failure and left the
            // variant queued forever. So the transport drops the socket and
            // retries once, setup included, on a fresh connection.
            $this->primaryClient()->reconnect();
            $attempt();
        }
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function consume(string $queueName, callable $callback): void
    {
        // Blocking consume loop for queue:work command.
        while ($this->running) {
            if ($this->consumeBatch($queueName, $callback) === 0) {
                sleep(1);
            }
        }
    }

    /**
     * Pull one batch and hand each message to $callback. Returns how many
     * arrived. The consume loop is this in a loop; tests drive it directly.
     *
     * At-least-once, like the database transport: a message is acked only
     * after the callback returned. A callback that throws gets it back after
     * the retry delay; a worker that dies mid-job gets it back after ACK_WAIT; one
     * that keeps failing stops being delivered after MAX_DELIVER. Messages
     * used to be acked on receipt, so a crash lost the job.
     *
     * @param callable(string):void $callback
     */
    public function consumeBatch(string $queueName, callable $callback): int
    {
        $this->ensureQueueStream();
        $subject      = self::SUBJECT_PREFIX . $queueName;
        $client       = $this->primaryClient();
        $consumerName = $this->consumerFor($client, $queueName, $subject);

        // A stop signal (QueueStopSignals) landing while the library waits in
        // stream_select() makes it return false, which the library takes as
        // "nothing yet"; PHP also warns "Interrupted system call". Only that
        // warning is dropped, and only here; any other goes to the handler
        // that was there.
        $previous = null;
        $previous = set_error_handler(static function (int $no, string $message, string $file = '', int $line = 0) use (&$previous): bool {
            if (str_contains($message, 'Interrupted system call')) {
                return true;
            }

            return $previous !== null && (bool) $previous($no, $message, $file, $line);
        }, E_WARNING);
        try {
            $messages = $client->pullMessages(
                streamName:   self::STREAM_NAME,
                consumerName: $consumerName,
                batchSize:    10,
            );
        } finally {
            restore_error_handler();
        }

        foreach ($messages as $msg) {
            // Stopped (SIGTERM) in the middle of a batch: the rest are
            // already delivered to this worker. Back to the stream at once,
            // for another replica or the next start, rather than run here
            // past the stop or left for ACK_WAIT — except one on its last
            // delivery: every nak spends one, and that nak was the end of it,
            // with nothing logged. That one runs before the worker stops (at
            // most a batch of them; past WORKER_STOP_GRACE_PERIOD the kill
            // ends it as before).
            if (!$this->running && $msg->deliveryCount < self::MAX_DELIVER) {
                $this->settle(static fn () => $msg->nak(0.0), 'nak', $queueName);
                continue;
            }
            $early = $msg->notBeforeMs - (int) floor(microtime(true) * 1000);
            // On its last delivery a message is run, early or not: a nak there
            // would be the end of it, with nothing logged.
            if ($early > self::DUE_TOLERANCE_MS && $msg->deliveryCount < self::MAX_DELIVER) {
                // A delayed retry, pulled before it is due: back to the stream
                // for the time that remains. Whole seconds, rounded up: the
                // client sends seconds × 10⁹, and a fraction such as 0.267 s
                // comes out as 267000000.00000003 ns, which the server refuses
                // and treats as a plain nak — straight back, a delivery spent.
                $this->settle(static fn () => $msg->nak((float) (int) ceil($early / 1000)), 'nak', $queueName);
                continue;
            }

            try {
                $callback($msg->body);
            } catch (\Throwable $e) {
                $context = [
                    'queue'    => $queueName,
                    'attempt'  => $msg->deliveryCount,
                    'error'    => $e->getMessage(),
                    'class'    => get_class($e),
                    'trace'    => $e->getTraceAsString(),
                ];
                if ($msg->deliveryCount >= self::MAX_DELIVER) {
                    // JetStream will not deliver it again. The stream still
                    // holds it (for NATS_QUEUE_MAX_AGE_DAYS): this says where,
                    // `nats stream get QUEUE <seq>`. Not the body itself, which
                    // can carry what a log must not (a reset link, an address).
                    // Nor a plain hash of it: a predictable body (an address)
                    // is found again by hashing guesses.
                    $context['stream_sequence'] = $msg->streamSequence;
                    $context['body_bytes'] = strlen($msg->body);
                    StaticLoggerBridge::error('queue', sprintf('NATS queue job dropped after %d attempts', self::MAX_DELIVER), $context);
                } else {
                    StaticLoggerBridge::error('queue', 'NATS queue job failed, will be redelivered', $context);
                }
                $delay = $this->retryDelaySeconds;
                $this->settle(static fn () => $msg->nak($delay), 'nak', $queueName);
                continue;
            }

            $this->settle(static fn () => $msg->ack(), 'ack', $queueName);
        }

        return count($messages);
    }

    /**
     * An ack or nak that fails (the connection dropped) ended the batch and
     * the worker with it, the rest of the batch unrun. Logged, and the batch
     * goes on: JetStream delivers the message again after ACK_WAIT, so a job
     * whose ack was lost can run twice.
     */
    private function settle(\Closure $reply, string $kind, string $queueName): void
    {
        try {
            $reply();
        } catch (\Throwable $e) {
            StaticLoggerBridge::warning('queue', "NATS queue: the {$kind} failed; the message comes back after its ack wait" . ($kind === 'ack' ? ' and runs again' : ''), [
                'queue' => $queueName,
                'error' => $e->getMessage(),
            ]);
        }
    }

    /**
     * The name a queue's durable consumer is looked for under first: the queue
     * name mapped onto [a-z0-9-], as every queue's consumer has always been
     * named. The mapping can join two queues (Mail and mail, media.thumbs and
     * media-thumbs); {@see consumerFor()} then gives the second one
     * {@see ownConsumerName()}.
     */
    public static function consumerName(string $queueName): string
    {
        return 'queue-' . preg_replace('/[^a-z0-9-]/', '-', strtolower($queueName));
    }

    /**
     * A name only this queue maps to: the mapped name, then a hash of the
     * queue name after a character the mapping never produces.
     */
    public static function ownConsumerName(string $queueName): string
    {
        return self::consumerName($queueName) . '_' . substr(hash('sha256', $queueName), 0, 12);
    }

    /**
     * The consumer this queue is read through. A consumer keeps the subject it
     * was made with, so when the mapped name holds another queue's subject,
     * reading through it took that queue's jobs and left this one's unread:
     * the queue then uses its own name instead. A consumer that is this
     * queue's own keeps its name and its place, whatever other queues do, and
     * nothing is ever moved or deleted here. A name that is free is created
     * only if it still is ({@see NatsClient::createPullConsumerIfAbsent()}), so
     * two queues' workers that reach it at once cannot share it.
     *
     * The choice holds as long as the consumers do: delete the one under the
     * mapped name, and the next queue to start takes it from the stream's
     * first message, while the queue that had it moves to its own name and
     * starts there too. Both then run their jobs again.
     */
    private function consumerFor(\Semitexa\Ledger\Application\Service\Nats\NatsClient $client, string $queueName, string $subject): string
    {
        if (isset($this->consumerNames[$queueName])) {
            return $this->consumerNames[$queueName];
        }

        foreach ([self::consumerName($queueName), self::ownConsumerName($queueName)] as $candidate) {
            $state = $client->consumerState(self::STREAM_NAME, $candidate);
            if ($state === null) {
                if (!$client->createPullConsumerIfAbsent(self::STREAM_NAME, $candidate, $subject, self::ACK_WAIT, self::MAX_DELIVER)) {
                    continue;
                }
            } elseif ($state['filterSubject'] !== $subject) {
                continue;
            } else {
                // Ours already: same subject, so this only brings its settings up to date.
                $client->ensurePullConsumer(
                    streamName:     self::STREAM_NAME,
                    consumerName:   $candidate,
                    filterSubject:  $subject,
                    ackWaitSeconds: self::ACK_WAIT,
                    maxDeliver:     self::MAX_DELIVER,
                );
            }
            if ($candidate !== self::consumerName($queueName)) {
                StaticLoggerBridge::info('queue', 'NATS queue reads through a consumer of its own: its mapped name belongs to another queue', [
                    'queue'    => $queueName,
                    'consumer' => $candidate,
                ]);
            }

            return $this->consumerNames[$queueName] = $candidate;
        }

        throw new \RuntimeException(sprintf(
            "NATS consumers '%s' and '%s' both read another subject than '%s': delete or rename the one that is not a queue's.",
            self::consumerName($queueName),
            self::ownConsumerName($queueName),
            $subject,
        ));
    }

    /** Messages no worker has taken yet: the queue's consumer backlog (0 before any worker read it). */
    public function depth(string $queue): int
    {
        return $this->backlog($queue)['pending'] ?? 0;
    }

    public function oldestAgeSeconds(string $queue): ?int
    {
        $backlog = $this->backlog($queue);
        if ($backlog === null || $backlog['pending'] === 0) {
            return null;
        }
        $next = $this->primaryClient()->streamMessage(self::STREAM_NAME, $backlog['deliveredSeq'] + 1, self::SUBJECT_PREFIX . $queue);
        $at = $next === null ? false : strtotime($next['time']);

        return $at === false ? null : max(0, time() - $at);
    }

    /**
     * What QueueWorker dead-lettered to `<queue>.failed`; the id is the stream
     * sequence. Nothing consumes that subject, and the QUEUE stream refuses to
     * delete a message (deny_delete, the client library's default when the
     * stream was made), so a retried or forgotten one is marked handled by a
     * message on `<queue>.failed.handled` instead, and left out from then on.
     */
    public function deadLetters(string $queue, int $limit = 20): array
    {
        $subject = self::SUBJECT_PREFIX . $queue . '.failed';
        $newest = $this->primaryClient()->streamMessage(self::STREAM_NAME, 0, lastBySubject: $subject);
        if ($newest === null) {
            return [];
        }

        // Read back from the newest: a window before it, widened until it holds
        // $limit unhandled letters or reaches the stream's start. A walk from
        // sequence 1 cost every letter ever and, past its cap, hid the newest.
        // A letter's tombstone is published after it, so the window's
        // tombstones are found from the same start.
        //
        // A walk stops at its cap from the OLDEST end: a wider window that
        // holds more letters than that lost the newest ones, and one with more
        // tombstones listed handled letters as unhandled. Such a window is not
        // used and the widening stops: the narrower one before it stands (the
        // first one is used either way, being all there is).
        $span = 256;
        $found = null;
        do {
            $from = max(1, $newest['seq'] - $span);
            $handled = $this->handledSequences($queue, $from);
            $window = [];
            $walked = 0;
            foreach ($this->walk($subject, $from) as $message) {
                $walked++;
                if (!isset($handled['sequences'][$message['seq']])) {
                    $window[] = $message;
                }
            }
            $complete = $handled['complete'] && $walked < $this->walkLimit;
            if ($complete || $found === null) {
                $found = $window;
            }
            $span *= 8;
        } while ($complete && count($found) < $limit && $from > 1);
        $found = array_slice($found, -$limit);

        return array_map(static function (array $message) use ($queue): \Semitexa\Core\Queue\DeadLetter {
            $letter = \Semitexa\Core\Queue\DeadLetter::fromStored((string) $message['seq'], $queue, $message['data']);
            if ($letter->failedAt !== null || $message['time'] === '') {
                return $letter;
            }

            return new \Semitexa\Core\Queue\DeadLetter($letter->id, $letter->queue, $letter->target, $letter->attempts, $letter->error, new \DateTimeImmutable($message['time']), $letter->payload, $letter->retryable);
        }, array_reverse($found));
    }

    /**
     * Publish it to the queue again, then mark it handled. Both carry an id
     * made from the sequence, so JetStream drops a second copy inside its
     * duplicate window: two operators retrying at once queue the job once.
     */
    public function retryDeadLetter(string $queue, string $id): bool
    {
        $message = $this->deadLetterMessage($queue, $id);
        if ($message === null) {
            return false;
        }
        $retry = \Semitexa\Core\Queue\DeadLetter::fromStored($id, $queue, $message['data'])->retryPayload();
        $this->publishOnce(self::SUBJECT_PREFIX . $queue, $retry, "dlq-retry-{$message['seq']}");
        // Queued, but still listed: the operator saw the raw error, took the
        // retry for failed, and ran it again; past JetStream's duplicate
        // window that queued the job a second time.
        try {
            $this->markHandled($queue, $message['seq']);
        } catch (\Throwable $e) {
            throw new \RuntimeException("Dead letter {$id} was queued again, but could not be marked handled ({$e->getMessage()}): it is still listed. Retrying it within two minutes queues it once; later it runs twice, so forget it instead (queue:retry --forget).", 0, $e);
        }

        return true;
    }

    public function forgetDeadLetter(string $queue, string $id): bool
    {
        $message = $this->deadLetterMessage($queue, $id);
        if ($message === null) {
            return false;
        }
        $this->markHandled($queue, $message['seq']);

        return true;
    }

    private function markHandled(string $queue, int $seq): void
    {
        $this->publishOnce(self::SUBJECT_PREFIX . $queue . '.failed.handled', (string) $seq, "dlq-done-{$seq}");
    }

    /** With publish()'s reconnect-and-retry: a stale socket after a NATS restart must not lose a tombstone. */
    private function publishOnce(string $subject, string $payload, string $messageId): void
    {
        $attempt = function () use ($subject, $payload, $messageId): void {
            $this->ensureQueueStream();
            $this->primaryClient()->jetStreamPublish($subject, $payload, ['Nats-Msg-Id' => $messageId]);
        };
        try {
            $attempt();
        } catch (\Throwable) {
            $this->primaryClient()->reconnect();
            $attempt();
        }
    }

    /**
     * Dead-letter sequences at or after $from already retried or forgotten;
     * not complete when the walk stopped at its cap.
     *
     * @return array{sequences: array<int, true>, complete: bool}
     */
    private function handledSequences(string $queue, int $from = 1): array
    {
        $handled = [];
        $walked = 0;
        foreach ($this->walk(self::SUBJECT_PREFIX . $queue . '.failed.handled', $from) as $message) {
            $walked++;
            if (ctype_digit($message['data'])) {
                $handled[(int) $message['data']] = true;
            }
        }

        return ['sequences' => $handled, 'complete' => $walked < $this->walkLimit];
    }

    /**
     * Every stored message of one subject, oldest first, at most the walk limit.
     *
     * @return \Generator<array{seq: int, subject: string, data: string, time: string}>
     */
    private function walk(string $subject, int $from = 1): \Generator
    {
        $client = $this->primaryClient();
        $seq = max(1, $from);
        for ($i = 0; $i < $this->walkLimit; $i++) {
            $message = $client->streamMessage(self::STREAM_NAME, $seq, $subject);
            if ($message === null) {
                return;
            }
            yield $message;
            $seq = $message['seq'] + 1;
        }
    }

    /** @return array{seq: int, subject: string, data: string, time: string}|null the message, only if it is this queue's dead letter */
    private function deadLetterMessage(string $queue, string $id): ?array
    {
        if (!ctype_digit($id) || (int) $id < 1) {
            return null;
        }
        $message = $this->primaryClient()->streamMessage(self::STREAM_NAME, (int) $id);
        if ($message === null || $message['subject'] !== self::SUBJECT_PREFIX . $queue . '.failed') {
            return null;
        }

        // Its tombstone, if any, came after it: only those are read. Past the
        // walk's cap it may be one not read, and a retry would run the job
        // again: refused rather than guessed.
        $handled = $this->handledSequences($queue, $message['seq']);
        if (isset($handled['sequences'][$message['seq']])) {
            return null;
        }
        if (!$handled['complete']) {
            throw new \RuntimeException("Dead letter {$id}: {$this->walkLimit} or more retries and forgets came after it, so whether it was handled is not known; it is left as it is.");
        }

        return $message;
    }

    /** @return array{pending: int, deliveredSeq: int}|null the backlog of the consumer that reads this queue */
    private function backlog(string $queue): ?array
    {
        $client = $this->primaryClient();
        $subject = self::SUBJECT_PREFIX . $queue;
        foreach ([self::consumerName($queue), self::ownConsumerName($queue)] as $candidate) {
            $state = $client->consumerState(self::STREAM_NAME, $candidate);
            if ($state !== null && $state['filterSubject'] === $subject) {
                return $client->consumerBacklog(self::STREAM_NAME, $candidate);
            }
        }

        return null;
    }

    private function primaryClient(): \Semitexa\Ledger\Application\Service\Nats\NatsClient
    {
        $clusters = $this->clusters->getOrderedByPriority();
        if ($clusters === []) {
            throw new \RuntimeException('No NATS clusters configured.');
        }

        return $clusters[0]['client'];
    }

    /**
     * NATS_QUEUE_MAX_AGE_DAYS as JetStream's nanoseconds: 14 days unless set,
     * 0 = forever. Anything but whole days (a typo, a negative number) keeps
     * the 14: read as 0 it would keep every message, the opposite of what
     * was meant. At most 36500 days, or the nanoseconds overflow an int.
     */
    public static function maxAgeNanoSeconds(?string $days): int
    {
        $days = trim((string) $days);
        $days = ctype_digit($days) ? min(36_500, (int) $days) : 14;

        return $days * 86_400 * 1_000_000_000;
    }

    private function ensureQueueStream(): void
    {
        if ($this->streamReady) {
            return;
        }

        // Every message, acked or dead-lettered, is kept NATS_QUEUE_MAX_AGE_DAYS
        // (14 by default; 0 keeps them forever): with no limit the stream kept
        // every job ever queued until the disk was full. A dead letter on
        // <queue>.failed expires the same way, so queue:failed sees that long.
        $maxAge = self::maxAgeNanoSeconds(Environment::getEnvValue('NATS_QUEUE_MAX_AGE_DAYS'));
        $client = $this->primaryClient();
        $client->ensureStream(self::STREAM_NAME, [
            'subjects' => [self::SUBJECT_PREFIX . '>'],
            'max_age' => $maxAge,
        ]);
        try {
            StreamMaxAge::ensure($client, self::STREAM_NAME, $maxAge);
        } catch (\Throwable $e) {
            // The queue still works on a stream whose age could not be set.
            StaticLoggerBridge::warning('queue', 'NATS QUEUE stream: its max age could not be set; it keeps every message', [
                'error' => $e->getMessage(),
            ]);
        }

        $this->streamReady = true;
    }
}
