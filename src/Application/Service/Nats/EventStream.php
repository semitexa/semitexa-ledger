<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

use Semitexa\Ledger\Domain\Model\LedgerEvent;

/**
 * The JetStream stream that carries ledger events between nodes, and the
 * subject namespace it captures.
 *
 * Nothing used to create this stream: the publisher sent to subjects no stream
 * captured, and the replayer's consumer needed a stream someone had made by
 * hand. Both sides now call {@see ensure()} before touching it.
 *
 * The name and prefix are configurable because they are the replication
 * boundary: two unrelated projects pointed at one NATS server must not share
 * them, or each would replay the other's events.
 */
final class EventStream
{
    /** Seconds within which JetStream drops a repeated Nats-Msg-Id. */
    private const DUPLICATE_WINDOW = 120.0;

    public function __construct(
        public readonly string $name = 'EVENTS',
        public readonly string $subjectPrefix = 'semitexa.events',
    ) {}

    public static function fromEnv(): self
    {
        return new self(
            (string) (getenv('LEDGER_STREAM') ?: 'EVENTS'),
            (string) (getenv('LEDGER_SUBJECT_PREFIX') ?: 'semitexa.events'),
        );
    }

    public function subjectFor(LedgerEvent $event): string
    {
        return sprintf(
            '%s.%s.%s.%s',
            $this->subjectPrefix,
            $event->originNode,
            $event->domain,
            $event->eventType,
        );
    }

    public function filterSubject(): string
    {
        return $this->subjectPrefix . '.>';
    }

    public function ensure(NatsClient $client): void
    {
        $client->ensureStream($this->name, [
            'subjects'         => [$this->filterSubject()],
            'duplicate_window' => self::DUPLICATE_WINDOW,
        ]);
    }
}
