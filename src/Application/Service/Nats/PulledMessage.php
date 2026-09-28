<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

/**
 * One message pulled from a JetStream consumer, NOT yet acknowledged.
 *
 * The caller decides the outcome. The previous pull handed back bare payloads
 * after the library had already acked every one on receipt, so a replayer that
 * failed to apply an event had no way to ask for it again — and its calls to
 * ack()/nak() on those payloads threw, losing the rest of the batch.
 */
final class PulledMessage
{
    /**
     * @param \Closure(): void       $ack
     * @param \Closure(float): void  $nak
     */
    public function __construct(
        public readonly string $body,
        /** Position in the stream; 0 when the reply subject did not carry it. */
        public readonly int $streamSequence,
        private readonly \Closure $ack,
        private readonly \Closure $nak,
    ) {}

    public function ack(): void
    {
        ($this->ack)();
    }

    /** Ask JetStream to redeliver, after $delaySeconds. */
    public function nak(float $delaySeconds = 0.0): void
    {
        ($this->nak)($delaySeconds);
    }

    /**
     * A JetStream delivery's reply subject is
     * `$JS.ACK.<stream>.<consumer>.<delivered>.<streamSeq>.<consumerSeq>.<ts>.<pending>`
     * (newer servers insert `<domain>.<account-hash>.` after `ACK` and may
     * append a random token, which moves the stream sequence to token 7).
     */
    public static function streamSequenceFromReplyTo(?string $replyTo): int
    {
        if ($replyTo === null || !str_starts_with($replyTo, '$JS.ACK.')) {
            return 0;
        }

        $tokens = explode('.', $replyTo);
        $count  = count($tokens);

        $index = match (true) {
            $count === 9  => 5,
            $count >= 11  => 7,
            default       => -1,
        };

        if ($index < 0 || !ctype_digit($tokens[$index] ?? '')) {
            return 0;
        }

        return (int) $tokens[$index];
    }
}
