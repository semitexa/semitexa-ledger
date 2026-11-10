<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Queue\NatsTransport;

/**
 * NATS_QUEUE_STREAM / NATS_QUEUE_SUBJECT_PREFIX: unset is today's names, and a
 * value NATS would refuse or misread is refused at boot, by name, not at the
 * first publish (tk-nats-subject-namespace).
 */
final class NatsQueueNamespaceConfigTest extends TestCase
{
    #[Test]
    public function unset_or_blank_is_todays_namespace(): void
    {
        self::assertSame('QUEUE', NatsTransport::streamName(null));
        self::assertSame('QUEUE', NatsTransport::streamName('  '));
        self::assertSame('semitexa.queue.', NatsTransport::subjectPrefix(null));
        self::assertSame('semitexa.queue.', NatsTransport::subjectPrefix(''));
    }

    #[Test]
    public function a_valid_namespace_is_kept_as_written(): void
    {
        self::assertSame('QUEUE_E2E', NatsTransport::streamName('QUEUE_E2E'));
        self::assertSame('e2e.queue.', NatsTransport::subjectPrefix('e2e.queue.'));
        self::assertSame('a-b_c.', NatsTransport::subjectPrefix(' a-b_c. '));
    }

    #[Test]
    public function a_stream_name_nats_would_refuse_is_refused_by_name(): void
    {
        foreach (['QUEUE.E2E', 'QUEUE E2E', 'QUEUE>', 'Q*'] as $bad) {
            try {
                NatsTransport::streamName($bad);
                self::fail("'{$bad}' was accepted");
            } catch (\InvalidArgumentException $e) {
                self::assertStringContainsString("NATS_QUEUE_STREAM '{$bad}'", $e->getMessage());
            }
        }
    }

    #[Test]
    public function a_prefix_that_is_not_dot_terminated_tokens_is_refused(): void
    {
        // Without the trailing dot "e2e" + "mail" reads as one token, "e2email";
        // a wildcard or an empty token would capture other apps' subjects.
        foreach (['e2e', 'e2e.queue', '.queue.', 'e2e..queue.', 'e2e.*.', 'e2e.>.'] as $bad) {
            try {
                NatsTransport::subjectPrefix($bad);
                self::fail("'{$bad}' was accepted");
            } catch (\InvalidArgumentException $e) {
                self::assertStringContainsString("NATS_QUEUE_SUBJECT_PREFIX '{$bad}'", $e->getMessage());
            }
        }
    }
}
