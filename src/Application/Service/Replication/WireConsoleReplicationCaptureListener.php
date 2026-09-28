<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Attribute\AsServerLifecycleListener;
use Semitexa\Core\Server\Lifecycle\ServerLifecycleContext;
use Semitexa\Core\Server\Lifecycle\ServerLifecycleListenerInterface;
use Semitexa\Core\Server\Lifecycle\ServerLifecyclePhase;
use Semitexa\Ledger\Application\Service\HybridLogicalClock;
use Semitexa\Ledger\Application\Service\LedgerBootstrap;
use Semitexa\Orm\Application\Service\Persistence\ReplicationCapture;

/**
 * Replication capture for a CLI process on a ledger node.
 *
 * Commands and scheduled jobs write rows too, and none of the worker
 * lifecycle runs for them: without this, a #[Replicated] row written from a
 * terminal would change here and never reach any other node — with no error
 * to say so. The outbox rows it writes are relayed by the running server.
 */
#[AsServerLifecycleListener(
    phase: ServerLifecyclePhase::ConsoleStartAfterContainer->value,
    priority: 0,
)]
final class WireConsoleReplicationCaptureListener implements ServerLifecycleListenerInterface
{
    public function handle(ServerLifecycleContext $context): void
    {
        $nodeId = (string) (getenv('LEDGER_NODE_ID') ?: '');
        if (!LedgerBootstrap::isEnabled() || $nodeId === '') {
            return;
        }

        $capture = new ReplicationCaptureService($nodeId, new HybridLogicalClock());
        ReplicationCapture::setResolver(static fn (): ReplicationCaptureService => $capture);
    }
}
