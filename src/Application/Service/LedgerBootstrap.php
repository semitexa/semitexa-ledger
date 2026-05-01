<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Core\Attribute\AsServerLifecycleListener;
use Semitexa\Core\Container\ContainerFactory;
use Semitexa\Core\Event\EventDispatcher;
use Semitexa\Core\Queue\QueueTransportRegistry;
use Semitexa\Core\Server\Lifecycle\ServerLifecycleContext;
use Semitexa\Core\Server\Lifecycle\ServerLifecycleListenerInterface;
use Semitexa\Core\Server\Lifecycle\ServerLifecyclePhase;
use Semitexa\Ledger\Application\Service\CommandBus;
use Semitexa\Ledger\Application\Service\CommandProcessor;
use Semitexa\Ledger\Application\Service\CommandRegistry;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Semitexa\Ledger\Application\Service\LedgerPublisher;
use Semitexa\Ledger\Application\Service\LedgerReplayer;
use Semitexa\Ledger\Application\Service\LedgerSchema;
use Semitexa\Ledger\Application\Service\LedgerWriter;
use Semitexa\Ledger\Application\Service\ReplayHandlerRegistry;
use Semitexa\Ledger\Application\Service\Nats\ClusterHealthTracker;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;
use Semitexa\Ledger\Application\Service\AggregateOwnershipService;
use Semitexa\Ledger\Application\Service\OwnershipCache;
use Semitexa\Ledger\Application\Service\Queue\DualWriteTransport;
use Semitexa\Ledger\Application\Service\Queue\NatsTransportFactory;
use Semitexa\Orm\Application\Service\Connection\ConnectionRegistry;

/**
 * Wires up the ledger on each Swoole worker startup:
 *
 *  1. Initialize the SQLite ledger (schema migration).
 *  2. Connect to all configured NATS clusters.
 *  3. Register NatsTransport with QueueTransportRegistry.
 *  4. Hook LedgerWriter into EventDispatcher's post-dispatch pipeline.
 *  5. Start LedgerPublisher + LedgerReplayer as background Swoole coroutines.
 *  6. Start CommandProcessor NATS subscription loop.
 *
 * Required environment variables:
 *   LEDGER_ENABLED    — set to 1/true/yes/on to enable the ledger explicitly
 *   LEDGER_NODE_ID    — unique node identifier (e.g. "store-a")
 *   LEDGER_HMAC_KEY   — HMAC secret for ledger signing
 *   NATS_URL / NATS_PRIMARY_URL — primary NATS server
 *
 * Optional:
 *   LEDGER_DB_PATH         — SQLite file path (default: /var/lib/semitexa/ledger/{node}.sqlite)
 *   LEDGER_DB_CONNECTION   — ORM connection name for aggregate_ownership table (default: "default")
 *   NATS_SECONDARY_URL     — enables secondary cluster for HA
 *   EVENTS_DUAL_PRIMARY    — enables dual-write mode (primary transport name)
 *   EVENTS_DUAL_SECONDARY  — secondary transport name
 */
#[AsServerLifecycleListener(
    phase: ServerLifecyclePhase::WorkerStartAfterContainer->value,
    priority: 100,
)]
final class LedgerBootstrap implements ServerLifecycleListenerInterface
{
    public function handle(ServerLifecycleContext $context): void
    {
        if (!$this->shouldBootLedger()) {
            return;
        }

        $nodeId  = $this->requireEnv('LEDGER_NODE_ID');
        $hmacKey = $this->requireEnv('LEDGER_HMAC_KEY');
        $dbPath  = (string) (getenv('LEDGER_DB_PATH') ?: "/var/lib/semitexa/ledger/{$nodeId}.sqlite");

        $this->ensureDir($dbPath);

        // 1. Initialize SQLite ledger.
        $db = new LedgerConnection($dbPath);
        (new LedgerSchema($db, $nodeId))->migrate();

        // 2. Connect to NATS clusters.
        $clusters = ClusterRegistry::fromEnv();
        $clusters->connect();

        $health = new ClusterHealthTracker();

        // 3. Register queue transports.
        $natsFactory = new NatsTransportFactory($clusters);
        QueueTransportRegistry::register('nats', $natsFactory);

        $dualPrimary   = getenv('EVENTS_DUAL_PRIMARY')   ?: null;
        $dualSecondary = getenv('EVENTS_DUAL_SECONDARY')  ?: null;
        if ($dualPrimary !== null && $dualSecondary !== null) {
            QueueTransportRegistry::register('dual', new class($dualPrimary, $natsFactory) implements \Semitexa\Core\Queue\QueueTransportFactoryInterface {
                public function __construct(
                    private readonly string $primaryName,
                    private readonly NatsTransportFactory $natsFactory,
                ) {}

                public function create(): \Semitexa\Core\Queue\QueueTransportInterface
                {
                    return new DualWriteTransport(
                        QueueTransportRegistry::create($this->primaryName),
                        $this->natsFactory->create(),
                    );
                }
            });
        }

        // Shared services.
        $container      = ContainerFactory::get();
        $ownershipCache = new OwnershipCache();

        // Resolve the ORM database adapter for the aggregate_ownership table.
        // By default uses the 'default' connection; override via LEDGER_DB_CONNECTION env var.
        $dbConnection = (string) (getenv('LEDGER_DB_CONNECTION') ?: 'default');
        $connectionRegistry = $container->get(ConnectionRegistry::class);
        $ownershipAdapter = $connectionRegistry->manager($dbConnection)->getAdapter();

        $ownership = new AggregateOwnershipService(
            $nodeId,
            $ownershipAdapter,
            $ownershipCache,
        );

        $handlerRegistry = new ReplayHandlerRegistry(
            $container->get(\Semitexa\Core\Discovery\ClassDiscovery::class),
        );

        // 4. Hook LedgerWriter into EventDispatcher.
        $writer = new LedgerWriter($db, $nodeId, $hmacKey, $ownership);

        /** @var EventDispatcher $dispatcher */
        $dispatcher = $container->get(EventDispatcher::class);
        $dispatcher->addPostDispatchHook(function (object $event) use ($writer): void {
            try {
                $writer->append($event);
            } catch (\Throwable $e) {
                error_log('[semitexa-ledger] LedgerWriter::append failed: ' . $e->getMessage());
            }
        });

        // 5. Start LedgerPublisher background coroutine.
        $publisher = new LedgerPublisher($db, $nodeId, $clusters, $health);
        \Swoole\Coroutine::create(fn () => $publisher->runRetryLoop());

        // 6. Start LedgerReplayer (starts per-cluster coroutines internally).
        $replayer = new LedgerReplayer($db, $nodeId, $hmacKey, $clusters, $handlerRegistry, $ownership);
        $replayer->start();

        // 7. Start CommandProcessor subscription.
        $commandRegistry = new CommandRegistry(
            $container->get(\Semitexa\Core\Discovery\ClassDiscovery::class),
        );
        $processor = new CommandProcessor($nodeId, $clusters, $ownership, $commandRegistry);
        $processor->startListeners();

        // 8. Register CommandBus in the container so application handlers can inject it.
        $commandBus = new CommandBus($nodeId, $ownership, $clusters, $processor);
        $container->set(CommandBus::class, $commandBus);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private function shouldBootLedger(): bool
    {
        $enabled = getenv('LEDGER_ENABLED');

        if ($enabled !== false && $enabled !== '') {
            if ($this->isTruthy($enabled)) {
                return true;
            }

            if ($this->isFalsy($enabled)) {
                return false;
            }
        }

        return getenv('LEDGER_NODE_ID') !== false
            && getenv('LEDGER_HMAC_KEY') !== false
            && (getenv('NATS_URL') !== false || getenv('NATS_PRIMARY_URL') !== false);
    }

    private function requireEnv(string $key): string
    {
        $value = getenv($key);
        if ($value === false || $value === '') {
            throw new \RuntimeException(
                "semitexa-ledger: Required environment variable '{$key}' is not set."
            );
        }
        return $value;
    }

    private function isTruthy(string $value): bool
    {
        return in_array(strtolower(trim($value)), ['1', 'true', 'yes', 'on'], true);
    }

    private function isFalsy(string $value): bool
    {
        return in_array(strtolower(trim($value)), ['0', 'false', 'no', 'off'], true);
    }

    private function ensureDir(string $dbPath): void
    {
        $dir = dirname($dbPath);
        if (!is_dir($dir) && !mkdir($dir, 0755, true) && !is_dir($dir)) {
            throw new \RuntimeException("Could not create ledger directory: {$dir}");
        }
    }

}
