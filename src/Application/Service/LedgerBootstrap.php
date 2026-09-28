<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Core\Attribute\AsServerLifecycleListener;
use Semitexa\Core\Event\EventDispatcher;
use Semitexa\Core\Queue\QueueTransportRegistry;
use Semitexa\Core\Server\Lifecycle\ServerLifecycleContext;
use Semitexa\Core\Server\Lifecycle\ServerLifecycleListenerInterface;
use Semitexa\Core\Server\Lifecycle\ServerLifecyclePhase;
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
use Semitexa\Ledger\Application\Service\Nats\EventStream;
use Semitexa\Ledger\Application\Service\Replication\ReplicationCaptureService;
use Semitexa\Ledger\Application\Service\Replication\ReplicationRelay;
use Semitexa\Orm\Application\Service\Persistence\ReplicationCapture;
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
 *  5. Start LedgerPublisher + LedgerReplayer as background Swoole coroutines,
 *     in worker 0 only (see ownsBackgroundLoops()).
 *  6. Start the CommandProcessor NATS subscription loop, same worker.
 *
 * CommandBus is not registered in the container (see the end of boot()).
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
 *   LEDGER_STREAM          — JetStream stream name (default: EVENTS)
 *   LEDGER_SUBJECT_PREFIX  — event subject namespace (default: semitexa.events);
 *                            distinct per project sharing one NATS server
 */
#[AsServerLifecycleListener(
    phase: ServerLifecyclePhase::WorkerStartAfterContainer->value,
    priority: 100,
)]
class LedgerBootstrap implements ServerLifecycleListenerInterface
{
    /**
     * Per-worker guard. The WorkerStartAfterContainer phase can re-run (the OS
     * listeners carry the same guard for exactly this reason), and the ledger
     * must boot ONCE per worker: a second run would add a duplicate post-dispatch
     * hook to the long-lived EventDispatcher — every ledger event then appended
     * twice and the hook list growing unbounded — and spawn a second set of
     * background Publisher/Replayer/CommandProcessor coroutines.
     */
    private static bool $booted = false;

    public function handle(ServerLifecycleContext $context): void
    {
        if (!$this->shouldBootLedger()) {
            return;
        }
        if (self::$booted) {
            return;
        }
        self::$booted = true;

        $this->boot($context);
    }

    /** Reset the boot guard (worker-stop / test hygiene). */
    public static function reset(): void
    {
        self::$booted = false;
    }

    protected function boot(ServerLifecycleContext $context): void
    {
        // The container arrives via the lifecycle context (populated for
        // post-container phases) — the DI-compliant path, so this composition
        // root no longer reaches for the static ContainerFactory. Fail fast if
        // it is absent before touching any infrastructure.
        $container = $context->container ?? throw new \RuntimeException(
            'LedgerBootstrap requires the application container on the WorkerStartAfterContainer lifecycle context.',
        );

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
        $ownershipCache = new OwnershipCache();

        // Resolve the ORM database adapter for the aggregate_ownership table.
        // By default uses the 'default' connection; override via LEDGER_DB_CONNECTION env var.
        $dbConnection = (string) (getenv('LEDGER_DB_CONNECTION') ?: 'default');
        $connectionRegistry = $container->get(ConnectionRegistry::class);
        if (!$connectionRegistry instanceof ConnectionRegistry) {
            throw new \RuntimeException('LedgerBootstrap needs the ORM ConnectionRegistry in the container.');
        }
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
        $dispatcher->addPostDispatchHook(new LedgerDispatchHook($writer->append(...)));

        // Writes to #[Replicated] resources are captured in their own
        // transaction (ADR 0001). Every worker writes, so every worker captures.
        $capture = new ReplicationCaptureService($nodeId, new HybridLogicalClock());
        ReplicationCapture::setResolver(static fn (): ReplicationCaptureService => $capture);

        // 5-7. Background loops: publisher, replayer, command listener.
        $commandRegistry = new CommandRegistry(
            $container->get(\Semitexa\Core\Discovery\ClassDiscovery::class),
        );

        if ($this->ownsBackgroundLoops($context)) {
            $relay = new ReplicationRelay(
                $writer,
                static fn (): \Semitexa\Orm\Adapter\DatabaseAdapterInterface => $connectionRegistry->manager($dbConnection)->getAdapter(),
            );
            \Swoole\Coroutine::create(static fn () => $relay->run());

            $this->startBackgroundLoops(
                $db,
                $nodeId,
                $hmacKey,
                $health,
                $handlerRegistry,
                $ownership,
                $commandRegistry,
                static fn (string $class): object => $container->resolve($class),
            );
        }

        // CommandBus is NOT registered. This used to $container->set() it here,
        // but the container is sealed by WorkerStartAfterContainer, so the call
        // threw and took down every worker of any server with the ledger
        // enabled — found by the two-node harness. Nothing injects it yet; it
        // gets a container registration together with the ownership design
        // (ep-multi-node-sync, tk-mn-conflict-model).
    }

    /**
     * The background loops run in ONE worker per node. Every worker used to
     * start its own set: N replayers shared the node's durable consumer, so a
     * worker received event 7 while another still held 6 and naked it as a
     * gap; N publishers raced over the same pending rows; and N plain NATS
     * subscriptions ran every routed command N times. The writer hook stays in
     * every worker — any worker can dispatch an event.
     */
    protected function ownsBackgroundLoops(ServerLifecycleContext $context): bool
    {
        return $context->workerId === 0;
    }

    /**
     * Each loop gets its own NATS connections. basis-company/nats reads replies
     * off one socket; two coroutines waiting on the same client steal each
     * other's frames (a publisher's PubAck consumed by the replayer's pull).
     *
     * @param \Closure(class-string): object $resolveHandler
     */
    private function startBackgroundLoops(
        LedgerConnection $db,
        string $nodeId,
        string $hmacKey,
        ClusterHealthTracker $health,
        ReplayHandlerRegistry $handlerRegistry,
        AggregateOwnershipService $ownership,
        CommandRegistry $commandRegistry,
        \Closure $resolveHandler,
    ): void {
        $stream = EventStream::fromEnv();

        $publisherClusters = ClusterRegistry::fromEnv();
        $publisherClusters->connect();
        $publisher = new LedgerPublisher($db, $nodeId, $publisherClusters, $health, $stream);
        \Swoole\Coroutine::create(fn () => $publisher->runRetryLoop());

        $replayerClusters = ClusterRegistry::fromEnv();
        $replayerClusters->connect();
        $replayer = new LedgerReplayer(
            $db,
            $nodeId,
            $hmacKey,
            $replayerClusters,
            $handlerRegistry,
            $ownership,
            $resolveHandler,
            $stream,
        );
        $replayer->start();

        $listenerClusters = ClusterRegistry::fromEnv();
        $listenerClusters->connect();
        (new CommandProcessor($nodeId, $listenerClusters, $ownership, $commandRegistry))->startListeners();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private function shouldBootLedger(): bool
    {
        return self::isEnabled();
    }

    /**
     * Whether this process runs as a ledger node. Shared with the console
     * wiring, so a command and a worker never disagree about it.
     */
    public static function isEnabled(): bool
    {
        $enabled = getenv('LEDGER_ENABLED');

        if ($enabled !== false && $enabled !== '') {
            $normalized = strtolower(trim($enabled));
            if (in_array($normalized, ['1', 'true', 'yes', 'on'], true)) {
                return true;
            }

            if (in_array($normalized, ['0', 'false', 'no', 'off'], true)) {
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

    private function ensureDir(string $dbPath): void
    {
        $dir = dirname($dbPath);
        if (!is_dir($dir) && !mkdir($dir, 0755, true) && !is_dir($dir)) {
            throw new \RuntimeException("Could not create ledger directory: {$dir}");
        }
    }

}
