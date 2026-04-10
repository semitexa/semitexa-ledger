<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Command;

use Semitexa\Core\Container\ContainerFactory;
use Semitexa\Ledger\Nats\ClusterRegistry;
use Semitexa\Ledger\Nats\NatsClient;
use Semitexa\Ledger\Ownership\AggregateOwnershipService;

/**
 * Handles incoming aggregate commands on the owner node.
 *
 * Two paths:
 *
 *   handleLocally()   — Called by CommandBus when this node is the owner.
 *                       Finds the handler by convention and executes it directly.
 *
 *   startListeners()  — Called on worker boot. Subscribes to
 *                       `semitexa.commands.{type}.{nodeId}` for each aggregate
 *                       type this node currently owns.
 *
 * Command handlers are resolved from the container by the naming convention:
 *   aggregateType "product" + command class "UpdateProductPrice"
 *   → resolved from container as "UpdateProductPriceHandler" or similar
 *
 * Handlers are expected to:
 *   1. Validate input.
 *   2. Load and mutate domain state.
 *   3. Dispatch one or more events via EventDispatcher (which triggers LedgerWriter).
 *   4. Return a CommandResult.
 */
final class CommandProcessor
{
    public function __construct(
        private readonly string $nodeId,
        private readonly ClusterRegistry $clusters,
        private readonly AggregateOwnershipService $ownership,
    ) {}

    /**
     * Execute a command locally (same-node shortcut from CommandBus).
     *
     * Resolves a handler from the container by appending "Handler" to the
     * command class name. Handlers must implement a handle(Command): CommandResult method.
     */
    public function handleLocally(object $command): CommandResult
    {
        $handlerClass = get_class($command) . 'Handler';

        $container = ContainerFactory::get();

        if (!$container->has($handlerClass)) {
            throw new \RuntimeException(
                "No handler found for command " . get_class($command) . ". " .
                "Expected class: {$handlerClass}"
            );
        }

        $handler = $container->get($handlerClass);

        if (!method_exists($handler, 'handle')) {
            throw new \RuntimeException("Handler {$handlerClass} must have a handle() method.");
        }

        try {
            $result = $handler->handle($command);
            return $result instanceof CommandResult ? $result : CommandResult::ok();
        } catch (\Throwable $e) {
            return CommandResult::error($e->getMessage());
        }
    }

    /**
     * Subscribe to NATS command subjects for aggregate types owned by this node.
     * Each message is processed and a reply is published for request-reply callers.
     *
     * Called from the server lifecycle boot hook. Runs in a dedicated Swoole coroutine.
     */
    public function startListeners(): void
    {
        $client = $this->getPrimaryClient();
        if ($client === null) {
            return;
        }

        // Subscribe to all commands for this node.
        $wildcardSubject = "semitexa.commands.*.{$this->nodeId}";

        $client->subscribe($wildcardSubject, function (
            string $rawPayload,
            ?string $replyTo,
        ) use ($client): void {
            $result = $this->processCommandMessage($rawPayload);

            if ($replyTo !== null) {
                $client->publish($replyTo, $result->toJson());
            }
        });

        // Process incoming messages in a loop.
        \Swoole\Coroutine::create(function () use ($client): void {
            while (true) {
                try {
                    $client->process(1.0);
                } catch (\Throwable $e) {
                    error_log('[semitexa-ledger] CommandProcessor error: ' . $e->getMessage());
                    \Swoole\Coroutine::sleep(1.0);
                }
            }
        });
    }

    private function processCommandMessage(string $rawPayload): CommandResult
    {
        try {
            $envelope = json_decode($rawPayload, true, 512, JSON_THROW_ON_ERROR);
            $commandClass = $envelope['command_class'] ?? null;

            if ($commandClass === null || !class_exists($commandClass)) {
                return CommandResult::error("Unknown command class: {$commandClass}");
            }

            // Reconstruct command from envelope payload.
            $command = $this->hydrateCommand($commandClass, $envelope['payload'] ?? []);

            return $this->handleLocally($command);
        } catch (\Throwable $e) {
            error_log('[semitexa-ledger] Command processing error: ' . $e->getMessage());
            return CommandResult::error($e->getMessage());
        }
    }

    private function hydrateCommand(string $commandClass, array $payload): object
    {
        $ref  = new \ReflectionClass($commandClass);
        $ctor = $ref->getConstructor();

        if ($ctor === null) {
            return $ref->newInstance();
        }

        $args = [];
        foreach ($ctor->getParameters() as $param) {
            $name = $param->getName();
            if (array_key_exists($name, $payload)) {
                $args[] = $payload[$name];
            } elseif ($param->isDefaultValueAvailable()) {
                $args[] = $param->getDefaultValue();
            } elseif ($param->allowsNull()) {
                $args[] = null;
            } else {
                throw new \InvalidArgumentException(
                    "Missing required parameter '{$name}' for command {$commandClass}"
                );
            }
        }

        return $ref->newInstanceArgs($args);
    }

    private function getPrimaryClient(): ?NatsClient
    {
        $clusters = $this->clusters->getOrderedByPriority();
        return $clusters !== [] ? $clusters[0]['client'] : null;
    }
}
