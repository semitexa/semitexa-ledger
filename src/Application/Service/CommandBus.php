<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

use Semitexa\Ledger\Domain\Model\CommandResult;

use Semitexa\Core\Container\ContainerFactory;
use Semitexa\Core\Support\PayloadSerializer;
use Semitexa\Ledger\Attribute\AsAggregateCommand;
use Semitexa\Ledger\Exception\AggregateNotFoundException;
use Semitexa\Ledger\Exception\OwnerNodeUnavailableException;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;
use Semitexa\Ledger\Application\Service\AggregateOwnershipService;

/**
 * Routes aggregate mutation commands to the owning node.
 *
 * If the current node is the owner the command is executed directly (no NATS
 * round-trip). Otherwise it is serialized and published to the NATS subject
 * `semitexa.commands.{aggregate_type}.{owner_node_id}`.
 *
 * Two delivery modes:
 *
 *   send()         — fire-and-forget via JetStream (durable, retried by NATS).
 *   sendAndWait()  — NATS request-reply; returns CommandResult or throws on timeout.
 */
final class CommandBus
{
    public function __construct(
        private readonly string $nodeId,
        private readonly AggregateOwnershipService $ownership,
        private readonly ClusterRegistry $clusters,
        private readonly CommandProcessor $processor,
    ) {}

    /**
     * Send a command fire-and-forget.
     * If this node owns the aggregate, execute locally without NATS.
     */
    public function send(object $command): void
    {
        [$aggregateType, $aggregateId] = $this->readMeta($command);
        $ownerNode = $this->resolveOwnerOrFail($aggregateType, $aggregateId);

        if ($ownerNode === $this->nodeId) {
            $this->processor->handleLocally($command);
            return;
        }

        $subject = "semitexa.commands.{$aggregateType}.{$ownerNode}";
        $payload = $this->serialize($command);

        $client = $this->primaryClient();
        if ($client !== null) {
            $client->jetStreamPublish($subject, $payload);
        }
    }

    /**
     * Send a command and wait for the owner's response.
     *
     * @throws OwnerNodeUnavailableException on timeout
     */
    public function sendAndWait(object $command, float $timeout = 5.0): CommandResult
    {
        [$aggregateType, $aggregateId] = $this->readMeta($command);
        $ownerNode = $this->resolveOwnerOrFail($aggregateType, $aggregateId);

        if ($ownerNode === $this->nodeId) {
            return $this->processor->handleLocally($command);
        }

        $subject = "semitexa.commands.{$aggregateType}.{$ownerNode}";
        $payload = $this->serialize($command);

        $client = $this->primaryClient();
        if ($client === null) {
            throw new OwnerNodeUnavailableException($aggregateType, $aggregateId, $ownerNode, $timeout);
        }

        try {
            $responseJson = $client->request($subject, $payload, $timeout);
            return CommandResult::fromJson($responseJson);
        } catch (\RuntimeException) {
            throw new OwnerNodeUnavailableException($aggregateType, $aggregateId, $ownerNode, $timeout);
        }
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    /**
     * @return array{0: string, 1: string} [$aggregateType, $aggregateId]
     */
    private function readMeta(object $command): array
    {
        $class = get_class($command);
        $attrs = (new \ReflectionClass($class))->getAttributes(AsAggregateCommand::class);

        if ($attrs === []) {
            throw new \InvalidArgumentException(
                "Command class {$class} must be annotated with #[AsAggregateCommand]."
            );
        }

        /** @var AsAggregateCommand $meta */
        $meta        = $attrs[0]->newInstance();
        $idField     = $meta->aggregateIdField;
        $payload     = PayloadSerializer::toArray($command);
        $aggregateId = $this->readValue($command, $idField, $payload)
            ?? throw new \InvalidArgumentException("Command {$class} missing property '{$idField}'");

        return [$meta->aggregateType, $aggregateId];
    }

    private function resolveOwnerOrFail(string $aggregateType, string $aggregateId): string
    {
        $owner = $this->ownership->resolveOwner($aggregateType, $aggregateId);

        if ($owner === null) {
            throw new AggregateNotFoundException($aggregateType, $aggregateId);
        }

        return $owner;
    }

    private function primaryClient(): ?\Semitexa\Ledger\Application\Service\Nats\NatsClient
    {
        $clusters = $this->clusters->getOrderedByPriority();
        return $clusters !== [] ? $clusters[0]['client'] : null;
    }

    private function serialize(object $command): string
    {
        $class = get_class($command);
        $attrs = (new \ReflectionClass($class))->getAttributes(AsAggregateCommand::class);
        /** @var AsAggregateCommand $meta */
        $meta = $attrs[0]->newInstance();
        $payload = PayloadSerializer::toArray($command);

        return json_encode([
            'command_id'     => bin2hex(random_bytes(16)),
            'command_class'  => $class,
            'aggregate_type' => $meta->aggregateType,
            'aggregate_id'   => $this->readValue($command, $meta->aggregateIdField, $payload),
            'source_node'    => $this->nodeId,
            'payload'        => $payload,
            'sent_at'        => gmdate('Y-m-d\TH:i:s\Z'),
        ], JSON_THROW_ON_ERROR);
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function readValue(object $command, string $field, array $payload): ?string
    {
        $candidates = [$field];
        $camel = lcfirst(str_replace('_', '', ucwords($field, '_')));
        if (!in_array($camel, $candidates, true)) {
            $candidates[] = $camel;
        }

        foreach ($candidates as $candidate) {
            if (array_key_exists($candidate, $payload)) {
                $value = $payload[$candidate];
                return $value !== null ? (string) $value : null;
            }
        }

        foreach ($candidates as $candidate) {
            if (!property_exists($command, $candidate)) {
                continue;
            }

            $property = new \ReflectionProperty($command, $candidate);
            $property->setAccessible(true);
            $value = $property->getValue($command);

            return $value !== null ? (string) $value : null;
        }

        return null;
    }
}
