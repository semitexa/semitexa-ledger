<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Exception;

use Semitexa\Core\Exception\DomainException;
use Semitexa\Core\Http\HttpStatus;

/**
 * Thrown when a node attempts to emit an event for an aggregate it does not own.
 *
 * This is a programming error — the caller should have sent a command to the
 * owner node via CommandBus instead of dispatching the event directly.
 */
final class OwnershipViolationException extends DomainException
{
    public function __construct(
        public readonly string $aggregateType,
        public readonly string $aggregateId,
        public readonly string $ownerNodeId,
        public readonly string $currentNodeId,
    ) {
        parent::__construct(sprintf(
            'Node "%s" cannot emit events for %s:%s (owned by "%s"). Send a command to the owner node instead.',
            $currentNodeId,
            $aggregateType,
            $aggregateId,
            $ownerNodeId,
        ));
    }

    public function getStatusCode(): HttpStatus
    {
        return HttpStatus::Conflict;
    }
}
