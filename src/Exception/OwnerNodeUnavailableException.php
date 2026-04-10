<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Exception;

use Semitexa\Core\Exception\DomainException;
use Semitexa\Core\Http\HttpStatus;

/**
 * Thrown by CommandBus::sendAndWait() when the owner node does not respond
 * within the configured timeout — typically during a network partition.
 *
 * The caller should either retry later or surface a user-facing error.
 */
final class OwnerNodeUnavailableException extends DomainException
{
    public function __construct(
        public readonly string $aggregateType,
        public readonly string $aggregateId,
        public readonly string $ownerNodeId,
        public readonly float $timeout,
    ) {
        parent::__construct(sprintf(
            'Owner node "%s" for %s:%s did not respond within %.1f seconds.',
            $ownerNodeId,
            $aggregateType,
            $aggregateId,
            $timeout,
        ));
    }

    public function getStatusCode(): HttpStatus
    {
        return HttpStatus::ServiceUnavailable;
    }
}
