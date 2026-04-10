<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Exception;

use Semitexa\Core\Exception\DomainException;
use Semitexa\Core\Http\HttpStatus;

/**
 * Thrown when ownership of an aggregate cannot be resolved — the aggregate
 * is unknown to the current node's ownership registry.
 */
final class AggregateNotFoundException extends DomainException
{
    public function __construct(
        public readonly string $aggregateType,
        public readonly string $aggregateId,
    ) {
        parent::__construct(sprintf(
            'No ownership record found for aggregate %s:%s.',
            $aggregateType,
            $aggregateId,
        ));
    }

    public function getStatusCode(): HttpStatus
    {
        return HttpStatus::NotFound;
    }
}
