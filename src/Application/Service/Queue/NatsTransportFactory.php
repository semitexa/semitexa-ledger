<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Queue;

use Semitexa\Core\Queue\QueueTransportFactoryInterface;
use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Ledger\Application\Service\Nats\ClusterRegistry;

final class NatsTransportFactory implements QueueTransportFactoryInterface
{
    public function __construct(
        private readonly ClusterRegistry $clusters,
    ) {}

    public function create(): QueueTransportInterface
    {
        return new NatsTransport($this->clusters);
    }
}
