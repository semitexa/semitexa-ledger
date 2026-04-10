<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Queue;

use Semitexa\Core\Queue\QueueTransportFactoryInterface;
use Semitexa\Core\Queue\QueueTransportInterface;
use Semitexa\Ledger\Nats\ClusterRegistry;

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
