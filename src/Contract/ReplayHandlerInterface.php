<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Contract;

use Semitexa\Ledger\Domain\Contract\ReplayHandlerInterface as DomainReplayHandlerInterface;

/**
 * @deprecated Use Semitexa\Ledger\Domain\Contract\ReplayHandlerInterface instead.
 */
if (!interface_exists(ReplayHandlerInterface::class, false)) {
    class_alias(DomainReplayHandlerInterface::class, ReplayHandlerInterface::class);
}
