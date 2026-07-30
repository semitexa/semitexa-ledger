<?php

declare(strict_types=1);

namespace Semitexa\Ledger;

use Semitexa\Core\Attribute\Capability;

/**
 * What this package offers, for the capability catalog.
 *
 * Without this the package is invisible to anyone whose project has not
 * installed it - which is precisely the audience worth telling, since they are
 * the ones about to build it by hand. The convention is one `Capabilities` class
 * per package: a definite place to look, and a definite place for a guard to
 * check.
 *
 * Nothing reads this at runtime.
 */
#[Capability(
    id: 'ledger.events',
    summary: 'An append-only, hash-chained event ledger with NATS JetStream propagation between nodes.',
    useWhen: 'What happened must stay provable after the fact, or several nodes must agree on an ordered history.',
    avoidWhen: 'A log line answers the question. Tamper evidence you do not need is cost you do.',
    replaces: [
        'an audit table anyone with UPDATE can rewrite',
        'a message broker wired up by hand, with ordering and replay reinvented per aggregate',
    ],
)]
final class Capabilities
{
}
