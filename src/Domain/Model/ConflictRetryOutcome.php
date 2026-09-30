<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Model;

/** What ReplicationConflicts::retry() did with a journaled conflict. */
enum ConflictRetryOutcome: string
{
    /** Applied again, and it holds no more: the row or its fields landed, or a newer write overtook them. */
    case Resolved = 'resolved';

    /** Applied again, and the constraint still refuses it; it stays journaled and is not announced again. */
    case StillOpen = 'still_open';

    /** Already resolved before this call; nothing was applied. */
    case AlreadyResolved = 'already_resolved';

    /** No conflict has this key on this node. */
    case Unknown = 'unknown';

    /** Journaled before the change itself was kept, so there is nothing to apply again. */
    case NotRetryable = 'not_retryable';
}
