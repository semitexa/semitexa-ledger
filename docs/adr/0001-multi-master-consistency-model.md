# ADR 0001 — Multi-master consistency model

- Status: accepted (2026-09-28)
- Epic: `ep-multi-node-sync`
- Decided by: the operator, from the options below

## Context

Semitexa is meant to run as many clones of one project on servers spread around
the world, storing data on all of their disks, and to **survive network
partitions that last weeks** without manual intervention. Every node must keep
accepting writes while it cannot see the others, and all nodes must converge to
the same data once they can.

That is an AP system in CAP terms. It rules out any model in which a write needs
a remote node's answer — including routing every write to an aggregate's owner
(`#[OwnedAggregate]` + `CommandBus`), because an owner behind a partition would
make its aggregates read-only for everyone else.

Facts from the code that shape the design (2026-09-28):

- The ORM has no dirty tracking: `AggregateWriteEngine` UPDATEs every column and
  never reads old values. `ResourceChangedEvent` carries no data and is
  dispatched after commit.
- Cascade children and pivot rows are written as delete-then-reinsert, with no
  identity of their own.
- Many packages write their tables with raw SQL, bypassing the engine.
- The ledger (SQLite per node) already behaves as an outbox towards NATS: events
  wait as `pending` while NATS is unreachable.

## Decision

### 1. Every node accepts every write (AP, strong eventual consistency)

No write waits for another node. Nodes that have received the same set of
changes hold the same data, whatever order the changes arrived in.

### 2. Field-level last-writer-wins, ordered by a hybrid logical clock

- Each node keeps a **hybrid logical clock** (HLC: physical time + logical
  counter, advanced on every local write and on every received change), so a
  node with a skewed wall clock cannot win forever, and a change is never
  ordered before one it has seen.
- Every **field** of a replicated row carries the HLC timestamp and node id of
  the write that set it. A remote change wins a field only if its (HLC, node id)
  is greater. The comparison is total, so every node picks the same winner.
- Merging is commutative, associative and idempotent: replaying a change, or
  receiving it twice, or out of order, changes nothing. This also removes the
  per-aggregate ordering problem of retried applies.

### 3. Deletes are tombstones

A delete records a tombstone with its HLC. A later write (by HLC) resurrects the
row; an earlier one is discarded. Tombstones are kept at least as long as the
stream retention (see 8).

### 4. Relations are fields holding a set

Pivot rows and owned children are replicated as one value per relation — the
whole set — merged with the same field rule. (Later work may refine this to an
add/remove set.)

### 5. Invariants that cannot merge are accepted, then reported

Unique values (an e-mail) and bounds (stock ≥ 0) cannot be guaranteed while
nodes are apart. Nodes accept the writes; when applying a remote change breaks a
constraint, the node records a **replication conflict** (both versions, both
clocks) and dispatches a local event for application code to resolve — for
example by merging two accounts. The resolution is an ordinary replicated write.
All fields not involved in the conflict still converge.
*Implemented 2026-09-29:* the journal is `replication_conflict` (one row per
conflict, keyed by the unapplied fields' stamps, so replays do not repeat it)
and the event is `ReplicationConflictDetected`, dispatched once, after the
partial apply commits. A conflict here means a unique value or a CHECK bound;
a missing parent or a NOT NULL gap still fails the apply and is retried. An
unapplied field keeps no clock, so the next change of the row tries it again —
once the value is free, it lands. A new row that breaks a constraint is not
created at all: a row cannot exist in part.
*Retry, 2026-09-30:* a conflict is usually resolved by writing ANOTHER row (the
one holding the value), and nothing about that write reaches the row kept out.
The journal keeps the change as received, and `ReplicationConflicts::retry(key)`
— called by the resolver after its write — applies it again through the same
merge, so a field a newer write has taken since still loses by its clock. The
journal closes a conflict itself (`resolved_at`) when the row or its fields
land, or a newer write overtakes them, whichever path brought that about.
Chosen over re-trying on every write of the table (cost on the hot path) and a
periodic sweep (a timer deciding what the application meant): resolution stays
in application code, as above.

### 6. Replication is opt-in per resource: `#[Replicated]`

- Only resource models marked `#[Replicated]` leave the node. Infrastructure
  tables — queues, leases, locks, sessions, caches — never do.
- A replicated resource must use a UUIDv7 primary key (`auto` would collide).
- A replicated resource must be written through the ORM engine; writing its
  table any other way is refused, because such writes would never be
  captured. *Amended 2026-09-28:* the refusal is a runtime guard in the ORM
  adapters (`ReplicatedWriteGuard`), not a static rule — nearly every raw
  write in the codebase names its table through a variable (`UPDATE %s`,
  `new InsertQuery($metadata->tableName, …)`), which no static rule can
  follow. The adapters see the real table name; only the write engine and
  the replication applier are permitted.

### 7. Capture: a transactional outbox in the application database

- For a replicated resource, the ORM engine, inside the write transaction, reads
  the current row, diffs it against the new values and writes the changed
  fields with their clocks — and one outbox row — **in the same MySQL
  transaction** as the data. A crash can lose neither half.
- A relay in the ledger worker moves outbox rows into the SQLite ledger (the
  outbox row id is the event id, so a crash between the two steps repeats
  harmlessly) and the publisher sends them on.
- Applying a remote change writes data and clocks without capturing it again,
  so changes never echo back.
- The ORM defines the capture contract with a no-op default; the ledger
  implements it. The ORM does not depend on the ledger.

### 8. Partitions of weeks: 30-day stream retention, snapshots beyond

- The event stream keeps changes for 30 days, so a node that was cut off for
  weeks catches up from the stream by itself.
- A node away for longer, or a new node, is seeded from a snapshot of a peer and
  then catches up from the stream (`tk-mn-node-bootstrap`).

### 9. Mixed versions after a partition

Nodes may run different code versions when they meet again. A change naming a
column this node does not have yet is stored in the ledger and its known fields
applied; after the node upgrades, replaying the ledger applies the rest —
harmless, because merging is idempotent.

## Consequences

- Every write to a replicated resource costs one extra read (the row before the
  write) and writes clock rows in the same transaction.
- "Last writer wins" loses the earlier of two concurrent edits to the same field
  — by design; it is the price of never blocking a write.
- Application code that needs a guaranteed unique value must handle replication
  conflicts.
- `#[OwnedAggregate]` and `CommandBus` stay unused; `CommandBus` is not
  registered in the container. They may return later as an opt-in for data that
  must not diverge, accepting that such data is read-only during a partition.

## Not decided here

- Per-node signing keys instead of one shared HMAC key (one compromised node can
  currently forge events for all).
- Counters (merge by sum instead of last-writer-wins) for numeric fields.
- Replicating across tenants with different data-residency rules.
