#!/usr/bin/env bash
# Two-node replication harness.
#
# Boots two full Semitexa servers (own database, Redis, ledger each) against one
# NATS JetStream server and proves that events cross between them:
#   1. a probe written on node-a arrives and is applied on node-b
#   2. the same the other way round
#   3. a burst of BURST probes from node-a all arrive on node-b in order,
#      nothing quarantined, nothing left unapplied
#
# Usage (from anywhere):
#   packages/semitexa-ledger/tests/Harness/two-node/run.sh          # run, then tear down
#   KEEP=1 packages/semitexa-ledger/tests/Harness/two-node/run.sh   # leave the stack up
#
# Exit code 0 only when every check passed.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SEMITEXA_ROOT="${SEMITEXA_ROOT:-$(cd "$HERE/../../../../.." && pwd)}"
export HARNESS_UID="${HARNESS_UID:-$(id -u)}"
export HARNESS_GID="${HARNESS_GID:-$(id -g)}"
BURST="${BURST:-20}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"

compose() { docker compose -f "$HERE/docker-compose.yml" "$@"; }
cli() { local node="$1"; shift; compose exec -T "$node" php vendor/bin/semitexa "$@"; }
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; dump_logs; exit 1; }

dump_logs() {
    for node in node-a node-b; do
        echo "--- $node: last server log lines"
        compose exec -T "$node" sh -c 'tail -n 40 /tmp/swoole-*.log* var/log/*.log 2>/dev/null' || true
        compose logs --tail=40 "$node" 2>/dev/null || true
    done
}

cleanup() {
    if [ "${KEEP:-0}" = "1" ]; then
        echo "KEEP=1 — stack left running: docker compose -f $HERE/docker-compose.yml down -v"
    else
        compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# Wait until $2 (a shell snippet) succeeds, or fail with $1.
wait_for() {
    local what="$1" check="$2" deadline=$((SECONDS + WAIT_SECONDS))
    until eval "$check" >/dev/null 2>&1; do
        [ $SECONDS -lt $deadline ] || fail "timed out after ${WAIT_SECONDS}s waiting for $what"
        sleep 1
    done
}

# json_field <json> <python expression over d>
json_field() { python3 -c "import json,sys; d=json.loads(sys.argv[1]); print($2)" "$1"; }

say "Infrastructure: mysql (node_a, node_b), nats, redis-a, redis-b"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d --wait mysql nats redis-a redis-b >/dev/null

for node in node-a node-b; do
    say "Schema on $node"
    compose run --rm -T "$node" php vendor/bin/semitexa orm:sync --allow-destructive 2>&1 | tail -n 2
done

say "Servers"
compose up -d node-a node-b >/dev/null
for node in node-a node-b; do
    wait_for "$node ledger boot" "cli $node ledger:status --json"
    echo "$node: ledger up"
done

# probe <from> <to>: write a probe on <from>, wait until <to> has applied it.
probe() {
    local from="$1" to="$2" out id
    out="$(cli "$from" ledger:probe --json --note="$from->$to")"
    id="$(json_field "$out" "d['probe_id']")"
    echo "$from wrote probe $id (seq $(json_field "$out" "d['sequence']"))"
    wait_for "probe $id on $to" \
        "[ \"\$(json_field \"\$(cli $to ledger:status --json --probe=$id)\" \"d['probe']['applied']\")\" = True ]"
    echo "$to applied it"
}

say "1. node-a → node-b"
probe node-a node-b

say "2. node-b → node-a"
probe node-b node-a

say "3. burst of $BURST probes from node-a"
for _ in $(seq 1 "$BURST"); do cli node-a ledger:probe --json >/dev/null; done
expected=$((BURST + 1))
wait_for "$expected node-a events on node-b" \
    "[ \"\$(json_field \"\$(cli node-b ledger:status --json)\" \"sum(o['events'] for o in d['origins'] if o['origin']=='node-a')\")\" = $expected ]"

status_b="$(cli node-b ledger:status --json)"
echo "$status_b" | python3 -m json.tool
unapplied="$(json_field "$status_b" "sum(o['unapplied'] for o in d['origins'])")"
quarantined="$(json_field "$status_b" "d['quarantined']")"
last_seq="$(json_field "$status_b" "max(o['last_sequence'] for o in d['origins'] if o['origin']=='node-a')")"
[ "$unapplied" = 0 ] || fail "node-b left $unapplied remote event(s) unapplied"
[ "$quarantined" = 0 ] || fail "node-b quarantined $quarantined event(s)"
[ "$last_seq" = "$expected" ] || fail "node-b has node-a up to seq $last_seq, expected $expected"

for node in node-a node-b; do
    cli "$node" ledger:verify >/dev/null || fail "ledger:verify failed on $node"
done

say "PASS: both directions + burst of $BURST, chains verified on both nodes"
