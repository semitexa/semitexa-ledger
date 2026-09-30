#!/usr/bin/env bash
# Two-node replication harness.
#
# Boots two full Semitexa servers (own database, Redis, ledger each) against one
# NATS JetStream server and proves that events cross between them:
#   1. a probe written on node-a arrives and is applied on node-b
#   2. the same the other way round
#   3. a burst of BURST probes from node-a all arrive on node-b in order,
#      nothing quarantined, nothing left unapplied
#   4. rows of a #[Replicated] resource written on one node appear on the other
#   5. a partition: both nodes cut off from NATS, both edit the same rows —
#      different fields, the same field, a delete against a later edit — and
#      after the network heals both hold the same, expected rows
#   6. a replication conflict: apart, both nodes create a row with the same
#      unique title. After the heal each journals and announces the other's row
#      once, keeps it out, and the stream still flows past it; renaming one
#      row (an ordinary replicated write) and retrying the conflict on that
#      node lets both converge, and both journals close it
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
    local what="$1" check="$2" deadline=$((SECONDS + ${WAIT_SECONDS:-60}))
    until eval "$check" >/dev/null 2>&1; do
        [ $SECONDS -lt $deadline ] || fail "timed out waiting for $what"
        sleep 1
    done
}

# json_field <json> <python expression over d>
json_field() { python3 -c "import json,sys; d=json.loads(sys.argv[1]); print($2)" "$1"; }

say "Infrastructure: nats, mysql-a, mysql-b, redis-a, redis-b"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d --wait nats mysql-a mysql-b redis-a redis-b >/dev/null

for node in node-a node-b; do
    say "Schema on $node"
    out="$(compose run --rm -T "$node" php vendor/bin/semitexa orm:sync --allow-destructive 2>&1)" \
        || { printf '%s\n' "$out" | grep -v ' Container ' | tail -n 20; fail "orm:sync failed on $node"; }
    printf '%s\n' "$out" | tail -n 2
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

note() { local node="$1"; shift; cli "$node" harness:note "$@"; }
dump() { cli "$1" harness:note dump; }
wan() { # wan <connect|disconnect> <node>
    docker network "$1" "semitexa-2node_wan" "$(compose ps -q "$2")"
}
N1=01a0e7b0-0000-7000-8000-000000000001
N2=01a0e7b0-0000-7000-8000-000000000002
N3=01a0e7b0-0000-7000-8000-000000000003

say "4. replicated rows: node-a writes, node-b receives"
note node-a create --id=$N1 --title=T1 --body=B1
note node-a create --id=$N3 --title=T3 --body=B3
wait_for "node-a's rows on node-b" '[ "$(dump node-b)" = "$(dump node-a)" ] && [ "$(dump node-b)" != "[]" ]'
echo "node-b: $(dump node-b)"

say "5. partition: both nodes cut off from NATS"
wan disconnect node-a
wan disconnect node-b
note node-a set --id=$N1 --field=title --value=A-title
note node-a create --id=$N2 --title=T2 --body=only-on-A
note node-a delete --id=$N3
sleep 1.2
note node-b set --id=$N1 --field=body --value=B-body
note node-b set --id=$N1 --field=title --value=B-title
note node-b set --id=$N3 --field=body --value=B-kept-it
sleep "${PARTITION_SECONDS:-10}"
echo "during partition, node-a: $(dump node-a)"
echo "during partition, node-b: $(dump node-b)"
[ "$(dump node-a)" != "$(dump node-b)" ] || fail "the nodes agree during the partition — it did not hold"

say "   heal"
wan connect node-a
wan connect node-b
expected="$(python3 -c 'import json,sys; print(json.dumps(sorted([
    {"id": sys.argv[1], "title": "B-title", "body": "B-body"},
    {"id": sys.argv[2], "title": "T2", "body": "only-on-A"},
    {"id": sys.argv[3], "title": "T3", "body": "B-kept-it"},
], key=lambda r: r["id"]), ensure_ascii=False, separators=(",", ":")))' $N1 $N2 $N3)"
same_as_expected() {
    python3 -c 'import json,sys; sys.exit(0 if json.loads(sys.argv[1]) == json.loads(sys.argv[2]) else 1)' "$(dump "$1")" "$expected"
}
WAIT_SECONDS="${HEAL_WAIT_SECONDS:-120}" wait_for "node-a to converge after the heal" "same_as_expected node-a"
WAIT_SECONDS="${HEAL_WAIT_SECONDS:-120}" wait_for "node-b to converge after the heal" "same_as_expected node-b"
echo "both nodes: $(dump node-a)"

for node in node-a node-b; do
    cli "$node" ledger:verify >/dev/null || fail "ledger:verify failed on $node after the partition"
    q="$(json_field "$(cli "$node" ledger:status --json)" "d['quarantined']")"
    [ "$q" = 0 ] || fail "$node quarantined $q event(s)"
done

conflicts() { cli "$1" harness:note conflicts; }
N4=01a0e7b0-0000-7000-8000-000000000004
N5=01a0e7b0-0000-7000-8000-000000000005
N6=01a0e7b0-0000-7000-8000-000000000006
has_row() { # has_row <node> <id>
    python3 -c 'import json,sys; sys.exit(0 if any(r["id"] == sys.argv[2] for r in json.loads(sys.argv[1])) else 1)' "$(dump "$1")" "$2"
}

say "6. conflict: apart, both nodes take the same unique title"
wan disconnect node-a
wan disconnect node-b
note node-a create --id=$N4 --title=DUP --body=on-A
note node-b create --id=$N5 --title=DUP --body=on-B
sleep "${PARTITION_SECONDS:-10}"
wan connect node-a
wan connect node-b

# Past the conflict, the stream still flows — both ways. Before the journal,
# the conflicting event was retried forever and everything behind it waited.
note node-a create --id=$N6 --title=T6 --body=after-the-conflict
note node-b set --id=$N1 --field=body --value=after-the-conflict
WAIT_SECONDS="${HEAL_WAIT_SECONDS:-120}" wait_for "node-a's later row on node-b" "has_row node-b $N6"
WAIT_SECONDS="${HEAL_WAIT_SECONDS:-120}" wait_for "node-b's later edit on node-a" \
    "[ \"\$(json_field \"\$(dump node-a)\" \"[r['body'] for r in d if r['id']=='$N1'][0]\")\" = after-the-conflict ]"
echo "later changes crossed both ways"

for pair in "node-a $N5 $N4" "node-b $N4 $N5"; do
    set -- $pair
    node="$1" missing="$2" own="$3"
    has_row "$node" "$own" || fail "$node lost its own row $own"
    ! has_row "$node" "$missing" || fail "$node created $missing although its title is taken here"
    c="$(conflicts "$node")"
    echo "$node conflicts: $c"
    [ "$(json_field "$c" "[(j['row'], j['columns']) for j in d['journaled']] == [('$missing', [])]")" = True ] \
        || fail "$node did not journal exactly the conflict on $missing"
    [ "$(json_field "$c" "[a['row'] for a in d['announced']] == ['$missing']")" = True ] \
        || fail "$node did not announce the conflict on $missing exactly once"
done

say "   resolve: node-b renames its row, then retries the conflict it holds"
note node-b set --id=$N5 --field=title --value=DUP-b
# The rename is a change of N5, so node-a tries N5 again on its own. On node-b
# nothing about the rename reaches N4: the resolver retries it — nobody on
# node-a has to touch N4 again.
r="$(cli node-b harness:note retry --id=$N4)"
echo "node-b retry: $r"
[ "$r" = '["resolved"]' ] || fail "node-b's retry of $N4 did not resolve it: $r"
WAIT_SECONDS="${HEAL_WAIT_SECONDS:-120}" wait_for "both nodes to converge after the resolution" \
    '[ "$(dump node-a)" = "$(dump node-b)" ] && has_row node-a '$N5' && has_row node-b '$N4
echo "both nodes: $(dump node-a)"

for node in node-a node-b; do
    c="$(conflicts "$node")"
    [ "$(json_field "$c" "len(d['announced'])")" = 1 ] || fail "$node announced the conflict again: $c"
    [ "$(json_field "$c" "[j['row'] for j in d['journaled'] if j['open']]")" = "[]" ] || fail "$node still holds an open conflict: $c"
    cli "$node" ledger:verify >/dev/null || fail "ledger:verify failed on $node after the conflict"
    q="$(json_field "$(cli "$node" ledger:status --json)" "d['quarantined']")"
    [ "$q" = 0 ] || fail "$node quarantined $q event(s)"
done

say "PASS: probes both ways + burst of $BURST; replicated rows converge after a partition (field merge, same-field LWW, delete vs later edit); a unique-title conflict is journaled, announced once, does not block the stream, and converges once resolved"
