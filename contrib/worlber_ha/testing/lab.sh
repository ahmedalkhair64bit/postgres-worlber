#!/usr/bin/env bash
# worlber_ha Docker lab: three PostgreSQL nodes (pg1, pg2, pg3) with agents.
#
#   ./lab.sh up                 build the image and start pg1..pg3
#   ./lab.sh setup              create_cluster on pg1, add pg2 (sync) and pg3 (async)
#   ./lab.sh status             show worlber_ha.cluster_status
#   ./lab.sh psql [node]        interactive psql (default: current primary)
#   ./lab.sh sql "<query>"      run one query on the current primary
#   ./lab.sh kill <node>        hard crash of a node (container SIGKILL)
#   ./lab.sh start <node>       start a stopped/killed node again
#   ./lab.sh isolate <node>     cut a node off the network (partition)
#   ./lab.sh heal <node>        reconnect an isolated node
#   ./lab.sh monitor            health check, replication/redo queues, blocking, events
#   ./lab.sh agentlog [node]    agent log of a node, read through SQL
#   ./lab.sh logs <node>        follow agent logs of a node
#   ./lab.sh pglog <node>       follow PostgreSQL log of a node
#   ./lab.sh test               run the full automated failover test suite
#   ./lab.sh down               stop the lab and delete all its data
#
# Environment:
#   LAB_FAILOVER_TIMEOUT   failover_timeout used by setup (default 10 seconds)
#   LAB_CA_CERT            CA certificate for builds behind a TLS proxy
#   LAB_BUILD_ARGS         extra "docker build" arguments (e.g. --network host)
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$DIR/../../.." && pwd)
COMPOSE="$DIR/docker/docker-compose.yml"
NET=worlber-lab-net
NODES=(pg1 pg2 pg3)
declare -A IP=([pg1]=172.28.0.11 [pg2]=172.28.0.12 [pg3]=172.28.0.13 [witness]=172.28.0.20)
TIMEOUT=${LAB_FAILOVER_TIMEOUT:-10}

dc() { docker compose -f "$COMPOSE" "$@"; }
cid() { dc ps -aq "$1"; }
running() { [ "$(docker inspect -f '{{.State.Running}}' "$(cid "$1")" 2>/dev/null)" = true ]; }

# viapg <client node> <host> <port> <query>: connect over TCP (listener, VIP)
viapg() {
    local node=$1 host=$2 port=$3; shift 3
    dc exec -T -u postgres -e PGPASSWORD=postgres "$node" \
        psql -X -qAt -h "$host" -p "$port" -U postgres -d postgres -v ON_ERROR_STOP=1 -c "$*"
}

# sql <node> <query>: run a query, unaligned output
sql() {
    local node=$1; shift
    dc exec -T -u postgres "$node" psql -X -qAt -v ON_ERROR_STOP=1 -c "$*"
}

# anysql <query>: run on the first reachable node
anysql() {
    local n
    for n in "${NODES[@]}"; do
        running "$n" || continue
        if out=$(sql "$n" "$@" 2>/dev/null); then
            printf '%s\n' "$out"
            return 0
        fi
    done
    return 1
}

primary() {
    anysql "SELECT node FROM worlber_ha.cluster_status WHERE role = 'primary' AND state = 'running'" || true
}

# wait_until <seconds> <description> <command...>
wait_until() {
    local limit=$1 desc=$2; shift 2
    local start=$SECONDS
    while (( SECONDS - start < limit )); do
        if "$@" >/dev/null 2>&1; then
            echo "  ok: $desc ($((SECONDS - start))s)"
            return 0
        fi
        sleep 1
    done
    echo "  TIMEOUT: $desc"
    return 1
}

cmd_up() {
    echo "== building image worlber-pg:lab (first build compiles PostgreSQL, takes a few minutes)"
    # shellcheck disable=SC2086
    docker build ${LAB_BUILD_ARGS:-} ${LAB_CA_CERT:+--secret id=ca,src=$LAB_CA_CERT} \
        -f "$DIR/docker/Dockerfile" -t worlber-pg:lab "$ROOT"
    dc up -d --no-build pg1 pg2 pg3
    for n in "${NODES[@]}"; do
        wait_until 60 "agent on $n is up" dc exec -T "$n" test -S /var/run/postgresql/.s.WORLBER_HA.5432
    done
    wait_until 60 "PostgreSQL on pg1 accepts connections" sql pg1 "SELECT 1"
}

cmd_setup() {
    echo "== creating cluster 'lab' on pg1 (failover_timeout ${TIMEOUT}s)"
    sql pg1 "CREATE EXTENSION IF NOT EXISTS worlber_ha"
    sql pg1 "SELECT worlber_ha.create_cluster('lab', failover_timeout => '${TIMEOUT} seconds')"
    echo "== adding pg2 (synchronous) and pg3 (asynchronous)"
    sql pg1 "SELECT worlber_ha.add_node('pg2', sync => true)"
    sql pg1 "SELECT worlber_ha.add_node('pg3')"
    cmd_status
}

cmd_status() {
    local n
    for n in "${NODES[@]}"; do
        running "$n" || continue
        if dc exec -T -u postgres "$n" psql -X -c \
            "SELECT node, role, state, sync, sync_state, timeline, lsn, lag_bytes, raft_role, last_seen, agent_status FROM worlber_ha.cluster_status" 2>/dev/null; then
            return 0
        fi
    done
    echo "no node answered (is the cluster set up?)"
    return 1
}

cmd_monitor() {
    local n=${1:-$(primary)}
    n=${n:-pg1}
    local q
    for q in \
        "SELECT * FROM worlber_ha.health_check()" \
        "SELECT node, role, state, sync, sync_state, timeline, lag_bytes, raft_role, agent_status FROM worlber_ha.cluster_status" \
        "SELECT node, sync_state, log_send_queue, redo_queue, write_lag, flush_lag, replay_lag FROM worlber_ha.replication" \
        "SELECT pid, reason, blocked_by, waiting_for, query FROM worlber_ha.blocking" \
        "SELECT event_time, kind, node, detail FROM worlber_ha.events LIMIT 15"; do
        echo "-- $q"
        dc exec -T -u postgres "$n" psql -X -c "$q" || true
    done
}

cmd_psql() {
    local node=${1:-$(primary)}
    node=${node:-pg1}
    shift || true
    dc exec -u postgres "$node" psql "$@"
}

cmd_isolate() {
    docker network disconnect "$NET" "$(cid "$1")"
    echo "$1 disconnected from $NET"
}

cmd_heal() {
    docker network connect --ip "${IP[$1]}" "$NET" "$(cid "$1")"
    echo "$1 reconnected to $NET"
}

# ---------------------------------------------------------------------------
# Automated test suite
# ---------------------------------------------------------------------------
PASS=0
FAIL=0
check() { # check <description> <command...>
    local desc=$1; shift
    if "$@"; then PASS=$((PASS + 1)); echo "PASS: $desc"; else FAIL=$((FAIL + 1)); echo "FAIL: $desc"; fi
}
is_role() { # is_role <node> <role> <state>
    [ "$(anysql "SELECT role || ':' || state FROM worlber_ha.cluster_status WHERE node = '$1'")" = "$2:$3" ]
}
count_is() { # count_is <node> <n>
    [ "$(sql "$1" "SELECT count(*) FROM lab_test" 2>/dev/null)" = "$2" ]
}
has_new_primary() { # has_new_primary <excluded node>
    local p
    p=$(primary)
    [ -n "$p" ] && [ "$p" != "$1" ]
}
pg_down_on() { ! dc exec -T -u postgres "$1" pg_ctl -D /var/lib/postgresql/data status >/dev/null 2>&1; }

cmd_test() {
    local t=$((TIMEOUT * 6 + 30)) p old new others n
    echo "== fresh lab"
    dc down -v --remove-orphans >/dev/null 2>&1 || true
    cmd_up
    cmd_setup

    echo; echo "== 1. cluster creation and replication"
    check "pg1 is primary" is_role pg1 primary running
    check "pg2 streaming" wait_until 60 "pg2 streaming" is_role pg2 replica streaming
    check "pg3 streaming" wait_until 60 "pg3 streaming" is_role pg3 replica streaming
    check "pg2 is a synchronous standby" \
        test "$(anysql "SELECT sync_state FROM worlber_ha.cluster_status WHERE node = 'pg2'")" = quorum
    sql pg1 "CREATE TABLE lab_test (id int PRIMARY KEY, node text, at timestamptz DEFAULT now())"
    sql pg1 "INSERT INTO lab_test SELECT g, 'pg1' FROM generate_series(1, 1000) g"
    check "rows replicated to pg2" wait_until 30 "pg2 has 1000 rows" count_is pg2 1000
    check "rows replicated to pg3" wait_until 30 "pg3 has 1000 rows" count_is pg3 1000
    check "replicas are read-only" bash -c "! docker compose -f '$COMPOSE' exec -T -u postgres pg2 psql -X -c 'INSERT INTO lab_test VALUES (0)' >/dev/null 2>&1"

    echo; echo "== 1b. synchronous replication"
    sql pg1 "SET synchronous_commit = remote_apply; INSERT INTO lab_test VALUES (1001, 'remote_apply')"
    check "remote_apply: row visible on sync replica pg2 immediately after COMMIT" \
        test "$(sql pg2 "SELECT count(*) FROM lab_test WHERE id = 1001")" = 1
    check "health_check reports a healthy cluster" test "$(anysql "SELECT string_agg(severity, ',') FROM worlber_ha.health_check()")" = OK
    check "replication view lists both standbys" test "$(anysql "SELECT string_agg(node || ':' || sync_state, ',' ORDER BY node) FROM worlber_ha.replication")" = "pg2:quorum,pg3:async"
    check "join of pg2 is in the event log" wait_until 10 "seeding events" test "$(anysql "SELECT count(*) FROM worlber_ha.events WHERE node = 'pg2' AND kind IN ('seeding_started', 'seeding_completed')")" = 2
    check "agent log of pg2 readable from SQL" test "$(sql pg1 "SELECT count(*) > 0 FROM worlber_ha.agent_log('pg2', 200) WHERE message LIKE '%base backup%'")" = t

    echo "-- strict mode: commits wait while the sync replica is down"
    sql pg1 "SELECT worlber_ha.set_config('synchronous_mode', 'strict')" >/dev/null
    sleep 2
    dc stop pg2 >/dev/null
    rm -f /tmp/worlber_lab_strict
    ( sql pg1 "INSERT INTO lab_test VALUES (1002, 'strict')" >/dev/null 2>&1; echo done > /tmp/worlber_lab_strict ) &
    sleep 4
    check "strict: commit is blocked while pg2 is down" test ! -e /tmp/worlber_lab_strict
    check "blocking view shows the waiting commit" \
        test "$(sql pg1 "SELECT count(*) FROM worlber_ha.blocking WHERE reason = 'commit waiting for synchronous replica'")" = 1
    check "health_check flags the sync replica" wait_until 20 "health CRITICAL for pg2" bash -c "docker compose -f '$COMPOSE' exec -T -u postgres pg1 psql -XAt -c \"SELECT severity FROM worlber_ha.health_check() WHERE node = 'pg2'\" | grep -q CRITICAL"
    dc start pg2 >/dev/null
    check "strict: commit completes when pg2 is back" wait_until 90 "blocked commit finished" test -e /tmp/worlber_lab_strict
    check "strict: committed row reached pg2" wait_until 30 "row on pg2" test "$(sql pg2 "SELECT count(*) FROM lab_test WHERE id = 1002" 2>/dev/null)" = 1
    wait
    sql pg1 "SELECT worlber_ha.set_config('synchronous_mode', 'available')" >/dev/null
    wait_until 60 "pg2 synchronous again" test "$(anysql "SELECT sync_state FROM worlber_ha.cluster_status WHERE node = 'pg2'")" = quorum || true

    echo "-- available mode: commits continue when the sync replica is down"
    dc stop pg2 >/dev/null
    check "available: commits continue without pg2" wait_until 20 "insert ok" timeout 3 docker compose -f "$COMPOSE" exec -T -u postgres pg1 psql -X -qAt -c "INSERT INTO lab_test VALUES (1003, 'available')"
    dc start pg2 >/dev/null
    check "pg2 back and synchronous" wait_until 90 "pg2 quorum" test "$(anysql "SELECT state || ':' || sync_state FROM worlber_ha.cluster_status WHERE node = 'pg2'")" = "streaming:quorum"
    sql pg1 "DELETE FROM lab_test WHERE id > 1000"

    echo; echo "== 1c. listener and VIP"
    check "listener 5000 on pg3 reaches the primary" test "$(viapg pg3 pg3 5000 "SELECT pg_is_in_recovery()")" = f
    check "listener 5001 on pg3 reaches a replica" test "$(viapg pg3 pg3 5001 "SELECT pg_is_in_recovery()")" = t
    check "VIP is on pg1" wait_until 20 "VIP on pg1" dc exec -T pg1 sh -c "ip -o addr show eth0 | grep -q 172.28.0.100"
    check "VIP reaches the primary" test "$(viapg pg3 172.28.0.100 5432 "SELECT pg_is_in_recovery()")" = f

    echo; echo "== 2. planned switchover pg1 -> pg2 (requested on pg3)"
    sql pg3 "SELECT worlber_ha.switchover('pg2')"
    check "pg2 became primary" wait_until 60 "pg2 primary" is_role pg2 primary running
    check "old primary pg1 rejoined" wait_until 90 "pg1 streaming" is_role pg1 replica streaming
    sql pg2 "INSERT INTO lab_test SELECT g, 'pg2' FROM generate_series(1001, 2000) g"
    check "pg1 follows pg2" wait_until 30 "pg1 has 2000 rows" count_is pg1 2000
    check "pg3 follows pg2" wait_until 30 "pg3 has 2000 rows" count_is pg3 2000

    echo; echo "== 3. crash of the primary (pg2): automatic failover"
    dc kill pg2 >/dev/null
    check "a new primary was elected" wait_until "$t" "new primary" has_new_primary pg2
    new=$(primary)
    echo "  new primary: $new"
    check "no committed rows lost" count_is "$new" 2000
    check "VIP moved to the new primary $new" wait_until 30 "VIP on $new" dc exec -T "$new" sh -c "ip -o addr show eth0 | grep -q 172.28.0.100"
    check "VIP reaches the new primary" wait_until 30 "VIP rw" test "$(viapg pg3 172.28.0.100 5432 "SELECT pg_is_in_recovery()" 2>/dev/null)" = f
    n=pg3; [ "$new" = pg3 ] && n=pg1
    check "listener on $n follows the new primary" wait_until 30 "listener rw" test "$(viapg "$n" "$n" 5000 "SELECT pg_is_in_recovery()" 2>/dev/null)" = f
    check "failover recorded in events" test "$(anysql "SELECT count(*) > 0 FROM worlber_ha.events WHERE kind = 'automatic_failover'")" = t
    sql "$new" "INSERT INTO lab_test SELECT g, '$new' FROM generate_series(2001, 3000) g"

    echo; echo "== 4. crashed node pg2 comes back and rejoins"
    dc start pg2 >/dev/null
    check "pg2 rejoined as replica" wait_until 120 "pg2 streaming" is_role pg2 replica streaming
    check "pg2 caught up" wait_until 30 "pg2 has 3000 rows" count_is pg2 3000
    check "rejoin of pg2 is in the event log" test "$(anysql "SELECT count(*) > 0 FROM worlber_ha.events WHERE node = 'pg2' AND kind IN ('rewind_completed', 'seeding_completed')")" = t

    echo; echo "== 5. network partition: isolate the primary ($new)"
    old=$new
    cmd_isolate "$old"
    check "isolated primary fenced itself (PostgreSQL stopped)" wait_until "$t" "$old fenced" pg_down_on "$old"
    check "majority elected a new primary" wait_until "$t" "new primary" has_new_primary "$old"
    check "fencing recorded in events" wait_until 30 "fenced event" test "$(anysql "SELECT count(*) > 0 FROM worlber_ha.events WHERE kind = 'fenced' AND node = '$old'")" = t
    new=$(primary)
    echo "  new primary: $new"
    sql "$new" "INSERT INTO lab_test SELECT g, '$new' FROM generate_series(3001, 4000) g"
    cmd_heal "$old"
    check "healed node $old rejoined" wait_until 120 "$old streaming" is_role "$old" replica streaming
    check "healed node $old caught up" wait_until 30 "$old has 4000 rows" count_is "$old" 4000

    echo; echo "== 6. remove a replica and add it back"
    p=$(primary)
    others=()
    for n in "${NODES[@]}"; do [ "$n" != "$p" ] && others+=("$n"); done
    n=${others[1]}
    sql "$p" "SELECT worlber_ha.remove_node('$n')"
    check "$n removed" test "$(anysql "SELECT count(*) FROM worlber_ha.cluster_status WHERE node = '$n'")" = 0
    wait_until 60 "$n agent restarted fresh" dc exec -T "$n" test -S /var/run/postgresql/.s.WORLBER_HA.5432 || true
    sleep 3
    check "$n added again" wait_until 60 "add_node $n" sql "$p" "SELECT worlber_ha.add_node('$n')"
    check "$n streaming again" wait_until 120 "$n streaming" is_role "$n" replica streaming

    echo; echo "== 7. final consistency"
    p=$(primary)
    sql "$p" "SELECT pg_switch_wal(); CHECKPOINT" >/dev/null
    for n in "${NODES[@]}"; do
        check "$n has all 4000 rows" wait_until 30 "$n count" count_is "$n" 4000
    done
    check "health_check is OK at the end" wait_until 60 "health OK" test "$(anysql "SELECT string_agg(severity, ',') FROM worlber_ha.health_check()")" = OK
    echo; cmd_monitor || true
    echo
    echo "== RESULT: $PASS passed, $FAIL failed"
    [ "$FAIL" -eq 0 ]
}

case "${1:-help}" in
    up) cmd_up ;;
    setup) cmd_setup ;;
    status) cmd_status ;;
    psql) shift; cmd_psql "$@" ;;
    sql) shift; p=$(primary); sql "${p:-pg1}" "$*" ;;
    kill) dc kill "$2" ;;
    stop) dc stop "$2" ;;
    start) dc start "$2" ;;
    isolate) cmd_isolate "$2" ;;
    heal) cmd_heal "$2" ;;
    monitor) cmd_monitor "${2:-}" ;;
    agentlog)
        p=$(primary); arg=NULL; [ -n "${2:-}" ] && arg="'$2'"
        dc exec -T -u postgres "${p:-pg1}" psql -X -c "SELECT * FROM worlber_ha.agent_log($arg, 50)" ;;
    logs) dc logs -f "$2" ;;
    pglog) dc exec "$2" tail -f /var/lib/worlber_ha/postgresql.log ;;
    witness) dc --profile witness up -d --no-build witness ;;
    test) cmd_test ;;
    down) dc --profile witness down -v --remove-orphans ;;
    *) sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
