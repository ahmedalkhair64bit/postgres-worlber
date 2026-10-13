#!/usr/bin/env bash
# worlber_ha chaos harness: fault scenarios under ledger load on the docker lab.
#
#   ./chaos/run.sh <scenario> [runs]
#
# Scenarios (see docs/designs/worlber-ha-production-proof.md):
#   s1   kill -9 the postmaster on the primary (agent alive)
#   s2   kill -9 the agent on the primary (PostgreSQL alive, agent stays dead;
#        needs the lab started with CHAOS_KEEP_PG=1)
#   s7   soak: load and reports with no fault (CHAOS_SOAK, default 30m)
#
# Every run writes results/<scenario>-<stamp>-<n>.json with the ledger verdict
# (acked rows missing, acked rows only on the old primary, RTO), the events
# recorded by the cluster, and pass/fail.  The exit code is non-zero if any
# run failed.
#
# Environment:
#   CHAOS_CLIENTS   writer clients (default 8)
#   CHAOS_RATE      attempts/s per client, 0 = unthrottled (default 0)
#   CHAOS_WARMUP    seconds of load before the fault (default 15)
#   CHAOS_TAIL      seconds of load after the fault (default 60)
#   CHAOS_RTO_MAX   pass bar for RTO in ms (default 30000)
#   CHAOS_SOAK      s7 duration (default 30m)
#   CHAOS_REPORTS   report sessions on the read-only listener (default 2)
#   LEDGER          worlber-ledger binary (default: built from the agent module)
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
LAB="$DIR/../lab.sh"
COMPOSE="$DIR/../docker/docker-compose.yml"
RESULTS="$DIR/results"
NODES=(pg1 pg2 pg3)
declare -A IP=([pg1]=172.28.0.11 [pg2]=172.28.0.12 [pg3]=172.28.0.13)
CLIENTS=${CHAOS_CLIENTS:-8}
RATE=${CHAOS_RATE:-0}
WARMUP=${CHAOS_WARMUP:-15}
TAIL=${CHAOS_TAIL:-60}
RTO_MAX=${CHAOS_RTO_MAX:-30000}
SOAK=${CHAOS_SOAK:-30m}
REPORTS=${CHAOS_REPORTS:-2}
AUTH="user=postgres password=postgres dbname=postgres"
# Through the read-write listener on every node: a dead node is skipped,
# a live listener forwards to whatever the cluster says is the primary.
RW_DSN="host=172.28.0.11,172.28.0.12,172.28.0.13 port=5000 connect_timeout=1 $AUTH"
RO_DSN="host=172.28.0.11,172.28.0.12,172.28.0.13 port=5001 connect_timeout=1 $AUTH"

dc() { docker compose -f "$COMPOSE" "$@"; }
now_ms() { date +%s%3N; }
log() { echo "[$(date +%H:%M:%S)] $*"; }

if [ -z "${LEDGER:-}" ]; then
    LEDGER="$RESULTS/worlber-ledger"
    mkdir -p "$RESULTS"
    (cd "$DIR/../../agent" && GOTOOLCHAIN=local go build -o "$LEDGER" ./cmd/worlber-ledger)
fi

sql() { # sql <node> <query>
    local node=$1; shift
    dc exec -T -u postgres "$node" psql -X -qAt -v ON_ERROR_STOP=1 -c "$*"
}
anysql() {
    local n out
    for n in "${NODES[@]}"; do
        if out=$(timeout 5 bash -c 'docker compose -f "$0" exec -T -u postgres "$1" psql -X -qAt -v ON_ERROR_STOP=1 -c "$2"' "$COMPOSE" "$n" "$*" 2>/dev/null); then
            printf '%s\n' "$out"; return 0
        fi
    done
    return 1
}
primary() { anysql "SELECT node FROM worlber_ha.cluster_status WHERE role = 'primary' AND state = 'running'" || true; }

wait_until() { # wait_until <seconds> <command...>
    local limit=$1 start=$SECONDS; shift
    while (( SECONDS - start < limit )); do
        "$@" >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}
healthy() {
    [ -n "$(primary)" ] &&
    [ "$(anysql "SELECT count(*) FROM worlber_ha.cluster_status WHERE role = 'replica' AND state = 'streaming'")" = 2 ]
}

ensure_lab() {
    if ! healthy; then
        log "lab not healthy: fresh lab (CHAOS_KEEP_PG=${CHAOS_KEEP_PG:-0})"
        "$LAB" down >/dev/null 2>&1 || true
        "$LAB" up
        "$LAB" setup
        wait_until 180 healthy || { log "lab did not become healthy"; "$LAB" status || true; exit 2; }
    fi
}

restore() { # restore <node>: bring a faulted node back and wait for health
    local node=$1
    # Only restart what the fault left broken: restarting a node that already
    # recovered (S1: the agent restarts PostgreSQL) would be a second fault.
    if healthy && dc exec -T "$node" pidof worlber-ha-agent >/dev/null 2>&1; then
        return 0
    fi
    log "restoring $node"
    dc restart "$node" >/dev/null
    if ! wait_until 240 healthy; then
        log "cluster not healthy after restoring $node; recreating the lab for the next run"
        "$LAB" down >/dev/null 2>&1 || true
    fi
}

# fault_<scenario> <primary>: inject the fault and confirm it took effect;
# a non-zero exit fails the run (a run whose fault never happened must
# never count as a pass).
fault_s1() {
    local pid
    pid=$(dc exec -T "$1" head -1 /var/lib/postgresql/data/postmaster.pid | tr -d '\r')
    [ -n "$pid" ] || { echo "s1: no postmaster pid on $1" >&2; return 1; }
    dc exec -T "$1" kill -9 "$pid"
    dc exec -T "$1" bash -c "! kill -0 $pid 2>/dev/null" || { echo "s1: postmaster $pid still alive" >&2; return 1; }
}
fault_s2() {
    local pids
    if [ "$(dc exec -T "$1" printenv CHAOS_KEEP_PG 2>/dev/null | tr -d '\r')" != "1" ]; then
        echo "s2 needs the lab started with CHAOS_KEEP_PG=1 (CHAOS_KEEP_PG=1 ./lab.sh up)" >&2
        return 1
    fi
    # pkill -x matches only the first 15 characters of the name: use pidof.
    pids=$(dc exec -T "$1" pidof worlber-ha-agent | tr -d '\r')
    [ -n "$pids" ] || { echo "s2: no agent process on $1" >&2; return 1; }
    # shellcheck disable=SC2086
    dc exec -T "$1" kill -9 $pids
    sleep 1
    if dc exec -T "$1" pidof worlber-ha-agent >/dev/null 2>&1; then
        echo "s2: agent still running on $1" >&2; return 1
    fi
    dc exec -T -u postgres "$1" pg_ctl -D /var/lib/postgresql/data status >/dev/null ||
        { echo "s2: PostgreSQL on $1 is not running (the scenario needs it up)" >&2; return 1; }
}

# dump_old_keys <node> <file>: read the ledger keys of a stopped old primary
# without letting it serve anything.
dump_old_keys() {
    local node=$1 out=$2 data=/var/lib/postgresql/data
    dc exec -T -u postgres "$node" pg_ctl -D "$data" -w -t 60 -l /tmp/isolated.log start \
        -o "-c listen_addresses='' -c port=5499 -c unix_socket_directories=/tmp -c worlber_ha.fence=off -c default_transaction_read_only=on -c archive_mode=off" >/dev/null || return 1
    dc exec -T -u postgres "$node" psql -X -qAt -h /tmp -p 5499 -d postgres \
        -c "COPY (SELECT client, seq FROM worlber_ledger) TO STDOUT WITH CSV" >"$out"
    local rc=$?
    dc exec -T -u postgres "$node" pg_ctl -D "$data" -w -m fast stop >/dev/null || true
    return $rc
}

run_one() { # run_one <scenario> <n> <stamp>
    local sc=$1 n=$2 stamp=$3 p fault_at=0 dur verify newp failovers events pass
    local base="$RESULTS/$sc-$stamp-$n"
    ensure_lab
    p=$(primary)
    log "$sc run $n: primary is $p"
    "$LEDGER" setup --dsn "$RW_DSN" --reset

    if [ "$sc" = s7 ]; then
        dur=$SOAK
    else
        dur="$((WARMUP + TAIL))s"
    fi
    "$LEDGER" write --dsn "$RW_DSN" --clients "$CLIENTS" --rate "$RATE" \
        --connect-timeout 5s --duration "$dur" --out "$base.ledger" 2>"$base.writer.log" &
    local writer=$!
    local reporter=""
    if [ "$sc" = s7 ] && [ "$REPORTS" -gt 0 ]; then
        "$LEDGER" report --dsn "$RO_DSN" --sessions "$REPORTS" --duration "$dur" --out "$base.reports" &
        reporter=$!
    fi

    local start_s
    start_s=$(date +%s)
    if [ "$sc" != s7 ]; then
        sleep "$WARMUP"
        fault_at=$(now_ms)
        log "$sc run $n: fault on $p"
        if ! "fault_$sc" "$p"; then
            log "$sc run $n: FAULT NOT INJECTED, run fails"
            kill "$writer" 2>/dev/null || true
            wait "$writer" || true
            jq -n --arg sc "$sc" --argjson n "$n" --arg old "$p" \
                '{scenario: $sc, run: $n, old_primary: $old, pass: false, error: "fault not injected"}' >"$base.json"
            restore "$p"
            return 1
        fi
    fi
    wait "$writer" || true
    [ -n "$reporter" ] && { wait "$reporter" || true; }

    newp=$(primary)
    # S2: rows on the old primary that are not on the new primary must never
    # be acknowledged.  If fencing already stopped the old primary, start
    # its data in isolation (unix socket only, read-only, fence off, no
    # agent: it died in S2) and dump the ledger keys.
    local old_args=() old_state=""
    if [ "$sc" = s2 ] && [ -n "$newp" ] && [ "$newp" != "$p" ]; then
        if dc exec -T -u postgres "$p" pg_ctl -D /var/lib/postgresql/data status >/dev/null 2>&1; then
            old_state="running (not fenced)"
            old_args=(--old-dsn "host=${IP[$p]} port=5432 connect_timeout=2 $AUTH")
        else
            old_state="stopped (fenced)"
            if dump_old_keys "$p" "$base.oldkeys"; then
                old_args=(--old-keys "$base.oldkeys")
            else
                old_state="stopped (fenced); isolated start failed"
            fi
        fi
    fi
    if [ -n "$newp" ]; then
        verify=$("$LEDGER" verify --ledger "$base.ledger" --dsn "host=${IP[$newp]} port=5432 connect_timeout=2 $AUTH" \
            "${old_args[@]}" --fault-at "$fault_at" || true)
    fi
    [ -n "${verify:-}" ] || verify='{"pass":false,"error":"no primary to verify against"}'
    events=$(anysql "SELECT coalesce(json_agg(json_build_object('time', event_time, 'kind', kind, 'node', node, 'detail', detail) ORDER BY seq DESC), '[]') FROM worlber_ha.events WHERE event_time >= to_timestamp($start_s)" || echo '[]')
    failovers=$(jq '[.[] | select(.kind == "automatic_failover")] | length' <<<"$events")
    local reports='null'
    [ -f "$base.reports" ] && reports=$(jq -s 'group_by(.o) | map({(.[0].o): length}) | add // {}' "$base.reports")

    pass=$(jq -n --argjson v "$verify" --arg sc "$sc" --argjson fo "$failovers" --argjson max "$RTO_MAX" '
        if $sc == "s7" then ($v.pass == true and $fo == 0)
        else ($v.pass == true and ($v.summary.rto_ms // -1) >= 0 and ($v.summary.rto_ms // -1) <= $max)
        end')
    jq -n --arg sc "$sc" --argjson n "$n" --arg old "$p" --arg new "${newp:-}" \
        --argjson fault_at "$fault_at" --argjson clients "$CLIENTS" --argjson rate "$RATE" \
        --argjson v "$verify" --argjson ev "$events" --argjson fo "$failovers" \
        --argjson reports "$reports" --argjson pass "$pass" --arg old_checked "${old_args[*]:+yes}" --arg old_state "$old_state" \
        '{scenario: $sc, run: $n, old_primary: $old, new_primary: $new, fault_at_ms: $fault_at,
          clients: $clients, rate_per_client: $rate, automatic_failovers: $fo,
          old_primary_checked: ($old_checked == "yes"), old_primary_state: $old_state, verdict: $v, reports: $reports,
          events: $ev, pass: $pass}' >"$base.json"
    log "$sc run $n: pass=$pass old=$p new=${newp:-none} rto_ms=$(jq '.summary.rto_ms // null' <<<"$verify") acked=$(jq '.acked // 0' <<<"$verify") missing=$(jq '.acked_missing // null' <<<"$verify") old_only_acked=$(jq '.old_only_acked // null' <<<"$verify") failovers=$failovers${old_state:+ old_primary=$old_state}"
    [ "$sc" = s7 ] || restore "$p"
    [ "$pass" = true ]
}

sc=${1:-}
runs=${2:-1}
case "$sc" in s1|s2|s7) ;; *) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;; esac
mkdir -p "$RESULTS"
stamp=$(date +%Y%m%d-%H%M%S)
failed=0
for ((i = 1; i <= runs; i++)); do
    run_one "$sc" "$i" "$stamp" || failed=$((failed + 1))
done
log "$sc: $((runs - failed))/$runs runs passed (results in $RESULTS/$sc-$stamp-*.json)"
[ "$failed" -eq 0 ]
