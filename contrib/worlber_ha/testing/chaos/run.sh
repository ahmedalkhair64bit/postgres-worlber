#!/usr/bin/env bash
# worlber_ha chaos harness: fault scenarios under ledger load on the docker lab.
#
#   ./chaos/run.sh <scenario> [runs]
#
# Scenarios (see docs/designs/worlber-ha-production-proof.md):
#   s1   kill -9 the postmaster on the primary (agent alive)
#   s2   kill -9 the agent on the primary (PostgreSQL alive, agent stays dead;
#        needs the lab started with CHAOS_KEEP_PG=1)
#   s3   freeze the whole primary node (docker pause) until the run ends
#   s4   network partition of the primary until the run ends
#   s2b  partition + SIGSTOP a backend so fast shutdown hangs (agent SIGKILLs)
#   s1late  kill the postmaster and freeze the agent ~19s (CHAOS_LATE) so it
#           cannot restart PostgreSQL near the failover deadline
#   s5   kill the sync standby, then the primary; must block (async_failover never)
#   s7   soak: load and reports with no fault (CHAOS_SOAK, default 30m)
#   s9   rolling upgrade from the 1.1 agent (origin/master) to this tree under
#        load, with a pre-gate rollback of one replica; ALTER EXTENSION opens
#        the fencing gate (recreates the lab)
#   s10  WAL archive catch-up: offline replica recovers from the archive
#   s11  full power loss: kill all nodes, start them again, must self-heal
#   s12  network failure between all nodes, then recovery, must self-heal
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
#   CHAOS_S7_ARCHIVE  s7 with the WAL archive on (default 1)
#   CHAOS_S7_WAL_MBS  s7 extra WAL rate in MB/s during the soak (default 1)
#   CHAOS_S7_BURST    s7 unthrottled WAL burst before the soak, seconds (default 10)
#   LEDGER          worlber-ledger binary (default: built from the agent module)
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
LAB="$DIR/../lab.sh"
COMPOSE="$DIR/../docker/docker-compose.yml"
RESULTS="$DIR/results"
NODES=(pg1 pg2 pg3)
NET=worlber-lab-net
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
        LAB_SKIP_BUILD=1 "$LAB" up
        "$LAB" setup
        wait_until 180 healthy || { log "lab did not become healthy"; "$LAB" status || true; exit 2; }
    fi
}

restore() { # restore <node>: bring a faulted node back and wait for health
    local node=$1 c
    c=$(dc ps -q "$node")
    if [ -n "$c" ] && [ "$(docker inspect -f '{{.State.Paused}}' "$c" 2>/dev/null)" = true ]; then
        log "resuming $node"; docker unpause "$c" >/dev/null || true
    fi
    if [ -n "$c" ] && ! docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$c" | grep -qw "$NET"; then
        log "reconnecting $node"; docker network connect --ip "${IP[$node]}" "$NET" "$c" || true
    fi
    # Only restart what the fault left broken: restarting a node that already
    # recovered (S1: the agent restarts PostgreSQL) would be a second fault.
    if healthy && dc exec -T "$node" pidof worlber-ha-agent >/dev/null 2>&1; then
        return 0
    fi
    dc exec -T "$node" bash -c "pidof worlber-ha-agent >/dev/null && kill -CONT \$(pidof worlber-ha-agent)" 2>/dev/null || true
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

# s3: freeze the whole primary node (all its processes) for CHAOS_PAUSE
# seconds; it is resumed by restore after the verdict.
fault_s3() {
    docker pause "$(dc ps -q "$1")" >/dev/null || return 1
    [ "$(docker inspect -f '{{.State.Paused}}' "$(dc ps -q "$1")")" = true ]
}
# s4: network partition of the primary; healed by restore after the verdict.
fault_s4() {
    docker network disconnect "$NET" "$(dc ps -q "$1")" || return 1
    ! docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$(dc ps -q "$1")" | grep -qw "$NET"
}

# s2b: partition the primary (its agent stops confirming, so the lease
# expires) and SIGSTOP a backend so fast shutdown hangs; the agent must
# escalate to SIGKILL.  The agent stays alive to do that.
fault_s2b() {
    local c be
    c=$(dc ps -q "$1")
    be=$(dc exec -T "$1" bash -c "pgrep -f 'postgres:' | head -1" | tr -d '\r')
    [ -n "$be" ] || { echo "s2b: no backend to stop on $1" >&2; return 1; }
    dc exec -T "$1" kill -STOP "$be" || return 1
    docker network disconnect "$NET" "$c" || return 1
    ! docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$c" | grep -qw "$NET"
}

fault_s1late() {
    local pid agent
    pid=$(dc exec -T "$1" head -1 /var/lib/postgresql/data/postmaster.pid | tr -d '\r')
    agent=$(dc exec -T "$1" pidof worlber-ha-agent | tr -d '\r')
    [ -n "$pid" ] && [ -n "$agent" ] || { echo "s1late: no postmaster/agent on $1" >&2; return 1; }
    # freeze the agent first so it cannot restart PostgreSQL after the kill
    dc exec -T "$1" kill -STOP $agent || return 1
    dc exec -T "$1" kill -9 "$pid" || return 1
    ( sleep "${CHAOS_LATE:-19}"; dc exec -T "$1" kill -CONT $agent >/dev/null 2>&1 ) &
    dc exec -T "$1" bash -c "! kill -0 $pid 2>/dev/null"
}

# sync_standby: the node the primary currently treats as synchronous.
sync_standby() { anysql "SELECT node FROM worlber_ha.cluster_status WHERE role='replica' AND sync_state IN ('sync','quorum') LIMIT 1" || true; }

# s5: lose the synchronous standby and then the primary (both nodes gone).
# In a three-node cluster that leaves one node, which is not a Raft majority,
# so the safe outcome is NO promotion (never split-brain) and zero data loss;
# the cluster recovers when a node returns.  The async_failover block for the
# case where a majority survives is covered by the agent unit tests.
fault_s5() {
    local ss
    ss=$(sync_standby)
    [ -n "$ss" ] && [ "$ss" != "$1" ] || { echo "s5: no sync standby" >&2; return 1; }
    log "s5: killing sync standby $ss, then primary $1 in ${CHAOS_S5_GAP:-5}s"
    dc kill "$ss" >/dev/null || return 1
    S5_SYNC_STANDBY=$ss
    sleep "${CHAOS_S5_GAP:-5}"
    dc kill "$1" >/dev/null || return 1
    ! running "$1"
}

# s11: full power loss.  Kill every node at once (SIGKILL, no shutdown), keep
# them down CHAOS_DOWN seconds, then start them all; nobody intervenes.
# Note: docker kill does not drop the host page cache, so this tests restart
# and self-healing, not fsync durability against a real power cut (that is
# PostgreSQL's own guarantee with fsync=on).
fault_s11() {
    dc kill pg1 pg2 pg3 >/dev/null || return 1
    ( sleep "${CHAOS_DOWN:-20}"; dc start pg1 pg2 pg3 >/dev/null 2>&1 ) &
    ! running pg1 && ! running pg2 && ! running pg3
}
# s12: network failure between all nodes.  Disconnect every node from the lab
# network for CHAOS_DOWN seconds (no majority anywhere), then reconnect.
fault_s12() {
    local n
    for n in pg1 pg2 pg3; do
        docker network disconnect "$NET" "$(dc ps -q "$n")" || return 1
    done
    ( sleep "${CHAOS_DOWN:-30}"
      for n in pg1 pg2 pg3; do
          docker network connect --ip "${IP[$n]}" "$NET" "$(dc ps -q "$n")" >/dev/null 2>&1
      done ) &
    return 0
}


# --- S7 measurements (D14, P7) ---------------------------------------------
# s7_archive_on: enable the shared WAL archive (needs a restart of every node
# for archive_mode) so the soak measures archive throughput.
s7_archive_on() {
    local p; p=$(primary)
    [ "$(sql "$p" "SHOW archive_mode")" = on ] && return 0
    log "s7: enabling the WAL archive (rolling restart of the lab)"
    dc exec -T -u postgres "$p" bash -c "rm -f /mnt/wal-archive/* 2>/dev/null || true"
    sql "$p" "SELECT worlber_ha.set_config('wal_archive_dir', '/mnt/wal-archive')" >/dev/null
    dc restart pg1 pg2 pg3 >/dev/null
    wait_until 240 healthy || { log "s7: cluster not healthy after enabling the archive"; return 1; }
}
# s7_latency <base>: P7, 60s of ledger writes through the listener, then 60s
# direct to the primary; prints {listener:{...}, direct:{...}} with tps/p50/p99.
s7_latency() {
    local base=$1 p mode dsn out='{}' v secs=${CHAOS_S7_LAT_SECS:-60}
    p=$(primary)
    for mode in listener direct; do
        if [ $mode = listener ]; then dsn=$RW_DSN; else dsn="host=${IP[$p]} port=5432 connect_timeout=1 $AUTH"; fi
        "$LEDGER" setup --dsn "$RW_DSN" --reset >/dev/null
        "$LEDGER" write --dsn "$dsn" --clients "$CLIENTS" --rate "$RATE" --duration "${secs}s" --out "$base.lat-$mode.ledger" 2>/dev/null || true
        v=$("$LEDGER" verify --ledger "$base.lat-$mode.ledger" --dsn "host=${IP[$p]} port=5432 connect_timeout=2 $AUTH" || echo '{}')
        out=$(jq --arg m $mode --argjson v "$v" --argjson s "$secs" \
            '. + {($m): {tps: (($v.summary.acked // 0) / $s | floor), ack_p50_ms: $v.summary.ack_p50_ms, ack_p99_ms: $v.summary.ack_p99_ms}}' <<<"$out")
    done
    log "s7: latency (P7): $(jq -c . <<<"$out")" >&2
    printf '%s\n' "$out"
}
# s7_sampler <file> <seconds>: every 30s on the primary, record time, WAL
# position, archived segments and segments waiting to be archived (.ready).
s7_sampler() {
    local f=$1 end=$((SECONDS + $2)) p
    : >"$f"
    while (( SECONDS < end )); do
        p=$(primary)
        [ -n "$p" ] && sql "$p" "SELECT extract(epoch FROM now())::bigint, pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, archived_count, failed_count, (SELECT count(*) FROM pg_ls_archive_statusdir() WHERE name LIKE '%.ready') FROM pg_stat_archiver" 2>/dev/null | tr '|' ' ' >>"$f" || true
        sleep 30
    done
}
# s7_walgen <seconds>: steady extra WAL on the primary during the soak
# (CHAOS_S7_WAL_MBS MB/s, default 2, 0 = off), so the archive must keep up
# with a known WAL rate for the whole soak.  Bounded so 30 minutes fit the
# lab disk (the archive keeps archive_keep = 7d).
s7_walgen() {
    local end=$((SECONDS + $1)) mbs=${CHAOS_S7_WAL_MBS:-1} p t0
    [ "$mbs" -gt 0 ] || return 0
    while (( SECONDS < end )); do
        t0=$(now_ms)
        p=$(primary)
        [ -n "$p" ] && sql "$p" "SELECT count(pg_logical_emit_message(false, 's7', repeat(md5(g::text), 32768))) FROM generate_series(1, $mbs) g" >/dev/null 2>&1
        local left=$(( 1000 - ($(now_ms) - t0) ))
        (( left > 0 )) && sleep "0.$(printf '%03d' "$left")"
    done
}
# s7_burst <seconds>: D14 ceiling.  Unthrottled WAL for a short burst; prints
# WAL MB/s, archive MB/s during the burst, and how long the backlog took to
# drain afterwards.
s7_burst() {
    local secs=${1:-30} p a0 a1 w0 w1 t0 t1 ready drain=-1 s
    p=$(primary)
    read -r w0 a0 <<<"$(sql "$p" "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, archived_count FROM pg_stat_archiver" | tr '|' ' ')"
    t0=$(date +%s)
    while (( $(date +%s) - t0 < secs )); do
        sql "$p" "SELECT count(pg_logical_emit_message(false, 's7b', repeat(md5(g::text), 32768))) FROM generate_series(1, 64) g" >/dev/null
    done
    read -r w1 a1 <<<"$(sql "$p" "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, archived_count FROM pg_stat_archiver" | tr '|' ' ')"
    t1=$(date +%s)
    ready=$(sql "$p" "SELECT count(*) FROM pg_ls_archive_statusdir() WHERE name LIKE '%.ready'")
    for s in $(seq 0 600); do
        [ "$(sql "$p" "SELECT count(*) FROM pg_ls_archive_statusdir() WHERE name LIKE '%.ready'")" -le 1 ] && { drain=$s; break; }
        sleep 1
    done
    jq -n --argjson d $((t1 - t0)) --argjson w $((w1 - w0)) --argjson a $((a1 - a0)) --argjson r "$ready" --argjson drain "$drain" \
        '{seconds: $d, wal_mb_s: (($w / 1048576 / $d) * 10 | floor / 10), archive_mb_s: (($a * 16 / $d) * 10 | floor / 10),
          ready_after_burst: $r, drain_s: $drain, archive_type: "local docker volume"}'
}
# s7_archive_summary <file>: D14 numbers from the samples.
s7_archive_summary() {
    awk 'NF==5 { n++; t[n]=$1; w[n]=$2; a[n]=$3; f[n]=$4; r[n]=$5; if ($5>maxr) maxr=$5
            # counters restart from 0 when PostgreSQL restarts: count the
            # restarts and sum only forward steps
            if (n>1) { if ($3>=a[n-1]) da+=$3-a[n-1]; else { resets++; da+=$3 }
                       if ($4>=f[n-1]) df+=$4-f[n-1]; else df+=$4
                       if ($2>=w[n-1]) dw+=$2-w[n-1] } }
        END {
            if (n < 2) { print "{\"samples\":" n "}"; exit }
            d=t[n]-t[1]; if (d<1) d=1
            third=int(n/3); if (third<1) third=1
            for (i=1;i<=third;i++) first+=r[i]; for (i=n-third+1;i<=n;i++) last+=r[i]
            printf "{\"samples\":%d,\"seconds\":%d,\"wal_mb_s\":%.2f,\"archive_mb_s\":%.2f,\"archived_segments\":%d,\"archive_failures\":%d,\"pg_restarts\":%d,\"ready_max\":%d,\"ready_first_third_avg\":%.1f,\"ready_last_third_avg\":%.1f,\"ready_final\":%d}\n",
                n, d, dw/1048576/d, da*16/d, da, df, resets+0, maxr, first/third, last/third, r[n]
        }' "$1"
}

run_one() { # run_one <scenario> <n> <stamp>
    local sc=$1 n=$2 stamp=$3 p fault_at=0 dur verify newp failovers events pass
    local base="$RESULTS/$sc-$stamp-$n"
    ensure_lab
    p=$(primary)
    log "$sc run $n: primary is $p"
    "$LEDGER" setup --dsn "$RW_DSN" --reset

    local latency='null' sampler="" walgen="" burst='null'
    if [ "$sc" = s7 ]; then
        dur=$SOAK
        if [ "${CHAOS_S7_ARCHIVE:-1}" = 1 ]; then s7_archive_on || return 1; fi
        local free_gb; free_gb=$(df -BG --output=avail / | tail -1 | tr -dc 0-9)
        if (( free_gb < ${CHAOS_S7_MIN_FREE_GB:-8} )); then
            log "s7: only ${free_gb}G free on the docker host; the soak with the archive needs ${CHAOS_S7_MIN_FREE_GB:-8}G"; return 1
        fi
        latency=$(s7_latency "$base" | tail -1)
        burst='null'
        if [ "${CHAOS_S7_BURST:-10}" -gt 0 ] && [ "${CHAOS_S7_ARCHIVE:-1}" = 1 ]; then
            burst=$(s7_burst "${CHAOS_S7_BURST:-10}")
            # the burst segments are only a measurement: drop them so the
            # soak does not start with GBs in the archive (replicas stream)
            sql "$(primary)" "CHECKPOINT" >/dev/null
            dc exec -T -u postgres "$(primary)" bash -c "find /mnt/wal-archive -type f -mmin -5 -delete" || true
            log "s7: archive burst (D14 ceiling): $(jq -c . <<<"$burst")"
        fi
        "$LEDGER" setup --dsn "$RW_DSN" --reset
        local soak_s; soak_s=$(( $(sed -E 's/([0-9]+)m$/\1*60/; s/([0-9]+)s$/\1/; s/([0-9]+)h$/\1*3600/' <<<"$SOAK") ))
        s7_sampler "$base.archive" "$soak_s" &
        sampler=$!
        s7_walgen "$soak_s" &
        walgen=$!
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
    local archive='null'
    if [ -n "$sampler" ]; then
        kill "$sampler" $walgen 2>/dev/null || true; wait "$sampler" $walgen 2>/dev/null || true
        archive=$(s7_archive_summary "$base.archive")
        log "s7: archive (D14): $archive"
    fi

    local healed_s=-1
    if [[ "$sc" =~ ^(s11|s12)$ ]]; then
        local h0=$SECONDS
        if wait_until 300 healthy; then healed_s=$(( SECONDS - h0 )); fi
        log "$sc run $n: cluster healthy again by itself: $([ $healed_s -ge 0 ] && echo "yes (${healed_s}s after the load ended)" || echo NO)"
    fi
    newp=$(primary)
    # S2: rows on the old primary that are not on the new primary must never
    # be acknowledged.  If fencing already stopped the old primary, start
    # its data in isolation (unix socket only, read-only, fence off, no
    # agent: it died in S2) and dump the ledger keys.
    local old_args=() old_state=""
    if [ "$sc" = s3 ]; then
        # the frozen node can only be inspected after it is resumed; its
        # fence worker must stop it at once (the lease clock kept running)
        docker unpause "$(dc ps -q "$p")" >/dev/null 2>&1 || true
        sleep 2
    fi
    if [ "$sc" = s5 ]; then
        # no majority during the fault: start both nodes back, then verify
        # the old primary kept every acknowledged row.
        [ -n "${S5_SYNC_STANDBY:-}" ] && dc start "$S5_SYNC_STANDBY" >/dev/null 2>&1 || true
        dc start "$p" >/dev/null 2>&1 || true
        wait_until 120 "primary back after s5" primary || true
        newp=$(primary)
    fi
    if [[ "$sc" =~ ^(s2|s2b|s3|s4|s1late)$ ]] && [ -n "$newp" ] && [ "$newp" != "$p" ]; then
        if dc exec -T -u postgres "$p" pg_ctl -D /var/lib/postgresql/data status >/dev/null 2>&1; then
            old_state="running (not fenced)"
            if dc exec -T -u postgres "$p" pg_ctl -D /var/lib/postgresql/data status >/dev/null 2>&1; then
                # still running (partitioned): read it from inside its container
                dc exec -T -u postgres "$p" psql -X -qAt -d postgres \
                    -c "COPY (SELECT client, seq FROM worlber_ledger) TO STDOUT WITH CSV" >"$base.oldkeys" &&
                    old_args=(--old-keys "$base.oldkeys")
            else
                old_args=(--old-dsn "host=${IP[$p]} port=5432 connect_timeout=2 $AUTH")
            fi
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

    local blocked=0
    blocked=$(jq '[.[] | select(.kind == "failover_blocked")] | length' <<<"$events")
    pass=$(jq -n --argjson v "$verify" --arg sc "$sc" --argjson fo "$failovers" --argjson max "$RTO_MAX" --argjson blk "$blocked" --argjson healed "$healed_s" --argjson arch "$archive" '
        if $sc == "s7" then ($v.pass == true and $fo == 0 and ($arch == null or (($arch.archive_failures // 0) == 0 and ($arch.pg_restarts // 0) == 0 and ($arch.ready_final // 0) <= 8)))
        elif $sc == "s5" then ($fo == 0 and $v.pass == true)
        elif ($sc == "s11" or $sc == "s12") then ($v.pass == true and $healed >= 0)
        else ($v.pass == true and ($v.summary.rto_ms // -1) >= 0 and ($v.summary.rto_ms // -1) <= $max)
        end')
    jq -n --arg sc "$sc" --argjson n "$n" --arg old "$p" --arg new "${newp:-}" \
        --argjson fault_at "$fault_at" --argjson clients "$CLIENTS" --argjson rate "$RATE" \
        --argjson v "$verify" --argjson ev "$events" --argjson fo "$failovers" \
        --argjson reports "$reports" --argjson pass "$pass" --argjson latency "$latency" --argjson archive "$archive" --argjson burst "$burst" --arg old_checked "${old_args[*]:+yes}" --arg old_state "$old_state" \
        '{scenario: $sc, run: $n, old_primary: $old, new_primary: $new, fault_at_ms: $fault_at,
          clients: $clients, rate_per_client: $rate, automatic_failovers: $fo,
          old_primary_checked: ($old_checked == "yes"), old_primary_state: $old_state, verdict: $v, reports: $reports, latency: $latency, archive: $archive, archive_burst: $burst,
          events: $ev, pass: $pass}' >"$base.json"
    log "$sc run $n: pass=$pass old=$p new=${newp:-none} rto_ms=$(jq '.summary.rto_ms // null' <<<"$verify") acked=$(jq '.acked // 0' <<<"$verify") missing=$(jq '.acked_missing // null' <<<"$verify") old_only_acked=$(jq '.old_only_acked // null' <<<"$verify") failovers=$failovers${old_state:+ old_primary=$old_state}"
    if [ "$sc" = s2b ] && [ -n "$newp" ] && [ "$newp" != "$p" ]; then
        local left
        left=$(dc exec -T "$p" bash -c "pgrep -c postgres || true" 2>/dev/null | tr -d '\r')
        left=${left:-0}
        if [ "$left" != 0 ]; then
            log "s2b run $n: $left postgres process(es) still alive on the old primary (SIGKILL escalation failed)"
            pass=false
        fi
    fi
    if [ "$sc" = s5 ]; then
        # recover: bring both killed nodes back; quorum returns and the old
        # primary resumes (no promotion happened).
        [ -n "${S5_SYNC_STANDBY:-}" ] && dc start "$S5_SYNC_STANDBY" >/dev/null 2>&1 || true
        dc start "$p" >/dev/null 2>&1 || true
    fi
    [ "$sc" = s7 ] || restore "$p"
    [ "$pass" = true ]
}


# s9: rolling upgrade 1.1 -> 1.2 under ledger load (W8 / D15).
#   1. Recreate the lab with the 1.1 agent (built from origin/master) on every
#      node and the extension at version 1.1.
#   2. Upgrade the async replica, roll it back to 1.1 (pre-gate rollback),
#      upgrade it again.  Move sync to it, upgrade the old sync standby.
#   3. Switch over to an upgraded replica, upgrade the old primary.
#   4. The gate stays closed until ALTER EXTENSION worlber_ha UPDATE TO '1.2'
#      on the primary; then it opens and the primary's fence is armed.
# Pass: 0 automatic failovers, 0 acknowledged rows lost, gate open, fence armed.
OLD_REF=${CHAOS_S9_OLD_REF:-origin/master}
s9_build_old() {
    local wt="$RESULTS/s9-old-src"
    rm -rf "$wt" "$RESULTS/s9-old"
    git -C "$DIR" worktree remove --force "$wt" >/dev/null 2>&1 || true
    git -C "$DIR" worktree add -q --detach "$wt" "$OLD_REF" || return 1
    mkdir -p "$RESULTS/s9-old/override"
    (cd "$wt/contrib/worlber_ha/agent" && CGO_ENABLED=0 GOTOOLCHAIN=local go build -trimpath -o "$RESULTS/s9-old/override/worlber-ha-agent" ./cmd/worlber-ha-agent) || return 1
    git -C "$DIR" worktree remove --force "$wt" >/dev/null 2>&1 || true
}
s9_set_agent() { # s9_set_agent <node> old|new : install the agent and restart the node
    local node=$1 which=$2 c
    c=$(dc ps -aq "$node")
    dc exec -T "$node" rm -rf /var/lib/worlber_ha/override 2>/dev/null || true
    if [ "$which" = old ]; then
        docker cp "$RESULTS/s9-old/override" "$c:/var/lib/worlber_ha/" >/dev/null || return 1
    fi
    log "s9: $node -> $which agent (restart)"
    dc restart "$node" >/dev/null
    wait_until 60 dc exec -T "$node" test -S /var/run/postgresql/.s.WORLBER_HA.5432 || return 1
    # healthy() can pass on a status taken before the restart: wait until
    # this replica streams again locally, then let heartbeats settle.
    wait_until 180 s9_streaming "$node" || { log "s9: $node not streaming after -> $which"; return 1; }
    sleep 5
    wait_until 180 healthy || { log "s9: cluster not healthy after $node -> $which"; return 1; }
}
s9_streaming() { [ "$(sql "$1" "SELECT status FROM pg_stat_wal_receiver" 2>/dev/null)" = streaming ]; }
s9_gate() { anysql "SELECT coalesce(worlber_ha.status()->>'fencing_gate', 'false')" || echo unknown; }
s9_failovers() { anysql "SELECT count(*) FROM worlber_ha.events WHERE kind = 'automatic_failover' AND event_time >= to_timestamp($1)" || echo -1; }

cmd_s9() {
    local base="$RESULTS/s9-$(date +%Y%m%d-%H%M%S)-1" t0 steps=() ok=true
    mkdir -p "$RESULTS"
    log "s9: building the old agent from $OLD_REF"
    s9_build_old || { log "s9: cannot build the old agent"; return 1; }
    "$RESULTS/s9-old/override/worlber-ha-agent" --version 2>&1 | head -1 | sed 's/^/[s9] old agent: /'
    log "s9: recreating the lab on the old agent"
    "$LAB" down >/dev/null 2>&1 || true
    dc create pg1 pg2 pg3 >/dev/null
    local n
    for n in pg1 pg2 pg3; do
        docker cp "$RESULTS/s9-old/override" "$(dc ps -aq "$n"):/var/lib/worlber_ha/" >/dev/null
    done
    LAB_SKIP_BUILD=1 "$LAB" up >/dev/null
    sql pg1 "CREATE EXTENSION worlber_ha VERSION '1.1'"
    sql pg1 "SELECT worlber_ha.create_cluster('lab', failover_timeout => '${LAB_FAILOVER_TIMEOUT:-10} seconds')" >/dev/null
    sql pg1 "SELECT worlber_ha.add_node('pg2', sync => true)" >/dev/null
    sql pg1 "SELECT worlber_ha.add_node('pg3')" >/dev/null
    wait_until 240 healthy || { log "s9: old cluster not healthy"; return 1; }
    log "s9: old cluster up: extension $(sql pg1 "SELECT extversion FROM pg_extension WHERE extname='worlber_ha'"), gate=$(s9_gate)"

    t0=$(date +%s)
    "$LEDGER" setup --dsn "$RW_DSN" --reset
    "$LEDGER" write --dsn "$RW_DSN" --clients "$CLIENTS" --rate "$RATE" \
        --connect-timeout 5s --duration "${CHAOS_S9_DURATION:-900s}" --out "$base.ledger" 2>"$base.writer.log" &
    local writer=$!
    sleep "$WARMUP"

    local p; p=$(primary)
    local ss; ss=$(sync_standby)
    local as; as=$(anysql "SELECT node FROM worlber_ha.cluster_status WHERE role='replica' AND node <> '$ss' LIMIT 1")
    log "s9: primary=$p sync=$ss async=$as"
    # 2. async replica first, then the pre-gate rollback (D15), then again
    s9_set_agent "$as" new || ok=false; steps+=("upgrade $as")
    s9_set_agent "$as" old || ok=false; steps+=("rollback $as to 1.1")
    local hc; hc=$(anysql "SELECT count(*) FROM worlber_ha.health_check() WHERE severity = 'CRITICAL'" || echo -1)
    log "s9: after rollback: healthy, health_check CRITICAL rows=$hc, failovers=$(s9_failovers "$t0")"
    [ "$hc" = 0 ] || ok=false
    s9_set_agent "$as" new || ok=false; steps+=("upgrade $as again")
    # move sync to the upgraded replica before restarting the sync standby
    sql "$p" "SELECT worlber_ha.set_sync('$as', true)" >/dev/null
    sql "$p" "SELECT worlber_ha.set_sync('$ss', false)" >/dev/null
    wait_until 60 bash -c "[ \"\$(docker compose -f '$COMPOSE' exec -T -u postgres '$p' psql -X -qAt -c \"SELECT sync_state IN ('sync', 'quorum') FROM pg_stat_replication WHERE application_name = '$as'\")\" = t ]" || { log "s9: $as did not become the sync standby"; ok=false; }
    s9_set_agent "$ss" new || ok=false; steps+=("upgrade $ss")
    # 3. switchover to the upgraded sync replica, then upgrade the old primary
    log "s9: switchover to $as"
    local sw0; sw0=$(now_ms)
    # run it on the target: the switchover stops the primary, cutting its sessions
    sql "$as" "SELECT worlber_ha.switchover('$as')" >/dev/null || { log "s9: switchover failed"; ok=false; }
    wait_until 120 bash -c "[ \"\$(docker compose -f '$COMPOSE' exec -T -u postgres '$as' psql -X -qAt -c 'SELECT pg_is_in_recovery()')\" = f ]" || ok=false
    wait_until 180 s9_streaming "$p" || ok=false
    wait_until 120 healthy || ok=false
    [ "$(primary)" = "$as" ] || { log "s9: primary is $(primary), expected $as"; ok=false; }
    local sw_ms=$(( $(now_ms) - sw0 ))
    steps+=("switchover to $as (${sw_ms} ms)")
    s9_set_agent "$p" new || ok=false; steps+=("upgrade $p")
    local gate_before; gate_before=$(s9_gate)
    log "s9: all agents upgraded; gate=$gate_before (must stay closed until ALTER EXTENSION)"
    [ "$gate_before" = false ] || ok=false
    # before the gate opens the fence must be off everywhere (an older leader
    # never confirms leases, so an armed fence could stop a healthy primary)
    local fence_pre="" n2
    for n2 in pg1 pg2 pg3; do fence_pre+="$n2=$(sql "$n2" "SHOW worlber_ha.fence" 2>/dev/null) "; done
    log "s9: fence before the gate: $fence_pre"
    [[ "$fence_pre" == *"=on"* ]] && ok=false
    # 4. point of no return
    local np; np=$(primary)
    sql "$np" "ALTER EXTENSION worlber_ha UPDATE TO '1.2'"
    steps+=("ALTER EXTENSION on $np")
    local fence=""
    for _ in $(seq 1 120); do
        fence=$(anysql "SELECT fence_state FROM worlber_ha.cluster_status WHERE role='primary'" || true)
        [ "$(s9_gate)" = true ] && [ "$fence" = armed ] && break
        sleep 1
    done
    local gate_after; gate_after=$(s9_gate)
    log "s9: gate=$gate_after primary fence=$fence"
    kill "$writer" 2>/dev/null || true
    wait "$writer" 2>/dev/null || true
    np=$(primary)
    local verify; verify=$("$LEDGER" verify --ledger "$base.ledger" --dsn "host=${IP[$np]} port=5432 connect_timeout=2 $AUTH" || true)
    [ -n "$verify" ] || verify='{"pass":false}'
    local fo; fo=$(s9_failovers "$t0")
    local pass=false
    [ "$ok" = true ] && [ "$fo" = 0 ] && [ "$gate_after" = true ] && [ "$fence" = armed ] && [ "$(jq -r .pass <<<"$verify")" = true ] && pass=true
    jq -n --argjson v "$verify" --argjson fo "$fo" --arg gb "$gate_before" --arg ga "$gate_after" --arg fence "$fence" --arg fence_pre "$fence_pre" \
        --argjson sw "$sw_ms" --argjson pass "$pass" --arg steps "$(printf '%s\n' "${steps[@]}")" \
        '{scenario:"s9", run:1, steps:($steps|split("\n")|map(select(.!=""))), automatic_failovers:$fo,
          gate_before_alter:$gb, gate_after_alter:$ga, fence_before_gate:$fence_pre, primary_fence:$fence, switchover_ms:$sw,
          verdict:$v, pass:$pass}' >"$base.json"
    log "s9: pass=$pass failovers=$fo acked=$(jq '.acked // 0' <<<"$verify") missing=$(jq '.acked_missing // null' <<<"$verify") switchover=${sw_ms}ms gate=$gate_before->$gate_after fence=$fence"
    [ "$pass" = true ]
}

# s10: WAL archive catch-up.  Enable the archive, stop a replica, write WAL
# past max_slot_wal_keep so its slot is invalidated, restart it, and check it
# caught up from the archive (no reseed_started event, verify_table matches).
cmd_s10() {
    local rep base="$RESULTS/s10-$(date +%Y%m%d-%H%M%S)"
    ensure_lab
    local p; p=$(primary)
    log "s10: clearing the archive and enabling it on the cluster"
    dc exec -T -u postgres "$p" bash -c "rm -f /mnt/wal-archive/* 2>/dev/null || true"
    sql "$p" "SELECT worlber_ha.set_config('wal_archive_dir', '/mnt/wal-archive')" >/dev/null
    sql "$p" "SELECT worlber_ha.set_config('max_slot_wal_keep', '32MB')" >/dev/null
    # the lab sets wal_keep_size=256MB, which would keep WAL regardless of the
    # slot limit; drop it so a stopped replica's slot can be invalidated.
    sql "$p" "ALTER SYSTEM SET wal_keep_size = 0" >/dev/null
    sql "$p" "SELECT pg_reload_conf()" >/dev/null
    sql "$p" "CREATE EXTENSION IF NOT EXISTS pgcrypto" >/dev/null
    # archive_mode needs a restart on every node: restart the containers once.
    log "s10: restarting nodes so archive_mode takes effect"
    dc restart pg1 pg2 pg3 >/dev/null
    wait_until 180 healthy || { log "s10: cluster not healthy after restart"; return 1; }
    p=$(primary)
    rep=$(anysql "SELECT node FROM worlber_ha.cluster_status WHERE role='replica' AND state='streaming' LIMIT 1")
    [ -n "$rep" ] || { log "s10: no replica"; return 1; }

    log "s10: stopping replica $rep"
    dc stop "$rep" >/dev/null
    log "s10: writing WAL on $p (2 GB) to invalidate $rep's slot"
    # pg_logical_emit_message writes its payload straight into WAL (no table,
    # no TOAST compression): ~320 MB of WAL in seconds, far past
    # max_slot_wal_keep (32 MB).  Checkpoints let the slot be invalidated.
    sql "$p" "CREATE TABLE IF NOT EXISTS s10_bulk (id bigserial PRIMARY KEY, note text)" >/dev/null
    sql "$p" "INSERT INTO s10_bulk (note) SELECT 'row ' || g FROM generate_series(1, 10000) g" >/dev/null
    local b
    for b in 1 2 3 4; do
        sql "$p" "SELECT count(pg_logical_emit_message(false, 's10', repeat(md5(g::text), 32768))) FROM generate_series(1, 80) g" >/dev/null
        sql "$p" "SELECT pg_switch_wal()" >/dev/null
        sql "$p" "CHECKPOINT" >/dev/null
    done
    sql "$p" "INSERT INTO s10_bulk (note) SELECT 'after ' || g FROM generate_series(1, 10000) g" >/dev/null
    sql "$p" "CHECKPOINT" >/dev/null
    local slotstat; slotstat=$(sql "$p" "SELECT coalesce(max(wal_status),'gone') FROM pg_replication_slots WHERE slot_name='worlber_ha_$rep'" 2>/dev/null)
    log "s10: slot for $rep is now '$slotstat'"
    if [ "$slotstat" != lost ]; then
        log "s10: SKIP - could not invalidate $rep's slot (wal_status=$slotstat); needs more WAL or a smaller max_slot_wal_keep here"
        sql "$p" "SELECT worlber_ha.set_config('wal_archive_dir', '')" >/dev/null 2>&1 || true
        sql "$p" "SELECT worlber_ha.set_config('max_slot_wal_keep', '100GB')" >/dev/null 2>&1 || true
        dc start "$rep" >/dev/null 2>&1 || true
        return 0
    fi
    local archived; archived=$(dc exec -T "$p" bash -c "ls /mnt/wal-archive | wc -l" | tr -d '\r')
    log "s10: $archived files in the archive; restarting $rep"
    local t0; t0=$(date +%s)
    dc start "$rep" >/dev/null

    # wait until the replica is streaming again (caught up)
    local ok=
    for _ in $(seq 1 600); do
        [ "$(anysql "SELECT state FROM worlber_ha.cluster_status WHERE node='$rep'")" = streaming ] && { ok=1; break; }
        sleep 1
    done
    [ -n "$ok" ] || { log "s10: $rep did not catch up"; return 1; }
    local caught=$(( $(date +%s) - t0 ))
    local reseeds; reseeds=$(anysql "SELECT count(*) FROM worlber_ha.events WHERE node='$rep' AND kind IN ('reseed_started','standby_reseed') AND event_time > to_timestamp($t0)")
    # every node must match the primary (bool_and over verify_table rows)
    local match; match=$(sql "$p" "SELECT bool_and(matches_primary) FROM worlber_ha.verify_table('s10_bulk') WHERE role='replica'" 2>/dev/null)
    local mbps; mbps=$(awk "BEGIN{c=$caught; if (c<1) c=1; printf \"%.1f\", $archived*16/c}")
    log "s10: caught up in ${caught}s, archived_segments=$archived (~${mbps} MB/s WAL), reseeds=$reseeds, verify_table match=$match"
    local matched=false
    [ "$match" = t ] || [ "$match" = true ] && matched=true
    jq -n --argjson caught "$caught" --argjson archived "$archived" --argjson reseeds "${reseeds:-0}" \
        --argjson matched "$matched" --arg mbps "$mbps" \
        '{scenario:"s10", catchup_s:$caught, archived_segments:$archived, reseeds:$reseeds, verify_match:$matched, wal_mbps:$mbps, pass:($reseeds==0 and $matched)}' \
        | tee "$base.json"
    [ "${reseeds:-1}" = 0 ] && [ "$matched" = true ]
}

sc=${1:-}
runs=${2:-1}
if [ "$sc" = s10 ]; then cmd_s10; exit $?; fi
if [ "$sc" = s9 ]; then cmd_s9; exit $?; fi
case "$sc" in s1|s1late|s2|s2b|s3|s4|s5|s7|s11|s12) ;; *) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;; esac
mkdir -p "$RESULTS"
stamp=$(date +%Y%m%d-%H%M%S)
failed=0
for ((i = 1; i <= runs; i++)); do
    run_one "$sc" "$i" "$stamp" || failed=$((failed + 1))
done
log "$sc: $((runs - failed))/$runs runs passed (results in $RESULTS/$sc-$stamp-*.json)"
[ "$failed" -eq 0 ]
