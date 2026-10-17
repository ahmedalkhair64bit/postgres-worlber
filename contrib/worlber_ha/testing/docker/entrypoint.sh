#!/bin/bash
# Container entrypoint: prepare directories, optionally initdb the first
# primary, then run worlber-ha-agent as the postgres user.  The agent starts,
# stops, promotes and seeds PostgreSQL itself.
set -euo pipefail

NODE_NAME=${NODE_NAME:-$(hostname)}
: "${WORLBER_HA_TOKEN:?set WORLBER_HA_TOKEN (same on every node)}"
PGDATA=${PGDATA:-/var/lib/postgresql/data}
SOCKDIR=/var/run/postgresql
STATE=/var/lib/worlber_ha

mkdir -p "$SOCKDIR" "$STATE" "$(dirname "$PGDATA")"
chown postgres:postgres "$SOCKDIR" "$STATE" "$(dirname "$PGDATA")"
# shared WAL archive (W6), present only when the compose mounts it
[ -d /mnt/wal-archive ] && chown postgres:postgres /mnt/wal-archive || true
chmod 2775 "$SOCKDIR"

as_pg() { runuser -u postgres -- "$@"; }

if [ "${INIT_PRIMARY:-0}" = "1" ] && [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "entrypoint: initializing primary in $PGDATA"
    pwfile=$(mktemp); echo "${POSTGRES_PASSWORD:-postgres}" > "$pwfile"; chown postgres "$pwfile"
    as_pg initdb -D "$PGDATA" -U postgres --pwfile="$pwfile" \
        --auth-local=trust --auth-host=scram-sha-256 >/dev/null
    rm -f "$pwfile"
    cat >> "$PGDATA/postgresql.conf" <<CONF

# worlber_ha lab settings
listen_addresses = '*'
unix_socket_directories = '$SOCKDIR'
wal_keep_size = '256MB'
log_destination = 'stderr'
logging_collector = off
# fence worker: stops this server if it is primary and its agent stopped
# confirming it with the cluster (worlber_ha.fence is turned on by the agent)
shared_preload_libraries = 'worlber_ha'
CONF
    # application access from the lab network (and the docker host)
    echo "host all all 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
fi

# The agent never stops PostgreSQL when it exits (an agent restart must not
# take the database down).  In a container, the server dies with the
# container, so stop it cleanly after the agent ends.
# With a VIP the agent needs CAP_NET_ADMIN/CAP_NET_RAW (ip addr, arping):
# pass them as ambient capabilities to the unprivileged agent.
# Chaos S9 (rolling upgrade): a binary at $STATE/override/worlber-ha-agent replaces the
# image's agent, so a scenario can run an older or newer agent per node.
AGENT=worlber-ha-agent
if [ -x "$STATE/override/worlber-ha-agent" ]; then
    AGENT=$STATE/override/worlber-ha-agent
    echo "entrypoint: using $AGENT ($("$AGENT" --version 2>&1 | head -1))"
fi
caps=()
if [ -n "${VIP:-}" ]; then
    caps=(--inh-caps=+net_admin,+net_raw --ambient-caps=+net_admin,+net_raw)
fi
setpriv --reuid=postgres --regid=postgres --init-groups "${caps[@]}" -- "$AGENT" \
    --name "$NODE_NAME" \
    --token "$WORLBER_HA_TOKEN" \
    --data-dir "$STATE" \
    --pgdata "$PGDATA" \
    --pgbin /usr/local/pgsql/bin \
    --pg-socket-dir "$SOCKDIR" \
    --pg-port 5432 \
    --pg-advertise-host "$NODE_NAME" \
    --pg-log "$STATE/postgresql.log" \
    --api-addr "$NODE_NAME:7400" --api-bind 0.0.0.0:7400 \
    --raft-addr "$NODE_NAME:7401" --raft-bind 0.0.0.0:7401 \
    --listen-rw 0.0.0.0:5000 --listen-ro 0.0.0.0:5001 \
    ${VIP:+--vip "$VIP" --vip-interface "${VIP_INTERFACE:-eth0}"} \
    --log-level "${LOG_LEVEL:-info}" \
    "$@" &
agent=$!
trap 'kill -TERM $agent 2>/dev/null' TERM INT
wait $agent || true
wait $agent 2>/dev/null || true
if [ "${CHAOS_KEEP_PG:-0}" = "1" ]; then
    # Chaos scenario S2: behave like production systemd (KillMode=process)
    # with an agent that does not come back: PostgreSQL keeps running.
    echo "entrypoint: agent ended; CHAOS_KEEP_PG=1 keeps PostgreSQL running"
    trap 'exit 0' TERM INT
    while :; do sleep 1; done
fi
if as_pg pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
    echo "entrypoint: agent ended, stopping PostgreSQL"
    as_pg pg_ctl -D "$PGDATA" stop -m fast -w -t 25 || true
fi
