# Testing worlber_ha

There are two ways to test:

* **A. Docker lab:** three nodes on one machine, ready in about 10 minutes.
  Use it to learn the commands and to run the automated failover suite.
* **B. Real servers / VMs:** three Linux VMs (on-prem or GCP). Use it to test
  real networks, real disks, and your operating procedures.

Both run the same code: PostgreSQL built from this repository, the
`worlber_ha` extension, and `worlber-ha-agent`.

---

## A. Docker lab

You need Docker with Compose v2 and about 4 GB of free disk.

```sh
cd contrib/worlber_ha/testing
./lab.sh up        # builds image worlber-pg:lab (compiles PostgreSQL), starts pg1, pg2, pg3
./lab.sh setup     # create_cluster on pg1, add pg2 (sync) and pg3 (async)
./lab.sh status
```

### Automated test suite

```sh
./lab.sh test
```

This starts from a fresh lab and runs every scenario below automatically.
It prints `PASS`/`FAIL` per check and ends with `== RESULT: N passed, 0 failed`.

1. Cluster creation, sync and async replication, read-only replicas.
2. Planned switchover pg1 → pg2, requested from pg3. The old primary rejoins.
3. The primary container is killed: automatic failover with no committed rows
   lost.
4. The crashed node comes back and rejoins with `pg_rewind`.
5. **Network partition:** the primary is cut off the network. It fences itself
   (PostgreSQL stops), the majority promotes a new primary, and after healing
   the old primary rejoins.
6. A replica is removed with `remove_node()` and added back.
7. Final check: every node has the same rows.

### Manual exploration

```sh
./lab.sh psql              # psql on the current primary (also: ./lab.sh psql pg3)
./lab.sh sql "SELECT * FROM worlber_ha.events"
./lab.sh kill pg1          # hard crash: agent and PostgreSQL die (like a power loss)
./lab.sh start pg1         # node comes back, rejoins automatically
./lab.sh isolate pg2       # network partition of one node
./lab.sh heal pg2
./lab.sh monitor           # health_check, cluster_status, replication queues, blocking, events
./lab.sh agentlog pg2      # agent log of pg2, read through SQL (worlber_ha.agent_log)
./lab.sh logs pg2          # agent log (decisions: failover, rewind, fencing...)
./lab.sh pglog pg2         # PostgreSQL server log
./lab.sh witness           # start an optional 4th agent as witness, then:
./lab.sh sql "SELECT worlber_ha.add_witness('witness')"
./lab.sh down              # delete everything
```

From the docker host you can connect to the nodes with ports 5441, 5442 and
5443 (user `postgres`, password `postgres`):

```sh
psql "postgresql://postgres:postgres@localhost:5441,localhost:5442,localhost:5443/postgres?target_session_attrs=read-write"
```

Easier: use the built-in listener: `psql -h localhost -p 5000 -U postgres`
always reaches the primary, and `-p 5001` reaches a replica (read-only). Inside
the lab network, the VIP `172.28.0.100:5432` always points at the primary.

The multi-host URI above also always lands on the current primary, so you can use it to test how an
application reconnects during a failover. For example, keep this loop running
while you `./lab.sh kill` the primary:

```sh
while true; do
  psql "postgresql://postgres:postgres@localhost:5441,localhost:5442,localhost:5443/postgres?target_session_attrs=read-write&connect_timeout=2" \
    -Atc "CREATE TABLE IF NOT EXISTS app(t timestamptz); INSERT INTO app VALUES (now()) RETURNING inet_server_port(), now()" 2>&1 | tail -1
  sleep 1
done
```

Building behind a TLS-intercepting proxy:
`LAB_CA_CERT=/path/ca.crt LAB_BUILD_ARGS="--network host" ./lab.sh up`.

### Chaos harness (load + faults, measured RTO/RPO)

`chaos/run.sh` runs a fault scenario under write load and prints a verdict
per run. Writers use `worlber-ledger` (built from `agent/cmd/worlber-ledger`).
They write through the read-write listener (port 5000) on all three nodes.
Every write attempt is logged as one of:
- **ack**: COMMIT returned OK;
- **failed**: an error before COMMIT was sent;
- **unknown**: COMMIT was sent but its result is not known.

Only **ack** rows count as data loss when they are missing.

```bash
./chaos/run.sh s1 5                     # kill -9 the primary's postmaster, 5 runs
CHAOS_KEEP_PG=1 ./lab.sh up && ./lab.sh setup
./chaos/run.sh s2 5                     # kill -9 the primary's agent; PostgreSQL stays up
CHAOS_SOAK=30m ./chaos/run.sh s7        # no fault: false failovers, report cancellations
```

| Scenario | Fault | Pass |
|---|---|---|
| s1 | `kill -9` of the postmaster on the primary; its agent stays alive | 0 acked rows missing, RTO ≤ `CHAOS_RTO_MAX` (30s) |
| s2 | `kill -9` of the agent on the primary; PostgreSQL keeps running (like systemd `KillMode=process`) | 0 acked rows missing, 0 acked rows only on the old primary, RTO ≤ 30s. The fence worker should stop the old primary. If it did, the harness starts the old data in isolation to compare its rows: Unix socket only, read-only, `worlber_ha.fence=off`. The verdict records `old_primary_state` |
| s2b | network partition of the primary **and** `SIGSTOP` a backend so fast shutdown hangs | 0 acked rows missing; every postgres process of the old primary gone (the agent escalates to SIGKILL) |
| s3 | `docker pause` the whole primary node until the run ends | 0 acked rows missing (the lease clock keeps running while paused) |
| s4 | network partition of the primary until the run ends | 0 acked rows missing |
| s7 | none, for `CHAOS_SOAK`; long reports on port 5001 | 0 acked rows missing, 0 automatic failovers |

Each run writes `chaos/results/<scenario>-<stamp>-<n>.json`. It holds the
ledger verdict (`acked`, `acked_missing`, `old_only_acked`, `rto_ms`, ack
latency p50/p99), the cluster events since the run started, the old and new
primary, and `pass`.

To check a ledger by hand:
`worlber-ledger verify --ledger run.ledger --dsn "<new primary>" [--old-dsn "<old primary>"] --fault-at <unix ms>`.
It exits 1 when the run fails.

---

## B. Three real servers (VMs)

Example: `db1` 10.0.0.11, `db2` 10.0.0.12, `db3` 10.0.0.13, running
Ubuntu 22.04/24.04 or RHEL/Rocky 8/9. On GCP, use three e2-standard-2 VMs in
the same VPC, ideally in three zones.

### 1. Build and install (on every server)

```sh
# Ubuntu
sudo apt-get install -y build-essential bison flex libreadline-dev zlib1g-dev libssl-dev libicu-dev pkg-config golang-go git
# RHEL/Rocky
sudo dnf groupinstall -y "Development Tools"; sudo dnf install -y bison flex readline-devel zlib-devel openssl-devel libicu-devel golang git

git clone -b claude/brave-archimedes-ru6o5o https://github.com/ahmedalkhair64bit/postgres-worlber.git
cd postgres-worlber
./configure --prefix=/usr/local/pgsql --with-openssl
make -j"$(nproc)" && sudo make install
sudo make -C contrib install                     # includes worlber_ha
make -C contrib/worlber_ha agent && sudo cp contrib/worlber_ha/agent/worlber-ha-agent /usr/local/pgsql/bin/
```

The agent needs Go ≥ 1.24. If your distribution ships an older one, install Go
from https://go.dev/dl/, or build the agent on any machine and copy the
static binary: `CGO_ENABLED=0 go build ./cmd/worlber-ha-agent`.

### 2. OS user, directories, firewall (on every server)

```sh
sudo useradd -m -d /var/lib/postgresql postgres 2>/dev/null || true
sudo mkdir -p /var/lib/worlber_ha /var/run/postgresql /etc/worlber_ha
sudo chown postgres: /var/lib/worlber_ha /var/run/postgresql
openssl rand -hex 24 | sudo tee /etc/worlber_ha/token    # generate ONCE, copy the same file to all servers
sudo chown postgres: /etc/worlber_ha/token && sudo chmod 600 /etc/worlber_ha/token
# open 5432 (PostgreSQL), 7400 (agent API), 7401 (Raft) between the servers
sudo firewall-cmd --permanent --add-port={5432,7400,7401}/tcp && sudo firewall-cmd --reload   # RHEL
sudo ufw allow from 10.0.0.0/24 to any port 5432,7400,7401 proto tcp                          # Ubuntu
```

On GCP, create a VPC firewall rule that allows tcp:5432,7400,7401 between the
three VMs (by network tag).

### 3. Initialize PostgreSQL on db1 only

```sh
sudo -u postgres /usr/local/pgsql/bin/initdb -D /var/lib/postgresql/data --auth-local=peer --auth-host=scram-sha-256
sudo -u postgres tee -a /var/lib/postgresql/data/postgresql.conf <<'EOF'
listen_addresses = '*'
unix_socket_directories = '/var/run/postgresql'
wal_keep_size = '1GB'
EOF
```

Do **not** initdb on db2 and db3. Their data directories are created by the
agent with `pg_basebackup`.

### 4. Start the agent on every server

Create `/etc/worlber_ha/agent.env` with this server's name and address (here
for db1; change the three db1 values on the other servers):

```sh
WORLBER_HA_NAME=db1
WORLBER_HA_TOKEN_FILE=/etc/worlber_ha/token
WORLBER_HA_DATA_DIR=/var/lib/worlber_ha
WORLBER_HA_API_ADDR=10.0.0.11:7400
WORLBER_HA_RAFT_ADDR=10.0.0.11:7401
WORLBER_HA_PG_HOST=10.0.0.11
WORLBER_HA_PGBIN=/usr/local/pgsql/bin
WORLBER_HA_PG_SOCKET_DIR=/var/run/postgresql
PGDATA=/var/lib/postgresql/data
```

```sh
sudo cp contrib/worlber_ha/packaging/worlber-ha-agent.service /etc/systemd/system/
sudo sed -i 's#/usr/pgsql/bin#/usr/local/pgsql/bin#' /etc/systemd/system/worlber-ha-agent.service
sudo systemctl daemon-reload && sudo systemctl enable --now worlber-ha-agent
journalctl -u worlber-ha-agent -f
```

On db1 the agent starts PostgreSQL itself, as a standalone server. On db2 and
db3 it waits with "waiting: not part of a cluster".

### 5. Build the cluster from psql (on db1)

```sql
sudo -u postgres psql
CREATE EXTENSION worlber_ha;
SELECT worlber_ha.create_cluster('prod');
SELECT worlber_ha.add_node('10.0.0.12', sync => true);
SELECT worlber_ha.add_node('10.0.0.13');
SELECT * FROM worlber_ha.cluster_status;
SELECT worlber_ha.connection_string('postgres');   -- give this to applications
```

If `create_cluster` warns that `wal_log_hints` or `wal_level` changed, run
`sudo -u postgres pg_ctl -D /var/lib/postgresql/data restart` before
`add_node`.

---

## Test plan (both setups)

Run `SELECT * FROM worlber_ha.cluster_status` and `SELECT * FROM
worlber_ha.events` after each step.

| # | Test | How | Expected |
|---|---|---|---|
| 1 | Replication | create a table and insert rows on the primary | rows visible on both replicas; `sync_state` = `sync` for the sync node, `potential` for the other |
| 2 | Sync commit | stop the sync replica (`systemctl stop worlber-ha-agent; pg_ctl stop`) and insert | default mode `available`: insert still succeeds; `synchronous_standby_names` becomes empty |
| 3 | Strict mode | `SELECT worlber_ha.set_config('synchronous_mode','strict')`, then repeat 2 | insert waits until the sync replica is back |
| 4 | Switchover | `SELECT worlber_ha.switchover('db2')` | db2 primary within seconds, db1 `streaming`, no data lost |
| 5 | Primary crash | on the primary: `sudo kill -9 $(pgrep -f worlber-ha-agent); sudo -u postgres pg_ctl -D $PGDATA stop -m immediate`, or power off the VM | after `failover_timeout` (default 30s) another node is primary; event `automatic_failover` |
| 6 | Rejoin | start the crashed node again (`systemctl start worlber-ha-agent`) | it rejoins as `replica / streaming` (`pg_rewind`); it never comes back as a second primary |
| 7 | PostgreSQL crash only | `kill -9` the postmaster on the primary (agent still running) | the agent restarts PostgreSQL on the same node; no failover |
| 8 | Agent crash only | `kill -9` the agent on the primary | PostgreSQL keeps serving; systemd restarts the agent; no failover |
| 9 | Network partition | on the primary: `sudo iptables -A INPUT -p tcp -m multiport --dports 7400,7401 -j DROP; sudo iptables -A OUTPUT -p tcp -m multiport --dports 7400,7401 -j DROP` | within about `failover_timeout/3` the isolated primary stops PostgreSQL (fencing); the other two promote a new primary. Remove the rules (`iptables -F`): the old primary rejoins as a replica |
| 10 | Maintenance | `SELECT worlber_ha.pause()`, then stop the primary | no failover while paused; `resume()` re-enables it |
| 11 | Lag protection | `set_config('maximum_lag_on_failover','1MB')`; on the async replica block replication: `iptables -A OUTPUT -p tcp --dport 5432 -j DROP`; generate >1MB of WAL on the primary (`INSERT ... generate_series(1,100000)`); crash the primary | the lagging replica (lag is measured on *received* WAL) is not promoted; the other one is. With every replica lagging, no automatic failover; `failover(force => true)` overrides |
| 12 | Remove / re-add | `SELECT worlber_ha.remove_node('db3')`, then `add_node('10.0.0.13')` | node leaves (its agent restarts with fresh state) and joins again |
| 13 | Application | run the reconnect loop from section A against `connection_string()` while doing 4 and 5 | writes resume on the new primary after a short interruption |
| 14 | Permissions | `CREATE ROLE ops LOGIN IN ROLE worlber_ha_admin; CREATE ROLE mon LOGIN IN ROLE pg_monitor;` | `ops` can manage; `mon` can read the views but cannot call `switchover()` |

## Reporting a problem

Please collect:

```sh
psql -c "SELECT * FROM worlber_ha.cluster_status" -c "SELECT * FROM worlber_ha.events ORDER BY seq"
journalctl -u worlber-ha-agent --since "-30 min" > agent-$(hostname).log     # each node (lab: ./lab.sh logs pgN)
tail -500 /var/lib/worlber_ha/postgresql.log > pg-$(hostname).log
```

## Known limitations of this version

* Agent traffic (ports 7400/7401) is not encrypted yet. It is authenticated with
  the token, so use a private network or VPC.
* There is no virtual IP. Use `connection_string()` (libpq multi-host) or
  HAProxy.
* At least three agents are needed. With two database servers, add a witness
  (`--witness`) on a third small VM.
