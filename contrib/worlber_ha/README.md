# worlber_ha: availability groups for PostgreSQL, managed from psql

`worlber_ha` turns ordinary PostgreSQL servers into a highly available cluster,
similar to a SQL Server Always On availability group. You run a few SQL
commands in psql. Replicas are seeded automatically, synchronous/asynchronous
commit is per node, and switchover is planned with no data loss. Automatic
failover and automatic rejoin of the old primary are built in. No Patroni,
etcd or Pacemaker is needed.

```sql
CREATE EXTENSION worlber_ha;

SELECT worlber_ha.create_cluster('prod');                       -- this server becomes the primary
SELECT worlber_ha.add_node('pg2.example.com', sync => true);    -- seeded with pg_basebackup, sync commit
SELECT worlber_ha.add_node('pg3.example.com');                  -- async commit

SELECT * FROM worlber_ha.cluster_status;
SELECT worlber_ha.switchover('pg2.example.com');                -- planned failover, zero data loss
```

## Architecture

```
            psql / application
                  |
   +--------------+---------------+   +----------------+   +----------------+
   | node pg1                     |   | node pg2       |   | node pg3       |
   |  PostgreSQL + worlber_ha ext |   |  PostgreSQL    |   |  PostgreSQL    |
   |        | unix socket         |   |      |         |   |      |         |
   |  worlber-ha-agent  <=========Raft=====> agent  <=========Raft=> agent   |
   +------------------------------+   +----------------+   +----------------+
```

* **Extension (`worlber_ha`, C + SQL).** This is the user interface only. Every
  function sends a JSON request to the local agent over a Unix socket and returns
  the reply. You can call it on any node; replicas forward the request to the
  Raft leader.
* **Agent (`worlber-ha-agent`, Go).** One runs on every node. The agents form a
  Raft group (hashicorp/raft, embedded, no etcd). That group stores the cluster
  topology: which node is primary (with an *epoch* number), the members, and the
  options. Each agent:
  * probes its local PostgreSQL every second and reports to the Raft leader;
  * reconciles the local server with the replicated state: it promotes, follows
    the primary, seeds with `pg_basebackup`, and rejoins with `pg_rewind`;
  * on the leader, decides on failover when the primary has been unhealthy for
    longer than `failover_timeout`;
  * on the primary, maintains one physical replication slot per standby and
    manages `synchronous_standby_names`.

Failover logic never runs inside PostgreSQL. If the logic lived in the server
that just died, failover would die with it.

### Split-brain protection

* Changing the primary is a Raft commit, so only a majority of agents can do it.
* **Fencing with a lease.** The primary holds a lease file
  (`$PGDATA/worlber_ha.lease`). Its agent renews it only while the Raft leader
  confirms, in the reply to each heartbeat, that this node is still the
  primary.
  * A **fence worker inside PostgreSQL** checks the lease every 500 ms and shuts
    the server down once it is older than its TTL (`failover_timeout / 3`).
    Because it runs in PostgreSQL, it works even when the agent is dead or hung.
  * The agent stops PostgreSQL the same way, and escalates to SIGKILL if a fast
    shutdown hangs for more than 5 s.
  * The leader stops confirming the old primary before a failover. It then
    waits `2s + TTL + 2.5s` after the last confirmation before promoting a
    standby. By then the old primary has been fenced, so two primaries never
    accept writes at the same time.
  * The lease is stamped with `CLOCK_BOOTTIME`, which is not moved by NTP or by
    hand and keeps counting while a VM or container is paused.
  * `failover_timeout` must be at least 7 seconds for this bound to hold.
* The agent on the primary also stops PostgreSQL (fast shutdown) when it loses
  contact with the Raft majority for `failover_timeout / 3`.
* An agent only starts or promotes PostgreSQL as primary, or demotes it, after
  the Raft leader confirms the current epoch. A restarted agent replaying an old
  Raft log can therefore never bring back a stale primary.
* A former primary is never started read-write again. It is rewound with
  `pg_rewind`, or re-seeded if the rewind fails (the old directory is kept as
  `PGDATA.worlber_ha_diverged.<time>`), and then joins as a standby.

A cluster needs **at least three agents** to survive the loss of one node. With
two database servers, add a third agent as a **witness** (`--witness`). It votes
in Raft but has no PostgreSQL, like a SQL Server file share witness.

## Installation

Build PostgreSQL with this contrib module as usual, then build the agent (it
needs Go 1.24 or newer):

```sh
make -C contrib/worlber_ha install          # extension
make -C contrib/worlber_ha install-agent    # builds agent/worlber-ha-agent, installs into bindir
```

On every node, run the agent as the PostgreSQL OS user. Use the same `--token`
on every node. See `packaging/worlber-ha-agent.service` for a systemd unit.

```sh
worlber-ha-agent \
  --name pg1 \
  --token-file /etc/worlber_ha/token \
  --pgdata /var/lib/pgsql/data --pgbin /usr/pgsql/bin \
  --pg-socket-dir /var/run/postgresql --pg-port 5432 \
  --pg-advertise-host pg1.example.com \
  --api-addr pg1.example.com:7400 --raft-addr pg1.example.com:7401 \
  --data-dir /var/lib/worlber_ha
```

* On the **primary**, the agent starts PostgreSQL itself if it is not
  running. This only happens while the node has never been in a cluster.
* Use `--api-bind 0.0.0.0:7400 --raft-bind 0.0.0.0:7401` to listen on all
  interfaces while advertising `--api-addr`/`--raft-addr`.
* On a **new replica**, leave `--pgdata` empty or absent. The agent fills it with
  `pg_basebackup` when the node is added. A data directory that belongs to
  another database system is never overwritten unless you pass
  `--reinit-pgdata`.
* A **witness** only needs `--witness`, the token and the addresses.

**Fencing needs `worlber_ha` in `shared_preload_libraries`** on every node,
like `pg_stat_statements`:

```
shared_preload_libraries = 'worlber_ha'      # append to an existing list
```

If it is missing, the agent adds it with `ALTER SYSTEM` and records a
`fencing_inactive` event. Fencing then becomes active at the next PostgreSQL
restart. Until then only the agent can fence; the fence worker, which works
when the agent is dead, is not running. The agent turns `worlber_ha.fence` on by
itself after the first lease is written.

`create_cluster` prepares the primary:
* it creates the role `worlber_ha_repl` (REPLICATION, plus the `pg_rewind`
  function grants) with a random password;
* it appends `scram-sha-256` lines for that role to `pg_hba.conf`;
* it enables `wal_log_hints` when data checksums are off.

If `wal_level` or `wal_log_hints` had to be changed, it warns you to restart
PostgreSQL before adding replicas.

Open TCP ports 7400 (agent API) and 7401 (Raft) between the nodes, as well as
the PostgreSQL port.

## SQL reference

All objects live in schema `worlber_ha`. Management needs superuser or the role
`worlber_ha_admin`. The views can be read by `pg_monitor`.

| Function | Meaning (SQL Server equivalent) |
|---|---|
| `create_cluster(name, synchronous_mode => 'available', failover_timeout => '30s')` | `CREATE AVAILABILITY GROUP` on the current primary |
| `add_node(host, agent_port => 7400, sync => false, wait => true, timeout_s => 900)` | `ADD REPLICA` with automatic seeding; waits until it streams |
| `add_witness(host, agent_port => 7400)` | file share witness |
| `remove_node(node)` | `REMOVE REPLICA`; the removed agent resets its state and restarts, so it can be added again |
| `switchover(target => NULL)` | planned manual failover, no data loss (NULL = best standby) |
| `failover(target => NULL, force => false)` | forced failover; `force` = `FORCE_FAILOVER_ALLOW_DATA_LOSS` |
| `set_sync(node, true/false)` | `AVAILABILITY_MODE = SYNCHRONOUS_COMMIT / ASYNCHRONOUS_COMMIT` |
| `set_config(option, value)` | cluster options, see below |
| `reinit_node(node)` | wipe a standby and seed it again from the primary (after a long outage) |
| `verify_table(table, dbname)` | checksum of a table on every node at the same LSN, with `matches_primary` |
| `pause()` / `resume()` | maintenance mode: no automatic failover, agents leave PostgreSQL alone |
| `connection_string(dbname, read_only => false)` | multi-host libpq URI with `target_session_attrs` |
| `health_check()` | one row per problem (`CRITICAL`/`WARNING`), or a single `OK` row |
| `agent_log(node => NULL, lines => 100, min_level => 'info')` | the agent log of any node, read through SQL |

| View | Content |
|---|---|
| `cluster_status` | node, host, port, role, state, sync, sync_state, timeline, lsn, replay_lsn, lag_bytes, raft_role, last_seen, agent_status |
| `cluster_config` | current options |
| `events` | history: cluster created, nodes added, seeding, rewind/rejoin, promotion, demotion, fencing, VIP moves, switchovers, failovers, option changes |
| `replication` | per standby: sent/write/flush/replay LSN, **log send queue**, **redo queue**, write/flush/replay lag (like `sys.dm_hadr_database_replica_states`) |
| `redo` | on the node you are connected to: WAL received vs replayed, redo queue, replay delay, pause state |
| `blocking` | on the node you are connected to: commits waiting for a synchronous replica, lock waits, and redo blocked by queries on a standby |

Options:

| Option | Default | Meaning |
|---|---|---|
| `failover_timeout` | `30s` | how long the primary must be unreachable or down before automatic failover |
| `maximum_lag_on_failover` | `16MB` | a standby lagging more than this is not promoted automatically |
| `synchronous_mode` | `available` | `available`: sync standbys that are connected are required (commits continue if all are down). `strict`: commits wait until a sync standby is back, and only a sync standby can be promoted (zero data loss) |
| `paused` | `false` | set by `pause()` / `resume()` |
| `max_slot_wal_keep` | `100GB` | most WAL the primary keeps for a disconnected standby (`max_slot_wal_keep_size`); `'unlimited'` to never give up |
| `auto_reinit` | `true` | re-seed a standby automatically when the WAL it needs is gone |
| `seed_max_rate` | (unlimited) | `pg_basebackup --max-rate` for seeding, e.g. `'200M'` |

GUCs: `worlber_ha.agent_socket` (default
`<first unix_socket_directories>/.s.WORLBER_HA.<port>`, the same default the
agent uses) and `worlber_ha.agent_timeout` (default 30s).

## Connecting applications: listener, VIP, load balancer

Only the primary accepts writes. Replicas are hot standbys and accept
read-only queries (single writer, no multi-master). There are four ways for
applications to always find the primary:

| Option | How | Best for |
|---|---|---|
| **Listener** (built in, like an AG listener) | start the agents with `--listen-rw 0.0.0.0:5000 --listen-ro 0.0.0.0:5001`. Port 5000 on *any* node forwards to the current primary; port 5001 spreads read-only sessions over streaming standbys (and falls back to the primary). On a role change, the listener closes connections to the old primary so that clients reconnect at once. | every setup; combine with a VIP or DNS name listing all nodes |
| **VIP** (built in) | `--vip 10.0.0.100/24 --vip-interface eth0`. The primary's agent adds the IP and sends gratuitous ARP; the agent of a demoted or fenced primary removes it. Requires `CAP_NET_ADMIN`/`CAP_NET_RAW` (the systemd unit grants them). | on-prem / VMware / bare metal, same L2 network |
| **Load balancer** | health checks against the agent: `GET http://node:7400/health/primary` returns 200 only on the primary; `/health/replica` returns 200 only on a streaming standby. | GCP internal TCP LB, AWS NLB, HAProxy, F5 |
| **Role-change hook** | `--on-role-change /path/script`, called as `script <primary\|replica\|stopped> <cluster> <node>`. See `packaging/role-change-gcp-alias-ip.sh` (moves a GCP alias IP). | cloud alias IPs, DNS updates, custom actions |
| **libpq multi-host** | `SELECT worlber_ha.connection_string()` | apps that can use libpq/JDBC multi-host URIs |

The lab (`testing/lab.sh`) has the listener on ports 5000/5001 and the VIP
`172.28.0.100` enabled.

## How data is replicated

worlber_ha uses PostgreSQL's built-in physical streaming replication. It
does not copy data itself.

1. **Seeding:** `add_node` makes the new node's agent run `pg_basebackup` from
   the primary through its own replication slot. This is like AG automatic
   seeding.
2. **Streaming:** every change on the primary is written to the WAL. The
   primary's WAL sender streams it to each standby's WAL receiver, which
   writes and flushes it to the standby's disk.
3. **Redo:** the standby's startup process replays (redoes) the WAL. Data
   becomes visible to read-only queries on the replica once it is replayed.
4. **Synchronous commit:** a COMMIT on the primary waits until one
   synchronous standby has flushed the WAL. That is PostgreSQL's
   `synchronous_commit = on`, managed by the agent through
   `synchronous_standby_names = 'FIRST 1 (...)'`. Nodes added with
   `sync => true` are listed first. Every other standby follows as a
   fallback, so a synchronous standby still exists when the node marked sync
   is the primary itself (after a failover or switchover) or is down.
   `health_check()` reports CRITICAL when no standby is synchronous, and a
   WARNING when a fallback standby holds that role. A session can ask for
   more or less:
   * `SET synchronous_commit = remote_apply`: wait until the standby has
     *replayed* the change (read-your-writes on the replica);
   * `remote_write`: the standby has received the change;
   * `local`: do not wait.

   If all synchronous standbys are down:
   * `synchronous_mode = 'available'` (default): the agent removes them from
     `synchronous_standby_names` and commits continue;
   * `'strict'`: commits wait (visible in `worlber_ha.blocking`) until a
     synchronous standby is back. There is never data loss on failover.
5. **Slots:** each standby has a physical replication slot
   (`worlber_ha_<node>`) on the primary, so WAL is kept until the standby has
   received it.
6. **Failover/rejoin:** a standby is promoted with `pg_promote()`. The others
   are re-pointed to it with ALTER SYSTEM `primary_conninfo` and a reload. The
   old primary is synchronized with `pg_rewind` and restarted as a standby.

## Reliability questions

**Multiple databases.** Replication is physical. Every database of the
instance (everything in `\l`, including new ones created later), every role
and every tablespace is replicated and fails over together. It is like an
availability group that contains all databases. There is nothing to configure
per database.

**A replica was offline for 2 days.**
1. While a replica is down, the primary keeps the WAL it needs in the
   replica's replication slot.
2. When the replica comes back, it catches up automatically from that WAL. No
   full copy is needed.
3. To protect the primary's disk, it keeps at most `max_slot_wal_keep` of WAL
   per replica (default `100GB`; set it to what your disk can hold, with
   `set_config('max_slot_wal_keep', '500GB')`).
4. If the replica fell further behind, the primary invalidates its slot
   (event `slot_invalidated`), and its disk is safe.
5. When the replica returns, its agent sees that the WAL it needs is gone. It
   re-seeds the replica with a fresh `pg_basebackup` (events `reseed_started`,
   `seeding_completed`). To do this by hand instead, set
   `set_config('auto_reinit', 'false')`. `cluster_status.agent_status` then
   says "needs re-seed", and you run `SELECT worlber_ha.reinit_node('pg2')`
   when it suits you.

**Adding a node to a 1 TB database.**
* `add_node` seeds the new node with `pg_basebackup`, a full physical copy of
  all databases streamed from the primary. Writes continue on the primary
  while it runs; the WAL produced meanwhile is streamed afterwards, so the
  replica ends up exactly in sync.
* Time is about size / bandwidth: 1 TB takes about 2.5 hours at 1 Gbit/s and
  about 20 minutes at 10 Gbit/s.
* Use `add_node('pg3', wait => false)` and follow progress in
  `SELECT node, agent_status FROM worlber_ha.cluster_status`. It shows, for
  example: `seeding from pg1: 45% (450.0 GB of 1.0 TB), streaming database
  files`.
* `set_config('seed_max_rate', '200M')` limits the copy to 200 MB/s so that it
  does not saturate the primary's disk or network.
* The new node only becomes a failover candidate once it is streaming.

**Seeing data loss.**

| Question | Command |
|---|---|
| How much would I lose if I fail over now? | `SELECT node, sync_state, data_loss_if_failover_now FROM worlber_ha.replication;`: `none (synchronous)` for sync standbys, otherwise the WAL not yet received |
| How much was lost in the last failover? | `SELECT event_time, detail FROM worlber_ha.events WHERE kind IN ('automatic_failover', 'manual_failover', 'switchover');`: each event records the estimate, e.g. `estimated data loss: none (pg2 was a synchronous standby)` |
| Are the replicas really identical? | `SELECT * FROM worlber_ha.verify_table('orders');` (and `verify_table('orders', 'app2')` for another database): checksum and row count on every node at the same LSN, plus `matches_primary` |
| Can a failover lose committed transactions? | Not if the new primary is a synchronous standby (always chosen first). With `synchronous_mode = 'strict'`, only a synchronous standby can be promoted, so committed transactions are never lost. |

The lab suite (`testing/lab.sh test`) verifies all of this:
* it kills the primary while a client is committing through the listener, then
  checks that every acknowledged commit exists on the new primary;
* it leaves a replica down while 400 MB of WAL is written with
  `max_slot_wal_keep = 64MB`, then checks the automatic re-seed;
* it runs `reinit_node` with a throttled seed;
* it compares table checksums across all nodes, in two databases.

## Monitoring cheat sheet

```sql
SELECT * FROM worlber_ha.health_check();          -- is everything OK? what is wrong?
SELECT * FROM worlber_ha.cluster_status;          -- roles, states, lag, Raft roles, agent status
SELECT * FROM worlber_ha.replication;             -- log send queue / redo queue / lag per standby
SELECT * FROM worlber_ha.redo;                    -- on a replica: redo queue, replay delay
SELECT * FROM worlber_ha.blocking;                -- sync-commit waits, lock waits, blocked redo
SELECT * FROM worlber_ha.events LIMIT 20;         -- failover, rejoin, seeding, fencing history
SELECT * FROM worlber_ha.events WHERE node = 'pg2';                 -- the join/rejoin story of one node
SELECT * FROM worlber_ha.agent_log('pg2', 50);                      -- the agent log of pg2
SELECT * FROM worlber_ha.agent_log(min_level => 'warn', lines => 50); -- warnings/errors here
```

Typical sequence of `events` during an automatic failover and rejoin:
`primary_unhealthy` → `automatic_failover` → `promoted` → `role_primary` /
`vip_acquired` → `repointed` (other standbys) → later, on the old primary,
`demoted` or `rejoin_started` → `rewind_completed` → `standby_started`.

## Testing

**To try it, start with [`testing/TESTING.md`](testing/TESTING.md).** It has
a ready-made Docker lab (`testing/lab.sh up && testing/lab.sh test`), a
step-by-step guide for three VMs, and a test plan.

```sh
make -C contrib/worlber_ha agent-check          # Go unit tests
make -C contrib/worlber_ha agent
# SQL regression test + TAP test with 3 nodes on localhost (run as non-root;
# without WORLBER_HA_AGENT the TAP test is skipped)
WORLBER_HA_AGENT=$PWD/contrib/worlber_ha/agent/worlber-ha-agent \
  make -C contrib/worlber_ha check
```

The TAP test builds a 3-node cluster from psql, checks replication and
synchronous commit, switches over, kills the primary (agent and server),
verifies the automatic failover, and verifies that the old primary rejoins
through `pg_rewind`.

## Current limitations (next milestones)

* Agent-to-agent traffic (API and Raft) is plain TCP, authenticated by the
  shared token. Put it on a private network until TLS (mTLS) is added. The
  replication password is stored in the Raft state (`--data-dir`, mode 0700).
* Backup preferences (which node runs backups) are not implemented yet.
