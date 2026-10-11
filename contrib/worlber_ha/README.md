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
* **Fencing.** The agent on the primary stops PostgreSQL (fast shutdown) when it
  loses contact with the Raft majority for `failover_timeout / 3`. The majority
  only promotes another node after `failover_timeout`.
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
| `pause()` / `resume()` | maintenance mode: no automatic failover, agents leave PostgreSQL alone |
| `connection_string(dbname, read_only => false)` | multi-host libpq URI with `target_session_attrs` (the "listener") |

| View | Content |
|---|---|
| `cluster_status` | node, host, port, role, state, sync, sync_state, timeline, lsn, replay_lsn, lag_bytes, raft_role, last_seen, agent_status |
| `cluster_config` | current options |
| `events` | history: cluster created, nodes added, switchovers, failovers, option changes |

Options:

| Option | Default | Meaning |
|---|---|---|
| `failover_timeout` | `30s` | how long the primary must be unreachable or down before automatic failover |
| `maximum_lag_on_failover` | `16MB` | a standby lagging more than this is not promoted automatically |
| `synchronous_mode` | `available` | `available`: sync standbys that are connected are required (commits continue if all are down). `strict`: commits wait until a sync standby is back, and only a sync standby can be promoted (zero data loss) |
| `paused` | `false` | set by `pause()` / `resume()` |

GUCs: `worlber_ha.agent_socket` (default
`<first unix_socket_directories>/.s.WORLBER_HA.<port>`, the same default the
agent uses) and `worlber_ha.agent_timeout` (default 30s).

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
* No virtual IP yet. Applications use `connection_string()` (libpq multi-host
  with `target_session_attrs=read-write`), or a proxy such as HAProxy that
  checks `pg_is_in_recovery()`.
* Read-only routing lists and backup preferences are not implemented yet.
