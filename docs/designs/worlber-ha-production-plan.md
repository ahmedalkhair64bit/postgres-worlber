# worlber_ha: production readiness plan (CEO review)

Branch: claude/brave-archimedes-ru6o5o | Base: master | Reviewed 2026-10-13

## Binding decisions
- B1 (user, 2026-10-13): **leave encryption alone.** No TLS/mTLS work in this plan.
- B2 (user): management stays at the psql level (SQL Server Always On style). No YAML, no external DCS.

## Problem (user's words, distilled)
Patroni works and self-heals, but it needs etcd plus a hand-edited YAML on
every node, and the steering, failover and failback tooling around it is often
paid. The user wants the Always On experience: a few SQL commands, fast
failover, fast recovery, and data that is reliably on the replicas under **high
production volume**. "If not stable, it's dangerous."

## Step 0A: Premise challenge
The real problem is **trust under production load**, not features. Feature
parity with Patroni is mostly done (1.0 to 1.2). What is missing is evidence
that it holds at volume, in measured numbers a DBA would sign off on. If we do
nothing, every customer conversation ends at "has it been tested under load?",
and the honest answer today is no.

| premise | claim | receipt | verdict |
|---|---|---|---|
| P1 | No YAML/etcd; managed from psql | worlber_ha--1.0.sql (create_cluster/add_node), agent/internal/agent/agent.go:148 (embedded Raft) | true |
| P2 | Each node still needs agent install + flags/env + token | packaging/worlber-ha-agent.service, testing/TESTING.md section B.4 | true: per-node setup moved, not gone |
| P3 | Failover is fast | fsm.go:32 failover_timeout default 30s; lab uses 10s, measured ~10-11s [measured: lab events 2026-10-11] | partly: no RTO percentile, no load |
| P4 | Recovery is fast | rejoin via pg_rewind 1-3s [measured: lab events]; re-seed is a full pg_basebackup (reconcile.go cloneFromPrimary) | rewind yes; re-seed of 1TB is hours |
| P5 | Data is on the replica under high volume | only test: 1 client, 452 commits (testing/lab.sh:257-280) | **unverified at volume** |
| P6 | Stable for production | no soak test, no load test, no chaos beyond kill/partition; no pgbench anywhere (grep: 0 hits) | **unverified** |
| P7 | Listener proxy adds no meaningful latency | router.go io.Copy proxy; never benchmarked | unverified |

unverified=3 (P5, P6, P7)

## Step 0B: Existing code leverage
| Sub-problem | Reuse |
|---|---|
| Load + failover harness | testing/lab.sh (check/wait_until/eq helpers, kill/isolate/heal), docker lab |
| RPO proof | the acked-writer pattern in lab.sh:257-280; scale it from 1 to N clients |
| Data equality | worlber_ha.verify_table() (ops.go verifyTable) |
| RTO measurement | worlber_ha.events timestamps (primary_unhealthy, automatic_failover, promoted) |
| Fast recovery | WAL archive + restore_command (standard PostgreSQL); incremental re-seed rejected (D11) |
| Metrics | agent heartbeat/status data already computed (buildStatus, replicationStatus) |

## Step 0C: Dream state
```
CURRENT (1.2 WIP)                 THIS PLAN                          12-MONTH IDEAL
features ~= Patroni,          ->  measured RTO/RPO under load,   ->  "Always On for Postgres":
tested on 1 client, docker        soak + chaos suite, published      install package, 3 SQL commands,
                                  numbers, the gaps they expose      published SLOs, Prometheus,
                                  fixed                              rolling upgrades, 1TB re-seed in minutes
```

## Landscape
- Patroni: flexible template, Python + DCS (etcd/Consul). Its flexibility adds caveats. ([Percona](https://www.percona.com/blog/patroni-the-key-postgresql-component-for-enterprise-high-availability/))
- pg_auto_failover: the closest competitor. A monitor node plus a keeper per node; the primary demotes itself after a 20s partition timeout. It prefers data availability over service availability. ([docs](https://pg-auto-failover.readthedocs.io/en/main/fault-tolerance.html), [architecture](https://pg-auto-failover.readthedocs.io/en/main/architecture.html))
- [Layer 3] Differentiation: no separate monitor database (embedded Raft), management from SQL on any node, and a built-in listener/VIP. Nobody publishes RTO/RPO numbers under load. **Publishing measured numbers is the moat.**

## Design doc
`docs/designs/worlber-ha-production-proof.md`: approach B, APPROVED after the
7 minor fixes. The round-3 blocking findings R3-1 to R3-4 were fixed after
the review cap; the helper verified the text mechanically ("issues verified:
4 of 4"), not by a fresh review. The changes this CEO review requires in the
design doc are listed under "Design doc deltas" below.

## Decision ledger
| ID / owner | Contract and evidence | Current | Proposed | Status | Approval |
|---|---|---|---|---|---|
| L0 mode | review mode | SELECTIVE EXPANSION | n/a | approved | D2 answer: "Selective expansion (recommended)" |
| L1 design doc | /office-hours output, approach B | APPROVED, docs/designs/worlber-ha-production-proof.md | n/a | approved | plan approval after "Fix the 7 minor concerns" |
| E1 fast recovery | 0G cherry-pick; serves "recovery is fast". Reopened: a running replica is not a valid incremental-backup base (pg_combinebackup requires untouched backups with backup_label) | full pg_basebackup on re-seed | **WAL archive + restore_command**: a long-offline replica catches up from the archive, with no re-seed | approved (ADD, revised) | D7 "Add", reopened by spec review; D11 answer "B: WAL archive, no re-seed (recommended)" |
| E2 Prometheus metrics | 0G cherry-pick; Prime Directive 5 (dashboards/alerts are launch scope) | SQL views only | agent `GET /metrics` on :7400 (role, lag, failovers, fence state, drill RTO) + example alert rules | deferred (TODOS.md) | D8 answer "Defer to TODOS.md" |
| E3 public scorecard | 0G cherry-pick; office-hours approach C | numbers in lab logs only | nightly CI publishes RTO/RPO per run to a `scorecard` branch + badge (W7) | approved (ADD) | D9 answer "Add to this plan (recommended)" |
| E4 rolling agent upgrade scenario | 0G cherry-pick; production stability | no upgrade test | S9: rolling 1.2→1.3 upgrade under ledger load (replicas, then a switchover, then the old primary), 0 failovers, 0 acknowledged rows lost | approved (ADD) | D10 answer "Add to this plan (recommended)" |
| W10 install + join wizard | D12: user asked "will it be easy to add more replica or remove replica" after the per-node setup was explained | agent install + hand-written env file + copied token on every node (P2) | deb/rpm packages, `worlber-ha-agent init`, one-time join token from psql (W10) | approved (ADD) | D12 answer "Approve + install/join wizard" |
| D13 archive + fence visibility (S2/S8) | Section 2: a failing `archive_command` is silent until pg_wal fills; fence state is not visible | free-space warning only | health_check CRIT on `pg_stat_archiver` failures or last archive > 5 min; event `archive_failing`; `cluster_status.fence_state` + `lease_age`; README runbooks | approved (ADD) | D13 answer "Full visibility (Recommended)" |
| D14 archive throughput proof (S7) | Section 7: gcsfuse/NFS per-segment latency may not keep up at high WAL rates | not measured | S7 soak and S10 run with the archive on; record archive MB/s vs WAL MB/s and archiver lag; publish the ceiling | approved (ADD) | D14 answer "Measure in S7 + S10 (Recommended)" |
| D15 upgrade rollback (S9/S10) | Section 9: after the gate opens there is no downgrade (one-way door) | not tested | S9 downgrades one replica 1.3 → 1.2 before the gate and proves health; docs mark `ALTER EXTENSION ... '1.3'` as the point of no return | approved (ADD) | D15 answer "Test pre-gate rollback (Recommended)" |
| D16 join hardening (S3/S4) | Section 3/4: a crash after consume leaves a used token and no node; /peer/join has no token auth | single-use only | retry-safe join (same host within TTL), 5/min per-IP rate limit, event `join_rejected` with IP, `--join-file`/stdin; TAP tests | approved (ADD) | D16 answer "All of it (Recommended)" |

## Accepted scope (working plan)
Source of truth for the details: the approved design doc,
docs/designs/worlber-ha-production-proof.md (approach B), plus E1, E3 and E4.

| # | Work item | Files (main) | Proof |
|---|---|---|---|
| W1 | `worlber-ledger` (3-outcome ledger) + report workload + divergence check + scenario runner; listener-vs-direct latency measurement (P7) | agent/cmd/worlber-ledger/, testing/chaos/run.sh, testing/chaos/scenarios/*.sh | S1/S2/S7 run; S1 and S2 fail before W2/W3 |
| W2 | Fencing: lease file, fence bgworker + `worlber_ha.fence`, Q=2s renewal rule, failover_timeout 20s, 3-probe health, agent fencing goroutine (**unconditional**, it does the SIGKILL escalation) | worlber_ha.c + new fence.c, agent/internal/agent/{reconcile,failover,agent}.go, fsm.go | S2, S2b, S3, S4 pass |
| W3 | Promotion safety: last good LSN, `FIRST 1` + `actual_sync` with safe ordering, `async_failover` | agent/internal/agent/{agent,failover,reconcile,ops}.go, fsm.go | S1, S5, S5b pass |
| W4 | Replica report policy, `ro_use_sync_standby=auto`, `ro_max_lag`, `ro_fallback` | router.go, reconcile.go, fsm.go | S8 passes |
| W5 | Live proof: canary `worlber_ha_internal.canary`, `worlber_ha.proof`, `drill()`, failover receipts; extension 1.3 | ops.go, new proof.go, worlber_ha--1.2--1.3.sql | the proof view shows a passing drill |
| W6 (E1) | WAL archive catch-up (spec below) | reconcile.go, pg.go, fsm.go, ops.go | S10 passes |
| W7 (E3) | Nightly public scorecard (spec below) | .github/workflows/worlber-ha-nightly.yml, testing/chaos/report.sh | every run published, pass or fail |
| W8 (E4) | S9 rolling 1.2 → 1.3 upgrade under load (spec below) | testing/chaos/scenarios/s9.sh, TESTING.md, fsm.go (version gate) | S9 passes |
| W9 | TODOS.md: record the deferred items (written during the review) | contrib/worlber_ha/TODOS.md | file lists E2, scheduled drills, standby-side health confirmation, softdog |
| W10 | Install + join wizard (spec below): deb/rpm, `worlber-ha-agent init`, `worlber_ha.join_token()` | agent/cmd/worlber-ha-agent/init.go, api.go (/peer/join), fsm.go (join_token op), worlber_ha--1.2--1.3.sql, packaging/nfpm.yaml | S11 passes; package install smoke on Ubuntu + Rocky |

Order: W9 → W1 → W2 → W3 → W4 → W5 → W6 → W10 → W8 → W7 (CI last, once the scenarios are stable; W10 before W8 so its op is covered by the version gate).

### W6 (E1) spec: WAL archive catch-up
- **Options:**
  - `wal_archive_dir` (default empty = off): a path mounted on **every** node
    (NFS, or a GCS bucket mounted with gcsfuse). With it set, the agent
    configures:
    - `archive_mode=on` on all nodes;
    - an idempotent `archive_command`: if `DIR/%f` exists and `cmp` matches,
      exit 0; if it differs, fail (PostgreSQL docs); otherwise
      `cp %p DIR/%f.tmp && mv DIR/%f.tmp DIR/%f`. This avoids a permanent
      stall after a crash between `mv` and `.done`;
    - `restore_command = 'cp DIR/%f %p'`.
  - Advanced: `archive_command` / `restore_command` set directly with
    `set_config` (e.g. gsutil). In that case the agent cannot check the
    archive contents and relies on replay progress (below).
  - `archive_mode=on` is set **only when an archive is configured**. With no
    archive it stays `off`; `archive_mode=on` without a working
    `archive_command` would keep all WAL and fill the disk.
  - `archive_mode` is a postmaster setting. Enabling the archive later, on a
    running 1.3 cluster, makes the agent perform a **rolling restart**:
    replicas one by one, then a switchover, then the old primary. It is
    recorded as events. It applies the S9 rule: before the sync standby
    restarts, sync is moved to another replica.
- **Re-seed trigger (changes `walGone` and `followPrimary`, reconcile.go:420-450):**
  - Archive recovery is in progress while the replica is in recovery and its
    replay LSN advanced within the last 120s.
  - While it is in progress, **both** the 30s "not streaming → rewind" path
    and **both** `walGone` branches (slot lost, and `restart_lsn > local`)
    are suppressed. A slot recreated by `ensureSlots` (reconcile.go:206) past
    the replica's position is expected. Once caught up, the standby resumes
    streaming at its own LSN.
  - Re-seed happens only if replay makes no progress for 120s, or the needed
    segment is in neither the archive nor the primary's `pg_wal` (streaming
    reports "requested WAL segment has already been removed"). The current,
    not yet archived segment at the end of catch-up is normal and never
    triggers a re-seed.
  - The event `catchup_from_archive` records the start and end LSN and the
    duration.
- **Retention:**
  - The leader prunes the archive with `pg_archivecleanup`, keeping
    max(`archive_keep` (default `7d`), the oldest segment needed by any
    member), **capped at `archive_max_keep` (default `30d`)**.
  - A member that needs older WAL than the cap is marked for re-seed (event
    `archive_retention_exceeded`), so a dead member cannot fill the archive.
  - In custom `archive_command` mode, retention and free space are the
    operator's job, and the docs say so.
  - `health_check()` warns when archive free space is below 10%, or when the
    archive is older than a member's needs (the member would re-seed).
  - **(D13)** `health_check()` reports **CRIT** when `pg_stat_archiver.failed_count`
    rose since the last check or `last_archived_time` is older than 5 min while
    WAL is being written; the agent records event `archive_failing` (once per
    episode, with the last failed WAL file) and `archive_recovered`.
- **S10:**
  - Setup: `max_slot_wal_keep=64MB` and an archive on a shared docker volume.
  - Stop a replica, write 2 GB of WAL, start it again.
  - Pass: 0 `reseed_started` events, `verify_table` matches, catch-up time
    recorded.
  - The measured catch-up rate (MB/s) is used to extrapolate 1 TB of WAL,
    clearly labelled as an extrapolation.
- **(D14) Archive throughput:** S7 (the high-volume soak) and S10 run with the
  archive enabled. They record WAL generation MB/s, archive MB/s and archiver
  lag (segments `.ready` in `pg_wal/archive_status`). Pass: archiver lag stays
  bounded (does not grow for the whole soak). The ceiling is published per
  archive type ("local volume", "NFS", "gcsfuse"), labelled with the host.

### W7 (E3) spec: public scorecard
- **Runner:** GitHub-hosted `ubuntu-latest` (it has Docker, and containers
  get `--cap-add NET_ADMIN`). No self-hosted runner, so a public repository
  exposes no runner of ours.
- **Triggers:** `schedule` (nightly) and `workflow_dispatch` only, never
  `pull_request`. A `concurrency: scorecard` group stops two runs from pushing
  at the same time.
- **Publishing:**
  - The job publishes **every** run, pass or fail, to a `scorecard` branch
    using `GITHUB_TOKEN` with `contents: write`, scoped to that job. It writes
    `results/<date>-<sha>.json` and `latest.json` (a shields.io endpoint
    badge).
  - A final `if: always()` step publishes `status: error` when the job
    crashes before writing its results, so every run is recorded.
  - The README (on master) shows the badge and **links** to `SCORECARD.md`
    on the `scorecard` branch, which the job regenerates with the last 30
    runs. The job never commits to master.
  - The workflow lives on the default branch. GitHub disables scheduled
    workflows in public repos after 60 days without activity; the scorecard
    shows the last run date so a stopped schedule is visible.
- **Labels:** the smoke uses the highest TPS the runner sustains (recorded),
  and the table reads "max RTO of 3 runs at N TPS". Lab-matrix p95 numbers are
  published separately, labelled "lab p95, 20 runs, <host>".

### W8 (E4) spec: S9 rolling upgrade 1.2 → 1.3
- **Sync mode:** S9 runs with `synchronous_mode=available` and one sync
  standby. Before restarting the sync standby, `set_sync` moves sync to
  another, already upgraded replica, so commits never wait on a restarting
  node.
- The **async replica is upgraded first** (in the lab, pg3), so an upgraded
  node exists to take the sync role before the sync standby restarts.
- **Order, per replica (DBA-run steps, documented):**
  1. Stop the agent.
  2. Install the 1.3 agent binary and the 1.3 extension files (`.so`, the
     `--1.2--1.3.sql` script, control file). The 1.3 `.so` keeps every C
     symbol the 1.2 SQL uses.
  3. **Append** `worlber_ha` to the existing `shared_preload_libraries` with
     ALTER SYSTEM, keeping any existing entries (e.g. `pg_stat_statements`).
  4. Restart PostgreSQL, start the agent.
- Then `switchover` to an upgraded replica, and repeat the steps for the old
  primary (now a replica).
- Finally, on the primary: `ALTER EXTENSION worlber_ha UPDATE TO '1.3'`.
- The primary's agent is never restarted in place, so there is no
  `lease_ttl` race. `archive_mode` is not touched by the upgrade (W6
  handles it).
- **Mixed versions:**
  - Agents report their version in heartbeats.
  - **Version gate.** Until **all** members report ≥ 1.3, the leader:
    - rejects every new op (`actual_sync`, `suspend_primary`, `drill_fence`,
      canary, `join_token`, `consume_join_token`) and every new `set_config` key (`fencing`, `wal_archive_dir`,
      `archive_keep`, `archive_max_keep`, `archive_command`,
      `restore_command`, `async_failover`, `ro_use_sync_standby`,
      `ro_max_lag`, `ro_fallback`), so 1.2 and 1.3 FSMs never diverge.
      A heartbeat without a version field, or a member without a fresh
      heartbeat (e.g. right after a leader change), counts as < 1.3;
    - keeps the 1.2 behaviours: `ANY 1`, the 30s `failover_timeout` default,
      the single-probe health check.
  - The gate opens **once**, recorded in the FSM, and only after the DBA has
    run `ALTER EXTENSION worlber_ha UPDATE TO '1.3'` on the primary. The
    agent checks `extversion`; it never runs the update itself.
  - After the gate opens, `add_node` accepts only agents reporting ≥ 1.3.
  - **Ordered transition on gate open** (each step recorded as an event):
    1. Every agent writes its lease, then sets `worlber_ha.fence=on` (the
       bootstrap order).
    2. The primary switches `ANY 1` → `FIRST 1`, and the leader commits the
       first `actual_sync`. Until that commit, failover follows 1.2 rules: it
       is **not** blocked by an empty `actual_sync`.
    3. Only then are `failover_timeout` lowered to 20s and the 3-probe check
       enabled.
  - From 1.3 on, the FSM ignores unknown ops (logs them, no error), so later
    versions can be added the same way.
- **(D15) Pre-gate rollback:** before the gate opens, S9 downgrades one
  upgraded replica back to the 1.2 agent and extension files, and proves the
  cluster stays healthy (streaming, `health_check()` OK, 0 failovers). The
  docs and README mark `ALTER EXTENSION worlber_ha UPDATE TO '1.3'` as the
  **point of no return** (it opens the gate) and say to run `drill()` on a
  staging cluster first.
- **Pass:** 0 automatic failovers, 0 acknowledged rows lost, the ledger is
  write-unavailable only during the switchover (that RTO is recorded), and
  every node ends on 1.3 with fencing on.
- The procedure is documented in TESTING.md and the README.

### W10 spec: install + join wizard
Goal: a new server needs **one shell command** and **one SQL command**, like
"install the instance, then `ALTER AVAILABILITY GROUP ... ADD REPLICA`".

- **Packages:** `.deb` and `.rpm` built with nfpm in the release workflow:
  - agent binary in the Worlber `bin` directory;
  - the systemd unit;
  - `/etc/worlber_ha/` (owned by postgres, 0750).
  The package never starts anything by itself.
- **First node:**
  ```
  sudo worlber-ha-agent init --pgdata /var/lib/pgsql/data
  ```
  It does the following:
  1. Detects `pgbin`, the hostname and the address of the default route
     (all can be overridden with flags).
  2. Generates a 32-byte random cluster secret into `/etc/worlber_ha/token`
     (0600 postgres).
  3. Writes `agent.env`.
  4. **Appends** `worlber_ha` to `shared_preload_libraries` with ALTER
     SYSTEM, keeping the existing entries, and restarts PostgreSQL only if
     the setting changed.
  5. Runs `systemctl enable --now worlber-ha-agent`.
  6. Prints the next step:
     `CREATE EXTENSION worlber_ha; SELECT worlber_ha.create_cluster('prod');`
- **Join token (psql, on any member):**
  ```sql
  SELECT worlber_ha.join_token(ttl => '1 hour', sync => false);
  ```
  - The result is `wjt1.<base64 {cluster, seed api addrs, nonce, expiry, sync}>`.
  - The leader commits `join_token(sha256(nonce), expiry, sync)` to Raft.
    Only the hash is stored, the token is single-use, and expired entries
    are pruned.
  - Event: `join_token_created`. Only `worlber_ha_admin` can call it.
- **New node:**
  ```
  sudo worlber-ha-agent init --pgdata ... --join 'wjt1....'
  ```
  1. Checks first: PGDATA is empty or absent (otherwise refuse unless
     `--reinit-pgdata`), and the seed peer port is reachable.
  2. POSTs `/peer/join` with the nonce. The leader checks the hash and the
     expiry, then commits `consume_join_token`. A second use fails with
     "token already used".
  3. The leader replies with the cluster secret and the node's assigned
     name.
  4. `init` writes the token and `agent.env`, then starts the agent.
  5. The leader then runs the normal `add_node(host, sync, wait => false)`,
     so seeding (pg_basebackup), `seed_max_rate`, slot and pg_hba handling
     are exactly the existing code paths.
  6. Progress shows in `cluster_status`. Event: `node_joined`.
- **Security (B1, encryption left alone):** the secret crosses the network
  in cleartext once. That is the same exposure as today's per-request token
  header on :7400. The docs say so, and `--token-file` (copy the secret by
  hand) remains available. No TLS work is added.
- **(D16) Hardening:**
  - **Retry-safe:** consuming marks the token `claimed_by=<host>`; a retry
    from the same host with the same nonce within the TTL returns the same
    reply, so a crash during `init` never strands a used token. `add_node`
    runs only after the new agent's first heartbeat.
  - **Rate limit:** 5 `/peer/join` attempts per minute per source IP;
    excess gets 429.
  - **Audit:** every rejected attempt records event `join_rejected` (source
    IP, reason: unknown, expired, used, rate-limited).
  - **Token input:** `--join-file <path>` and `--join -` (stdin) so the
    token stays out of shell history and `ps`.
  - TAP tests for retry, rate limit, the event, and file/stdin input.
- **Remove:** `SELECT worlber_ha.remove_node('pg4')` is unchanged. The node
  then shows `left_cluster` and `worlber-ha-agent init --join` can re-add it
  with a new token.
- **Version gate:** `join_token` and `consume_join_token` are new ops covered
  by the W8 gate (rejected until all members report ≥ 1.3).
- **Tests:**
  - TAP: a reused token is rejected, an expired token is rejected, and a
    non-admin role cannot call `join_token()`.
  - **S11 (lab):** a fresh empty container `pg5` joins by token while the
    ledger writes. Pass: it reaches `streaming`, `verify_table` matches, and
    0 acknowledged rows are lost.
  - Package smoke in CI: install the .deb on `ubuntu-latest` and the .rpm in
    a Rocky 9 container, then run `init --help` and `init --dry-run`.

### Lab time budget
- Full 20-run matrix (S1–S6, S2b; S5b counts RPO only) at the highest rung
  the lab sustains; 5 runs at the lower rungs, gated on max.
- 8 scenarios × (20 + 5 + 5) runs × ~3 min ≈ 12h, plus S7 (3 × 30 min × 3
  rungs = 4.5h), S8 (~2h), S9, S10 and S11 (~1.5h).
- **About 20h**, run as a weekend batch. Any rung the lab cannot sustain is
  skipped and recorded.

### Design doc deltas (applied to docs/designs/worlber-ha-production-proof.md)
1. The canary table is named `worlber_ha_internal.canary` everywhere.
2. The agent fencing goroutine is unconditional, and it is the only
   SIGKILL escalation.
   - **Trigger:** it runs independently of reconcile, on its own
     `CLOCK_BOOTTIME` ticker. It fences when its own last lease is older than
     `lease_ttl`, which is exactly when renewal has stopped (quorum lost,
     suspended, or role lost).
   - When it fences, it sends SIGINT to the postmaster. If PostgreSQL is
     still up **5s** later, it lists the postmaster's child PIDs first, sends
     SIGKILL to the children, then to the postmaster (PID from
     `postmaster.pid`). Children are listed before the postmaster dies,
     because they get re-parented after it.
   - Scenario **S2b**: the harness sends `SIGSTOP` to one backend. A stopped
     process ignores the fast-shutdown SIGTERM, so shutdown hangs, but
     SIGKILL still removes it. The Go build tag `chaosdebug`, absent from
     release builds, is used only to block reconcile.
   - S2b pass: every postgres process of the old primary is gone within
     lease expiry + 5s + 1s.
3. S5b is gated on RPO only (not RTO). The 20-run RTO p95 gate applies to
   **S1–S4, S6 and S2b** (S5 expects no promotion and has no RTO).
4. Health timer and fencing when PostgreSQL dies but its agent lives (S1).
   This closes a split-brain window.
   - The leader's failover timer starts at the last heartbeat reporting
     `Running=true`.
   - When the primary's agent reports `Running=false` (after 3 failed
     probes), the leader commits **`suspend_primary(epoch)`** to Raft.
   - The suspended agent stops renewing the lease and does **not** restart
     PostgreSQL as primary. On the primary, the agent also sets
     `restart_after_crash=off`.
   - **Leader-confirmed:** every heartbeat ack and `/peer/info` reply carries
     the leader's `suspended` flag. Renewal requires "not suspended" from
     **both** the local FSM and the leader, so an agent whose Raft log is
     behind cannot keep renewing.
   - **Resume:**
     1. The leader commits `resume_primary(epoch)` when PostgreSQL is back,
        the epoch is unchanged, and no `set_primary` was committed.
     2. The agent then does a quorum check and writes a fresh lease under
        the normal rules.
     3. Only then does it start PostgreSQL as primary. This mirrors the
        drill-abort rule.
   - **Fence wait.** Promotion requires that `Q + lease_ttl + 0.5s + 2s` has
     passed since the `suspend_primary` commit, or since the last heartbeat
     when the agent is silent. That is about 3s + 11.5s, inside 20s, so RTO
     is unchanged.
   - `fence_confirmed` in the failover receipt records that this wait
     completed.
   - S1 adds a variant where PostgreSQL restarts at about 19s. Pass: 0
     writes accepted after the fence-wait deadline, and 0 acknowledged rows
     lost or divergent. A pre-suspend lease can stay valid until
     Q + lease_ttl, which is safe because promotion waits for that.
5. S7 adds the listener-vs-direct latency (p50/p99) measurement (P7).
6. The E1/E3/E4 success criteria (S9, S10, the scorecard) are in the design
   doc, with a pointer to the working plan W6–W8 for the full spec.
7. Under the status line, a note that R3-1 to R3-4 were fixed after the cap
   and verified mechanically.
8. Distribution Plan CI bullet and Next Steps rewritten to match W6–W9: a
   GitHub-hosted runner, the highest sustained TPS, publishing to the
   `scorecard` branch.
9. Drop the drill-timing Open Question (scheduled drills are deferred).

## Step 0I: Temporal interrogation
- **Hour 1 in production:** the DBA installs the packages, runs `init`, `create_cluster`, `join_token()` and `init --join`; `health_check()` must be all OK. Risk: `shared_preload_libraries` restart surprises → `init` says when it restarts (W10).
- **Day 1:** first real failover or switchover. Risk: the DBA cannot tell whether data was lost → failover receipts + `data_loss_if_failover_now` (W3/W5).
- **Week 1:** a replica down for maintenance for a weekend. Risk: slot limit hit, full re-seed of 1 TB → archive catch-up (W6), visible archiver health (D13).
- **Month 6:** first agent upgrade. Risk: mixed versions diverge → version gate + pre-gate rollback (W8, D15).

## Review Sections (CEO review, 2026-10-13)

### Current scope
- Mode L0 SELECTIVE EXPANSION (D2). Accepted: approach B (L1), E1 revised as WAL archive (D11), E3 (D9), E4 (D10), W10 install/join (D12), D13–D16. Deferred: E2 Prometheus (D8), scheduled drills, standby-side confirmation, softdog → contrib/worlber_ha/TODOS.md. Rejected: incremental re-seed (infeasible, D11). Bound: B1 no encryption work, B2 psql-level management.

### Section 1: Architecture
```
            apps ──► VIP / GCP ILB (/health/primary) ──► :5000 RW / :5001 RO listener (in every agent)
                                                              │
   psql (any node) ─ worlber_ha.* SQL ─► extension (C) ─unix socket─► agent (Go)
                                          │  fence bgworker ◄─ lease file ◄─┤ fencing goroutine (SIGKILL path)
                                          ▼                                 │
                                     PostgreSQL ◄── pg_ctl / pg_rewind / pg_basebackup / restore_command
                                                                            │
                       agents ◄── Raft :7401 (FSM: epoch, primary, nodes, options, actual_sync,
                                  suspended, join tokens, version gate) ──► peers :7400 (token)
                       new node: init --join ─► /peer/join (nonce, rate-limited) ─► leader
                       archive dir (NFS/gcsfuse) ◄── archive_command (all) / pg_archivecleanup (leader)
```
- **OK** Component boundaries: SQL is only a front end; all decisions are in the agent FSM; fencing is in two independent places (bgworker + agent goroutine).
- **WARNING** SPOF: apps pointed at one node's :5000 lose access when that node dies. Fix (factual, docs): README must say "point apps at the VIP / ILB / multi-host string, never one node's listener".
- **WARNING** New coupling: extension ↔ agent through the lease file format. Justified (fencing must work with the agent dead); version the lease file format (`v1`) — directly required by W8 mixed versions.
- **OK** Scaling: 10x connections → 2 goroutines per proxied connection, fine; 100x (10k+ conns) → listener fd limits; documented `LimitNOFILE` in the unit (task T7).
- Rollback posture: before the gate, reinstall 1.2 (now tested, D15); after the gate, roll forward only (documented, D15).
- Cherry-pick fit: W6 archive changes the replica path in reconcile.go (Section 5 refactor); W10 adds the only unauthenticated endpoint (Section 3).

### Section 2: Error & Rescue Map (capability level)
```
CAPABILITY                 | WHAT CAN GO WRONG                     | RESCUE / USER SEES
---------------------------|---------------------------------------|------------------------------------------
lease renewal (W2)         | disk full / fsync error               | PG fences itself → failover; event lease_write_failed + health CRIT
fence goroutine (W2)       | postmaster.pid unreadable             | pg_ctl stop -m immediate; event fence_fallback, log CRIT
suspend/resume (W2)        | leader lost mid-suspend               | new leader reads suspended from FSM; fence wait applies
promotion (W3)             | candidate LSN 0 / unknown             | not eligible (fixes found bug); event candidate_rejected
archive_command (W6)       | target unreachable / slow             | PG retries; D13 CRIT + archive_failing  ← was silent GAP, fixed
archive catch-up (W6)      | segment missing                       | re-seed; event reseed_started with reason
archive retention (W6)     | member older than archive_max_keep    | marked for re-seed; event archive_retention_exceeded
join (W10)                 | crash after consume                   | retry-safe within TTL (D16)  ← was GAP, fixed
join (W10)                 | bad / expired / used token            | 4xx with reason; event join_rejected (D16)
init (W10)                 | PGDATA not empty                      | refuse, tell user --reinit-pgdata
gate open (W8)             | member silent at gate time            | counts as <1.3, gate stays closed; health_check WARN gate_pending
drill (W5)                 | candidate fails to promote            | drill_abort; quorum-checked restart of old primary
```
GAPS found: 2 (archiver silent, join stranded) — both fixed by D13/D16. Remaining: 0.

### Section 3: Security & threat model
| Threat | Likelihood | Impact | Mitigated? |
|---|---|---|---|
| Brute force on `/peer/join` nonce | Low (256-bit) | High | Yes: single-use, TTL, 5/min/IP, join_rejected events (D16) |
| Join token in shell history / ps | Med | High (until used) | Yes: --join-file / stdin (D16), single use |
| Cleartext secret on the wire once | Med on shared LANs | High | **Accepted under B1** (same exposure as the existing token header); documented |
| Command injection via `wal_archive_dir` in archive_command | Low (admin only) | High | Must validate: absolute path, `[A-Za-z0-9/._-]` only, single-quoted (task T5) |
| Non-admin calls drill()/join_token()/set_config | Low | High | worlber_ha_admin only (design doc + TAP, D16) |
| Audit trail of HA ops | — | — | worlber_ha.events records every op with time and node |
No dependency additions except nfpm (build-time only).

### Section 4: Data flow & edge cases
- Join: `join_token()` → FSM(hash, expiry) → `/peer/join` → consume(claimed_by) → secret → init writes env → agent heartbeat → add_node → seed. Shadow paths: empty/garbled token → 400 + event; expired → 410; used by another host → 409; leader change between steps → state is in the FSM, new leader continues; two hosts racing one token → Raft serialises, loser gets 409.
- Async ordering checked: suspend_primary vs set_primary (epoch-tagged; resume refused if epoch changed); actual_sync commit vs PickCandidate (candidate read from committed FSM only); gate open vs add_node of a 1.2 agent (add_node checks gate in the same FSM apply). Regression tests: TAP with a controlled pause in the leader (chaosdebug tag) for the join race and the gate/add_node race (task T6).
- Interaction: double `add_node` → "already a member" (ops.go:374); `remove_node` during seeding → seeding cancelled, slot dropped (verify in S11, task T6).

### Section 5: Code quality
- **WARNING** reconcile.go replica path (clone/rewind/follow/walGone/reseed + archive catch-up) will exceed 5 branches. Refactor into an explicit replica state machine (`seeding → catching_up_archive → streaming → rewinding → reseeding`) as the first step of W6 (task T4).
- Version gate checks must be in one function used by every op and set_config (not scattered) (task T3).
- No over-engineering found; softdog and Prometheus correctly deferred.

### Section 6: Test review
```
NEW THING              | TYPE        | TEST                                   | FAILURE PATH
lease + fence          | system      | S2, S2b, S3, S4                        | agent dead, SIGSTOP backend
suspend/fence wait     | system      | S1 + 19s variant                       | PG restarts during wait
promotion safety       | unit+system | PickCandidate unit, S1, S5, S5b        | LSN 0, stale actual_sync
lag-aware RO           | system      | S8                                     | replica past ro_max_lag
proof/drill            | system+TAP  | drill passes, abort path               | candidate fails
archive catch-up       | system      | S10 (+ throughput D14)                 | missing segment → re-seed
archive health         | TAP         | failing archive_command → CRIT (D13)   | archiver stuck
version gate           | TAP+system  | S9 + pre-gate rollback (D15)           | silent member keeps gate shut
join                   | TAP+system  | S11, retry/rate/expiry/used (D16)      | crash mid-join
packages               | CI smoke    | deb on Ubuntu, rpm on Rocky 9          | init --dry-run
```
- 2am-Friday test: S1 + S2 under S7 load, 20 runs, 0 acked rows lost. Hostile QA: SIGSTOP the postmaster itself (covered by S2b SIGKILL path). Chaos: S1–S11.
- Flakiness: timing-based scenarios use the 20-run matrix and `wait_until`, never fixed sleeps.

### Section 7: Performance
- Canary write every few seconds: negligible WAL.
- Lease fsync every Q=2s: negligible.
- `verify_table` on 1 TB tables is a full scan on every node: documented as off-peak; no change.
- **Archive on gcsfuse/NFS** may not keep up at high WAL rates → D14 measures and publishes the ceiling.
- Listener latency measured in S7 (P7).

### Section 8: Observability
- Accepted (D13): archiver CRIT + events, `cluster_status.fence_state` and `lease_age`, README runbooks for: node fenced, archive failing/full, re-seed loop, join token expired/rejected, gate pending.
- Every new op records an event with node, epoch and reason; agent_log() exposes structured logs from psql.
- Debuggability 3 weeks later: failover receipts + events (500 kept) — keep events at 500, receipts persist in the proof table.
- Metrics/dashboards: Prometheus deferred (E2, TODOS.md).

### Section 9: Deployment & rollout
- Migration: `worlber_ha--1.2--1.3.sql` adds functions/views/table (canary, proof); no rewrite of user tables.
- Feature flags: `fencing`, `wal_archive_dir`, `async_failover`, `ro_*` are set_config keys; fencing can be turned off per cluster.
- Order and mixed-version window: W8 spec (replicas → switchover → old primary → ALTER EXTENSION → gate).
- Rollback: pre-gate tested (D15); post-gate roll forward only.
- Post-deploy checklist: `health_check()` all OK, `cluster_status` all streaming, `fence_state=on`, then `drill('switchover')` on staging.

### Section 10: Long-term trajectory
- Reversibility: **3/5** (gate opening and the lease format are one-way; everything else is a set_config switch).
- Debt: C + Go split (lease format shared), docs for 10+ new options, lab time (~20h per full matrix).
- Next phases: Prometheus (E2), scheduled drills, multi-site / async DR replica, then the TDE/masking/audit work of the Worlber distribution.
- Retrospective: E1 revision was right (incremental re-seed was infeasible); W10 was load-bearing for the "easy like Always On" goal and correctly pulled in.

### Section 11: Design & UX
SKIPPED (no UI scope).

## Outside voice
Codex CLI not installed; native fallback unavailable (TaskOutput not available in this session). **Outside voice unavailable — no completed external review.** (A same-harness second opinion ran earlier in Step 0 and its findings are in W2/W3.)

## Approval readiness
Checked rows L0 (D2), L1 (plan approval), E1 (D11), E2 (D8 defer), E3 (D9), E4 (D10), W10 (D12), D13–D16 (answers above). Factual/doc fixes (SPOF wording, lease format version, archive path validation, LimitNOFILE, one gate function) are directly required by accepted items. **Approval readiness: PASS.**

## NOT in scope
- E2 Prometheus /metrics — deferred (D8), TODOS.md.
- Scheduled drills — deferred (design doc), TODOS.md.
- Standby-side confirmation (R3-8) — deferred, TODOS.md.
- softdog — deferred, TODOS.md.
- TLS / mTLS / encryption — excluded by B1.
- Incremental re-seed with pg_combinebackup — rejected (infeasible, D11).

## What already exists
lab.sh helpers and docker lab (reused for the harness), acked-writer pattern (scaled up for the ledger), verify_table (divergence check), events timestamps (RTO), add_node seeding / pg_hba / slots (reused by join), set_config validation (extended with new keys).

## Dream state delta
After this plan: measured RTO/RPO under load, chaos suite, published scorecard, packages + one-command join, rolling upgrades. Still missing for the 12-month ideal: Prometheus, scheduled drills, fast 1 TB re-seed when the archive is gone.

## Failure Modes Registry
```
CODEPATH            | FAILURE MODE                  | RESCUED? | TEST?      | USER SEES?            | LOGGED?
dead agent primary  | unfenced primary              | Y (W2)   | S2         | event + failover      | Y
LSN 0 candidate     | lag check skipped             | Y (W3)   | S5         | candidate_rejected    | Y
actual sync unknown | wrong replica promoted        | Y (W3)   | S1/S5b     | receipt               | Y
archive failing     | pg_wal fills                  | Y (D13)  | TAP        | health CRIT           | Y
archive too slow    | archiver lag grows            | Y (D14)  | S7/S10     | health CRIT           | Y
join crash          | token stranded                | Y (D16)  | TAP        | retry works           | Y
gate + silent node  | FSM divergence                | Y (W8)   | S9         | gate_pending WARN     | Y
1.3 broken pre-gate | stuck upgrade                 | Y (D15)  | S9         | rollback doc          | Y
lease disk full     | primary fences itself         | Y (W2)   | S3 variant | event + failover      | Y
```
CRITICAL GAPS: 0.

## Stale diagram audit
README "how data replicates" diagram: still accurate for 1.2; must gain the archive path and listener/VIP note when W6/W10 land (task T8).

## Implementation Tasks
- [ ] **T1 (P1, human ~3d / CC ~4h)** — fencing — lease file `v1` format + fence bgworker + agent goroutine; events lease_write_failed, fence_fallback. Surfaced by: S1/S2. Files: contrib/worlber_ha/fence.c, agent/internal/agent/{reconcile,agent}.go. Verify: S2, S2b, S3, S4 pass.
- [ ] **T2 (P1, human ~2d / CC ~3h)** — promotion — reject LSN 0 candidates, FIRST 1 + actual_sync. Files: failover.go, reconcile.go, fsm.go. Verify: PickCandidate unit tests, S1, S5, S5b.
- [ ] **T3 (P1, human ~1d / CC ~1h)** — version gate — one gate function used by every op and set_config key. Files: fsm.go, ops.go. Verify: S9 + TAP.
- [ ] **T4 (P2, human ~1d / CC ~2h)** — reconcile — refactor replica path into an explicit state machine before adding archive catch-up. Files: reconcile.go. Verify: TAP 001 + lab test unchanged.
- [ ] **T5 (P1, human ~2h / CC ~15min)** — security — validate `wal_archive_dir` (absolute, safe chars, quoted). Files: fsm.go ValidateOption. Verify: regression test with `; rm -rf` value rejected.
- [ ] **T6 (P1, human ~1d / CC ~2h)** — join — retry-safe consume, rate limit, join_rejected, --join-file/stdin, race tests. Files: api.go, fsm.go, cmd/worlber-ha-agent/init.go, t/002_join.pl. Verify: TAP + S11.
- [ ] **T7 (P2, human ~1h / CC ~10min)** — packaging — `LimitNOFILE=65536` in the unit; nfpm config. Files: packaging/. Verify: CI package smoke.
- [ ] **T8 (P2, human ~4h / CC ~30min)** — docs — runbooks (D13), point-of-no-return (D15), "never point apps at one node", updated diagram. Files: README.md, testing/TESTING.md. Verify: doc review.
- [ ] **T9 (P1, human ~1d / CC ~1h)** — archive health — pg_stat_archiver CRIT, archive_failing events, fence_state/lease_age columns. Files: worlber_ha--1.2--1.3.sql, ops.go. Verify: TAP with failing archive_command.
- [ ] **T10 (P2, human ~4h / CC ~30min)** — chaos — throughput measurement in S7/S10 (D14), pre-gate rollback in S9 (D15). Files: testing/chaos/scenarios/. Verify: scenario outputs in the scorecard.

## Completion Summary
```
+====================================================================+
|            MEGA PLAN REVIEW — COMPLETION SUMMARY                   |
+====================================================================+
| Mode selected        | SELECTIVE EXPANSION                         |
| System Audit         | 3 real safety bugs; 0 load tests; P5-P7 unverified |
| Step 0               | approach B; E1(rev) E3 E4 W10 added; E2 deferred |
| Section 1  (Arch)    | 3 issues found                              |
| Section 2  (Errors)  | 12 error paths mapped, 2 GAPS (both fixed)  |
| Section 3  (Security)| 5 issues found, 3 High severity (mitigated) |
| Section 4  (Data/UX) | 7 edge cases mapped, 0 unhandled            |
| Section 5  (Quality) | 2 issues found                              |
| Section 6  (Tests)   | Diagram produced, 0 gaps                    |
| Section 7  (Perf)    | 1 issue found (D14)                         |
| Section 8  (Observ)  | 1 gap found (D13, fixed)                    |
| Section 9  (Deploy)  | 1 risk flagged (D15, fixed)                 |
| Section 10 (Future)  | Reversibility: 3/5, debt items: 3           |
| Section 11 (Design)  | SKIPPED (no UI scope)                       |
+--------------------------------------------------------------------+
| NOT in scope         | written (6 items)                           |
| What already exists  | written                                     |
| Dream state delta    | written                                     |
| Error/rescue registry| 12 rows, 0 CRITICAL GAPS                    |
| Failure modes        | 9 total, 0 CRITICAL GAPS                    |
| TODOS.md updates     | 4 items (E2, drills, R3-8, softdog)         |
| Scope proposals      | 5 proposed, 4 accepted (EXP + SEL)          |
| CEO plan             | written                                     |
| Outside voice        | codex: unavailable (not installed)          |
| Lake Score           | 4/4 (D13-D16); Step 0 picks not rescored    |
| Diagrams produced    | 3 (architecture, error map, test map)       |
| Stale diagrams found | 1 (README replication diagram)              |
| Unresolved decisions | 0                                           |
| Reality rows         | 7/7 rows (PLAN_REALITY), 3 unverified       |
+====================================================================+
```

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 1 | CLEAR | 5 proposals, 4 accepted, 1 deferred |
| Outside Review | codex (auto) | Independent 2nd opinion | 0 | unavailable | codex not installed; no completed external review |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 0 | — | — |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | — |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | — | — |

- **OUTSIDE COVERAGE:** codex, plan-review, unavailable (CLI not installed; native fallback unavailable). No completed external review.
- **VERDICT:** CEO CLEARED — eng review required.

NO UNRESOLVED DECISIONS
