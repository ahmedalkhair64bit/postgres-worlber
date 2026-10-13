# TODOS

## worlber_ha

### Prometheus `/metrics` endpoint and alert rules (E2)

**What:** `GET /metrics` on the agent peer port (:7400) with role, lag, redo queue, failovers, fence state, archiver lag and the last drill RTO, plus example alert rules and a Grafana dashboard.

**Why:** Teams that already run Prometheus want alerts without polling SQL views.

**Context:** Deferred in the CEO review (D8). Today the same data is in `worlber_ha.health_check()`, `cluster_status`, `replication` and `proof`. Start from `buildStatus` / `replicationStatus` in `agent/internal/agent/ops.go`. Pick the signals after the chaos harness (W1) shows which ones matter.

**Effort:** S (human: M)
**Priority:** P2
**Depends on:** W1 chaos harness, W5 proof view

### Scheduled production drills

**What:** Run `worlber_ha.drill()` on a schedule (e.g. weekly in a maintenance window) and keep the results in `worlber_ha.proof`.

**Why:** Keeps the proof view fresh without a DBA starting drills by hand.

**Context:** Deferred in the design doc (drill-timing question dropped). `drill()` is manual in 1.3. A schedule needs a maintenance-window option and an opt-in, because a drill is a real failover.

**Effort:** S (human: M)
**Priority:** P3
**Depends on:** W5 drill()

### Standby-side confirmation of primary failure

**What:** Before an automatic failover, the leader asks the standbys whether they also lost the primary (WAL receiver state), not only the primary's agent heartbeat.

**Why:** Reduces false failovers when only the agent-to-leader path is broken.

**Context:** Design review finding R3-8. The 3-probe health check plus fencing (W2) covers the safety side; this is an extra guard against unnecessary failovers.

**Effort:** M (human: L)
**Priority:** P2
**Depends on:** W2 fencing

### softdog as optional fencing add-on

**What:** Optional Linux softdog/watchdog integration so a hung node reboots itself when the agent stops feeding the watchdog.

**Why:** Covers a kernel-level hang where neither the agent nor the fence worker can run.

**Context:** Deferred from the fencing design. The lease + fence bgworker + agent SIGKILL path (W2) is the primary mechanism; softdog needs root and is off by default.

**Effort:** S (human: M)
**Priority:** P3
**Depends on:** W2 fencing

## Completed
