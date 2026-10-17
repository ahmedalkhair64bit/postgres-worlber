package agent

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
)

// Candidate is a standby considered for promotion.
type Candidate struct {
	Name         string
	Sync         bool // configured as synchronous (the sync flag)
	InActualSync bool // the primary last reported it as actually synchronous
	Fresh        bool
	Running      bool
	InRecovery   bool
	LSN          uint64
}

// PickOptions controls PickCandidate.
type PickOptions struct {
	PrimaryLSN uint64
	MaxLag     uint64 // 0 disables the lag check
	Strict     bool   // only configured-sync standbys are eligible
	// Automatic failover must not lose acknowledged data: it promotes a
	// standby only from the actual synchronous set, unless HasActualSync is
	// false (no sync standby was tracked) or Async is "within_lag".
	Automatic     bool
	HasActualSync bool
	Async         string // "never" (default) or "within_lag"
}

// PickCandidate chooses the standby to promote and, on failure, explains why.
func PickCandidate(cands []Candidate, opts PickOptions) (string, string) {
	var ok []Candidate
	reasons := ""
	for _, c := range cands {
		switch {
		case !c.Fresh:
			reasons += c.Name + ": unreachable; "
		case !c.Running:
			reasons += c.Name + ": not running; "
		case !c.InRecovery:
			reasons += c.Name + ": not a standby; "
		case opts.Strict && !c.Sync:
			reasons += c.Name + ": not synchronous (strict mode); "
		case opts.MaxLag > 0 && opts.PrimaryLSN > c.LSN && opts.PrimaryLSN-c.LSN > opts.MaxLag:
			reasons += fmt.Sprintf("%s: lag %d bytes exceeds maximum_lag_on_failover; ", c.Name, opts.PrimaryLSN-c.LSN)
		default:
			ok = append(ok, c)
		}
	}
	// Automatic failover prefers the actual synchronous standbys, which are
	// guaranteed to hold every acknowledged commit.
	if opts.Automatic {
		var sync []Candidate
		for _, c := range ok {
			if c.InActualSync {
				sync = append(sync, c)
			}
		}
		if len(sync) > 0 {
			ok = sync
		} else if opts.HasActualSync && opts.Async != "within_lag" {
			return "", "the synchronous standby is not available; " + eligibleNote(ok) +
				"run SELECT worlber_ha.failover(force => true) to promote another node and accept data loss (async_failover is 'never')"
		} else if !opts.HasActualSync && opts.Async != "within_lag" {
			return "", "no synchronous standby was tracked, so promoting any node may lose data; " +
				"run SELECT worlber_ha.failover(force => true), or set async_failover = 'within_lag'"
		}
		// within_lag: fall through with the lag-filtered set
	}
	if len(ok) == 0 {
		if reasons == "" {
			reasons = "no standbys"
		}
		return "", reasons
	}
	sort.Slice(ok, func(i, j int) bool {
		if ok[i].InActualSync != ok[j].InActualSync {
			return ok[i].InActualSync
		}
		if ok[i].Sync != ok[j].Sync {
			return ok[i].Sync
		}
		if ok[i].LSN != ok[j].LSN {
			return ok[i].LSN > ok[j].LSN
		}
		return ok[i].Name < ok[j].Name
	})
	return ok[0].Name, ""
}

func eligibleNote(ok []Candidate) string {
	if len(ok) == 0 {
		return "no other standby is reachable either; "
	}
	return ""
}

// leaderLoop watches the primary and fails over when it stays unhealthy
// longer than failover_timeout.  It only acts on the Raft leader.
func (a *Agent) leaderLoop(ctx context.Context) {
	t := time.NewTicker(a.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		leader := a.isLeader()
		a.mu.Lock()
		if leader && !a.wasLeader {
			a.leaderSince = time.Now()
			a.primaryBad = time.Time{}
			a.beats = map[string]heartbeat{}
		}
		a.wasLeader = leader
		term := ""
		if leader {
			term = a.raft.Stats()["term"]
		}
		// a new term (even if we were leader before) needs a new barrier
		ready := a.leaderReady && a.readyTerm == term
		if !ready {
			a.leaderReady = false
		}
		a.mu.Unlock()
		if !leader {
			continue
		}
		if !ready {
			if err := a.raft.Barrier(5 * time.Second).Error(); err != nil {
				continue
			}
			a.mu.Lock()
			a.leaderReady = true
			a.readyTerm = term
			a.mu.Unlock()
		}
		a.maybeOpenGate()
		a.reconcileActualSync()
		a.reconcileFenceState()
		a.checkPrimary()
	}
}

// maybeOpenGate opens the fencing gate once every member reports a fresh
// heartbeat from an agent that understands fencing (version >= 2).  Until
// then the cluster keeps the older behaviour.  It runs on the leader only.
func (a *Agent) maybeOpenGate() {
	st := a.fsm.Get()
	if !st.Bootstrapped() || st.FencingGate {
		return
	}
	// The DBA opens the gate with ALTER EXTENSION worlber_ha UPDATE on the
	// primary; until then every node can still be rolled back to the old
	// agent (the point of no return is that ALTER EXTENSION).
	if hb, fresh := a.beat(st.Primary); !fresh || !extReady(hb.ExtVersion) {
		return
	}
	members := make([]string, 0, len(st.Nodes))
	for name := range st.Nodes {
		hb, fresh := a.beat(name)
		if !fresh || hb.Version < AgentVersion {
			return // someone is old or silent: keep the gate closed
		}
		members = append(members, name)
	}
	sort.Strings(members)
	if err := a.apply(fsm.Command{Op: "open_gate", GateMembers: members}); err != nil {
		a.log.Warn("cannot open the fencing gate", "error", err)
	} else {
		a.log.Info("fencing gate opened: all members run the fencing agent")
	}
}

// reconcileFenceState commits each data node's fence worker state (loaded or
// not) as its agent reports it, so the decision in checkPrimary survives a
// leader change and the loss of the primary's agent.  Leader only.
func (a *Agent) reconcileFenceState() {
	st := a.fsm.Get()
	if !st.Bootstrapped() || !st.FencingGate {
		return // fence_state is a new op; do not commit it to a mixed cluster
	}
	for name, n := range st.Nodes {
		if n.Witness {
			continue
		}
		hb, fresh := a.beat(name)
		if !fresh {
			continue
		}
		var loaded bool
		switch hb.Fence {
		case "armed", "expired":
			loaded = true
		case "inactive":
			loaded = false
		default:
			continue // not checked yet (unknown, or the gate is not open on that node yet)
		}
		if loaded == n.FenceLoaded {
			continue
		}
		if err := a.apply(fsm.Command{Op: "fence_state", Name: name, FenceLoaded: loaded, Time: time.Now().Unix()}); err != nil {
			a.log.Warn("cannot commit fence_state", "node", name, "error", err)
		}
	}
}

// fenceBlocksFailover reports why an automatic failover must not run: the
// primary's agent is unreachable and its fence worker was never confirmed
// loaded, so nothing would stop the old PostgreSQL from accepting writes
// after a standby is promoted.  An agent that is reachable keeps its own
// PostgreSQL down, so a primary whose server crashed is not blocked.
func fenceBlocksFailover(st fsm.State, primaryAgentFresh bool) string {
	if !st.FencingGate || primaryAgentFresh {
		return ""
	}
	if n := st.Nodes[st.Primary]; n != nil && !n.FenceLoaded {
		return fmt.Sprintf("primary %s is unreachable and its fence worker was never confirmed loaded "+
			"(worlber_ha not in shared_preload_libraries, or PostgreSQL not restarted since): "+
			"automatic failover refused, it could leave two writable primaries.  "+
			"If %s is really down, run SELECT worlber_ha.failover()", st.Primary, st.Primary)
	}
	return ""
}

// reconcileActualSync commits the set of standbys the running primary reports
// as synchronous, so a later leader knows which nodes hold every
// acknowledged commit.  It runs on the leader only.
func (a *Agent) reconcileActualSync() {
	st := a.fsm.Get()
	if !st.Bootstrapped() || st.Paused() {
		return
	}
	if !st.FencingGate {
		return // actual_sync is a new op; do not commit it to a mixed cluster
	}
	seen, known := a.lastSync(st.Primary, st.Epoch)
	if !known {
		return // no running-primary report this epoch yet; keep the committed set
	}
	cur := map[string]bool{}
	for _, n := range st.ActualSync {
		cur[n] = true
	}
	same := len(cur) == len(seen)
	for n := range seen {
		if !cur[n] {
			same = false
		}
	}
	if same {
		return
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	if err := a.apply(fsm.Command{Op: "actual_sync", SyncNodes: names,
		Detail: fmt.Sprintf("synchronous standbys: %v", names)}); err != nil {
		a.log.Warn("cannot commit actual_sync", "error", err)
	}
}

func (a *Agent) checkPrimary() {
	st := a.fsm.Get()
	if !st.Bootstrapped() || st.Paused() {
		a.mu.Lock()
		a.primaryBad = time.Time{}
		a.lease.failingOver = false
		a.mu.Unlock()
		return
	}
	hb, fresh := a.beat(st.Primary)
	healthy := fresh && hb.Health.Running
	a.mu.Lock()
	if healthy {
		a.primaryBad = time.Time{}
		// a failover that was waiting for the fence is no longer wanted:
		// confirm this primary again
		a.lease.failingOver = false
		a.mu.Unlock()
		return
	}
	if a.primaryBad.IsZero() {
		a.primaryBad = time.Now()
		if hb.At.IsZero() || hb.At.Before(a.leaderSince) {
			// no heartbeat since we became leader: count from then
			a.primaryBad = a.leaderSince
		}
		a.log.Warn("primary unhealthy", "primary", st.Primary, "fresh", fresh, "running", hb.Health.Running)
		if !hb.At.IsZero() {
			why := "agent unreachable"
			if fresh {
				why = "PostgreSQL not running"
			}
			a.report(context.Background(), "primary_unhealthy", fmt.Sprintf("%s: %s; failover in %s unless it recovers",
				st.Primary, why, st.Duration("failover_timeout")))
		}
	}
	badFor := time.Since(a.primaryBad)
	a.mu.Unlock()

	if badFor < st.Duration("failover_timeout") {
		return
	}
	if !a.opMu.TryLock() {
		return // a switchover or manual operation is in progress
	}
	defer a.opMu.Unlock()
	st = a.fsm.Get()

	maxLag, _ := fsm.ParseBytes(st.Option("maximum_lag_on_failover"))
	name, why := PickCandidate(a.candidates(st), a.pickOptions(st, hb.Health.LSN, true))
	_ = maxLag
	if block := fenceBlocksFailover(st, fresh); block != "" {
		name, why = "", block
	}
	if name == "" {
		a.mu.Lock()
		if time.Since(a.lastNoCand) > 60*time.Second {
			a.lastNoCand = time.Now()
			a.log.Error("primary down but no failover candidate", "primary", st.Primary, "reason", why)
			a.report(context.Background(), "failover_blocked", why)
		}
		a.mu.Unlock()
		return
	}
	// Fence wait: no more confirmations for the old primary from now on,
	// and no promotion until the last one it got has run out.
	if ok, left := a.beginFailover(st); !ok {
		a.log.Info("failover: waiting for the old primary's lease to expire", "primary", st.Primary, "left", left.Round(100*time.Millisecond))
		return
	}
	loss := a.lossEstimate(st, name, hb)
	a.log.Warn("automatic failover", "old_primary", st.Primary, "new_primary", name, "down_for", badFor.Round(time.Second), "data_loss", loss)
	if err := a.apply(fsm.Command{Op: "set_primary", Name: name, ExpectedEpoch: st.Epoch, Reason: "automatic_failover",
		Detail: fmt.Sprintf("primary down for %s; %s; old primary fenced (lease expired)", badFor.Round(time.Second), loss)}); err != nil {
		a.log.Error("failover commit failed", "error", err)
		a.abortFailover()
		return
	}
	a.mu.Lock()
	a.primaryBad = time.Time{}
	a.mu.Unlock()
}

// lossEstimate describes how much committed data a promotion of cand may
// lose, from the last state the leader saw.
func (a *Agent) lossEstimate(st fsm.State, cand string, primary heartbeat) string {
	chb, _ := a.beat(cand)
	sync := ""
	for _, r := range primary.Health.Replicas {
		if r.AppName == cand {
			sync = r.SyncState
		}
	}
	if seen, ok := a.lastSync(st.Primary, st.Epoch); ok && seen[cand] {
		sync = "sync" // reported by the primary while it was still running
	}
	if sync == "sync" || sync == "quorum" {
		return "estimated data loss: none (" + cand + " was a synchronous standby)"
	}
	if primary.Health.LSN == 0 {
		return "estimated data loss: unknown (no recent primary status)"
	}
	var lag uint64
	if primary.Health.LSN > chb.Health.LSN {
		lag = primary.Health.LSN - chb.Health.LSN
	}
	ago := "unknown"
	if !primary.At.IsZero() {
		ago = time.Since(primary.At).Round(time.Second).String()
	}
	return fmt.Sprintf("estimated data loss: up to %d bytes of WAL (asynchronous standby; primary last seen %s ago)", lag, ago)
}

// GateExtVersion is the extension version whose installation opens the
// fencing gate on an upgraded cluster.
const GateExtVersion = "1.2"

// extReady reports whether the primary's extension is at GateExtVersion or
// later.  "absent" (the extension lives in another database) does not block,
// so such a cluster opens the gate on agent versions alone.
func extReady(v string) bool {
	switch v {
	case "":
		return false // not reported (old agent, or no primary report yet)
	case "absent":
		return true
	}
	return versionAtLeast(v, GateExtVersion)
}

// versionAtLeast compares dotted numeric versions ("1.10" > "1.2").
func versionAtLeast(v, min string) bool {
	a, b := strings.Split(v, "."), strings.Split(min, ".")
	for i := 0; i < len(a) || i < len(b); i++ {
		var x, y int
		if i < len(a) {
			x, _ = strconv.Atoi(a[i])
		}
		if i < len(b) {
			y, _ = strconv.Atoi(b[i])
		}
		if x != y {
			return x > y
		}
	}
	return true
}

// extVersion returns the installed worlber_ha extension version in the
// agent's database, "absent" if it is not installed there, or "" on error.
func (a *Agent) extVersion(ctx context.Context) string {
	var v string
	err := a.pg.QueryRow(ctx, "SELECT coalesce((SELECT extversion FROM pg_extension WHERE extname = 'worlber_ha'), 'absent')", nil, &v)
	if err != nil {
		return ""
	}
	return v
}
