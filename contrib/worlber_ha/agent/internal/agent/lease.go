package agent

// Fencing with a lease (see internal/safety for the rules):
//
//   leader:  verifyLoop keeps proof that it is still the Raft leader; the
//            reply to every heartbeat says whether the sender is confirmed
//            as the primary (confirmFor).
//   primary: heartbeatLoop remembers the CLOCK_BOOTTIME at which it sent the
//            last confirmed heartbeat; leaseLoop writes $PGDATA/worlber_ha.lease
//            stamped with that time; fencingLoop stops the local PostgreSQL
//            when the lease runs out, like the fence worker inside it.
//   failover: the leader stops confirming the old primary, then waits
//            safety.FenceWait after its last confirmation before promoting.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
	"github.com/worlber/worlber-ha-agent/internal/pg"
	"github.com/worlber/worlber-ha-agent/internal/safety"
)

// leaseState is the agent's fencing state; guarded by Agent.mu.
type leaseState struct {
	bootID       string
	verifiedAt   time.Time            // leader: last successful VerifyLeader (start time)
	lastConfirm  map[string]time.Time // leader: last confirmation given per node
	failingOver  bool                 // leader: decided to replace the primary of failEpoch
	failEpoch    uint64
	confirmedNS  int64 // this node: send time of the last confirmed heartbeat
	writtenTS    int64 // this node: timestamp in the lease file we last wrote
	writtenTTL   time.Duration
	fenceChecked bool // fence settings checked since PostgreSQL last started
	fencing      bool // fencingLoop is stopping PostgreSQL
}

func (a *Agent) leaseTTL(st fsm.State) time.Duration {
	return safety.LeaseTTL(st.Duration("failover_timeout"))
}

// verifyLoop proves leadership every 500ms while this agent is the leader.
func (a *Agent) verifyLoop(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !a.isLeader() {
			continue
		}
		start := time.Now()
		if err := a.raft.VerifyLeader().Error(); err == nil {
			a.mu.Lock()
			a.lease.verifiedAt = start
			a.mu.Unlock()
		}
	}
}

// confirmFor decides whether the leader confirms hb's sender as primary and
// records the confirmation.  It runs on the leader only.
func (a *Agent) confirmFor(hb heartbeat) bool {
	st := a.fsm.Get()
	if !a.isLeader() || !st.Bootstrapped() {
		return false
	}
	if !st.FencingGate {
		return st.Primary == hb.Node && st.Epoch == hb.Epoch
	}
	// A primary that is down past the cutoff is not confirmed only when the
	// leader could actually replace it; otherwise (no candidate, paused)
	// keep it able to restart.
	canReplace := !st.Paused()
	if canReplace {
		phb, _ := a.beat(st.Primary)
		name, _ := PickCandidate(a.candidates(st), a.pickOptions(st, phb.Health.LSN, true))
		canReplace = name != ""
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	since := time.Duration(-1)
	if !a.lease.verifiedAt.IsZero() {
		since = time.Since(a.lease.verifiedAt)
	}
	var badFor time.Duration
	if !a.primaryBad.IsZero() && canReplace {
		badFor = time.Since(a.primaryBad)
	}
	ok := safety.Confirm(safety.ConfirmInput{
		Node: hb.Node, NodeEpoch: hb.Epoch, Running: hb.Health.Running,
		Primary: st.Primary, Epoch: st.Epoch, SinceVerified: since,
		FailingOver:     a.lease.failingOver && a.lease.failEpoch == st.Epoch,
		BadFor:          badFor,
		FailoverTimeout: st.Duration("failover_timeout"),
	})
	if ok {
		if a.lease.lastConfirm == nil {
			a.lease.lastConfirm = map[string]time.Time{}
		}
		a.lease.lastConfirm[hb.Node] = time.Now()
	}
	return ok
}

// setConfirmed records that the heartbeat sent at sentNS was confirmed.
func (a *Agent) setConfirmed(sentNS int64) {
	a.mu.Lock()
	if sentNS > a.lease.confirmedNS {
		a.lease.confirmedNS = sentNS
	}
	a.mu.Unlock()
}

// beginFailover stops confirmations for the current primary.  It returns
// whether the fence wait has passed and how long is left.
func (a *Agent) beginFailover(st fsm.State) (bool, time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lease.failingOver = true
	a.lease.failEpoch = st.Epoch
	return safety.PromotionAllowed(time.Now(), a.lease.lastConfirm[st.Primary], a.leaderSince,
		safety.FenceWait(a.leaseTTL(st)))
}

// abortFailover lets the leader confirm the primary again.
func (a *Agent) abortFailover() {
	a.mu.Lock()
	a.lease.failingOver = false
	a.mu.Unlock()
}

func (a *Agent) leasePath() string {
	return filepath.Join(a.pg.PGData, safety.LeaseFile)
}

// leaseLoop writes the lease while this node is confirmed as the primary.
func (a *Agent) leaseLoop(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.cfg.Witness {
			return
		}
		a.renewLease(ctx)
	}
}

func (a *Agent) renewLease(ctx context.Context) {
	st := a.fsm.Get()
	if st.Primary != a.cfg.Name || !a.pg.HasDataDir() {
		return
	}
	a.mu.Lock()
	conf, written, boot := a.lease.confirmedNS, a.lease.writtenTS, a.lease.bootID
	a.mu.Unlock()
	if boot == "" {
		id, err := safety.BootID()
		if err != nil {
			a.log.Error("cannot read the boot id; no lease written", "error", err)
			return
		}
		boot = id
		a.mu.Lock()
		a.lease.bootID = id
		a.mu.Unlock()
	}
	if !safety.Renew(safety.BootTime(), conf) || conf == written {
		return
	}
	l := safety.Lease{BootID: boot, TS: conf, TTL: a.leaseTTL(st), Term: raftTerm(a), Epoch: st.Epoch}
	if err := writeFileAtomic(a.leasePath(), l.Format()); err != nil {
		a.log.Error("cannot write the lease", "error", err)
		a.report(ctx, "lease_write_failed", err.Error())
		return
	}
	a.mu.Lock()
	a.lease.writtenTS, a.lease.writtenTTL = l.TS, l.TTL
	a.mu.Unlock()
}

func writeFileAtomic(path, content string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// leaseFresh reports whether this node holds a lease that stays valid for
// at least margin more: required before starting or promoting PostgreSQL as
// primary, so the fence worker never stops a primary that the cluster just
// confirmed.
func (a *Agent) leaseFresh(margin time.Duration) bool {
	a.mu.Lock()
	ts, ttl := a.lease.writtenTS, a.lease.writtenTTL
	a.mu.Unlock()
	if ts == 0 {
		return false
	}
	left := ttl - time.Duration(safety.BootTime()-ts)
	return left >= margin
}

// fencingLoop stops the local PostgreSQL when it runs as primary on an
// expired lease.  It is independent of reconcile, so a stuck reconcile does
// not keep a stale primary alive; it is the only place that escalates to
// SIGKILL.
func (a *Agent) fencingLoop(ctx context.Context) {
	t := time.NewTicker(safety.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.cfg.Witness {
			return
		}
		if !a.fsm.Get().FencingGate {
			continue // older agents in the cluster: quorum-loss fencing only
		}
		h := a.localHealth()
		if !h.Running || h.InRecovery {
			continue
		}
		b, err := os.ReadFile(a.leasePath())
		if err != nil {
			continue // no lease ever written here: the fence worker covers it
		}
		boot, _ := safety.BootID()
		l, err := safety.ParseLease(string(b))
		if err == nil && l.Valid(boot, safety.BootTime()) {
			continue
		}
		a.fence(ctx)
	}
}

// fence stops PostgreSQL: SIGINT (fast shutdown), then after 5s SIGKILL to
// every child process and the postmaster.  Children are listed before the
// postmaster dies because they are re-parented afterwards.
func (a *Agent) fence(ctx context.Context) {
	a.mu.Lock()
	if a.lease.fencing {
		a.mu.Unlock()
		return
	}
	a.lease.fencing = true
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.lease.fencing = false
		a.mu.Unlock()
	}()
	pid, err := postmasterPID(a.pg.PGData)
	if err != nil {
		return
	}
	a.log.Warn("lease expired, fencing local primary", "postmaster", pid)
	a.setStatus("fenced: lease expired")
	a.report(ctx, "fenced", "lease expired, PostgreSQL stopped")
	_ = syscall.Kill(pid, syscall.SIGINT)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(pid, 0) != nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	children := childPIDs(pid)
	a.log.Warn("fast shutdown did not finish in 5s, killing PostgreSQL", "postmaster", pid, "children", len(children))
	for _, c := range children {
		_ = syscall.Kill(c, syscall.SIGKILL)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	a.report(ctx, "fence_killed", fmt.Sprintf("fast shutdown hung; SIGKILL sent to postmaster %d and %d children", pid, len(children)))
}

func postmasterPID(pgdata string) (int, error) {
	b, err := os.ReadFile(filepath.Join(pgdata, "postmaster.pid"))
	if err != nil {
		return 0, err
	}
	line, _, _ := strings.Cut(string(b), "\n")
	return strconv.Atoi(strings.TrimSpace(line))
}

// childPIDs lists the direct children of pid from /proc.
func childPIDs(pid int) []int {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var out []int
	for _, e := range ents {
		n, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		// "pid (comm) state ppid ...": comm may contain spaces, so parse
		// after the last ')'.
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(s[i+1:])
		if len(f) >= 2 {
			if pp, err := strconv.Atoi(f[1]); err == nil && pp == pid {
				out = append(out, n)
			}
		}
	}
	return out
}

// ensureFence turns the fence worker on once this node holds a lease (or is
// a standby, so it is armed before a promotion), and makes sure worlber_ha
// is in shared_preload_libraries.
func (a *Agent) ensureFence(ctx context.Context, h pg.Health) {
	a.mu.Lock()
	done := a.lease.fenceChecked
	a.mu.Unlock()
	if done || !h.Running {
		return
	}
	var loaded bool
	if err := a.pg.QueryRow(ctx, "SELECT count(*) > 0 FROM pg_settings WHERE name = 'worlber_ha.fence'", nil, &loaded); err != nil {
		return
	}
	if !loaded {
		var spl string
		if err := a.pg.QueryRow(ctx, "SELECT current_setting('shared_preload_libraries')", nil, &spl); err != nil {
			return
		}
		if !preloadHas(spl) {
			want := strings.TrimSpace(spl)
			if want != "" {
				want += ","
			}
			want += "worlber_ha"
			if err := a.pg.Exec(ctx, "ALTER SYSTEM SET shared_preload_libraries = "+pg.QuoteLiteral(want)); err != nil {
				a.log.Warn("cannot add worlber_ha to shared_preload_libraries", "error", err)
				return
			}
		}
		a.log.Warn("fencing is not active: restart PostgreSQL to load worlber_ha (shared_preload_libraries)")
		a.report(ctx, "fencing_inactive", "worlber_ha is not loaded; restart PostgreSQL to enable the fence worker")
		a.mu.Lock()
		a.lease.fenceChecked = true
		a.mu.Unlock()
		return
	}
	if !h.InRecovery && !a.leaseFresh(0) {
		return // primary: arm only after the first lease is on disk
	}
	var on string
	if err := a.pg.QueryRow(ctx, "SELECT current_setting('worlber_ha.fence')", nil, &on); err != nil {
		return
	}
	if on != "on" {
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET worlber_ha.fence = on"); err != nil {
			a.log.Warn("cannot turn on worlber_ha.fence", "error", err)
			return
		}
		_ = a.pg.Exec(ctx, "SELECT pg_reload_conf()")
		a.report(ctx, "fencing_enabled", "fence worker armed")
	}
	a.mu.Lock()
	a.lease.fenceChecked = true
	a.mu.Unlock()
}

func preloadHas(spl string) bool {
	for _, p := range strings.Split(spl, ",") {
		if strings.Trim(strings.TrimSpace(p), `"`) == "worlber_ha" {
			return true
		}
	}
	return false
}

// fenceState is shown in cluster_status.
func (a *Agent) fenceState() (string, *float64) {
	a.mu.Lock()
	ts, ttl, checked := a.lease.writtenTS, a.lease.writtenTTL, a.lease.fenceChecked
	a.mu.Unlock()
	var age *float64
	if ts != 0 {
		s := float64(safety.BootTime()-ts) / 1e9
		age = &s
		if time.Duration(safety.BootTime()-ts) > ttl {
			return "expired", age
		}
	}
	if !checked {
		return "unknown", age
	}
	return "armed", age
}

func raftTerm(a *Agent) uint64 {
	t, _ := strconv.ParseUint(a.raft.Stats()["term"], 10, 64)
	return t
}
