package agent

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
)

// Candidate is a standby considered for promotion.
type Candidate struct {
	Name       string
	Sync       bool
	Fresh      bool
	Running    bool
	InRecovery bool
	LSN        uint64
}

// PickCandidate chooses the standby to promote.  Synchronous standbys are
// preferred, then the one with the most WAL received.  maxLag (bytes behind
// primaryLSN) of 0 disables the lag check; strict allows only sync standbys.
func PickCandidate(cands []Candidate, primaryLSN, maxLag uint64, strict bool) (string, string) {
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
		case strict && !c.Sync:
			reasons += c.Name + ": not synchronous (strict mode); "
		case maxLag > 0 && primaryLSN > c.LSN && primaryLSN-c.LSN > maxLag:
			reasons += fmt.Sprintf("%s: lag %d bytes exceeds maximum_lag_on_failover; ", c.Name, primaryLSN-c.LSN)
		default:
			ok = append(ok, c)
		}
	}
	if len(ok) == 0 {
		if reasons == "" {
			reasons = "no standbys"
		}
		return "", reasons
	}
	sort.Slice(ok, func(i, j int) bool {
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
		a.checkPrimary()
	}
}

func (a *Agent) checkPrimary() {
	st := a.fsm.Get()
	if !st.Bootstrapped() || st.Paused() {
		a.mu.Lock()
		a.primaryBad = time.Time{}
		a.mu.Unlock()
		return
	}
	hb, fresh := a.beat(st.Primary)
	healthy := fresh && hb.Health.Running
	a.mu.Lock()
	if healthy {
		a.primaryBad = time.Time{}
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
	strict := st.Option("synchronous_mode") == "strict"
	name, why := PickCandidate(a.candidates(st), hb.Health.LSN, maxLag, strict)
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
	loss := a.lossEstimate(st, name, hb)
	a.log.Warn("automatic failover", "old_primary", st.Primary, "new_primary", name, "down_for", badFor.Round(time.Second), "data_loss", loss)
	if err := a.apply(fsm.Command{Op: "set_primary", Name: name, ExpectedEpoch: st.Epoch, Reason: "automatic_failover",
		Detail: fmt.Sprintf("primary down for %s; %s", badFor.Round(time.Second), loss)}); err != nil {
		a.log.Error("failover commit failed", "error", err)
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
