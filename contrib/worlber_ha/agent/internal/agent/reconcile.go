package agent

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
	"github.com/worlber/worlber-ha-agent/internal/pg"
)

// reconcileLoop drives the local PostgreSQL towards the role assigned by
// the replicated state.  It runs sequentially so long actions (base backup,
// rewind) never overlap.
func (a *Agent) reconcileLoop(ctx context.Context) {
	t := time.NewTicker(a.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-a.fsm.Changed():
		}
		if a.cfg.Witness {
			continue
		}
		if err := a.reconcile(ctx); err != nil {
			a.setStatus("error: %v", err)
		}
	}
}

func (a *Agent) reconcile(ctx context.Context) error {
	st := a.fsm.Get()
	if _, member := st.Nodes[a.cfg.Name]; !member {
		if st.Bootstrapped() {
			a.setStatus("not a member of cluster %s", st.ClusterName)
		} else {
			a.setStatus("waiting: not part of a cluster")
		}
		return nil
	}
	if st.Paused() {
		a.setStatus("paused")
		return nil
	}
	switch st.RoleOf(a.cfg.Name) {
	case fsm.RolePrimary:
		return a.reconcilePrimary(ctx, st)
	case fsm.RoleReplica:
		return a.reconcileReplica(ctx, st)
	}
	return nil
}

// held reports whether a switchover asked us not to restart the primary.
func (a *Agent) held(epoch uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.holdEpoch == epoch && time.Now().Before(a.holdUntil)
}

func (a *Agent) reconcilePrimary(ctx context.Context, st fsm.State) error {
	timeout := st.Duration("failover_timeout")
	// Fencing: a primary that lost contact with the Raft majority must stop
	// before the majority can elect a new primary (after failover_timeout).
	if !a.hasQuorum(timeout / 3) {
		a.mu.Lock()
		if a.quorumLost.IsZero() {
			a.quorumLost = time.Now()
		}
		lost := time.Since(a.quorumLost)
		a.mu.Unlock()
		if lost >= timeout/3 && a.pg.IsRunning() {
			a.log.Warn("lost Raft quorum, fencing local primary", "lost_for", lost.Round(time.Second))
			a.setStatus("fenced: lost quorum")
			return a.pg.Stop(ctx, "fast")
		}
		a.setStatus("waiting for quorum")
		return nil
	}
	a.mu.Lock()
	a.quorumLost = time.Time{}
	a.mu.Unlock()
	if !a.synced(ctx, st) {
		return nil
	}
	a.mu.Lock()
	a.needRewind = false
	a.mu.Unlock()

	if a.held(st.Epoch) {
		return nil
	}
	if !a.pg.HasDataDir() {
		return fmt.Errorf("PGDATA %s has no database cluster", a.pg.PGData)
	}
	if !a.pg.IsRunning() {
		a.setStatus("starting primary")
		if err := a.pg.Start(ctx); err != nil {
			return err
		}
	}
	h := a.pg.Probe(ctx)
	if !h.Running {
		return fmt.Errorf("primary not reachable: %s", h.Error)
	}
	if h.InRecovery {
		a.log.Info("promoting local standby", "epoch", st.Epoch)
		a.setStatus("promoting")
		if err := a.pg.Promote(ctx); err != nil {
			return fmt.Errorf("promote: %w", err)
		}
		return nil
	}
	if err := a.ensureSlots(ctx, st); err != nil {
		return fmt.Errorf("slots: %w", err)
	}
	if err := a.ensureSyncNames(ctx, st, h); err != nil {
		return fmt.Errorf("synchronous_standby_names: %w", err)
	}
	a.setStatus("")
	return nil
}

// synced asks the Raft leader whether our replicated state is current.  A
// restarted agent replays its local Raft log first and may briefly see an
// outdated primary; acting on that could start a second primary or demote
// the real one, so every action except fencing needs this confirmation.
func (a *Agent) synced(ctx context.Context, st fsm.State) bool {
	var info nodeInfo
	if a.isLeader() {
		info = a.info()
		if !info.Leader {
			a.setStatus("waiting for Raft barrier")
			return false
		}
	} else {
		addr, err := a.leaderAPI()
		if err == nil {
			err = a.peerPostTimeout(ctx, addr, "/peer/info", nil, &info, 5*time.Second)
		}
		if err != nil || !info.Leader {
			a.setStatus("waiting for the Raft leader to confirm the primary")
			return false
		}
	}
	if info.Primary != st.Primary || info.Epoch != st.Epoch {
		a.setStatus("waiting for Raft log to catch up (leader says primary=%s epoch=%d)", info.Primary, info.Epoch)
		return false
	}
	return true
}

// ensureSlots keeps one physical slot per standby on the primary.
func (a *Agent) ensureSlots(ctx context.Context, st fsm.State) error {
	have, err := a.pg.QueryStrings(ctx, "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'worlber\\_ha\\_%' AND slot_type = 'physical'")
	if err != nil {
		return err
	}
	exists := map[string]bool{}
	for _, s := range have {
		exists[s] = true
	}
	want := map[string]bool{}
	for _, n := range st.Nodes {
		if n.Name == a.cfg.Name || n.Witness {
			continue
		}
		want[n.SlotName] = true
		if !exists[n.SlotName] {
			if err := a.pg.Exec(ctx, "SELECT pg_create_physical_replication_slot($1, true)", n.SlotName); err != nil {
				return err
			}
			a.log.Info("created replication slot", "slot", n.SlotName)
		}
	}
	for s := range exists {
		if !want[s] {
			if err := a.pg.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", s); err == nil {
				a.log.Info("dropped replication slot", "slot", s)
			}
		}
	}
	return nil
}

// SyncNames computes synchronous_standby_names for the primary.
func SyncNames(st fsm.State, self string, connected map[string]bool) string {
	var names []string
	strict := st.Option("synchronous_mode") == "strict"
	for _, n := range st.Nodes {
		if n.Name == self || n.Witness || !n.Sync {
			continue
		}
		if strict || connected[n.Name] {
			names = append(names, pg.QuoteIdent(n.Name))
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return "ANY 1 (" + strings.Join(names, ", ") + ")"
}

func (a *Agent) ensureSyncNames(ctx context.Context, st fsm.State, h pg.Health) error {
	connected := map[string]bool{}
	for _, r := range h.Replicas {
		if r.State == "streaming" {
			connected[r.AppName] = true
		}
	}
	want := SyncNames(st, a.cfg.Name, connected)
	var cur string
	if err := a.pg.QueryRow(ctx, "SELECT current_setting('synchronous_standby_names')", nil, &cur); err != nil {
		return err
	}
	if cur == want {
		return nil
	}
	if err := a.pg.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = "+pg.QuoteLiteral(want)); err != nil {
		return err
	}
	a.log.Info("synchronous_standby_names changed", "value", want)
	return a.pg.Exec(ctx, "SELECT pg_reload_conf()")
}

// primaryConnInfo is the primary_conninfo this standby should use.
func (a *Agent) primaryConnInfo(st fsm.State) (string, *fsm.Node) {
	p := st.Nodes[st.Primary]
	if p == nil {
		return "", nil
	}
	return pg.ConnInfo("host", p.PGHost, "port", fmt.Sprint(p.PGPort), "user", st.ReplUser,
		"password", st.ReplPassword, "application_name", a.cfg.Name, "dbname", "postgres"), p
}

// sourceConnInfo is used for pg_basebackup / pg_rewind (password via env).
func sourceConnInfo(st fsm.State, p *fsm.Node, appName string) string {
	return pg.ConnInfo("host", p.PGHost, "port", fmt.Sprint(p.PGPort), "user", st.ReplUser,
		"dbname", "postgres", "application_name", appName, "connect_timeout", "5")
}

// standbySettings are written into postgresql.auto.conf of a standby.
func (a *Agent) standbySettings(st fsm.State) map[string]*string {
	ci, _ := a.primaryConnInfo(st)
	port := fmt.Sprint(a.pg.Port)
	slot := st.Nodes[a.cfg.Name].SlotName
	return map[string]*string{
		"port":                      &port,
		"primary_conninfo":          &ci,
		"primary_slot_name":         &slot,
		"synchronous_standby_names": nil,
	}
}

func (a *Agent) reconcileReplica(ctx context.Context, st fsm.State) error {
	a.mu.Lock()
	a.quorumLost = time.Time{}
	a.mu.Unlock()
	if !a.synced(ctx, st) {
		return nil
	}
	if a.pg.DataDirEmpty() {
		return a.cloneFromPrimary(ctx, st)
	}
	if !a.pg.HasDataDir() {
		return fmt.Errorf("PGDATA %s is not empty and is not a PostgreSQL data directory", a.pg.PGData)
	}
	if ok, err := a.sameSystem(ctx, st); err != nil {
		return err
	} else if !ok {
		if !a.cfg.ReinitPGData {
			return fmt.Errorf("PGDATA belongs to a different database system; restart the agent with --reinit-pgdata to replace it")
		}
		return a.moveAsidePGData(ctx, "foreign")
	}

	running := a.pg.IsRunning()
	if running {
		h := a.pg.Probe(ctx)
		if h.Running && !h.InRecovery {
			// A former primary that is no longer primary: stop and rejoin.
			a.log.Warn("demoting local server: it is not the primary anymore", "primary", st.Primary, "epoch", st.Epoch)
			a.setStatus("demoting")
			a.mu.Lock()
			a.needRewind = true
			a.mu.Unlock()
			return a.pg.Stop(ctx, "fast")
		}
		if h.Running {
			return a.followPrimary(ctx, st, h)
		}
		return nil
	}

	a.mu.Lock()
	rewind := a.needRewind || !a.pg.HasStandbySignal()
	a.mu.Unlock()
	if rewind {
		if done, err := a.rewindFromPrimary(ctx, st); err != nil || !done {
			return err
		}
	}
	if err := a.pg.WriteStandbySignal(); err != nil {
		return err
	}
	if err := a.pg.SetAutoConf(a.standbySettings(st)); err != nil {
		return err
	}
	a.setStatus("starting standby")
	if err := a.pg.Start(ctx); err != nil {
		return err
	}
	a.setStatus("")
	return nil
}

// followPrimary keeps a running standby connected to the current primary.
func (a *Agent) followPrimary(ctx context.Context, st fsm.State, h pg.Health) error {
	want, _ := a.primaryConnInfo(st)
	var cur, slot string
	if err := a.pg.QueryRow(ctx, "SELECT current_setting('primary_conninfo'), current_setting('primary_slot_name')", nil, &cur, &slot); err != nil {
		return err
	}
	wantSlot := st.Nodes[a.cfg.Name].SlotName
	if cur != want || slot != wantSlot {
		a.log.Info("re-pointing standby", "primary", st.Primary)
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET primary_conninfo = "+pg.QuoteLiteral(want)); err != nil {
			return err
		}
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET primary_slot_name = "+pg.QuoteLiteral(wantSlot)); err != nil {
			return err
		}
		if err := a.pg.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
			return err
		}
		a.mu.Lock()
		a.noStreamAt = time.Now()
		a.mu.Unlock()
		return nil
	}
	a.mu.Lock()
	if h.WalReceiver == "streaming" {
		a.noStreamAt = time.Time{}
		a.mu.Unlock()
		a.setStatus("")
		return nil
	}
	if a.noStreamAt.IsZero() {
		a.noStreamAt = time.Now()
	}
	since := time.Since(a.noStreamAt)
	a.mu.Unlock()
	a.setStatus("standby not streaming")
	// Not streaming for a long time while the primary is up usually means
	// this standby diverged (e.g. it was ahead of the promoted node).
	if since > 30*time.Second && a.primaryReachable(ctx, st) {
		a.log.Warn("standby not streaming for 30s, scheduling rewind")
		a.mu.Lock()
		a.needRewind = true
		a.noStreamAt = time.Time{}
		a.mu.Unlock()
		return a.pg.Stop(ctx, "fast")
	}
	return nil
}

func (a *Agent) primaryReachable(ctx context.Context, st fsm.State) bool {
	p := st.Nodes[st.Primary]
	if p == nil {
		return false
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(sourceConnInfo(st, p, "worlber_ha_probe"))
	if err != nil {
		return false
	}
	cfg.Password = st.ReplPassword
	c, err := pgx.ConnectConfig(cctx, cfg)
	if err != nil {
		return false
	}
	defer c.Close(cctx)
	var rec bool
	return c.QueryRow(cctx, "SELECT pg_is_in_recovery()").Scan(&rec) == nil && !rec
}

func (a *Agent) sameSystem(ctx context.Context, st fsm.State) (bool, error) {
	cd, err := a.pg.ControlData(ctx)
	if err != nil {
		return false, err
	}
	return cd["Database system identifier"] == st.SystemID, nil
}

func (a *Agent) moveAsidePGData(ctx context.Context, why string) error {
	if a.pg.IsRunning() {
		if err := a.pg.Stop(ctx, "fast"); err != nil {
			return err
		}
	}
	dst := fmt.Sprintf("%s.worlber_ha_%s.%s", strings.TrimRight(a.pg.PGData, "/"), why, time.Now().Format("20060102T150405"))
	a.log.Warn("moving PGDATA aside", "to", dst)
	return os.Rename(a.pg.PGData, dst)
}

// cloneFromPrimary seeds an empty PGDATA with pg_basebackup.
func (a *Agent) cloneFromPrimary(ctx context.Context, st fsm.State) error {
	_, p := a.primaryConnInfo(st)
	if p == nil {
		return fmt.Errorf("no primary")
	}
	slot := st.Nodes[a.cfg.Name].SlotName
	if !a.slotExists(ctx, st, p, slot) {
		a.setStatus("waiting for replication slot %s on %s", slot, p.Name)
		return nil
	}
	a.setStatus("seeding: pg_basebackup from %s", p.Name)
	a.log.Info("starting base backup", "primary", p.Name)
	if err := a.pg.BaseBackup(ctx, sourceConnInfo(st, p, a.cfg.Name), st.ReplPassword, slot); err != nil {
		_ = os.RemoveAll(a.pg.PGData)
		return err
	}
	if err := a.pg.SetAutoConf(a.standbySettings(st)); err != nil {
		return err
	}
	a.setStatus("starting standby")
	if err := a.pg.Start(ctx); err != nil {
		return err
	}
	a.log.Info("standby started", "primary", p.Name)
	a.setStatus("")
	return nil
}

func (a *Agent) slotExists(ctx context.Context, st fsm.State, p *fsm.Node, slot string) bool {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(sourceConnInfo(st, p, "worlber_ha_probe"))
	if err != nil {
		return false
	}
	cfg.Password = st.ReplPassword
	c, err := pgx.ConnectConfig(cctx, cfg)
	if err != nil {
		a.setStatus("cannot connect to primary %s: %v", p.Name, err)
		return false
	}
	defer c.Close(cctx)
	var ok bool
	return c.QueryRow(cctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&ok) == nil && ok
}

// rewindFromPrimary resynchronizes a former primary with pg_rewind.
// It reports done=false when the caller must retry later.
func (a *Agent) rewindFromPrimary(ctx context.Context, st fsm.State) (bool, error) {
	_, p := a.primaryConnInfo(st)
	if p == nil || p.Name == a.cfg.Name {
		return false, fmt.Errorf("no primary to rewind from")
	}
	if !a.primaryReachable(ctx, st) {
		a.setStatus("waiting for primary %s before rejoining", p.Name)
		return false, nil
	}
	a.setStatus("rejoining: pg_rewind from %s", p.Name)
	out, err := a.pg.Rewind(ctx, sourceConnInfo(st, p, a.cfg.Name), st.ReplPassword)
	if err != nil {
		a.log.Error("pg_rewind failed, re-seeding from primary", "error", err)
		return false, a.moveAsidePGData(ctx, "diverged")
	}
	a.log.Info("pg_rewind done", "output", strings.TrimSpace(out))
	a.mu.Lock()
	a.needRewind = false
	a.mu.Unlock()
	return true, nil
}
