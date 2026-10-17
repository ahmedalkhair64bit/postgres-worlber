package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
		// An agent that never joined a cluster manages a standalone server:
		// start it so that the DBA can connect and run create_cluster().
		if !st.Bootstrapped() && !a.info().HasRaft && a.pg.HasDataDir() &&
			!a.pg.HasStandbySignal() && !a.pg.IsRunning() {
			a.setStatus("starting standalone server")
			if err := a.pg.Start(ctx); err != nil {
				return err
			}
		}
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
			a.report(ctx, "fenced", fmt.Sprintf("lost Raft quorum for %s, PostgreSQL stopped", lost.Round(time.Second)))
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
	// Start or promote only with a lease the cluster confirmed, valid for a
	// while yet: otherwise the fence worker would stop the new primary, or
	// worse, this node may no longer be the primary.
	const leaseMargin = 500 * time.Millisecond
	gated := st.FencingGate
	if !a.pg.IsRunning() {
		if gated && !a.leaseFresh(leaseMargin) {
			a.setStatus("waiting for the cluster to confirm this primary (lease)")
			return nil
		}
		a.setStatus("starting primary")
		if err := a.pg.Start(ctx); err != nil {
			return err
		}
		a.report(ctx, "primary_started", "PostgreSQL (re)started on the primary")
	}
	h := a.pg.Probe(ctx)
	if !h.Running {
		return fmt.Errorf("primary not reachable: %s", h.Error)
	}
	if h.InRecovery {
		if gated && !a.leaseFresh(leaseMargin) {
			a.setStatus("waiting for the cluster to confirm this primary (lease)")
			return nil
		}
		a.log.Info("promoting local standby", "epoch", st.Epoch)
		a.setStatus("promoting")
		if err := a.pg.Promote(ctx); err != nil {
			a.report(ctx, "promote_failed", err.Error())
			return fmt.Errorf("promote: %w", err)
		}
		a.report(ctx, "promoted", fmt.Sprintf("standby promoted at epoch %d", st.Epoch))
		return nil
	}
	if err := a.ensureSlotLimit(ctx, st); err != nil {
		return fmt.Errorf("max_slot_wal_keep_size: %w", err)
	}
	if err := a.ensureSlots(ctx, st); err != nil {
		return fmt.Errorf("slots: %w", err)
	}
	if err := a.ensureSyncNames(ctx, st, h); err != nil {
		return fmt.Errorf("synchronous_standby_names: %w", err)
	}
	if _, err := a.ensureArchiving(ctx); err != nil {
		a.log.Warn("cannot configure WAL archiving", "error", err)
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

// ensureSlotLimit caps the WAL kept for disconnected standbys, so that a
// standby that is down for days cannot fill the primary's disk.  A standby
// that falls further behind is re-seeded (auto_reinit) instead.
func (a *Agent) ensureSlotLimit(ctx context.Context, st fsm.State) error {
	want := int64(-1)
	if v := st.Option("max_slot_wal_keep"); v != "unlimited" {
		n, err := fsm.ParseBytes(v)
		if err != nil {
			return err
		}
		want = int64(n >> 20) // the setting is in MB
	}
	var cur int64
	if err := a.pg.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name = 'max_slot_wal_keep_size'", nil, &cur); err != nil {
		return err
	}
	if cur == want {
		return nil
	}
	if err := a.pg.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET max_slot_wal_keep_size = '%dMB'", want)); err != nil {
		return err
	}
	a.log.Info("max_slot_wal_keep_size changed", "mb", want)
	return a.pg.Exec(ctx, "SELECT pg_reload_conf()")
}

// ensureSlots keeps one physical slot per standby on the primary.
func (a *Agent) ensureSlots(ctx context.Context, st fsm.State) error {
	// A slot invalidated by max_slot_wal_keep_size cannot be used anymore:
	// drop it, it is recreated below and the standby re-seeds.
	lost, err := a.pg.QueryStrings(ctx, "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'worlber\\_ha\\_%' AND wal_status = 'lost' AND NOT active")
	if err != nil {
		return err
	}
	for _, sl := range lost {
		if err := a.pg.Exec(ctx, "SELECT pg_drop_replication_slot($1)", sl); err == nil {
			a.log.Warn("dropped invalidated replication slot (standby fell behind max_slot_wal_keep)", "slot", sl)
			a.report(ctx, "slot_invalidated", sl+": standby fell more than max_slot_wal_keep behind; it must be re-seeded")
		}
	}
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
//
// It is always "FIRST 1 (...)": exactly one standby is synchronous at a time.
// Standbys marked sync come first; every other standby follows as a
// fallback, so the cluster keeps a synchronous standby when the node marked
// sync is itself the primary (after a failover or switchover to it) or is
// down.  PostgreSQL moves to the next listed standby on its own when the
// current one disconnects.
//
// synchronous_mode "available" lists only connected standbys (no standby
// connected: commits do not wait).  "strict" lists them all, so commits
// wait until one of them is connected.
func SyncNames(st fsm.State, self string, connected map[string]bool) string {
	strict := st.Option("synchronous_mode") == "strict"
	var preferred, fallback []string
	for _, n := range st.Nodes {
		if n.Name == self || n.Witness {
			continue
		}
		if !strict && !connected[n.Name] {
			continue
		}
		if n.Sync {
			preferred = append(preferred, pg.QuoteIdent(n.Name))
		} else {
			fallback = append(fallback, pg.QuoteIdent(n.Name))
		}
	}
	if len(preferred)+len(fallback) == 0 {
		return ""
	}
	sort.Strings(preferred)
	sort.Strings(fallback)
	return "FIRST 1 (" + strings.Join(append(preferred, fallback...), ", ") + ")"
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
	m := map[string]*string{
		"port":                      &port,
		"primary_conninfo":          &ci,
		"primary_slot_name":         &slot,
		"synchronous_standby_names": nil,
	}
	a.archiveSettings(st, m)
	return m
}

func (a *Agent) reconcileReplica(ctx context.Context, st fsm.State) error {
	a.mu.Lock()
	a.quorumLost = time.Time{}
	reinit := a.reinitReason
	a.reinitReason = ""
	a.mu.Unlock()
	if !a.synced(ctx, st) {
		if reinit != "" {
			a.mu.Lock()
			a.reinitReason = reinit
			a.mu.Unlock()
		}
		return nil
	}
	if reinit != "" && st.Primary != a.cfg.Name {
		return a.wipeForReseed(ctx, reinit)
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
			a.report(ctx, "demoted", "former primary stopped, will rejoin as replica of "+st.Primary)
			return a.pg.Stop(ctx, "fast")
		}
		if h.Running {
			a.ensureStandbyArchive(ctx, st)
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
	a.report(ctx, "standby_started", "following "+st.Primary)
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
		a.report(ctx, "repointed", "now following "+st.Primary)
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
		a.archiveProgressAt = time.Time{}
		a.lastReplay = 0
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
	// Not streaming for a long time while the primary is up means either
	// that the WAL this standby needs is gone (it was down too long), or
	// that it diverged (e.g. it was ahead of the promoted node).
	if since > 30*time.Second && a.primaryReachable(ctx, st) {
		// With a WAL archive, a standby that fell behind replays missing
		// segments from the archive (restore_command) instead of re-seeding.
		// Suppress the re-seed/rewind while replay keeps advancing; only act
		// after 120s with no progress.
		if a.archiveDir(st) != "" {
			a.mu.Lock()
			advanced := h.ReplayLSN > a.lastReplay
			if advanced {
				a.lastReplay = h.ReplayLSN
				a.archiveProgressAt = time.Now()
			}
			if a.archiveProgressAt.IsZero() {
				a.archiveProgressAt = time.Now()
			}
			stalled := time.Since(a.archiveProgressAt)
			a.mu.Unlock()
			if advanced || stalled < 120*time.Second {
				a.setStatus("catching up from the WAL archive")
				return nil
			}
		}
		if gone, why := a.walGone(ctx, st, h); gone {
			a.mu.Lock()
			a.noStreamAt = time.Time{}
			a.mu.Unlock()
			if st.Option("auto_reinit") != "true" {
				a.setStatus("needs re-seed: %s; run SELECT worlber_ha.reinit_node('%s')", why, a.cfg.Name)
				return nil
			}
			return a.wipeForReseed(ctx, why)
		}
		a.log.Warn("standby not streaming for 30s, scheduling rewind")
		a.report(ctx, "diverged", "standby not streaming for 30s; rewinding from "+st.Primary)
		a.mu.Lock()
		a.needRewind = true
		a.noStreamAt = time.Time{}
		a.mu.Unlock()
		return a.pg.Stop(ctx, "fast")
	}
	return nil
}

// walGone reports whether the primary no longer has the WAL this standby
// needs: its slot was invalidated, or recreated past our position.
func (a *Agent) walGone(ctx context.Context, st fsm.State, h pg.Health) (bool, string) {
	p := st.Nodes[st.Primary]
	if p == nil {
		return false, ""
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(sourceConnInfo(st, p, "worlber_ha_probe"))
	if err != nil {
		return false, ""
	}
	cfg.Password = st.ReplPassword
	c, err := pgx.ConnectConfig(cctx, cfg)
	if err != nil {
		return false, ""
	}
	defer c.Close(cctx)
	var status, restart *string
	err = c.QueryRow(cctx, "SELECT wal_status, restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
		st.Nodes[a.cfg.Name].SlotName).Scan(&status, &restart)
	if err != nil {
		return false, "" // slot not there yet: the primary recreates it
	}
	if status != nil && *status == "lost" {
		return true, "the WAL this standby needs was removed from the primary (replication slot invalidated by max_slot_wal_keep)"
	}
	local := h.LSN
	if h.ReplayLSN > local {
		local = h.ReplayLSN
	}
	if restart != nil {
		if r, err := pg.ParseLSN(*restart); err == nil && local > 0 && r > local {
			return true, fmt.Sprintf("the primary only has WAL from %s, this standby is at %s", *restart, pg.FormatLSN(local))
		}
	}
	return false, ""
}

// wipeForReseed deletes the data directory of this standby so that the next
// reconcile seeds it again with pg_basebackup.  Only ever done on a node
// whose confirmed role is replica.
func (a *Agent) wipeForReseed(ctx context.Context, why string) error {
	st := a.fsm.Get()
	if st.RoleOf(a.cfg.Name) != fsm.RoleReplica {
		return fmt.Errorf("refusing to wipe PGDATA: this node is not a replica")
	}
	a.log.Warn("re-seeding this standby", "reason", why)
	a.report(ctx, "reseed_started", why)
	a.setStatus("re-seeding: %s", why)
	if a.pg.IsRunning() {
		if err := a.pg.Stop(ctx, "fast"); err != nil {
			if err := a.pg.Stop(ctx, "immediate"); err != nil {
				return err
			}
		}
	}
	ents, err := os.ReadDir(a.pg.PGData)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if err := os.RemoveAll(filepath.Join(a.pg.PGData, e.Name())); err != nil {
			return err
		}
	}
	return nil // the data directory is empty now: cloneFromPrimary follows
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
	a.report(ctx, "pgdata_moved_aside", dst)
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
	a.report(ctx, "seeding_started", "pg_basebackup from "+p.Name+rateNote(st.Option("seed_max_rate")))
	began := time.Now()
	pctx, stopProgress := context.WithCancel(ctx)
	go a.seedProgress(pctx, st, p)
	err := a.pg.BaseBackup(ctx, sourceConnInfo(st, p, a.cfg.Name), st.ReplPassword, slot, st.Option("seed_max_rate"))
	stopProgress()
	if err != nil {
		_ = os.RemoveAll(a.pg.PGData)
		a.report(ctx, "seeding_failed", err.Error())
		return err
	}
	a.report(ctx, "seeding_completed", fmt.Sprintf("pg_basebackup from %s took %s", p.Name, time.Since(began).Round(time.Second)))
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

func rateNote(r string) string {
	if r == "" {
		return ""
	}
	return " (max rate " + r + "/s)"
}

// seedProgress shows pg_basebackup progress (from pg_stat_progress_basebackup
// on the primary) in the agent status, e.g. "seeding from pg1: 45% ...".
func (a *Agent) seedProgress(ctx context.Context, st fsm.State, p *fsm.Node) {
	cfg, err := pgx.ParseConfig(sourceConnInfo(st, p, "worlber_ha_probe"))
	if err != nil {
		return
	}
	cfg.Password = st.ReplPassword
	var c *pgx.Conn
	defer func() {
		if c != nil {
			c.Close(context.Background())
		}
	}()
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if c == nil || c.IsClosed() {
			if c, err = pgx.ConnectConfig(ctx, cfg); err != nil {
				c = nil
				continue
			}
		}
		var phase string
		var done, total *int64
		err := c.QueryRow(ctx, `SELECT b.phase, b.backup_streamed, b.backup_total
			FROM pg_stat_progress_basebackup b JOIN pg_stat_activity a USING (pid)
			WHERE a.application_name = $1`, a.cfg.Name).Scan(&phase, &done, &total)
		if err != nil {
			continue
		}
		msg := fmt.Sprintf("seeding from %s: %s", p.Name, phase)
		if done != nil && total != nil && *total > 0 {
			msg = fmt.Sprintf("seeding from %s: %d%% (%s of %s), %s", p.Name, *done*100 / *total,
				humanBytes(*done), humanBytes(*total), phase)
		} else if done != nil {
			msg = fmt.Sprintf("seeding from %s: %s copied, %s", p.Name, humanBytes(*done), phase)
		}
		a.setStatusQuiet("%s", msg)
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "kMGTPE"[exp])
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
	a.report(ctx, "rejoin_started", "pg_rewind from "+p.Name)
	out, err := a.pg.Rewind(ctx, sourceConnInfo(st, p, a.cfg.Name), st.ReplPassword)
	if err != nil {
		a.log.Error("pg_rewind failed, re-seeding from primary", "error", err)
		a.report(ctx, "rewind_failed", "re-seeding: "+err.Error())
		return false, a.moveAsidePGData(ctx, "diverged")
	}
	a.log.Info("pg_rewind done", "output", strings.TrimSpace(out))
	detail := "data rewound"
	if strings.Contains(out, "no rewind required") {
		detail = "no rewind required (clean switchover)"
	}
	a.report(ctx, "rewind_completed", detail+" from "+p.Name)
	a.mu.Lock()
	a.needRewind = false
	a.mu.Unlock()
	return true, nil
}
