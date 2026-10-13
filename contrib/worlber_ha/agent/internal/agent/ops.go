package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jackc/pgx/v5"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
	"github.com/worlber/worlber-ha-agent/internal/pg"
	"github.com/worlber/worlber-ha-agent/internal/safety"
)

// localOps run on the agent that received the call; everything else is
// executed by the Raft leader (forwarded if necessary).
var localOps = map[string]bool{"create_cluster": true, "events": true, "config": true, "local_status": true, "log": true}

var leaderOps = map[string]bool{
	"status": true, "add_node": true, "remove_node": true, "set_sync": true,
	"set_config": true, "switchover": true, "failover": true,
	"replication": true, "record_event": true, "reinit_node": true, "verify_table": true,
}

// execute dispatches a request from the SQL extension.
func (a *Agent) execute(ctx context.Context, op string, args json.RawMessage) (opResult, error) {
	switch {
	case localOps[op]:
		return a.localOp(ctx, op, args)
	case leaderOps[op]:
		if !a.fsm.Get().Bootstrapped() {
			return opResult{}, errHint("Run SELECT worlber_ha.create_cluster('name') on the primary first, or add this node to an existing cluster with worlber_ha.add_node() on the primary.",
				"this node is not a member of a worlber_ha cluster")
		}
		if a.isLeader() {
			return a.leaderOp(ctx, op, args)
		}
		addr, err := a.leaderAPI()
		if err != nil {
			if op == "status" {
				res, _ := a.localOp(ctx, "local_status", args)
				res.Warnings = append(res.Warnings, "no Raft leader reachable; showing this node's local view only")
				return res, nil
			}
			return opResult{}, err
		}
		var rep reply
		timeout := 30 * time.Second
		if op == "add_node" || op == "switchover" {
			timeout = 15 * time.Minute
		}
		if err := a.peerPostTimeout(ctx, addr, "/peer/op/"+op, args, &rep, timeout); err != nil {
			return opResult{}, err
		}
		return opResult{Result: rep.Result, Warnings: rep.Warnings}, nil
	}
	return opResult{}, fmt.Errorf("unknown operation %q", op)
}

func (a *Agent) localOp(ctx context.Context, op string, args json.RawMessage) (opResult, error) {
	st := a.fsm.Get()
	switch op {
	case "create_cluster":
		return a.createCluster(ctx, args)
	case "events":
		ev := st.Events
		if ev == nil {
			ev = []fsm.Event{}
		}
		return opResult{Result: ev}, nil
	case "config":
		cfg := map[string]string{}
		for k := range fsm.DefaultConfig {
			cfg[k] = st.Option(k)
		}
		return opResult{Result: cfg}, nil
	case "local_status":
		return opResult{Result: a.buildStatus(st, true)}, nil
	case "log":
		var req struct {
			Node  string `json:"node"`
			Lines int    `json:"lines"`
			Level string `json:"level"`
		}
		_ = json.Unmarshal(args, &req)
		if req.Node == "" || req.Node == a.cfg.Name {
			return opResult{Result: a.logs.Tail(clampLines(req.Lines), req.Level)}, nil
		}
		n, ok := st.Nodes[req.Node]
		if !ok {
			return opResult{}, fmt.Errorf("node %q is not a member", req.Node)
		}
		var rep reply
		if err := a.peerPost(ctx, n.APIAddr, "/peer/log", req, &rep); err != nil {
			return opResult{}, err
		}
		return opResult{Result: rep.Result}, nil
	}
	return opResult{}, fmt.Errorf("unknown operation %q", op)
}

func (a *Agent) leaderOp(ctx context.Context, op string, args json.RawMessage) (opResult, error) {
	switch op {
	case "status":
		return opResult{Result: a.buildStatus(a.fsm.Get(), false)}, nil
	case "replication":
		return opResult{Result: a.replicationStatus(a.fsm.Get())}, nil
	case "reinit_node":
		var req struct{ Name string }
		_ = json.Unmarshal(args, &req)
		return a.reinitNode(ctx, req.Name)
	case "verify_table":
		var req struct {
			Table string `json:"table"`
			DB    string `json:"dbname"`
		}
		_ = json.Unmarshal(args, &req)
		return a.verifyTable(ctx, req.DB, req.Table)
	case "record_event":
		var c fsm.Command
		if err := json.Unmarshal(args, &c); err != nil {
			return opResult{}, err
		}
		if c.Kind == "" {
			return opResult{}, fmt.Errorf("kind is required")
		}
		return opResult{Result: "ok"}, a.apply(fsm.Command{Op: "event", Kind: c.Kind, Name: c.Name, Detail: c.Detail, Time: c.Time})
	case "add_node":
		return a.addNode(ctx, args)
	case "remove_node":
		var req struct{ Name string }
		_ = json.Unmarshal(args, &req)
		return a.removeNode(ctx, req.Name)
	case "set_sync":
		var req struct {
			Name string
			Sync bool
		}
		_ = json.Unmarshal(args, &req)
		if _, ok := a.fsm.Get().Nodes[req.Name]; !ok {
			return opResult{}, fmt.Errorf("node %q is not a member", req.Name)
		}
		return opResult{Result: "ok"}, a.apply(fsm.Command{Op: "set_sync", Name: req.Name, Sync: req.Sync})
	case "set_config":
		var req struct{ Key, Value string }
		_ = json.Unmarshal(args, &req)
		if err := validateNewOption(req.Key, req.Value); err != nil {
			return opResult{}, err
		}
		return opResult{Result: "ok"}, a.apply(fsm.Command{Op: "set_config", Key: req.Key, Value: req.Value})
	case "switchover":
		var req struct{ Target string }
		_ = json.Unmarshal(args, &req)
		return a.switchover(ctx, req.Target)
	case "failover":
		var req struct {
			Target string
			Force  bool
		}
		_ = json.Unmarshal(args, &req)
		return a.manualFailover(req.Target, req.Force)
	}
	return opResult{}, fmt.Errorf("unknown operation %q", op)
}

func randomSecret() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// createCluster turns the local primary into a one-node cluster.
func (a *Agent) createCluster(ctx context.Context, args json.RawMessage) (opResult, error) {
	var req struct {
		Name            string `json:"name"`
		SynchronousMode string `json:"synchronous_mode"`
		FailoverTimeout string `json:"failover_timeout"`
	}
	if err := json.Unmarshal(args, &req); err != nil {
		return opResult{}, err
	}
	if req.Name == "" {
		return opResult{}, fmt.Errorf("cluster name is required")
	}
	if a.cfg.Witness {
		return opResult{}, fmt.Errorf("a witness agent cannot create a cluster")
	}
	st := a.fsm.Get()
	if st.Bootstrapped() {
		return opResult{}, errHint("Use worlber_ha.cluster_status to see it.", "this node already belongs to cluster %q", st.ClusterName)
	}
	if a.info().HasRaft {
		return opResult{}, fmt.Errorf("this agent already has Raft state from another cluster; remove it from that cluster first")
	}
	for k, v := range map[string]string{"synchronous_mode": req.SynchronousMode, "failover_timeout": req.FailoverTimeout} {
		if v != "" {
			if err := validateNewOption(k, v); err != nil {
				return opResult{}, err
			}
		}
	}

	h := a.pg.Probe(ctx)
	if !h.Running {
		return opResult{}, fmt.Errorf("cannot connect to local PostgreSQL: %s", h.Error)
	}
	if h.InRecovery {
		return opResult{}, fmt.Errorf("create_cluster must be run on a primary, this server is in recovery")
	}

	var warnings []string
	var sysid string
	if err := a.pg.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()", nil, &sysid); err != nil {
		return opResult{}, err
	}
	var walLevel, checksums, hints string
	_ = a.pg.QueryRow(ctx, "SELECT current_setting('wal_level'), current_setting('data_checksums'), current_setting('wal_log_hints')", nil, &walLevel, &checksums, &hints)
	restart := false
	if walLevel == "minimal" {
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET wal_level = 'replica'"); err != nil {
			return opResult{}, err
		}
		restart = true
	}
	if checksums != "on" && hints != "on" {
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET wal_log_hints = on"); err != nil {
			return opResult{}, err
		}
		restart = true
	}
	if restart {
		warnings = append(warnings, "wal_level/wal_log_hints were changed with ALTER SYSTEM; restart PostgreSQL before adding replicas")
	}

	pw := randomSecret()
	if err := a.prepareReplRole(ctx, pw); err != nil {
		return opResult{}, err
	}
	if w, err := a.ensureHBA(ctx); err != nil {
		warnings = append(warnings, "could not update pg_hba.conf: "+err.Error())
	} else if w != "" {
		warnings = append(warnings, w)
	}

	f := a.raft.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
		{Suffrage: raft.Voter, ID: raft.ServerID(a.cfg.Name), Address: raft.ServerAddress(a.cfg.RaftAddr)},
	}})
	if err := f.Error(); err != nil {
		return opResult{}, fmt.Errorf("raft bootstrap: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !a.isLeader() {
		if time.Now().After(deadline) {
			return opResult{}, fmt.Errorf("timed out waiting for Raft leadership")
		}
		time.Sleep(100 * time.Millisecond)
	}
	node := &fsm.Node{Name: a.cfg.Name, PGHost: a.cfg.PGAdvertise, PGPort: a.cfg.PG.Port,
		APIAddr: a.cfg.APIAddr, RaftAddr: a.cfg.RaftAddr}
	if err := a.apply(fsm.Command{Op: "bootstrap", ClusterName: req.Name, SystemID: sysid,
		ReplUser: a.cfg.ReplUser, ReplPassword: pw, Node: node}); err != nil {
		return opResult{}, err
	}
	for k, v := range map[string]string{"synchronous_mode": req.SynchronousMode, "failover_timeout": req.FailoverTimeout} {
		if v != "" {
			if err := a.apply(fsm.Command{Op: "set_config", Key: k, Value: v}); err != nil {
				return opResult{}, err
			}
		}
	}
	a.log.Info("cluster created", "cluster", req.Name, "primary", a.cfg.Name)
	return opResult{Result: map[string]any{"cluster": req.Name, "primary": a.cfg.Name, "system_identifier": sysid},
		Warnings: warnings}, nil
}

// prepareReplRole creates the replication role used by standbys and pg_rewind.
func (a *Agent) prepareReplRole(ctx context.Context, password string) error {
	u := pg.QuoteIdent(a.cfg.ReplUser)
	var exists bool
	if err := a.pg.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", []any{a.cfg.ReplUser}, &exists); err != nil {
		return err
	}
	verb := "CREATE"
	if exists {
		verb = "ALTER"
	}
	stmts := []string{
		fmt.Sprintf("%s ROLE %s WITH LOGIN REPLICATION PASSWORD %s", verb, u, pg.QuoteLiteral(password)),
		// privileges needed by pg_rewind --source-server
		"GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_dir(text, boolean, boolean) TO " + u,
		"GRANT EXECUTE ON FUNCTION pg_catalog.pg_stat_file(text, boolean) TO " + u,
		"GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text) TO " + u,
		"GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint, boolean) TO " + u,
	}
	for _, s := range stmts {
		if err := a.pg.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", strings.SplitN(s, " PASSWORD", 2)[0], err)
		}
	}
	return nil
}

// ensureHBA allows the replication role to connect from other nodes.
func (a *Agent) ensureHBA(ctx context.Context) (string, error) {
	var hbaFile string
	if err := a.pg.QueryRow(ctx, "SELECT current_setting('hba_file')", nil, &hbaFile); err != nil {
		return "", err
	}
	data, err := os.ReadFile(hbaFile)
	if err != nil {
		return "", err
	}
	marker := "# worlber_ha: replication and pg_rewind access"
	if strings.Contains(string(data), marker) {
		return "", nil
	}
	lines := fmt.Sprintf("\n%s\nhost replication %s all scram-sha-256\nhost postgres %s all scram-sha-256\n",
		marker, a.cfg.ReplUser, a.cfg.ReplUser)
	fh, err := os.OpenFile(hbaFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	if _, err := fh.WriteString(lines); err != nil {
		fh.Close()
		return "", err
	}
	fh.Close()
	if err := a.pg.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return "", err
	}
	return "", nil
}

// addNode joins a remote agent to the cluster (leader only).
func (a *Agent) addNode(ctx context.Context, args json.RawMessage) (opResult, error) {
	var req struct {
		Host      string `json:"host"`
		AgentPort int    `json:"agent_port"`
		Sync      bool   `json:"sync"`
		Wait      bool   `json:"wait"`
		Timeout   int    `json:"timeout"`
	}
	if err := json.Unmarshal(args, &req); err != nil {
		return opResult{}, err
	}
	if req.Host == "" {
		return opResult{}, fmt.Errorf("host is required")
	}
	if req.AgentPort == 0 {
		req.AgentPort = 7400
	}
	if req.Timeout <= 0 {
		req.Timeout = 600
	}
	a.opMu.Lock()
	defer a.opMu.Unlock()

	addr := net.JoinHostPort(req.Host, strconv.Itoa(req.AgentPort))
	var info nodeInfo
	if err := a.peerPost(ctx, addr, "/peer/info", nil, &info); err != nil {
		return opResult{}, errHint("Install and start worlber-ha-agent on "+req.Host+" with the same --token, then retry.",
			"cannot reach agent on %s: %v", addr, err)
	}
	st := a.fsm.Get()
	if _, ok := st.Nodes[info.Name]; ok {
		return opResult{}, fmt.Errorf("node %q is already a member", info.Name)
	}
	if info.ClusterName != "" || info.HasRaft {
		return opResult{}, fmt.Errorf("agent %s already belongs to a cluster (%q)", info.Name, info.ClusterName)
	}
	if err := a.raft.AddVoter(raft.ServerID(info.Name), raft.ServerAddress(info.RaftAddr), 0, 15*time.Second).Error(); err != nil {
		return opResult{}, fmt.Errorf("raft add voter: %w", err)
	}
	node := &fsm.Node{Name: info.Name, PGHost: info.PGHost, PGPort: info.PGPort, APIAddr: addr,
		RaftAddr: info.RaftAddr, Sync: req.Sync && !info.Witness, Witness: info.Witness}
	if err := a.apply(fsm.Command{Op: "add_node", Node: node}); err != nil {
		_ = a.raft.RemoveServer(raft.ServerID(info.Name), 0, 10*time.Second).Error()
		return opResult{}, err
	}
	res := map[string]any{"node": info.Name, "witness": info.Witness}
	if !req.Wait || info.Witness {
		return opResult{Result: res}, nil
	}
	deadline := time.Now().Add(time.Duration(req.Timeout) * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return opResult{Result: res, Warnings: []string{"wait cancelled; the node keeps initializing in the background"}}, nil
		case <-time.After(time.Second):
		}
		if hb, fresh := a.beat(info.Name); fresh && hb.Health.Running && hb.Health.InRecovery && hb.Health.WalReceiver == "streaming" {
			res["state"] = "streaming"
			return opResult{Result: res}, nil
		}
	}
	return opResult{Result: res, Warnings: []string{"node added but not streaming yet; check worlber_ha.cluster_status"}}, nil
}

func (a *Agent) removeNode(ctx context.Context, name string) (opResult, error) {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	st := a.fsm.Get()
	n, ok := st.Nodes[name]
	if !ok {
		return opResult{}, fmt.Errorf("node %q is not a member", name)
	}
	if st.Primary == name {
		return opResult{}, errHint("Run worlber_ha.switchover() to another node first.", "cannot remove the primary %q", name)
	}
	if name == a.cfg.Name {
		// A leader cannot remove itself: hand leadership to another node and
		// let the new leader do the removal.
		a.opMu.Unlock()
		defer a.opMu.Lock()
		if err := a.raft.LeadershipTransfer().Error(); err != nil {
			return opResult{}, fmt.Errorf("leadership transfer: %w", err)
		}
		deadline := time.Now().Add(15 * time.Second)
		for {
			if _, id := a.raft.LeaderWithID(); id != "" && string(id) != a.cfg.Name {
				break
			}
			if time.Now().After(deadline) {
				return opResult{}, fmt.Errorf("no new Raft leader after leadership transfer")
			}
			time.Sleep(100 * time.Millisecond)
		}
		addr, err := a.leaderAPI()
		if err != nil {
			return opResult{}, err
		}
		var rep reply
		if err := a.peerPostTimeout(ctx, addr, "/peer/op/remove_node", map[string]string{"name": name}, &rep, 60*time.Second); err != nil {
			return opResult{}, err
		}
		return opResult{Result: rep.Result, Warnings: rep.Warnings}, nil
	}
	if err := a.apply(fsm.Command{Op: "remove_node", Name: name}); err != nil {
		return opResult{}, err
	}
	var warnings []string
	if err := a.raft.RemoveServer(raft.ServerID(name), 0, 15*time.Second).Error(); err != nil {
		warnings = append(warnings, "raft remove server: "+err.Error())
	}
	a.mu.Lock()
	delete(a.beats, name)
	a.mu.Unlock()
	if err := a.peerPost(ctx, n.APIAddr, "/peer/leave", nil, nil); err != nil {
		warnings = append(warnings, fmt.Sprintf("could not tell %s to leave (%v); before adding it again, stop its agent and delete its --data-dir", name, err))
	}
	return opResult{Result: "ok", Warnings: warnings}, nil
}

// candidates returns failover candidates from the leader's heartbeats.
func (a *Agent) candidates(st fsm.State) []Candidate {
	var out []Candidate
	// Prefer the standby the primary actually used as synchronous; the sync
	// flag is only a preference and may name the primary itself.  Without a
	// report from the running primary (new leader), fall back to the flag.
	seen, known := a.lastSync(st.Primary, st.Epoch)
	inActual := map[string]bool{}
	for _, n := range st.ActualSync {
		inActual[n] = true
	}
	for _, n := range st.SortedNodes() {
		if n.Name == st.Primary || n.Witness {
			continue
		}
		hb, fresh := a.beat(n.Name)
		sync := n.Sync
		if known {
			sync = seen[n.Name]
		}
		out = append(out, Candidate{Name: n.Name, Sync: sync, InActualSync: inActual[n.Name], Fresh: fresh,
			Running: hb.Health.Running, InRecovery: hb.Health.InRecovery, LSN: hb.Health.LSN})
	}
	return out
}

func (a *Agent) primaryHealthy(st fsm.State) bool {
	hb, fresh := a.beat(st.Primary)
	return fresh && hb.Health.Running && !hb.Health.InRecovery
}

// switchover performs a planned, zero-data-loss role change.
func (a *Agent) switchover(ctx context.Context, target string) (opResult, error) {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	st := a.fsm.Get()
	if st.Paused() {
		return opResult{}, errHint("Run SELECT worlber_ha.resume() first.", "cluster is paused")
	}
	if target == "" {
		phb, _ := a.beat(st.Primary)
		name, why := PickCandidate(a.candidates(st), a.pickOptions(st, phb.Health.LSN, false))
		if name == "" {
			return opResult{}, fmt.Errorf("no switchover candidate: %s", why)
		}
		target = name
	}
	n, ok := st.Nodes[target]
	if !ok {
		return opResult{}, fmt.Errorf("node %q is not a member", target)
	}
	if n.Witness {
		return opResult{}, fmt.Errorf("node %q is a witness", target)
	}
	if target == st.Primary {
		return opResult{}, fmt.Errorf("node %q is already the primary", target)
	}
	thb, fresh := a.beat(target)
	if !fresh || !thb.Health.Running || !thb.Health.InRecovery || thb.Health.WalReceiver != "streaming" {
		return opResult{}, fmt.Errorf("target %q is not a healthy streaming standby", target)
	}
	if !a.primaryHealthy(st) {
		return opResult{}, errHint("Use worlber_ha.failover() instead.", "primary %q is not healthy", st.Primary)
	}
	old := st.Nodes[st.Primary]

	// 1. stop the old primary cleanly; its shutdown checkpoint is the last WAL.
	var stop struct {
		CheckpointLSN string `json:"checkpoint_lsn"`
	}
	var err error
	if old.Name == a.cfg.Name {
		stop.CheckpointLSN, err = a.stopForSwitchover(ctx, st.Epoch)
	} else {
		err = a.peerPostTimeout(ctx, old.APIAddr, "/peer/stop_for_switchover", map[string]any{"epoch": st.Epoch}, &stop, 3*time.Minute)
	}
	if err != nil {
		a.releaseHold(ctx, old)
		return opResult{}, fmt.Errorf("stopping old primary: %w", err)
	}
	ckpt, _ := pg.ParseLSN(stop.CheckpointLSN)

	// 2. wait until the target replayed the shutdown checkpoint.
	deadline := time.Now().Add(60 * time.Second)
	for {
		hb, _ := a.beat(target)
		if hb.Health.ReplayLSN > ckpt {
			break
		}
		if time.Now().After(deadline) {
			a.releaseHold(ctx, old)
			return opResult{}, fmt.Errorf("target %q did not catch up to %s; switchover aborted, old primary restarts", target, stop.CheckpointLSN)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 3. commit the new primary; agents promote and re-point standbys.
	if err := a.apply(fsm.Command{Op: "set_primary", Name: target, ExpectedEpoch: st.Epoch, Reason: "switchover",
		Detail: "no data loss: " + target + " replayed the old primary's shutdown checkpoint " + stop.CheckpointLSN}); err != nil {
		a.releaseHold(ctx, old)
		return opResult{}, err
	}
	deadline = time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if hb, fresh := a.beat(target); fresh && hb.Health.Running && !hb.Health.InRecovery {
			return opResult{Result: map[string]any{"old_primary": old.Name, "new_primary": target}}, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return opResult{Result: map[string]any{"old_primary": old.Name, "new_primary": target},
		Warnings: []string{"new primary has not confirmed promotion yet"}}, nil
}

func (a *Agent) releaseHold(ctx context.Context, n *fsm.Node) {
	if n.Name == a.cfg.Name {
		a.mu.Lock()
		a.holdUntil = time.Time{}
		a.mu.Unlock()
		return
	}
	_ = a.peerPost(ctx, n.APIAddr, "/peer/release_hold", nil, nil)
}

// stopForSwitchover stops the local primary and returns its last checkpoint.
func (a *Agent) stopForSwitchover(ctx context.Context, epoch uint64) (string, error) {
	st := a.fsm.Get()
	if st.Primary != a.cfg.Name || st.Epoch != epoch {
		return "", fmt.Errorf("this node is not the primary of epoch %d", epoch)
	}
	a.mu.Lock()
	a.holdUntil = time.Now().Add(3 * time.Minute)
	a.holdEpoch = epoch
	a.mu.Unlock()
	_ = a.pg.Exec(ctx, "CHECKPOINT")
	a.setStatus("stopping for switchover")
	if err := a.pg.Stop(ctx, "fast"); err != nil {
		return "", err
	}
	cd, err := a.pg.ControlData(ctx)
	if err != nil {
		return "", err
	}
	if cd["Database cluster state"] != "shut down" {
		return "", fmt.Errorf("primary did not shut down cleanly (%s)", cd["Database cluster state"])
	}
	return cd["Latest checkpoint location"], nil
}

func (a *Agent) manualFailover(target string, force bool) (opResult, error) {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	st := a.fsm.Get()
	if a.primaryHealthy(st) && !force {
		return opResult{}, errHint("Use worlber_ha.switchover() for a planned change, or failover(force => true) to accept data loss.",
			"primary %q is healthy", st.Primary)
	}
	if target == "" {
		phb, _ := a.beat(st.Primary)
		opts := a.pickOptions(st, phb.Health.LSN, false)
		if force {
			opts.MaxLag = 0
		}
		name, why := PickCandidate(a.candidates(st), opts)
		if name == "" {
			return opResult{}, fmt.Errorf("no failover candidate: %s", why)
		}
		target = name
	}
	n, ok := st.Nodes[target]
	if !ok || n.Witness || target == st.Primary {
		return opResult{}, fmt.Errorf("invalid failover target %q", target)
	}
	if hb, fresh := a.beat(target); !fresh || !hb.Health.Running {
		return opResult{}, fmt.Errorf("target %q is not running", target)
	}
	// Stop confirming the old primary and wait until its lease is gone, so
	// it cannot accept writes once the target is promoted.
	for {
		ok, left := a.beginFailover(st)
		if ok {
			break
		}
		a.log.Info("manual failover: waiting for the old primary's lease to expire", "left", left.Round(100*time.Millisecond))
		time.Sleep(left)
	}
	phb, _ := a.beat(st.Primary)
	if err := a.apply(fsm.Command{Op: "set_primary", Name: target, ExpectedEpoch: st.Epoch, Reason: "manual_failover",
		Detail: a.lossEstimate(st, target, phb) + "; old primary fenced (lease expired)"}); err != nil {
		a.abortFailover()
		return opResult{}, err
	}
	return opResult{Result: map[string]any{"old_primary": st.Primary, "new_primary": target}}, nil
}

// nodeStatus is one row of worlber_ha.cluster_status.
type nodeStatus struct {
	Node       string `json:"node"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Role       string `json:"role"`
	State      string `json:"state"`
	Timeline   int    `json:"timeline"`
	LSN        string `json:"lsn,omitempty"`
	ReplayLSN  string `json:"replay_lsn,omitempty"`
	LagBytes   *int64 `json:"lag_bytes"`
	Sync       bool   `json:"sync"`
	SyncState  string `json:"sync_state"`
	RaftRole   string `json:"raft_role"`
	LastSeenMs *int64 `json:"last_seen_ms"`
	Agent      string `json:"agent_status"`
}

func (a *Agent) buildStatus(st fsm.State, localOnly bool) map[string]any {
	leaderAddr, leaderID := a.raft.LeaderWithID()
	_ = leaderAddr
	voters := map[string]bool{}
	if cf := a.raft.GetConfiguration(); cf.Error() == nil {
		for _, s := range cf.Configuration().Servers {
			voters[string(s.ID)] = true
		}
	}
	if localOnly {
		a.mu.Lock()
		a.storeBeatLocked(heartbeat{Node: a.cfg.Name, Health: a.local, Status: a.status, Epoch: st.Epoch, At: time.Now()})
		a.mu.Unlock()
	}
	phb, _ := a.beat(st.Primary)
	syncStates := map[string]string{}
	for _, r := range phb.Health.Replicas {
		syncStates[r.AppName] = r.SyncState
	}
	var rows []nodeStatus
	for _, n := range st.SortedNodes() {
		ns := nodeStatus{Node: n.Name, Host: n.PGHost, Port: n.PGPort, Role: st.RoleOf(n.Name), Sync: n.Sync}
		switch {
		case string(leaderID) == n.Name:
			ns.RaftRole = "leader"
		case voters[n.Name]:
			ns.RaftRole = "follower"
		default:
			ns.RaftRole = "none"
		}
		hb, fresh := a.beat(n.Name)
		if !hb.At.IsZero() {
			ms := time.Since(hb.At).Milliseconds()
			ns.LastSeenMs = &ms
		}
		ns.Agent = hb.Status
		h := hb.Health
		switch {
		case n.Witness:
			ns.State = "witness"
			if !fresh {
				ns.State = "unreachable"
			}
		case !fresh:
			ns.State = "unreachable"
		case !h.Running:
			ns.State = "stopped"
		case h.InRecovery && h.WalReceiver == "streaming":
			ns.State = "streaming"
		case h.InRecovery:
			ns.State = "recovering"
		default:
			ns.State = "running"
		}
		if !n.Witness && h.Running {
			ns.Timeline = h.Timeline
			ns.LSN = pg.FormatLSN(h.LSN)
			if h.InRecovery {
				ns.ReplayLSN = pg.FormatLSN(h.ReplayLSN)
				if phb.Health.LSN > 0 && n.Name != st.Primary {
					lag := int64(phb.Health.LSN) - int64(h.ReplayLSN)
					if lag < 0 {
						lag = 0
					}
					ns.LagBytes = &lag
				}
			}
		}
		if n.Name != st.Primary && !n.Witness {
			ns.SyncState = syncStates[n.Name]
			if ns.SyncState == "" {
				ns.SyncState = "disconnected"
			}
		}
		rows = append(rows, ns)
	}
	sort.Slice(rows, func(i, j int) bool {
		if (rows[i].Role == fsm.RolePrimary) != (rows[j].Role == fsm.RolePrimary) {
			return rows[i].Role == fsm.RolePrimary
		}
		return rows[i].Node < rows[j].Node
	})
	return map[string]any{
		"cluster": st.ClusterName, "primary": st.Primary, "epoch": st.Epoch,
		"leader": string(leaderID), "paused": st.Paused(), "nodes": rows,
	}
}

// replicationRow is one row of worlber_ha.replication: how far each standby
// is behind, in the terms of SQL Server's log send queue and redo queue.
type replicationRow struct {
	Node       string  `json:"node"`
	ClientAddr string  `json:"client_addr"`
	State      string  `json:"state"`
	SyncState  string  `json:"sync_state"`
	PrimaryLSN string  `json:"primary_lsn"`
	SentLSN    string  `json:"sent_lsn"`
	WriteLSN   string  `json:"write_lsn"`
	FlushLSN   string  `json:"flush_lsn"`
	ReplayLSN  string  `json:"replay_lsn"`
	SendQueue  int64   `json:"send_queue_bytes"`
	RedoQueue  int64   `json:"redo_queue_bytes"`
	TotalLag   int64   `json:"total_lag_bytes"`
	WriteLag   float64 `json:"write_lag_s"`
	FlushLag   float64 `json:"flush_lag_s"`
	ReplayLag  float64 `json:"replay_lag_s"`
}

func lsnDiff(a, b uint64) int64 {
	if a < b {
		return 0
	}
	return int64(a - b)
}

func (a *Agent) replicationStatus(st fsm.State) []replicationRow {
	phb, _ := a.beat(st.Primary)
	rows := []replicationRow{}
	for _, r := range phb.Health.Replicas {
		if _, ok := st.Nodes[r.AppName]; !ok {
			continue
		}
		p := phb.Health.LSN
		rows = append(rows, replicationRow{
			Node: r.AppName, ClientAddr: r.ClientAddr, State: r.State, SyncState: r.SyncState,
			PrimaryLSN: pg.FormatLSN(p), SentLSN: pg.FormatLSN(r.SentLSN), WriteLSN: pg.FormatLSN(r.WriteLSN),
			FlushLSN: pg.FormatLSN(r.FlushLSN), ReplayLSN: pg.FormatLSN(r.ReplayLSN),
			SendQueue: lsnDiff(p, r.FlushLSN), RedoQueue: lsnDiff(r.FlushLSN, r.ReplayLSN), TotalLag: lsnDiff(p, r.ReplayLSN),
			WriteLag: r.WriteLag, FlushLag: r.FlushLag, ReplayLag: r.ReplayLag,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Node < rows[j].Node })
	return rows
}

// reinitNode asks a standby to wipe its data and re-seed from the primary.
func (a *Agent) reinitNode(ctx context.Context, name string) (opResult, error) {
	st := a.fsm.Get()
	n, ok := st.Nodes[name]
	if !ok {
		return opResult{}, fmt.Errorf("node %q is not a member", name)
	}
	if n.Witness {
		return opResult{}, fmt.Errorf("node %q is a witness", name)
	}
	if st.Primary == name {
		return opResult{}, errHint("Switch over to another node first.", "cannot re-initialize the primary %q", name)
	}
	if err := a.peerPost(ctx, n.APIAddr, "/peer/reinit", map[string]string{"reason": "requested with reinit_node()"}, nil); err != nil {
		return opResult{}, err
	}
	return opResult{Result: map[string]any{"node": name, "state": "re-seeding",
		"follow": "SELECT node, state, agent_status FROM worlber_ha.cluster_status"}}, nil
}

// fingerprint is the checksum of one table on one node.
type fingerprint struct {
	Node    string `json:"node"`
	Role    string `json:"role"`
	LSN     string `json:"lsn"`
	Rows    int64  `json:"rows"`
	MD5     string `json:"md5"`
	Matches *bool  `json:"matches_primary"`
	Error   string `json:"error,omitempty"`
}

// verifyTable computes the checksum of a table on the primary, then on each
// standby once it has replayed up to the primary's LSN, and compares them.
func (a *Agent) verifyTable(ctx context.Context, db, table string) (opResult, error) {
	if table == "" {
		return opResult{}, fmt.Errorf("table is required")
	}
	if db == "" {
		db = "postgres"
	}
	st := a.fsm.Get()
	ask := func(n *fsm.Node, wait string) fingerprint {
		var fp fingerprint
		req := map[string]string{"dbname": db, "table": table, "wait_lsn": wait}
		var err error
		if n.Name == a.cfg.Name {
			fp, err = a.localFingerprint(ctx, db, table, wait)
		} else {
			err = a.peerPostTimeout(ctx, n.APIAddr, "/peer/fingerprint", req, &fp, 10*time.Minute)
		}
		fp.Node, fp.Role = n.Name, st.RoleOf(n.Name)
		if err != nil {
			fp.Error = err.Error()
		}
		return fp
	}
	primary := ask(st.Nodes[st.Primary], "")
	if primary.Error != "" {
		return opResult{}, fmt.Errorf("on primary %s: %s", st.Primary, primary.Error)
	}
	t := true
	primary.Matches = &t
	out := []fingerprint{primary}
	ch := make(chan fingerprint)
	count := 0
	for _, n := range st.SortedNodes() {
		if n.Name == st.Primary || n.Witness {
			continue
		}
		count++
		go func(n *fsm.Node) { ch <- ask(n, primary.LSN) }(n)
	}
	for i := 0; i < count; i++ {
		fp := <-ch
		if fp.Error == "" {
			m := fp.MD5 == primary.MD5 && fp.Rows == primary.Rows
			fp.Matches = &m
		}
		out = append(out, fp)
	}
	sort.Slice(out[1:], func(i, j int) bool { return out[1+i].Node < out[1+j].Node })
	return opResult{Result: out}, nil
}

// localFingerprint checksums a table on the local server, after waiting
// until it has replayed waitLSN (standbys).
func (a *Agent) localFingerprint(ctx context.Context, db, table, waitLSN string) (fingerprint, error) {
	var fp fingerprint
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return fp, fmt.Errorf("invalid table name %q", table)
	}
	ident := pgx.Identifier(parts).Sanitize()
	cfg, err := pgx.ParseConfig(pg.ConnInfo("host", a.pg.SocketDir, "port", fmt.Sprint(a.pg.Port), "user", a.pg.User,
		"dbname", db, "connect_timeout", "5", "application_name", "worlber_ha_verify"))
	if err != nil {
		return fp, err
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fp, err
	}
	defer c.Close(context.Background())
	if waitLSN != "" {
		deadline := time.Now().Add(60 * time.Second)
		for {
			var ok bool
			if err := c.QueryRow(ctx, "SELECT coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, true)", waitLSN).Scan(&ok); err != nil {
				return fp, err
			}
			if ok {
				break
			}
			if time.Now().After(deadline) {
				return fp, fmt.Errorf("did not replay up to %s within 60s", waitLSN)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	tx, err := c.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fp, err
	}
	defer tx.Rollback(context.Background())
	err = tx.QueryRow(ctx, `SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END::text,
	        count(*), coalesce(md5(string_agg(md5(t::text), '' ORDER BY md5(t::text))), md5(''))
	        FROM `+ident+` t`).Scan(&fp.LSN, &fp.Rows, &fp.MD5)
	return fp, err
}

// validateNewOption checks an option set by a DBA now.  It may be stricter
// than fsm.ValidateOption, which must keep accepting what older versions
// committed to the Raft log.
func validateNewOption(key, value string) error {
	if err := fsm.ValidateOption(key, value); err != nil {
		return err
	}
	if key == "failover_timeout" {
		if d, _ := time.ParseDuration(value); d < safety.MinFailoverTimeout {
			return fmt.Errorf("failover_timeout must be at least %s: the old primary's lease must expire before a standby is promoted", safety.MinFailoverTimeout)
		}
	}
	return nil
}

// pickOptions builds the failover selection options from the cluster state.
func (a *Agent) pickOptions(st fsm.State, primaryLSN uint64, automatic bool) PickOptions {
	maxLag, _ := fsm.ParseBytes(st.Option("maximum_lag_on_failover"))
	return PickOptions{
		PrimaryLSN:    primaryLSN,
		MaxLag:        maxLag,
		Strict:        st.Option("synchronous_mode") == "strict",
		Automatic:     automatic,
		HasActualSync: len(st.ActualSync) > 0,
		Async:         st.Option("async_failover"),
	}
}
