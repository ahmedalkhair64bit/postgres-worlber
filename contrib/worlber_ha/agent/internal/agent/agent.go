// Package agent implements the worlber_ha node agent: Raft membership,
// health reporting, reconciliation of the local PostgreSQL instance with
// the replicated cluster state, and (on the Raft leader) failover.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
	"github.com/worlber/worlber-ha-agent/internal/pg"
	"github.com/worlber/worlber-ha-agent/internal/safety"
)

// Config is the static configuration of one agent.
type Config struct {
	Name         string
	DataDir      string // raft state
	RaftAddr     string // host:port other agents use for Raft
	APIAddr      string // host:port other agents use for the peer API
	RaftBind     string // listen address for Raft (default RaftAddr)
	APIBind      string // listen address for the peer API (default APIAddr)
	Socket       string // local unix socket for the SQL extension
	Token        string // shared cluster secret
	Witness      bool
	PGAdvertise  string // host other nodes use to reach this PostgreSQL
	ReinitPGData bool   // allow moving aside a foreign PGDATA
	ReplUser     string
	ListenRW     string // listener for read-write clients (e.g. :5000)
	ListenRO     string // listener for read-only clients (e.g. :5001)
	VIP          string // floating IP held by the primary (e.g. 10.0.0.100/24)
	VIPInterface string // interface for the VIP (e.g. eth0)
	OnRoleChange string // script called as: <script> <role> <cluster> <node>
	Tick         time.Duration
	PG           *pg.Local
	LogLevel     string
}

// heartbeat is what each agent reports to the leader every tick.
type heartbeat struct {
	Node   string    `json:"node"`
	Health pg.Health `json:"health"`
	Status string    `json:"status"`
	Epoch  uint64    `json:"epoch"`
	At     time.Time `json:"-"`
}

// Agent is a running node agent.
type Agent struct {
	cfg  Config
	log  hclog.Logger
	fsm  *fsm.FSM
	raft *raft.Raft
	pg   *pg.Local
	http *http.Client

	mu    sync.Mutex
	beats map[string]heartbeat // leader only
	// syncSeen: for each primary, the standbys it last reported in
	// sync/quorum state while PostgreSQL was running (leader only).  A
	// heartbeat from a crashed primary has no replica list and does not
	// overwrite it.
	syncSeen     map[string]syncView
	local        pg.Health
	status       string
	holdUntil    time.Time // don't start PostgreSQL as primary (switchover)
	holdEpoch    uint64    // ... but only while the epoch is unchanged
	needRewind   bool
	reinitReason string // set by reinit_node: wipe and re-seed this standby
	noStreamAt   time.Time
	quorumLost   time.Time
	wasLeader    bool
	leaderReady  bool   // barrier done in this leadership term
	readyTerm    string // ... namely this Raft term
	leaderSince  time.Time
	primaryBad   time.Time
	lastNoCand   time.Time

	lease leaseState // fencing (lease.go)

	opMu sync.Mutex // serializes leader operations (switchover, failover...)

	stop context.CancelFunc // ends Run (used when removed from the cluster)

	logs   *logRing
	router *router

	evMu      sync.Mutex    // protects pending (separate from mu: report() may be called with mu held)
	pending   []fsm.Command // events waiting for delivery to the leader
	eventKick chan struct{}
}

// leftMarker is created when this node was removed from its cluster; on the
// next start the Raft state is wiped so the node can join a cluster again.
const leftMarker = "LEFT_CLUSTER"

// New creates an agent; call Run to start it.
func New(cfg Config) (*Agent, error) {
	if cfg.Name == "" {
		return nil, errors.New("node name is required")
	}
	if cfg.Tick == 0 {
		cfg.Tick = time.Second
	}
	if cfg.ReplUser == "" {
		cfg.ReplUser = "worlber_ha_repl"
	}
	lvl := hclog.LevelFromString(cfg.LogLevel)
	if lvl == hclog.NoLevel {
		lvl = hclog.Info
	}
	ring := newLogRing(2000)
	a := &Agent{
		cfg:  cfg,
		logs: ring,
		log: hclog.New(&hclog.LoggerOptions{Name: "worlber-ha[" + cfg.Name + "]", Level: lvl,
			IndependentLevels: true, Output: io.MultiWriter(os.Stderr, ring)}),
		fsm:      fsm.New(),
		pg:       cfg.PG,
		http:     &http.Client{Timeout: 15 * time.Second},
		beats:    map[string]heartbeat{},
		syncSeen: map[string]syncView{},

		eventKick: make(chan struct{}, 1),
	}
	return a, nil
}

func (a *Agent) startRaft() error {
	lvl := hclog.LevelFromString(a.cfg.LogLevel)
	if err := os.MkdirAll(a.cfg.DataDir, 0700); err != nil {
		return err
	}
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(a.cfg.Name)
	// Raft is chatty about unreachable peers (every 500ms); keep it at warn.
	rl := a.log.Named("raft")
	if lvl > hclog.Warn {
		rl.SetLevel(lvl)
	} else {
		rl.SetLevel(hclog.Warn)
	}
	rc.Logger = rl
	rc.HeartbeatTimeout = 1000 * time.Millisecond
	rc.ElectionTimeout = 1000 * time.Millisecond
	rc.LeaderLeaseTimeout = 500 * time.Millisecond
	rc.SnapshotThreshold = 1024

	if _, err := os.Stat(filepath.Join(a.cfg.DataDir, leftMarker)); err == nil {
		a.log.Warn("this node was removed from its cluster; resetting Raft state")
		_ = os.Remove(filepath.Join(a.cfg.DataDir, "raft.db"))
		_ = os.RemoveAll(filepath.Join(a.cfg.DataDir, "snapshots"))
		_ = os.Remove(filepath.Join(a.cfg.DataDir, leftMarker))
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(a.cfg.DataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("raft store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(a.cfg.DataDir, 2, rl.Named("snap"))
	if err != nil {
		return err
	}
	addr, err := net.ResolveTCPAddr("tcp", a.cfg.RaftAddr)
	if err != nil {
		return fmt.Errorf("raft address: %w", err)
	}
	bind := a.cfg.RaftBind
	if bind == "" {
		bind = a.cfg.RaftAddr
	}
	trans, err := raft.NewTCPTransportWithLogger(bind, addr, 3, 10*time.Second, rl.Named("net"))
	if err != nil {
		return fmt.Errorf("raft transport: %w", err)
	}
	r, err := raft.NewRaft(rc, a.fsm, store, store, snaps, trans)
	if err != nil {
		return err
	}
	a.raft = r
	return nil
}

// Run starts all servers and loops; it returns when ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	ctx, a.stop = context.WithCancel(ctx)
	defer a.stop()
	if err := a.startRaft(); err != nil {
		return err
	}
	apiBind := a.cfg.APIBind
	if apiBind == "" {
		apiBind = a.cfg.APIAddr
	}
	peerLn, err := net.Listen("tcp", apiBind)
	if err != nil {
		return fmt.Errorf("peer API: %w", err)
	}
	_ = os.Remove(a.cfg.Socket)
	localLn, err := net.Listen("unix", a.cfg.Socket)
	if err != nil {
		return fmt.Errorf("local socket %s: %w", a.cfg.Socket, err)
	}
	_ = os.Chmod(a.cfg.Socket, 0600)

	peerSrv := &http.Server{Handler: a.peerHandler()}
	localSrv := &http.Server{Handler: a.localHandler()}
	go peerSrv.Serve(peerLn)
	go localSrv.Serve(localLn)
	a.router = newRouter(a)
	for _, l := range []struct {
		addr string
		ro   bool
	}{{a.cfg.ListenRW, false}, {a.cfg.ListenRO, true}} {
		if l.addr != "" {
			if err := a.router.listen(ctx, l.addr, l.ro); err != nil {
				return err
			}
		}
	}
	a.log.Info("agent started", "raft", a.cfg.RaftAddr, "api", a.cfg.APIAddr, "socket", a.cfg.Socket, "witness", a.cfg.Witness)

	var wg sync.WaitGroup
	wg.Add(5)
	go func() { defer wg.Done(); a.eventLoop(ctx) }()
	go func() { defer wg.Done(); a.router.run(ctx) }()
	go func() { defer wg.Done(); a.heartbeatLoop(ctx) }()
	go func() { defer wg.Done(); a.reconcileLoop(ctx) }()
	go func() { defer wg.Done(); a.leaderLoop(ctx) }()
	go func() { defer wg.Done(); a.verifyLoop(ctx) }()
	go func() { defer wg.Done(); a.leaseLoop(ctx) }()
	go func() { defer wg.Done(); a.fencingLoop(ctx) }()

	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = peerSrv.Shutdown(sctx)
	_ = localSrv.Shutdown(sctx)
	_ = os.Remove(a.cfg.Socket)
	wg.Wait()
	_ = a.raft.Shutdown().Error()
	a.pg.Close()
	return nil
}

func (a *Agent) setStatus(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	a.mu.Lock()
	changed := a.status != s
	a.status = s
	a.mu.Unlock()
	if changed && s != "" {
		a.log.Info("status", "status", s)
	}
}

// setStatusQuiet updates the status without logging (progress messages).
func (a *Agent) setStatusQuiet(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	a.mu.Lock()
	a.status = s
	a.mu.Unlock()
}

func (a *Agent) isLeader() bool { return a.raft.State() == raft.Leader }

// leaderAPI returns the peer API address of the current Raft leader.
func (a *Agent) leaderAPI() (string, error) {
	_, id := a.raft.LeaderWithID()
	if id == "" {
		return "", errors.New("no Raft leader (cluster has no quorum)")
	}
	st := a.fsm.Get()
	n, ok := st.Nodes[string(id)]
	if !ok {
		return "", fmt.Errorf("leader %s unknown", id)
	}
	return n.APIAddr, nil
}

// hasQuorum reports whether this node is in contact with a Raft majority.
func (a *Agent) hasQuorum(within time.Duration) bool {
	switch a.raft.State() {
	case raft.Leader:
		return true
	case raft.Follower:
		lc := a.raft.LastContact()
		return !lc.IsZero() && time.Since(lc) < within
	}
	return false
}

// apply commits a command through Raft (leader only).
func (a *Agent) apply(c fsm.Command) error {
	if c.Time == 0 {
		c.Time = time.Now().Unix()
	}
	b, _ := json.Marshal(c)
	f := a.raft.Apply(b, 10*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	if err, ok := f.Response().(error); ok && err != nil {
		return err
	}
	return nil
}

// heartbeatLoop probes local PostgreSQL and reports to the leader.
func (a *Agent) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(a.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var h pg.Health
		if !a.cfg.Witness {
			h = a.pg.Probe(ctx)
		}
		a.mu.Lock()
		a.local = h
		hb := heartbeat{Node: a.cfg.Name, Health: h, Status: a.status, Epoch: a.fsm.Get().Epoch}
		a.mu.Unlock()

		st := a.fsm.Get()
		if _, member := st.Nodes[a.cfg.Name]; !member {
			continue
		}
		if !h.Running {
			a.mu.Lock()
			a.lease.fenceChecked = false // check the fence settings after the next start
			a.mu.Unlock()
		} else if !a.cfg.Witness {
			a.ensureFence(ctx, h)
		}
		// The lease may only be stamped with the time this heartbeat was
		// sent: the leader saw us no earlier than that.
		sent := safety.BootTime()
		if a.isLeader() {
			a.recordBeat(hb)
			if a.confirmFor(hb) {
				a.setConfirmed(sent)
			}
			continue
		}
		addr, err := a.leaderAPI()
		if err != nil {
			continue
		}
		var rep struct {
			Confirm bool `json:"confirm"`
		}
		// A reply older than safety.Q is useless for the lease anyway.
		if err := a.peerPostTimeout(ctx, addr, "/peer/heartbeat", hb, &rep, safety.Q); err == nil && rep.Confirm {
			a.setConfirmed(sent)
		}
	}
}

// report records something this agent did in the cluster event history.
// It never blocks: events are queued and delivered to the Raft leader by
// eventLoop, so an event raised while partitioned (e.g. "fenced") is
// recorded once the node can reach the leader again.
func (a *Agent) report(ctx context.Context, kind, detail string) {
	// keep the time the event happened, not the time it was delivered
	c := fsm.Command{Op: "event", Kind: kind, Name: a.cfg.Name, Detail: detail, Time: time.Now().Unix()}
	a.evMu.Lock()
	if len(a.pending) < 200 {
		a.pending = append(a.pending, c)
	}
	a.evMu.Unlock()
	select {
	case a.eventKick <- struct{}{}:
	default:
	}
}

// eventLoop delivers queued events in order.
func (a *Agent) eventLoop(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-a.eventKick:
		}
		for {
			a.evMu.Lock()
			if len(a.pending) == 0 {
				a.evMu.Unlock()
				break
			}
			c := a.pending[0]
			a.evMu.Unlock()
			var err error
			if a.isLeader() {
				err = a.apply(c)
			} else if addr, lerr := a.leaderAPI(); lerr != nil {
				err = lerr
			} else {
				err = a.peerPostTimeout(ctx, addr, "/peer/op/record_event", c, nil, 5*time.Second)
			}
			if err != nil {
				break // retry later
			}
			a.evMu.Lock()
			a.pending = a.pending[1:]
			a.evMu.Unlock()
		}
	}
}

func (a *Agent) recordBeat(hb heartbeat) {
	hb.At = time.Now()
	a.mu.Lock()
	a.storeBeatLocked(hb)
	a.mu.Unlock()
}

// syncView is what a running primary last said about its standbys.
type syncView struct {
	epoch uint64
	sync  map[string]bool // standbys in sync or quorum state
	at    time.Time
}

// storeBeatLocked records a heartbeat; a.mu must be held.
func (a *Agent) storeBeatLocked(hb heartbeat) {
	a.beats[hb.Node] = hb
	if hb.Health.Running && !hb.Health.InRecovery {
		v := syncView{epoch: hb.Epoch, sync: map[string]bool{}, at: hb.At}
		for _, r := range hb.Health.Replicas {
			if r.SyncState == "sync" || r.SyncState == "quorum" {
				v.sync[r.AppName] = true
			}
		}
		a.syncSeen[hb.Node] = v
	}
}

// lastSync returns the standbys the primary last reported as synchronous,
// and false when this agent never saw a running heartbeat from it in the
// current epoch (for example right after a leader change).
func (a *Agent) lastSync(primary string, epoch uint64) (map[string]bool, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.syncSeen[primary]
	if !ok || v.epoch != epoch {
		return nil, false
	}
	return v.sync, true
}

// beat returns the latest heartbeat of a node and whether it is fresh.
func (a *Agent) beat(node string) (heartbeat, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	hb, ok := a.beats[node]
	return hb, ok && time.Since(hb.At) < 3*a.cfg.Tick+2*time.Second
}

func (a *Agent) localHealth() pg.Health {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.local
}
