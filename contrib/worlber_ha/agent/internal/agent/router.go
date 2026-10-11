package agent

// Client routing, the equivalent of an availability group listener:
//
//   - listener: every agent can accept PostgreSQL connections on a read-write
//     port (forwarded to the current primary) and a read-only port (spread
//     over healthy standbys, falling back to the primary).  Applications use
//     any node, or a VIP / load balancer in front of the nodes.
//   - VIP: the agent of the primary holds a floating IP address.
//   - role hook: a script called whenever the local role changes (DNS, cloud
//     alias IP, load balancer...).
//   - health endpoints for external load balancers (GET /health/primary).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
)

type routeTable struct {
	epoch    uint64
	primary  string   // address of the primary
	replicas []string // addresses of streaming standbys
	at       time.Time
}

type router struct {
	a *Agent

	mu     sync.Mutex
	table  routeTable
	conns  map[net.Conn]uint64 // read-write client connections -> epoch
	rr     atomic.Uint64
	vipOn  bool
	role   string
	noArp  bool
	vipErr time.Time // last VIP error logged (rate limit)
}

func newRouter(a *Agent) *router {
	return &router{a: a, conns: map[net.Conn]uint64{}}
}

func pgAddr(n *fsm.Node) string {
	if strings.HasPrefix(n.PGHost, "/") {
		return n.PGHost + "/.s.PGSQL." + strconv.Itoa(n.PGPort)
	}
	return net.JoinHostPort(n.PGHost, strconv.Itoa(n.PGPort))
}

func dialPG(addr string) (net.Conn, error) {
	if strings.HasPrefix(addr, "/") {
		return net.DialTimeout("unix", addr, 3*time.Second)
	}
	return net.DialTimeout("tcp", addr, 3*time.Second)
}

// run refreshes routing state, VIP and role hooks every tick.
func (r *router) run(ctx context.Context) {
	t := time.NewTicker(r.a.cfg.Tick)
	defer t.Stop()
	for {
		// VIP and hooks first: they only need local state and must not wait
		// behind a slow status call while the old leader is unreachable.
		r.manageRole(ctx)
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		r.refresh(rctx)
		cancel()
		select {
		case <-ctx.Done():
			if r.vipOn {
				r.vip(false)
			}
			return
		case <-t.C:
		}
	}
}

// refresh rebuilds the routing table from the cluster status.
func (r *router) refresh(ctx context.Context) {
	st := r.a.fsm.Get()
	if !st.Bootstrapped() {
		return
	}
	var tbl routeTable
	tbl.epoch = st.Epoch
	tbl.at = time.Now()
	// Never route writes while this agent cannot confirm the primary:
	// during a partition the old primary may be fenced already.
	if r.a.hasQuorum(st.Duration("failover_timeout") / 3) {
		if p := st.Nodes[st.Primary]; p != nil {
			tbl.primary = pgAddr(p)
		}
	}
	res, err := r.a.execute(ctx, "status", json.RawMessage("{}"))
	if err == nil {
		if m, ok := res.Result.(map[string]any); ok {
			if rows, ok := m["nodes"].([]nodeStatus); ok {
				for _, ns := range rows {
					if ns.Role == fsm.RoleReplica && ns.State == "streaming" {
						if n := st.Nodes[ns.Node]; n != nil {
							tbl.replicas = append(tbl.replicas, pgAddr(n))
						}
					}
				}
			}
		} else if raw, ok := res.Result.(json.RawMessage); ok {
			var m struct {
				Nodes []nodeStatus `json:"nodes"`
			}
			if json.Unmarshal(raw, &m) == nil {
				for _, ns := range m.Nodes {
					if ns.Role == fsm.RoleReplica && ns.State == "streaming" {
						if n := st.Nodes[ns.Node]; n != nil {
							tbl.replicas = append(tbl.replicas, pgAddr(n))
						}
					}
				}
			}
		}
	}
	r.mu.Lock()
	old := r.table.epoch
	r.table = tbl
	var stale []net.Conn
	if old != 0 && old != tbl.epoch {
		for c, e := range r.conns {
			if e != tbl.epoch {
				stale = append(stale, c)
			}
		}
	}
	r.mu.Unlock()
	// After a role change, drop connections to the old primary so that
	// clients reconnect (to the new primary) right away.
	for _, c := range stale {
		_ = c.Close()
	}
	if len(stale) > 0 {
		r.a.log.Info("listener closed connections to the old primary", "count", len(stale))
	}
}

// listen serves one listener port.
func (r *router) listen(ctx context.Context, addr string, readOnly bool) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listener %s: %w", addr, err)
	}
	kind := "read-write"
	if readOnly {
		kind = "read-only"
	}
	r.a.log.Info("listener started", "addr", addr, "mode", kind)
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			go r.serve(c, readOnly)
		}
	}()
	return nil
}

func (r *router) targets(readOnly bool) ([]string, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !readOnly {
		if r.table.primary == "" {
			return nil, r.table.epoch
		}
		return []string{r.table.primary}, r.table.epoch
	}
	// read-only: round robin over streaming standbys, then the primary
	var out []string
	n := len(r.table.replicas)
	if n > 0 {
		start := int(r.rr.Add(1) % uint64(n))
		for i := 0; i < n; i++ {
			out = append(out, r.table.replicas[(start+i)%n])
		}
	}
	if r.table.primary != "" {
		out = append(out, r.table.primary)
	}
	return out, r.table.epoch
}

func (r *router) serve(client net.Conn, readOnly bool) {
	targets, epoch := r.targets(readOnly)
	var server net.Conn
	var err error
	for _, t := range targets {
		if server, err = dialPG(t); err == nil {
			break
		}
	}
	if server == nil {
		client.Close()
		return
	}
	if !readOnly {
		r.mu.Lock()
		r.conns[client] = epoch
		r.mu.Unlock()
		defer func() {
			r.mu.Lock()
			delete(r.conns, client)
			r.mu.Unlock()
		}()
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(server, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, server); done <- struct{}{} }()
	<-done
	client.Close()
	server.Close()
}

// localRole is "primary" only when this node really serves as primary.
func (r *router) localRole() string {
	st := r.a.fsm.Get()
	if _, ok := st.Nodes[r.a.cfg.Name]; !ok || r.a.cfg.Witness {
		return ""
	}
	h := r.a.localHealth()
	if st.Primary == r.a.cfg.Name && h.Running && !h.InRecovery &&
		r.a.hasQuorum(st.Duration("failover_timeout")/3) {
		return fsm.RolePrimary
	}
	if h.Running && h.InRecovery {
		return fsm.RoleReplica
	}
	return "stopped"
}

// manageRole moves the VIP and runs the role hook on role changes.
func (r *router) manageRole(ctx context.Context) {
	role := r.localRole()
	if role == "" {
		return
	}
	if r.a.cfg.VIP != "" {
		want := role == fsm.RolePrimary
		if want != r.vipOn {
			r.vip(want)
		}
	}
	if role != r.role {
		prev := r.role
		r.role = role
		if prev != "" || role == fsm.RolePrimary {
			r.a.report(ctx, "role_"+role, fmt.Sprintf("was %s", nonEmpty(prev, "unknown")))
		}
		if r.a.cfg.OnRoleChange != "" {
			go r.hook(role)
		}
	}
}

func nonEmpty(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func (r *router) hook(role string) {
	st := r.a.fsm.Get()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.a.cfg.OnRoleChange, role, st.ClusterName, r.a.cfg.Name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.a.log.Error("role change hook failed", "role", role, "error", err, "output", strings.TrimSpace(string(out)))
		return
	}
	r.a.log.Info("role change hook done", "role", role, "output", strings.TrimSpace(string(out)))
}

// vip adds or removes the floating IP address on the configured interface.
func (r *router) vip(on bool) {
	addr, iface := r.a.cfg.VIP, r.a.cfg.VIPInterface
	ip := strings.SplitN(addr, "/", 2)[0]
	if on {
		out, err := exec.Command("ip", "addr", "add", addr, "dev", iface).CombinedOutput()
		if err != nil && !strings.Contains(string(out), "File exists") {
			if time.Since(r.vipErr) > time.Minute {
				r.vipErr = time.Now()
				r.a.log.Error("cannot add VIP (the agent needs CAP_NET_ADMIN)", "vip", addr, "dev", iface, "error", strings.TrimSpace(string(out)))
			}
			return
		}
		r.vipOn = true
		r.a.log.Info("VIP added", "vip", addr, "dev", iface)
		r.a.report(context.Background(), "vip_acquired", addr)
		// gratuitous ARP so that neighbours update their caches at once
		if !r.noArp {
			if out, err := exec.Command("arping", "-c", "3", "-A", "-I", iface, ip).CombinedOutput(); err != nil {
				r.a.log.Warn("arping failed (install iputils-arping / give CAP_NET_RAW)", "output", strings.TrimSpace(string(out)))
				r.noArp = true
			}
		}
		return
	}
	out, err := exec.Command("ip", "addr", "del", addr, "dev", iface).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "Cannot assign") {
		r.a.log.Error("cannot remove VIP", "vip", addr, "error", strings.TrimSpace(string(out)))
		return
	}
	r.vipOn = false
	r.a.log.Info("VIP removed", "vip", addr)
}

// healthStatus answers the load balancer health checks.
func (r *router) healthStatus(check string) (int, string) {
	role := r.localRole()
	h := r.a.localHealth()
	switch check {
	case "primary", "read-write":
		if role == fsm.RolePrimary {
			return 200, "primary"
		}
	case "replica", "read-only":
		if role == fsm.RoleReplica && h.WalReceiver == "streaming" {
			return 200, "replica"
		}
	case "", "alive":
		if h.Running {
			return 200, nonEmpty(role, "running")
		}
	}
	return 503, nonEmpty(role, "not a member")
}
