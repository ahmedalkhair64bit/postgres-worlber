// Package fsm holds the replicated cluster state of a worlber_ha cluster.
//
// Every change to the cluster topology (who is primary, which nodes exist,
// cluster options) is a Command committed through Raft and applied here on
// every agent, so all agents agree on the desired state.  Volatile health
// information (LSNs, lag, heartbeats) is deliberately NOT stored here.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const maxEvents = 500

// Node roles as stored in the FSM.
const (
	RolePrimary = "primary"
	RoleReplica = "replica"
	RoleWitness = "witness"
)

// Cluster options and their defaults.
var DefaultConfig = map[string]string{
	"failover_timeout":        "30s",
	"async_failover":          "never",
	"ro_max_lag":              "0",
	"ro_fallback":             "primary",
	"maximum_lag_on_failover": "16MB",
	"synchronous_mode":        "available", // available | strict
	"paused":                  "false",
	// WAL the primary keeps for a disconnected standby before giving up on
	// it (max_slot_wal_keep_size); protects the primary's disk
	"max_slot_wal_keep": "100GB",
	// re-seed a standby automatically when the WAL it needs is gone
	"auto_reinit": "true",
	// pg_basebackup --max-rate when seeding (e.g. '100M'); empty = unlimited
	"seed_max_rate": "",
}

// Node is one member of the cluster.
type Node struct {
	Name     string `json:"name"`
	PGHost   string `json:"pg_host"`
	PGPort   int    `json:"pg_port"`
	APIAddr  string `json:"api_addr"`
	RaftAddr string `json:"raft_addr"`
	Sync     bool   `json:"sync"`
	Witness  bool   `json:"witness"`
	AddedAt  int64  `json:"added_at"`
	SlotName string `json:"slot_name"`
	// FenceLoaded is true once the node's agent reported the fence worker
	// loaded (worlber_ha in shared_preload_libraries).  Committed by the
	// leader as it changes, so a new leader still knows it when this node's
	// agent is gone: automatic failover away from a primary that never had
	// a fence worker could leave two writable primaries.
	FenceLoaded bool `json:"fence_loaded,omitempty"`
}

// Event is an entry in the cluster history (failovers, switchovers...).
type Event struct {
	Time   int64  `json:"time"`
	Kind   string `json:"kind"`
	Node   string `json:"node"`
	Epoch  uint64 `json:"epoch"`
	Detail string `json:"detail"`
}

// State is the full replicated state.
type State struct {
	ClusterName  string            `json:"cluster_name"`
	SystemID     string            `json:"system_id"`
	Primary      string            `json:"primary"`
	Epoch        uint64            `json:"epoch"`
	ReplUser     string            `json:"repl_user"`
	ReplPassword string            `json:"repl_password"`
	Nodes        map[string]*Node  `json:"nodes"`
	Config       map[string]string `json:"config"`
	Events       []Event           `json:"events"`
	// ActualSync is the set of standbys the running primary last reported as
	// synchronous (sync_state sync/quorum).  It is the only set that is
	// guaranteed to hold every acknowledged commit, so automatic failover
	// promotes from it.  Committed by the leader as it changes, so a new
	// leader still knows it.
	ActualSync []string `json:"actual_sync,omitempty"`
	// FencingGate is true once every member runs an agent that understands
	// leases, confirmations and actual_sync.  Until then the cluster keeps
	// the older behaviour (quorum-loss fencing only, best-eligible failover)
	// so a rolling upgrade never self-fences a healthy node or diverges the
	// FSM with ops an old agent cannot apply.  A cluster created at this
	// version opens the gate at bootstrap.
	FencingGate bool `json:"fencing_gate,omitempty"`
	// JoinTokens are the outstanding one-time join tokens (only the nonce
	// hash is kept).  Expired or consumed-and-old entries are pruned.
	JoinTokens []JoinToken `json:"join_tokens,omitempty"`
}

// JoinToken is a stored one-time join token.
type JoinToken struct {
	Hash      string `json:"hash"`
	Expiry    int64  `json:"exp"`
	Sync      bool   `json:"sync"`
	ClaimedBy string `json:"claimed_by,omitempty"`
}

// Command is a state change committed through Raft.
type Command struct {
	Op string `json:"op"`

	// bootstrap
	ClusterName  string `json:"cluster_name,omitempty"`
	SystemID     string `json:"system_id,omitempty"`
	ReplUser     string `json:"repl_user,omitempty"`
	ReplPassword string `json:"repl_password,omitempty"`

	Node *Node `json:"node,omitempty"`

	// set_primary
	Name          string `json:"name,omitempty"`
	ExpectedEpoch uint64 `json:"expected_epoch,omitempty"`
	Reason        string `json:"reason,omitempty"`

	// set_sync
	Sync bool `json:"sync,omitempty"`

	// actual_sync
	SyncNodes []string `json:"sync_nodes,omitempty"`

	// fence_state (Name, FenceLoaded)
	FenceLoaded bool `json:"fence_loaded,omitempty"`

	// open_gate
	GateMembers []string `json:"gate_members,omitempty"`
	// bootstrap: set by fencing-aware agents.  A bootstrap entry written by
	// an older agent lacks it, so replaying an old log keeps the gate closed
	// exactly as the older agents see it.
	OpenGate bool `json:"open_gate,omitempty"`

	// join_token / consume_join_token
	TokenHash string `json:"token_hash,omitempty"`
	Expiry    int64  `json:"expiry,omitempty"`
	ClaimedBy string `json:"claimed_by,omitempty"`

	// event
	Kind   string `json:"kind,omitempty"`
	Detail string `json:"detail,omitempty"`

	// set_config
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`

	Time int64 `json:"time"`
}

// FSM implements raft.FSM.
type FSM struct {
	mu    sync.RWMutex
	state State

	// notify is signalled (non-blocking) after each applied command.
	notify chan struct{}
}

func New() *FSM {
	return &FSM{
		state:  emptyState(),
		notify: make(chan struct{}, 1),
	}
}

func emptyState() State {
	return State{Nodes: map[string]*Node{}, Config: map[string]string{}}
}

// Changed returns a channel that receives a value after state changes.
func (f *FSM) Changed() <-chan struct{} { return f.notify }

// Snapshot returns a deep copy of the current state.
func (f *FSM) Get() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.clone()
}

func (s State) clone() State {
	b, _ := json.Marshal(s)
	var c State
	_ = json.Unmarshal(b, &c)
	if c.Nodes == nil {
		c.Nodes = map[string]*Node{}
	}
	if c.Config == nil {
		c.Config = map[string]string{}
	}
	return c
}

// Bootstrapped reports whether a cluster has been created.
func (s State) Bootstrapped() bool { return s.ClusterName != "" }

// Option returns a cluster option or its default.
func (s State) Option(key string) string {
	if v, ok := s.Config[key]; ok {
		return v
	}
	return DefaultConfig[key]
}

// Duration parses a duration option, falling back to the default.
func (s State) Duration(key string) time.Duration {
	if d, err := time.ParseDuration(s.Option(key)); err == nil {
		return d
	}
	d, _ := time.ParseDuration(DefaultConfig[key])
	return d
}

// Paused reports whether automatic actions are suspended.
func (s State) Paused() bool { return s.Option("paused") == "true" }

// RoleOf returns the role a node should have.
func (s State) RoleOf(name string) string {
	n, ok := s.Nodes[name]
	if !ok {
		return ""
	}
	if n.Witness {
		return RoleWitness
	}
	if s.Primary == name {
		return RolePrimary
	}
	return RoleReplica
}

// SortedNodes returns nodes ordered by name.
func (s State) SortedNodes() []*Node {
	out := make([]*Node, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Apply implements raft.FSM.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return fmt.Errorf("bad command: %w", err)
	}
	f.mu.Lock()
	err := f.state.apply(c)
	f.mu.Unlock()
	if err == nil {
		select {
		case f.notify <- struct{}{}:
		default:
		}
	}
	return err
}

// apply mutates the state.  It must be deterministic.
func (s *State) apply(c Command) error {
	switch c.Op {
	case "bootstrap":
		if s.Bootstrapped() {
			return fmt.Errorf("cluster %q already exists", s.ClusterName)
		}
		if c.Node == nil {
			return fmt.Errorf("bootstrap requires a node")
		}
		s.ClusterName = c.ClusterName
		s.SystemID = c.SystemID
		s.ReplUser = c.ReplUser
		s.ReplPassword = c.ReplPassword
		c.Node.AddedAt = c.Time
		c.Node.SlotName = SlotName(c.Node.Name)
		s.Nodes[c.Node.Name] = c.Node
		s.Primary = c.Node.Name
		s.Epoch = 1
		s.FencingGate = c.OpenGate
		s.event(c.Time, "cluster_created", c.Node.Name, "cluster "+c.ClusterName+" created")

	case "add_node":
		if !s.Bootstrapped() {
			return fmt.Errorf("no cluster")
		}
		if c.Node == nil || c.Node.Name == "" {
			return fmt.Errorf("add_node requires a node")
		}
		if _, ok := s.Nodes[c.Node.Name]; ok {
			return fmt.Errorf("node %q is already a member", c.Node.Name)
		}
		c.Node.AddedAt = c.Time
		c.Node.SlotName = SlotName(c.Node.Name)
		s.Nodes[c.Node.Name] = c.Node
		kind := "node_added"
		if c.Node.Witness {
			kind = "witness_added"
		}
		s.event(c.Time, kind, c.Node.Name, fmt.Sprintf("sync=%v", c.Node.Sync))

	case "remove_node":
		if _, ok := s.Nodes[c.Name]; !ok {
			return fmt.Errorf("node %q is not a member", c.Name)
		}
		if s.Primary == c.Name {
			return fmt.Errorf("cannot remove the primary %q; switch over first", c.Name)
		}
		delete(s.Nodes, c.Name)
		s.event(c.Time, "node_removed", c.Name, "")

	case "set_primary":
		n, ok := s.Nodes[c.Name]
		if !ok {
			return fmt.Errorf("node %q is not a member", c.Name)
		}
		if n.Witness {
			return fmt.Errorf("node %q is a witness", c.Name)
		}
		if c.ExpectedEpoch != 0 && c.ExpectedEpoch != s.Epoch {
			return fmt.Errorf("epoch changed (expected %d, have %d)", c.ExpectedEpoch, s.Epoch)
		}
		old := s.Primary
		s.Primary = c.Name
		s.Epoch++
		detail := fmt.Sprintf("old primary %s", old)
		if c.Detail != "" {
			detail += "; " + c.Detail
		}
		s.event(c.Time, c.Reason, c.Name, detail)

	case "set_sync":
		n, ok := s.Nodes[c.Name]
		if !ok {
			return fmt.Errorf("node %q is not a member", c.Name)
		}
		n.Sync = c.Sync
		s.event(c.Time, "sync_changed", c.Name, fmt.Sprintf("sync=%v", c.Sync))

	case "set_config":
		if err := ValidateOption(c.Key, c.Value); err != nil {
			return err
		}
		s.Config[c.Key] = c.Value
		s.event(c.Time, "config_changed", "", c.Key+"="+c.Value)

	case "event":
		// Something an agent did locally (seeding, rewind, promotion,
		// fencing...), recorded for the history in worlber_ha.events.
		if c.Kind == "" {
			return fmt.Errorf("event requires a kind")
		}
		s.event(c.Time, c.Kind, c.Name, c.Detail)

	case "open_gate":
		if s.FencingGate {
			return nil // already open
		}
		// Deterministic guard: the member set the leader checked must match
		// the committed set, so a node added between the check and the commit
		// cannot be left behind an open gate.
		have := make(map[string]bool, len(s.Nodes))
		for n := range s.Nodes {
			have[n] = true
		}
		if len(c.GateMembers) != len(have) {
			return fmt.Errorf("open_gate member set does not match the cluster")
		}
		for _, n := range c.GateMembers {
			if !have[n] {
				return fmt.Errorf("open_gate names unknown member %q", n)
			}
		}
		s.FencingGate = true
		s.event(c.Time, "fencing_gate_open", "", "all members run the fencing agent")

	case "join_token":
		// prune expired/consumed tokens, then add the new one
		kept := s.JoinTokens[:0]
		for _, jt := range s.JoinTokens {
			if jt.Expiry >= c.Time && jt.ClaimedBy == "" {
				kept = append(kept, jt)
			}
		}
		s.JoinTokens = append(kept, JoinToken{Hash: c.TokenHash, Expiry: c.Expiry, Sync: c.Sync})
		s.event(c.Time, "join_token_created", "", "a one-time join token was issued")

	case "consume_join_token":
		found := false
		for i := range s.JoinTokens {
			if s.JoinTokens[i].Hash == c.TokenHash {
				if s.JoinTokens[i].ClaimedBy != "" && s.JoinTokens[i].ClaimedBy != c.ClaimedBy {
					return fmt.Errorf("join token already used")
				}
				s.JoinTokens[i].ClaimedBy = c.ClaimedBy
				found = true
			}
		}
		if !found {
			return fmt.Errorf("unknown join token")
		}

	case "actual_sync":
		for _, n := range c.SyncNodes {
			if _, ok := s.Nodes[n]; !ok {
				return fmt.Errorf("actual_sync names unknown node %q", n)
			}
		}
		s.ActualSync = append([]string(nil), c.SyncNodes...)
		s.event(c.Time, "actual_sync", "", strings.Join(c.SyncNodes, ", "))

	case "fence_state":
		n, ok := s.Nodes[c.Name]
		if !ok {
			return fmt.Errorf("fence_state names unknown node %q", c.Name)
		}
		n.FenceLoaded = c.FenceLoaded
		if c.FenceLoaded {
			s.event(c.Time, "fence_loaded", c.Name, "fence worker loaded")
		} else {
			s.event(c.Time, "fence_not_loaded", c.Name, "worlber_ha is not in shared_preload_libraries; no automatic failover away from this node while it is primary")
		}

	default:
		return fmt.Errorf("unknown op %q", c.Op)
	}
	return nil
}

func (s *State) event(t int64, kind, node, detail string) {
	s.Events = append(s.Events, Event{Time: t, Kind: kind, Node: node, Epoch: s.Epoch, Detail: detail})
	if len(s.Events) > maxEvents {
		s.Events = s.Events[len(s.Events)-maxEvents:]
	}
}

// ValidateOption checks a cluster option.
func ValidateOption(key, value string) error {
	switch key {
	case "failover_timeout":
		d, err := time.ParseDuration(value)
		if err != nil || d < 2*time.Second {
			return fmt.Errorf("failover_timeout must be a duration >= 2s (e.g. '30s')")
		}
	case "maximum_lag_on_failover":
		if _, err := ParseBytes(value); err != nil {
			return err
		}
	case "async_failover":
		if value != "never" && value != "within_lag" {
			return fmt.Errorf("async_failover must be 'never' or 'within_lag'")
		}
	case "ro_max_lag":
		if _, err := ParseBytes(value); err != nil {
			return err
		}
	case "ro_fallback":
		if value != "primary" && value != "none" {
			return fmt.Errorf("ro_fallback must be 'primary' or 'none'")
		}
	case "wal_archive_dir":
		// validated more strictly in the agent (validateArchiveDir)
	case "archive_keep":
		// a duration like 7d / 168h; checked at prune time
	case "synchronous_mode":
		if value != "available" && value != "strict" {
			return fmt.Errorf("synchronous_mode must be 'available' or 'strict'")
		}
	case "paused", "auto_reinit":
		if value != "true" && value != "false" {
			return fmt.Errorf("%s must be 'true' or 'false'", key)
		}
	case "max_slot_wal_keep":
		if value != "unlimited" {
			if n, err := ParseBytes(value); err != nil || n < 1<<20 {
				return fmt.Errorf("max_slot_wal_keep must be a size >= 1MB (e.g. '100GB') or 'unlimited'")
			}
		}
	case "seed_max_rate":
		if value != "" && !rateRe.MatchString(value) {
			return fmt.Errorf("seed_max_rate must be like '50M' or '512k' (per second), or '' for unlimited")
		}
	default:
		return fmt.Errorf("unknown option %q", key)
	}
	return nil
}

var rateRe = regexp.MustCompile(`^[0-9]+[kM]$`)

// ParseBytes parses sizes like "16MB", "1GB", "0", "1048576".
func ParseBytes(v string) (uint64, error) {
	var n uint64
	var unit string
	if _, err := fmt.Sscanf(v, "%d%s", &n, &unit); err != nil {
		if _, err2 := fmt.Sscanf(v, "%d", &n); err2 != nil {
			return 0, fmt.Errorf("invalid size %q (use e.g. '16MB')", v)
		}
		return n, nil
	}
	switch unit {
	case "B", "":
		return n, nil
	case "kB", "KB":
		return n << 10, nil
	case "MB":
		return n << 20, nil
	case "GB":
		return n << 30, nil
	case "TB":
		return n << 40, nil
	}
	return 0, fmt.Errorf("invalid size unit in %q", v)
}

// SlotName derives a valid replication slot name from a node name.
func SlotName(node string) string {
	b := []byte("worlber_ha_")
	for _, r := range []byte(node) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b = append(b, r)
		case r >= 'A' && r <= 'Z':
			b = append(b, r+('a'-'A'))
		default:
			b = append(b, '_')
		}
	}
	if len(b) > 63 {
		b = b[:63]
	}
	return string(b)
}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{state: f.Get()}, nil
}

// Restore implements raft.FSM.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	st := emptyState()
	if err := json.NewDecoder(rc).Decode(&st); err != nil {
		return err
	}
	if st.Nodes == nil {
		st.Nodes = map[string]*Node{}
	}
	if st.Config == nil {
		st.Config = map[string]string{}
	}
	f.mu.Lock()
	f.state = st
	f.mu.Unlock()
	select {
	case f.notify <- struct{}{}:
	default:
	}
	return nil
}

type snapshot struct{ state State }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
