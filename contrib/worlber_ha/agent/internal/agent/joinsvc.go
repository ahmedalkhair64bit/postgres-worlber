package agent

// Server side of the one-time join token (W10): the join_token op mints a
// token on the leader, and /peer/join lets a new node redeem it for the
// cluster secret.  /peer/join is the only endpoint that is not authenticated
// by the cluster token (the new node does not have it yet); it is protected
// by the single-use token and a per-IP rate limit.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
	"github.com/worlber/worlber-ha-agent/internal/join"
)

// joinToken (leader op) mints a token and commits its hash.
func (a *Agent) joinToken(ctx context.Context, args json.RawMessage) (opResult, error) {
	st := a.fsm.Get()
	if !st.FencingGate {
		return opResult{}, errHint("Finish upgrading every node first (worlber_ha.status shows the members).",
			"join tokens need the fencing gate open (all members on the current version)")
	}
	var req struct {
		TTLSeconds int  `json:"ttl_seconds"`
		Sync       bool `json:"sync"`
	}
	_ = json.Unmarshal(args, &req)
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = time.Hour
	}
	// seeds: the api addresses of the current members (any can serve /peer/join)
	var seeds []string
	for _, n := range st.SortedNodes() {
		if n.APIAddr != "" {
			seeds = append(seeds, n.APIAddr)
		}
	}
	tok, hash, expiry, err := join.Mint(st.ClusterName, seeds, ttl, req.Sync)
	if err != nil {
		return opResult{}, err
	}
	if err := a.apply(fsm.Command{Op: "join_token", TokenHash: hash, Expiry: expiry, Sync: req.Sync}); err != nil {
		return opResult{}, err
	}
	return opResult{Result: map[string]any{"token": tok, "expiry": expiry}}, nil
}

// rateLimiter allows n events per window per key (source IP).
type rateLimiter struct {
	mu     sync.Mutex
	hits   map[string][]time.Time
	n      int
	window time.Duration
}

func newRateLimiter(n int, window time.Duration) *rateLimiter {
	return &rateLimiter{hits: map[string][]time.Time{}, n: n, window: window}
}

func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	cut := now.Add(-rl.window)
	kept := rl.hits[key][:0]
	for _, t := range rl.hits[key] {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= rl.n {
		rl.hits[key] = kept
		return false
	}
	rl.hits[key] = append(kept, now)
	return true
}

// joinRequest is what a new node sends to /peer/join.
type joinRequest struct {
	Nonce    string `json:"nonce"`
	Name     string `json:"name"`
	APIAddr  string `json:"api_addr"`
	RaftAddr string `json:"raft_addr"`
	PGHost   string `json:"pg_host"`
	PGPort   int    `json:"pg_port"`
	Sync     bool   `json:"sync"`
}

// handleJoin serves /peer/join on the outer (unauthenticated) mux.
func (a *Agent) handleJoin(w http.ResponseWriter, r *http.Request) {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if ip == "" {
		ip = r.RemoteAddr
	}
	if !a.joinLimiter.allow(ip) {
		a.report(r.Context(), "join_rejected", "rate limit from "+ip)
		http.Error(w, "too many join attempts", http.StatusTooManyRequests)
		return
	}
	var req joinRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeReply(w, opResult{}, fmt.Errorf("bad join request"))
		return
	}
	res, err := a.processJoin(r.Context(), req, ip)
	if err != nil {
		a.report(r.Context(), "join_rejected", fmt.Sprintf("%s from %s: %v", req.Name, ip, err))
	}
	writeReply(w, res, err)
}

// processJoin validates the token (forwarding to the leader if needed),
// consumes it, adds the node, and returns the cluster secret.
func (a *Agent) processJoin(ctx context.Context, req joinRequest, ip string) (opResult, error) {
	// forward to the leader if this node is not it
	if !a.isLeader() {
		addr, err := a.leaderAPI()
		if err != nil {
			return opResult{}, fmt.Errorf("no leader to accept the join: %w", err)
		}
		var rep reply
		if err := a.peerPostTimeout(ctx, addr, "/peer/op/join_consume", mustJSON(req), &rep, 2*time.Minute); err != nil {
			return opResult{}, err
		}
		if !rep.OK {
			return opResult{}, fmt.Errorf("%s", rep.Error)
		}
		return opResult{Result: rep.Result}, nil
	}
	return a.consumeJoin(ctx, req)
}

// consumeJoin runs on the leader: check the token, add the node, return the
// cluster secret.  Retry-safe: the same host re-presenting a consumed token
// within its expiry gets the secret again (add_node is idempotent).
func (a *Agent) consumeJoin(ctx context.Context, req joinRequest) (opResult, error) {
	if req.Nonce == "" || req.Name == "" || req.APIAddr == "" || req.RaftAddr == "" {
		return opResult{}, fmt.Errorf("join request missing fields")
	}
	st := a.fsm.Get()
	recs := make([]join.Record, len(st.JoinTokens))
	for i, jt := range st.JoinTokens {
		recs[i] = join.Record{Hash: jt.Hash, Expiry: jt.Expiry, Sync: jt.Sync, ClaimedBy: jt.ClaimedBy}
	}
	rec, action, err := join.Check(recs, req.Nonce, req.Name, time.Now().Unix())
	if err != nil {
		return opResult{}, err
	}
	if action == "ok" {
		if err := a.apply(fsm.Command{Op: "consume_join_token", TokenHash: rec.Hash, ClaimedBy: req.Name}); err != nil {
			return opResult{}, err
		}
		a.report(ctx, "node_joined", req.Name+" redeemed a one-time join token")
	}
	// Return the cluster secret so the new agent can start; the node is added
	// to the cluster by the normal add_node path once it is up and reachable
	// (init does that next, authenticated with this secret).
	return opResult{Result: map[string]any{
		"cluster": st.ClusterName,
		"token":   a.cfg.Token,
		"sync":    req.Sync || rec.Sync,
	}}, nil
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
