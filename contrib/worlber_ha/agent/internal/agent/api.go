package agent

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// reply is the envelope returned to the SQL extension and to peers.
type reply struct {
	OK       bool     `json:"ok"`
	Result   any      `json:"result,omitempty"`
	Error    string   `json:"error,omitempty"`
	Hint     string   `json:"hint,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// opError carries an optional hint for the DBA.
type opError struct{ msg, hint string }

func (e *opError) Error() string { return e.msg }

func errHint(hint, format string, args ...any) error {
	return &opError{msg: fmt.Sprintf(format, args...), hint: hint}
}

// opResult is what every operation returns.
type opResult struct {
	Result   any
	Warnings []string
}

func writeReply(w http.ResponseWriter, res opResult, err error) {
	w.Header().Set("Content-Type", "application/json")
	r := reply{OK: err == nil, Result: res.Result, Warnings: res.Warnings}
	if err != nil {
		r.Error = err.Error()
		if oe, ok := err.(*opError); ok {
			r.Hint = oe.hint
		}
	}
	_ = json.NewEncoder(w).Encode(r)
}

// localHandler serves the unix socket used by the SQL extension.
// POST /v1/<op> with a JSON object of arguments.
func (a *Agent) localHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		op := strings.TrimPrefix(r.URL.Path, "/v1/")
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if len(bytes.TrimSpace(body)) == 0 {
			body = []byte("{}")
		}
		res, err := a.execute(r.Context(), op, body)
		writeReply(w, res, err)
	})
	return mux
}

// peerHandler serves the agent-to-agent API, authenticated by the token.
func (a *Agent) peerHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/peer/info", func(w http.ResponseWriter, r *http.Request) {
		writeReply(w, opResult{Result: a.info()}, nil)
	})
	mux.HandleFunc("/peer/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var hb heartbeat
		if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
			writeReply(w, opResult{}, err)
			return
		}
		if !a.isLeader() {
			writeReply(w, opResult{}, fmt.Errorf("not leader"))
			return
		}
		a.recordBeat(hb)
		writeReply(w, opResult{}, nil)
	})
	mux.HandleFunc("/peer/op/", func(w http.ResponseWriter, r *http.Request) {
		op := strings.TrimPrefix(r.URL.Path, "/peer/op/")
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if !a.isLeader() {
			writeReply(w, opResult{}, fmt.Errorf("this agent is not the Raft leader"))
			return
		}
		res, err := a.leaderOp(r.Context(), op, body)
		writeReply(w, res, err)
	})
	mux.HandleFunc("/peer/stop_for_switchover", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Epoch uint64 `json:"epoch"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		lsn, err := a.stopForSwitchover(r.Context(), req.Epoch)
		writeReply(w, opResult{Result: map[string]any{"checkpoint_lsn": lsn}}, err)
	})
	mux.HandleFunc("/peer/leave", func(w http.ResponseWriter, r *http.Request) {
		// Sent by the leader after remove_node: remember to reset Raft state
		// and exit; the service manager restarts the agent as a fresh node.
		// PostgreSQL is left running.
		if a.raft.State() == raft.Leader {
			writeReply(w, opResult{}, fmt.Errorf("refusing to leave while Raft leader"))
			return
		}
		if err := os.WriteFile(filepath.Join(a.cfg.DataDir, leftMarker), nil, 0600); err != nil {
			writeReply(w, opResult{}, err)
			return
		}
		writeReply(w, opResult{}, nil)
		a.log.Warn("removed from the cluster; exiting so that the agent restarts as a new node")
		go func() {
			time.Sleep(500 * time.Millisecond)
			a.stop()
		}()
	})
	mux.HandleFunc("/peer/log", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Lines int    `json:"lines"`
			Level string `json:"level"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeReply(w, opResult{Result: a.logs.Tail(clampLines(req.Lines), req.Level)}, nil)
	})
	mux.HandleFunc("/peer/release_hold", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		a.holdUntil = time.Time{}
		a.mu.Unlock()
		writeReply(w, opResult{}, nil)
	})
	// Health checks for load balancers (HAProxy, GCP/AWS load balancers):
	// GET /health/primary -> 200 only on the primary, /health/replica -> 200
	// only on a streaming standby.  They need no token and expose nothing.
	outer := http.NewServeMux()
	outer.HandleFunc("/health", a.health)
	outer.HandleFunc("/health/", a.health)
	outer.Handle("/", a.requireToken(mux))
	return outer
}

func (a *Agent) health(w http.ResponseWriter, r *http.Request) {
	code, role := a.router.healthStatus(strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/health"), "/"))
	w.WriteHeader(code)
	fmt.Fprintln(w, role)
}

func clampLines(n int) int {
	if n <= 0 {
		return 100
	}
	if n > 2000 {
		return 2000
	}
	return n
}

func (a *Agent) requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-Worlber-HA-Token")
		if subtle.ConstantTimeCompare([]byte(got), []byte(a.cfg.Token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			writeReply(w, opResult{}, fmt.Errorf("invalid cluster token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// peerPost sends a request to another agent and decodes the result.
func (a *Agent) peerPost(ctx context.Context, addr, path string, in any, out any) error {
	return a.peerPostTimeout(ctx, addr, path, in, out, 15*time.Second)
}

func (a *Agent) peerPostTimeout(ctx context.Context, addr, path string, in any, out any, timeout time.Duration) error {
	var body io.Reader
	switch v := in.(type) {
	case nil:
		body = bytes.NewReader([]byte("{}"))
	case []byte:
		body = bytes.NewReader(v)
	case json.RawMessage:
		body = bytes.NewReader(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("X-Worlber-HA-Token", a.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	client := *a.http
	client.Timeout = timeout
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("agent %s unreachable: %w", addr, err)
	}
	defer resp.Body.Close()
	var rep struct {
		OK       bool            `json:"ok"`
		Result   json.RawMessage `json:"result"`
		Error    string          `json:"error"`
		Hint     string          `json:"hint"`
		Warnings []string        `json:"warnings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		return fmt.Errorf("agent %s: bad reply: %w", addr, err)
	}
	if !rep.OK {
		return &opError{msg: rep.Error, hint: rep.Hint}
	}
	switch o := out.(type) {
	case nil:
	case *reply:
		o.OK, o.Error, o.Hint, o.Warnings = rep.OK, rep.Error, rep.Hint, rep.Warnings
		o.Result = rep.Result
	default:
		if len(rep.Result) > 0 {
			return json.Unmarshal(rep.Result, out)
		}
	}
	return nil
}

// nodeInfo is returned by /peer/info and used by add_node.
type nodeInfo struct {
	Name        string `json:"name"`
	RaftAddr    string `json:"raft_addr"`
	APIAddr     string `json:"api_addr"`
	PGHost      string `json:"pg_host"`
	PGPort      int    `json:"pg_port"`
	Witness     bool   `json:"witness"`
	ClusterName string `json:"cluster_name"`
	HasRaft     bool   `json:"has_raft"`
	Leader      bool   `json:"leader"`
	Primary     string `json:"primary"`
	Epoch       uint64 `json:"epoch"`
}

func (a *Agent) info() nodeInfo {
	// leaderReady is set once a barrier confirmed that every committed
	// entry is applied on this leader.
	a.mu.Lock()
	leader := a.isLeader() && a.leaderReady
	a.mu.Unlock()
	st := a.fsm.Get()
	cfg := a.raft.GetConfiguration()
	hasRaft := cfg.Error() == nil && len(cfg.Configuration().Servers) > 0
	return nodeInfo{
		Name: a.cfg.Name, RaftAddr: a.cfg.RaftAddr, APIAddr: a.cfg.APIAddr,
		PGHost: a.cfg.PGAdvertise, PGPort: a.cfg.PG.Port, Witness: a.cfg.Witness,
		ClusterName: st.ClusterName, HasRaft: hasRaft,
		Leader: leader, Primary: st.Primary, Epoch: st.Epoch,
	}
}
