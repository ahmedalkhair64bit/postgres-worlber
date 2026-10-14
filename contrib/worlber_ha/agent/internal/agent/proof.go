package agent

// Live proof (W5): worlber_ha.drill() performs a planned switchover while
// measuring it, and proves no acknowledged write was lost:
//
//  1. the current primary writes a canary row (worlber_ha_internal.canary)
//     and its agent returns the row id;
//  2. the switchover runs;
//  3. the drill waits until the new primary accepts writes (RTO);
//  4. the new primary's agent checks that the canary row is there (RPO 0).
//
// The outcome is recorded as a drill_completed / drill_failed event whose
// detail is JSON, so worlber_ha.proof can show it.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// drillReceipt is stored as the JSON detail of the drill event.
type drillReceipt struct {
	Kind          string `json:"kind"`
	OldPrimary    string `json:"old_primary"`
	NewPrimary    string `json:"new_primary"`
	RTOms         int64  `json:"rto_ms"`
	CanaryID      int64  `json:"canary_id"`
	CanaryPresent bool   `json:"canary_present"`
	Error         string `json:"error,omitempty"`
}

// localCanaryWrite inserts a canary row on this node's PostgreSQL.
func (a *Agent) localCanaryWrite(ctx context.Context) (int64, error) {
	var id int64
	err := a.pg.QueryRow(ctx, "INSERT INTO worlber_ha_internal.canary DEFAULT VALUES RETURNING id", nil, &id)
	return id, err
}

// localCanaryHas reports whether this node has the canary row with id.
func (a *Agent) localCanaryHas(ctx context.Context, id int64) (bool, error) {
	var ok bool
	err := a.pg.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM worlber_ha_internal.canary WHERE id = $1)", []any{id}, &ok)
	return ok, err
}

// canaryHandlers serves /peer/canary_write and /peer/canary_check.
func (a *Agent) canaryHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/peer/canary_write", func(w http.ResponseWriter, r *http.Request) {
		id, err := a.localCanaryWrite(r.Context())
		writeReply(w, opResult{Result: map[string]int64{"id": id}}, err)
	})
	mux.HandleFunc("/peer/canary_check", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID int64 `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		ok, err := a.localCanaryHas(r.Context(), req.ID)
		writeReply(w, opResult{Result: map[string]bool{"present": ok}}, err)
	})
}

// drill runs on the leader.
func (a *Agent) drill(ctx context.Context, kind string) (opResult, error) {
	if kind == "" {
		kind = "switchover"
	}
	if kind != "switchover" {
		return opResult{}, fmt.Errorf("drill kind %q is not supported (use 'switchover')", kind)
	}
	st := a.fsm.Get()
	old := st.Nodes[st.Primary]
	if old == nil {
		return opResult{}, fmt.Errorf("no primary")
	}
	rc := drillReceipt{Kind: kind, OldPrimary: st.Primary}
	fail := func(err error) (opResult, error) {
		rc.Error = err.Error()
		b, _ := json.Marshal(rc)
		a.report(ctx, "drill_failed", string(b))
		return opResult{Result: rc}, err
	}

	// 1. canary on the current primary
	var w struct {
		ID int64 `json:"id"`
	}
	if err := a.peerPost(ctx, old.APIAddr, "/peer/canary_write", nil, &w); err != nil {
		return fail(fmt.Errorf("canary write on %s: %w", st.Primary, err))
	}
	rc.CanaryID = w.ID

	// 2. switchover
	t0 := time.Now()
	res, err := a.switchover(ctx, "")
	if err != nil {
		return fail(fmt.Errorf("switchover: %w", err))
	}
	if m, ok := res.Result.(map[string]any); ok {
		if np, ok := m["new_primary"].(string); ok {
			rc.NewPrimary = np
		}
	}
	if rc.NewPrimary == "" {
		rc.NewPrimary = a.fsm.Get().Primary
	}

	// 3. wait until the new primary accepts writes
	deadline := time.Now().Add(2 * time.Minute)
	for {
		hb, fresh := a.beat(rc.NewPrimary)
		if fresh && hb.Health.Running && !hb.Health.InRecovery {
			break
		}
		if time.Now().After(deadline) {
			return fail(fmt.Errorf("new primary %s not writable after 2 minutes", rc.NewPrimary))
		}
		time.Sleep(200 * time.Millisecond)
	}
	rc.RTOms = time.Since(t0).Milliseconds()

	// 4. the canary must be on the new primary
	np := a.fsm.Get().Nodes[rc.NewPrimary]
	var c struct {
		Present bool `json:"present"`
	}
	if err := a.peerPost(ctx, np.APIAddr, "/peer/canary_check", map[string]int64{"id": rc.CanaryID}, &c); err != nil {
		return fail(fmt.Errorf("canary check on %s: %w", rc.NewPrimary, err))
	}
	rc.CanaryPresent = c.Present
	b, _ := json.Marshal(rc)
	if !c.Present {
		a.report(ctx, "drill_failed", string(b))
		return opResult{Result: rc}, fmt.Errorf("canary row %d missing on the new primary %s", rc.CanaryID, rc.NewPrimary)
	}
	a.report(ctx, "drill_completed", string(b))
	return opResult{Result: rc}, nil
}
