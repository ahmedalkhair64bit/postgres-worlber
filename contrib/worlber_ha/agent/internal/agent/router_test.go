package agent

import (
	"net"
	"testing"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
)

func TestROEligible(t *testing.T) {
	lag := func(n int64) *int64 { return &n }
	rep := func(state string, l *int64) nodeStatus {
		return nodeStatus{Role: fsm.RoleReplica, State: state, LagBytes: l}
	}
	cases := []struct {
		name   string
		ns     nodeStatus
		maxLag uint64
		want   bool
	}{
		{"streaming, no limit", rep("streaming", lag(1<<30)), 0, true},
		{"streaming within limit", rep("streaming", lag(100)), 1000, true},
		{"streaming over limit", rep("streaming", lag(2000)), 1000, false},
		{"not streaming", rep("recovering", lag(0)), 0, false},
		{"unknown lag passes when no limit", rep("streaming", nil), 0, true},
		{"unknown lag passes even with a limit", rep("streaming", nil), 1000, true},
		{"primary is not an RO target", nodeStatus{Role: fsm.RolePrimary, State: "running"}, 0, false},
	}
	for _, c := range cases {
		if got := roEligible(c.ns, c.maxLag); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestROTargetsFallback(t *testing.T) {
	r := &router{conns: map[net.Conn]uint64{}}
	r.table = routeTable{primary: "P", replicas: []string{"a", "b"}, roFallback: true}
	// read-only: replicas first (round robin), then the primary as fallback
	got, _ := r.targets(true)
	if len(got) != 3 || got[len(got)-1] != "P" {
		t.Fatalf("with fallback: %v", got)
	}
	r.table.roFallback = false
	got, _ = r.targets(true)
	if len(got) != 2 {
		t.Fatalf("no fallback with replicas present must not include the primary: %v", got)
	}
	// but if no replica qualifies, fall back to the primary anyway
	r.table.replicas = nil
	got, _ = r.targets(true)
	if len(got) != 1 || got[0] != "P" {
		t.Fatalf("no replicas: must use the primary: %v", got)
	}
}
