package agent

import (
	"testing"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
)

func TestPickCandidate(t *testing.T) {
	ok := func(name string, sync bool, lsn uint64) Candidate {
		return Candidate{Name: name, Sync: sync, Fresh: true, Running: true, InRecovery: true, LSN: lsn}
	}
	cases := []struct {
		name    string
		cands   []Candidate
		primary uint64
		maxLag  uint64
		strict  bool
		want    string
	}{
		{"prefers sync", []Candidate{ok("b", false, 200), ok("a", true, 100)}, 200, 0, false, "a"},
		{"highest lsn", []Candidate{ok("b", false, 100), ok("c", false, 300)}, 300, 0, false, "c"},
		{"tie by name", []Candidate{ok("c", false, 100), ok("b", false, 100)}, 100, 0, false, "b"},
		{"lag filter", []Candidate{ok("b", false, 100)}, 1000, 500, false, ""},
		{"strict needs sync", []Candidate{ok("b", false, 100)}, 100, 0, true, ""},
		{"skip unreachable", []Candidate{{Name: "x", Sync: true}, ok("y", false, 1)}, 1, 0, false, "y"},
		{"skip primary-like", []Candidate{{Name: "x", Fresh: true, Running: true}}, 1, 0, false, ""},
		{"none", nil, 0, 0, false, ""},
	}
	for _, c := range cases {
		got, why := PickCandidate(c.cands, c.primary, c.maxLag, c.strict)
		if got != c.want {
			t.Errorf("%s: got %q (%s), want %q", c.name, got, why, c.want)
		}
		if got == "" && why == "" {
			t.Errorf("%s: no reason given", c.name)
		}
	}
}

func TestSyncNames(t *testing.T) {
	st := fsm.State{Primary: "p", Config: map[string]string{}, Nodes: map[string]*fsm.Node{
		"p": {Name: "p", Sync: true}, "s1": {Name: "s1", Sync: true}, "s2": {Name: "s2", Sync: true},
		"a": {Name: "a"}, "w": {Name: "w", Witness: true, Sync: true},
	}}
	if got := SyncNames(st, "p", map[string]bool{"s1": true}); got != `ANY 1 ("s1")` {
		t.Errorf("available mode: %s", got)
	}
	if got := SyncNames(st, "p", nil); got != "" {
		t.Errorf("available mode, nobody connected: %q", got)
	}
	st.Config["synchronous_mode"] = "strict"
	if got := SyncNames(st, "p", nil); got != `ANY 1 ("s1", "s2")` {
		t.Errorf("strict mode: %s", got)
	}
}

func TestHumanBytes(t *testing.T) {
	for in, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1536: "1.5 kB", 1 << 30: "1.0 GB", 1 << 40: "1.0 TB"} {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateNewOptions(t *testing.T) {
	ok := map[string]string{"max_slot_wal_keep": "50GB", "seed_max_rate": "100M", "auto_reinit": "false"}
	for k, v := range ok {
		if err := fsm.ValidateOption(k, v); err != nil {
			t.Errorf("%s=%s: %v", k, v, err)
		}
	}
	if err := fsm.ValidateOption("max_slot_wal_keep", "unlimited"); err != nil {
		t.Error(err)
	}
	bad := map[string]string{"max_slot_wal_keep": "10", "seed_max_rate": "fast", "auto_reinit": "yes"}
	for k, v := range bad {
		if err := fsm.ValidateOption(k, v); err == nil {
			t.Errorf("%s=%s accepted", k, v)
		}
	}
}
