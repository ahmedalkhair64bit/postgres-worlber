package agent

import (
	"testing"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
	"github.com/worlber/worlber-ha-agent/internal/pg"
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
	all := map[string]bool{"s1": true, "s2": true, "a": true, "w": true}
	cases := []struct {
		name      string
		self      string
		connected map[string]bool
		strict    bool
		want      string
	}{
		{"sync-flagged first, async as fallback", "p", all, false, `FIRST 1 ("s1", "s2", "a")`},
		{"only connected standbys in available mode", "p", map[string]bool{"s1": true}, false, `FIRST 1 ("s1")`},
		{"async standby used when no sync-flagged one is connected", "p", map[string]bool{"a": true}, false, `FIRST 1 ("a")`},
		{"nobody connected: commits do not wait", "p", nil, false, ""},
		// The regression the chaos test S2 found: the only sync-flagged
		// standby became the primary and nothing was synchronous any more.
		{"primary is the sync-flagged node", "s1", map[string]bool{"p": true, "a": true}, false, `FIRST 1 ("p", "a")`},
		{"strict lists every standby, connected or not", "p", nil, true, `FIRST 1 ("s1", "s2", "a")`},
	}
	for _, c := range cases {
		st.Config["synchronous_mode"] = "available"
		if c.strict {
			st.Config["synchronous_mode"] = "strict"
		}
		if got := SyncNames(st, c.self, c.connected); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestLastSyncKeepsRunningPrimaryReport(t *testing.T) {
	a := &Agent{beats: map[string]heartbeat{}, syncSeen: map[string]syncView{}}
	running := heartbeat{Node: "p", Epoch: 3, Health: pg.Health{Running: true, Replicas: []pg.ReplicaStat{
		{AppName: "s1", SyncState: "potential"}, {AppName: "a", SyncState: "sync"},
	}}}
	a.recordBeat(running)
	got, ok := a.lastSync("p", 3)
	if !ok || !got["a"] || got["s1"] {
		t.Fatalf("got %v %v, want a only", got, ok)
	}
	// PostgreSQL crashed: the heartbeat has no replicas and must not erase
	// what the running primary reported.
	a.recordBeat(heartbeat{Node: "p", Epoch: 3, Health: pg.Health{Running: false}})
	if got, ok := a.lastSync("p", 3); !ok || !got["a"] {
		t.Fatalf("crashed-primary heartbeat erased the sync report: %v %v", got, ok)
	}
	// A report from another epoch is not used.
	if _, ok := a.lastSync("p", 4); ok {
		t.Fatal("report from epoch 3 used for epoch 4")
	}
	// A running primary with no sync standby is a known, empty set.
	a.recordBeat(heartbeat{Node: "p", Epoch: 3, Health: pg.Health{Running: true}})
	if got, ok := a.lastSync("p", 3); !ok || len(got) != 0 {
		t.Fatalf("want known empty set, got %v %v", got, ok)
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
