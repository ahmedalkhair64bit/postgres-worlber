package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func applyCmd(t *testing.T, f *FSM, c Command) error {
	t.Helper()
	b, _ := json.Marshal(c)
	if r := f.Apply(&raft.Log{Data: b}); r != nil {
		return r.(error)
	}
	return nil
}

func TestLifecycle(t *testing.T) {
	f := New()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(applyCmd(t, f, Command{Op: "bootstrap", ClusterName: "prod", SystemID: "1", Node: &Node{Name: "pg1"}}))
	if err := applyCmd(t, f, Command{Op: "bootstrap", ClusterName: "x", Node: &Node{Name: "pg9"}}); err == nil {
		t.Fatal("second bootstrap must fail")
	}
	must(applyCmd(t, f, Command{Op: "add_node", Node: &Node{Name: "pg2.local", Sync: true}}))
	must(applyCmd(t, f, Command{Op: "add_node", Node: &Node{Name: "w1", Witness: true}}))
	st := f.Get()
	if st.Primary != "pg1" || st.Epoch != 1 || len(st.Nodes) != 3 {
		t.Fatalf("unexpected state %+v", st)
	}
	if st.Nodes["pg2.local"].SlotName != "worlber_ha_pg2_local" {
		t.Fatalf("slot name %q", st.Nodes["pg2.local"].SlotName)
	}
	if st.RoleOf("w1") != RoleWitness || st.RoleOf("pg2.local") != RoleReplica {
		t.Fatal("roles")
	}
	if err := applyCmd(t, f, Command{Op: "set_primary", Name: "w1"}); err == nil {
		t.Fatal("witness cannot be primary")
	}
	if err := applyCmd(t, f, Command{Op: "set_primary", Name: "pg2.local", ExpectedEpoch: 7}); err == nil {
		t.Fatal("stale epoch must be rejected")
	}
	must(applyCmd(t, f, Command{Op: "set_primary", Name: "pg2.local", ExpectedEpoch: 1, Reason: "switchover"}))
	if err := applyCmd(t, f, Command{Op: "remove_node", Name: "pg2.local"}); err == nil {
		t.Fatal("cannot remove primary")
	}
	must(applyCmd(t, f, Command{Op: "remove_node", Name: "pg1"}))
	must(applyCmd(t, f, Command{Op: "set_config", Key: "failover_timeout", Value: "10s"}))
	if err := applyCmd(t, f, Command{Op: "set_config", Key: "failover_timeout", Value: "1s"}); err == nil {
		t.Fatal("too small timeout accepted")
	}
	st = f.Get()
	if st.Epoch != 2 || st.Primary != "pg2.local" || st.Duration("failover_timeout").Seconds() != 10 {
		t.Fatalf("unexpected %+v", st)
	}

	// snapshot / restore round trip
	snap, _ := f.Snapshot()
	sink := &memSink{}
	must(snap.Persist(sink))
	g := New()
	must(g.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
	if g.Get().Primary != "pg2.local" || len(g.Get().Events) != len(st.Events) {
		t.Fatal("restore mismatch")
	}
}

func TestParseBytes(t *testing.T) {
	for in, want := range map[string]uint64{"0": 0, "16MB": 16 << 20, "1GB": 1 << 30, "512kB": 512 << 10, "100": 100} {
		got, err := ParseBytes(in)
		if err != nil || got != want {
			t.Errorf("ParseBytes(%q) = %d, %v", in, got, err)
		}
	}
	if _, err := ParseBytes("lots"); err == nil {
		t.Error("expected error")
	}
}

type memSink struct{ bytes.Buffer }

func (m *memSink) ID() string    { return "mem" }
func (m *memSink) Cancel() error { return nil }
func (m *memSink) Close() error  { return nil }
