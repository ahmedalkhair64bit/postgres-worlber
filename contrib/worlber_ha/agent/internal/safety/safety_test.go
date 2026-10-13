package safety

import (
	"testing"
	"time"
)

func TestTimingBound(t *testing.T) {
	// For every allowed failover_timeout, the old primary's lease is
	// guaranteed to be gone before a promotion at failover_timeout.
	for to := MinFailoverTimeout; to <= 120*time.Second; to += 500 * time.Millisecond {
		if w := FenceWait(LeaseTTL(to)); w > to {
			t.Fatalf("failover_timeout %s: fence wait %s is longer", to, w)
		}
	}
	if FenceWait(LeaseTTL(6*time.Second)) <= 6*time.Second {
		t.Fatal("MinFailoverTimeout is not the real minimum")
	}
	if got := ConfirmCutoff(10 * time.Second); got <= 0 || got >= 10*time.Second {
		t.Fatalf("cutoff for 10s: %s", got)
	}
	if ConfirmCutoff(3*time.Second) != 0 {
		t.Fatal("cutoff must not go negative")
	}
}

func TestConfirm(t *testing.T) {
	base := ConfirmInput{Node: "p", NodeEpoch: 4, Running: true, Primary: "p", Epoch: 4,
		SinceVerified: 300 * time.Millisecond, FailoverTimeout: 10 * time.Second}
	if !Confirm(base) {
		t.Fatal("healthy primary must be confirmed")
	}
	cut := ConfirmCutoff(base.FailoverTimeout)
	cases := []struct {
		name string
		mod  func(*ConfirmInput)
		want bool
	}{
		{"not the primary", func(c *ConfirmInput) { c.Node = "s" }, false},
		{"old epoch", func(c *ConfirmInput) { c.NodeEpoch = 3 }, false},
		{"leadership not verified recently", func(c *ConfirmInput) { c.SinceVerified = 1500 * time.Millisecond }, false},
		{"leadership never verified", func(c *ConfirmInput) { c.SinceVerified = -1 }, false},
		{"failover decided", func(c *ConfirmInput) { c.FailingOver = true }, false},
		{"down briefly: agent may restart it", func(c *ConfirmInput) { c.Running = false; c.BadFor = cut - time.Millisecond }, true},
		{"down past the cutoff", func(c *ConfirmInput) { c.Running = false; c.BadFor = cut }, false},
		{"running again after being down", func(c *ConfirmInput) { c.BadFor = cut + time.Second }, true},
	}
	for _, c := range cases {
		in := base
		c.mod(&in)
		if got := Confirm(in); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestRenew(t *testing.T) {
	s := int64(time.Second)
	if !Renew(10*s, 9*s) || !Renew(10*s, 8*s) {
		t.Fatal("confirmation within Q must allow renewal")
	}
	if Renew(10*s, 7*s) {
		t.Fatal("confirmation older than Q must not renew")
	}
	if Renew(10*s, 0) || Renew(10*s, 11*s) {
		t.Fatal("missing or future confirmation must not renew")
	}
}

func TestPromotionAllowed(t *testing.T) {
	now := time.Unix(1000, 0)
	w := 8 * time.Second
	if ok, _ := PromotionAllowed(now, now.Add(-7*time.Second), now.Add(-time.Hour), w); ok {
		t.Fatal("promotion before the fence wait")
	}
	if ok, _ := PromotionAllowed(now, now.Add(-8*time.Second), now.Add(-time.Hour), w); !ok {
		t.Fatal("promotion after the fence wait refused")
	}
	// A new leader never saw a confirmation, but its predecessor may have
	// given one just before: wait from the moment it became leader.
	if ok, left := PromotionAllowed(now, time.Time{}, now.Add(-3*time.Second), w); ok || left != 5*time.Second {
		t.Fatalf("new leader: ok=%v left=%s", ok, left)
	}
}

func TestLeaseRoundTripAndValid(t *testing.T) {
	l := Lease{BootID: "b-1", TS: 5_000_000_000, TTL: 3 * time.Second, Term: 7, Epoch: 2}
	got, err := ParseLease(l.Format())
	if err != nil || got != l {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	if !l.Valid("b-1", l.TS+int64(3*time.Second)) {
		t.Fatal("lease valid until its TTL")
	}
	if l.Valid("b-1", l.TS+int64(3*time.Second)+1) {
		t.Fatal("expired lease accepted")
	}
	if l.Valid("b-2", l.TS) {
		t.Fatal("lease from another boot accepted")
	}
	if l.Valid("b-1", l.TS-1) {
		t.Fatal("lease from the future accepted")
	}
	for _, bad := range []string{"", "v2 b 1 1 1 1", "v1 b x 1 1 1", "v1 b 1 0 1 1", "v1 b 1 1 1"} {
		if _, err := ParseLease(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestBootClock(t *testing.T) {
	a := BootTime()
	b := BootTime()
	if a <= 0 || b < a {
		t.Fatalf("CLOCK_BOOTTIME not monotonic: %d %d", a, b)
	}
	if id, err := BootID(); err != nil || id == "" {
		t.Fatalf("boot id: %q %v", id, err)
	}
}
