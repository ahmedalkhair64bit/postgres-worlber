// Package safety holds the pure decision rules that keep a worlber_ha
// cluster from running two primaries.  Everything here is deterministic and
// has no I/O beyond reading clocks in BootTime, so the rules are unit tested
// on every pull request.
//
// The lease protocol:
//
//   - The Raft leader confirms "you are the primary" in its reply to a
//     heartbeat (Confirm).  The primary's agent stamps its lease with the
//     CLOCK_BOOTTIME at which it SENT that heartbeat, so a slow reply can
//     never make a lease look newer than the leader's view.
//   - The agent rewrites the lease while its last confirmation is at most Q
//     old (Renew).  The fence worker inside PostgreSQL stops the server once
//     the lease is older than its TTL; the agent's fencing goroutine does the
//     same and escalates to SIGKILL.
//   - The leader promotes a standby only FenceWait after the last
//     confirmation it gave (PromotionAllowed), and gives no confirmation
//     after it decided to fail over.  By then the old primary's lease has
//     expired and its PostgreSQL has been stopped.
package safety

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Q is how long a confirmation lets the primary keep renewing its lease.
const Q = 2 * time.Second

// CheckInterval is how often the fence worker reads the lease.
const CheckInterval = 500 * time.Millisecond

// ShutdownMargin covers the fence worker's fast shutdown and clock-rate
// differences between nodes.
const ShutdownMargin = 2 * time.Second

// MinFailoverTimeout is the smallest failover_timeout for which
// FenceWait(LeaseTTL(t)) <= t holds.
const MinFailoverTimeout = 7 * time.Second

// LeaseTTL is the lease lifetime for a failover_timeout.
func LeaseTTL(failoverTimeout time.Duration) time.Duration {
	return failoverTimeout / 3
}

// FenceWait is how long after its last confirmation of the old primary the
// leader must wait before a standby may be promoted.
func FenceWait(ttl time.Duration) time.Duration {
	return Q + ttl + CheckInterval + ShutdownMargin
}

// ConfirmCutoff is how long the leader keeps confirming a primary whose
// PostgreSQL is reported down, so its agent can restart it (crash
// recovery) without a failover.  After the cutoff the lease runs out and a
// failover at failover_timeout is safe.
func ConfirmCutoff(failoverTimeout time.Duration) time.Duration {
	c := failoverTimeout - FenceWait(LeaseTTL(failoverTimeout))
	if c < 0 {
		return 0
	}
	return c
}

// ConfirmInput is what the leader knows when a heartbeat arrives.
type ConfirmInput struct {
	Node            string
	NodeEpoch       uint64 // epoch in the heartbeat
	Running         bool   // PostgreSQL running on the node
	Primary         string // FSM primary
	Epoch           uint64 // FSM epoch
	SinceVerified   time.Duration
	FailingOver     bool          // leader decided to replace this primary
	BadFor          time.Duration // how long the leader has seen the primary unhealthy (0 = healthy)
	FailoverTimeout time.Duration
}

// Confirm decides whether the leader confirms a node as the primary.
func Confirm(in ConfirmInput) bool {
	switch {
	case in.Node != in.Primary, in.NodeEpoch != in.Epoch:
		return false
	case in.SinceVerified < 0 || in.SinceVerified > time.Second:
		return false // not sure we are still the leader
	case in.FailingOver:
		return false
	case !in.Running && in.BadFor >= ConfirmCutoff(in.FailoverTimeout):
		return false
	}
	return true
}

// Renew reports whether the agent may write a lease now: it holds a
// confirmation at most Q old (both CLOCK_BOOTTIME nanoseconds).
func Renew(nowNS, confirmedNS int64) bool {
	return confirmedNS > 0 && nowNS >= confirmedNS && time.Duration(nowNS-confirmedNS) <= Q
}

// PromotionAllowed reports whether the fence wait has passed.  lastConfirm
// is the last time the leader confirmed the old primary (zero if never);
// leaderSince is when this leader took office (a previous leader may have
// confirmed it until shortly before).
func PromotionAllowed(now, lastConfirm, leaderSince time.Time, wait time.Duration) (bool, time.Duration) {
	from := lastConfirm
	if leaderSince.After(from) {
		from = leaderSince
	}
	left := wait - now.Sub(from)
	return left <= 0, left
}

// Lease is the content of $PGDATA/worlber_ha.lease.
type Lease struct {
	BootID string
	TS     int64 // CLOCK_BOOTTIME ns of the confirmation
	TTL    time.Duration
	Term   uint64
	Epoch  uint64
}

// LeaseFile is the lease's name inside PGDATA.
const LeaseFile = "worlber_ha.lease"

// Format renders the lease line read by fence.c.
func (l Lease) Format() string {
	return fmt.Sprintf("v1 %s %d %d %d %d\n", l.BootID, l.TS, l.TTL.Milliseconds(), l.Term, l.Epoch)
}

// ParseLease parses a lease line.
func ParseLease(s string) (Lease, error) {
	f := strings.Fields(s)
	if len(f) != 6 || f[0] != "v1" {
		return Lease{}, fmt.Errorf("not a v1 lease")
	}
	var l Lease
	var err error
	l.BootID = f[1]
	if l.TS, err = strconv.ParseInt(f[2], 10, 64); err != nil {
		return Lease{}, err
	}
	ms, err := strconv.ParseInt(f[3], 10, 64)
	if err != nil || ms <= 0 {
		return Lease{}, fmt.Errorf("bad ttl")
	}
	l.TTL = time.Duration(ms) * time.Millisecond
	if l.Term, err = strconv.ParseUint(f[4], 10, 64); err != nil {
		return Lease{}, err
	}
	if l.Epoch, err = strconv.ParseUint(f[5], 10, 64); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// Valid mirrors fence.c: the lease belongs to this boot, is not in the
// future and is younger than its TTL.
func (l Lease) Valid(bootID string, nowNS int64) bool {
	return l.BootID == bootID && l.TS <= nowNS && time.Duration(nowNS-l.TS) <= l.TTL
}

// BootTime returns CLOCK_BOOTTIME in nanoseconds.
func BootTime() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0
	}
	return ts.Nano()
}

// BootID returns the kernel boot id.
func BootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
