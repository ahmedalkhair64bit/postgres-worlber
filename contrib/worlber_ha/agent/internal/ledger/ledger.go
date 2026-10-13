// Package ledger is the acknowledged-write ledger used by the chaos harness.
//
// Every write attempt gets exactly one of three outcomes:
//
//	ack      COMMIT returned OK (and no synchronous-replication cancel warning)
//	failed   an error came back before COMMIT was sent
//	unknown  COMMIT was sent but its result is not known: the connection was
//	         lost, the 15s commit timeout closed the socket, or COMMIT returned
//	         with "canceling wait for synchronous replication"
//
// Only ack rows count toward data loss.  Unknown rows may or may not exist on
// the new primary; both are correct.
package ledger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// Outcome of one write attempt.
type Outcome string

const (
	Ack     Outcome = "ack"
	Failed  Outcome = "failed"
	Unknown Outcome = "unknown"
)

// Phase is how far an attempt got before it ended.
type Phase int

const (
	// BeforeCommit: connect, BEGIN or INSERT; COMMIT was never sent.
	BeforeCommit Phase = iota
	// CommitSent: COMMIT was written to the socket.
	CommitSent
)

// SyncCancelWarning is the text PostgreSQL sends when a backend stops
// waiting for its synchronous standby; the commit is durable locally only.
const SyncCancelWarning = "canceling wait for synchronous replication"

// Classify maps how an attempt ended to its outcome.  err is the error the
// attempt ended with (nil on success); syncWarned is true when a
// SyncCancelWarning notice arrived during COMMIT.
func Classify(phase Phase, err error, syncWarned bool) Outcome {
	if phase == BeforeCommit {
		if err != nil {
			return Failed
		}
		// Ending before COMMIT without an error is a caller bug; never let
		// it count as acknowledged.
		return Unknown
	}
	if err != nil || syncWarned {
		return Unknown
	}
	return Ack
}

// IsSyncCancel reports whether a notice message is the synchronous
// replication cancel warning.
func IsSyncCancel(msg string) bool {
	return strings.Contains(msg, SyncCancelWarning)
}

// Record is one line of the ledger file (JSON Lines).
type Record struct {
	Client int     `json:"c"`
	Seq    int64   `json:"s"`
	Out    Outcome `json:"o"`
	Start  int64   `json:"t0"` // unix ms when the attempt started
	End    int64   `json:"t1"` // unix ms when the outcome was known
	Err    string  `json:"e,omitempty"`
}

// Key identifies a ledger row in the database table.
type Key struct {
	Client int
	Seq    int64
}

func (r Record) Key() Key { return Key{r.Client, r.Seq} }

// Read loads a ledger file.
func Read(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Decode(f)
}

// Decode reads JSON Lines records.  A truncated last line (the writer was
// killed mid-write) is ignored; any other bad line is an error.
func Decode(r io.Reader) ([]Record, error) {
	var out []Record
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	line := 0
	var pending error
	for sc.Scan() {
		line++
		if pending != nil {
			return nil, pending
		}
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(b, &rec); err != nil {
			pending = fmt.Errorf("ledger line %d: %w", line, err)
			continue
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

// Summary is the analysis of one run's ledger.
type Summary struct {
	Attempts int `json:"attempts"`
	Acked    int `json:"acked"`
	Failed   int `json:"failed"`
	Unknown  int `json:"unknown"`
	// MaxAckGapMS is the longest time any client went between two
	// acknowledged commits over the whole run.
	MaxAckGapMS int64 `json:"max_ack_gap_ms"`
	// RTOMS is set when a fault time is given: for each client, the
	// longest gap between two consecutive acks where the later ack comes
	// after the fault (a fault is not instantaneous: writes can still
	// succeed for a moment after it is injected); the maximum over
	// clients.  -1 means some client never had an ack after the fault.
	RTOMS        *int64 `json:"rto_ms,omitempty"`
	Unrecovered  []int  `json:"unrecovered_clients,omitempty"`
	DuplicateKey int    `json:"duplicate_keys"`
	// Latency of acknowledged attempts (BEGIN..COMMIT), nearest rank.
	AckP50MS int64 `json:"ack_p50_ms"`
	AckP99MS int64 `json:"ack_p99_ms"`
}

// percentile returns the nearest-rank p-th percentile of sorted values.
func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := (p*len(sorted) + 99) / 100 // ceil(p/100 * n)
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}

// Summarize computes counts, the largest ack gap and, when faultAt > 0, the
// per-run RTO as defined in the design (max over clients).
func Summarize(recs []Record, faultAt int64) Summary {
	var s Summary
	acks := map[int][]int64{}
	var lat []int64
	seen := map[Key]bool{}
	for _, r := range recs {
		s.Attempts++
		if seen[r.Key()] {
			s.DuplicateKey++
		}
		seen[r.Key()] = true
		switch r.Out {
		case Ack:
			s.Acked++
			acks[r.Client] = append(acks[r.Client], r.End)
			lat = append(lat, r.End-r.Start)
		case Failed:
			s.Failed++
		default:
			s.Unknown++
		}
	}
	clients := make([]int, 0, len(acks))
	for c, ts := range acks {
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		for i := 1; i < len(ts); i++ {
			if g := ts[i] - ts[i-1]; g > s.MaxAckGapMS {
				s.MaxAckGapMS = g
			}
		}
		clients = append(clients, c)
	}
	sort.Ints(clients)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	s.AckP50MS, s.AckP99MS = percentile(lat, 50), percentile(lat, 99)
	if faultAt > 0 {
		var rto int64
		for _, c := range clients {
			ts := acks[c]
			recovered := false
			prev := faultAt // a client with no ack before the fault counts from it
			for _, t := range ts {
				if t > faultAt {
					recovered = true
					if g := t - prev; g > rto {
						rto = g
					}
				}
				prev = t
			}
			if !recovered {
				s.Unrecovered = append(s.Unrecovered, c)
			}
		}
		if len(s.Unrecovered) > 0 {
			rto = -1
		}
		s.RTOMS = &rto
	}
	return s
}

// Verdict compares the ledger with the rows found on the new primary and,
// optionally, on an isolated copy of the old primary.
type Verdict struct {
	Acked        int   `json:"acked"`
	AckedMissing int   `json:"acked_missing"`
	MissingKeys  []Key `json:"missing_sample,omitempty"`
	// Rows on the old primary copy that are not on the new primary,
	// grouped by their ledger outcome.  Only acked ones are divergence.
	OldOnlyAcked   int `json:"old_only_acked"`
	OldOnlyUnknown int `json:"old_only_unknown"`
	OldOnlyFailed  int `json:"old_only_failed"`
	// Rows in a database that the ledger never wrote: always a bug.
	NotInLedger int  `json:"not_in_ledger"`
	Pass        bool `json:"pass"`
}

const maxSample = 20

// Verify checks that every acknowledged row is on the new primary, and that
// rows only on the old primary (if given) were never acknowledged.
func Verify(recs []Record, newRows map[Key]bool, oldRows map[Key]bool) Verdict {
	var v Verdict
	byKey := make(map[Key]Outcome, len(recs))
	for _, r := range recs {
		// A key is written by one attempt only (seq never repeats), but be
		// conservative: ack wins.
		if prev, ok := byKey[r.Key()]; !ok || prev != Ack {
			byKey[r.Key()] = r.Out
		}
	}
	keys := make([]Key, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Client != keys[j].Client {
			return keys[i].Client < keys[j].Client
		}
		return keys[i].Seq < keys[j].Seq
	})
	for _, k := range keys {
		if byKey[k] != Ack {
			continue
		}
		v.Acked++
		if !newRows[k] {
			v.AckedMissing++
			if len(v.MissingKeys) < maxSample {
				v.MissingKeys = append(v.MissingKeys, k)
			}
		}
	}
	for k := range newRows {
		if _, ok := byKey[k]; !ok {
			v.NotInLedger++
		}
	}
	for k := range oldRows {
		if newRows[k] {
			continue
		}
		switch o, ok := byKey[k]; {
		case !ok:
			v.NotInLedger++
		case o == Ack:
			v.OldOnlyAcked++
		case o == Failed:
			v.OldOnlyFailed++
		default:
			v.OldOnlyUnknown++
		}
	}
	v.Pass = v.AckedMissing == 0 && v.OldOnlyAcked == 0 && v.NotInLedger == 0
	return v
}
