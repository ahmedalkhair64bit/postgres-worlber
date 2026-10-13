package ledger

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassify(t *testing.T) {
	lost := errors.New("unexpected EOF")
	cases := []struct {
		name  string
		phase Phase
		err   error
		warn  bool
		want  Outcome
	}{
		{"insert error", BeforeCommit, lost, false, Failed},
		{"no error before commit is never ack", BeforeCommit, nil, false, Unknown},
		{"commit ok", CommitSent, nil, false, Ack},
		{"commit lost", CommitSent, lost, false, Unknown},
		{"sync cancel warning", CommitSent, nil, true, Unknown},
		{"sync cancel and error", CommitSent, lost, true, Unknown},
	}
	for _, c := range cases {
		if got := Classify(c.phase, c.err, c.warn); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestIsSyncCancel(t *testing.T) {
	msg := "canceling wait for synchronous replication due to user request"
	if !IsSyncCancel(msg) {
		t.Fatal("warning not recognised")
	}
	if IsSyncCancel("some other notice") {
		t.Fatal("false positive")
	}
}

func TestDecodeTruncatedLastLine(t *testing.T) {
	in := `{"c":1,"s":1,"o":"ack","t0":1,"t1":2}
{"c":1,"s":2,"o":"unk`
	recs, err := Decode(strings.NewReader(in))
	if err != nil || len(recs) != 1 {
		t.Fatalf("got %v, %v; want 1 record, nil", recs, err)
	}
	bad := `{"c":1,"s":1,"o":"ack","t0":1,"t1":2}
garbage
{"c":1,"s":3,"o":"ack","t0":1,"t1":2}`
	if _, err := Decode(strings.NewReader(bad)); err == nil {
		t.Fatal("a bad line in the middle must be an error")
	}
}

func TestSummarizeRTO(t *testing.T) {
	recs := []Record{
		{Client: 1, Seq: 1, Out: Ack, End: 1000},
		{Client: 1, Seq: 2, Out: Ack, End: 2000}, // last before fault
		{Client: 1, Seq: 6, Out: Ack, End: 2550}, // fault not effective yet
		{Client: 1, Seq: 3, Out: Unknown, End: 2600},
		{Client: 1, Seq: 4, Out: Failed, End: 5000},
		{Client: 1, Seq: 5, Out: Ack, End: 14000}, // first after fault
		{Client: 2, Seq: 1, Out: Ack, End: 2400},
		{Client: 2, Seq: 2, Out: Ack, End: 11000},
	}
	s := Summarize(recs, 2500)
	if s.Acked != 6 || s.Failed != 1 || s.Unknown != 1 || s.Attempts != 8 {
		t.Fatalf("counts wrong: %+v", s)
	}
	if s.RTOMS == nil || *s.RTOMS != 11450 {
		t.Fatalf("rto = %v, want 11450", *s.RTOMS)
	}
	if s.MaxAckGapMS != 11450 {
		t.Fatalf("max gap = %d", s.MaxAckGapMS)
	}
}

func TestSummarizeUnrecovered(t *testing.T) {
	recs := []Record{
		{Client: 1, Seq: 1, Out: Ack, End: 1000},
		{Client: 2, Seq: 1, Out: Ack, End: 1000},
		{Client: 2, Seq: 2, Out: Ack, End: 3000},
	}
	s := Summarize(recs, 2000)
	if s.RTOMS == nil || *s.RTOMS != -1 || len(s.Unrecovered) != 1 || s.Unrecovered[0] != 1 {
		t.Fatalf("got %+v", s)
	}
	if Summarize(recs, 0).RTOMS != nil {
		t.Fatal("no fault time must leave RTO unset")
	}
}

func TestVerify(t *testing.T) {
	recs := []Record{
		{Client: 1, Seq: 1, Out: Ack},
		{Client: 1, Seq: 2, Out: Ack},
		{Client: 1, Seq: 3, Out: Unknown},
		{Client: 1, Seq: 4, Out: Failed},
	}
	k := func(s int64) Key { return Key{1, s} }

	// all acked rows present; unknown row absent is fine
	v := Verify(recs, map[Key]bool{k(1): true, k(2): true}, nil)
	if !v.Pass || v.Acked != 2 {
		t.Fatalf("expected pass: %+v", v)
	}

	// acked row missing on the new primary
	v = Verify(recs, map[Key]bool{k(1): true}, nil)
	if v.Pass || v.AckedMissing != 1 || v.MissingKeys[0] != k(2) {
		t.Fatalf("expected missing ack: %+v", v)
	}

	// old primary has rows the new one lacks: unknown/failed are fine, ack is divergence
	v = Verify(recs, map[Key]bool{k(1): true, k(2): true},
		map[Key]bool{k(1): true, k(3): true, k(4): true})
	if !v.Pass || v.OldOnlyUnknown != 1 || v.OldOnlyFailed != 1 {
		t.Fatalf("unknown/failed old-only rows must pass: %+v", v)
	}
	v = Verify(recs, map[Key]bool{k(1): true},
		map[Key]bool{k(1): true, k(2): true})
	if v.Pass || v.OldOnlyAcked != 1 {
		t.Fatalf("acked old-only row is divergence: %+v", v)
	}

	// a row nobody wrote is always a failure
	v = Verify(recs, map[Key]bool{k(1): true, k(2): true, k(99): true}, nil)
	if v.Pass || v.NotInLedger != 1 {
		t.Fatalf("row not in ledger must fail: %+v", v)
	}
}

func TestReportOutcome(t *testing.T) {
	if o, _ := ReportOutcome(nil); o != "completed" {
		t.Fatal(o)
	}
	for _, code := range []string{"40001", "40P01", "57014"} {
		if o, c := ReportOutcome(&pgconn.PgError{Code: code}); o != "cancelled" || c != code {
			t.Fatalf("%s: %s", code, o)
		}
	}
	if o, c := ReportOutcome(&pgconn.PgError{Code: "42P01"}); o != "error" || c != "42P01" {
		t.Fatal(o)
	}
	if o, _ := ReportOutcome(errors.New("dial tcp: refused")); o != "error" {
		t.Fatal(o)
	}
}
