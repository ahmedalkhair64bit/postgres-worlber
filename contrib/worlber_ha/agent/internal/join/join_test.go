package join

import (
	"testing"
	"time"
)

func TestMintParseRoundTrip(t *testing.T) {
	tok, hash, exp, err := Mint("prod", []string{"pg1:7400", "pg2:7400"}, time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(tok)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Cluster != "prod" || len(parsed.Seeds) != 2 || !parsed.Sync || parsed.Expiry != exp {
		t.Fatalf("round trip: %+v", parsed)
	}
	if HashNonce(parsed.Nonce) != hash {
		t.Fatal("hash does not match the minted nonce")
	}
	// the stored hash must not reveal the nonce
	if hash == parsed.Nonce {
		t.Fatal("stored value equals the nonce")
	}
}

func TestParseRejects(t *testing.T) {
	for _, s := range []string{"", "nope", "wjt1.!!!", "wjt1.", "wjt1.e30"} {
		if _, err := Parse(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestCheck(t *testing.T) {
	_, hash, exp, _ := Mint("c", []string{"s:7400"}, time.Hour, false)
	// recover the nonce for the test by minting deterministically is not
	// possible; instead build a record and a matching nonce directly.
	nonce := "ab" + "00"[:0] // placeholder; use a real pair below
	_ = nonce
	// Use a known nonce/hash pair.
	n := "f0e1d2c3b4a5968778695a4b3c2d1e0ff0e1d2c3b4a5968778695a4b3c2d1e0ff"
	rec := Record{Hash: HashNonce(n), Expiry: exp, Sync: false}
	now := time.Now().Unix()

	if _, act, err := Check([]Record{rec}, n, "pg5", now); err != nil || act != "ok" {
		t.Fatalf("first use: act=%q err=%v", act, err)
	}
	// claimed by pg5: same host retries, another host is rejected
	claimed := rec
	claimed.ClaimedBy = "pg5"
	if _, act, err := Check([]Record{claimed}, n, "pg5", now); err != nil || act != "retry" {
		t.Fatalf("retry: act=%q err=%v", act, err)
	}
	if _, _, err := Check([]Record{claimed}, n, "pg6", now); err == nil {
		t.Fatal("a different host must be rejected")
	}
	// expired
	old := rec
	old.Expiry = now - 1
	if _, _, err := Check([]Record{old}, n, "pg5", now); err == nil {
		t.Fatal("expired token must be rejected")
	}
	// unknown nonce
	if _, _, err := Check([]Record{rec}, "deadbeef", "pg5", now); err == nil {
		t.Fatal("unknown nonce must be rejected")
	}
	_ = hash
}
