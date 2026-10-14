// Package join implements the one-time join token that lets a new node be
// added with a single command instead of copying the cluster secret by hand.
//
// A token is minted on an existing member:
//
//	wjt1.<base64url( {cluster, seeds, nonce, exp, sync} )>
//
// Only the SHA-256 of the nonce is stored in Raft, with its expiry and the
// sync flag, so the token is single-use and the stored copy cannot be
// replayed.  The new node POSTs the nonce to /peer/join; the leader checks
// the hash, marks it consumed, and returns the cluster secret.  The secret
// crosses the network in cleartext once (the same exposure as the per-request
// token header); encryption is out of scope by decision.
package join

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const prefix = "wjt1."

// Token is what the new node receives (and decodes to find the seeds).
type Token struct {
	Cluster string   `json:"cluster"`
	Seeds   []string `json:"seeds"` // api addresses of existing members
	Nonce   string   `json:"nonce"` // hex, 32 bytes
	Expiry  int64    `json:"exp"`   // unix seconds
	Sync    bool     `json:"sync"`
}

// Mint creates a token and returns it with the hash to store in Raft.
func Mint(cluster string, seeds []string, ttl time.Duration, sync bool) (tokenStr, nonceHash string, expiry int64, err error) {
	var b [32]byte
	if _, err = rand.Read(b[:]); err != nil {
		return "", "", 0, err
	}
	nonce := hex.EncodeToString(b[:])
	expiry = time.Now().Add(ttl).Unix()
	t := Token{Cluster: cluster, Seeds: seeds, Nonce: nonce, Expiry: expiry, Sync: sync}
	j, err := json.Marshal(t)
	if err != nil {
		return "", "", 0, err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(j), HashNonce(nonce), expiry, nil
}

// Parse decodes a token string.
func Parse(s string) (Token, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, prefix) {
		return Token{}, fmt.Errorf("not a worlber_ha join token")
	}
	j, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(s, prefix))
	if err != nil {
		return Token{}, fmt.Errorf("malformed join token: %w", err)
	}
	var t Token
	if err := json.Unmarshal(j, &t); err != nil {
		return Token{}, fmt.Errorf("malformed join token: %w", err)
	}
	if t.Cluster == "" || len(t.Nonce) != 64 || len(t.Seeds) == 0 {
		return Token{}, fmt.Errorf("incomplete join token")
	}
	return t, nil
}

// HashNonce returns the hex SHA-256 of a nonce, the form stored in Raft.
func HashNonce(nonce string) string {
	h := sha256.Sum256([]byte(nonce))
	return hex.EncodeToString(h[:])
}

// EqualHash compares two hashes in constant time.
func EqualHash(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// Record is a stored, unconsumed-or-claimed token in the FSM.
type Record struct {
	Hash      string `json:"hash"`
	Expiry    int64  `json:"exp"`
	Sync      bool   `json:"sync"`
	ClaimedBy string `json:"claimed_by,omitempty"` // host that consumed it
	Name      string `json:"name,omitempty"`       // assigned node name
}

// Expired reports whether the record is past its expiry at now (unix s).
func (r Record) Expired(now int64) bool { return now > r.Expiry }

// Check decides what /peer/join should do with a nonce from host.
// It returns the matching record and an action: "ok" (first valid use),
// "retry" (same host already consumed it, within expiry: return the same
// reply, so a crash during init is safe), or an error reason.
func Check(records []Record, nonce, host string, now int64) (Record, string, error) {
	hash := HashNonce(nonce)
	for _, r := range records {
		if !EqualHash(r.Hash, hash) {
			continue
		}
		switch {
		case r.Expired(now):
			return r, "", fmt.Errorf("join token has expired")
		case r.ClaimedBy == "":
			return r, "ok", nil
		case r.ClaimedBy == host:
			return r, "retry", nil
		default:
			return r, "", fmt.Errorf("join token was already used by another host")
		}
	}
	return Record{}, "", fmt.Errorf("unknown or already-pruned join token")
}
