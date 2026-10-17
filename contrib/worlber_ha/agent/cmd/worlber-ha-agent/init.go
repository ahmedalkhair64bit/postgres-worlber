package main

// `worlber-ha-agent init --join <token>` adds a new node with one command:
// it redeems a one-time join token for the cluster secret, writes the token
// file and agent.env, and prints the commands to start the agent and finish
// the join.  It does not itself start systemd or run initdb, so it is safe to
// run and easy to see what it will do; pass --start to run the agent inline
// for a container or a quick test.

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/join"
)

func runInit(args []string) {
	host, _ := os.Hostname()
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	var (
		joinTok  = fs.String("join", "", "one-time join token from SELECT worlber_ha.join_token()")
		joinFile = fs.String("join-file", "", "read the join token from a file (- for stdin), keeping it out of shell history")
		name     = fs.String("name", host, "unique node name")
		confDir  = fs.String("conf-dir", "/etc/worlber_ha", "directory for token and agent.env")
		pgData   = fs.String("pgdata", env("PGDATA", ""), "PostgreSQL data directory")
		pgBin    = fs.String("pgbin", env("WORLBER_HA_PGBIN", ""), "directory with pg_ctl, pg_basebackup, pg_rewind")
		adv      = fs.String("advertise-host", host, "host other nodes use to reach this node")
		apiPort  = fs.Int("api-port", 7400, "agent API port")
		raftPort = fs.Int("raft-port", 7401, "Raft port")
		pgPort   = fs.Int("pg-port", 5432, "local PostgreSQL port")
		dryRun   = fs.Bool("dry-run", false, "print what would be written, change nothing")
	)
	fs.Parse(args)

	tok := strings.TrimSpace(*joinTok)
	if *joinFile != "" {
		var b []byte
		var err error
		if *joinFile == "-" {
			b, err = io.ReadAll(os.Stdin)
		} else {
			b, err = os.ReadFile(*joinFile)
		}
		if err != nil {
			fatal("reading the join token: %v", err)
		}
		tok = strings.TrimSpace(string(b))
	}
	if tok == "" {
		fatal("a join token is required (--join, or --join-file to keep it out of shell history)")
	}
	parsed, err := join.Parse(tok)
	if err != nil {
		fatal("%v", err)
	}
	apiAddr := net.JoinHostPort(*adv, strconv.Itoa(*apiPort))
	raftAddr := net.JoinHostPort(*adv, strconv.Itoa(*raftPort))

	if *dryRun {
		fmt.Printf("worlber-ha-agent init (dry run)\n  cluster: %s\n  node:    %s\n  api:     %s\n  raft:    %s\n  seeds:   %s\n  would write token and agent.env under %s\n",
			parsed.Cluster, *name, apiAddr, raftAddr, strings.Join(parsed.Seeds, ", "), *confDir)
		return
	}

	secret, sync, err := redeem(parsed, joinRequestOut{
		Nonce: parsed.Nonce, Name: *name, APIAddr: apiAddr, RaftAddr: raftAddr,
		PGHost: *adv, PGPort: *pgPort, Sync: parsed.Sync,
	})
	if err != nil {
		fatal("redeeming the join token: %v", err)
	}
	fmt.Printf("worlber-ha-agent: joined cluster %q as %q (synchronous=%v)\n", parsed.Cluster, *name, sync)

	envBody := buildEnv(map[string]string{
		"WORLBER_HA_NAME":       *name,
		"WORLBER_HA_TOKEN_FILE": filepath.Join(*confDir, "token"),
		"WORLBER_HA_API_ADDR":   apiAddr,
		"WORLBER_HA_RAFT_ADDR":  raftAddr,
		"WORLBER_HA_API_BIND":   net.JoinHostPort("0.0.0.0", strconv.Itoa(*apiPort)),
		"WORLBER_HA_RAFT_BIND":  net.JoinHostPort("0.0.0.0", strconv.Itoa(*raftPort)),
		"WORLBER_HA_PG_HOST":    *adv,
		"WORLBER_HA_PGBIN":      *pgBin,
		"PGDATA":                *pgData,
	})
	tokenPath := filepath.Join(*confDir, "token")
	envPath := filepath.Join(*confDir, "agent.env")
	if err := os.MkdirAll(*confDir, 0750); err != nil {
		fatal("creating %s: %v", *confDir, err)
	}
	if err := os.WriteFile(tokenPath, []byte(secret+"\n"), 0600); err != nil {
		fatal("writing %s: %v", tokenPath, err)
	}
	if err := os.WriteFile(envPath, []byte(envBody), 0640); err != nil {
		fatal("writing %s: %v", envPath, err)
	}
	fmt.Printf("wrote %s and %s\n", tokenPath, envPath)
	fmt.Printf(`
Next:
  1. start the agent:      systemctl enable --now worlber-ha-agent
  2. on any member, add this node from psql:
         SELECT worlber_ha.add_node('%s', sync => %v);
     (or it is added automatically if an operator script calls add_node)
`, *adv, sync)
}

// joinRequestOut mirrors the agent's joinRequest for the redemption POST.
type joinRequestOut struct {
	Nonce    string `json:"nonce"`
	Name     string `json:"name"`
	APIAddr  string `json:"api_addr"`
	RaftAddr string `json:"raft_addr"`
	PGHost   string `json:"pg_host"`
	PGPort   int    `json:"pg_port"`
	Sync     bool   `json:"sync"`
}

// redeem POSTs /peer/join to each seed until one accepts, returning the
// cluster secret.  Plain HTTP: the secret crosses the wire once in cleartext,
// the same exposure as the per-request token header (encryption is out of
// scope by decision).
func redeem(t join.Token, req joinRequestOut) (secret string, sync bool, err error) {
	body, _ := json.Marshal(req)
	client := &http.Client{Timeout: 10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{}}}
	var lastErr error
	for _, seed := range t.Seeds {
		resp, e := client.Post("http://"+seed+"/peer/join", "application/json", bytes.NewReader(body))
		if e != nil {
			lastErr = e
			continue
		}
		var rep struct {
			OK     bool `json:"ok"`
			Result struct {
				Token string `json:"token"`
				Sync  bool   `json:"sync"`
			} `json:"result"`
			Error string `json:"error"`
		}
		dec := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&rep)
		resp.Body.Close()
		if dec != nil {
			lastErr = dec
			continue
		}
		if !rep.OK {
			// a definite rejection (bad/used/expired token) is final
			return "", false, fmt.Errorf("%s", rep.Error)
		}
		return rep.Result.Token, rep.Result.Sync, nil
	}
	return "", false, fmt.Errorf("no seed accepted the join (last error: %v)", lastErr)
}

func buildEnv(kv map[string]string) string {
	// stable order for readability
	order := []string{"WORLBER_HA_NAME", "WORLBER_HA_TOKEN_FILE", "WORLBER_HA_API_ADDR",
		"WORLBER_HA_RAFT_ADDR", "WORLBER_HA_API_BIND", "WORLBER_HA_RAFT_BIND",
		"WORLBER_HA_PG_HOST", "WORLBER_HA_PGBIN", "PGDATA"}
	var b strings.Builder
	b.WriteString("# written by worlber-ha-agent init\n")
	for _, k := range order {
		if v := kv[k]; v != "" {
			fmt.Fprintf(&b, "%s=%s\n", k, v)
		}
	}
	return b.String()
}
