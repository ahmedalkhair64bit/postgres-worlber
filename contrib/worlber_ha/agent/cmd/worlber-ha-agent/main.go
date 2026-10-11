// worlber-ha-agent is the per-node control plane of worlber_ha.
//
// Run one agent next to every PostgreSQL server (and optionally one witness
// agent without PostgreSQL), then manage the cluster from psql with
// CREATE EXTENSION worlber_ha.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/agent"
	"github.com/worlber/worlber-ha-agent/internal/pg"
)

var version = "1.0.0"

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func main() {
	host, _ := os.Hostname()
	var (
		name        = flag.String("name", env("WORLBER_HA_NAME", host), "unique node name")
		dataDir     = flag.String("data-dir", env("WORLBER_HA_DATA_DIR", "/var/lib/worlber_ha"), "directory for Raft state")
		raftAddr    = flag.String("raft-addr", env("WORLBER_HA_RAFT_ADDR", host+":7401"), "host:port for Raft traffic (bound and advertised)")
		apiAddr     = flag.String("api-addr", env("WORLBER_HA_API_ADDR", host+":7400"), "host:port for the agent peer API (bound and advertised)")
		raftBind    = flag.String("raft-bind", env("WORLBER_HA_RAFT_BIND", ""), "listen address for Raft (default: --raft-addr), e.g. 0.0.0.0:7401")
		apiBind     = flag.String("api-bind", env("WORLBER_HA_API_BIND", ""), "listen address for the peer API (default: --api-addr), e.g. 0.0.0.0:7400")
		token       = flag.String("token", env("WORLBER_HA_TOKEN", ""), "shared cluster secret (same on every node)")
		tokenFile   = flag.String("token-file", env("WORLBER_HA_TOKEN_FILE", ""), "file containing the cluster secret")
		witness     = flag.Bool("witness", false, "run as a witness (Raft vote only, no PostgreSQL)")
		pgData      = flag.String("pgdata", env("PGDATA", ""), "PostgreSQL data directory")
		pgBin       = flag.String("pgbin", env("WORLBER_HA_PGBIN", ""), "directory with pg_ctl, pg_basebackup, pg_rewind")
		pgSock      = flag.String("pg-socket-dir", env("WORLBER_HA_PG_SOCKET_DIR", "/var/run/postgresql"), "unix socket directory of the local PostgreSQL")
		pgPort      = flag.Int("pg-port", envInt("WORLBER_HA_PG_PORT", 5432), "port of the local PostgreSQL")
		pgUser      = flag.String("pg-user", env("WORLBER_HA_PG_USER", "postgres"), "superuser the agent connects as (local socket)")
		pgAdv       = flag.String("pg-advertise-host", env("WORLBER_HA_PG_HOST", host), "host other nodes use to reach this PostgreSQL")
		pgLog       = flag.String("pg-log", "", "log file passed to pg_ctl start (default: <data-dir>/postgresql.log)")
		socket      = flag.String("socket", "", "unix socket for the SQL extension (default: <pg-socket-dir>/.s.WORLBER_HA.<pg-port>)")
		reinit      = flag.Bool("reinit-pgdata", false, "allow replacing a PGDATA that belongs to another database system")
		tick        = flag.Duration("tick", time.Second, "health check interval")
		logLevel    = flag.String("log-level", "info", "trace|debug|info|warn|error")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *showVersion {
		fmt.Println("worlber-ha-agent", version)
		return
	}
	if *tokenFile != "" {
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			fatal("reading token file: %v", err)
		}
		*token = strings.TrimSpace(string(b))
	}
	if *token == "" {
		fatal("a cluster token is required (--token or --token-file)")
	}
	if !*witness && *pgData == "" {
		fatal("--pgdata is required (or use --witness)")
	}
	if *socket == "" {
		*socket = filepath.Join(*pgSock, fmt.Sprintf(".s.WORLBER_HA.%d", *pgPort))
	}
	if *pgLog == "" {
		*pgLog = filepath.Join(*dataDir, "postgresql.log")
	}

	a, err := agent.New(agent.Config{
		Name: *name, DataDir: *dataDir, RaftAddr: *raftAddr, APIAddr: *apiAddr,
		RaftBind: *raftBind, APIBind: *apiBind,
		Socket: *socket, Token: *token, Witness: *witness, PGAdvertise: *pgAdv,
		ReinitPGData: *reinit, Tick: *tick, LogLevel: *logLevel,
		PG: &pg.Local{PGData: *pgData, PGBin: *pgBin, SocketDir: *pgSock, Port: *pgPort, User: *pgUser, LogFile: *pgLog},
	})
	if err != nil {
		fatal("%v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := a.Run(ctx); err != nil {
		fatal("%v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "worlber-ha-agent: "+format+"\n", args...)
	os.Exit(1)
}
