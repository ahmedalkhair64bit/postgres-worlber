// Command worlber-ledger is the load and verdict tool of the worlber_ha
// chaos harness.
//
//	worlber-ledger setup  --dsn DSN [--reset]
//	worlber-ledger write  --dsn RW_DSN --clients 16 --rate 50 --duration 5m --out run.ledger
//	worlber-ledger report --dsn RO_DSN --sessions 2 --duration 5m --out run.reports
//	worlber-ledger verify --ledger run.ledger --dsn NEW_PRIMARY [--old-dsn OLD_COPY | --old-keys FILE] [--fault-at MS]
//
// "verify" prints a JSON verdict and exits 1 when an acknowledged row is
// missing, an acknowledged row exists only on the old primary, or a row the
// ledger never wrote is found.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/ledger"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: worlber-ledger setup|write|report|verify [flags]  (-h for flags)")
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "setup":
		err = cmdSetup(ctx, os.Args[2:])
	case "write":
		err = cmdWrite(ctx, os.Args[2:])
	case "report":
		err = cmdReport(ctx, os.Args[2:])
	case "verify":
		err = cmdVerify(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "worlber-ledger:", err)
		os.Exit(1)
	}
}

func cmdSetup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	dsn := fs.String("dsn", "", "connection string (read-write)")
	reset := fs.Bool("reset", false, "empty the ledger table (start of a new run)")
	fs.Parse(args)
	return ledger.Setup(ctx, *dsn, *reset)
}

func create(path string) (*os.File, error) {
	if path == "" || path == "-" {
		return os.Stdout, nil
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
}

func cmdWrite(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	var o ledger.WriteOptions
	fs.StringVar(&o.DSN, "dsn", "", "connection string through the read-write listener")
	fs.IntVar(&o.Clients, "clients", 8, "number of writer clients")
	fs.IntVar(&o.FirstClient, "first-client", 1, "id of the first client (use distinct ranges for parallel writers)")
	fs.Float64Var(&o.Rate, "rate", 0, "attempts per second per client (0 = unthrottled)")
	fs.DurationVar(&o.Duration, "duration", 0, "how long to write (0 = until SIGINT/SIGTERM)")
	fs.DurationVar(&o.CommitTimeout, "commit-timeout", 15*time.Second, "close the socket of a COMMIT still waiting after this")
	fs.DurationVar(&o.ConnTimeout, "connect-timeout", 2*time.Second, "connect timeout per attempt")
	out := fs.String("out", "", "ledger file (JSON Lines, appended); default stdout")
	fs.Parse(args)
	f, err := create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	w := ledger.NewWriter(f)
	err = ledger.RunWriters(ctx, o, w)
	fmt.Fprintf(os.Stderr, "worlber-ledger: %d attempts recorded\n", w.Count())
	return err
}

func cmdReport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	var o ledger.ReportOptions
	fs.StringVar(&o.DSN, "dsn", "", "connection string through the read-only listener")
	fs.IntVar(&o.Sessions, "sessions", 2, "concurrent report sessions")
	fs.StringVar(&o.Query, "query",
		"SELECT client, count(*), max(seq), pg_sleep(300) FROM "+ledger.Table+" GROUP BY client",
		"report query, run in a loop on every session")
	fs.DurationVar(&o.Duration, "duration", 0, "how long to run (0 = until SIGINT/SIGTERM)")
	out := fs.String("out", "", "report log (JSON Lines, appended); default stdout")
	fs.Parse(args)
	f, err := create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	return ledger.RunReports(ctx, o, f)
}

type verifyResult struct {
	ledger.Verdict
	Summary ledger.Summary `json:"summary"`
}

func cmdVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	path := fs.String("ledger", "", "ledger file written by 'write'")
	dsn := fs.String("dsn", "", "connection string of the new primary")
	oldDSN := fs.String("old-dsn", "", "optional: isolated, read-only copy of the old primary")
	oldKeys := fs.String("old-keys", "", "optional: file of client,seq rows dumped from the old primary")
	faultAt := fs.Int64("fault-at", 0, "fault injection time, unix ms (enables RTO)")
	fs.Parse(args)
	recs, err := ledger.Read(*path)
	if err != nil {
		return err
	}
	newRows, err := ledger.ReadKeys(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("new primary: %w", err)
	}
	var oldRows map[ledger.Key]bool
	switch {
	case *oldDSN != "":
		if oldRows, err = ledger.ReadKeys(ctx, *oldDSN); err != nil {
			return fmt.Errorf("old primary copy: %w", err)
		}
	case *oldKeys != "":
		if oldRows, err = ledger.ReadKeysFile(*oldKeys); err != nil {
			return fmt.Errorf("old primary keys: %w", err)
		}
	}
	res := verifyResult{Verdict: ledger.Verify(recs, newRows, oldRows), Summary: ledger.Summarize(recs, *faultAt)}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(res)
	if !res.Pass {
		os.Exit(1)
	}
	return nil
}
