package ledger

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Table is the ledger table.  (client, seq) is unique, so a retried attempt
// can never create a second row for the same key.
const Table = "worlber_ledger"

// Setup creates the ledger table if it does not exist; reset empties it.
func Setup(ctx context.Context, dsn string, reset bool) error {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	_, err = c.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+Table+` (
		client int NOT NULL,
		seq bigint NOT NULL,
		ts timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (client, seq))`)
	if err == nil && reset {
		_, err = c.Exec(ctx, "TRUNCATE "+Table)
	}
	return err
}

// checkEmpty refuses to start when the table already has rows for the
// client ids this run will use: they would collide with the new ledger and
// make the verdict meaningless.
func checkEmpty(ctx context.Context, cfg *pgx.ConnConfig, first, last int) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c, err := pgx.ConnectConfig(cctx, cfg)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	var n int64
	if err := c.QueryRow(cctx, "SELECT count(*) FROM "+Table+" WHERE client BETWEEN $1 AND $2", first, last).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("%s already has %d rows for clients %d..%d; run \"worlber-ledger setup --reset\" or use --first-client", Table, n, first, last)
	}
	return nil
}

// WriteOptions configures the writers.
type WriteOptions struct {
	DSN           string
	Clients       int
	Rate          float64       // attempts per second per client; 0 = as fast as possible
	Duration      time.Duration // 0 = until ctx is done
	CommitTimeout time.Duration // a COMMIT still waiting after this has its socket closed
	ConnTimeout   time.Duration
	FirstClient   int // client ids are FirstClient .. FirstClient+Clients-1
}

// Writer serialises records to a JSON Lines stream.
type Writer struct {
	mu  sync.Mutex
	w   *bufio.Writer
	enc *json.Encoder
	n   atomic.Int64
}

func NewWriter(w io.Writer) *Writer {
	bw := bufio.NewWriterSize(w, 1<<16)
	return &Writer{w: bw, enc: json.NewEncoder(bw)}
}

func (w *Writer) Put(r Record) {
	w.mu.Lock()
	_ = w.enc.Encode(r)
	w.mu.Unlock()
	w.n.Add(1)
}

func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Flush()
}

func (w *Writer) Count() int64 { return w.n.Load() }

// RunWriters runs the ledger workers until Duration elapses or ctx ends.
// Every attempt is recorded exactly once.
func RunWriters(ctx context.Context, o WriteOptions, out *Writer) error {
	if o.Clients <= 0 {
		return errors.New("clients must be > 0")
	}
	if o.CommitTimeout <= 0 {
		o.CommitTimeout = 15 * time.Second
	}
	if o.ConnTimeout <= 0 {
		o.ConnTimeout = 2 * time.Second
	}
	cfg, err := pgx.ParseConfig(o.DSN)
	if err != nil {
		return err
	}
	if err := checkEmpty(ctx, cfg, o.FirstClient, o.FirstClient+o.Clients-1); err != nil {
		return err
	}
	if o.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Duration)
		defer cancel()
	}
	stopFlush := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_ = out.Flush()
			case <-stopFlush:
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < o.Clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			(&worker{id: id, opt: o, cfg: cfg.Copy(), out: out}).run(ctx)
		}(o.FirstClient + i)
	}
	wg.Wait()
	close(stopFlush)
	return out.Flush()
}

// connectRetryDelay is the pause after a failed connect; it bounds the RTO
// resolution.
const connectRetryDelay = 100 * time.Millisecond

type worker struct {
	id   int
	opt  WriteOptions
	cfg  *pgx.ConnConfig
	out  *Writer
	conn *pgx.Conn
	warn atomic.Bool
}

func (w *worker) run(ctx context.Context) {
	// Notices arrive on the connection's goroutine; remember a sync-rep
	// cancel warning for the attempt in flight.
	w.cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if IsSyncCancel(n.Message) {
			w.warn.Store(true)
		}
	}
	var tick <-chan time.Time
	if w.opt.Rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / w.opt.Rate))
		defer t.Stop()
		tick = t.C
	}
	defer w.closeConn()
	for seq := int64(1); ; seq++ {
		if ctx.Err() != nil {
			return
		}
		if tick != nil {
			select {
			case <-tick:
			case <-ctx.Done():
				return
			}
		}
		w.out.Put(w.attempt(ctx, seq))
	}
}

func (w *worker) closeConn() {
	if w.conn != nil {
		cctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = w.conn.Close(cctx)
		cancel()
		w.conn = nil
	}
}

// attempt does one insert transaction.  The run context only stops new
// attempts: an attempt that already started is finished with its own
// timeouts, so ending the run never turns into a client cancel.
func (w *worker) attempt(_ context.Context, seq int64) Record {
	rec := Record{Client: w.id, Seq: seq, Start: time.Now().UnixMilli()}
	finish := func(phase Phase, err error) Record {
		rec.Out = Classify(phase, err, w.warn.Load())
		rec.End = time.Now().UnixMilli()
		if err != nil {
			rec.Err = err.Error()
			w.closeConn() // reconnect on the next attempt
		}
		return rec
	}
	bg := context.Background()
	if w.conn == nil {
		cctx, cancel := context.WithTimeout(bg, w.opt.ConnTimeout)
		c, err := pgx.ConnectConfig(cctx, w.cfg)
		cancel()
		if err != nil {
			// Nothing is listening (failover in progress): do not spin.
			time.Sleep(connectRetryDelay)
			return finish(BeforeCommit, err)
		}
		w.conn = c
	}
	sctx, cancel := context.WithTimeout(bg, w.opt.ConnTimeout+w.opt.CommitTimeout)
	defer cancel()
	if _, err := w.conn.Exec(sctx, "BEGIN"); err != nil {
		return finish(BeforeCommit, err)
	}
	if _, err := w.conn.Exec(sctx, "INSERT INTO "+Table+" (client, seq) VALUES ($1, $2)", w.id, seq); err != nil {
		return finish(BeforeCommit, err)
	}
	w.warn.Store(false)
	// pgx's default context handler sets a socket deadline on timeout; it
	// never sends a cancel request, so a timed-out COMMIT is "unknown".
	cctx, ccancel := context.WithTimeout(bg, w.opt.CommitTimeout)
	_, err := w.conn.Exec(cctx, "COMMIT")
	ccancel()
	return finish(CommitSent, err)
}

// ReadKeys returns every (client, seq) in the ledger table at dsn.
func ReadKeys(ctx context.Context, dsn string) (map[Key]bool, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, "SELECT client, seq FROM "+Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[Key]bool{}
	for rows.Next() {
		var k Key
		if err := rows.Scan(&k.Client, &k.Seq); err != nil {
			return nil, err
		}
		out[k] = true
	}
	return out, rows.Err()
}

// ReportOptions configures the read-only report workload.
type ReportOptions struct {
	DSN      string
	Sessions int
	Query    string
	Duration time.Duration
}

// ReportRecord is one report query run.
type ReportRecord struct {
	Session int    `json:"session"`
	Out     string `json:"o"` // completed | cancelled | error
	Server  string `json:"server,omitempty"`
	Start   int64  `json:"t0"`
	End     int64  `json:"t1"`
	Code    string `json:"sqlstate,omitempty"`
	Err     string `json:"e,omitempty"`
}

// ReportOutcome classifies a report query's error.  Recovery conflicts on a
// standby surface as serialization_failure (40001), deadlock (40P01) or
// query_canceled (57014); those count as cancelled.
func ReportOutcome(err error) (string, string) {
	if err == nil {
		return "completed", ""
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "40001", "40P01", "57014":
			return "cancelled", pe.Code
		}
		return "error", pe.Code
	}
	return "error", ""
}

// RunReports runs long read-only queries in a loop on each session and
// writes one ReportRecord per run as JSON Lines.
func RunReports(ctx context.Context, o ReportOptions, w io.Writer) error {
	if o.Sessions <= 0 {
		return errors.New("sessions must be > 0")
	}
	if o.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Duration)
		defer cancel()
	}
	var mu sync.Mutex
	enc := json.NewEncoder(w)
	put := func(r ReportRecord) {
		mu.Lock()
		_ = enc.Encode(r)
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for i := 1; i <= o.Sessions; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for ctx.Err() == nil {
				put(reportOnce(ctx, o, id))
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func reportOnce(ctx context.Context, o ReportOptions, id int) ReportRecord {
	r := ReportRecord{Session: id, Start: time.Now().UnixMilli()}
	done := func(err error) ReportRecord {
		r.Out, r.Code = ReportOutcome(err)
		if err != nil {
			r.Err = err.Error()
		}
		r.End = time.Now().UnixMilli()
		return r
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	c, err := pgx.Connect(cctx, o.DSN)
	cancel()
	if err != nil {
		time.Sleep(time.Second) // listener may have no standby right now
		return done(err)
	}
	defer c.Close(context.Background())
	var srv *string
	if err := c.QueryRow(ctx, "SELECT host(inet_server_addr())").Scan(&srv); err == nil && srv != nil {
		r.Server = *srv
	}
	rows, err := c.Query(ctx, o.Query)
	if err == nil {
		for rows.Next() {
		}
		rows.Close()
		err = rows.Err()
	}
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("run ended: %w", err)
	}
	return done(err)
}
