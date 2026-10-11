// Package pg controls and observes the local PostgreSQL instance.
package pg

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Local describes the PostgreSQL instance managed by this agent.
type Local struct {
	PGData    string
	PGBin     string
	SocketDir string
	Port      int
	User      string
	LogFile   string

	mu   sync.Mutex
	conn *pgx.Conn
}

// ReplicaStat is one row of pg_stat_replication.
type ReplicaStat struct {
	AppName    string  `json:"application_name"`
	ClientAddr string  `json:"client_addr"`
	State      string  `json:"state"`
	SyncState  string  `json:"sync_state"`
	SentLSN    uint64  `json:"sent_lsn"`
	WriteLSN   uint64  `json:"write_lsn"`
	FlushLSN   uint64  `json:"flush_lsn"`
	ReplayLSN  uint64  `json:"replay_lsn"`
	WriteLag   float64 `json:"write_lag"`  // seconds
	FlushLag   float64 `json:"flush_lag"`  // seconds
	ReplayLag  float64 `json:"replay_lag"` // seconds
}

// Health is a point-in-time observation of the local instance.
type Health struct {
	Running     bool          `json:"running"`
	InRecovery  bool          `json:"in_recovery"`
	LSN         uint64        `json:"lsn"`        // write LSN (primary) or receive LSN (standby)
	ReplayLSN   uint64        `json:"replay_lsn"` // standby only
	Timeline    int           `json:"timeline"`
	WalReceiver string        `json:"wal_receiver"`
	Replicas    []ReplicaStat `json:"replicas,omitempty"`
	Error       string        `json:"error,omitempty"`
}

func (l *Local) bin(name string) string {
	if l.PGBin == "" {
		return name
	}
	return filepath.Join(l.PGBin, name)
}

func (l *Local) connString(db string) string {
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s connect_timeout=3 application_name=worlber_ha_agent",
		l.SocketDir, l.Port, l.User, db)
}

func (l *Local) getConn(ctx context.Context) (*pgx.Conn, error) {
	if l.conn != nil && !l.conn.IsClosed() {
		return l.conn, nil
	}
	c, err := pgx.Connect(ctx, l.connString("postgres"))
	if err != nil {
		return nil, err
	}
	l.conn = c
	return c, nil
}

func (l *Local) dropConn() {
	if l.conn != nil {
		_ = l.conn.Close(context.Background())
		l.conn = nil
	}
}

// Exec runs a statement on the local instance.
func (l *Local) Exec(ctx context.Context, sql string, args ...any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, err := l.getConn(ctx)
	if err != nil {
		return err
	}
	if _, err = c.Exec(ctx, sql, args...); err != nil && c.IsClosed() {
		l.dropConn()
	}
	return err
}

// QueryRow runs a single-row query.
func (l *Local) QueryRow(ctx context.Context, sql string, args []any, dest ...any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, err := l.getConn(ctx)
	if err != nil {
		return err
	}
	err = c.QueryRow(ctx, sql, args...).Scan(dest...)
	if err != nil && c.IsClosed() {
		l.dropConn()
	}
	return err
}

// QueryStrings returns the first column of all rows.
func (l *Local) QueryStrings(ctx context.Context, sql string, args ...any) ([]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, err := l.getConn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		if c.IsClosed() {
			l.dropConn()
		}
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Probe connects to the instance and collects its health.
func (l *Local) Probe(ctx context.Context) Health {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	l.mu.Lock()
	defer l.mu.Unlock()

	c, err := l.getConn(ctx)
	if err != nil {
		return Health{Running: false, Error: err.Error()}
	}
	h := Health{Running: true}
	var lsn, replay, wr *string
	var tli *int
	err = c.QueryRow(ctx, `
		SELECT pg_is_in_recovery(),
		       CASE WHEN pg_is_in_recovery()
		            THEN coalesce(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::text
		            ELSE pg_current_wal_lsn()::text END,
		       pg_last_wal_replay_lsn()::text,
		       CASE WHEN pg_is_in_recovery()
		            THEN coalesce((SELECT received_tli FROM pg_stat_wal_receiver),
		                          (SELECT timeline_id FROM pg_control_checkpoint()))
		            ELSE ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int END,
		       (SELECT status FROM pg_stat_wal_receiver)`).
		Scan(&h.InRecovery, &lsn, &replay, &tli, &wr)
	if err != nil {
		if c.IsClosed() {
			l.dropConn()
		}
		return Health{Running: !c.IsClosed(), Error: err.Error()}
	}
	if lsn != nil {
		h.LSN, _ = ParseLSN(*lsn)
	}
	if replay != nil {
		h.ReplayLSN, _ = ParseLSN(*replay)
	}
	if tli != nil {
		h.Timeline = *tli
	}
	if wr != nil {
		h.WalReceiver = *wr
	}
	if !h.InRecovery {
		rows, err := c.Query(ctx, `SELECT application_name, coalesce(client_addr::text, 'local'),
		                                  coalesce(state, ''), coalesce(sync_state, ''),
		                                  coalesce(sent_lsn::text, '0/0'), coalesce(write_lsn::text, '0/0'),
		                                  coalesce(flush_lsn::text, '0/0'), coalesce(replay_lsn::text, '0/0'),
		                                  coalesce(extract(epoch FROM write_lag), 0)::float8,
		                                  coalesce(extract(epoch FROM flush_lag), 0)::float8,
		                                  coalesce(extract(epoch FROM replay_lag), 0)::float8
		                           FROM pg_stat_replication`)
		if err == nil {
			for rows.Next() {
				var r ReplicaStat
				var sl, wl, fl, rl string
				if rows.Scan(&r.AppName, &r.ClientAddr, &r.State, &r.SyncState, &sl, &wl, &fl, &rl,
					&r.WriteLag, &r.FlushLag, &r.ReplayLag) == nil {
					r.SentLSN, _ = ParseLSN(sl)
					r.WriteLSN, _ = ParseLSN(wl)
					r.FlushLSN, _ = ParseLSN(fl)
					r.ReplayLSN, _ = ParseLSN(rl)
					h.Replicas = append(h.Replicas, r)
				}
			}
			rows.Close()
		}
	}
	return h
}

// Close drops the cached connection (e.g. before a stop).
func (l *Local) Close() {
	l.mu.Lock()
	l.dropConn()
	l.mu.Unlock()
}

func (l *Local) run(ctx context.Context, env []string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, l.bin(name), args...)
	cmd.Env = append(os.Environ(), env...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("%s failed: %v: %s", name, err, strings.TrimSpace(lastLines(out.String(), 5)))
	}
	return out.String(), nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// IsRunning asks pg_ctl whether a postmaster is running on PGDATA.
func (l *Local) IsRunning() bool {
	if !l.HasDataDir() {
		return false
	}
	cmd := exec.Command(l.bin("pg_ctl"), "status", "-D", l.PGData)
	return cmd.Run() == nil
}

// Start starts the instance and waits for it to accept connections.
func (l *Local) Start(ctx context.Context) error {
	l.Close()
	args := []string{"start", "-w", "-t", "120", "-D", l.PGData}
	if l.LogFile != "" {
		args = append(args, "-l", l.LogFile)
	}
	_, err := l.run(ctx, nil, "pg_ctl", args...)
	return err
}

// Stop stops the instance with the given mode (smart/fast/immediate).
func (l *Local) Stop(ctx context.Context, mode string) error {
	l.Close()
	_, err := l.run(ctx, nil, "pg_ctl", "stop", "-w", "-t", "120", "-m", mode, "-D", l.PGData)
	return err
}

// Promote promotes a standby and waits for it to finish.
func (l *Local) Promote(ctx context.Context) error {
	var ok bool
	if err := l.QueryRow(ctx, "SELECT pg_promote(true, 60)", nil, &ok); err != nil {
		return err
	}
	if !ok {
		return errors.New("pg_promote returned false")
	}
	return nil
}

// HasDataDir reports whether PGDATA contains a cluster.
func (l *Local) HasDataDir() bool {
	_, err := os.Stat(filepath.Join(l.PGData, "PG_VERSION"))
	return err == nil
}

// DataDirEmpty reports whether PGDATA is missing or empty.
func (l *Local) DataDirEmpty() bool {
	ents, err := os.ReadDir(l.PGData)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return err == nil && len(ents) == 0
}

// HasStandbySignal reports whether standby.signal exists.
func (l *Local) HasStandbySignal() bool {
	_, err := os.Stat(filepath.Join(l.PGData, "standby.signal"))
	return err == nil
}

// WriteStandbySignal creates standby.signal.
func (l *Local) WriteStandbySignal() error {
	return os.WriteFile(filepath.Join(l.PGData, "standby.signal"), nil, 0600)
}

// ControlData returns pg_controldata output as a map.
func (l *Local) ControlData(ctx context.Context) (map[string]string, error) {
	out, err := l.run(ctx, []string{"LC_ALL=C"}, "pg_controldata", "-D", l.PGData)
	if err != nil {
		return nil, err
	}
	m := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if ok {
			m[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return m, nil
}

// BaseBackup clones a primary into the (empty) PGDATA.
func (l *Local) BaseBackup(ctx context.Context, conninfo, password, slot string) error {
	if err := os.MkdirAll(l.PGData, 0700); err != nil {
		return err
	}
	_ = os.Chmod(l.PGData, 0700)
	env := []string{"PGPASSWORD=" + password}
	args := []string{"-D", l.PGData, "-d", conninfo, "-X", "stream", "-c", "fast", "-R", "--no-password"}
	if slot != "" {
		args = append(args, "-S", slot)
	}
	_, err := l.run(ctx, env, "pg_basebackup", args...)
	return err
}

// Rewind resynchronizes PGDATA with the given source server.
func (l *Local) Rewind(ctx context.Context, conninfo, password string) (string, error) {
	env := []string{"PGPASSWORD=" + password}
	return l.run(ctx, env, "pg_rewind", "--target-pgdata", l.PGData, "--source-server", conninfo)
}

// SetAutoConf edits postgresql.auto.conf while the server is stopped.
// A nil value removes the setting.
func (l *Local) SetAutoConf(settings map[string]*string) error {
	path := filepath.Join(l.PGData, "postgresql.auto.conf")
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var out []string
	seen := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		key := strings.TrimSpace(strings.SplitN(strings.SplitN(line, "=", 2)[0], " ", 2)[0])
		if v, ok := settings[key]; ok && key != "" && !strings.HasPrefix(strings.TrimSpace(line), "#") {
			if !seen[key] && v != nil {
				out = append(out, fmt.Sprintf("%s = %s", key, QuoteLiteral(*v)))
			}
			seen[key] = true
			continue
		}
		if line != "" {
			out = append(out, line)
		}
	}
	for k, v := range settings {
		if !seen[k] && v != nil {
			out = append(out, fmt.Sprintf("%s = %s", k, QuoteLiteral(*v)))
		}
	}
	if len(out) == 0 || !strings.HasPrefix(out[0], "#") {
		out = append([]string{"# Do not edit this file manually!",
			"# It will be overwritten by the ALTER SYSTEM command."}, out...)
	}
	return os.WriteFile(path, []byte(strings.Join(out, "\n")+"\n"), 0600)
}

// QuoteLiteral quotes a value for postgresql.conf.
func QuoteLiteral(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// QuoteIdent quotes an SQL identifier.
func QuoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

// ParseLSN parses "X/Y" into a 64-bit position.
func ParseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("invalid LSN %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, err
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, err
	}
	return h<<32 | l, nil
}

// FormatLSN formats a 64-bit position as "X/Y".
func FormatLSN(v uint64) string {
	return fmt.Sprintf("%X/%08X", v>>32, v&0xffffffff)
}

// ConnInfo builds a libpq keyword/value connection string.
func ConnInfo(kv ...string) string {
	var parts []string
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] == "" {
			continue
		}
		v := strings.ReplaceAll(kv[i+1], `\`, `\\`)
		v = strings.ReplaceAll(v, `'`, `\'`)
		parts = append(parts, fmt.Sprintf("%s='%s'", kv[i], v))
	}
	return strings.Join(parts, " ")
}
