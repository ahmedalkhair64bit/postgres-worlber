package agent

// WAL archive catch-up (W6).
//
// With worlber_ha.set_config('wal_archive_dir', '/mnt/archive') the agent
// turns on WAL archiving on every node and sets a restore_command on the
// standbys.  A standby that falls behind then fetches the missing WAL from
// the archive by itself (PostgreSQL's restore_command), instead of a full
// re-seed.  The directory must be mounted on every node (NFS, or a bucket
// mounted with gcsfuse).
//
// archive_mode is a postmaster setting, so turning archiving on needs a
// PostgreSQL restart; the agent sets it with ALTER SYSTEM and records an
// event, and archiving becomes active at the next restart (like
// shared_preload_libraries).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/worlber/worlber-ha-agent/internal/fsm"
	"github.com/worlber/worlber-ha-agent/internal/pg"
)

// archiveCommand returns an idempotent archive_command for a directory.
// It succeeds if the segment is already archived and identical (safe to
// retry after a crash between copy and the server marking it done), fails if
// a different file is already there, and otherwise copies atomically.
func archiveCommand(dir string) string {
	d := shellQuote(dir)
	// %p = path to the WAL file to archive, %f = its name.
	return fmt.Sprintf(`bash -c 'set -e; d=%s; f="$d/%%f"; `+
		`if [ -f "$f" ]; then cmp -s "$f" "%%p"; else cp "%%p" "$f.tmp.$$" && mv "$f.tmp.$$" "$f"; fi'`, d)
}

// restoreCommand returns the restore_command for a directory.
func restoreCommand(dir string) string {
	return fmt.Sprintf(`cp %s/%%f %%p`, shellQuote(dir))
}

// shellQuote single-quotes a string for a shell.  Combined with the path
// validation in validateArchiveDir this keeps the archive commands safe.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// validateArchiveDir rejects anything but an absolute path of safe
// characters, so it cannot inject shell into the archive/restore commands.
func validateArchiveDir(dir string) error {
	if dir == "" {
		return nil
	}
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("wal_archive_dir must be an absolute path")
	}
	for _, r := range dir {
		ok := r == '/' || r == '.' || r == '_' || r == '-' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !ok {
			return fmt.Errorf("wal_archive_dir may only contain letters, digits and /._-")
		}
	}
	return nil
}

// archiveDir is the configured archive directory, or "".
func (a *Agent) archiveDir(st fsm.State) string { return st.Option("wal_archive_dir") }

// ensureArchiving configures WAL archiving on the local PostgreSQL to match
// wal_archive_dir.  Returns true when a restart is needed for archive_mode
// to take effect.  Runs on any running node (every node archives, because
// any can become the primary).
func (a *Agent) ensureArchiving(ctx context.Context) (bool, error) {
	st := a.fsm.Get()
	dir := a.archiveDir(st)
	var curMode, curCmd string
	if err := a.pg.QueryRow(ctx, "SELECT current_setting('archive_mode'), current_setting('archive_command')", nil, &curMode, &curCmd); err != nil {
		return false, err
	}
	if dir == "" {
		return false, nil // archiving not configured: leave it as the operator set it
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		a.log.Warn("cannot create the WAL archive directory", "dir", dir, "error", err)
	}
	wantCmd := archiveCommand(dir)
	if curCmd != wantCmd {
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET archive_command = "+pg.QuoteLiteral(wantCmd)); err != nil {
			return false, err
		}
		_ = a.pg.Exec(ctx, "SELECT pg_reload_conf()")
		a.log.Info("archive_command set", "dir", dir)
	}
	if curMode != "on" && curMode != "always" {
		// Already written and only waiting for a restart: do not write it
		// (or report it) again on every reconcile pass.
		var pending string
		_ = a.pg.QueryRow(ctx, "SELECT coalesce((SELECT setting FROM pg_file_settings WHERE name = 'archive_mode' ORDER BY seqno DESC LIMIT 1), '')", nil, &pending)
		if pending == "on" || pending == "always" {
			return true, nil
		}
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET archive_mode = on"); err != nil {
			return false, err
		}
		a.report(ctx, "archive_restart_needed", "archive_mode=on set; restart PostgreSQL to start WAL archiving")
		return true, nil
	}
	return false, nil
}

// ensureStandbyArchive keeps a running standby's archive settings current
// when wal_archive_dir is set or changed later: restore_command (reloadable)
// now, and archive_mode/archive_command so that a later promotion archives
// without waiting for another restart.
func (a *Agent) ensureStandbyArchive(ctx context.Context, st fsm.State) {
	dir := a.archiveDir(st)
	if dir == "" {
		return
	}
	var cur string
	if err := a.pg.QueryRow(ctx, "SELECT current_setting('restore_command')", nil, &cur); err != nil {
		return
	}
	if want := restoreCommand(dir); cur != want {
		if err := a.pg.Exec(ctx, "ALTER SYSTEM SET restore_command = "+pg.QuoteLiteral(want)); err != nil {
			a.log.Warn("cannot set restore_command", "error", err)
			return
		}
		_ = a.pg.Exec(ctx, "SELECT pg_reload_conf()")
		a.log.Info("restore_command set", "dir", dir)
	}
	if _, err := a.ensureArchiving(ctx); err != nil {
		a.log.Warn("cannot configure WAL archiving", "error", err)
	}
}

// archiveSettings adds restore_command for standbys when an archive is set.
func (a *Agent) archiveSettings(st fsm.State, into map[string]*string) {
	dir := a.archiveDir(st)
	if dir == "" {
		into["restore_command"] = nil
		return
	}
	rc := restoreCommand(dir)
	into["restore_command"] = &rc
}

// retentionLoop prunes the archive from the primary's agent, keeping
// archive_keep of history (default 7 days), so a crashed member cannot fill
// it forever.  Not the Raft leader: a witness can hold leadership and has no
// archive mounted.
func (a *Agent) retentionLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.cfg.Witness || a.fsm.Get().Primary != a.cfg.Name {
			continue
		}
		a.pruneArchive(ctx)
	}
}

func (a *Agent) pruneArchive(ctx context.Context) {
	st := a.fsm.Get()
	dir := a.archiveDir(st)
	if dir == "" {
		return
	}
	keep := st.Option("archive_keep")
	if keep == "" {
		keep = "7d"
	}
	d, err := time.ParseDuration(normalizeDur(keep))
	if err != nil {
		return
	}
	// The oldest WAL still needed is the lowest replayed LSN across members;
	// pg_archivecleanup removes segments older than a cutoff file.  We keep
	// it simple and time-based: delete files not modified within the window.
	cutoff := time.Now().Add(-d)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var removed int
	for _, e := range ents {
		info, err := e.Info()
		if err != nil || info.IsDir() || !info.ModTime().Before(cutoff) {
			continue
		}
		// only prune completed WAL segments (24 hex chars) and their backups
		name := e.Name()
		if len(name) >= 24 && isHex(name[:24]) {
			if os.Remove(filepath.Join(dir, name)) == nil {
				removed++
			}
		}
	}
	if removed > 0 {
		a.log.Info("pruned the WAL archive", "removed", removed, "keep", keep)
	}
}

func isHex(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

// normalizeDur turns "7d"/"30d" into something time.ParseDuration accepts.
func normalizeDur(s string) string {
	if strings.HasSuffix(s, "d") {
		if n, err := strconv.Atoi(strings.TrimSuffix(s, "d")); err == nil {
			return fmt.Sprintf("%dh", n*24)
		}
	}
	return s
}

// archiveRow is the primary's WAL archiver state (pg_stat_archiver).
type archiveRow struct {
	Mode          string   `json:"mode"`
	FailedCount   int64    `json:"failed_count"`
	LastArchivedS *float64 `json:"last_archived_s"` // seconds since the last success
	LastFailedS   *float64 `json:"last_failed_s"`   // seconds since the last failure
	Failing       bool     `json:"failing"`
}

// archiveStatus reads pg_stat_archiver on the local primary and records
// archive_failing / archive_recovered events on a change.  Archiving counts
// as failing when the last attempt failed (last_failed_time is newer than
// last_archived_time).
func (a *Agent) archiveStatus(ctx context.Context) *archiveRow {
	var r archiveRow
	err := a.pg.QueryRow(ctx, `SELECT current_setting('archive_mode'), failed_count,
			extract(epoch FROM now() - last_archived_time)::float8,
			extract(epoch FROM now() - last_failed_time)::float8,
			coalesce(last_failed_time > coalesce(last_archived_time, '-infinity'), false)
		FROM pg_stat_archiver`, nil, &r.Mode, &r.FailedCount, &r.LastArchivedS, &r.LastFailedS, &r.Failing)
	if err != nil {
		return nil
	}
	if r.Mode == "off" {
		r.Failing = false
	}
	a.mu.Lock()
	was := a.archiveFailing
	a.archiveFailing = r.Failing
	a.mu.Unlock()
	if r.Failing && !was {
		a.report(ctx, "archive_failing", fmt.Sprintf("WAL archiving is failing (%d failures); pg_wal grows until it succeeds", r.FailedCount))
	} else if !r.Failing && was {
		a.report(ctx, "archive_recovered", "WAL archiving works again")
	}
	return &r
}
