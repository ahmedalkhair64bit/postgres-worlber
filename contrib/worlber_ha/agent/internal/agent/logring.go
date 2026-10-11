package agent

import (
	"regexp"
	"strings"
	"sync"
)

// logRing keeps the most recent agent log entries in memory so that they
// can be read from SQL with worlber_ha.agent_log().
type logRing struct {
	mu      sync.Mutex
	entries []LogEntry
	max     int
	partial string
}

// LogEntry is one agent log message.
type LogEntry struct {
	Time    string `json:"time"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

var logLine = regexp.MustCompile(`^(\d{4}-\d\d-\d\dT[0-9:.]+[-+Z0-9:]*) \[([A-Z]+)\] +(.*)$`)

func newLogRing(max int) *logRing { return &logRing{max: max} }

// Write implements io.Writer for hclog output.
func (r *logRing) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data := r.partial + string(p)
	lines := strings.Split(data, "\n")
	r.partial = lines[len(lines)-1]
	for _, line := range lines[:len(lines)-1] {
		if m := logLine.FindStringSubmatch(line); m != nil {
			msg := m[3]
			// drop the "worlber-ha[name]: " prefix
			if i := strings.Index(msg, "]: "); i >= 0 && strings.HasPrefix(msg, "worlber-ha[") {
				msg = msg[i+3:]
			}
			r.entries = append(r.entries, LogEntry{Time: m[1], Level: m[2], Message: msg})
		} else if len(r.entries) > 0 && strings.TrimSpace(line) != "" {
			// continuation of a multi-line value (e.g. pg_rewind output)
			e := &r.entries[len(r.entries)-1]
			e.Message += "\n" + strings.TrimRight(line, " ")
		}
	}
	if len(r.entries) > r.max {
		r.entries = append([]LogEntry(nil), r.entries[len(r.entries)-r.max:]...)
	}
	return len(p), nil
}

// Tail returns the last n entries, optionally only those at or above minLevel.
func (r *logRing) Tail(n int, minLevel string) []LogEntry {
	rank := map[string]int{"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4}
	min := rank[strings.ToUpper(minLevel)]
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []LogEntry
	for i := len(r.entries) - 1; i >= 0 && len(out) < n; i-- {
		if rank[r.entries[i].Level] >= min {
			out = append(out, r.entries[i])
		}
	}
	// oldest first
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if out == nil {
		out = []LogEntry{}
	}
	return out
}
