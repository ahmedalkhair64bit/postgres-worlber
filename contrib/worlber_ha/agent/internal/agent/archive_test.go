package agent

import (
	"strings"
	"testing"
)

func TestValidateArchiveDir(t *testing.T) {
	ok := []string{"", "/mnt/archive", "/var/lib/pg_wal-archive", "/a/b.c/d_e"}
	for _, d := range ok {
		if err := validateArchiveDir(d); err != nil {
			t.Errorf("%q should be valid: %v", d, err)
		}
	}
	bad := []string{"relative/path", "/mnt/$(rm -rf /)", "/a b/c", "/a;rm", "/a'b", "/a`b`"}
	for _, d := range bad {
		if err := validateArchiveDir(d); err == nil {
			t.Errorf("%q should be rejected", d)
		}
	}
}

func TestArchiveCommandsQuoteSafely(t *testing.T) {
	// validateArchiveDir forbids quotes, but the builders must still quote so
	// a path with spaces (not reachable today) can never split the command.
	cmd := archiveCommand("/mnt/wal archive")
	if !strings.Contains(cmd, "'/mnt/wal archive'") {
		t.Fatalf("archive_command not quoted: %s", cmd)
	}
	if !strings.Contains(cmd, "%f") || !strings.Contains(cmd, "%p") {
		t.Fatalf("archive_command missing %%f/%%p: %s", cmd)
	}
	rc := restoreCommand("/mnt/a")
	if rc != "cp '/mnt/a'/%f %p" {
		t.Fatalf("restore_command: %s", rc)
	}
}

func TestNormalizeDur(t *testing.T) {
	for in, want := range map[string]string{"7d": "168h", "30d": "720h", "12h": "12h", "90m": "90m"} {
		if got := normalizeDur(in); got != want {
			t.Errorf("normalizeDur(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIsHex(t *testing.T) {
	if !isHex("0000000100000001000000FE") || isHex("not-hex-wal-segment-name") {
		t.Fatal("isHex")
	}
}
