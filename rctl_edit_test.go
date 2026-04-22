package main

import (
	"strings"
	"testing"
)

func TestUpdateJailConfigRctlMetadata(t *testing.T) {
	original := strings.Join([]string{
		"web01 {",
		"  host.hostname = \"web01\";",
		"  path = \"/usr/local/jails/containers/web01\";",
		"  # freebsd-jails-tui: note=frontend;",
		"  persist;",
		"}",
		"",
	}, "\n")

	values := jailWizardValues{
		CPUPercent:   "25",
		MemoryLimit:  "2G",
		ProcessLimit: "512",
	}
	updated, err := updateJailConfigRctlMetadata(original, "web01", values)
	if err != nil {
		t.Fatalf("updateJailConfigRctlMetadata returned error: %v", err)
	}
	if !strings.Contains(updated, "# freebsd-jails-tui: rctl_mode=persistent cpu_percent=25 memory_limit=2G process_limit=512;") {
		t.Fatalf("updated config did not include managed rctl metadata:\n%s", updated)
	}
	if !strings.Contains(updated, "# freebsd-jails-tui: note=frontend;") {
		t.Fatalf("updated config should preserve unrelated metadata:\n%s", updated)
	}

	cleared, err := updateJailConfigRctlMetadata(updated, "web01", jailWizardValues{})
	if err != nil {
		t.Fatalf("clearing rctl metadata returned error: %v", err)
	}
	if strings.Contains(cleared, "rctl_mode=") || strings.Contains(cleared, "cpu_percent=") || strings.Contains(cleared, "memory_limit=") || strings.Contains(cleared, "process_limit=") {
		t.Fatalf("cleared config still contains rctl metadata:\n%s", cleared)
	}
	if !strings.Contains(cleared, "# freebsd-jails-tui: note=frontend;") {
		t.Fatalf("cleared config should preserve note metadata:\n%s", cleared)
	}
}

func TestNormalizeRctlLimitValues(t *testing.T) {
	values, fieldID, err := normalizeRctlLimitValues(jailWizardValues{
		CPUPercent:   " 50 ",
		MemoryLimit:  " 2g ",
		ProcessLimit: " 512 ",
	})
	if err != nil {
		t.Fatalf("normalizeRctlLimitValues returned error: %v", err)
	}
	if fieldID != "" {
		t.Fatalf("expected empty field id, got %q", fieldID)
	}
	if values.CPUPercent != "50" || values.MemoryLimit != "2G" || values.ProcessLimit != "512" {
		t.Fatalf("unexpected normalized values: %+v", values)
	}
}
