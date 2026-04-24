package main

import (
	"strings"
	"testing"
)

func TestSummarizeActivityLogs(t *testing.T) {
	summary := summarizeActivityLogs([]string{
		"$ service jail onestart web01",
		"$ write /etc/jail.conf.d/web01.conf",
		"restore: /etc/jail.conf.d/web01.conf <- /tmp/web01.bak",
		"rollback warning: failed to restart jail after destroy error: exit status 1",
		"$ service jail onestart web01",
	})

	if got, want := strings.Join(summary.Commands, "\n"), "service jail onestart web01\nwrite /etc/jail.conf.d/web01.conf"; got != want {
		t.Fatalf("Commands = %q, want %q", got, want)
	}
	if got, want := strings.Join(summary.FilesTouched, "\n"), "/etc/jail.conf.d/web01.conf"; got != want {
		t.Fatalf("FilesTouched = %q, want %q", got, want)
	}
	if len(summary.RollbackWarnings) != 2 {
		t.Fatalf("RollbackWarnings len = %d, want 2", len(summary.RollbackWarnings))
	}
}

func TestBuildPostCreateChecklistForLinux(t *testing.T) {
	values := jailWizardValues{
		JailType:             "linux",
		LinuxBootstrapMethod: "debootstrap",
		LinuxBootstrap:       "auto",
	}
	checklist := buildPostCreateChecklist(values, []string{
		"linux bootstrap preflight failed: no IPv4 or IPv6 default route inside the jail",
	}, true)

	joined := strings.Join(checklist, "\n")
	for _, want := range []string{
		"Summary or Drift tab",
		"Linux bootstrap preflight failed",
		"Review the Runtime tab",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("buildPostCreateChecklist() missing %q in:\n%s", want, joined)
		}
	}
}

func TestBuildPostCreateChecklistForLinuxSkip(t *testing.T) {
	values := jailWizardValues{
		JailType:             "linux",
		LinuxBootstrapMethod: "archive",
		LinuxBootstrap:       "skip",
	}
	checklist := buildPostCreateChecklist(values, nil, true)
	joined := strings.Join(checklist, "\n")
	if !strings.Contains(joined, "Stop the jail when ready") {
		t.Fatalf("archive skip checklist missing stop-and-retry guidance:\n%s", joined)
	}
}

func TestWizardSetExecutionResultStoresNextActions(t *testing.T) {
	var w jailCreationWizard
	w.setExecutionResult(JailCreationResult{
		Logs:        []string{"$ service jail onestart linux01"},
		NextActions: []string{"Use detail view action 'b' to run Linux bootstrap after networking is ready inside the jail."},
	})

	if len(w.executionNextActions) != 1 {
		t.Fatalf("executionNextActions len = %d, want 1", len(w.executionNextActions))
	}
	if w.executionNextActions[0] == "" {
		t.Fatal("executionNextActions[0] is empty")
	}
}
