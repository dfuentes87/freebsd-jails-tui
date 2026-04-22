package main

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestFilterAndSortDashboardJails(t *testing.T) {
	jails := []Jail{
		{Name: "web02", Hostname: "web02.local", Note: "frontend", Type: "thick", Path: "/jails/web02", ConfigPath: "/etc/jail.conf.d/web02.conf", Running: false, StartupOrder: 3},
		{Name: "db01", Hostname: "db01.local", Note: "primary prod db", Type: "linux", Path: "/jails/db01", ConfigPath: "/etc/jail.conf.d/db01.conf", Running: true, StartupOrder: 1},
		{Name: "app01", Hostname: "app01.local", Note: "batch", Type: "vnet", Path: "/srv/app01", ConfigPath: "/etc/jail.conf.d/app01.conf", Running: true, StartupOrder: 2},
	}

	filtered := filterAndSortDashboardJails(jails, dashboardViewState{
		query:        "prod",
		statusFilter: dashboardStatusRunning,
		typeFilter:   dashboardTypeLinux,
		sortMode:     dashboardSortStartup,
	})
	if len(filtered) != 1 || filtered[0].Name != "db01" {
		t.Fatalf("unexpected filtered result: %+v", filtered)
	}

	sorted := filterAndSortDashboardJails(jails, dashboardViewState{sortMode: dashboardSortStartup})
	got := []string{sorted[0].Name, sorted[1].Name, sorted[2].Name}
	want := []string{"db01", "app01", "web02"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("startup sort order = %v, want %v", got, want)
	}
}

func TestFilterPaletteActionsFuzzyAndContext(t *testing.T) {
	m := model{
		mode: screenDashboard,
		snapshot: DashboardSnapshot{
			Jails: []Jail{{Name: "web01"}},
		},
		selectedJails: map[string]struct{}{"web01": {}},
	}

	actions := m.commandPaletteActions()
	foundBulkNote := false
	foundBulkSnapshot := false
	for _, action := range actions {
		if action.ID == paletteActionBulkNote {
			foundBulkNote = true
		}
		if action.ID == paletteActionBulkSnapshot {
			foundBulkSnapshot = true
		}
	}
	if !foundBulkNote || !foundBulkSnapshot {
		t.Fatalf("dashboard palette should include selected-jail bulk actions: %+v", actions)
	}

	filtered := filterPaletteActions(actions, "bulk snp")
	if len(filtered) == 0 || filtered[0].ID != paletteActionBulkSnapshot {
		t.Fatalf("unexpected fuzzy match result: %+v", filtered)
	}
}

func TestDashboardSearchAndFilterClearSelection(t *testing.T) {
	m := model{
		mode:          screenDashboard,
		selectedJails: map[string]struct{}{"web01": {}},
		snapshot: DashboardSnapshot{
			Jails: []Jail{{Name: "web01", Running: true}, {Name: "db01", Running: false}},
		},
	}

	updated, _ := m.updateDashboardKeys(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	current := updated.(model)
	if !current.dashboardView.searchMode {
		t.Fatalf("search mode was not enabled")
	}

	updated, _ = current.updateDashboardKeys(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'w'}})
	current = updated.(model)
	if current.dashboardView.query != "w" {
		t.Fatalf("search query = %q, want %q", current.dashboardView.query, "w")
	}
	if len(current.selectedJails) != 0 {
		t.Fatalf("selection was not cleared after search input")
	}

	updated, _ = current.updateDashboardKeys(tea.KeyMsg{Type: tea.KeyEsc})
	current = updated.(model)
	current.selectedJails["db01"] = struct{}{}
	updated, _ = current.updateDashboardKeys(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	current = updated.(model)
	if current.dashboardView.statusFilter != dashboardStatusRunning {
		t.Fatalf("status filter = %v, want %v", current.dashboardView.statusFilter, dashboardStatusRunning)
	}
	if len(current.selectedJails) != 0 {
		t.Fatalf("selection was not cleared after filter change")
	}
}

func TestDetailEditOpenAndCancel(t *testing.T) {
	m := model{
		mode: screenJailDetail,
		detail: JailDetail{
			Name:           "linux01",
			JailConfRaw:    []string{"linux01 {", "  path = \"/jails/linux01\";", "}"},
			JailConfValues: map[string]string{},
			SourceErrors:   map[string]string{},
		},
	}

	updated, _ := m.updateDetailKeys(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	current := updated.(model)
	if current.detailEdit.kind != detailEditNote {
		t.Fatalf("detail edit kind = %v, want %v", current.detailEdit.kind, detailEditNote)
	}

	updated, _ = current.updateDetailKeys(tea.KeyMsg{Type: tea.KeyEsc})
	current = updated.(model)
	if current.detailEdit.active() {
		t.Fatalf("detail edit should be canceled")
	}
}

func TestBulkActionApplyMsgSummaries(t *testing.T) {
	m := model{
		mode:          screenDashboard,
		selectedJails: map[string]struct{}{"web01": {}, "db01": {}},
		bulkAction:    bulkActionState{kind: bulkActionSnapshot, input: "daily"},
	}

	updated, _ := m.Update(bulkActionApplyMsg{
		result: bulkActionResult{
			Kind:  bulkActionSnapshot,
			Input: "daily",
			Results: []bulkActionTargetResult{
				{Name: "web01"},
				{Name: "db01", Reason: "no exact ZFS dataset"},
			},
		},
	})
	current := updated.(model)
	if !strings.Contains(current.notice, "Some jails were skipped") {
		t.Fatalf("notice = %q, want skipped summary", current.notice)
	}
	if len(current.activityEntries) == 0 || !strings.Contains(current.activityEntries[0].Message, "no exact ZFS dataset") {
		t.Fatalf("activity log did not capture skip reason: %+v", current.activityEntries)
	}
}
