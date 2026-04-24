package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type dashboardStatusFilter int

const (
	dashboardStatusAll dashboardStatusFilter = iota
	dashboardStatusRunning
	dashboardStatusStopped
)

func (f dashboardStatusFilter) label() string {
	switch f {
	case dashboardStatusRunning:
		return "running"
	case dashboardStatusStopped:
		return "stopped"
	default:
		return "all"
	}
}

func (f dashboardStatusFilter) next() dashboardStatusFilter {
	return (f + 1) % 3
}

type dashboardTypeFilter int

const (
	dashboardTypeAll dashboardTypeFilter = iota
	dashboardTypeThick
	dashboardTypeThin
	dashboardTypeVNET
	dashboardTypeLinux
)

func (f dashboardTypeFilter) label() string {
	switch f {
	case dashboardTypeThick:
		return "thick"
	case dashboardTypeThin:
		return "thin"
	case dashboardTypeVNET:
		return "vnet"
	case dashboardTypeLinux:
		return "linux"
	default:
		return "all"
	}
}

func (f dashboardTypeFilter) next() dashboardTypeFilter {
	return (f + 1) % 5
}

type dashboardSortMode int

const (
	dashboardSortName dashboardSortMode = iota
	dashboardSortStatus
	dashboardSortStartup
	dashboardSortType
)

func (m dashboardSortMode) label() string {
	switch m {
	case dashboardSortStatus:
		return "status"
	case dashboardSortStartup:
		return "startup_order"
	case dashboardSortType:
		return "type"
	default:
		return "name"
	}
}

func (m dashboardSortMode) next() dashboardSortMode {
	return (m + 1) % 4
}

type dashboardViewState struct {
	searchMode   bool
	query        string
	statusFilter dashboardStatusFilter
	typeFilter   dashboardTypeFilter
	sortMode     dashboardSortMode
}

func filterAndSortDashboardJails(jails []Jail, state dashboardViewState) []Jail {
	filtered := make([]Jail, 0, len(jails))
	for _, jail := range jails {
		if !dashboardMatchesStatusFilter(jail, state.statusFilter) {
			continue
		}
		if !dashboardMatchesTypeFilter(jail, state.typeFilter) {
			continue
		}
		if !dashboardJailMatchesQuery(jail, state.query) {
			continue
		}
		filtered = append(filtered, jail)
	}
	sort.Slice(filtered, func(i, j int) bool {
		left := filtered[i]
		right := filtered[j]
		switch state.sortMode {
		case dashboardSortStatus:
			if left.Running != right.Running {
				return left.Running
			}
		case dashboardSortStartup:
			if left.StartupOrder == 0 && right.StartupOrder != 0 {
				return false
			}
			if left.StartupOrder != 0 && right.StartupOrder == 0 {
				return true
			}
			if left.StartupOrder != right.StartupOrder {
				return left.StartupOrder < right.StartupOrder
			}
		case dashboardSortType:
			if normalizedJailType(left.Type) != normalizedJailType(right.Type) {
				return normalizedJailType(left.Type) < normalizedJailType(right.Type)
			}
		}
		return strings.ToLower(left.Name) < strings.ToLower(right.Name)
	})
	return filtered
}

func dashboardMatchesStatusFilter(jail Jail, filter dashboardStatusFilter) bool {
	switch filter {
	case dashboardStatusRunning:
		return jail.Running
	case dashboardStatusStopped:
		return !jail.Running
	default:
		return true
	}
}

func dashboardMatchesTypeFilter(jail Jail, filter dashboardTypeFilter) bool {
	switch filter {
	case dashboardTypeThick:
		return normalizedJailType(jail.Type) == "thick"
	case dashboardTypeThin:
		return normalizedJailType(jail.Type) == "thin"
	case dashboardTypeVNET:
		return normalizedJailType(jail.Type) == "vnet"
	case dashboardTypeLinux:
		return normalizedJailType(jail.Type) == "linux"
	default:
		return true
	}
}

func dashboardJailMatchesQuery(jail Jail, query string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return true
	}
	haystack := []string{
		jail.Name,
		jail.Hostname,
		jail.Note,
		jail.Type,
		jail.Path,
		jail.ConfigPath,
	}
	for _, candidate := range haystack {
		if strings.Contains(strings.ToLower(strings.TrimSpace(candidate)), query) {
			return true
		}
	}
	return false
}

type detailEditKind int

const (
	detailEditNone detailEditKind = iota
	detailEditNote
	detailEditRctl
	detailEditHostname
	detailEditStartupOrder
	detailEditDependencies
	detailEditLinuxMetadata
)

func (k detailEditKind) title() string {
	switch k {
	case detailEditNote:
		return "note"
	case detailEditRctl:
		return "resource limits"
	case detailEditHostname:
		return "hostname"
	case detailEditStartupOrder:
		return "startup order"
	case detailEditDependencies:
		return "dependencies"
	case detailEditLinuxMetadata:
		return "linux metadata"
	default:
		return "edit"
	}
}

type detailEditFieldSpec struct {
	id          string
	label       string
	allowSpaces bool
}

type detailEditState struct {
	kind   detailEditKind
	saving bool
	field  int
	values jailWizardValues
}

func newDetailEditState(kind detailEditKind, detail JailDetail) detailEditState {
	state := detailEditState{
		kind: kind,
	}
	switch kind {
	case detailEditNote:
		state.values.Note = strings.TrimSpace(detail.Note)
	case detailEditRctl:
		state.values = detailRctlValuesFromDetail(detail)
	case detailEditHostname:
		state.values.Hostname = strings.TrimSpace(detail.JailConfValues["host.hostname"])
	case detailEditStartupOrder:
		if detail.StartupConfig != nil && detail.StartupConfig.InJailList {
			state.values.StartupOrder = fmt.Sprintf("%d", detail.StartupConfig.Position)
		}
	case detailEditDependencies:
		if detail.StartupConfig != nil {
			state.values.Dependencies = strings.Join(detail.StartupConfig.Dependencies, " ")
		} else {
			state.values.Dependencies = strings.TrimSpace(detail.JailConfValues["depend"])
		}
	case detailEditLinuxMetadata:
		state.values = linuxBootstrapConfigFromRawLines(detail.JailConfRaw)
	}
	return state
}

func (s detailEditState) active() bool {
	return s.kind != detailEditNone
}

func (s detailEditState) title() string {
	switch s.kind {
	case detailEditNote:
		return "Note editor"
	case detailEditRctl:
		return "Resource limit editor"
	case detailEditHostname:
		return "Hostname editor"
	case detailEditStartupOrder:
		return "Startup order editor"
	case detailEditDependencies:
		return "Dependency editor"
	case detailEditLinuxMetadata:
		return "Linux metadata editor"
	default:
		return ""
	}
}

func (s detailEditState) hint() string {
	if !s.active() {
		return ""
	}
	if len(detailEditFieldSpecs(s.kind)) > 1 {
		return "tab or j/k: switch field | enter: save | backspace: delete | esc: cancel | ctrl+c: quit"
	}
	return "type to edit | enter: save | backspace: delete | esc: cancel | ctrl+c: quit"
}

func detailEditFieldSpecs(kind detailEditKind) []detailEditFieldSpec {
	switch kind {
	case detailEditNote:
		return []detailEditFieldSpec{{id: "note", label: "Note", allowSpaces: true}}
	case detailEditRctl:
		return []detailEditFieldSpec{
			{id: "cpu_percent", label: "CPU %"},
			{id: "memory_limit", label: "Memory"},
			{id: "process_limit", label: "Max processes"},
		}
	case detailEditHostname:
		return []detailEditFieldSpec{{id: "hostname", label: "Hostname"}}
	case detailEditStartupOrder:
		return []detailEditFieldSpec{{id: "startup_order", label: "Startup order"}}
	case detailEditDependencies:
		return []detailEditFieldSpec{{id: "dependencies", label: "Dependencies", allowSpaces: true}}
	case detailEditLinuxMetadata:
		return []detailEditFieldSpec{
			{id: "linux_preset", label: "Bootstrap preset"},
			{id: "linux_distro", label: "Bootstrap family"},
			{id: "linux_bootstrap_method", label: "Bootstrap method"},
			{id: "linux_release", label: "Bootstrap release"},
			{id: "linux_bootstrap", label: "Bootstrap mode"},
			{id: "linux_mirror_mode", label: "Mirror mode"},
			{id: "linux_mirror_url", label: "Mirror URL"},
			{id: "linux_archive_url", label: "Archive source"},
		}
	default:
		return nil
	}
}

func (s *detailEditState) valuePtr(idx int) *string {
	fields := detailEditFieldSpecs(s.kind)
	if idx < 0 || idx >= len(fields) {
		return nil
	}
	switch fields[idx].id {
	case "note":
		return &s.values.Note
	case "cpu_percent":
		return &s.values.CPUPercent
	case "memory_limit":
		return &s.values.MemoryLimit
	case "process_limit":
		return &s.values.ProcessLimit
	case "hostname":
		return &s.values.Hostname
	case "startup_order":
		return &s.values.StartupOrder
	case "dependencies":
		return &s.values.Dependencies
	case "linux_preset":
		return &s.values.LinuxPreset
	case "linux_distro":
		return &s.values.LinuxDistro
	case "linux_bootstrap_method":
		return &s.values.LinuxBootstrapMethod
	case "linux_release":
		return &s.values.LinuxRelease
	case "linux_bootstrap":
		return &s.values.LinuxBootstrap
	case "linux_mirror_mode":
		return &s.values.LinuxMirrorMode
	case "linux_mirror_url":
		return &s.values.LinuxMirrorURL
	case "linux_archive_url":
		return &s.values.LinuxArchiveURL
	default:
		return nil
	}
}

func (s *detailEditState) appendInput(text string) {
	field := s.valuePtr(s.field)
	if field == nil || text == "" {
		return
	}
	specs := detailEditFieldSpecs(s.kind)
	allowSpaces := specs[s.field].allowSpaces
	for _, r := range text {
		if r == '\n' || r == '\r' {
			continue
		}
		if !allowSpaces && (r == ' ' || r == '\t') {
			continue
		}
		if s.kind == detailEditNote && jailNoteLength(*field) >= maxJailNoteLen {
			break
		}
		*field += string(r)
	}
}

func (s *detailEditState) backspace() {
	field := s.valuePtr(s.field)
	if field == nil {
		return
	}
	runes := []rune(*field)
	if len(runes) == 0 {
		return
	}
	*field = string(runes[:len(runes)-1])
}

func (s *detailEditState) nextField() {
	fields := detailEditFieldSpecs(s.kind)
	if len(fields) == 0 {
		return
	}
	s.field++
	if s.field >= len(fields) {
		s.field = 0
	}
}

func (s *detailEditState) prevField() {
	fields := detailEditFieldSpecs(s.kind)
	if len(fields) == 0 {
		return
	}
	s.field--
	if s.field < 0 {
		s.field = len(fields) - 1
	}
}

func detailEditSuccessMessage(kind detailEditKind, values jailWizardValues) string {
	switch kind {
	case detailEditNote:
		if strings.TrimSpace(values.Note) == "" {
			return "Jail note cleared."
		}
		return "Jail note updated."
	case detailEditRctl:
		return "Resource limits updated."
	case detailEditHostname:
		if strings.TrimSpace(values.Hostname) == "" {
			return "Hostname cleared."
		}
		return "Hostname updated."
	case detailEditStartupOrder:
		if strings.TrimSpace(values.StartupOrder) == "" {
			return "Startup order updated."
		}
		return "Startup order updated."
	case detailEditDependencies:
		if strings.TrimSpace(values.Dependencies) == "" {
			return "Dependencies cleared."
		}
		return "Dependencies updated."
	case detailEditLinuxMetadata:
		return "Linux metadata updated."
	default:
		return "Changes saved."
	}
}

type bulkActionKind int

const (
	bulkActionNone bulkActionKind = iota
	bulkActionNote
	bulkActionSnapshot
)

type bulkActionState struct {
	kind     bulkActionKind
	input    string
	applying bool
}

func (s bulkActionState) active() bool {
	return s.kind != bulkActionNone
}

func (s bulkActionState) title() string {
	switch s.kind {
	case bulkActionNote:
		return "Bulk note apply/clear"
	case bulkActionSnapshot:
		return "Bulk snapshot create"
	default:
		return ""
	}
}

type activityStatus int

const (
	activityStatusSuccess activityStatus = iota
	activityStatusWarning
	activityStatusError
)

type activityEntry struct {
	At        time.Time
	Action    string
	Target    string
	Status    activityStatus
	Message   string
	Checklist []string
	Logs      []string
}

type activityLogState struct {
	cursor int
}

type paletteActionID string

const (
	paletteActionDashboard         paletteActionID = "dashboard"
	paletteActionHelp              paletteActionID = "help"
	paletteActionCreate            paletteActionID = "create"
	paletteActionTemplateManager   paletteActionID = "template_manager"
	paletteActionRefresh           paletteActionID = "refresh"
	paletteActionInitialCheck      paletteActionID = "initial_check"
	paletteActionOpenDetail        paletteActionID = "open_detail"
	paletteActionToggleService     paletteActionID = "toggle_service"
	paletteActionRestart           paletteActionID = "restart"
	paletteActionUpgrade           paletteActionID = "upgrade"
	paletteActionDestroy           paletteActionID = "destroy"
	paletteActionZFS               paletteActionID = "zfs"
	paletteActionEditNote          paletteActionID = "edit_note"
	paletteActionEditRctl          paletteActionID = "edit_rctl"
	paletteActionEditHostname      paletteActionID = "edit_hostname"
	paletteActionEditStartup       paletteActionID = "edit_startup"
	paletteActionEditDependencies  paletteActionID = "edit_dependencies"
	paletteActionEditLinuxMetadata paletteActionID = "edit_linux_metadata"
	paletteActionBulkToggleService paletteActionID = "bulk_toggle_service"
	paletteActionBulkRestart       paletteActionID = "bulk_restart"
	paletteActionBulkNote          paletteActionID = "bulk_note"
	paletteActionBulkSnapshot      paletteActionID = "bulk_snapshot"
	paletteActionActivityLog       paletteActionID = "activity_log"
)

type paletteAction struct {
	ID          paletteActionID
	Label       string
	Description string
	Aliases     []string
}

type commandPaletteState struct {
	active bool
	query  string
	cursor int
}

func (s *commandPaletteState) appendInput(text string) {
	if text == "" {
		return
	}
	s.query += text
}

func (s *commandPaletteState) backspace() {
	runes := []rune(s.query)
	if len(runes) == 0 {
		return
	}
	s.query = string(runes[:len(runes)-1])
}

func filterPaletteActions(actions []paletteAction, query string) []paletteAction {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return append([]paletteAction(nil), actions...)
	}
	type scoredAction struct {
		action paletteAction
		score  int
	}
	scored := make([]scoredAction, 0, len(actions))
	for _, action := range actions {
		best := fuzzyMatchScore(strings.ToLower(action.Label), query)
		if best < 0 {
			best = fuzzyMatchScore(strings.ToLower(action.Description), query)
		}
		for _, alias := range action.Aliases {
			score := fuzzyMatchScore(strings.ToLower(alias), query)
			if score > best {
				best = score
			}
		}
		if best >= 0 {
			scored = append(scored, scoredAction{action: action, score: best})
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return strings.ToLower(scored[i].action.Label) < strings.ToLower(scored[j].action.Label)
	})
	filtered := make([]paletteAction, 0, len(scored))
	for _, item := range scored {
		filtered = append(filtered, item.action)
	}
	return filtered
}

func fuzzyMatchScore(text, query string) int {
	if query == "" {
		return 0
	}
	idx := 0
	score := 0
	last := -1
	for _, r := range query {
		pos := strings.IndexRune(text[idx:], r)
		if pos < 0 {
			return -1
		}
		abs := idx + pos
		score++
		if last >= 0 && abs == last+1 {
			score += 2
		}
		last = abs
		idx = abs + 1
	}
	if strings.Contains(text, query) {
		score += 4
	}
	return score
}

func (m model) visibleJails() []Jail {
	return filterAndSortDashboardJails(m.snapshot.Jails, m.dashboardView)
}

func (m *model) clearDashboardSelection() {
	m.selectedJails = make(map[string]struct{})
}

func (m *model) boundActivityCursor() {
	if len(m.activityEntries) == 0 {
		m.activity.cursor = 0
		return
	}
	if m.activity.cursor < 0 {
		m.activity.cursor = 0
	}
	if m.activity.cursor >= len(m.activityEntries) {
		m.activity.cursor = len(m.activityEntries) - 1
	}
}

func (m *model) addActivityEntry(action, target, message string, status activityStatus, logs []string) {
	m.addActivityEntryDetailed(action, target, message, status, logs, nil)
}

func (m *model) addActivityEntryDetailed(action, target, message string, status activityStatus, logs []string, checklist []string) {
	entry := activityEntry{
		At:        time.Now(),
		Action:    strings.TrimSpace(action),
		Target:    strings.TrimSpace(target),
		Status:    status,
		Message:   strings.TrimSpace(message),
		Checklist: append([]string(nil), checklist...),
		Logs:      append([]string(nil), logs...),
	}
	m.activityEntries = append([]activityEntry{entry}, m.activityEntries...)
	m.boundActivityCursor()
}

func (m model) renderActivityLogView() string {
	title := titleStyle.Render("Activity / History")
	meta := summaryStyle.Render(fmt.Sprintf("Entries:%d", len(m.activityEntries)))
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)
	hint := "j/k: select | enter/right: view details | esc/left: back | A: close | ctrl+c: quit"
	footer := m.renderFooterWithMessage(hint, "", footerStyle)
	bodyHeight := m.pageBodyHeight(header, footer, 0)
	leftWidth := max(28, m.width/3)
	if leftWidth >= m.width {
		leftWidth = m.width
	}
	rightWidth := m.width - leftWidth - 1
	if rightWidth < 0 {
		rightWidth = 0
	}

	listLines := []string{panelTitleStyle.Render("Recent activity")}
	if len(m.activityEntries) == 0 {
		listLines = append(listLines, "No activity recorded in this session.")
	} else {
		for idx, entry := range m.activityEntries[:min(len(m.activityEntries), max(1, bodyHeight-2))] {
			status := "ok"
			if entry.Status == activityStatusWarning {
				status = "warn"
			} else if entry.Status == activityStatusError {
				status = "error"
			}
			line := fmt.Sprintf("%s %s %s", entry.At.Format("15:04:05"), status, entry.Action)
			line = truncate(line, max(1, leftWidth-2))
			if idx == m.activity.cursor {
				line = selectedRowStyle.Width(max(1, leftWidth-2)).Render(line)
			}
			listLines = append(listLines, line)
		}
	}
	listPanel := lipgloss.NewStyle().Width(leftWidth).Height(bodyHeight).Padding(0, 1).Render(strings.Join(listLines, "\n"))
	if rightWidth <= 0 {
		return lipgloss.JoinVertical(lipgloss.Left, header, "", listPanel, footer)
	}

	detailLines := []string{panelTitleStyle.Render("Selected entry")}
	if len(m.activityEntries) == 0 {
		detailLines = append(detailLines, "Select an entry once activity exists.")
	} else {
		entry := m.activityEntries[m.activity.cursor]
		summary := summarizeActivityLogs(entry.Logs)
		detailLines = append(detailLines, renderKeyValueLines(max(12, rightWidth-2),
			[2]string{"Time", entry.At.Format(time.RFC3339)},
			[2]string{"Action", entry.Action},
			[2]string{"Target", valueOrDash(entry.Target)},
			[2]string{"Status", valueOrDash(activityStatusLabel(entry.Status))},
			[2]string{"Commands", fmt.Sprintf("%d", len(summary.Commands))},
			[2]string{"Files touched", fmt.Sprintf("%d", len(summary.FilesTouched))},
			[2]string{"Rollback warnings", fmt.Sprintf("%d", len(summary.RollbackWarnings))},
		)...)
		if entry.Message != "" {
			appendSection(&detailLines, max(12, rightWidth-2), "Message")
			for _, line := range wrapText(entry.Message, max(12, rightWidth-2)) {
				detailLines = append(detailLines, truncate(line, max(12, rightWidth-2)))
			}
		}
		if len(entry.Checklist) > 0 {
			appendSection(&detailLines, max(12, rightWidth-2), "Next required actions")
			for _, item := range entry.Checklist {
				for _, line := range wrapText("- "+item, max(12, rightWidth-2)) {
					detailLines = append(detailLines, truncate(line, max(12, rightWidth-2)))
				}
			}
		}
		if len(summary.Commands) > 0 {
			appendSection(&detailLines, max(12, rightWidth-2), "Commands run")
			for _, line := range summary.Commands {
				detailLines = append(detailLines, truncate(line, max(12, rightWidth-2)))
			}
		}
		if len(summary.FilesTouched) > 0 {
			appendSection(&detailLines, max(12, rightWidth-2), "Files touched")
			for _, line := range summary.FilesTouched {
				detailLines = append(detailLines, truncate(line, max(12, rightWidth-2)))
			}
		}
		if len(summary.RollbackWarnings) > 0 {
			appendSection(&detailLines, max(12, rightWidth-2), "Rollback warnings")
			for _, line := range summary.RollbackWarnings {
				detailLines = append(detailLines, truncate(line, max(12, rightWidth-2)))
			}
		}
		if len(entry.Logs) > 0 {
			appendSection(&detailLines, max(12, rightWidth-2), "Full logs")
			for _, line := range entry.Logs {
				detailLines = append(detailLines, truncate(line, max(12, rightWidth-2)))
			}
		}
	}
	detailPanel := lipgloss.NewStyle().Width(rightWidth).Height(bodyHeight).Padding(0, 1).Render(strings.Join(detailLines, "\n"))
	separator := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(strings.Repeat("|\n", bodyHeight-1) + "|")
	body := lipgloss.JoinHorizontal(lipgloss.Top, listPanel, separator, detailPanel)
	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
}

func activityStatusLabel(status activityStatus) string {
	switch status {
	case activityStatusWarning:
		return "warning"
	case activityStatusError:
		return "error"
	default:
		return "success"
	}
}

func (m model) renderCommandPaletteView() string {
	title := titleStyle.Render("Command Palette")
	header := headerBarStyle.Width(m.width).Render(title)
	hint := "type to search | j/k: select | enter: run | esc: cancel | ctrl+c: quit"
	footer := m.renderFooterWithMessage(hint, "", footerStyle)
	bodyHeight := m.pageBodyHeight(header, footer, 0)
	actions := filterPaletteActions(m.commandPaletteActions(), m.commandPalette.query)
	lines := []string{
		panelTitleStyle.Render("Query"),
		truncate("> "+m.commandPalette.query, max(12, m.width-2)),
		"",
		panelTitleStyle.Render("Actions"),
	}
	if len(actions) == 0 {
		lines = append(lines, "No matching actions.")
	} else {
		for idx, action := range actions[:min(len(actions), max(1, bodyHeight-7))] {
			line := action.Label
			if idx == m.commandPalette.cursor {
				line = selectedRowStyle.Width(max(1, m.width-2)).Render(truncate("> "+line, max(1, m.width-2)))
			} else {
				line = truncate("  "+line, max(1, m.width-2))
			}
			lines = append(lines, line)
		}
		selected := actions[m.commandPalette.cursor]
		lines = append(lines, "", panelTitleStyle.Render("Description"))
		for _, line := range wrapText(selected.Description, max(12, m.width-2)) {
			lines = append(lines, truncate(line, max(12, m.width-2)))
		}
	}
	body := lipgloss.NewStyle().Width(m.width).Height(bodyHeight).Padding(0, 1).Render(strings.Join(lines, "\n"))
	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
}

func (m *model) updateActivityLogKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "left", "A":
		m.mode = screenDashboard
	case "j", "down":
		m.activity.cursor++
	case "k", "up":
		m.activity.cursor--
	}
	m.boundActivityCursor()
	return m, nil
}

func (m *model) updateCommandPaletteKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	actions := filterPaletteActions(m.commandPaletteActions(), m.commandPalette.query)
	switch msg.String() {
	case "esc":
		m.commandPalette = commandPaletteState{}
		return m, nil
	case "j", "down":
		m.commandPalette.cursor++
	case "k", "up":
		m.commandPalette.cursor--
	case "backspace", "delete":
		m.commandPalette.backspace()
	case "enter":
		if len(actions) == 0 {
			return m, nil
		}
		action := actions[m.commandPalette.cursor]
		m.commandPalette = commandPaletteState{}
		return m.executePaletteAction(action.ID)
	default:
		if msg.Type == tea.KeyRunes {
			m.commandPalette.appendInput(string(msg.Runes))
		}
	}
	actions = filterPaletteActions(m.commandPaletteActions(), m.commandPalette.query)
	if len(actions) == 0 {
		m.commandPalette.cursor = 0
		return m, nil
	}
	if m.commandPalette.cursor < 0 {
		m.commandPalette.cursor = 0
	}
	if m.commandPalette.cursor >= len(actions) {
		m.commandPalette.cursor = len(actions) - 1
	}
	return m, nil
}

func (m model) commandPaletteActions() []paletteAction {
	actions := []paletteAction{
		{ID: paletteActionDashboard, Label: "Open Dashboard", Description: "Return to the dashboard view.", Aliases: []string{"home dashboard"}},
		{ID: paletteActionHelp, Label: "Open Help", Description: "Open the help screen for the current context.", Aliases: []string{"help shortcuts"}},
		{ID: paletteActionCreate, Label: "Open Create Wizard", Description: "Start the jail creation wizard.", Aliases: []string{"create jail wizard"}},
		{ID: paletteActionTemplateManager, Label: "Open Template Manager", Description: "Open reusable template dataset management.", Aliases: []string{"templates datasets"}},
		{ID: paletteActionRefresh, Label: "Refresh Current View", Description: "Refresh the current dashboard or detail data.", Aliases: []string{"reload poll refresh"}},
		{ID: paletteActionInitialCheck, Label: "Open Initial Check", Description: "Reopen the initial configuration check.", Aliases: []string{"setup config"}},
		{ID: paletteActionActivityLog, Label: "Open Activity Log", Description: "Review actions from this app session.", Aliases: []string{"history log activity"}},
	}
	if jail, ok := m.selectedJail(); ok && m.mode == screenDashboard {
		actions = append(actions,
			paletteAction{ID: paletteActionOpenDetail, Label: "Open Jail Detail", Description: "Open detail view for the highlighted jail.", Aliases: []string{"detail inspect"}},
			paletteAction{ID: paletteActionToggleService, Label: "Start/Stop Jail", Description: "Toggle the highlighted jail's running state.", Aliases: []string{"service toggle"}},
			paletteAction{ID: paletteActionRestart, Label: "Restart Jail", Description: "Restart the highlighted jail.", Aliases: []string{"service restart"}},
			paletteAction{ID: paletteActionUpgrade, Label: "Open Upgrade Wizard", Description: "Open the upgrade wizard for the highlighted jail.", Aliases: []string{"upgrade"}},
			paletteAction{ID: paletteActionDestroy, Label: "Open Destroy Confirmation", Description: "Open destroy confirmation for the highlighted jail.", Aliases: []string{"destroy remove"}},
			paletteAction{ID: paletteActionZFS, Label: "Open ZFS Panel", Description: "Inspect ZFS snapshots and properties for the highlighted jail.", Aliases: []string{"zfs snapshots"}},
		)
		if len(m.selectedJails) > 0 {
			actions = append(actions,
				paletteAction{ID: paletteActionBulkToggleService, Label: "Bulk Start/Stop Selected", Description: "Toggle running state for the selected jails.", Aliases: []string{"bulk toggle service"}},
				paletteAction{ID: paletteActionBulkRestart, Label: "Bulk Restart Selected", Description: "Restart the selected jails.", Aliases: []string{"bulk restart"}},
				paletteAction{ID: paletteActionBulkNote, Label: "Bulk Apply/Clear Note", Description: "Apply one note to all selected jails or clear it by saving blank.", Aliases: []string{"bulk note"}},
				paletteAction{ID: paletteActionBulkSnapshot, Label: "Bulk Snapshot Create", Description: "Create one ZFS snapshot across selected exact-dataset jails.", Aliases: []string{"bulk snapshot"}},
			)
		}
		_ = jail
	}
	if m.mode == screenJailDetail && strings.TrimSpace(m.detail.Name) != "" {
		actions = append(actions,
			paletteAction{ID: paletteActionEditNote, Label: "Edit Jail Note", Description: "Edit the managed short note for this jail.", Aliases: []string{"note"}},
			paletteAction{ID: paletteActionEditRctl, Label: "Edit Resource Limits", Description: "Edit managed rctl limits for this jail.", Aliases: []string{"rctl limits"}},
			paletteAction{ID: paletteActionEditHostname, Label: "Edit Hostname", Description: "Edit the jail's host.hostname value.", Aliases: []string{"hostname"}},
			paletteAction{ID: paletteActionEditStartup, Label: "Edit Startup Order", Description: "Edit the jail_list position used at startup.", Aliases: []string{"startup"}},
			paletteAction{ID: paletteActionEditDependencies, Label: "Edit Dependencies", Description: "Edit the jail depend list.", Aliases: []string{"depend dependencies"}},
			paletteAction{ID: paletteActionToggleService, Label: "Start/Stop Jail", Description: "Toggle this jail's running state.", Aliases: []string{"service toggle"}},
			paletteAction{ID: paletteActionRestart, Label: "Restart Jail", Description: "Restart this jail.", Aliases: []string{"service restart"}},
			paletteAction{ID: paletteActionUpgrade, Label: "Open Upgrade Wizard", Description: "Open the upgrade wizard for this jail.", Aliases: []string{"upgrade"}},
			paletteAction{ID: paletteActionDestroy, Label: "Open Destroy Confirmation", Description: "Open destroy confirmation for this jail.", Aliases: []string{"destroy remove"}},
			paletteAction{ID: paletteActionZFS, Label: "Open ZFS Panel", Description: "Inspect ZFS state for this jail.", Aliases: []string{"zfs snapshots"}},
		)
		if detailLooksLikeLinuxJail(m.detail) {
			actions = append(actions, paletteAction{
				ID:          paletteActionEditLinuxMetadata,
				Label:       "Edit Linux Metadata",
				Description: "Edit managed Linux bootstrap metadata for this jail.",
				Aliases:     []string{"linux metadata bootstrap"},
			})
		}
	}
	return actions
}

func (m *model) executePaletteAction(id paletteActionID) (tea.Model, tea.Cmd) {
	switch id {
	case paletteActionDashboard:
		m.mode = screenDashboard
		return m, nil
	case paletteActionHelp:
		m.openHelp(m.mode)
		return m, nil
	case paletteActionCreate:
		m.mode = screenCreateWizard
		m.wizard = newJailCreationWizard(initialWizardDestination(m.initCheck.status))
		m.wizardScroll = 0
		m.notice = ""
		return m, nil
	case paletteActionTemplateManager:
		m.mode = screenTemplateDatasetCreate
		m.templateCreate = newTemplateDatasetCreateState("", m.initCheck.status, screenDashboard, false)
		m.templateCreate.loading = true
		m.notice = ""
		m.err = nil
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
	case paletteActionRefresh:
		if m.mode == screenJailDetail {
			jail, ok := m.detailJail()
			if !ok {
				return m, nil
			}
			m.detailLoading = true
			return m, detailCmd(jail)
		}
		return m, pollCmd()
	case paletteActionInitialCheck:
		m.mode = screenInitialCheck
		m.initCheck = newInitialCheckState()
		m.notice = ""
		m.err = nil
		return m, collectInitialConfigCmd()
	case paletteActionActivityLog:
		m.mode = screenActivityLog
		return m, nil
	case paletteActionOpenDetail:
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		m.openDetailView(jail)
		return m, detailCmd(jail)
	case paletteActionToggleService:
		if m.mode == screenJailDetail {
			jail, ok := m.detailJail()
			if !ok {
				return m, nil
			}
			action := "start"
			if jail.Running {
				action = "stop"
			}
			return m, jailServiceCmd(jail, action)
		}
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		action := "start"
		if jail.Running {
			action = "stop"
		}
		return m, jailServiceCmd(jail, action)
	case paletteActionRestart:
		if m.mode == screenJailDetail {
			jail, ok := m.detailJail()
			if !ok {
				return m, nil
			}
			return m, jailServiceCmd(jail, "restart")
		}
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		return m, jailServiceCmd(jail, "restart")
	case paletteActionUpgrade:
		jail, ok := m.currentActionJail()
		if !ok {
			return m, nil
		}
		m.upgrade = newUpgradeState(jail, m.mode)
		m.mode = screenUpgradeWizard
		return m, nil
	case paletteActionDestroy:
		jail, ok := m.currentActionJail()
		if !ok {
			return m, nil
		}
		m.destroy = newDestroyState(jail, m.mode)
		m.mode = screenDestroyConfirm
		return m, nil
	case paletteActionZFS:
		jail, ok := m.currentActionJail()
		if !ok {
			return m, nil
		}
		return m, openZFSPanelCmd(jail)
	case paletteActionEditNote:
		m.beginDetailEdit(detailEditNote)
		return m, nil
	case paletteActionEditRctl:
		m.beginDetailEdit(detailEditRctl)
		return m, nil
	case paletteActionEditHostname:
		m.beginDetailEdit(detailEditHostname)
		return m, nil
	case paletteActionEditStartup:
		m.beginDetailEdit(detailEditStartupOrder)
		return m, nil
	case paletteActionEditDependencies:
		m.beginDetailEdit(detailEditDependencies)
		return m, nil
	case paletteActionEditLinuxMetadata:
		m.beginDetailEdit(detailEditLinuxMetadata)
		return m, nil
	case paletteActionBulkToggleService:
		return m, bulkToggleServiceCmd(m.selectedJailList())
	case paletteActionBulkRestart:
		return m, bulkServiceCmd(m.selectedJailList(), "restart")
	case paletteActionBulkNote:
		m.bulkAction = bulkActionState{kind: bulkActionNote}
		m.notice = ""
		m.err = nil
		return m, nil
	case paletteActionBulkSnapshot:
		m.bulkAction = bulkActionState{kind: bulkActionSnapshot}
		m.notice = ""
		m.err = nil
		return m, nil
	default:
		return m, nil
	}
}

func (m model) currentActionJail() (Jail, bool) {
	if m.mode == screenJailDetail {
		return m.detailJail()
	}
	return m.selectedJail()
}

func (m *model) openDetailView(jail Jail) {
	m.mode = screenJailDetail
	m.detailLoading = true
	m.detailScroll = 0
	m.detailTab = detailTabSummary
	m.detailShowAdvanced = false
	m.detailErr = nil
	m.detailNotice = ""
	m.detailEdit = detailEditState{}
	m.detail = JailDetail{
		Name:                  jail.Name,
		JID:                   jail.JID,
		Path:                  jail.Path,
		Hostname:              jail.Hostname,
		Note:                  jail.Note,
		JLSFields:             map[string]string{},
		RuntimeValues:         map[string]string{},
		AdvancedRuntimeFields: map[string]string{},
		JailConfValues:        map[string]string{},
		SourceErrors:          map[string]string{},
	}
}

func (m *model) beginDetailEdit(kind detailEditKind) {
	if strings.TrimSpace(m.detail.Name) == "" {
		return
	}
	if kind == detailEditLinuxMetadata && !detailLooksLikeLinuxJail(m.detail) {
		return
	}
	m.detailEdit = newDetailEditState(kind, m.detail)
	m.detailNotice = ""
	m.detailErr = nil
}
