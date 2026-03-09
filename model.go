package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const pollInterval = 2 * time.Second

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("39")).
			Padding(0, 1)
	summaryStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("248"))
	panelTitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("45"))
	selectedRowStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("230")).
				Background(lipgloss.Color("24"))
	runningBadgeStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("82"))
	stoppedBadgeStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("196"))
	detailKeyStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("45"))
	footerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("244")).
			Padding(0, 1)
	sectionStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("45"))
)

type snapshotMsg struct {
	snapshot DashboardSnapshot
	err      error
}

type jailDetailMsg struct {
	detail JailDetail
	err    error
}

type tickMsg time.Time

type screenMode int

const (
	screenDashboard screenMode = iota
	screenJailDetail
	screenCreateWizard
)

type model struct {
	width  int
	height int

	cursor int
	offset int

	snapshot DashboardSnapshot
	err      error

	mode          screenMode
	detail        JailDetail
	detailErr     error
	detailLoading bool
	detailScroll  int
	wizard        jailCreationWizard
	notice        string
}

func newModel() model {
	return model{
		mode:   screenDashboard,
		wizard: newJailCreationWizard(),
	}
}

func pollCmd() tea.Cmd {
	return func() tea.Msg {
		snapshot, err := CollectSnapshot(time.Now())
		return snapshotMsg{snapshot: snapshot, err: err}
	}
}

func detailCmd(jail Jail) tea.Cmd {
	return func() tea.Msg {
		detail, err := CollectJailDetail(jail.Name, jail.JID, jail.Path, time.Now())
		return jailDetailMsg{detail: detail, err: err}
	}
}

func tickerCmd() tea.Cmd {
	return tea.Tick(pollInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) Init() tea.Cmd {
	return pollCmd()
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.boundDetailScroll()
		return m, nil
	case snapshotMsg:
		m.snapshot = msg.snapshot
		m.err = msg.err
		m.boundCursor()
		m.ensureCursorVisible(m.listHeight())
		m.boundDetailScroll()
		return m, tickerCmd()
	case jailDetailMsg:
		m.detail = msg.detail
		m.detailErr = msg.err
		m.detailLoading = false
		m.boundDetailScroll()
		return m, nil
	case tickMsg:
		return m, pollCmd()
	case tea.KeyMsg:
		if msg.String() == "q" || msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
		if m.mode == screenCreateWizard {
			return m.updateWizardKeys(msg)
		}
		if m.mode == screenJailDetail {
			return m.updateDetailKeys(msg)
		}
		return m.updateDashboardKeys(msg)
	}
	return m, nil
}

func (m model) updateDashboardKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "j", "down":
		m.cursor++
	case "k", "up":
		m.cursor--
	case "g", "home":
		m.cursor = 0
	case "G", "end":
		m.cursor = len(m.snapshot.Jails) - 1
	case "pgdown":
		m.cursor += m.listHeight()
	case "pgup":
		m.cursor -= m.listHeight()
	case "r":
		return m, pollCmd()
	case "c", "n":
		m.mode = screenCreateWizard
		m.wizard = newJailCreationWizard()
		m.notice = ""
		return m, nil
	case "enter", "d", "right":
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		m.mode = screenJailDetail
		m.detailLoading = true
		m.detailScroll = 0
		m.detailErr = nil
		m.detail = JailDetail{
			Name:           jail.Name,
			JID:            jail.JID,
			Path:           jail.Path,
			Hostname:       jail.Hostname,
			JLSFields:      map[string]string{},
			JailConfValues: map[string]string{},
			SourceErrors:   map[string]string{},
		}
		return m, detailCmd(jail)
	}
	m.boundCursor()
	m.ensureCursorVisible(m.listHeight())
	return m, nil
}

func (m model) updateDetailKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "backspace", "left":
		m.mode = screenDashboard
		return m, nil
	case "j", "down":
		m.detailScroll++
	case "k", "up":
		m.detailScroll--
	case "g", "home":
		m.detailScroll = 0
	case "G", "end":
		m.detailScroll = 1 << 30
	case "pgdown":
		m.detailScroll += m.detailBodyHeight()
	case "pgup":
		m.detailScroll -= m.detailBodyHeight()
	case "r":
		jail, ok := m.detailJail()
		if !ok {
			return m, nil
		}
		m.detailLoading = true
		m.detailErr = nil
		return m, detailCmd(jail)
	}
	m.boundDetailScroll()
	return m, nil
}

func (m model) updateWizardKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.mode = screenDashboard
		m.notice = "Jail creation canceled."
		return m, nil
	case "left":
		m.wizard.prevStep()
		return m, nil
	case "right":
		if err := m.wizard.nextStep(); err != nil {
			return m, nil
		}
		return m, nil
	case "tab", "down":
		m.wizard.nextField()
		return m, nil
	case "shift+tab", "up":
		m.wizard.prevField()
		return m, nil
	case "enter":
		if m.wizard.isConfirmationStep() {
			if err := m.wizard.validateAll(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			m.mode = screenDashboard
			m.notice = fmt.Sprintf("Creation plan prepared for jail %s.", m.wizard.values.Name)
			m.wizard = newJailCreationWizard()
			return m, nil
		}
		_ = m.wizard.nextStep()
		return m, nil
	case "backspace", "delete":
		m.wizard.backspaceActive()
		return m, nil
	}

	if msg.Type == tea.KeyRunes {
		m.wizard.appendToActive(string(msg.Runes))
	}
	return m, nil
}

func (m model) View() string {
	if m.width == 0 || m.height == 0 {
		return "Loading dashboard..."
	}
	if m.mode == screenCreateWizard {
		return m.renderWizardView()
	}
	if m.mode == screenJailDetail {
		return m.renderJailDetailView()
	}
	return m.renderDashboard()
}

func (m model) renderDashboard() string {
	header := m.renderHeader()
	bodyHeight := max(6, m.height-4)
	leftWidth := max(50, (m.width*2)/3)
	if leftWidth > m.width-24 {
		leftWidth = m.width - 24
	}
	if leftWidth < 32 {
		leftWidth = m.width
	}
	rightWidth := m.width - leftWidth - 1
	if rightWidth < 0 {
		rightWidth = 0
	}

	listPanel := m.renderJailList(leftWidth, bodyHeight)
	if rightWidth == 0 {
		footer := m.renderFooter()
		return lipgloss.JoinVertical(lipgloss.Left, header, listPanel, footer)
	}

	detailPanel := m.renderDetailPanel(rightWidth, bodyHeight)
	separator := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		Render(strings.Repeat("|\n", bodyHeight-1) + "|")

	body := lipgloss.JoinHorizontal(lipgloss.Top, listPanel, separator, detailPanel)
	footer := m.renderFooter()

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) renderJailDetailView() string {
	title := titleStyle.Render("Jail Detail View")
	name := m.detail.Name
	if name == "" {
		name = "Unknown"
	}
	lastUpdated := "never"
	if !m.detail.LastUpdated.IsZero() {
		lastUpdated = m.detail.LastUpdated.Format("15:04:05")
	}
	meta := summaryStyle.Render(fmt.Sprintf("Jail:%s  Updated:%s", name, lastUpdated))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := m.detailBodyHeight()
	contentWidth := max(12, m.width-2)
	lines := m.detailLines(contentWidth)
	maxOffset := max(0, len(lines)-bodyHeight)
	offset := m.detailScroll
	if offset < 0 {
		offset = 0
	}
	if offset > maxOffset {
		offset = maxOffset
	}
	end := min(len(lines), offset+bodyHeight)
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines[offset:end], "\n"))

	hint := "j/k or up/down: scroll | pgup/pgdown | g/G | r: refresh detail | esc: back | q: quit"
	if m.detailLoading {
		hint += " | loading detail..."
	}
	if m.detailErr != nil {
		hint += " | warning: " + m.detailErr.Error()
	}
	footer := footerStyle.Width(m.width).Render(hint)

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) renderWizardView() string {
	step := m.wizard.currentStep()
	title := titleStyle.Render("Jail Creation Wizard")
	meta := summaryStyle.Render(fmt.Sprintf("Step %d/%d: %s", m.wizard.step+1, len(wizardSteps), step.Title))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := max(4, m.height-3)
	lines := m.wizardLines(max(12, m.width-2))
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))

	hint := "type to edit | tab/shift+tab: fields | enter/right: next | left: back | esc: cancel | q: quit"
	if m.wizard.isConfirmationStep() {
		hint = "enter: finish wizard | left: back | esc: cancel | q: quit"
	}
	if m.wizard.message != "" {
		hint += " | " + m.wizard.message
	}
	footer := footerStyle.Width(m.width).Render(hint)

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) wizardLines(width int) []string {
	step := m.wizard.currentStep()
	lines := []string{sectionStyle.Render(step.Title)}
	if step.Description != "" {
		lines = append(lines, truncate(step.Description, width))
	}
	lines = append(lines, "")

	if m.wizard.isConfirmationStep() {
		lines = append(lines, sectionStyle.Render("Summary"))
		for _, line := range m.wizard.summaryLines() {
			lines = append(lines, truncate(line, width))
		}
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("jail.conf preview"))
		for _, line := range m.wizard.jailConfPreviewLines() {
			lines = append(lines, truncate(line, width))
		}
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Creation plan"))
		for _, line := range m.wizard.commandPlanLines() {
			lines = append(lines, truncate(line, width))
		}
		return lines
	}

	for idx, field := range step.Fields {
		value := m.wizard.valueByID(field.ID)
		display := value
		if strings.TrimSpace(display) == "" {
			display = "(" + field.Placeholder + ")"
		}
		prefix := " "
		if idx == m.wizard.field {
			prefix = ">"
		}
		line := fmt.Sprintf("%s %s: %s", prefix, field.Label, display)
		line = truncate(line, width)
		if idx == m.wizard.field {
			line = selectedRowStyle.Width(max(1, width)).Render(line)
		}
		lines = append(lines, line)
		if field.Help != "" {
			lines = append(lines, truncate("  "+field.Help, width))
		}
	}

	return lines
}

func (m model) detailLines(width int) []string {
	lines := make([]string, 0, 64)
	appendLine := func(text string) {
		lines = append(lines, truncate(text, max(1, width)))
	}

	jail, hasJail := m.detailJail()
	state := "STOPPED"
	jidText := "-"
	cpuText := "0.00%"
	memText := "0MB"
	if hasJail {
		if jail.Running || jail.JID > 0 {
			state = "RUNNING"
		}
		if jail.JID > 0 {
			jidText = strconv.Itoa(jail.JID)
		}
		cpuText = fmt.Sprintf("%.2f%%", jail.CPUPercent)
		memText = fmt.Sprintf("%dMB", jail.MemoryMB)
	}

	lines = append(lines, sectionStyle.Render("Overview"))
	appendLine("Name: " + valueOrDash(m.detail.Name))
	appendLine("State: " + state)
	appendLine("JID: " + jidText)
	appendLine("CPU: " + cpuText)
	appendLine("Memory: " + memText)
	appendLine("Path: " + valueOrDash(m.detail.Path))
	appendLine("Hostname: " + valueOrDash(m.detail.Hostname))
	appendLine("")

	lines = append(lines, sectionStyle.Render("jls"))
	if len(m.detail.JLSFields) == 0 {
		appendLine("No running jls record for this jail.")
	} else {
		for _, key := range sortedKeys(m.detail.JLSFields) {
			appendLine(fmt.Sprintf("%s = %s", key, m.detail.JLSFields[key]))
		}
	}
	appendLine("")

	lines = append(lines, sectionStyle.Render("jail.conf"))
	appendLine("Source: " + valueOrDash(m.detail.JailConfSource))
	if len(m.detail.JailConfValues) == 0 && len(m.detail.JailConfFlags) == 0 {
		appendLine("No matching jail block found.")
	} else {
		for _, key := range sortedKeys(m.detail.JailConfValues) {
			appendLine(fmt.Sprintf("%s = %s", key, m.detail.JailConfValues[key]))
		}
		for _, flag := range m.detail.JailConfFlags {
			appendLine(flag)
		}
	}
	appendLine("")

	lines = append(lines, sectionStyle.Render("ZFS dataset"))
	if m.detail.ZFS == nil {
		appendLine("No dataset matched the jail path.")
	} else {
		appendLine("Dataset: " + m.detail.ZFS.Name)
		appendLine("Mountpoint: " + m.detail.ZFS.Mountpoint)
		appendLine("Match: " + m.detail.ZFS.MatchType)
		appendLine("Used: " + m.detail.ZFS.Used)
		appendLine("Avail: " + m.detail.ZFS.Avail)
		appendLine("Refer: " + m.detail.ZFS.Refer)
		appendLine("Compression: " + m.detail.ZFS.Compression)
		appendLine("Quota: " + m.detail.ZFS.Quota)
		appendLine("Reservation: " + m.detail.ZFS.Reservation)
	}
	appendLine("")

	lines = append(lines, sectionStyle.Render("rctl"))
	if len(m.detail.RctlRules) == 0 {
		appendLine("No matching rctl rules.")
	} else {
		for _, rule := range m.detail.RctlRules {
			appendLine(rule)
		}
	}

	if len(m.detail.SourceErrors) > 0 {
		appendLine("")
		lines = append(lines, sectionStyle.Render("Source errors"))
		for _, source := range sortedKeys(m.detail.SourceErrors) {
			appendLine(fmt.Sprintf("%s: %s", source, m.detail.SourceErrors[source]))
		}
	}
	return lines
}

func (m model) detailJail() (Jail, bool) {
	if m.detail.Name == "" {
		return Jail{}, false
	}
	for _, jail := range m.snapshot.Jails {
		if jail.Name == m.detail.Name {
			return jail, true
		}
	}
	return Jail{
		Name:     m.detail.Name,
		JID:      m.detail.JID,
		Path:     m.detail.Path,
		Hostname: m.detail.Hostname,
		Running:  m.detail.JID > 0,
	}, true
}

func (m *model) boundDetailScroll() {
	if m.mode != screenJailDetail {
		return
	}
	if m.width <= 0 || m.height <= 0 {
		m.detailScroll = 0
		return
	}
	lines := m.detailLines(max(12, m.width-2))
	maxOffset := max(0, len(lines)-m.detailBodyHeight())
	if m.detailScroll < 0 {
		m.detailScroll = 0
	}
	if m.detailScroll > maxOffset {
		m.detailScroll = maxOffset
	}
}

func (m model) detailBodyHeight() int {
	return max(3, m.height-3)
}

func (m model) selectedJail() (Jail, bool) {
	if len(m.snapshot.Jails) == 0 {
		return Jail{}, false
	}
	idx := m.cursor
	if idx < 0 {
		idx = 0
	}
	if idx >= len(m.snapshot.Jails) {
		idx = len(m.snapshot.Jails) - 1
	}
	return m.snapshot.Jails[idx], true
}

func (m model) renderHeader() string {
	total := len(m.snapshot.Jails)
	lastUpdated := "never"
	if !m.snapshot.LastUpdated.IsZero() {
		lastUpdated = m.snapshot.LastUpdated.Format("15:04:05")
	}
	title := titleStyle.Render("FreeBSD Jails Dashboard")
	summary := summaryStyle.Render(fmt.Sprintf(
		"Total:%d  Running:%d  Stopped:%d  CPU:%0.2f%%  MEM:%dMB  Updated:%s",
		total,
		m.snapshot.RunningCount,
		m.snapshot.StoppedCount,
		m.snapshot.TotalCPUPercent,
		m.snapshot.TotalMemoryMB,
		lastUpdated,
	))
	return lipgloss.NewStyle().Width(m.width).Render(title + "  " + summary)
}

func (m model) renderFooter() string {
	hint := "j/k or up/down: scroll | g/G: top/bottom | enter/d: details | c: create wizard | r: refresh | q: quit"
	if m.notice != "" {
		hint += " | " + m.notice
	}
	if m.err != nil {
		hint += " | warning: " + m.err.Error()
	}
	return footerStyle.Width(m.width).Render(hint)
}

func (m model) renderJailList(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	contentHeight := max(1, height-2)
	marker := ""
	if m.offset > 0 {
		marker = " (scrollable)"
	}

	lines := []string{
		panelTitleStyle.Render("Jails" + marker),
		m.renderRows(contentHeight, width),
	}

	return lipgloss.NewStyle().
		Width(width).
		Height(height).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))
}

func (m model) renderRows(maxRows, width int) string {
	if len(m.snapshot.Jails) == 0 {
		return "No jails discovered. Add jails in /etc/jail.conf and run them with service jail start <name>."
	}
	start := m.offset
	end := min(len(m.snapshot.Jails), start+maxRows)
	rows := make([]string, 0, end-start)

	for idx := start; idx < end; idx++ {
		jail := m.snapshot.Jails[idx]
		prefix := " "
		if idx == m.cursor {
			prefix = ">"
		}
		jid := "-"
		if jail.JID > 0 {
			jid = strconv.Itoa(jail.JID)
		}

		line := fmt.Sprintf(
			"%s %s %-18s JID:%-5s CPU:%6.2f%% MEM:%5dMB",
			prefix,
			statusBadge(jail.Running),
			truncate(jail.Name, 18),
			jid,
			jail.CPUPercent,
			jail.MemoryMB,
		)
		line = truncate(line, max(1, width-3))
		if idx == m.cursor {
			line = selectedRowStyle.Width(max(1, width-2)).Render(line)
		}
		rows = append(rows, line)
	}
	return strings.Join(rows, "\n")
}

func (m model) renderDetailPanel(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	lines := []string{panelTitleStyle.Render("Quick Details")}
	if len(m.snapshot.Jails) == 0 {
		lines = append(lines, "Select a jail once discovered.")
	} else {
		j := m.snapshot.Jails[m.cursor]
		state := "STOPPED"
		jidText := "-"
		if j.Running {
			state = "RUNNING"
			jidText = strconv.Itoa(j.JID)
		}
		lines = append(lines,
			fmt.Sprintf("%s %s", detailKeyStyle.Render("Name:"), j.Name),
			fmt.Sprintf("%s %s", detailKeyStyle.Render("State:"), state),
			fmt.Sprintf("%s %s", detailKeyStyle.Render("JID:"), jidText),
			fmt.Sprintf("%s %.2f%%", detailKeyStyle.Render("CPU:"), j.CPUPercent),
			fmt.Sprintf("%s %dMB", detailKeyStyle.Render("Memory:"), j.MemoryMB),
			"Press enter for full detail view.",
		)
	}

	return lipgloss.NewStyle().
		Width(width).
		Height(height).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))
}

func (m *model) boundCursor() {
	if len(m.snapshot.Jails) == 0 {
		m.cursor = 0
		m.offset = 0
		return
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= len(m.snapshot.Jails) {
		m.cursor = len(m.snapshot.Jails) - 1
	}
}

func (m *model) ensureCursorVisible(visibleRows int) {
	if visibleRows <= 0 {
		return
	}
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+visibleRows {
		m.offset = m.cursor - visibleRows + 1
	}
	if m.offset < 0 {
		m.offset = 0
	}
	maxOffset := max(0, len(m.snapshot.Jails)-visibleRows)
	if m.offset > maxOffset {
		m.offset = maxOffset
	}
}

func (m model) listHeight() int {
	return max(1, m.height-6)
}

func statusBadge(running bool) string {
	if running {
		return runningBadgeStyle.Render("[+]")
	}
	return stoppedBadgeStyle.Render("[-]")
}

func valueOrDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func truncate(input string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if len(input) <= maxLen {
		return input
	}
	if maxLen <= 3 {
		return input[:maxLen]
	}
	return input[:maxLen-3] + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
