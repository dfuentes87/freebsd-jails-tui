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

type wizardApplyMsg struct {
	result JailCreationResult
}

type tickMsg time.Time

type screenMode int

const (
	screenDashboard screenMode = iota
	screenJailDetail
	screenCreateWizard
	screenZFSPanel
	screenHelp
)

type model struct {
	width  int
	height int

	cursor int
	offset int

	snapshot DashboardSnapshot
	err      error

	mode           screenMode
	detail         JailDetail
	detailErr      error
	detailLoading  bool
	detailScroll   int
	zfsPanel       zfsPanelState
	wizard         jailCreationWizard
	wizardApplying bool
	helpReturnMode screenMode
	helpScroll     int
	notice         string
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

func createJailCmd(values jailWizardValues) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteJailCreation(values)
		return wizardApplyMsg{result: result}
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
		m.boundHelpScroll()
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
	case zfsSnapshotListMsg:
		if m.mode != screenZFSPanel {
			return m, nil
		}
		m.zfsPanel.loading = false
		m.zfsPanel.snapshots = msg.snapshots
		m.zfsPanel.err = msg.err
		m.zfsPanel.message = msg.message
		m.zfsPanel.boundCursor(m.zfsListHeight())
		return m, nil
	case zfsActionMsg:
		if m.mode != screenZFSPanel {
			return m, nil
		}
		m.zfsPanel.actionRunning = false
		m.zfsPanel.logs = msg.logs
		m.zfsPanel.err = msg.err
		m.zfsPanel.message = msg.message
		return m, listZFSSnapshotsCmd(m.zfsPanel.dataset)
	case wizardApplyMsg:
		m.wizardApplying = false
		m.wizard.setExecutionResult(msg.result)
		if msg.result.Err == nil {
			m.mode = screenDashboard
			m.notice = fmt.Sprintf("Jail %s created and started.", msg.result.Name)
			m.wizard = newJailCreationWizard()
			return m, pollCmd()
		}
		return m, nil
	case tickMsg:
		return m, pollCmd()
	case tea.KeyMsg:
		key := msg.String()
		if key == "ctrl+c" {
			return m, tea.Quit
		}
		if key == "q" && !m.isTextEntryMode() {
			return m, tea.Quit
		}
		if m.mode == screenHelp {
			return m.updateHelpKeys(msg)
		}
		if (key == "?" || key == "h") && !m.isTextEntryMode() {
			m.helpReturnMode = m.mode
			m.helpScroll = 0
			m.mode = screenHelp
			return m, nil
		}
		if m.mode == screenZFSPanel {
			return m.updateZFSPanelKeys(msg)
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

func (m model) isTextEntryMode() bool {
	switch m.mode {
	case screenCreateWizard:
		if m.wizardApplying {
			return false
		}
		if m.wizard.templateMode == wizardTemplateModeSave {
			return true
		}
		if m.wizard.templateMode == wizardTemplateModeLoad {
			return false
		}
		return !m.wizard.isConfirmationStep()
	case screenZFSPanel:
		return m.zfsPanel.inputMode
	default:
		return false
	}
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
	case "z":
		if m.detail.ZFS == nil || strings.TrimSpace(m.detail.ZFS.Name) == "" {
			m.detailErr = fmt.Errorf("no ZFS dataset detected for this jail")
			return m, nil
		}
		m.mode = screenZFSPanel
		m.zfsPanel = newZFSPanelState(m.detail.ZFS.Name)
		return m, listZFSSnapshotsCmd(m.zfsPanel.dataset)
	}
	m.boundDetailScroll()
	return m, nil
}

func (m model) updateWizardKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.wizard.templateMode == wizardTemplateModeSave {
		switch msg.String() {
		case "esc", "left":
			m.wizard.endTemplateMode()
			m.wizard.message = "Template save canceled."
			return m, nil
		case "enter":
			name := strings.TrimSpace(m.wizard.templateInput)
			if name == "" {
				m.wizard.message = "Template name is required."
				return m, nil
			}
			if err := saveWizardTemplate(name, m.wizard.values); err != nil {
				m.wizard.message = "Template save failed: " + err.Error()
				return m, nil
			}
			m.wizard.endTemplateMode()
			m.wizard.message = fmt.Sprintf("Template %q saved.", name)
			return m, nil
		case "backspace", "delete":
			m.wizard.backspaceTemplateInput()
			return m, nil
		}
		if msg.Type == tea.KeyRunes {
			m.wizard.appendTemplateInput(string(msg.Runes))
		}
		return m, nil
	}

	if m.wizard.templateMode == wizardTemplateModeLoad {
		switch msg.String() {
		case "esc", "left":
			m.wizard.endTemplateMode()
			m.wizard.message = "Template load canceled."
			return m, nil
		case "j", "down", "tab":
			m.wizard.templateCursor++
		case "k", "up", "shift+tab":
			m.wizard.templateCursor--
		case "g", "home":
			m.wizard.templateCursor = 0
		case "G", "end":
			m.wizard.templateCursor = len(m.wizard.templates) - 1
		case "r", "R":
			if err := m.wizard.beginTemplateLoad(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			return m, nil
		case "enter":
			template, ok := m.wizard.selectedTemplate()
			if !ok {
				m.wizard.message = "No template selected."
				return m, nil
			}
			m.wizard.values = template.Values
			if strings.TrimSpace(m.wizard.values.Interface) == "" {
				m.wizard.values.Interface = "vnet0"
			}
			m.wizard.endTemplateMode()
			m.wizard.message = fmt.Sprintf("Template %q loaded.", template.Name)
			return m, nil
		}
		m.wizard.boundTemplateCursor()
		return m, nil
	}

	if !m.wizardApplying && !m.wizard.isConfirmationStep() && msg.Type == tea.KeyRunes {
		m.wizard.appendToActive(string(msg.Runes))
		return m, nil
	}

	switch msg.String() {
	case "esc":
		if m.wizardApplying {
			return m, nil
		}
		m.mode = screenDashboard
		m.notice = "Jail creation canceled."
		return m, nil
	case "left":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.prevStep()
		return m, nil
	case "right":
		if m.wizardApplying {
			return m, nil
		}
		if err := m.wizard.nextStep(); err != nil {
			return m, nil
		}
		return m, nil
	case "tab", "down":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.nextField()
		return m, nil
	case "shift+tab", "up":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.prevField()
		return m, nil
	case "s", "S", "ctrl+s":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.beginTemplateSave()
		return m, nil
	case "l", "L", "ctrl+l":
		if m.wizardApplying {
			return m, nil
		}
		if err := m.wizard.beginTemplateLoad(); err != nil {
			m.wizard.message = err.Error()
			return m, nil
		}
		return m, nil
	case "enter":
		if m.wizard.isConfirmationStep() {
			if m.wizardApplying {
				return m, nil
			}
			if err := m.wizard.validateAll(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			m.wizard.clearExecutionResult()
			m.wizard.message = "Applying creation plan..."
			m.wizardApplying = true
			return m, createJailCmd(m.wizard.values)
		}
		if m.wizardApplying {
			return m, nil
		}
		_ = m.wizard.nextStep()
		return m, nil
	case "backspace", "delete":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.backspaceActive()
		return m, nil
	}

	return m, nil
}

func (m model) updateHelpKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "left", "backspace", "enter", "h", "?":
		m.mode = m.helpReturnMode
		return m, nil
	case "j", "down":
		m.helpScroll++
	case "k", "up":
		m.helpScroll--
	case "g", "home":
		m.helpScroll = 0
	case "G", "end":
		m.helpScroll = 1 << 30
	case "pgdown":
		m.helpScroll += m.helpBodyHeight()
	case "pgup":
		m.helpScroll -= m.helpBodyHeight()
	}
	m.boundHelpScroll()
	return m, nil
}

func (m model) View() string {
	if m.width == 0 || m.height == 0 {
		return "Loading dashboard..."
	}
	if m.mode == screenCreateWizard {
		return m.renderWizardView()
	}
	if m.mode == screenZFSPanel {
		return m.renderZFSPanelView()
	}
	if m.mode == screenHelp {
		return m.renderHelpView()
	}
	if m.mode == screenJailDetail {
		return m.renderJailDetailView()
	}
	return m.renderDashboard()
}

func (m model) renderHelpView() string {
	title := titleStyle.Render("Help / Shortcuts")
	meta := summaryStyle.Render("Press esc to return")
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := m.helpBodyHeight()
	lines := m.helpLines(max(12, m.width-2))
	maxOffset := max(0, len(lines)-bodyHeight)
	offset := m.helpScroll
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

	footer := footerStyle.Width(m.width).Render("j/k or pgup/pgdown scroll | esc/enter: close help | q: quit")
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) helpLines(width int) []string {
	lines := []string{
		sectionStyle.Render("Global"),
		truncate("?: open help page (h works outside text input)", width),
		truncate("q: quit the application", width),
		"",
		sectionStyle.Render("Dashboard"),
		truncate("j/k, arrows, pgup/pgdown, g/G: navigate jail list", width),
		truncate("enter or d: open jail detail view", width),
		truncate("c: open jail creation wizard", width),
		truncate("r: refresh dashboard data", width),
		"",
		sectionStyle.Render("Jail Detail"),
		truncate("j/k, pgup/pgdown, g/G: scroll detail", width),
		truncate("r: refresh selected jail details", width),
		truncate("z: open ZFS integration panel", width),
		truncate("esc: return to dashboard", width),
		"",
		sectionStyle.Render("ZFS Panel"),
		truncate("j/k: select snapshot", width),
		truncate("n: new snapshot", width),
		truncate("r: rollback selected snapshot (confirmation required)", width),
		truncate("R: refresh snapshot list", width),
		truncate("esc: cancel prompt or return to detail", width),
		"",
		sectionStyle.Render("Creation Wizard"),
		truncate("tab/shift+tab: move field", width),
		truncate("enter/right: next step", width),
		truncate("left: previous step", width),
		truncate("s/l on step 6: save/load templates", width),
		truncate("?: open help page", width),
		truncate("step 6 enter: execute create actions", width),
	}
	return lines
}

func (m model) helpBodyHeight() int {
	return max(5, m.height-3)
}

func (m *model) boundHelpScroll() {
	if m.mode != screenHelp {
		return
	}
	lines := m.helpLines(max(12, m.width-2))
	maxOffset := max(0, len(lines)-m.helpBodyHeight())
	if m.helpScroll < 0 {
		m.helpScroll = 0
	}
	if m.helpScroll > maxOffset {
		m.helpScroll = maxOffset
	}
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

	hint := "j/k or up/down: scroll | pgup/pgdown | g/G | r: refresh detail | z: ZFS panel | h: help | esc: back | q: quit"
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

	hint := "type to edit | tab/shift+tab: fields | enter/right: next | left: back | ?: help | esc: cancel | q: quit"
	if m.wizard.isConfirmationStep() {
		hint = "enter: create jail now | left: back | s: save tmpl | l: load tmpl | ?: help | esc: cancel | q: quit"
	}
	if m.wizard.templateMode == wizardTemplateModeSave {
		hint = "Template save: type name | enter: save | backspace: edit | esc: cancel"
	}
	if m.wizard.templateMode == wizardTemplateModeLoad {
		hint = "Template load: j/k select | enter: load | r: refresh list | esc: cancel"
	}
	if m.wizardApplying {
		hint = "Applying changes... please wait | q: quit"
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

	if m.wizard.templateMode == wizardTemplateModeSave {
		lines = append(lines, sectionStyle.Render("Save Template"))
		lines = append(lines, truncate("Template name: "+m.wizard.templateInput, width))
		lines = append(lines, "Press enter to save current wizard values as a template.")
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Current Values Preview"))
		for _, line := range m.wizard.summaryLines() {
			lines = append(lines, truncate(line, width))
		}
		return lines
	}

	if m.wizard.templateMode == wizardTemplateModeLoad {
		lines = append(lines, sectionStyle.Render("Load Template"))
		if len(m.wizard.templates) == 0 {
			lines = append(lines, "No templates available.")
			return lines
		}
		for idx, template := range m.wizard.templates {
			row := "  " + template.Name
			if idx == m.wizard.templateCursor {
				row = selectedRowStyle.Width(max(1, width)).Render("> " + template.Name)
			}
			lines = append(lines, truncate(row, width))
		}
		if template, ok := m.wizard.selectedTemplate(); ok {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Selected Template Preview"))
			lines = append(lines, truncate("Name: "+template.Name, width))
			lines = append(lines, truncate("Dataset: "+template.Values.Dataset, width))
			lines = append(lines, truncate("Template/Release: "+template.Values.TemplateRelease, width))
			lines = append(lines, truncate("IPv4: "+template.Values.IP4, width))
		}
		return lines
	}

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
		if len(m.wizard.executionLogs) > 0 {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Execution output"))
			for _, line := range m.wizard.executionLogs {
				lines = append(lines, truncate(line, width))
			}
		}
		if m.wizard.executionError != "" {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Execution error"))
			lines = append(lines, truncate(m.wizard.executionError, width))
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
	hint := "j/k or up/down: scroll | g/G: top/bottom | enter/d: details | c: create wizard | h: help | r: refresh | q: quit"
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
		return "No jails discovered yet. Create one manually in jail.conf/jail.conf.d or press c to open the jail creation wizard."
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
			"Inside detail view, press z for ZFS panel.",
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
