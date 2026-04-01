package main

import (
	"fmt"
	"path/filepath"
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
	wizardActionStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("230")).
				Background(lipgloss.Color("31")).
				Padding(0, 1)
	wizardErrorStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("196"))
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

type destroyApplyMsg struct {
	result JailDestroyResult
}

type jailServiceApplyMsg struct {
	result jailServiceResult
}

type linuxBootstrapApplyMsg struct {
	result linuxBootstrapResult
}

type templateDatasetApplyMsg struct {
	result TemplateDatasetResult
}

type templateParentApplyMsg struct {
	result TemplateParentDatasetResult
}

type zfsOpenMsg struct {
	result zfsOpenResult
}

type tickMsg time.Time

type screenMode int

const (
	screenInitialCheck screenMode = iota
	screenDashboard
	screenJailDetail
	screenCreateWizard
	screenTemplateDatasetCreate
	screenZFSPanel
	screenDestroyConfirm
	screenHelp
)

type destroyState struct {
	returnMode screenMode
	target     Jail
	applying   bool
	logs       []string
	err        error
	message    string
}

type templateDatasetCreateState struct {
	returnMode       screenMode
	sourceInput      string
	preview          TemplateDatasetPreview
	applying         bool
	parentApplying   bool
	logs             []string
	message          string
	parentDataset    string
	parentMountpoint string
	parentEdit       bool
	parentField      int
	parentCustom     bool
}

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
	detailNotice   string
	zfsPanel       zfsPanelState
	wizard         jailCreationWizard
	wizardApplying bool
	templateCreate templateDatasetCreateState
	destroy        destroyState
	initCheck      initialCheckState
	helpReturnMode screenMode
	helpScroll     int
	notice         string
}

func newModel() model {
	mode := screenInitialCheck
	if completed, err := initialCheckCompleted(); err == nil && completed {
		mode = screenDashboard
	}
	m := model{
		mode:      mode,
		initCheck: newInitialCheckState(),
	}
	m.wizard = newJailCreationWizard(initialWizardDestination(m.initCheck.status))
	m.templateCreate = newTemplateDatasetCreateState("", m.initCheck.status, screenDashboard)
	return m
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

func destroyJailCmd(target Jail) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteJailDestroy(target)
		return destroyApplyMsg{result: result}
	}
}

func jailServiceCmd(target Jail, action string) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteJailServiceAction(target, action)
		return jailServiceApplyMsg{result: result}
	}
}

func linuxBootstrapCmd(detail JailDetail) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteLinuxBootstrapAction(detail)
		return linuxBootstrapApplyMsg{result: result}
	}
}

func templateDatasetCreateCmd(sourceInput string, parentOverride *templateDatasetParent) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteTemplateDatasetCreateWithParent(sourceInput, parentOverride)
		return templateDatasetApplyMsg{result: result}
	}
}

func templateParentCreateCmd(dataset, mountpoint string) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteTemplateParentDatasetCreate(dataset, mountpoint)
		return templateParentApplyMsg{result: result}
	}
}

func openZFSPanelCmd(target Jail) tea.Cmd {
	return func() tea.Msg {
		result := resolveZFSPanelTarget(target)
		return zfsOpenMsg{result: result}
	}
}

func tickerCmd() tea.Cmd {
	return tea.Tick(pollInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) Init() tea.Cmd {
	if m.mode == screenInitialCheck {
		return collectInitialConfigCmd()
	}
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
		if m.mode == screenInitialCheck {
			return m, nil
		}
		m.snapshot = msg.snapshot
		m.err = msg.err
		m.boundCursor()
		m.ensureCursorVisible(m.listHeight())
		m.boundDetailScroll()
		return m, tickerCmd()
	case initialConfigMsg:
		m.initCheck.loading = false
		m.initCheck.status = msg.status
		m.initCheck.err = msg.err
		m.initCheck.setPhaseFromStatus()
		return m, nil
	case initialActionMsg:
		if m.mode != screenInitialCheck {
			return m, nil
		}
		m.initCheck.applying = false
		m.initCheck.logs = msg.logs
		m.initCheck.err = msg.err
		if msg.err != nil {
			if msg.message != "" {
				m.initCheck.message = msg.message
			} else {
				m.initCheck.message = "Action failed."
			}
			return m, nil
		}
		if msg.message != "" {
			m.initCheck.message = msg.message
		}
		if msg.refresh {
			m.initCheck.loading = true
			return m, collectInitialConfigCmd()
		}
		m.initCheck.setPhaseFromStatus()
		return m, nil
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
			if len(msg.result.Warnings) > 0 {
				m.notice = fmt.Sprintf("Jail %s created and started with warnings: %s", msg.result.Name, msg.result.Warnings[0])
			} else {
				m.notice = fmt.Sprintf("Jail %s created and started.", msg.result.Name)
			}
			m.wizard = newJailCreationWizard(initialWizardDestination(m.initCheck.status))
			return m, pollCmd()
		}
		return m, nil
	case destroyApplyMsg:
		m.destroy.applying = false
		m.destroy.logs = append([]string(nil), msg.result.Logs...)
		m.destroy.err = msg.result.Err
		m.destroy.message = destroyResultMessage(msg.result)
		if msg.result.Err == nil {
			m.mode = screenDashboard
			m.destroy = destroyState{}
			m.notice = timestampedDestroyNotice(msg.result.Name)
			return m, pollCmd()
		}
		return m, nil
	case jailServiceApplyMsg:
		if msg.result.Err != nil {
			m.err = msg.result.Err
			m.notice = ""
			return m, pollCmd()
		}
		m.err = nil
		actionWord := "started"
		if msg.result.Action == "stop" {
			actionWord = "stopped"
		}
		m.notice = fmt.Sprintf("Jail %s %s.", msg.result.Name, actionWord)
		return m, pollCmd()
	case linuxBootstrapApplyMsg:
		if msg.result.Err != nil {
			m.detailErr = msg.result.Err
			m.detailNotice = ""
			return m, nil
		}
		m.detailErr = nil
		if len(msg.result.Warnings) > 0 {
			m.detailNotice = "Linux bootstrap warning: " + msg.result.Warnings[0]
		} else {
			m.detailNotice = "Linux bootstrap completed."
		}
		jail, ok := m.detailJail()
		if !ok {
			return m, nil
		}
		m.detailLoading = true
		return m, detailCmd(jail)
	case templateDatasetApplyMsg:
		if m.mode == screenTemplateDatasetCreate {
			m.templateCreate.applying = false
			m.templateCreate.logs = append([]string(nil), msg.result.Logs...)
			if msg.result.Err != nil {
				m.templateCreate.message = msg.result.Err.Error()
				return m, nil
			}
			if m.templateCreate.returnMode == screenCreateWizard {
				m.wizard.datasetCreateRunning = false
				m.wizard.values.TemplateRelease = msg.result.Mountpoint
				m.wizard.endThinDatasetSelect()
				m.wizard.executionLogs = append([]string(nil), msg.result.Logs...)
				m.wizard.message = fmt.Sprintf("Template dataset created: %s", msg.result.Dataset)
				m.mode = screenCreateWizard
				return m, nil
			}
			m.templateCreate.message = fmt.Sprintf("Template dataset created: %s", msg.result.Dataset)
			return m, nil
		}
		m.wizard.datasetCreateRunning = false
		m.wizard.executionLogs = append([]string(nil), msg.result.Logs...)
		if msg.result.Err != nil {
			m.wizard.message = msg.result.Err.Error()
			return m, nil
		}
		m.wizard.values.TemplateRelease = msg.result.Mountpoint
		m.wizard.endThinDatasetSelect()
		m.wizard.message = fmt.Sprintf("Template dataset created: %s", msg.result.Dataset)
		return m, nil
	case templateParentApplyMsg:
		if m.mode != screenTemplateDatasetCreate {
			return m, nil
		}
		m.templateCreate.parentApplying = false
		m.templateCreate.logs = append([]string(nil), msg.result.Logs...)
		if msg.result.Err != nil {
			m.templateCreate.message = msg.result.Err.Error()
			return m, nil
		}
		m.templateCreate.parentDataset = msg.result.Dataset
		m.templateCreate.parentMountpoint = msg.result.Mountpoint
		m.templateCreate.parentCustom = true
		m.templateCreate.parentEdit = false
		m.templateCreate.refreshPreview()
		m.templateCreate.message = "Template parent dataset created. Press enter to create the template dataset."
		return m, nil
	case zfsOpenMsg:
		if msg.result.Detail.ZFS == nil || strings.TrimSpace(msg.result.Detail.ZFS.Name) == "" {
			if msg.result.Err != nil {
				m.err = msg.result.Err
				m.notice = ""
			} else {
				m.err = nil
				m.notice = "No ZFS dataset detected for selected jail."
			}
			return m, nil
		}
		m.detail = msg.result.Detail
		m.detailErr = msg.result.Err
		m.detailLoading = false
		m.mode = screenZFSPanel
		m.zfsPanel = newZFSPanelState(m.detail.ZFS.Name, screenDashboard)
		return m, listZFSSnapshotsCmd(m.zfsPanel.dataset)
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
		if m.mode == screenDestroyConfirm {
			return m.updateDestroyKeys(msg)
		}
		if m.mode == screenInitialCheck {
			return m.updateInitialCheckKeys(msg)
		}
		if m.mode == screenZFSPanel {
			return m.updateZFSPanelKeys(msg)
		}
		if m.mode == screenCreateWizard {
			return m.updateWizardKeys(msg)
		}
		if m.mode == screenTemplateDatasetCreate {
			return m.updateTemplateDatasetKeys(msg)
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
	case screenInitialCheck:
		return m.initCheck.phase == initialPhaseDirsCustomInput || m.initCheck.phase == initialPhaseDatasetsCustomInput
	case screenCreateWizard:
		if m.wizardApplying {
			return false
		}
		if m.wizard.userlandMode {
			return false
		}
		if m.wizard.thinDatasetMode {
			return false
		}
		if m.wizard.templateMode == wizardTemplateModeSave {
			return true
		}
		if m.wizard.templateMode == wizardTemplateModeLoad {
			return false
		}
		return !m.wizard.isConfirmationStep()
	case screenTemplateDatasetCreate:
		return !m.templateCreate.applying && !m.templateCreate.parentApplying
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
		m.wizard = newJailCreationWizard(initialWizardDestination(m.initCheck.status))
		m.notice = ""
		return m, nil
	case "t", "T":
		m.mode = screenTemplateDatasetCreate
		m.templateCreate = newTemplateDatasetCreateState("", m.initCheck.status, screenDashboard)
		m.notice = ""
		m.err = nil
		return m, nil
	case "s", "S":
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		action := "start"
		if jail.Running {
			action = "stop"
		}
		return m, jailServiceCmd(jail, action)
	case "z":
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		m.notice = ""
		m.err = nil
		return m, openZFSPanelCmd(jail)
	case "x", "X":
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		m.destroy = destroyState{
			returnMode: screenDashboard,
			target:     buildDestroyTarget(jail),
			message:    "Press enter to destroy this jail, or esc to cancel.",
		}
		m.mode = screenDestroyConfirm
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
		m.detailNotice = ""
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
		m.detailNotice = ""
		return m, detailCmd(jail)
	case "b", "B":
		if !detailLooksLikeLinuxJail(m.detail) {
			m.detailErr = fmt.Errorf("linux bootstrap retry is only available for linux jails")
			return m, nil
		}
		jail, ok := m.detailJail()
		if !ok || jail.JID <= 0 {
			m.detailErr = fmt.Errorf("linux bootstrap retry requires the jail to be running")
			return m, nil
		}
		m.detailErr = nil
		m.detailNotice = "Retrying Linux bootstrap..."
		return m, linuxBootstrapCmd(m.detail)
	case "z":
		if m.detail.ZFS == nil || strings.TrimSpace(m.detail.ZFS.Name) == "" {
			m.detailErr = fmt.Errorf("no ZFS dataset detected for this jail")
			return m, nil
		}
		m.mode = screenZFSPanel
		m.zfsPanel = newZFSPanelState(m.detail.ZFS.Name, screenJailDetail)
		return m, listZFSSnapshotsCmd(m.zfsPanel.dataset)
	case "x", "X":
		jail, ok := m.detailJail()
		if !ok {
			return m, nil
		}
		m.destroy = destroyState{
			returnMode: screenJailDetail,
			target:     buildDestroyTarget(jail),
			message:    "Press enter to destroy this jail, or esc to cancel.",
		}
		m.mode = screenDestroyConfirm
		return m, nil
	}
	m.boundDetailScroll()
	return m, nil
}

func (m model) updateDestroyKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "backspace", "left", "n", "N":
		if m.destroy.applying {
			return m, nil
		}
		m.mode = m.destroy.returnMode
		m.notice = "Destroy canceled."
		m.destroy = destroyState{}
		return m, nil
	case "enter", "y", "Y":
		if m.destroy.applying {
			return m, nil
		}
		m.destroy.logs = nil
		m.destroy.err = nil
		m.destroy.message = "Destroying jail..."
		m.destroy.applying = true
		return m, destroyJailCmd(m.destroy.target)
	}
	return m, nil
}

func (m model) updateWizardKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "?" {
		m.helpReturnMode = m.mode
		m.helpScroll = 0
		m.mode = screenHelp
		return m, nil
	}
	if m.wizard.thinDatasetMode {
		switch msg.String() {
		case "esc", "left":
			if m.wizard.datasetCreateRunning {
				return m, nil
			}
			m.wizard.endThinDatasetSelect()
			m.wizard.message = "Thin template dataset selection canceled."
			return m, nil
		case "j", "down", "tab":
			m.wizard.thinDatasetCursor++
		case "k", "up", "shift+tab":
			m.wizard.thinDatasetCursor--
		case "g", "home":
			m.wizard.thinDatasetCursor = 0
		case "G", "end":
			m.wizard.thinDatasetCursor = len(m.wizard.thinDatasetOpts) - 1
		case "r", "R":
			if m.wizard.datasetCreateRunning {
				return m, nil
			}
			if err := m.wizard.beginThinDatasetSelect(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			return m, nil
		case "c", "C":
			if m.wizard.datasetCreateRunning {
				return m, nil
			}
			sourceInput := strings.TrimSpace(m.wizard.values.TemplateRelease)
			if sourceInput == "" {
				m.wizard.message = "Enter Template/Release first, then press c to create a template dataset."
				return m, nil
			}
			m.templateCreate = newTemplateDatasetCreateState(sourceInput, m.initCheck.status, screenCreateWizard)
			m.templateCreate.message = "Review the template dataset preview, then press enter to create it."
			m.mode = screenTemplateDatasetCreate
			return m, nil
		case "enter":
			if m.wizard.datasetCreateRunning {
				return m, nil
			}
			option, ok := m.wizard.selectedThinDatasetOption()
			if !ok {
				m.wizard.message = "No thin template dataset selected."
				return m, nil
			}
			m.wizard.values.TemplateRelease = option.Value
			m.wizard.endThinDatasetSelect()
			m.wizard.message = fmt.Sprintf("Selected thin template dataset: %s", option.Label)
			return m, nil
		}
		m.wizard.boundThinDatasetCursor()
		return m, nil
	}
	if m.wizard.userlandMode {
		switch msg.String() {
		case "esc", "left":
			m.wizard.endUserlandSelect()
			m.wizard.message = "Userland selection canceled."
			return m, nil
		case "j", "down", "tab":
			m.wizard.userlandCursor++
		case "k", "up", "shift+tab":
			m.wizard.userlandCursor--
		case "g", "home":
			m.wizard.userlandCursor = 0
		case "G", "end":
			m.wizard.userlandCursor = len(m.wizard.userlandOpts) - 1
		case "r", "R":
			if err := m.wizard.beginUserlandSelect(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			return m, nil
		case "enter":
			option, ok := m.wizard.selectedUserlandOption()
			if !ok {
				m.wizard.message = "No userland option selected."
				return m, nil
			}
			m.wizard.values.TemplateRelease = option.Value
			m.wizard.endUserlandSelect()
			m.wizard.message = fmt.Sprintf("Selected userland: %s", option.Label)
			return m, nil
		}
		m.wizard.boundUserlandCursor()
		return m, nil
	}

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
			if strings.TrimSpace(m.wizard.values.JailType) == "" {
				m.wizard.values.JailType = "thick"
			}
			if strings.TrimSpace(m.wizard.values.Interface) == "" {
				m.wizard.values.Interface = "em0"
			}
			m.wizard.normalizeStep()
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
	case "ctrl+u":
		if m.wizardApplying {
			return m, nil
		}
		if err := m.wizard.beginUserlandSelect(); err != nil {
			m.wizard.message = err.Error()
			return m, nil
		}
		return m, nil
	case "ctrl+t":
		if m.wizardApplying {
			return m, nil
		}
		if normalizedJailType(m.wizard.values.JailType) != "thin" {
			m.wizard.message = "Thin template dataset selector is only used for thin jails."
			return m, nil
		}
		if err := m.wizard.beginThinDatasetSelect(); err != nil {
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

func (m model) updateTemplateDatasetKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "?" {
		m.helpReturnMode = m.mode
		m.helpScroll = 0
		m.mode = screenHelp
		return m, nil
	}

	switch msg.String() {
	case "esc", "left":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		if m.templateCreate.parentEdit {
			m.templateCreate.parentEdit = false
			m.templateCreate.message = "Template parent edit canceled."
			return m, nil
		}
		m.mode = m.templateCreate.returnMode
		if m.templateCreate.returnMode == screenDashboard {
			m.notice = "Template dataset creation canceled."
		} else {
			m.wizard.datasetCreateRunning = false
			m.wizard.message = "Template dataset creation canceled."
		}
		return m, nil
	case "tab", "down":
		if m.templateCreate.parentEdit {
			m.templateCreate.parentField++
			if m.templateCreate.parentField > 1 {
				m.templateCreate.parentField = 0
			}
		}
		return m, nil
	case "shift+tab", "up":
		if m.templateCreate.parentEdit {
			m.templateCreate.parentField--
			if m.templateCreate.parentField < 0 {
				m.templateCreate.parentField = 1
			}
		}
		return m, nil
	case "e", "E":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		m.templateCreate.parentEdit = true
		if strings.TrimSpace(m.templateCreate.parentDataset) == "" {
			if dataset, mountpoint, ok := suggestTemplateParentDataset(m.initCheck.status); ok {
				m.templateCreate.parentDataset = dataset
				m.templateCreate.parentMountpoint = mountpoint
			}
		}
		if strings.TrimSpace(m.templateCreate.parentDataset) == "" {
			m.templateCreate.parentDataset = docDatasetTemplate
		}
		if strings.TrimSpace(m.templateCreate.parentMountpoint) == "" {
			m.templateCreate.parentMountpoint = filepath.Join(docJailsPath, "templates")
		}
		m.templateCreate.message = "Edit parent dataset and mountpoint, then press enter to create the parent."
		return m, nil
	case "enter":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		if m.templateCreate.parentEdit || m.templateCreate.preview.NeedsParentCreate {
			dataset := strings.TrimSpace(m.templateCreate.parentDataset)
			mountpoint := strings.TrimSpace(m.templateCreate.parentMountpoint)
			if dataset == "" {
				m.templateCreate.message = "parent dataset is required"
				return m, nil
			}
			if mountpoint == "" {
				m.templateCreate.message = "parent mountpoint is required"
				return m, nil
			}
			m.templateCreate.parentApplying = true
			m.templateCreate.message = "Creating template parent dataset..."
			m.templateCreate.logs = nil
			return m, templateParentCreateCmd(dataset, mountpoint)
		}
		if strings.TrimSpace(m.templateCreate.sourceInput) == "" {
			m.templateCreate.message = "template/release is required"
			return m, nil
		}
		if m.templateCreate.preview.Err != nil {
			m.templateCreate.message = m.templateCreate.preview.Err.Error()
			return m, nil
		}
		m.templateCreate.message = "Creating template dataset..."
		m.templateCreate.logs = nil
		m.templateCreate.applying = true
		return m, templateDatasetCreateCmd(m.templateCreate.sourceInput, m.templateCreate.parentOverride())
	case "r", "R":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		m.templateCreate.refreshPreview()
		m.templateCreate.message = "Template dataset preview refreshed."
		return m, nil
	case "backspace", "delete":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		if m.templateCreate.parentEdit {
			m.templateCreate.backspaceParentField()
			return m, nil
		}
		m.templateCreate.backspaceSource()
		return m, nil
	}

	if !m.templateCreate.applying && !m.templateCreate.parentApplying && msg.Type == tea.KeyRunes {
		if m.templateCreate.parentEdit {
			m.templateCreate.appendParentField(string(msg.Runes))
			return m, nil
		}
		m.templateCreate.appendSource(string(msg.Runes))
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
	if m.mode == screenInitialCheck {
		return m.renderInitialCheckView()
	}
	if m.mode == screenCreateWizard {
		return m.renderWizardView()
	}
	if m.mode == screenTemplateDatasetCreate {
		return m.renderTemplateDatasetCreateView()
	}
	if m.mode == screenZFSPanel {
		return m.renderZFSPanelView()
	}
	if m.mode == screenDestroyConfirm {
		return m.renderDestroyView()
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
		sectionStyle.Render("Initial Config Check"),
		truncate("runs at startup before dashboard", width),
		truncate("y/n or d/c/n prompts apply or skip setup actions", width),
		truncate("enter continues to dashboard when complete", width),
		"",
		sectionStyle.Render("Dashboard"),
		truncate("j/k, arrows, pgup/pgdown, g/G: navigate jail list", width),
		truncate("enter or d: open jail detail view", width),
		truncate("c: open jail creation wizard", width),
		truncate("t: open template dataset creation", width),
		truncate("s: start or stop selected jail", width),
		truncate("z: open ZFS panel for selected jail", width),
		truncate("x: destroy selected jail (confirmation required)", width),
		truncate("r: refresh dashboard data", width),
		"",
		sectionStyle.Render("Jail Detail"),
		truncate("j/k, pgup/pgdown, g/G: scroll detail", width),
		truncate("r: refresh selected jail details", width),
		truncate("b: retry linux bootstrap for a running linux jail", width),
		truncate("z: open ZFS integration panel", width),
		truncate("x: destroy this jail (confirmation required)", width),
		truncate("esc: return to dashboard", width),
		"",
		sectionStyle.Render("Destroy Confirm"),
		truncate("enter/y: stop and destroy selected jail", width),
		truncate("esc/n: cancel and return", width),
		"",
		sectionStyle.Render("ZFS Panel"),
		truncate("j/k: select snapshot", width),
		truncate("c: create snapshot", width),
		truncate("r: rollback selected snapshot (confirmation required)", width),
		truncate("x: refresh snapshot list", width),
		truncate("esc: cancel prompt or return to detail", width),
		"",
		sectionStyle.Render("Creation Wizard"),
		truncate("steps 1-5 are shown together on one page", width),
		truncate("tab/shift+tab/up/down: move field", width),
		truncate("enter/right: next step", width),
		truncate("left: previous step", width),
		truncate("s/l on the confirmation step: save/load templates", width),
		truncate("ctrl+u: open userland selector", width),
		truncate("ctrl+t: open thin template dataset selector", width),
		truncate("thin selector c: create a template dataset from current Template/Release", width),
		truncate("?: open help page", width),
		truncate("confirmation enter: execute create actions", width),
		"",
		sectionStyle.Render("Template Dataset Create"),
		truncate("type a source path, release tag, userland entry, or custom URL", width),
		truncate("enter: create the previewed template dataset", width),
		truncate("when parent templates dataset is missing: enter creates proposed parent, e edits parent values", width),
		truncate("r: refresh preview", width),
		truncate("esc: return to dashboard", width),
	}
	return lines
}

func (m model) renderDestroyView() string {
	title := titleStyle.Render("Destroy Jail")
	meta := detailKeyStyle.Render("Selected:") + " " + selectedRowStyle.Render(valueOrDash(m.destroy.target.Name))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyWidth := max(12, m.width-2)
	lines := []string{"", sectionStyle.Render("Confirmation")}
	for _, line := range buildDestroyPreview(m.destroy.target) {
		lines = append(lines, truncate(line, bodyWidth))
	}
	if m.destroy.message != "" {
		lines = append(lines, "")
		noticeText := truncate(m.destroy.message, max(1, bodyWidth-8))
		lines = append(lines, detailKeyStyle.Render("Notice:")+" "+styleWizardMessage(noticeText))
	}
	if m.destroy.err != nil {
		lines = append(lines, wizardErrorStyle.Render(truncate("Error: "+m.destroy.err.Error(), bodyWidth)))
	}
	if len(m.destroy.logs) > 0 {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Execution output"))
		maxLogs := min(12, len(m.destroy.logs))
		for _, line := range m.destroy.logs[len(m.destroy.logs)-maxLogs:] {
			lines = append(lines, truncate(line, bodyWidth))
		}
	}

	bodyHeight := max(5, m.height-3)
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))

	hint := "enter/y: destroy jail | esc/n: cancel | q: quit"
	if m.destroy.applying {
		hint = "Destroying jail... please wait | q: quit"
	}
	footer := footerStyle.Width(m.width).Render(hint)
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
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

	hint := "j/k or up/down: scroll | pgup/pgdown | g/G | r: refresh detail"
	if detailLooksLikeLinuxJail(m.detail) {
		hint += " | b: retry linux bootstrap"
	}
	hint += " | z: ZFS panel | x: destroy | h: help | esc: back | q: quit"
	if m.detailLoading {
		hint += " | loading detail..."
	}
	footerRenderer := footerStyle
	if m.detailErr != nil {
		hint += " | warning: " + m.detailErr.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	} else if m.detailNotice != "" {
		hint += " | " + m.detailNotice
	}
	footer := footerRenderer.Width(m.width).Render(hint)

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) renderWizardView() string {
	step := m.wizard.currentStep()
	title := titleStyle.Render("Jail Creation Wizard")
	meta := summaryStyle.Render(fmt.Sprintf("Step %d/%d: %s", m.wizard.step+1, len(m.wizard.steps()), step.Title))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := max(4, m.height-3)
	lines := m.wizardLines(max(12, m.width-2))
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))

	hint := "type to edit | tab/shift+tab/up/down: fields | ctrl+u: userland select | enter/right: next | left: back | ?: help | esc: cancel | q: quit"
	if normalizedJailType(m.wizard.values.JailType) == "thin" {
		hint = "type to edit | tab/shift+tab/up/down: fields | ctrl+u: userland select | ctrl+t: thin template selector | enter/right: next | left: back | ?: help | esc: cancel | q: quit"
	}
	if m.wizard.isConfirmationStep() {
		hint = "enter: create jail now | left: back | s: save tmpl | l: load tmpl | ?: help | esc: cancel | q: quit"
	}
	if m.wizard.templateMode == wizardTemplateModeSave {
		hint = "Template save: type name | enter: save | backspace: edit | esc: cancel"
	}
	if m.wizard.templateMode == wizardTemplateModeLoad {
		hint = "Template load: j/k select | enter: load | r: refresh list | esc: cancel"
	}
	if m.wizard.userlandMode {
		hint = "Userland select: j/k choose | enter: apply | r: refresh options | esc: cancel"
	}
	if m.wizard.thinDatasetMode {
		hint = "Thin template select: j/k choose | enter: apply | c: create from Template/Release | r: refresh options | esc: cancel"
	}
	if m.wizard.datasetCreateRunning {
		hint = "Creating template dataset... please wait | q: quit"
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

func (m model) renderTemplateDatasetCreateView() string {
	title := titleStyle.Render("Template Dataset Create")
	meta := summaryStyle.Render("Reusable ZFS templates for thin jails")
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := max(4, m.height-3)
	lines := m.templateDatasetCreateLines(max(12, m.width-2))
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))

	hint := "type source | enter: create | backspace: edit | r: refresh preview | ?: help | esc: back | q: quit"
	if m.templateCreate.parentEdit {
		hint = "type parent values | tab/shift+tab: switch field | enter: create parent | esc: stop editing | q: quit"
	}
	if m.templateCreate.preview.NeedsParentCreate && !m.templateCreate.parentEdit {
		hint = "enter: create proposed parent | e: edit parent values | r: refresh preview | ?: help | esc: back | q: quit"
	}
	if m.templateCreate.parentApplying {
		hint = "Creating template parent dataset... please wait | q: quit"
	}
	if m.templateCreate.applying {
		hint = "Creating template dataset... please wait | q: quit"
	}
	if m.templateCreate.message != "" {
		hint += " | " + m.templateCreate.message
	}
	footer := footerStyle.Width(m.width).Render(hint)

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) templateDatasetCreateLines(width int) []string {
	lines := []string{
		sectionStyle.Render("Source"),
		truncate("Supported sources: local directory, local archive, release tag, custom https URL, or a named userland entry.", width),
	}

	sourceDisplay := m.templateCreate.sourceInput
	if strings.TrimSpace(sourceDisplay) == "" {
		sourceDisplay = "(15.0-RELEASE)"
	}
	sourceLine := truncate("> Template/Release: "+sourceDisplay, width)
	lines = append(lines, selectedRowStyle.Width(max(1, width)).Render(sourceLine))

	if m.templateCreate.message != "" {
		lines = append(lines, styleWizardMessage(truncate("Notice: "+m.templateCreate.message, width)))
	}

	lines = append(lines, "")
	lines = append(lines, sectionStyle.Render("Preview"))
	preview := m.templateCreate.preview

	parentDataset := preview.ParentDataset
	parentMountpoint := preview.ParentMountpoint
	if parentDataset == "" && strings.TrimSpace(m.templateCreate.parentDataset) != "" {
		parentDataset = m.templateCreate.parentDataset
	}
	if parentMountpoint == "" && strings.TrimSpace(m.templateCreate.parentMountpoint) != "" {
		parentMountpoint = filepath.Clean(m.templateCreate.parentMountpoint)
	}

	if parentDataset != "" {
		label := "Parent dataset"
		if preview.NeedsParentCreate {
			label = "Proposed parent dataset"
		}
		if m.templateCreate.parentEdit && m.templateCreate.parentField == 0 {
			lines = append(lines, selectedRowStyle.Width(max(1, width)).Render(truncate("> "+label+": "+parentDataset, width)))
		} else {
			lines = append(lines, truncate(label+": "+parentDataset, width))
		}
	}
	if parentMountpoint != "" {
		label := "Parent mountpoint"
		if preview.NeedsParentCreate {
			label = "Proposed parent mountpoint"
		}
		if m.templateCreate.parentEdit && m.templateCreate.parentField == 1 {
			lines = append(lines, selectedRowStyle.Width(max(1, width)).Render(truncate("> "+label+": "+parentMountpoint, width)))
		} else {
			lines = append(lines, truncate(label+": "+parentMountpoint, width))
		}
	}
	if preview.Dataset != "" {
		lines = append(lines, truncate("Derived dataset: "+preview.Dataset, width))
	}
	if preview.Mountpoint != "" {
		lines = append(lines, truncate("Target mountpoint: "+preview.Mountpoint, width))
	}
	if preview.SourceKind != "" {
		lines = append(lines, truncate("Source type: "+preview.SourceKind, width))
	}
	if preview.ResolvedSource != "" {
		lines = append(lines, truncate("Resolved source: "+preview.ResolvedSource, width))
	}
	if preview.Action != "" {
		lines = append(lines, truncate("Create action: "+preview.Action, width))
	}

	if preview.NeedsParentCreate {
		lines = append(lines, truncate("No templates parent dataset was discovered. Create the proposed parent dataset or edit the values first.", width))
	} else if preview.Err != nil {
		lines = append(lines, wizardErrorStyle.Render(truncate("Error: "+preview.Err.Error(), width)))
	} else if strings.TrimSpace(m.templateCreate.sourceInput) == "" {
		lines = append(lines, truncate("Enter a source above to preview the dataset name and mountpoint before creation.", width))
	}

	if len(m.templateCreate.logs) > 0 {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Execution output"))
		for _, line := range m.templateCreate.logs {
			lines = append(lines, truncate(line, width))
		}
	}

	return lines
}

func (m model) wizardLines(width int) []string {
	step := m.wizard.currentStep()
	lines := []string{sectionStyle.Render(step.Title)}
	if step.Description != "" {
		lines = append(lines, truncate(step.Description, width))
	}
	if m.wizard.message != "" {
		lines = append(lines, styleWizardMessage(truncate("Notice: "+m.wizard.message, width)))
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
				row = truncate("> "+template.Name, width)
				row = selectedRowStyle.Width(max(1, width)).Render(row)
				lines = append(lines, row)
				continue
			}
			lines = append(lines, truncate(row, width))
		}
		if template, ok := m.wizard.selectedTemplate(); ok {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Selected Template Preview"))
			lines = append(lines, truncate("Name: "+template.Name, width))
			lines = append(lines, truncate("Destination: "+template.Values.Dataset, width))
			lines = append(lines, truncate("Template/Release: "+template.Values.TemplateRelease, width))
			lines = append(lines, truncate("IPv4: "+template.Values.IP4, width))
		}
		return lines
	}

	if m.wizard.userlandMode {
		lines = append(lines, sectionStyle.Render("Select Userland Source"))
		if len(m.wizard.userlandOpts) == 0 {
			lines = append(lines, "No userland options found.")
			return lines
		}
		for idx, option := range m.wizard.userlandOpts {
			row := "  " + option.Label
			if idx == m.wizard.userlandCursor {
				row = truncate("> "+option.Label, width)
				row = selectedRowStyle.Width(max(1, width)).Render(row)
				lines = append(lines, row)
				continue
			}
			lines = append(lines, truncate(row, width))
		}
		if option, ok := m.wizard.selectedUserlandOption(); ok {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Selected Value"))
			lines = append(lines, truncate(option.Value, width))
			lines = append(lines, truncate("Tip: type a custom https URL directly in Template/Release for custom download.", width))
		}
		return lines
	}

	if m.wizard.thinDatasetMode {
		lines = append(lines, sectionStyle.Render("Select Thin Template Dataset"))
		if len(m.wizard.thinDatasetOpts) == 0 {
			lines = append(lines, "No thin template datasets found.")
			lines = append(lines, "")
			lines = append(lines, truncate("Press c to create a template dataset from the current Template/Release value.", width))
			return lines
		}
		for idx, option := range m.wizard.thinDatasetOpts {
			row := "  " + option.Label
			if idx == m.wizard.thinDatasetCursor {
				row = truncate("> "+option.Label, width)
				row = selectedRowStyle.Width(max(1, width)).Render(row)
				lines = append(lines, row)
				continue
			}
			lines = append(lines, truncate(row, width))
		}
		if option, ok := m.wizard.selectedThinDatasetOption(); ok {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Selected Value"))
			lines = append(lines, truncate(option.Value, width))
			lines = append(lines, truncate("Thin jails require an extracted template dataset mountpoint, not a release tag or archive.", width))
			lines = append(lines, truncate("Press c to create a new template dataset from the current Template/Release value.", width))
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

	currentSection := ""
	fields := m.wizard.visibleFields()
	m.wizard.normalizeField()
	for idx, field := range fields {
		section := wizardSectionForField(field.ID)
		if section != "" && section != currentSection {
			if len(lines) > 0 && lines[len(lines)-1] != "" {
				lines = append(lines, "")
			}
			lines = append(lines, sectionStyle.Render(section))
			currentSection = section
		}

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
		if field.ID == "template_release" {
			lines = append(lines, truncate("  ctrl+u: select from local userland media or official release downloads", width))
			if normalizedJailType(m.wizard.values.JailType) == "thin" {
				lines = append(lines, truncate("  ctrl+t: select an extracted ZFS template dataset mountpoint", width))
			}
		}
		if field.ID == "name" || field.ID == "interface" || field.ID == "uplink" || field.ID == "ip6" {
			lines = append(lines, "")
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
			lines = append(lines, wizardErrorStyle.Render(truncate(fmt.Sprintf("%s: %s", source, m.detail.SourceErrors[source]), width)))
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

func newTemplateDatasetCreateState(sourceInput string, status initialConfigStatus, returnMode screenMode) templateDatasetCreateState {
	state := templateDatasetCreateState{
		returnMode:  returnMode,
		sourceInput: strings.TrimSpace(sourceInput),
	}
	if dataset, mountpoint, ok := suggestTemplateParentDataset(status); ok {
		state.parentDataset = dataset
		state.parentMountpoint = mountpoint
	} else if liveStatus, err := collectInitialConfigStatus(time.Now()); err == nil {
		if dataset, mountpoint, ok := suggestTemplateParentDataset(liveStatus); ok {
			state.parentDataset = dataset
			state.parentMountpoint = mountpoint
		}
	}
	state.refreshPreview()
	return state
}

func (s *templateDatasetCreateState) refreshPreview() {
	s.preview = InspectTemplateDatasetCreateWithParent(s.sourceInput, s.parentOverride())
}

func (s *templateDatasetCreateState) appendSource(text string) {
	if text == "" {
		return
	}
	s.sourceInput += text
	s.logs = nil
	s.message = ""
	s.refreshPreview()
}

func (s *templateDatasetCreateState) backspaceSource() {
	if s.sourceInput == "" {
		return
	}
	s.sourceInput = s.sourceInput[:len(s.sourceInput)-1]
	s.logs = nil
	s.message = ""
	s.refreshPreview()
}

func (s *templateDatasetCreateState) parentOverride() *templateDatasetParent {
	if !s.parentCustom {
		return nil
	}
	if strings.TrimSpace(s.parentDataset) == "" || strings.TrimSpace(s.parentMountpoint) == "" {
		return nil
	}
	return &templateDatasetParent{
		Name:       strings.TrimSpace(s.parentDataset),
		Mountpoint: filepath.Clean(strings.TrimSpace(s.parentMountpoint)),
	}
}

func (s *templateDatasetCreateState) appendParentField(text string) {
	if text == "" {
		return
	}
	ref := s.parentFieldRef()
	if ref == nil {
		return
	}
	*ref += text
	s.logs = nil
	s.message = ""
}

func (s *templateDatasetCreateState) backspaceParentField() {
	ref := s.parentFieldRef()
	if ref == nil || *ref == "" {
		return
	}
	*ref = (*ref)[:len(*ref)-1]
	s.logs = nil
	s.message = ""
}

func (s *templateDatasetCreateState) parentFieldRef() *string {
	switch s.parentField {
	case 0:
		return &s.parentDataset
	case 1:
		return &s.parentMountpoint
	default:
		return nil
	}
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
	hint := "j/k or up/down: scroll | g/G: top/bottom | enter/d: details | c: create wizard | t: template dataset | s: start/stop | z: ZFS | x: destroy | h: help | r: refresh | q: quit"
	if m.notice != "" {
		hint += " | " + m.notice
	}
	footerRenderer := footerStyle
	if m.err != nil {
		hint += " | warning: " + m.err.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	}
	return footerRenderer.Width(m.width).Render(hint)
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
		return "No jails discovered yet. Create one manually in jail.conf/jail.conf.d, press c to open the jail creation wizard, or press t to create a template dataset."
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
			"t: create a reusable template dataset.",
			"s: start/stop selected jail.",
			"z: open ZFS panel for selected jail.",
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

func styleWizardMessage(message string) string {
	lower := strings.ToLower(message)
	if strings.Contains(lower, "applying creation plan") || strings.Contains(lower, "creating template dataset") || strings.Contains(lower, "creating template parent dataset") {
		return wizardActionStyle.Render(message)
	}
	if strings.Contains(lower, "failed") ||
		strings.Contains(lower, "required") ||
		strings.Contains(lower, "invalid") ||
		strings.Contains(lower, "must") ||
		strings.Contains(lower, "already exists") ||
		strings.Contains(lower, "error") {
		return wizardErrorStyle.Render(message)
	}
	return summaryStyle.Render(message)
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
