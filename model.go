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

type templateManagerRefreshMsg struct {
	items  []TemplateDatasetInfo
	parent *templateDatasetParent
	err    error
}

type templateDatasetRenameApplyMsg struct {
	result TemplateDatasetRenameResult
}

type templateDatasetDestroyApplyMsg struct {
	result TemplateDatasetDestroyResult
}

type templateSnapshotListMsg struct {
	snapshots []ZFSSnapshot
	err       error
}

type templateSnapshotCloneApplyMsg struct {
	result TemplateDatasetSnapshotCloneResult
}

type jailSnapshotCloneApplyMsg struct {
	result JailSnapshotCloneResult
}

type templateParentApplyMsg struct {
	result TemplateParentDatasetResult
}

type zfsOpenMsg struct {
	result zfsOpenResult
}

type zfsPropertyStateMsg struct {
	properties zfsEditablePropertyState
	err        error
}

type zfsPropertyApplyMsg struct {
	properties zfsEditablePropertyState
	logs       []string
	err        error
	message    string
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
	preview    []string
	applying   bool
	logs       []string
	err        error
	message    string
}

type templateManagerMode int

const (
	templateManagerModeBrowse templateManagerMode = iota
	templateManagerModeCreate
	templateManagerModeRename
	templateManagerModeDestroy
	templateManagerModeClone
)

type templateDatasetCreateState struct {
	returnMode       screenMode
	selectMode       bool
	mode             templateManagerMode
	items            []TemplateDatasetInfo
	cursor           int
	parent           *templateDatasetParent
	loading          bool
	sourceInput      string
	preview          TemplateDatasetPreview
	renameInput      string
	renamePreview    TemplateDatasetRenamePreview
	destroyPreview   TemplateDatasetDestroyPreview
	cloneSnapshots   []ZFSSnapshot
	cloneCursor      int
	cloneName        string
	clonePreview     TemplateDatasetSnapshotClonePreview
	cloneLoading     bool
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

	mode               screenMode
	detail             JailDetail
	detailErr          error
	detailLoading      bool
	detailScroll       int
	detailShowAdvanced bool
	detailNotice       string
	wizardScroll       int
	zfsPanel           zfsPanelState
	wizard             jailCreationWizard
	wizardApplying     bool
	templateCreate     templateDatasetCreateState
	destroy            destroyState
	initCheck          initialCheckState
	helpReturnMode     screenMode
	helpScroll         int
	notice             string
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
	m.templateCreate = newTemplateDatasetCreateState("", m.initCheck.status, screenDashboard, false)
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

func templateManagerRefreshCmd(parentOverride *templateDatasetParent) tea.Cmd {
	return func() tea.Msg {
		items, parent, err := ListTemplateDatasets(parentOverride)
		return templateManagerRefreshMsg{items: items, parent: parent, err: err}
	}
}

func templateDatasetRenameCmd(dataset, newName string, parentOverride *templateDatasetParent) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteTemplateDatasetRename(dataset, newName, parentOverride)
		return templateDatasetRenameApplyMsg{result: result}
	}
}

func templateDatasetDestroyCmd(dataset string, parentOverride *templateDatasetParent) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteTemplateDatasetDestroy(dataset, parentOverride)
		return templateDatasetDestroyApplyMsg{result: result}
	}
}

func templateSnapshotListCmd(dataset string) tea.Cmd {
	return func() tea.Msg {
		snapshots, err := listZFSSnapshots(dataset)
		return templateSnapshotListMsg{snapshots: snapshots, err: err}
	}
}

func templateSnapshotCloneCmd(dataset, snapshot, newName string, parentOverride *templateDatasetParent) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteTemplateSnapshotClone(dataset, snapshot, newName, parentOverride)
		return templateSnapshotCloneApplyMsg{result: result}
	}
}

func jailSnapshotCloneCmd(detail JailDetail, snapshot, newName, destination string, writeConfig bool) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteJailSnapshotClone(detail, snapshot, newName, destination, writeConfig)
		return jailSnapshotCloneApplyMsg{result: result}
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
		m.boundWizardScroll()
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
		if msg.err != nil || m.zfsPanel.message == "" || strings.HasPrefix(strings.ToLower(m.zfsPanel.message), "loading") || strings.HasPrefix(strings.ToLower(m.zfsPanel.message), "refreshing") {
			m.zfsPanel.message = msg.message
		}
		m.zfsPanel.boundCursor(m.zfsListHeight())
		return m, nil
	case zfsPropertyStateMsg:
		if m.mode != screenZFSPanel {
			return m, nil
		}
		if msg.err != nil {
			m.zfsPanel.err = msg.err
			m.zfsPanel.message = msg.err.Error()
			return m, nil
		}
		m.zfsPanel.propertyState = msg.properties
		if m.zfsPanel.propertyEditMode {
			m.zfsPanel.syncPropertyEditValue()
		}
		return m, nil
	case zfsActionMsg:
		if m.mode != screenZFSPanel {
			return m, nil
		}
		m.zfsPanel.actionRunning = false
		m.zfsPanel.logs = msg.logs
		m.zfsPanel.err = msg.err
		m.zfsPanel.message = msg.message
		return m, tea.Batch(listZFSSnapshotsCmd(m.zfsPanel.dataset), zfsPropertyStateCmd(m.zfsPanel.dataset))
	case zfsPropertyApplyMsg:
		if m.mode != screenZFSPanel {
			return m, nil
		}
		m.zfsPanel.actionRunning = false
		m.zfsPanel.logs = msg.logs
		m.zfsPanel.err = msg.err
		m.zfsPanel.message = msg.message
		if msg.err == nil {
			m.zfsPanel.propertyState = msg.properties
			m.zfsPanel.propertyEditMode = false
			if m.zfsPanel.sourceDetail.ZFS != nil {
				m.zfsPanel.sourceDetail.ZFS.Compression = msg.properties.Compression
				m.zfsPanel.sourceDetail.ZFS.Quota = msg.properties.Quota
				m.zfsPanel.sourceDetail.ZFS.Reservation = msg.properties.Reservation
			}
		}
		return m, tea.Batch(listZFSSnapshotsCmd(m.zfsPanel.dataset), zfsPropertyStateCmd(m.zfsPanel.dataset))
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
			m.templateCreate.message = fmt.Sprintf("Template dataset created: %s", msg.result.Dataset)
			if m.templateCreate.selectMode && m.templateCreate.returnMode == screenCreateWizard {
				m.wizard.datasetCreateRunning = false
				m.wizard.values.TemplateRelease = msg.result.Mountpoint
				m.wizard.executionLogs = append([]string(nil), msg.result.Logs...)
				m.wizard.message = fmt.Sprintf("Template dataset created: %s", msg.result.Dataset)
				m.mode = screenCreateWizard
				return m, nil
			}
			m.templateCreate.mode = templateManagerModeBrowse
			m.templateCreate.renameInput = ""
			return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
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
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
	case templateManagerRefreshMsg:
		if m.mode != screenTemplateDatasetCreate {
			return m, nil
		}
		m.templateCreate.loading = false
		m.templateCreate.items = append([]TemplateDatasetInfo(nil), msg.items...)
		m.templateCreate.parent = msg.parent
		m.templateCreate.boundCursor()
		m.templateCreate.syncSelection()
		if msg.err != nil {
			m.templateCreate.message = msg.err.Error()
			return m, nil
		}
		if m.templateCreate.message == "" && len(m.templateCreate.items) == 0 {
			m.templateCreate.message = "No template datasets found."
		}
		return m, nil
	case templateDatasetRenameApplyMsg:
		if m.mode != screenTemplateDatasetCreate {
			return m, nil
		}
		m.templateCreate.applying = false
		m.templateCreate.logs = append([]string(nil), msg.result.Logs...)
		if msg.result.Err != nil {
			m.templateCreate.message = msg.result.Err.Error()
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeBrowse
		m.templateCreate.renameInput = ""
		m.templateCreate.message = fmt.Sprintf("Template dataset renamed to %s", msg.result.Dataset)
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
	case templateDatasetDestroyApplyMsg:
		if m.mode != screenTemplateDatasetCreate {
			return m, nil
		}
		m.templateCreate.applying = false
		m.templateCreate.logs = append([]string(nil), msg.result.Logs...)
		if msg.result.Err != nil {
			m.templateCreate.message = msg.result.Err.Error()
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeBrowse
		m.templateCreate.message = fmt.Sprintf("Template dataset destroyed: %s", msg.result.Dataset)
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
	case templateSnapshotListMsg:
		if m.mode != screenTemplateDatasetCreate {
			return m, nil
		}
		m.templateCreate.cloneLoading = false
		m.templateCreate.cloneSnapshots = append([]ZFSSnapshot(nil), msg.snapshots...)
		m.templateCreate.boundCloneCursor()
		m.templateCreate.refreshClonePreview()
		if msg.err != nil {
			m.templateCreate.message = msg.err.Error()
			return m, nil
		}
		if len(m.templateCreate.cloneSnapshots) == 0 {
			m.templateCreate.message = "No snapshots found for this template dataset."
		}
		return m, nil
	case templateSnapshotCloneApplyMsg:
		if m.mode != screenTemplateDatasetCreate {
			return m, nil
		}
		m.templateCreate.applying = false
		m.templateCreate.logs = append([]string(nil), msg.result.Logs...)
		if msg.result.Err != nil {
			m.templateCreate.message = msg.result.Err.Error()
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeBrowse
		m.templateCreate.cloneSnapshots = nil
		m.templateCreate.cloneName = ""
		m.templateCreate.message = fmt.Sprintf("Template dataset cloned: %s", msg.result.Dataset)
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
	case jailSnapshotCloneApplyMsg:
		if m.mode != screenZFSPanel {
			return m, nil
		}
		m.zfsPanel.actionRunning = false
		m.zfsPanel.logs = append([]string(nil), msg.result.Logs...)
		if msg.result.Err != nil {
			m.zfsPanel.err = msg.result.Err
			m.zfsPanel.message = msg.result.Err.Error()
			return m, nil
		}
		m.zfsPanel.cloneMode = false
		m.zfsPanel.cloneName = ""
		m.zfsPanel.cloneDestination = ""
		m.zfsPanel.cloneWriteConfig = true
		m.zfsPanel.err = nil
		m.zfsPanel.message = fmt.Sprintf("Jail clone created: %s", msg.result.Name)
		return m, pollCmd()
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
		m.zfsPanel = newZFSPanelState(m.detail.ZFS.Name, screenDashboard, m.detail)
		return m, tea.Batch(listZFSSnapshotsCmd(m.zfsPanel.dataset), zfsPropertyStateCmd(m.zfsPanel.dataset))
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
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return false
		}
		if m.templateCreate.parentEdit {
			return true
		}
		return m.templateCreate.mode == templateManagerModeCreate || m.templateCreate.mode == templateManagerModeRename || m.templateCreate.mode == templateManagerModeClone
	case screenZFSPanel:
		return m.zfsPanel.inputMode || m.zfsPanel.cloneMode
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
		m.wizardScroll = 0
		m.notice = ""
		return m, nil
	case "i", "I":
		m.mode = screenInitialCheck
		m.initCheck = newInitialCheckState()
		m.notice = ""
		m.err = nil
		return m, collectInitialConfigCmd()
	case "t", "T":
		m.mode = screenTemplateDatasetCreate
		m.templateCreate = newTemplateDatasetCreateState("", m.initCheck.status, screenDashboard, false)
		m.templateCreate.loading = true
		m.notice = ""
		m.err = nil
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
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
		m.destroy = newDestroyState(jail, screenDashboard)
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
		m.detailShowAdvanced = false
		m.detailErr = nil
		m.detailNotice = ""
		m.detail = JailDetail{
			Name:                  jail.Name,
			JID:                   jail.JID,
			Path:                  jail.Path,
			Hostname:              jail.Hostname,
			JLSFields:             map[string]string{},
			RuntimeValues:         map[string]string{},
			AdvancedRuntimeFields: map[string]string{},
			JailConfValues:        map[string]string{},
			SourceErrors:          map[string]string{},
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
	case "a", "A":
		m.detailShowAdvanced = !m.detailShowAdvanced
		if m.detailShowAdvanced {
			m.detailNotice = "Showing advanced runtime parameters."
		} else {
			m.detailNotice = "Advanced runtime parameters hidden."
		}
		m.detailErr = nil
		return m, nil
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
		m.zfsPanel = newZFSPanelState(m.detail.ZFS.Name, screenJailDetail, m.detail)
		return m, tea.Batch(listZFSSnapshotsCmd(m.zfsPanel.dataset), zfsPropertyStateCmd(m.zfsPanel.dataset))
	case "x", "X":
		jail, ok := m.detailJail()
		if !ok {
			return m, nil
		}
		m.destroy = newDestroyState(jail, screenJailDetail)
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
			m.ensureWizardFieldVisible()
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
			m.templateCreate = newTemplateDatasetCreateState(sourceInput, m.initCheck.status, screenCreateWizard, true)
			m.templateCreate.mode = templateManagerModeCreate
			m.templateCreate.message = "Review the template dataset preview, then press enter to create it."
			m.mode = screenTemplateDatasetCreate
			m.templateCreate.loading = true
			return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
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
			m.ensureWizardFieldVisible()
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
			m.ensureWizardFieldVisible()
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
			m.ensureWizardFieldVisible()
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
			m.wizard.clearValidationError()
			m.wizard.refreshLinuxPrereqs()
			m.wizard.refreshNetworkPrereqs()
			m.wizard.normalizeStep()
			m.wizard.endTemplateMode()
			m.wizard.message = fmt.Sprintf("Template %q loaded.", template.Name)
			m.wizardScroll = 0
			m.ensureWizardFieldVisible()
			return m, nil
		}
		m.wizard.boundTemplateCursor()
		return m, nil
	}

	if !m.wizardApplying && !m.wizard.isConfirmationStep() && msg.Type == tea.KeyRunes {
		m.wizard.appendToActive(string(msg.Runes))
		m.boundWizardScroll()
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
		m.wizardScroll = 0
		m.ensureWizardFieldVisible()
		return m, nil
	case "right":
		if m.wizardApplying {
			return m, nil
		}
		if err := m.wizard.nextStep(); err != nil {
			m.ensureWizardFieldVisible()
			return m, nil
		}
		m.wizardScroll = 0
		m.ensureWizardFieldVisible()
		return m, nil
	case "tab", "down":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.nextField()
		m.ensureWizardFieldVisible()
		return m, nil
	case "shift+tab", "up":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.prevField()
		m.ensureWizardFieldVisible()
		return m, nil
	case "pgdown":
		if m.wizardApplying {
			return m, nil
		}
		m.wizardScroll += m.wizardBodyHeight()
		m.boundWizardScroll()
		return m, nil
	case "pgup":
		if m.wizardApplying {
			return m, nil
		}
		m.wizardScroll -= m.wizardBodyHeight()
		m.boundWizardScroll()
		return m, nil
	case "home":
		if m.wizardApplying {
			return m, nil
		}
		m.wizardScroll = 0
		return m, nil
	case "end":
		if m.wizardApplying {
			return m, nil
		}
		m.wizardScroll = 1 << 30
		m.boundWizardScroll()
		return m, nil
	case "s", "S", "ctrl+s":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.beginTemplateSave()
		m.wizardScroll = 0
		return m, nil
	case "l", "L", "ctrl+l":
		if m.wizardApplying {
			return m, nil
		}
		if err := m.wizard.beginTemplateLoad(); err != nil {
			m.wizard.message = err.Error()
			return m, nil
		}
		m.wizardScroll = 0
		return m, nil
	case "ctrl+u":
		if m.wizardApplying {
			return m, nil
		}
		if err := m.wizard.beginUserlandSelect(); err != nil {
			m.wizard.message = err.Error()
			return m, nil
		}
		m.wizardScroll = 0
		return m, nil
	case "ctrl+t":
		if m.wizardApplying {
			return m, nil
		}
		if normalizedJailType(m.wizard.values.JailType) != "thin" {
			m.wizard.message = "Thin template dataset selector is only used for thin jails."
			return m, nil
		}
		m.templateCreate = newTemplateDatasetCreateState(strings.TrimSpace(m.wizard.values.TemplateRelease), m.initCheck.status, screenCreateWizard, true)
		m.mode = screenTemplateDatasetCreate
		m.templateCreate.loading = true
		m.templateCreate.message = "Select a template dataset or press c to create one."
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
	case "enter":
		if m.wizard.isConfirmationStep() {
			if m.wizardApplying {
				return m, nil
			}
			if stepIdx, fieldID, err := m.wizard.validateAllDetailed(); err != nil {
				if stepIdx >= 0 {
					m.wizard.step = stepIdx
				}
				m.wizard.normalizeField()
				m.wizard.applyValidationError(fieldID, err)
				m.wizardScroll = 0
				m.ensureWizardFieldVisible()
				return m, nil
			}
			m.wizard.clearExecutionResult()
			m.wizard.clearValidationError()
			m.wizard.message = "Applying creation plan..."
			m.wizardApplying = true
			return m, createJailCmd(m.wizard.values)
		}
		if m.wizardApplying {
			return m, nil
		}
		_ = m.wizard.nextStep()
		m.wizardScroll = 0
		m.ensureWizardFieldVisible()
		return m, nil
	case "backspace", "delete":
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.backspaceActive()
		m.boundWizardScroll()
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

	if !m.templateCreate.applying && !m.templateCreate.parentApplying && msg.Type == tea.KeyRunes {
		if m.templateCreate.parentEdit {
			m.templateCreate.appendParentField(string(msg.Runes))
			return m, nil
		}
		switch m.templateCreate.mode {
		case templateManagerModeCreate:
			m.templateCreate.appendSource(string(msg.Runes))
			return m, nil
		case templateManagerModeRename:
			m.templateCreate.appendRenameInput(string(msg.Runes))
			return m, nil
		case templateManagerModeClone:
			m.templateCreate.appendCloneName(string(msg.Runes))
			return m, nil
		}
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
		if m.templateCreate.mode != templateManagerModeBrowse {
			m.templateCreate.mode = templateManagerModeBrowse
			m.templateCreate.message = "Template manager action canceled."
			return m, nil
		}
		m.mode = m.templateCreate.returnMode
		if m.templateCreate.returnMode == screenDashboard {
			m.notice = "Template manager closed."
		} else {
			m.wizard.datasetCreateRunning = false
			m.wizard.message = "Template selection canceled."
		}
		return m, nil
	case "tab", "down":
		if m.templateCreate.mode == templateManagerModeBrowse {
			m.templateCreate.cursor++
			m.templateCreate.boundCursor()
			return m, nil
		}
		if m.templateCreate.mode == templateManagerModeClone {
			m.templateCreate.cloneCursor++
			m.templateCreate.boundCloneCursor()
			m.templateCreate.refreshClonePreview()
			return m, nil
		}
		if m.templateCreate.parentEdit {
			m.templateCreate.parentField++
			if m.templateCreate.parentField > 1 {
				m.templateCreate.parentField = 0
			}
		}
		return m, nil
	case "shift+tab", "up":
		if m.templateCreate.mode == templateManagerModeBrowse {
			m.templateCreate.cursor--
			m.templateCreate.boundCursor()
			return m, nil
		}
		if m.templateCreate.mode == templateManagerModeClone {
			m.templateCreate.cloneCursor--
			m.templateCreate.boundCloneCursor()
			m.templateCreate.refreshClonePreview()
			return m, nil
		}
		if m.templateCreate.parentEdit {
			m.templateCreate.parentField--
			if m.templateCreate.parentField < 0 {
				m.templateCreate.parentField = 1
			}
		}
		return m, nil
	case "g", "home":
		if m.templateCreate.mode == templateManagerModeBrowse {
			m.templateCreate.cursor = 0
			m.templateCreate.boundCursor()
		}
		return m, nil
	case "G", "end":
		if m.templateCreate.mode == templateManagerModeBrowse {
			m.templateCreate.cursor = len(m.templateCreate.items) - 1
			m.templateCreate.boundCursor()
		}
		return m, nil
	case "c", "C":
		if m.templateCreate.applying || m.templateCreate.parentApplying || m.templateCreate.mode != templateManagerModeBrowse {
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeCreate
		m.templateCreate.logs = nil
		if strings.TrimSpace(m.templateCreate.sourceInput) == "" {
			if m.templateCreate.selectMode {
				m.templateCreate.sourceInput = strings.TrimSpace(m.wizard.values.TemplateRelease)
			}
		}
		m.templateCreate.refreshPreview()
		m.templateCreate.message = "Review the template dataset preview, then press enter to create it."
		return m, nil
	case "r":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		if m.templateCreate.mode == templateManagerModeBrowse {
			item, ok := m.templateCreate.selectedItem()
			if !ok {
				m.templateCreate.message = "No template dataset selected."
				return m, nil
			}
			m.templateCreate.mode = templateManagerModeRename
			m.templateCreate.renameInput = filepath.Base(item.Name)
			m.templateCreate.logs = nil
			m.templateCreate.refreshRenamePreview()
			m.templateCreate.message = "Edit the template name, then press enter to rename it."
			return m, nil
		}
		return m, nil
	case "x", "X":
		if m.templateCreate.applying || m.templateCreate.parentApplying || m.templateCreate.mode != templateManagerModeBrowse {
			return m, nil
		}
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			m.templateCreate.message = "No template dataset selected."
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeDestroy
		m.templateCreate.logs = nil
		m.templateCreate.destroyPreview = InspectTemplateDatasetDestroy(item.Name, m.templateCreate.parentOverride())
		if m.templateCreate.destroyPreview.Err != nil {
			m.templateCreate.message = m.templateCreate.destroyPreview.Err.Error()
		} else {
			m.templateCreate.message = "Press enter to destroy this template dataset."
		}
		return m, nil
	case "n", "N":
		if m.templateCreate.applying || m.templateCreate.parentApplying || m.templateCreate.mode != templateManagerModeBrowse {
			return m, nil
		}
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			m.templateCreate.message = "No template dataset selected."
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeClone
		m.templateCreate.cloneLoading = true
		m.templateCreate.cloneSnapshots = nil
		m.templateCreate.cloneCursor = 0
		m.templateCreate.cloneName = filepath.Base(item.Name) + "-clone"
		m.templateCreate.clonePreview = TemplateDatasetSnapshotClonePreview{}
		m.templateCreate.logs = nil
		m.templateCreate.message = "Loading template snapshots..."
		return m, templateSnapshotListCmd(item.Name)
	case "ctrl+e":
		if m.templateCreate.applying || m.templateCreate.parentApplying || m.templateCreate.mode != templateManagerModeCreate {
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
		if m.templateCreate.mode == templateManagerModeBrowse {
			if !m.templateCreate.selectMode {
				m.templateCreate.message = "Template details are shown on the right panel."
				return m, nil
			}
			item, ok := m.templateCreate.selectedItem()
			if !ok {
				m.templateCreate.message = "No template dataset selected."
				return m, nil
			}
			m.wizard.datasetCreateRunning = false
			m.wizard.values.TemplateRelease = item.Mountpoint
			m.wizard.message = fmt.Sprintf("Selected thin template dataset: %s", item.Name)
			m.mode = screenCreateWizard
			return m, nil
		}
		if m.templateCreate.mode == templateManagerModeCreate && (m.templateCreate.parentEdit || m.templateCreate.preview.NeedsParentCreate) {
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
		if m.templateCreate.mode == templateManagerModeCreate {
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
		}
		if m.templateCreate.mode == templateManagerModeRename {
			if m.templateCreate.renamePreview.Err != nil {
				m.templateCreate.message = m.templateCreate.renamePreview.Err.Error()
				return m, nil
			}
			m.templateCreate.message = "Renaming template dataset..."
			m.templateCreate.logs = nil
			m.templateCreate.applying = true
			return m, templateDatasetRenameCmd(m.templateCreate.renamePreview.Current.Name, m.templateCreate.renameInput, m.templateCreate.parentOverride())
		}
		if m.templateCreate.mode == templateManagerModeDestroy {
			if m.templateCreate.destroyPreview.Err != nil {
				m.templateCreate.message = m.templateCreate.destroyPreview.Err.Error()
				return m, nil
			}
			m.templateCreate.message = "Destroying template dataset..."
			m.templateCreate.logs = nil
			m.templateCreate.applying = true
			return m, templateDatasetDestroyCmd(m.templateCreate.destroyPreview.Current.Name, m.templateCreate.parentOverride())
		}
		if m.templateCreate.mode == templateManagerModeClone {
			if m.templateCreate.clonePreview.Err != nil {
				m.templateCreate.message = m.templateCreate.clonePreview.Err.Error()
				return m, nil
			}
			m.templateCreate.message = "Cloning template snapshot..."
			m.templateCreate.logs = nil
			m.templateCreate.applying = true
			return m, templateSnapshotCloneCmd(m.templateCreate.clonePreview.Current.Name, m.templateCreate.clonePreview.Snapshot, m.templateCreate.cloneName, m.templateCreate.parentOverride())
		}
	case "ctrl+r":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		if m.templateCreate.mode == templateManagerModeCreate {
			m.templateCreate.refreshPreview()
			m.templateCreate.message = "Template dataset preview refreshed."
			return m, nil
		}
		if m.templateCreate.mode == templateManagerModeClone {
			item, ok := m.templateCreate.selectedItem()
			if !ok {
				m.templateCreate.message = "No template dataset selected."
				return m, nil
			}
			m.templateCreate.cloneLoading = true
			m.templateCreate.message = "Refreshing template snapshots..."
			return m, templateSnapshotListCmd(item.Name)
		}
		m.templateCreate.loading = true
		m.templateCreate.message = "Template dataset list refreshed."
		return m, templateManagerRefreshCmd(m.templateCreate.parentOverride())
	case "backspace", "delete":
		if m.templateCreate.applying || m.templateCreate.parentApplying {
			return m, nil
		}
		if m.templateCreate.parentEdit {
			m.templateCreate.backspaceParentField()
			return m, nil
		}
		switch m.templateCreate.mode {
		case templateManagerModeCreate:
			m.templateCreate.backspaceSource()
		case templateManagerModeRename:
			m.templateCreate.backspaceRenameInput()
		case templateManagerModeClone:
			m.templateCreate.backspaceCloneName()
		}
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

	footer := m.renderFooterWithMessage("j/k or pgup/pgdown scroll | esc/enter: close help | ctrl+c: quit", "", footerStyle)
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) helpLines(width int) []string {
	lines := []string{
		sectionStyle.Render("Global"),
		truncate("?: open help page (h works outside text input)", width),
		truncate("ctrl+c: quit the application", width),
		truncate("q: quit outside text input", width),
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
		truncate("i: re-run initial config check", width),
		truncate("t: open template manager", width),
		truncate("s: start or stop selected jail", width),
		truncate("z: open ZFS panel for selected jail", width),
		truncate("x: destroy selected jail (confirmation required)", width),
		truncate("r: refresh dashboard data", width),
		"",
		sectionStyle.Render("Jail Detail"),
		truncate("j/k, pgup/pgdown, g/G: scroll detail", width),
		truncate("a: toggle advanced runtime/default parameters", width),
		truncate("startup policy shows jail_list order and configured depend values", width),
		truncate("network summary shows configured/runtime network state plus host validation", width),
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
		truncate("n: clone selected snapshot as a new jail", width),
		truncate("e: edit compression, quota, or reservation on an exact jail dataset", width),
		truncate("selected snapshot details show creation time, used size, and rollback implications", width),
		truncate("x: refresh snapshot list", width),
		truncate("esc: cancel prompt or return to detail", width),
		"",
		sectionStyle.Render("Creation Wizard"),
		truncate("common jail settings are on one page; linux adds a bootstrap step before confirmation", width),
		truncate("tab/shift+tab/up/down: move field", width),
		truncate("pgup/pgdown: scroll long wizard pages", width),
		truncate("enter/right: next step", width),
		truncate("left: previous step", width),
		truncate("s/l on the confirmation step: save/load templates", width),
		truncate("ctrl+u: open userland selector", width),
		truncate("ctrl+t: open template manager in thin-jail selection mode", width),
		truncate("startup order updates rc.conf jail_list; dependencies write depend in jail.conf", width),
		truncate("linux step supports default or custom bootstrap mirrors; retry uses the saved mirror choice", width),
		truncate("vnet preflight checks bridge/uplink host state, running-jail IP conflicts, subnet overlap warnings, and bridge policy before create", width),
		truncate("?: open help page", width),
		truncate("confirmation enter: execute create actions", width),
		"",
		sectionStyle.Render("Template Manager"),
		truncate("j/k: select | c: create | n: clone snapshot | r: rename | x: destroy", width),
		truncate("enter applies the selected mountpoint when opened from the thin-jail wizard", width),
		truncate("create mode accepts a source path, release tag, userland entry, or custom URL", width),
		truncate("clone mode lets you pick a snapshot and clone it into a new template dataset", width),
		truncate("when parent templates dataset is missing: enter creates proposed parent, ctrl+e edits parent values", width),
		truncate("ctrl+r: refresh the template list, or refresh create preview while creating", width),
		truncate("esc: return to dashboard", width),
	}
	return lines
}

func (m model) renderDestroyView() string {
	title := titleStyle.Render("Destroy Jail")
	meta := detailKeyStyle.Render("Selected:") + " " + selectedRowStyle.Render(valueOrDash(m.destroy.target.Name))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyWidth := max(12, m.width-2)
	lines := []string{sectionStyle.Render("Confirmation")}
	for _, line := range m.destroy.preview {
		lines = append(lines, truncate(line, bodyWidth))
	}
	if len(m.destroy.logs) > 0 {
		appendSection(&lines, bodyWidth, "Execution output")
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
	footerRenderer := footerStyle
	message := m.destroy.message
	if m.destroy.err != nil {
		message = "error: " + m.destroy.err.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	}
	footer := m.renderFooterWithMessage(hint, message, footerRenderer)
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

	hint := "j/k or up/down: scroll | pgup/pgdown | g/G | a: advanced runtime | r: refresh detail"
	if detailLooksLikeLinuxJail(m.detail) {
		hint += " | b: retry linux bootstrap"
	}
	hint += " | z: ZFS panel | x: destroy | h: help | esc: back | q: quit"
	message := ""
	footerRenderer := footerStyle
	if m.detailErr != nil {
		message = "warning: " + m.detailErr.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	} else if m.detailNotice != "" {
		message = m.detailNotice
		if looksLikeWarningText(m.detailNotice) {
			footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
		}
	} else if m.detailLoading {
		message = "loading detail..."
	}
	footer := m.renderFooterWithMessage(hint, message, footerRenderer)

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) renderWizardView() string {
	step := m.wizard.currentStep()
	title := titleStyle.Render("Jail Creation Wizard")
	meta := summaryStyle.Render(fmt.Sprintf("Step %d/%d: %s", m.wizard.step+1, len(m.wizard.steps()), step.Title))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	hint := m.wizardFooterHint()
	footer := m.renderFooterWithMessage(hint, m.wizard.message, footerStyle)
	bodyHeight := max(4, m.height-lipgloss.Height(header)-lipgloss.Height(footer))
	body := ""
	if leftWidth, rightWidth, ok := m.wizardSplitPaneWidths(); ok {
		leftLines, _ := m.wizardFieldEntryLayout(max(12, leftWidth-2), false)
		offset, end := wizardViewportBounds(len(leftLines), m.wizardScroll, bodyHeight)
		leftPanel := lipgloss.NewStyle().
			Width(leftWidth).
			Height(bodyHeight).
			Padding(0, 1).
			Render(strings.Join(leftLines[offset:end], "\n"))
		rightPanel := lipgloss.NewStyle().
			Width(rightWidth).
			Height(bodyHeight).
			Padding(0, 1).
			Render(strings.Join(m.wizardFieldContextLines(max(12, rightWidth-2)), "\n"))
		separator := lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Render(strings.Repeat("|\n", bodyHeight-1) + "|")
		body = lipgloss.JoinHorizontal(lipgloss.Top, leftPanel, separator, rightPanel)
	}
	if body == "" {
		lines := m.wizardLines(max(12, m.width-2))
		offset, end := wizardViewportBounds(len(lines), m.wizardScroll, bodyHeight)
		body = lipgloss.NewStyle().
			Width(m.width).
			Height(bodyHeight).
			Padding(1, 1).
			Render(strings.Join(lines[offset:end], "\n"))
	}

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) wizardFooterHint() string {
	hint := "type to edit | tab/shift+tab/up/down: fields | pgup/pgdown: scroll | ctrl+u: userland select | enter/right: next | left: back | ?: help | esc: cancel | ctrl+c: quit"
	if normalizedJailType(m.wizard.values.JailType) == "thin" {
		hint = "type to edit | tab/shift+tab/up/down: fields | pgup/pgdown: scroll | ctrl+u: userland select | ctrl+t: template manager | enter/right: next | left: back | ?: help | esc: cancel | ctrl+c: quit"
	}
	if m.wizard.isConfirmationStep() {
		hint = "pgup/pgdown: scroll | enter: create jail now | left: back | s: save tmpl | l: load tmpl | ?: help | esc: cancel | q: quit | ctrl+c: quit"
	}
	if m.wizard.templateMode == wizardTemplateModeSave {
		hint = "Template save: type name | enter: save | backspace: edit | esc: cancel | ctrl+c: quit"
	}
	if m.wizard.templateMode == wizardTemplateModeLoad {
		hint = "Template load: j/k select | enter: load | r: refresh list | esc: cancel | q: quit | ctrl+c: quit"
	}
	if m.wizard.userlandMode {
		hint = "Userland select: j/k choose | enter: apply | r: refresh options | esc: cancel | q: quit | ctrl+c: quit"
	}
	if m.wizard.thinDatasetMode {
		hint = "Thin template select: j/k choose | enter: apply | c: create from Template/Release | r: refresh options | esc: cancel | q: quit | ctrl+c: quit"
	}
	if m.wizard.datasetCreateRunning {
		hint = "Creating template dataset... please wait | ctrl+c: quit"
	}
	if m.wizardApplying {
		hint = "Applying changes... please wait | ctrl+c: quit"
	}
	return hint
}

func (m model) wizardBodyHeight() int {
	step := m.wizard.currentStep()
	title := titleStyle.Render("Jail Creation Wizard")
	meta := summaryStyle.Render(fmt.Sprintf("Step %d/%d: %s", m.wizard.step+1, len(m.wizard.steps()), step.Title))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)
	footer := m.renderFooterWithMessage(m.wizardFooterHint(), m.wizard.message, footerStyle)
	return max(4, m.height-lipgloss.Height(header)-lipgloss.Height(footer))
}

func (m model) wizardSplitPaneWidths() (int, int, bool) {
	if !m.wizardUsesSplitPane() {
		return 0, 0, false
	}
	leftWidth := max(48, (m.width-1)*3/5)
	if leftWidth > m.width-34 {
		leftWidth = m.width - 34
	}
	if leftWidth < 40 {
		leftWidth = 40
	}
	rightWidth := m.width - leftWidth - 1
	if rightWidth < 32 {
		return 0, 0, false
	}
	return leftWidth, rightWidth, true
}

func wizardViewportBounds(totalLines, scroll, height int) (int, int) {
	if height <= 0 {
		height = 1
	}
	maxOffset := max(0, totalLines-height)
	if scroll < 0 {
		scroll = 0
	}
	if scroll > maxOffset {
		scroll = maxOffset
	}
	end := min(totalLines, scroll+height)
	if end < scroll {
		end = scroll
	}
	return scroll, end
}

func (m model) renderTemplateDatasetCreateView() string {
	title := titleStyle.Render("Template Manager")
	meta := summaryStyle.Render("Reusable ZFS templates for thin jails")
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := max(6, m.height-3)
	leftWidth := max(30, m.width/3)
	if leftWidth > m.width-28 {
		leftWidth = m.width - 28
	}
	if leftWidth < 24 {
		leftWidth = m.width
	}
	rightWidth := m.width - leftWidth - 1
	if rightWidth < 0 {
		rightWidth = 0
	}

	listPanel := lipgloss.NewStyle().
		Width(leftWidth).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(m.templateManagerListLines(max(12, leftWidth-2), bodyHeight), "\n"))

	body := listPanel
	if rightWidth > 0 {
		detailPanel := lipgloss.NewStyle().
			Width(rightWidth).
			Height(bodyHeight).
			Padding(0, 1).
			Render(strings.Join(m.templateManagerDetailLines(max(12, rightWidth-2)), "\n"))
		separator := lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Render(strings.Repeat("|\n", bodyHeight-1) + "|")
		body = lipgloss.JoinHorizontal(lipgloss.Top, listPanel, separator, detailPanel)
	}

	hint := "j/k: select | c: create | n: clone | r: rename | x: destroy | ctrl+r: refresh | ?: help | esc: back | ctrl+c: quit"
	if m.templateCreate.selectMode && m.templateCreate.mode == templateManagerModeBrowse {
		hint = "j/k: select | enter: apply mountpoint | c: create | n: clone | r: rename | x: destroy | ctrl+r: refresh | ?: help | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.mode == templateManagerModeCreate {
		hint = "type source | enter: create | backspace: edit | ctrl+r: refresh preview | ctrl+e: edit parent | esc: back | ctrl+c: quit"
		if m.templateCreate.parentEdit {
			hint = "type parent values | tab/shift+tab: switch field | enter: create parent | esc: stop editing | ctrl+c: quit"
		}
		if m.templateCreate.preview.NeedsParentCreate && !m.templateCreate.parentEdit {
			hint = "type source | enter: create proposed parent | ctrl+e: edit parent values | ctrl+r: refresh preview | esc: back | ctrl+c: quit"
		}
	}
	if m.templateCreate.mode == templateManagerModeRename {
		hint = "type new name | enter: rename | backspace: edit | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.mode == templateManagerModeDestroy {
		hint = "enter: destroy dataset | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.mode == templateManagerModeClone {
		hint = "type clone name | j/k: snapshot | enter: clone | backspace: edit | ctrl+r: refresh snapshots | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.loading {
		hint = "Refreshing template datasets... please wait | ctrl+c: quit"
	}
	if m.templateCreate.cloneLoading {
		hint = "Loading template snapshots... please wait | ctrl+c: quit"
	}
	if m.templateCreate.parentApplying {
		hint = "Creating template parent dataset... please wait | ctrl+c: quit"
	}
	if m.templateCreate.applying {
		switch m.templateCreate.mode {
		case templateManagerModeRename:
			hint = "Renaming template dataset... please wait | ctrl+c: quit"
		case templateManagerModeDestroy:
			hint = "Destroying template dataset... please wait | ctrl+c: quit"
		case templateManagerModeClone:
			hint = "Cloning template snapshot... please wait | ctrl+c: quit"
		default:
			hint = "Creating template dataset... please wait | ctrl+c: quit"
		}
	}
	footer := m.renderFooterWithMessage(hint, m.templateCreate.message, footerStyle)

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) templateManagerListLines(width, height int) []string {
	lines := []string{panelTitleStyle.Render("Template Datasets")}
	if m.templateCreate.parent != nil {
		lines = append(lines, truncate("Parent: "+m.templateCreate.parent.Name, width))
	} else {
		lines = append(lines, truncate("Parent: (not discovered)", width))
	}
	lines = append(lines, "")
	if m.templateCreate.loading {
		lines = append(lines, "Loading template datasets...")
		return lines
	}
	if len(m.templateCreate.items) == 0 {
		lines = append(lines, "No template datasets found.")
		lines = append(lines, "Press c to create one.")
		return lines
	}
	maxRows := max(1, height-4)
	start := 0
	if m.templateCreate.cursor >= maxRows {
		start = m.templateCreate.cursor - maxRows + 1
	}
	end := min(len(m.templateCreate.items), start+maxRows)
	for idx := start; idx < end; idx++ {
		item := m.templateCreate.items[idx]
		row := fmt.Sprintf("  %s", filepath.Base(item.Name))
		if idx == m.templateCreate.cursor {
			row = selectedRowStyle.Width(max(1, width)).Render(truncate("> "+filepath.Base(item.Name), width))
		} else {
			row = truncate(row, width)
		}
		lines = append(lines, row)
	}
	return lines
}

func (m model) templateManagerDetailLines(width int) []string {
	lines := []string{}
	switch m.templateCreate.mode {
	case templateManagerModeCreate:
		appendSection(&lines, width, "Create template dataset",
			"Sources: local directory, local archive, release tag, custom URL, or named userland entry.",
		)
		sourceDisplay := m.templateCreate.sourceInput
		if strings.TrimSpace(sourceDisplay) == "" {
			sourceDisplay = "(15.0-RELEASE)"
		}
		lines = append(lines, selectedRowStyle.Width(max(1, width)).Render(truncate("> Template/Release: "+sourceDisplay, width)))
		appendSection(&lines, width, "Preview")
		preview := m.templateCreate.preview
		parentDataset := preview.ParentDataset
		parentMountpoint := preview.ParentMountpoint
		if parentDataset == "" && strings.TrimSpace(m.templateCreate.parentDataset) != "" {
			parentDataset = m.templateCreate.parentDataset
		}
		if parentMountpoint == "" && strings.TrimSpace(m.templateCreate.parentMountpoint) != "" {
			parentMountpoint = filepath.Clean(m.templateCreate.parentMountpoint)
		}
		showParentFields := preview.NeedsParentCreate || m.templateCreate.parentEdit || parentDataset != "" || parentMountpoint != ""
		if showParentFields {
			datasetLabel := "Parent dataset"
			mountLabel := "Parent mountpoint"
			if preview.NeedsParentCreate {
				datasetLabel = "Proposed parent dataset"
				mountLabel = "Proposed parent mountpoint"
			}
			datasetDisplay := parentDataset
			if strings.TrimSpace(datasetDisplay) == "" {
				datasetDisplay = "(enter dataset name)"
			}
			mountDisplay := parentMountpoint
			if strings.TrimSpace(mountDisplay) == "" {
				mountDisplay = "(enter absolute mountpoint)"
			}
			datasetLine := truncate(datasetLabel+": "+datasetDisplay, width)
			if m.templateCreate.parentEdit && m.templateCreate.parentField == 0 {
				datasetLine = selectedRowStyle.Width(max(1, width)).Render(truncate("> "+datasetLabel+": "+datasetDisplay, width))
			}
			lines = append(lines, datasetLine)
			mountLine := truncate(mountLabel+": "+mountDisplay, width)
			if m.templateCreate.parentEdit && m.templateCreate.parentField == 1 {
				mountLine = selectedRowStyle.Width(max(1, width)).Render(truncate("> "+mountLabel+": "+mountDisplay, width))
			}
			lines = append(lines, mountLine)
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
			lines = append(lines, truncate("No templates parent dataset was discovered. Create the proposed parent or edit the values first.", width))
		} else if preview.Err != nil {
			for _, line := range wrapText("Error: "+preview.Err.Error(), width) {
				lines = append(lines, wizardErrorStyle.Render(line))
			}
		}
	case templateManagerModeRename:
		appendSection(&lines, width, "Rename template dataset")
		preview := m.templateCreate.renamePreview
		current := preview.Current
		if current.Name == "" {
			lines = append(lines, "No template dataset selected.")
		} else {
			lines = append(lines, truncate("Current dataset: "+current.Name, width))
			lines = append(lines, truncate("Current mountpoint: "+current.Mountpoint, width))
			lines = append(lines, selectedRowStyle.Width(max(1, width)).Render(truncate("> New name: "+valueOrPlaceholder(m.templateCreate.renameInput, filepath.Base(current.Name)), width)))
			if preview.NewDataset != "" {
				lines = append(lines, truncate("Renamed dataset: "+preview.NewDataset, width))
			}
			if preview.NewMountpoint != "" {
				lines = append(lines, truncate("New mountpoint: "+preview.NewMountpoint, width))
			}
			if len(preview.UpdatedWizardTemplates) > 0 {
				lines = append(lines, truncate("Saved wizard templates updated on rename: "+strings.Join(preview.UpdatedWizardTemplates, ", "), width))
			}
			if preview.Err != nil {
				for _, line := range wrapText("Error: "+preview.Err.Error(), width) {
					lines = append(lines, wizardErrorStyle.Render(line))
				}
			}
		}
	case templateManagerModeDestroy:
		appendSection(&lines, width, "Destroy template dataset")
		preview := m.templateCreate.destroyPreview
		current := preview.Current
		if current.Name == "" {
			lines = append(lines, "No template dataset selected.")
		} else {
			lines = append(lines, truncate("Dataset: "+current.Name, width))
			lines = append(lines, truncate("Mountpoint: "+current.Mountpoint, width))
			lines = append(lines, truncate("Destroy scope: zfs destroy -r "+preview.DestroyScope, width))
			lines = append(lines, truncate(fmt.Sprintf("Snapshots: %d", current.SnapshotCount), width))
			lines = append(lines, truncate(fmt.Sprintf("Clone dependents: %d", len(current.CloneDependents)), width))
			lines = append(lines, truncate(fmt.Sprintf("Saved wizard template refs: %d", len(preview.ReferencedTemplates)), width))
			if len(current.CloneDependents) > 0 {
				lines = append(lines, truncate("Dependents: "+strings.Join(current.CloneDependents, ", "), width))
			}
			if len(preview.ReferencedTemplates) > 0 {
				lines = append(lines, truncate("Referenced by: "+strings.Join(preview.ReferencedTemplates, ", "), width))
			}
			if preview.Err != nil {
				for _, line := range wrapText("Error: "+preview.Err.Error(), width) {
					lines = append(lines, wizardErrorStyle.Render(line))
				}
			}
		}
	case templateManagerModeClone:
		appendSection(&lines, width, "Clone template snapshot")
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			lines = append(lines, "No template dataset selected.")
			break
		}
		lines = append(lines, truncate("Dataset: "+item.Name, width))
		lines = append(lines, selectedRowStyle.Width(max(1, width)).Render(truncate("> New name: "+valueOrPlaceholder(m.templateCreate.cloneName, filepath.Base(item.Name)+"-clone"), width)))
		appendSection(&lines, width, "Snapshots")
		if m.templateCreate.cloneLoading {
			lines = append(lines, "Loading snapshots...")
			break
		}
		if len(m.templateCreate.cloneSnapshots) == 0 {
			lines = append(lines, "No snapshots found for this template dataset.")
			break
		}
		for idx, snapshot := range m.templateCreate.cloneSnapshots {
			row := fmt.Sprintf("  %s  %s  %s", truncate(snapshotShortName(snapshot.Name), 18), truncate(snapshot.Creation, 18), snapshot.Used)
			if idx == m.templateCreate.cloneCursor {
				row = selectedRowStyle.Width(max(1, width)).Render(truncate("> "+fmt.Sprintf("%s  %s  %s", snapshotShortName(snapshot.Name), snapshot.Creation, snapshot.Used), width))
			} else {
				row = truncate(row, width)
			}
			lines = append(lines, row)
		}
		appendSection(&lines, width, "Preview")
		preview := m.templateCreate.clonePreview
		if preview.Snapshot != "" {
			lines = append(lines, truncate("Snapshot: "+preview.Snapshot, width))
		}
		if preview.NewDataset != "" {
			lines = append(lines, truncate("Clone dataset: "+preview.NewDataset, width))
		}
		if preview.NewMountpoint != "" {
			lines = append(lines, truncate("Clone mountpoint: "+preview.NewMountpoint, width))
		}
		if preview.Err != nil {
			for _, line := range wrapText("Error: "+preview.Err.Error(), width) {
				lines = append(lines, wizardErrorStyle.Render(line))
			}
		}
	default:
		appendSection(&lines, width, "Inspect")
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			lines = append(lines, "No template dataset selected.")
		} else {
			lines = append(lines, truncate("Dataset: "+item.Name, width))
			lines = append(lines, truncate("Mountpoint: "+item.Mountpoint, width))
			lines = append(lines, truncate("Used: "+item.Used+"  Avail: "+item.Avail, width))
			lines = append(lines, truncate("Refer: "+item.Refer+"  Compression: "+item.Compression, width))
			lines = append(lines, truncate("Quota: "+valueOrDash(item.Quota)+"  Reservation: "+valueOrDash(item.Reservation), width))
			if strings.TrimSpace(item.Origin) != "" && item.Origin != "-" {
				lines = append(lines, truncate("Origin: "+item.Origin, width))
			}
			lines = append(lines, truncate(fmt.Sprintf("Snapshots: %d", item.SnapshotCount), width))
			lines = append(lines, truncate(fmt.Sprintf("Child datasets: %d", len(item.ChildDatasets)), width))
			lines = append(lines, truncate(fmt.Sprintf("Clone dependents: %d", len(item.CloneDependents)), width))
			lines = append(lines, truncate(fmt.Sprintf("Saved wizard template refs: %d", len(item.WizardTemplateRefs)), width))
			appendSection(&lines, width, "Safety")
			lines = append(lines, truncate("Rename allowed: "+yesNoText(item.RenameAllowed), width))
			lines = append(lines, truncate("Destroy allowed: "+yesNoText(item.DestroyAllowed), width))
			if len(item.SafetyIssues) > 0 {
				for _, issue := range item.SafetyIssues {
					lines = append(lines, wizardErrorStyle.Render(truncate("Issue: "+issue, width)))
				}
			}
			if len(item.CloneDependents) > 0 {
				lines = append(lines, truncate("Dependents: "+strings.Join(item.CloneDependents, ", "), width))
			}
			if len(item.ChildDatasets) > 0 {
				lines = append(lines, truncate("Child datasets: "+strings.Join(item.ChildDatasets, ", "), width))
			}
			if len(item.WizardTemplateRefs) > 0 {
				lines = append(lines, truncate("Saved template refs: "+strings.Join(item.WizardTemplateRefs, ", "), width))
			}
		}
	}
	if len(m.templateCreate.logs) > 0 {
		appendSection(&lines, width, "Execution output")
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
		if shouldShowNetworkPrereqs(m.wizard.networkPrereqs) {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Network prerequisites"))
			for _, line := range networkWizardPrereqLines(m.wizard.networkPrereqs) {
				appendStyledWizardLine(&lines, line, width)
			}
		}
		if normalizedJailType(m.wizard.values.JailType) == "linux" {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Linux prerequisites"))
			for _, line := range linuxWizardPrereqLines(m.wizard.linuxPrereqs) {
				appendStyledWizardLine(&lines, line, width)
			}
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

	lines = append(lines, m.wizardFieldEntryLines(width, true)...)
	return lines
}

func (m model) wizardUsesSplitPane() bool {
	if m.width < 110 {
		return false
	}
	if m.wizard.templateMode != wizardTemplateModeNone || m.wizard.userlandMode || m.wizard.thinDatasetMode {
		return false
	}
	if m.wizard.datasetCreateRunning || m.wizardApplying || m.wizard.isConfirmationStep() {
		return false
	}
	return len(m.wizard.visibleFields()) > 0
}

func (m model) wizardFieldEntryLines(width int, inlineHelp bool) []string {
	lines, _ := m.wizardFieldEntryLayout(width, inlineHelp)
	return lines
}

func (m model) wizardFieldEntryLayout(width int, inlineHelp bool) ([]string, int) {
	lines := []string{}
	activeLine := -1
	currentSection := ""
	fields := m.wizard.visibleFields()
	if len(fields) == 0 {
		return []string{"No editable fields on this step."}, -1
	}
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
		label := m.wizardFieldDisplayLabel(field)
		line := fmt.Sprintf("%s %s: %s", prefix, label, display)
		line = truncate(line, width)
		if idx == m.wizard.field {
			line = selectedRowStyle.Width(max(1, width)).Render(line)
		}
		lines = append(lines, line)
		if idx == m.wizard.field {
			activeLine = len(lines) - 1
		}
		if inlineHelp && idx == m.wizard.field && field.Help != "" {
			if field.ID == "template_release" {
				lines = append(lines, "")
			}
			lines = append(lines, truncate("  "+field.Help, width))
		}
		if inlineHelp && idx == m.wizard.field && field.ID == "template_release" {
			lines = append(lines, truncate("  ctrl+u: select local userland or release download", width))
			if normalizedJailType(m.wizard.values.JailType) == "thin" {
				lines = append(lines, truncate("  ctrl+t: choose an extracted ZFS template dataset", width))
			}
		}
		if idx == m.wizard.field && field.ID == m.wizard.validationField && strings.TrimSpace(m.wizard.validationError) != "" {
			for _, line := range wrapText("  error: "+m.wizard.validationError, max(8, width-2)) {
				lines = append(lines, wizardErrorStyle.Render(truncate(line, width)))
			}
		}
	}

	if inlineHelp && shouldShowNetworkPrereqs(m.wizard.networkPrereqs) && wizardShowsNetworkPrereqs(m.wizard.currentStep()) {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Network prerequisites"))
		for _, line := range networkWizardPrereqLines(m.wizard.networkPrereqs) {
			appendStyledWizardLine(&lines, line, width)
		}
	}

	if inlineHelp && normalizedJailType(m.wizard.values.JailType) == "linux" && wizardsShowsLinuxPrereqs(m.wizard.currentStep()) {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Linux prerequisites"))
		for _, line := range linuxWizardPrereqLines(m.wizard.linuxPrereqs) {
			appendStyledWizardLine(&lines, line, width)
		}
	}

	return lines, activeLine
}

func (m model) wizardFieldContextLines(width int) []string {
	lines := []string{panelTitleStyle.Render("Field Guide")}
	field, ok := m.wizard.activeField()
	if !ok {
		lines = append(lines, "Select a field to see accepted values and examples.")
		return lines
	}
	lines = append(lines, renderKeyValueLines(width,
		[2]string{"Field", m.wizardFieldDisplayLabel(field)},
	)...)

	guide := m.wizardFieldGuide(field)
	if guide.Purpose != "" {
		appendSection(&lines, width, "Purpose")
		for _, line := range wrapText(guide.Purpose, width) {
			lines = append(lines, line)
		}
	}
	if guide.Format != "" {
		appendSection(&lines, width, "Accepted format")
		for _, line := range wrapText(guide.Format, width) {
			lines = append(lines, line)
		}
	}
	if len(guide.Examples) > 0 {
		appendSection(&lines, width, "Examples")
		for _, example := range guide.Examples {
			for _, line := range wrapText("- "+example, width) {
				lines = append(lines, line)
			}
		}
	}
	if len(guide.Notes) > 0 {
		appendSection(&lines, width, "Notes")
		for _, note := range guide.Notes {
			isWarning := looksLikeWarningText(note)
			for _, line := range wrapText(note, width) {
				rendered := line
				if isWarning {
					rendered = wizardErrorStyle.Render(line)
				}
				lines = append(lines, rendered)
			}
		}
	}
	if len(guide.Shortcuts) > 0 {
		appendSection(&lines, width, "Shortcuts")
		for _, shortcut := range guide.Shortcuts {
			for _, line := range wrapText("- "+shortcut, width) {
				lines = append(lines, line)
			}
		}
	}

	if wizardFieldUsesNetworkContext(field.ID) && shouldShowNetworkPrereqs(m.wizard.networkPrereqs) {
		appendSection(&lines, width, "Host checks")
		for _, line := range networkWizardPrereqLines(m.wizard.networkPrereqs) {
			appendWrappedStyledWizardLine(&lines, line, width)
		}
	}
	if wizardFieldUsesLinuxContext(field.ID) {
		appendSection(&lines, width, "Linux prerequisites")
		for _, line := range linuxWizardContextLines(m.wizard.linuxPrereqs) {
			appendWrappedStyledWizardLine(&lines, line, width)
		}
	}

	return lines
}

type wizardFieldGuide struct {
	Purpose   string
	Format    string
	Examples  []string
	Notes     []string
	Shortcuts []string
}

func (m model) wizardFieldDisplayLabel(field wizardField) string {
	if field.ID == "template_release" && normalizedJailType(m.wizard.values.JailType) == "linux" {
		return "FreeBSD Base/Release"
	}
	return field.Label
}

func (m model) wizardFieldGuide(field wizardField) wizardFieldGuide {
	jailType := normalizedJailType(m.wizard.values.JailType)
	switch field.ID {
	case "jail_type":
		return wizardFieldGuide{
			Purpose: "Choose the jail model. This decides how the root is provisioned and how networking is configured.",
			Format:  "One of: thick, thin, vnet, linux.",
			Examples: []string{
				"thick for a copied root filesystem",
				"thin for a ZFS template dataset clone",
				"vnet for bridge-backed virtual networking",
				"linux for a FreeBSD jail with Linux userland bootstrap",
			},
		}
	case "name":
		return wizardFieldGuide{
			Purpose: "The jail name is used for jail.conf, fstab, rctl rules, and several generated paths.",
			Format:  "Letters, numbers, dot, underscore, and dash only.",
			Examples: []string{
				"web01",
				"db-primary",
			},
		}
	case "hostname":
		return wizardFieldGuide{
			Purpose: "Hostname assigned inside the jail. Leave blank to reuse the jail name.",
			Examples: []string{
				"web01.example.internal",
				"pkg-cache.local",
			},
		}
	case "dataset":
		return wizardFieldGuide{
			Purpose: "Absolute jail root path. The final path must end with the jail name.",
			Format:  "Absolute path only. Shared roots like /usr/local/jails/containers are not valid jail roots by themselves.",
			Examples: []string{
				"/usr/local/jails/containers/web01",
				"/usr/local/jails/thick/db01",
			},
		}
	case "template_release":
		guide := wizardFieldGuide{
			Purpose: "Source used to populate the FreeBSD jail root.",
			Format:  "Local path, FreeBSD release tag, named userland source, or custom https URL.",
			Examples: []string{
				"15.0-RELEASE",
				"/usr/local/jails/media/base.txz",
				"https://download.freebsd.org/ftp/releases/amd64/amd64/15.0-RELEASE/base.txz",
			},
			Shortcuts: []string{
				"ctrl+u selects a discovered local userland source or release download",
			},
		}
		switch jailType {
		case "thin":
			guide.Notes = append(guide.Notes,
				"Thin jails require an extracted ZFS template dataset mountpoint, not a release tag or archive, at create time.",
			)
			guide.Shortcuts = append(guide.Shortcuts, "ctrl+t opens Template Manager to select or create a reusable dataset")
		case "linux":
			guide.Purpose = "Source used to build the FreeBSD jail base before Linux userland is bootstrapped under /compat."
			guide.Notes = append(guide.Notes, "This is still a FreeBSD base/root source. The Linux bootstrap family and release are configured on the next step.")
		}
		return guide
	case "interface":
		return wizardFieldGuide{
			Purpose: "Host interface used by shared-stack jails.",
			Format:  "Existing host interface name.",
			Examples: []string{
				"em0",
				"igb0",
			},
			Notes: []string{
				"Required for thick, thin, and linux jails. VNET jails use bridge and uplink instead.",
			},
		}
	case "bridge":
		return wizardFieldGuide{
			Purpose: "Bridge device used for VNET connectivity.",
			Format:  "Bridge interface name such as bridge0.",
			Examples: []string{
				"bridge0",
				"bridge1",
			},
		}
	case "bridge_policy":
		return wizardFieldGuide{
			Purpose: "Controls whether the TUI may create a missing bridge before jail creation.",
			Format:  "auto-create or require-existing.",
			Examples: []string{
				"auto-create to let the TUI create bridge0 if it is missing",
				"require-existing to fail unless the named bridge already exists",
			},
		}
	case "uplink":
		return wizardFieldGuide{
			Purpose: "Optional host interface to attach to the selected bridge.",
			Format:  "Existing host interface name or blank.",
			Examples: []string{
				"em0",
				"ix0",
			},
			Notes: []string{
				"Use this when the bridge should be connected to a physical or VLAN-backed host interface.",
			},
		}
	case "ip4":
		notes := []string{}
		if jailType == "vnet" {
			notes = append(notes, "VNET jails require an explicit IPv4 address. inherit is not allowed.")
		}
		return wizardFieldGuide{
			Purpose: "Primary IPv4 address assigned to the jail.",
			Format:  "CIDR address, or inherit for non-VNET jails.",
			Examples: []string{
				"192.168.1.20/24",
				"inherit",
			},
			Notes: notes,
		}
	case "ip6":
		notes := []string{}
		if jailType == "vnet" {
			notes = append(notes, "VNET jails require an explicit IPv6 address when IPv6 is used. inherit is not allowed.")
		}
		return wizardFieldGuide{
			Purpose: "Optional IPv6 address assigned to the jail.",
			Format:  "CIDR address, or inherit for non-VNET jails.",
			Examples: []string{
				"2001:db8::20/64",
				"inherit",
			},
			Notes: notes,
		}
	case "default_router":
		return wizardFieldGuide{
			Purpose: "Optional default gateway configured for the jail.",
			Format:  "IPv4 or IPv6 address. Leave blank if routing is handled elsewhere.",
			Examples: []string{
				"192.168.1.1",
				"2001:db8::1",
			},
		}
	case "startup_order":
		return wizardFieldGuide{
			Purpose: "Optional position for this jail in rc.conf jail_list.",
			Format:  "Positive integer, or blank to append when jail_list is already managed.",
			Examples: []string{
				"1",
				"5",
			},
			Notes: []string{
				"Leaving this blank preserves the default FreeBSD behavior when jail_list is currently unset.",
			},
		}
	case "dependencies":
		return wizardFieldGuide{
			Purpose: "Optional jail names used for depend ordering in jail.conf.",
			Format:  "Space- or comma-separated jail names.",
			Examples: []string{
				"db01 cache01",
				"router01, storage01",
			},
			Notes: []string{
				"Dependencies refine startup order beyond plain jail_list position.",
			},
		}
	case "cpu_percent":
		return wizardFieldGuide{
			Purpose: "Optional rctl CPU cap for the jail.",
			Format:  "Integer from 1 to 100.",
			Examples: []string{
				"25",
				"50",
			},
		}
	case "memory_limit":
		return wizardFieldGuide{
			Purpose: "Optional rctl memory limit.",
			Format:  "Integer with optional size suffix K, M, G, T, or P.",
			Examples: []string{
				"512M",
				"2G",
			},
		}
	case "process_limit":
		return wizardFieldGuide{
			Purpose: "Optional rctl maximum process count.",
			Format:  "Positive integer.",
			Examples: []string{
				"256",
				"1024",
			},
		}
	case "mount_points":
		return wizardFieldGuide{
			Purpose: "Optional extra mount targets created through mount.fstab.",
			Format:  "Comma-separated list of target-only entries or source:target pairs.",
			Examples: []string{
				"/var/cache/pkg",
				"/data:/srv/data, /logs:/var/log/app",
			},
			Notes: []string{
				"Mount targets must stay inside the jail root after normalization.",
			},
		}
	case "linux_distro":
		return wizardFieldGuide{
			Purpose: "Bootstrap family passed to debootstrap and used for the compat root name.",
			Format:  "Free-form family name. Ubuntu and Debian have built-in default mirrors; other families require a custom mirror.",
			Examples: []string{
				"ubuntu",
				"debian",
				"devuan",
			},
		}
	case "linux_release":
		return wizardFieldGuide{
			Purpose: "Free-form codename, suite, or release string passed directly to debootstrap.",
			Examples: []string{
				"noble",
				"bookworm",
				"trixie",
			},
		}
	case "linux_bootstrap":
		return wizardFieldGuide{
			Purpose: "Choose whether Linux userland should be bootstrapped immediately after jail creation.",
			Format:  "auto or skip.",
			Examples: []string{
				"auto to run networking preflight and debootstrap now",
				"skip to create the jail first and retry later from detail view",
			},
		}
	case "linux_mirror_mode":
		return wizardFieldGuide{
			Purpose: "Choose whether bootstrap uses the built-in distro mirror or a custom base URL.",
			Format:  "default or custom. default only works for bootstrap families ubuntu and debian.",
			Examples: []string{
				"default",
				"custom",
			},
		}
	case "linux_mirror_url":
		return wizardFieldGuide{
			Purpose: "Base repository URL used for Linux bootstrap, readiness checks, and retry.",
			Format:  "http or https base URL with a host.",
			Examples: []string{
				"https://archive.ubuntu.com/ubuntu",
				"https://deb.debian.org/debian",
			},
			Notes: []string{
				"Enter the repository base URL, not a full Release file URL.",
			},
		}
	default:
		return wizardFieldGuide{Purpose: field.Help}
	}
}

func wizardFieldUsesNetworkContext(id string) bool {
	switch id {
	case "interface", "bridge", "bridge_policy", "uplink", "ip4", "ip6", "default_router":
		return true
	default:
		return false
	}
}

func wizardFieldUsesLinuxContext(id string) bool {
	switch id {
	case "linux_distro", "linux_release", "linux_bootstrap", "linux_mirror_mode", "linux_mirror_url":
		return true
	default:
		return false
	}
}

func wizardsShowsLinuxPrereqs(step wizardStep) bool {
	for _, field := range step.Fields {
		switch field.ID {
		case "linux_distro", "linux_release", "linux_bootstrap", "linux_mirror_mode", "linux_mirror_url":
			return true
		}
	}
	return false
}

func wizardShowsNetworkPrereqs(step wizardStep) bool {
	for _, field := range step.Fields {
		switch field.ID {
		case "interface", "bridge", "uplink", "ip4", "ip6", "default_router":
			return true
		}
	}
	return false
}

func shouldShowNetworkPrereqs(prereqs NetworkWizardPrereqs) bool {
	if strings.TrimSpace(prereqs.InspectError) != "" || len(prereqs.Errors) > 0 || len(prereqs.Warnings) > 0 {
		return true
	}
	if strings.TrimSpace(prereqs.RouterStatus) != "" && !strings.EqualFold(strings.TrimSpace(prereqs.RouterStatus), "ok") {
		return true
	}
	if prereqs.BridgeCreateNeeded || prereqs.UplinkAttachNeeded {
		return true
	}
	return false
}

func appendStyledWizardLine(lines *[]string, text string, width int) {
	if looksLikeWarningText(text) {
		*lines = append(*lines, wizardErrorStyle.Render(truncate(text, width)))
		return
	}
	*lines = append(*lines, truncate(text, width))
}

func appendWrappedStyledWizardLine(lines *[]string, text string, width int) {
	for _, line := range wrapText(text, max(8, width)) {
		if looksLikeWarningText(text) {
			*lines = append(*lines, wizardErrorStyle.Render(line))
			continue
		}
		*lines = append(*lines, line)
	}
}

func linuxWizardPrereqLines(prereqs LinuxWizardPrereqs) []string {
	lines := []string{
		"Host Linux ABI will be enabled with: sysrc linux_enable=YES",
		"Host Linux service will be started with: service linux start",
		fmt.Sprintf("Host linux_enable configured: %s (%s)", yesNoText(prereqs.Host.EnableConfigured), valueOrDash(prereqs.Host.EnableValue)),
		fmt.Sprintf("Linux service present: %s", yesNoText(prereqs.Host.ServicePresent)),
		fmt.Sprintf("Linux service running: %s", yesNoText(prereqs.Host.ServiceRunning)),
		fmt.Sprintf("Effective mirror URL: %s", valueOrDash(prereqs.MirrorURL)),
		fmt.Sprintf("Bootstrap mirror host: %s", valueOrDash(prereqs.MirrorHost)),
		fmt.Sprintf("Bootstrap preflight URL: %s", valueOrDash(prereqs.PreflightURL)),
		"Auto bootstrap requires a running jail plus working route, DNS, and fetch access inside the jail.",
		"Skip mode creates the jail without bootstrapping; use b in jail detail to retry later.",
	}
	if prereqs.ResolveError != "" {
		lines = append(lines, "Mirror resolution: "+prereqs.ResolveError)
	}
	if prereqs.Host.EnableReadError != "" {
		lines = append(lines, "Host linux_enable check: "+prereqs.Host.EnableReadError)
	}
	for _, reason := range prereqs.Host.EnableDrift {
		lines = append(lines, "Warning: linux_enable drift: "+reason)
	}
	if prereqs.Host.ServicePresent && prereqs.Host.ServiceStatusErr != "" && !prereqs.Host.ServiceRunning {
		lines = append(lines, "Linux service status: "+prereqs.Host.ServiceStatusErr)
	}
	return lines
}

func linuxWizardContextLines(prereqs LinuxWizardPrereqs) []string {
	lines := []string{
		fmt.Sprintf("Host linux_enable configured: %s (%s)", yesNoText(prereqs.Host.EnableConfigured), valueOrDash(prereqs.Host.EnableValue)),
		fmt.Sprintf("Linux service running: %s", yesNoText(prereqs.Host.ServiceRunning)),
		fmt.Sprintf("Effective mirror URL: %s", valueOrDash(prereqs.MirrorURL)),
		fmt.Sprintf("Bootstrap preflight URL: %s", valueOrDash(prereqs.PreflightURL)),
		"Auto bootstrap requires route, DNS, and fetch access inside the running jail.",
		"Skip mode creates the jail first; use b in jail detail to retry bootstrap later.",
	}
	if prereqs.ResolveError != "" {
		lines = append(lines, "Mirror resolution: "+prereqs.ResolveError)
	}
	if prereqs.Host.EnableReadError != "" {
		lines = append(lines, "Host linux_enable check: "+prereqs.Host.EnableReadError)
	}
	for _, reason := range prereqs.Host.EnableDrift {
		lines = append(lines, "Warning: linux_enable drift: "+reason)
	}
	if prereqs.Host.ServicePresent && prereqs.Host.ServiceStatusErr != "" && !prereqs.Host.ServiceRunning {
		lines = append(lines, "Linux service status: "+prereqs.Host.ServiceStatusErr)
	}
	return lines
}

func (m model) detailLines(width int) []string {
	lines := make([]string, 0, 64)

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

	appendRenderedSection(&lines, "Overview", renderKeyValueLines(width,
		[2]string{"Name", m.detail.Name},
		[2]string{"State", state},
		[2]string{"JID", jidText},
		[2]string{"CPU", cpuText},
		[2]string{"Memory", memText},
		[2]string{"Path", m.detail.Path},
		[2]string{"Hostname", m.detail.Hostname},
	))

	appendSection(&lines, width, "Configured state")
	lines = append(lines, renderKeyValueLines(width, [2]string{"Source", m.detail.JailConfSource})...)
	if len(m.detail.JailConfValues) == 0 && len(m.detail.JailConfFlags) == 0 {
		lines = append(lines, truncate("No matching jail block found.", width))
	} else {
		for _, key := range sortedKeys(m.detail.JailConfValues) {
			lines = append(lines, renderKeyValueLines(width, [2]string{key, m.detail.JailConfValues[key]})...)
		}
		for _, flag := range m.detail.JailConfFlags {
			lines = append(lines, renderKeyValueLines(width, [2]string{flag, "enabled"})...)
		}
	}

	appendSection(&lines, width, "Startup policy")
	if m.detail.StartupConfig == nil {
		lines = append(lines, truncate("Startup policy unavailable.", width))
	} else {
		if m.detail.StartupConfig.InJailList {
			lines = append(lines, renderKeyValueLines(width,
				[2]string{"jail_list position", fmt.Sprintf("%d of %d", m.detail.StartupConfig.Position, len(m.detail.StartupConfig.JailList))},
			)...)
		} else if len(m.detail.StartupConfig.JailList) == 0 {
			lines = append(lines, renderKeyValueLines(width, [2]string{"jail_list", "empty"})...)
		} else {
			lines = append(lines, renderKeyValueLines(width, [2]string{"jail_list", "not present (manual start required when jail_list is used)"})...)
		}
		lines = append(lines, renderKeyValueLines(width, [2]string{"Dependencies", dependencySummary(strings.Join(m.detail.StartupConfig.Dependencies, " "))})...)
		if len(m.detail.StartupConfig.JailList) > 0 {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Effective jail_list", strings.Join(m.detail.StartupConfig.JailList, " ")})...)
		}
		if m.detail.StartupConfig.ReadError != "" {
			lines = append(lines, wizardErrorStyle.Render(truncate("jail_list read error: "+m.detail.StartupConfig.ReadError, max(1, width))))
		}
	}

	appendRenderedSection(&lines, "Runtime state", renderKeyValueLines(width,
		[2]string{"State", state},
		[2]string{"CPU", cpuText},
		[2]string{"Memory", memText},
	))
	runtimeNotes := make([]string, 0, 2)
	if len(m.detail.RuntimeValues) == 0 {
		runtimeNotes = append(runtimeNotes, "No running runtime record for this jail.")
	} else {
		for _, key := range orderedRuntimeKeys(m.detail.RuntimeValues) {
			lines = append(lines, renderKeyValueLines(width, [2]string{key, m.detail.RuntimeValues[key]})...)
		}
	}
	if !m.detailShowAdvanced {
		runtimeNotes = append(runtimeNotes, "Raw runtime/default parameters hidden; press a to show.")
	}

	if m.detail.NetworkSummary != nil {
		appendSection(&lines, width, "Network summary")
		networkLabelWidth := 25
		if width < 72 {
			networkLabelWidth = 20
		}
		networkPairs := make([][2]string, 0, len(m.detail.NetworkSummary.Configured)+len(m.detail.NetworkSummary.Runtime))
		for _, key := range orderedNetworkSummaryKeys(m.detail.NetworkSummary.Configured) {
			networkPairs = append(networkPairs, [2]string{"Configured " + key, m.detail.NetworkSummary.Configured[key]})
		}
		for _, key := range orderedNetworkSummaryKeys(m.detail.NetworkSummary.Runtime) {
			networkPairs = append(networkPairs, [2]string{"Runtime " + key, m.detail.NetworkSummary.Runtime[key]})
		}
		if len(networkPairs) > 0 {
			lines = append(lines, renderKeyValueLinesWithLabelWidth(width, networkLabelWidth, networkPairs...)...)
		}
		if len(m.detail.NetworkSummary.Validation) > 0 {
			for _, line := range m.detail.NetworkSummary.Validation {
				if looksLikeWarningText(line) {
					lines = append(lines, wizardErrorStyle.Render(truncate(line, max(1, width))))
					continue
				}
				lines = append(lines, renderInformationalKeyValueWithLabelWidth(width, networkLabelWidth, line)...)
			}
		}
	}

	appendSection(&lines, width, "ZFS dataset")
	if m.detail.ZFS == nil {
		lines = append(lines, truncate("No dataset matched the jail path.", width))
	} else {
		lines = append(lines, renderKeyValueLines(width,
			[2]string{"Dataset", m.detail.ZFS.Name},
			[2]string{"Mountpoint", m.detail.ZFS.Mountpoint},
			[2]string{"Match", m.detail.ZFS.MatchType},
			[2]string{"Used", m.detail.ZFS.Used},
			[2]string{"Avail", m.detail.ZFS.Avail},
			[2]string{"Refer", m.detail.ZFS.Refer},
			[2]string{"Compression", m.detail.ZFS.Compression},
			[2]string{"Quota", m.detail.ZFS.Quota},
			[2]string{"Reservation", m.detail.ZFS.Reservation},
		)...)
	}

	appendSection(&lines, width, "rctl")
	if len(m.detail.RctlRules) == 0 {
		lines = append(lines, truncate("No matching rctl rules.", width))
	} else {
		for _, rule := range m.detail.RctlRules {
			lines = append(lines, truncate(rule, width))
		}
	}

	if m.detail.LinuxReadiness != nil {
		appendSection(&lines, width, "Linux readiness")
		for _, line := range m.linuxReadinessLines() {
			if looksLikeWarningText(line) || strings.HasPrefix(strings.ToLower(line), "readiness issue:") {
				lines = append(lines, wizardErrorStyle.Render(truncate(line, max(1, width))))
				continue
			}
			lines = append(lines, truncate(line, width))
		}
	}

	if len(m.detail.SourceErrors) > 0 {
		appendSection(&lines, width, "Source errors")
		for _, source := range sortedKeys(m.detail.SourceErrors) {
			lines = append(lines, wizardErrorStyle.Render(truncate(fmt.Sprintf("%s: %s", source, m.detail.SourceErrors[source]), width)))
		}
	}
	if len(runtimeNotes) > 0 {
		appendSection(&lines, width, "Runtime notes")
		for _, line := range runtimeNotes {
			lines = append(lines, truncate(line, width))
		}
	}
	if m.detailShowAdvanced {
		appendSection(&lines, width, "Advanced runtime parameters")
		if len(m.detail.AdvancedRuntimeFields) == 0 {
			lines = append(lines, truncate("No additional runtime/default parameters.", width))
		} else {
			for _, key := range sortedKeys(m.detail.AdvancedRuntimeFields) {
				lines = append(lines, renderKeyValueLines(width, [2]string{key, m.detail.AdvancedRuntimeFields[key]})...)
			}
		}
	}
	return lines
}

func orderedRuntimeKeys(values map[string]string) []string {
	order := []string{
		"JID",
		"Live path",
		"Live hostname",
		"Network mode",
		"Interface",
		"IPv4",
		"IPv6",
	}
	keys := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, key := range order {
		if _, ok := values[key]; ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}
	for _, key := range sortedKeys(values) {
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func (m model) linuxReadinessLines() []string {
	if m.detail.LinuxReadiness == nil {
		return nil
	}
	readiness := m.detail.LinuxReadiness
	lines := []string{
		fmt.Sprintf("Host linux_enable: %s", valueOrDash(readiness.Host.EnableValue)),
		fmt.Sprintf("Host ABI configured: %s", yesNoText(readiness.Host.EnableConfigured)),
		fmt.Sprintf("Linux service present: %s", yesNoText(readiness.Host.ServicePresent)),
		fmt.Sprintf("Linux service running: %s", yesNoText(readiness.Host.ServiceRunning)),
		fmt.Sprintf("Bootstrap family: %s", valueOrDash(readiness.BootstrapFamily)),
		fmt.Sprintf("Compat root: %s", valueOrDash(readiness.CompatRoot)),
		fmt.Sprintf("Bootstrap mode: %s", valueOrDash(readiness.BootstrapMode)),
		fmt.Sprintf("Mirror URL: %s", valueOrDash(readiness.MirrorURL)),
		fmt.Sprintf("Mirror host: %s", valueOrDash(readiness.MirrorHost)),
		fmt.Sprintf("Preflight URL: %s", valueOrDash(readiness.PreflightURL)),
		fmt.Sprintf("Linux userland present: %s", yesNoText(readiness.UserlandPresent)),
	}
	if readiness.MirrorResolveError != "" {
		lines = append(lines, "Warning: mirror resolution failed: "+readiness.MirrorResolveError)
	}
	if readiness.Host.EnableReadError != "" {
		lines = append(lines, "Warning: host ABI check failed: "+readiness.Host.EnableReadError)
	}
	for _, reason := range readiness.Host.EnableDrift {
		lines = append(lines, "Warning: linux_enable drift: "+reason)
	}
	if readiness.Host.ServicePresent && readiness.Host.ServiceStatusErr != "" && !readiness.Host.ServiceRunning {
		lines = append(lines, "Warning: linux service status check failed: "+readiness.Host.ServiceStatusErr)
	}
	if readiness.RuntimeChecked {
		lines = append(lines,
			fmt.Sprintf("IPv4 route: %s", yesNoText(readiness.IPv4Route)),
			fmt.Sprintf("IPv6 route: %s", yesNoText(readiness.IPv6Route)),
			fmt.Sprintf("IPv4 DNS: %s", yesNoText(readiness.IPv4DNS)),
			fmt.Sprintf("IPv6 DNS: %s", yesNoText(readiness.IPv6DNS)),
			fmt.Sprintf("IPv4 fetch: %s", yesNoText(readiness.IPv4Fetch)),
			fmt.Sprintf("IPv6 fetch: %s", yesNoText(readiness.IPv6Fetch)),
		)
	} else {
		lines = append(lines, "Route/DNS/fetch checks run when the jail is running.")
	}
	if readiness.RuntimeError != "" {
		lines = append(lines, "Warning: "+readiness.RuntimeError)
	}
	if readiness.HealthChecked {
		lines = append(lines,
			fmt.Sprintf("Package manager works: %s", yesNoText(readiness.PackageManagerOK)),
			"Package manager status: "+valueOrDash(readiness.PackageManagerStatus),
			fmt.Sprintf("DNS works: %s", yesNoText(readiness.DNSWorks)),
			"DNS status: "+valueOrDash(readiness.DNSStatus),
			fmt.Sprintf("Init present: %s", yesNoText(readiness.InitPresent)),
			"Init status: "+valueOrDash(readiness.InitStatus),
			"Services status: "+valueOrDash(readiness.ServiceStatus),
		)
	} else if readiness.UserlandPresent {
		lines = append(lines, "Post-bootstrap health checks run when the jail is running.")
	}
	if !readiness.UserlandPresent {
		lines = append(lines, "Use b to retry Linux bootstrap after prerequisites are ready.")
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

func (m model) wizardIsFieldEntryMode() bool {
	return m.mode == screenCreateWizard &&
		!m.wizardApplying &&
		!m.wizard.datasetCreateRunning &&
		!m.wizard.isConfirmationStep() &&
		m.wizard.templateMode == wizardTemplateModeNone &&
		!m.wizard.userlandMode &&
		!m.wizard.thinDatasetMode
}

func (m *model) boundWizardScroll() {
	if m.mode != screenCreateWizard {
		m.wizardScroll = 0
		return
	}
	if m.width <= 0 || m.height <= 0 {
		m.wizardScroll = 0
		return
	}

	var lines []string
	if leftWidth, _, ok := m.wizardSplitPaneWidths(); ok {
		lines, _ = m.wizardFieldEntryLayout(max(12, leftWidth-2), false)
	} else {
		lines = m.wizardLines(max(12, m.width-2))
	}

	maxOffset := max(0, len(lines)-m.wizardBodyHeight())
	if m.wizardScroll < 0 {
		m.wizardScroll = 0
	}
	if m.wizardScroll > maxOffset {
		m.wizardScroll = maxOffset
	}
}

func (m *model) ensureWizardFieldVisible() {
	if !m.wizardIsFieldEntryMode() || m.width <= 0 || m.height <= 0 {
		return
	}

	bodyHeight := m.wizardBodyHeight()
	if bodyHeight <= 0 {
		return
	}

	activeLine := -1
	if leftWidth, _, ok := m.wizardSplitPaneWidths(); ok {
		_, activeLine = m.wizardFieldEntryLayout(max(12, leftWidth-2), false)
	} else {
		_, activeLine = m.wizardFieldEntryLayout(max(12, m.width-2), true)
		prefixLines := 2
		if m.wizard.currentStep().Description != "" {
			prefixLines = 3
		}
		if activeLine >= 0 {
			activeLine += prefixLines
		}
	}
	if activeLine < 0 {
		m.boundWizardScroll()
		return
	}

	if activeLine < m.wizardScroll {
		m.wizardScroll = activeLine
	} else if activeLine >= m.wizardScroll+bodyHeight {
		m.wizardScroll = activeLine - bodyHeight + 1
	}
	m.boundWizardScroll()
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

func newTemplateDatasetCreateState(sourceInput string, status initialConfigStatus, returnMode screenMode, selectMode bool) templateDatasetCreateState {
	state := templateDatasetCreateState{
		returnMode:  returnMode,
		selectMode:  selectMode,
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

func (s *templateDatasetCreateState) refreshRenamePreview() {
	item, ok := s.selectedItem()
	if !ok {
		s.renamePreview = TemplateDatasetRenamePreview{}
		return
	}
	s.renamePreview = InspectTemplateDatasetRename(item.Name, s.renameInput, s.parentOverride())
}

func (s *templateDatasetCreateState) refreshClonePreview() {
	item, ok := s.selectedItem()
	if !ok {
		s.clonePreview = TemplateDatasetSnapshotClonePreview{}
		return
	}
	s.boundCloneCursor()
	snapshot, ok := s.selectedCloneSnapshot()
	if !ok {
		s.clonePreview = TemplateDatasetSnapshotClonePreview{
			Current: item,
			NewName: strings.TrimSpace(s.cloneName),
			Err:     fmt.Errorf("select a snapshot to clone"),
		}
		return
	}
	s.clonePreview = InspectTemplateSnapshotClone(item.Name, snapshot.Name, s.cloneName, s.parentOverride())
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

func (s *templateDatasetCreateState) appendRenameInput(text string) {
	if text == "" {
		return
	}
	s.renameInput += text
	s.logs = nil
	s.message = ""
	s.refreshRenamePreview()
}

func (s *templateDatasetCreateState) backspaceRenameInput() {
	if s.renameInput == "" {
		return
	}
	s.renameInput = s.renameInput[:len(s.renameInput)-1]
	s.logs = nil
	s.message = ""
	s.refreshRenamePreview()
}

func (s *templateDatasetCreateState) appendCloneName(text string) {
	if text == "" {
		return
	}
	s.cloneName += text
	s.logs = nil
	s.message = ""
	s.refreshClonePreview()
}

func (s *templateDatasetCreateState) backspaceCloneName() {
	if s.cloneName == "" {
		return
	}
	s.cloneName = s.cloneName[:len(s.cloneName)-1]
	s.logs = nil
	s.message = ""
	s.refreshClonePreview()
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

func (s *templateDatasetCreateState) selectedItem() (TemplateDatasetInfo, bool) {
	if len(s.items) == 0 {
		return TemplateDatasetInfo{}, false
	}
	if s.cursor < 0 {
		s.cursor = 0
	}
	if s.cursor >= len(s.items) {
		s.cursor = len(s.items) - 1
	}
	return s.items[s.cursor], true
}

func (s *templateDatasetCreateState) boundCursor() {
	if len(s.items) == 0 {
		s.cursor = 0
		return
	}
	if s.cursor < 0 {
		s.cursor = 0
	}
	if s.cursor >= len(s.items) {
		s.cursor = len(s.items) - 1
	}
}

func (s *templateDatasetCreateState) selectedCloneSnapshot() (ZFSSnapshot, bool) {
	if len(s.cloneSnapshots) == 0 {
		return ZFSSnapshot{}, false
	}
	s.boundCloneCursor()
	return s.cloneSnapshots[s.cloneCursor], true
}

func (s *templateDatasetCreateState) boundCloneCursor() {
	if len(s.cloneSnapshots) == 0 {
		s.cloneCursor = 0
		return
	}
	if s.cloneCursor < 0 {
		s.cloneCursor = 0
	}
	if s.cloneCursor >= len(s.cloneSnapshots) {
		s.cloneCursor = len(s.cloneSnapshots) - 1
	}
}

func (s *templateDatasetCreateState) syncSelection() {
	s.boundCursor()
	if s.mode == templateManagerModeRename {
		s.refreshRenamePreview()
		return
	}
	if s.mode == templateManagerModeDestroy {
		if item, ok := s.selectedItem(); ok {
			s.destroyPreview = InspectTemplateDatasetDestroy(item.Name, s.parentOverride())
		}
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
	hint := "j/k or up/down: scroll | g/G: top/bottom | enter/d: details | c: create wizard | i: initial config | t: template manager | s: start/stop | z: ZFS | x: destroy | h: help | r: refresh | q: quit"
	footerRenderer := footerStyle
	message := m.notice
	if m.err != nil {
		message = "warning: " + m.err.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	}
	return m.renderFooterWithMessage(hint, message, footerRenderer)
}

func (m model) renderFooterWithMessage(hint, message string, footerRenderer lipgloss.Style) string {
	lines := make([]string, 0, 4)
	width := max(1, m.width)

	message = strings.TrimSpace(message)
	if message != "" {
		prefixed := ">> " + message
		for _, line := range wrapText(prefixed, max(8, width-2)) {
			lines = append(lines, styleWizardMessage(line))
		}
	}

	lines = append(lines, footerRenderer.Width(width).Render(hint))
	return strings.Join(lines, "\n")
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
		return "No jails discovered yet. Create one manually in jail.conf/jail.conf.d, press c to open the jail creation wizard, or press t to open the template manager."
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
		lines = append(lines, renderKeyValueLines(max(12, width-2),
			[2]string{"Name", j.Name},
			[2]string{"State", state},
			[2]string{"JID", jidText},
			[2]string{"CPU", fmt.Sprintf("%.2f%%", j.CPUPercent)},
			[2]string{"Memory", fmt.Sprintf("%dMB", j.MemoryMB)},
		)...)
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
	if strings.Contains(lower, "applying") ||
		strings.Contains(lower, "creating") ||
		strings.Contains(lower, "refreshing") ||
		strings.Contains(lower, "retrying") ||
		strings.Contains(lower, "rolling back") ||
		strings.Contains(lower, "loading detail") {
		return wizardActionStyle.Render(message)
	}
	if looksLikeWarningText(message) {
		return wizardErrorStyle.Render(message)
	}
	return summaryStyle.Render(message)
}

func looksLikeWarningText(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "warning") ||
		strings.Contains(lower, "failed") ||
		strings.Contains(lower, "required") ||
		strings.Contains(lower, "invalid") ||
		strings.Contains(lower, "must") ||
		strings.Contains(lower, "already exists") ||
		strings.Contains(lower, "error") ||
		strings.Contains(lower, "unable") ||
		strings.Contains(lower, "refusing") ||
		strings.Contains(lower, "blocked") ||
		strings.Contains(lower, "cannot")
}

func appendSection(lines *[]string, width int, title string, body ...string) {
	if len(*lines) > 0 && (*lines)[len(*lines)-1] != "" {
		*lines = append(*lines, "")
	}
	*lines = append(*lines, sectionStyle.Render(title))
	for _, line := range body {
		*lines = append(*lines, truncate(line, width))
	}
}

func appendRenderedSection(lines *[]string, title string, body []string) {
	if len(*lines) > 0 && (*lines)[len(*lines)-1] != "" {
		*lines = append(*lines, "")
	}
	*lines = append(*lines, sectionStyle.Render(title))
	*lines = append(*lines, body...)
}

func renderKeyValueLines(width int, pairs ...[2]string) []string {
	labelWidth := 25
	if width < 72 {
		labelWidth = 20
	}
	return renderKeyValueLinesWithLabelWidth(width, labelWidth, pairs...)
}

func renderKeyValueLinesWithLabelWidth(width, labelWidth int, pairs ...[2]string) []string {
	lines := make([]string, 0, len(pairs)*2)
	for _, pair := range pairs {
		lines = append(lines, renderKeyValue(width, labelWidth, pair[0], pair[1])...)
	}
	return lines
}

func renderKeyValue(width, labelWidth int, label, value string) []string {
	label = strings.TrimSpace(label)
	if label == "" {
		return []string{truncate(valueOrDash(value), width)}
	}
	if labelWidth < 8 {
		labelWidth = 8
	}
	valueWidth := max(8, width-labelWidth-2)
	wrapped := wrapText(valueOrDash(value), valueWidth)
	prefix := fmt.Sprintf("%-*s", labelWidth, label+":")
	lines := make([]string, 0, len(wrapped))
	if len(wrapped) == 0 {
		return []string{detailKeyStyle.Render(prefix) + " -"}
	}
	lines = append(lines, detailKeyStyle.Render(prefix)+" "+wrapped[0])
	continuation := strings.Repeat(" ", labelWidth+1)
	for _, part := range wrapped[1:] {
		lines = append(lines, continuation+" "+part)
	}
	return lines
}

func renderInformationalKeyValue(width int, line string) []string {
	labelWidth := 25
	if width < 72 {
		labelWidth = 20
	}
	return renderInformationalKeyValueWithLabelWidth(width, labelWidth, line)
}

func renderInformationalKeyValueWithLabelWidth(width, labelWidth int, line string) []string {
	left, right, ok := strings.Cut(strings.TrimSpace(line), ":")
	if !ok {
		return []string{truncate(line, width)}
	}
	label := strings.TrimSpace(left)
	value := strings.TrimSpace(right)
	if label == "" || value == "" {
		return []string{truncate(line, width)}
	}
	return renderKeyValueLinesWithLabelWidth(width, labelWidth, [2]string{label, value})
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

func wrapText(input string, maxLen int) []string {
	if maxLen <= 0 {
		return nil
	}
	input = strings.TrimSpace(input)
	if input == "" {
		return []string{""}
	}

	words := strings.Fields(input)
	if len(words) == 0 {
		return []string{""}
	}

	lines := make([]string, 0, len(words))
	current := ""
	appendChunked := func(word string) {
		for len(word) > maxLen {
			lines = append(lines, word[:maxLen])
			word = word[maxLen:]
		}
		current = word
	}

	for _, word := range words {
		if current == "" {
			if len(word) <= maxLen {
				current = word
			} else {
				appendChunked(word)
			}
			continue
		}

		candidate := current + " " + word
		if len(candidate) <= maxLen {
			current = candidate
			continue
		}

		lines = append(lines, current)
		current = ""
		if len(word) <= maxLen {
			current = word
		} else {
			appendChunked(word)
		}
	}

	if current != "" {
		lines = append(lines, current)
	}

	return lines
}

func valueOrPlaceholder(value, placeholder string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return placeholder
	}
	return value
}

func yesNoText(value bool) string {
	if value {
		return "yes"
	}
	return "no"
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
