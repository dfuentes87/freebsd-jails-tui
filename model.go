package main

import (
	"context"
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
	headerBarStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("235"))
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("39")).
			Background(lipgloss.Color("235")).
			Padding(0, 1)
	summaryStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("248")).
			Background(lipgloss.Color("235"))
	panelTitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("45")).
			Underline(true)
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
			Background(lipgloss.Color("235")).
			Padding(0, 1)
	helpTabStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("248")).
			Background(lipgloss.Color("236")).
			Padding(0, 1)
	helpTabActiveStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("230")).
				Background(lipgloss.Color("24")).
				Padding(0, 1)
	sectionStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("33"))
	detailSectionStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("33"))
	helpNoteStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("244"))
	wizardActionStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("230")).
				Background(lipgloss.Color("31")).
				Padding(0, 1)
	wizardErrorStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("196"))
	wizardWarningStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("226"))
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

type bulkServiceApplyMsg struct {
	results []jailServiceResult
	action  string
}

type linuxBootstrapApplyMsg struct {
	result linuxBootstrapResult
}

type jailNoteApplyMsg struct {
	result JailNoteUpdateResult
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

type templateSnapshotDestroyApplyMsg struct {
	result TemplateDatasetSnapshotDestroyResult
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

type downloadProgressMsg struct {
	Read  int64
	Total int64
	Done  bool
}

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
	screenUpgradeWizard
)

type helpTab int

const (
	helpTabOverview helpTab = iota
	helpTabDashboard
	helpTabCreate
	helpTabZFS
	helpTabUpgrade
	helpTabSetup
	helpTabCount
)

type helpShortcut struct {
	Keys   string
	Action string
}

type helpSection struct {
	Title     string
	Summary   string
	Shortcuts []helpShortcut
	Notes     []string
}

type helpTabContent struct {
	Label    string
	Sections []helpSection
}

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
	templateManagerModeSnapshotDestroy
)

type templateDatasetCreateState struct {
	returnMode             screenMode
	selectMode             bool
	mode                   templateManagerMode
	items                  []TemplateDatasetInfo
	cursor                 int
	parent                 *templateDatasetParent
	loading                bool
	sourceInput            string
	preview                TemplateDatasetPreview
	renameInput            string
	renamePreview          TemplateDatasetRenamePreview
	destroyPreview         TemplateDatasetDestroyPreview
	cloneSnapshots         []ZFSSnapshot
	cloneCursor            int
	cloneName              string
	clonePreview           TemplateDatasetSnapshotClonePreview
	snapshotDestroyPreview TemplateDatasetSnapshotDestroyPreview
	cloneLoading           bool
	applying               bool
	parentApplying         bool
	logs                   []string
	message                string
	parentDataset          string
	parentMountpoint       string
	parentEdit             bool
	parentField            int
	parentCustom           bool
	patchBase              string
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
	detailNoteMode     bool
	detailNoteSaving   bool
	detailNoteInput    string
	detailNotice       string
	wizardScroll       int
	zfsPanel           zfsPanelState
	wizard             jailCreationWizard
	wizardApplying     bool
	wizardCancel       context.CancelFunc
	downloading        bool
	downloadRead       int64
	downloadTotal      int64
	progressChan       chan downloadProgressMsg
	templateCreate     templateDatasetCreateState
	destroy            destroyState
	initCheck          initialCheckState
	helpReturnMode     screenMode
	helpTab            helpTab
	helpScroll         int
	helpTabScrolls     []int
	notice             string
	selectedJails      map[string]struct{}
	upgrade            upgradeState
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
	m.helpTabScrolls = make([]int, int(helpTabCount))
	m.selectedJails = make(map[string]struct{})
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

func jailNoteUpdateCmd(detail JailDetail, note string) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteJailNoteUpdate(detail, note)
		return jailNoteApplyMsg{result: result}
	}
}

func createJailCmd(ctx context.Context, values jailWizardValues, progressChan chan<- downloadProgressMsg) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteJailCreation(ctx, values, progressChan)
		close(progressChan)
		return wizardApplyMsg{result: result}
	}
}

func waitForProgress(c chan downloadProgressMsg) tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-c
		if !ok {
			return downloadProgressMsg{Done: true}
		}
		return msg
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

func templateDatasetCreateCmd(sourceInput string, parentOverride *templateDatasetParent, patchPreference string) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteTemplateDatasetCreateWithParent(context.Background(), sourceInput, parentOverride, patchPreference)
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

func templateSnapshotDestroyCmd(dataset, snapshot string, parentOverride *templateDatasetParent) tea.Cmd {
	return func() tea.Msg {
		result := ExecuteTemplateSnapshotDestroy(dataset, snapshot, parentOverride)
		return templateSnapshotDestroyApplyMsg{result: result}
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
		result := ExecuteTemplateParentDatasetCreate(context.Background(), dataset, mountpoint)
		return templateParentApplyMsg{result: result}
	}
}

func openZFSPanelCmd(target Jail) tea.Cmd {
	return func() tea.Msg {
		result := resolveZFSPanelTarget(target)
		return zfsOpenMsg{result: result}
	}
}

func bulkToggleServiceCmd(targets []Jail) tea.Cmd {
	return func() tea.Msg {
		results := make([]jailServiceResult, 0, len(targets))
		for _, t := range targets {
			action := "start"
			if t.Running {
				action = "stop"
			}
			results = append(results, ExecuteJailServiceAction(t, action))
		}
		return bulkServiceApplyMsg{results: results, action: "toggle"}
	}
}

func bulkServiceCmd(targets []Jail, action string) tea.Cmd {
	return func() tea.Msg {
		results := ExecuteBulkJailServiceAction(targets, action)
		return bulkServiceApplyMsg{results: results, action: action}
	}
}

func upgradeJailCmd(target Jail, workflow upgradeWorkflow) tea.Cmd {
	return func() tea.Msg {
		return ExecuteJailUpgrade(target, workflow)
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
		if strings.EqualFold(strings.TrimSpace(m.initCheck.message), "Refreshing checks...") {
			m.initCheck.message = ""
		}
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
		if !m.detailNoteMode {
			m.detailNoteInput = msg.detail.Note
		}
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
	case downloadProgressMsg:
		if msg.Done {
			m.downloading = false
			return m, nil
		}
		m.downloading = true
		m.downloadRead = msg.Read
		m.downloadTotal = msg.Total
		return m, waitForProgress(m.progressChan)
	case wizardApplyMsg:
		m.wizardApplying = false
		m.downloading = false
		m.wizard.setExecutionResult(msg.result)
		if msg.result.Err == nil {
			m.mode = screenDashboard
			if len(msg.result.Warnings) > 0 {
				m.notice = fmt.Sprintf("Jail %s created and started with warnings: %s", msg.result.Name, summarizeCreationWarning(msg.result.Warnings[0]))
			} else {
				m.notice = fmt.Sprintf("Jail %s created and started.", msg.result.Name)
			}
			m.wizard = newJailCreationWizard(initialWizardDestination(m.initCheck.status))
			return m, pollCmd()
		}
		m.wizardScroll = 1 << 30
		m.boundWizardScroll()
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
			if m.mode == screenJailDetail {
				m.detailErr = msg.result.Err
				m.detailNotice = ""
			}
			return m, pollCmd()
		}
		m.err = nil
		actionWord := "started"
		if msg.result.Action == "stop" {
			actionWord = "stopped"
		} else if msg.result.Action == "restart" {
			actionWord = "restarted"
		}
		m.notice = fmt.Sprintf("Jail %s %s.", msg.result.Name, actionWord)
		if m.mode == screenJailDetail {
			m.detailNotice = fmt.Sprintf("Jail %s.", actionWord)
			m.detailErr = nil
		}
		return m, pollCmd()
	case bulkServiceApplyMsg:
		var failed []string
		var succeeded []string
		for _, r := range msg.results {
			if r.Err != nil {
				failed = append(failed, r.Name+": "+r.Err.Error())
			} else {
				succeeded = append(succeeded, r.Name)
			}
		}
		m.selectedJails = make(map[string]struct{})
		if len(failed) > 0 {
			m.err = fmt.Errorf("bulk %s errors: %s", msg.action, strings.Join(failed, "; "))
			m.notice = ""
		} else {
			actionWord := msg.action
			if actionWord == "toggle" {
				actionWord = "start/stop applied"
			} else {
				actionWord += "ed"
			}
			m.err = nil
			m.notice = fmt.Sprintf("%d jail(s): %s.", len(succeeded), actionWord)
		}
		return m, pollCmd()
	case upgradeApplyMsg:
		m.upgrade.applying = false
		m.upgrade.logs = append([]string(nil), msg.logs...)
		m.upgrade.err = msg.err
		if msg.err != nil {
			m.upgrade.message = "Upgrade failed. Review the output above."
		} else {
			m.upgrade.message = fmt.Sprintf("Upgrade completed for %s.", msg.name)
		}
		return m, nil
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
	case jailNoteApplyMsg:
		m.detailNoteSaving = false
		if msg.result.Err != nil {
			m.detailErr = msg.result.Err
			m.detailNotice = ""
			m.detailNoteMode = true
			return m, nil
		}
		m.detailErr = nil
		m.detailNoteMode = false
		m.detail.Note = msg.result.Note
		if msg.result.Note == "" {
			m.detailNotice = "Jail note cleared."
		} else {
			m.detailNotice = "Jail note updated."
		}
		jail, ok := m.detailJail()
		if !ok {
			return m, pollCmd()
		}
		return m, tea.Batch(pollCmd(), detailCmd(jail))
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
		if m.templateCreate.mode == templateManagerModeSnapshotDestroy {
			m.templateCreate.refreshSnapshotDestroyPreview()
		} else {
			m.templateCreate.refreshClonePreview()
		}
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
	case templateSnapshotDestroyApplyMsg:
		if m.mode != screenTemplateDatasetCreate {
			return m, nil
		}
		m.templateCreate.applying = false
		m.templateCreate.logs = append([]string(nil), msg.result.Logs...)
		if msg.result.Err != nil {
			m.templateCreate.message = msg.result.Err.Error()
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeClone
		m.templateCreate.snapshotDestroyPreview = TemplateDatasetSnapshotDestroyPreview{}
		m.templateCreate.cloneLoading = true
		m.templateCreate.message = fmt.Sprintf("Template snapshot destroyed: %s", msg.result.Snapshot)
		return m, templateSnapshotListCmd(msg.result.Dataset)
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
			m.openHelp(m.mode)
			return m, nil
		}
		if m.mode == screenUpgradeWizard {
			return m.updateUpgradeWizardKeys(msg)
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
	case screenJailDetail:
		return m.detailNoteMode && !m.detailNoteSaving
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
	case "esc":
		if len(m.selectedJails) > 0 {
			m.selectedJails = make(map[string]struct{})
			m.notice = "Selection cleared."
			return m, nil
		}
	case "space", " ":
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		if _, selected := m.selectedJails[jail.Name]; selected {
			delete(m.selectedJails, jail.Name)
		} else {
			m.selectedJails[jail.Name] = struct{}{}
		}
	case "ctrl+a":
		allSelected := len(m.snapshot.Jails) > 0
		for _, j := range m.snapshot.Jails {
			if _, ok := m.selectedJails[j.Name]; !ok {
				allSelected = false
				break
			}
		}
		if allSelected {
			m.selectedJails = make(map[string]struct{})
		} else {
			for _, j := range m.snapshot.Jails {
				m.selectedJails[j.Name] = struct{}{}
			}
		}
	case "s", "S":
		if len(m.selectedJails) > 0 {
			return m, bulkToggleServiceCmd(m.selectedJailList())
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
	case "R":
		if len(m.selectedJails) > 0 {
			return m, bulkServiceCmd(m.selectedJailList(), "restart")
		}
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		return m, jailServiceCmd(jail, "restart")
	case "u", "U":
		jail, ok := m.selectedJail()
		if !ok {
			return m, nil
		}
		m.upgrade = newUpgradeState(jail, screenDashboard)
		m.mode = screenUpgradeWizard
		return m, nil
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
		m.detailNoteMode = false
		m.detailNoteSaving = false
		m.detailNoteInput = ""
		m.detailErr = nil
		m.detailNotice = ""
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
		return m, detailCmd(jail)
	}
	m.boundCursor()
	m.ensureCursorVisible(m.listHeight())
	return m, nil
}

func (m model) updateDetailKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.detailNoteMode && !m.detailNoteSaving && msg.Type == tea.KeyRunes {
		m.appendDetailNoteInput(string(msg.Runes))
		return m, nil
	}
	if m.detailNoteSaving {
		return m, nil
	}

	switch msg.String() {
	case "space", " ":
		if m.detailNoteMode {
			m.appendDetailNoteInput(" ")
			return m, nil
		}
	case "esc", "left":
		if m.detailNoteMode {
			m.detailNoteMode = false
			m.detailNoteInput = ""
			m.detailNotice = "Jail note edit canceled."
			m.detailErr = nil
			return m, nil
		}
		m.mode = screenDashboard
		return m, nil
	case "n", "N":
		if m.detailNoteSaving {
			return m, nil
		}
		if m.detailNoteMode {
			return m, nil
		}
		m.detailNoteMode = true
		m.detailNoteInput = m.detail.Note
		m.detailNotice = ""
		m.detailErr = nil
		return m, nil
	case "enter":
		if !m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
		note, err := normalizeJailNote(m.detailNoteInput)
		if err != nil {
			m.detailErr = err
			m.detailNotice = ""
			return m, nil
		}
		m.detailNoteInput = note
		m.detailNoteSaving = true
		m.detailErr = nil
		m.detailNotice = "Saving jail note..."
		return m, jailNoteUpdateCmd(m.detail, note)
	case "delete":
		if m.detailNoteMode && !m.detailNoteSaving {
			m.backspaceDetailNoteInput()
			return m, nil
		}
	case "j", "down":
		if m.detailNoteMode {
			return m, nil
		}
		m.detailScroll++
	case "k", "up":
		if m.detailNoteMode {
			return m, nil
		}
		m.detailScroll--
	case "g", "home":
		if m.detailNoteMode {
			return m, nil
		}
		m.detailScroll = 0
	case "G", "end":
		if m.detailNoteMode {
			return m, nil
		}
		m.detailScroll = 1 << 30
	case "pgdown":
		if m.detailNoteMode {
			return m, nil
		}
		m.detailScroll += m.detailBodyHeight()
	case "pgup":
		if m.detailNoteMode {
			return m, nil
		}
		m.detailScroll -= m.detailBodyHeight()
	case "backspace":
		if m.detailNoteMode && !m.detailNoteSaving {
			m.backspaceDetailNoteInput()
			return m, nil
		}
	case "r":
		if m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
		jail, ok := m.detailJail()
		if !ok {
			return m, nil
		}
		m.detailLoading = true
		m.detailErr = nil
		m.detailNotice = ""
		return m, detailCmd(jail)
	case "a", "A":
		if m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
		m.detailShowAdvanced = !m.detailShowAdvanced
		m.detailNotice = ""
		m.detailErr = nil
		return m, nil
	case "b", "B":
		if m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
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
		if m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
		if m.detail.ZFS == nil || strings.TrimSpace(m.detail.ZFS.Name) == "" {
			m.detailErr = fmt.Errorf("no ZFS dataset detected for this jail")
			return m, nil
		}
		m.mode = screenZFSPanel
		m.zfsPanel = newZFSPanelState(m.detail.ZFS.Name, screenJailDetail, m.detail)
		return m, tea.Batch(listZFSSnapshotsCmd(m.zfsPanel.dataset), zfsPropertyStateCmd(m.zfsPanel.dataset))
	case "R":
		if m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
		jail, ok := m.detailJail()
		if !ok {
			return m, nil
		}
		m.detailErr = nil
		m.detailNotice = "Restarting jail..."
		return m, jailServiceCmd(jail, "restart")
	case "u", "U":
		if m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
		jail, ok := m.detailJail()
		if !ok {
			return m, nil
		}
		m.upgrade = newUpgradeState(jail, screenJailDetail)
		m.mode = screenUpgradeWizard
		return m, nil
	case "x", "X":
		if m.detailNoteMode || m.detailNoteSaving {
			return m, nil
		}
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
		m.openHelp(m.mode)
		return m, nil
	}
	if m.wizard.thinDatasetMode {
		if msg.String() == "c" || msg.String() == "C" {
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
		}
		action, handled := handleWizardSelectorNavigation(msg.String(), &m.wizard.thinDatasetCursor, len(m.wizard.thinDatasetOpts))
		if !handled {
			return m, nil
		}
		switch action {
		case wizardSelectorActionCancel:
			if m.wizard.datasetCreateRunning {
				return m, nil
			}
			m.wizard.endThinDatasetSelect()
			m.wizard.message = "Thin template dataset selection canceled."
			m.ensureWizardFieldVisible()
			return m, nil
		case wizardSelectorActionRefresh:
			if m.wizard.datasetCreateRunning {
				return m, nil
			}
			if err := m.wizard.beginThinDatasetSelect(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			return m, nil
		case wizardSelectorActionSubmit:
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
		return m, nil
	}
	if m.wizard.userlandMode {
		action, handled := handleWizardSelectorNavigation(msg.String(), &m.wizard.userlandCursor, len(m.wizard.userlandOpts))
		if !handled {
			return m, nil
		}
		switch action {
		case wizardSelectorActionCancel:
			m.wizard.endUserlandSelect()
			m.wizard.message = "Userland selection canceled."
			m.ensureWizardFieldVisible()
			return m, nil
		case wizardSelectorActionRefresh:
			if err := m.wizard.beginUserlandSelect(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			return m, nil
		case wizardSelectorActionSubmit:
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
		action, handled := handleWizardSelectorNavigation(msg.String(), &m.wizard.templateCursor, len(m.wizard.templates))
		if !handled {
			return m, nil
		}
		switch action {
		case wizardSelectorActionCancel:
			m.wizard.endTemplateMode()
			m.wizard.message = "Template load canceled."
			return m, nil
		case wizardSelectorActionRefresh:
			if err := m.wizard.beginTemplateLoad(); err != nil {
				m.wizard.message = err.Error()
				return m, nil
			}
			return m, nil
		case wizardSelectorActionSubmit:
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
		return m, nil
	}

	if !m.wizardApplying && !m.wizard.isConfirmationStep() && msg.Type == tea.KeyRunes {
		m.wizard.appendToActive(string(msg.Runes))
		m.boundWizardScroll()
		return m, nil
	}

	switch msg.String() {
	case "j":
		if m.wizard.isConfirmationStep() || m.wizardApplying {
			m.wizardScroll++
			m.boundWizardScroll()
			return m, nil
		}
	case "k":
		if m.wizard.isConfirmationStep() || m.wizardApplying {
			m.wizardScroll--
			m.boundWizardScroll()
			return m, nil
		}
	case "p", "P":
		if m.wizard.isConfirmationStep() {
			m.wizard.showJailConfPreview = !m.wizard.showJailConfPreview
			m.boundWizardScroll()
			return m, nil
		}
	case "space", " ":
		if !m.wizardApplying && !m.wizard.isConfirmationStep() {
			if field, ok := m.wizard.activeField(); ok && field.ID == "note" {
				m.wizard.appendToActive(" ")
				m.boundWizardScroll()
				return m, nil
			}
		}
	case "c", "C":
		if m.wizardApplying {
			if m.wizardCancel != nil {
				m.wizardCancel()
				m.wizard.message = "Canceling creation... (waiting for safe rollback)"
			}
			return m, nil
		}
		// otherwise allow typing 'c' in text fields? Wait, if it's KeyRunes it was already handled above.
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
		if m.wizard.isConfirmationStep() || m.wizardApplying {
			m.wizardScroll++
			m.boundWizardScroll()
			return m, nil
		}
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.nextField()
		m.ensureWizardFieldVisible()
		return m, nil
	case "shift+tab", "up":
		if m.wizard.isConfirmationStep() || m.wizardApplying {
			m.wizardScroll--
			m.boundWizardScroll()
			return m, nil
		}
		if m.wizardApplying {
			return m, nil
		}
		m.wizard.prevField()
		m.ensureWizardFieldVisible()
		return m, nil
	case "pgdown":
		m.wizardScroll += m.wizardBodyHeight()
		m.boundWizardScroll()
		return m, nil
	case "pgup":
		m.wizardScroll -= m.wizardBodyHeight()
		m.boundWizardScroll()
		return m, nil
	case "home":
		if m.wizardApplying && !m.wizard.isConfirmationStep() {
			return m, nil
		}
		m.wizardScroll = 0
		return m, nil
	case "end":
		if m.wizardApplying && !m.wizard.isConfirmationStep() {
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
			m.wizardScroll = 0
			m.downloading = false
			m.downloadRead = 0
			m.downloadTotal = 0
			m.progressChan = make(chan downloadProgressMsg, 100)
			ctx, cancel := context.WithCancel(context.Background())
			m.wizardCancel = cancel
			return m, tea.Batch(
				createJailCmd(ctx, m.wizard.values, m.progressChan),
				waitForProgress(m.progressChan),
			)
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

type wizardSelectorAction int

const (
	wizardSelectorActionNone wizardSelectorAction = iota
	wizardSelectorActionCancel
	wizardSelectorActionRefresh
	wizardSelectorActionSubmit
)

func handleWizardSelectorNavigation(key string, cursor *int, count int) (wizardSelectorAction, bool) {
	switch key {
	case "esc", "left":
		return wizardSelectorActionCancel, true
	case "j", "down", "tab":
		*cursor = *cursor + 1
	case "k", "up", "shift+tab":
		*cursor = *cursor - 1
	case "g", "home":
		*cursor = 0
	case "G", "end":
		*cursor = count - 1
	case "r", "R":
		return wizardSelectorActionRefresh, true
	case "enter":
		return wizardSelectorActionSubmit, true
	default:
		return wizardSelectorActionNone, false
	}
	*cursor = boundSelectionCursor(*cursor, count)
	return wizardSelectorActionNone, true
}

func (m model) updateTemplateDatasetKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "?" {
		m.openHelp(m.mode)
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
		if m.templateCreate.mode == templateManagerModeSnapshotDestroy {
			m.templateCreate.mode = templateManagerModeClone
			m.templateCreate.snapshotDestroyPreview = TemplateDatasetSnapshotDestroyPreview{}
			m.templateCreate.message = "Template snapshot destroy canceled."
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
		if m.templateCreate.mode == templateManagerModeClone || m.templateCreate.mode == templateManagerModeSnapshotDestroy {
			m.templateCreate.cloneCursor++
			m.templateCreate.boundCloneCursor()
			if m.templateCreate.mode == templateManagerModeSnapshotDestroy {
				m.templateCreate.refreshSnapshotDestroyPreview()
			} else {
				m.templateCreate.refreshClonePreview()
			}
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
		if m.templateCreate.mode == templateManagerModeClone || m.templateCreate.mode == templateManagerModeSnapshotDestroy {
			m.templateCreate.cloneCursor--
			m.templateCreate.boundCloneCursor()
			if m.templateCreate.mode == templateManagerModeSnapshotDestroy {
				m.templateCreate.refreshSnapshotDestroyPreview()
			} else {
				m.templateCreate.refreshClonePreview()
			}
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
	case "s", "S":
		if m.templateCreate.applying || m.templateCreate.parentApplying || m.templateCreate.mode != templateManagerModeBrowse {
			return m, nil
		}
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			m.templateCreate.message = "No template dataset selected."
			return m, nil
		}
		m.mode = screenZFSPanel
		m.zfsPanel = newZFSPanelState(item.Name, screenTemplateDatasetCreate, JailDetail{})
		return m, tea.Batch(listZFSSnapshotsCmd(m.zfsPanel.dataset), zfsPropertyStateCmd(m.zfsPanel.dataset))
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
		m.templateCreate.snapshotDestroyPreview = TemplateDatasetSnapshotDestroyPreview{}
		m.templateCreate.logs = nil
		m.templateCreate.message = "Loading template snapshots..."
		return m, templateSnapshotListCmd(item.Name)
	case "ctrl+x":
		if m.templateCreate.applying || m.templateCreate.parentApplying || m.templateCreate.mode != templateManagerModeClone {
			return m, nil
		}
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			m.templateCreate.message = "No template dataset selected."
			return m, nil
		}
		snapshot, ok := m.templateCreate.selectedCloneSnapshot()
		if !ok {
			m.templateCreate.message = "No snapshot selected."
			return m, nil
		}
		m.templateCreate.mode = templateManagerModeSnapshotDestroy
		m.templateCreate.logs = nil
		m.templateCreate.snapshotDestroyPreview = InspectTemplateSnapshotDestroy(item.Name, snapshot.Name, m.templateCreate.parentOverride())
		if m.templateCreate.snapshotDestroyPreview.Err != nil {
			m.templateCreate.message = m.templateCreate.snapshotDestroyPreview.Err.Error()
		} else {
			m.templateCreate.message = "Press enter to destroy this template snapshot."
		}
		return m, nil
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
	case "p", "P":
		if m.templateCreate.applying || m.templateCreate.parentApplying || m.templateCreate.mode != templateManagerModeCreate || m.templateCreate.parentEdit {
			return m, nil
		}
		decision := resolveFreeBSDPatchDecision(m.templateCreate.sourceInput, m.templateCreate.patchBase)
		if !decision.Eligible {
			m.templateCreate.message = "Patch-to-latest is only available for official FreeBSD release tags and recognizable base.txz release archives."
			return m, nil
		}
		normalized, _ := normalizeFreeBSDPatchPreference(m.templateCreate.patchBase)
		if normalized == "no" {
			m.templateCreate.patchBase = "auto"
			m.templateCreate.message = "Template patch-to-latest restored to automatic mode."
		} else {
			m.templateCreate.patchBase = "no"
			m.templateCreate.message = "Template patch-to-latest disabled for this create."
		}
		m.templateCreate.refreshPreview()
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
			return m, templateDatasetCreateCmd(m.templateCreate.sourceInput, m.templateCreate.parentOverride(), m.templateCreate.patchBase)
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
		if m.templateCreate.mode == templateManagerModeSnapshotDestroy {
			if m.templateCreate.snapshotDestroyPreview.Err != nil {
				m.templateCreate.message = m.templateCreate.snapshotDestroyPreview.Err.Error()
				return m, nil
			}
			m.templateCreate.message = "Destroying template snapshot..."
			m.templateCreate.logs = nil
			m.templateCreate.applying = true
			return m, templateSnapshotDestroyCmd(m.templateCreate.snapshotDestroyPreview.Current.Name, m.templateCreate.snapshotDestroyPreview.Snapshot, m.templateCreate.parentOverride())
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
		if m.templateCreate.mode == templateManagerModeClone || m.templateCreate.mode == templateManagerModeSnapshotDestroy {
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
	case "esc", "backspace", "enter", "?":
		m.mode = m.helpReturnMode
		return m, nil
	case "left", "h", "shift+tab":
		m.moveHelpTab(-1)
	case "right", "l", "tab":
		m.moveHelpTab(1)
	case "j", "down":
		m.helpScrollDelta(1)
	case "k", "up":
		m.helpScrollDelta(-1)
	case "g", "home":
		m.setHelpScroll(0)
	case "G", "end":
		m.setHelpScroll(1 << 30)
	case "pgdown":
		m.helpScrollDelta(m.helpContentHeight())
	case "pgup":
		m.helpScrollDelta(-m.helpContentHeight())
	}
	m.boundHelpScroll()
	return m, nil
}

func (m model) updateUpgradeWizardKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.upgrade.applying {
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
		return m, nil
	}
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "q":
		return m, tea.Quit
	case "esc", "backspace", "left":
		if !m.upgrade.selecting {
			m.upgrade.selecting = true
			m.upgrade.logs = nil
			m.upgrade.err = nil
			m.upgrade.message = ""
			return m, nil
		}
		m.mode = m.upgrade.returnMode
		return m, nil
	case "j", "down":
		if m.upgrade.selecting {
			m.upgrade.cursor++
			if m.upgrade.cursor >= len(upgradeWorkflowAll) {
				m.upgrade.cursor = len(upgradeWorkflowAll) - 1
			}
		}
	case "k", "up":
		if m.upgrade.selecting {
			m.upgrade.cursor--
			if m.upgrade.cursor < 0 {
				m.upgrade.cursor = 0
			}
		}
	case "enter", "right":
		if m.upgrade.selecting {
			m.upgrade.workflow = upgradeWorkflowAll[m.upgrade.cursor]
			m.upgrade.selecting = false
			m.upgrade.logs = nil
			m.upgrade.err = nil
			m.upgrade.message = "Press enter to execute this upgrade workflow."
			return m, nil
		}
		m.upgrade.applying = true
		m.upgrade.logs = nil
		m.upgrade.err = nil
		m.upgrade.message = "Running upgrade..."
		return m, upgradeJailCmd(m.upgrade.target, m.upgrade.workflow)
	}
	return m, nil
}

func (m model) renderUpgradeWizardView() string {
	title := titleStyle.Render("Jail Upgrade")
	meta := detailKeyStyle.Render("Jail:") + " " + selectedRowStyle.Render(valueOrDash(m.upgrade.target.Name))
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)

	bodyWidth := max(12, m.width-2)
	lines := []string{}

	if m.upgrade.selecting {
		lines = append(lines, sectionStyle.Render("Select Upgrade Workflow"))
		lines = append(lines, "")
		for idx, wf := range upgradeWorkflowAll {
			label := upgradeWorkflowLabel(wf)
			if idx == m.upgrade.cursor {
				lines = append(lines, selectedRowStyle.Width(max(1, bodyWidth)).Render(truncate("> "+label, bodyWidth)))
			} else {
				lines = append(lines, truncate("  "+label, bodyWidth))
			}
		}
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Description"))
		for _, line := range upgradeWorkflowDescriptionLines(upgradeWorkflowAll[m.upgrade.cursor], m.upgrade.target) {
			lines = append(lines, truncate(line, bodyWidth))
		}
	} else {
		lines = append(lines, sectionStyle.Render("Upgrade Plan"))
		lines = append(lines, truncate("Workflow: "+upgradeWorkflowLabel(m.upgrade.workflow), bodyWidth))
		lines = append(lines, "")
		for _, line := range upgradeWorkflowDescriptionLines(m.upgrade.workflow, m.upgrade.target) {
			lines = append(lines, truncate(line, bodyWidth))
		}
		if len(m.upgrade.logs) > 0 {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Execution output"))
			for _, line := range m.upgrade.logs {
				lines = append(lines, truncate(line, bodyWidth))
			}
		}
	}

	hint := "j/k: select workflow | enter: confirm | esc: cancel | q: quit"
	if !m.upgrade.selecting {
		hint = "enter: run upgrade | esc: back to selection | q: quit"
		if m.upgrade.applying {
			hint = "Upgrade in progress... please wait | ctrl+c: quit"
		}
	}
	message := m.upgrade.message
	footerRenderer := footerStyle
	if m.upgrade.err != nil {
		message = "error: " + m.upgrade.err.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	}
	footer := m.renderFooterWithMessage(hint, message, footerRenderer)
	bodyHeight := m.pageBodyHeight(header, footer, 0)
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))
	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
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
	if m.mode == screenUpgradeWizard {
		return m.renderUpgradeWizardView()
	}
	if m.mode == screenHelp {
		return m.renderHelpView()
	}
	if m.mode == screenJailDetail {
		return m.renderJailDetailView()
	}
	return m.renderDashboard()
}

func (m *model) ensureHelpState() {
	if len(m.helpTabScrolls) != int(helpTabCount) {
		m.helpTabScrolls = make([]int, int(helpTabCount))
	}
	if m.helpTab < 0 || m.helpTab >= helpTabCount {
		m.helpTab = helpTabOverview
	}
}

func helpDefaultTabForMode(mode screenMode) helpTab {
	switch mode {
	case screenDashboard, screenJailDetail, screenDestroyConfirm:
		return helpTabDashboard
	case screenCreateWizard, screenTemplateDatasetCreate:
		return helpTabCreate
	case screenZFSPanel:
		return helpTabZFS
	case screenUpgradeWizard:
		return helpTabUpgrade
	case screenInitialCheck:
		return helpTabSetup
	default:
		return helpTabOverview
	}
}

func (m *model) openHelp(returnMode screenMode) {
	m.ensureHelpState()
	m.helpReturnMode = returnMode
	m.helpTab = helpDefaultTabForMode(returnMode)
	m.helpTabScrolls[int(m.helpTab)] = 0
	m.helpScroll = 0
	m.mode = screenHelp
	m.boundHelpScroll()
}

func (m *model) moveHelpTab(delta int) {
	m.ensureHelpState()
	next := int(m.helpTab) + delta
	if next < 0 {
		next = int(helpTabCount) - 1
	}
	if next >= int(helpTabCount) {
		next = 0
	}
	m.helpTab = helpTab(next)
	m.helpScroll = m.helpTabScrolls[next]
	m.boundHelpScroll()
}

func (m *model) helpScrollDelta(delta int) {
	m.ensureHelpState()
	m.helpScroll = m.helpTabScrolls[int(m.helpTab)] + delta
}

func (m *model) setHelpScroll(value int) {
	m.ensureHelpState()
	m.helpScroll = value
}

func (m model) renderHelpView() string {
	title := titleStyle.Render("Help / Shortcuts")
	meta := summaryStyle.Render("Tab: " + m.currentHelpTabContent().Label + "  esc: close")
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)
	footer := m.renderFooterWithMessage("left/right or h/l: switch tabs | j/k or pgup/pgdown: scroll | esc/enter: close help | ctrl+c: quit", "", footerStyle)

	bodyHeight := m.helpBodyHeight()
	lines := m.helpLines(max(12, m.width-2))
	contentHeight := m.helpContentHeight()
	maxOffset := max(0, len(lines)-contentHeight)
	offset := m.helpScroll
	if offset < 0 {
		offset = 0
	}
	if offset > maxOffset {
		offset = maxOffset
	}
	end := min(len(lines), offset+contentHeight)

	bodyLines := []string{m.renderHelpTabBar(max(12, m.width-2)), ""}
	bodyLines = append(bodyLines, lines[offset:end]...)

	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(bodyLines, "\n"))

	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
}

func (m model) helpLines(width int) []string {
	tab := m.currentHelpTabContent()
	lines := make([]string, 0, 96)
	for idx, section := range tab.Sections {
		lines = append(lines, sectionStyle.Render(section.Title))
		if strings.TrimSpace(section.Summary) != "" {
			appendHelpWrappedLines(&lines, width, section.Summary, helpNoteStyle, "")
		}
		if len(section.Shortcuts) > 0 {
			lines = append(lines, detailSectionStyle.Render("Keys"))
			pairs := make([][2]string, 0, len(section.Shortcuts))
			for _, shortcut := range section.Shortcuts {
				pairs = append(pairs, [2]string{shortcut.Keys, shortcut.Action})
			}
			lines = append(lines, renderKeyValueLinesWithLabelWidth(width, helpShortcutLabelWidth(width), pairs...)...)
		}
		if len(section.Notes) > 0 {
			lines = append(lines, detailSectionStyle.Render("Notes"))
			for _, note := range section.Notes {
				appendHelpWrappedLines(&lines, max(12, width-2), note, helpNoteStyle, "  ")
			}
		}
		if idx+1 < len(tab.Sections) {
			lines = append(lines, "")
		}
	}
	if len(lines) == 0 {
		lines = append(lines, "No help available.")
	}
	return lines
}

func (m model) currentHelpTabContent() helpTabContent {
	tabs := helpTabs()
	if len(tabs) == 0 {
		return helpTabContent{Label: "Help"}
	}
	idx := int(m.helpTab)
	if idx < 0 || idx >= len(tabs) {
		return tabs[0]
	}
	return tabs[idx]
}

func (m model) renderHelpTabBar(width int) string {
	tabs := helpTabs()
	items := make([]string, 0, len(tabs))
	for idx, tab := range tabs {
		style := helpTabStyle
		if helpTab(idx) == m.helpTab {
			style = helpTabActiveStyle
		}
		items = append(items, style.Render(tab.Label))
	}
	line := lipgloss.JoinHorizontal(lipgloss.Top, items...)
	if lipgloss.Width(line) >= width {
		return line
	}
	return line + strings.Repeat(" ", width-lipgloss.Width(line))
}

func helpTabs() []helpTabContent {
	return []helpTabContent{
		{
			Label: "Overview",
			Sections: []helpSection{
				{
					Title:   "Global",
					Summary: "Keys available across the app outside active text entry fields.",
					Shortcuts: []helpShortcut{
						{Keys: "?, h", Action: "open help"},
						{Keys: "ctrl+c", Action: "quit the application"},
						{Keys: "q", Action: "quit outside text input"},
					},
					Notes: []string{
						"Help opens on the tab that matches the current screen.",
					},
				},
				{
					Title:   "Navigation Patterns",
					Summary: "Most screens reuse the same small set of movement and confirmation keys.",
					Shortcuts: []helpShortcut{
						{Keys: "j/k, arrows", Action: "move through lists or scrollable content"},
						{Keys: "pgup/pgdown", Action: "page through longer views"},
						{Keys: "g/G", Action: "jump to top or bottom where supported"},
						{Keys: "enter", Action: "open, confirm, or execute the selected action"},
						{Keys: "esc", Action: "cancel the current prompt or return to the previous screen"},
					},
					Notes: []string{
						"Destructive actions usually switch to a preview or confirmation step before anything is changed.",
						"Use left/right or h/l inside Help to switch tabs, then j/k or pgup/pgdown to scroll within the active tab.",
					},
				},
			},
		},
		{
			Label: "Dashboard",
			Sections: []helpSection{
				{
					Title:   "Dashboard",
					Summary: "Browse discovered jails, select multiple targets, and launch deeper workflows.",
					Shortcuts: []helpShortcut{
						{Keys: "j/k, arrows, pgup/pgdown, g/G", Action: "navigate the jail list"},
						{Keys: "enter, d", Action: "open the selected jail detail view"},
						{Keys: "c", Action: "open the jail creation wizard"},
						{Keys: "i", Action: "re-run the initial config check"},
						{Keys: "t", Action: "open the template manager"},
						{Keys: "s", Action: "start or stop the selected jail, or all selected jails"},
						{Keys: "R", Action: "restart the selected jail, or all selected jails"},
						{Keys: "u", Action: "open the guided upgrade wizard for the selected jail"},
						{Keys: "space", Action: "toggle bulk selection for the highlighted jail"},
						{Keys: "ctrl+a", Action: "select all jails or clear the current selection"},
						{Keys: "esc", Action: "clear jail selection when any jails are selected"},
						{Keys: "z", Action: "open the ZFS panel for the selected jail"},
						{Keys: "x", Action: "open destroy confirmation for the selected jail"},
						{Keys: "r", Action: "refresh dashboard data"},
					},
					Notes: []string{
						"Bulk service actions apply to the current jail selection when one or more jails are marked.",
					},
				},
				{
					Title:   "Jail Detail",
					Summary: "Inspect the selected jail, refresh runtime state, and branch into service, upgrade, or ZFS actions.",
					Shortcuts: []helpShortcut{
						{Keys: "j/k, pgup/pgdown, g/G", Action: "scroll the detail pane"},
						{Keys: "a", Action: "toggle advanced runtime and default parameter sections"},
						{Keys: "n", Action: "edit the short dashboard note stored for this jail"},
						{Keys: "r", Action: "refresh selected jail details"},
						{Keys: "R", Action: "restart this jail"},
						{Keys: "u", Action: "open the guided upgrade wizard for this jail"},
						{Keys: "b", Action: "retry linux bootstrap for a running linux jail"},
						{Keys: "z", Action: "open the ZFS integration panel"},
						{Keys: "x", Action: "open destroy confirmation for this jail"},
						{Keys: "esc", Action: "return to the dashboard"},
					},
					Notes: []string{
						"Startup policy shows jail_list order and configured depend values.",
						"Notes are stored in managed jail.conf metadata and shown on the dashboard quick-details panel.",
						"Network summary combines configured values, runtime state, and host-side validation results.",
					},
				},
				{
					Title:   "Destroy Confirm",
					Summary: "Final confirmation before stopping and destroying the selected jail.",
					Shortcuts: []helpShortcut{
						{Keys: "enter, y", Action: "stop and destroy the selected jail"},
						{Keys: "esc, n", Action: "cancel and return"},
					},
				},
			},
		},
		{
			Label: "Create",
			Sections: []helpSection{
				{
					Title:   "Creation Wizard",
					Summary: "Collect jail settings, validate prerequisites, and execute the create workflow.",
					Shortcuts: []helpShortcut{
						{Keys: "tab, shift+tab, up/down", Action: "move between fields"},
						{Keys: "pgup/pgdown", Action: "scroll longer wizard pages"},
						{Keys: "enter, right", Action: "advance to the next step or execute on confirmation"},
						{Keys: "left", Action: "return to the previous step"},
						{Keys: "s, l", Action: "save or load templates on the confirmation step"},
						{Keys: "ctrl+u", Action: "open the userland selector"},
						{Keys: "ctrl+t", Action: "open the template manager for thin-jail template selection"},
						{Keys: "?", Action: "open help from the wizard"},
					},
					Notes: []string{
						"Startup order updates rc.conf jail_list; dependency settings write depend in jail.conf.",
						"Linux setup supports default or custom bootstrap mirrors, and retry reuses the saved mirror choice.",
						"VNET preflight checks bridge and uplink state, running-jail IP conflicts, subnet overlap warnings, and bridge policy before create.",
					},
				},
				{
					Title:   "Template Manager",
					Summary: "Manage reusable thin-jail template datasets and choose a mountpoint when the wizard opens this screen in selection mode.",
					Shortcuts: []helpShortcut{
						{Keys: "j/k", Action: "select a template dataset"},
						{Keys: "enter", Action: "apply the selected mountpoint when opened from the thin-jail wizard"},
						{Keys: "c", Action: "create a template dataset from a source path, release tag, userland entry, or custom URL"},
						{Keys: "n", Action: "open snapshot management for the selected template dataset"},
						{Keys: "r", Action: "rename the selected template dataset"},
						{Keys: "x", Action: "destroy the selected template dataset"},
						{Keys: "s", Action: "open the selected template dataset in the ZFS panel"},
						{Keys: "ctrl+r", Action: "refresh the dataset list, or refresh the create preview while creating"},
					},
					Notes: []string{
						"When the templates parent dataset is missing, enter creates the proposed parent and ctrl+e lets you edit the parent dataset and mountpoint first.",
					},
				},
				{
					Title:   "Template Snapshots",
					Summary: "Within template snapshot management, work with snapshots from the selected template dataset.",
					Shortcuts: []helpShortcut{
						{Keys: "up/down", Action: "select a snapshot"},
						{Keys: "enter", Action: "clone the selected snapshot into a new template dataset"},
						{Keys: "ctrl+x", Action: "switch to snapshot destroy mode for the selected snapshot"},
						{Keys: "ctrl+r", Action: "refresh the snapshot list"},
						{Keys: "esc", Action: "return to the template dataset view"},
					},
					Notes: []string{
						"Snapshot destroy is blocked when the snapshot still has clone dependents or when current jails use datasets cloned from that snapshot.",
					},
				},
			},
		},
		{
			Label: "ZFS",
			Sections: []helpSection{
				{
					Title:   "ZFS Panel",
					Summary: "Inspect snapshots and selected dataset properties for an exact jail dataset or template dataset.",
					Shortcuts: []helpShortcut{
						{Keys: "j/k", Action: "select a snapshot"},
						{Keys: "c", Action: "create a snapshot"},
						{Keys: "r", Action: "rollback to the selected snapshot with confirmation"},
						{Keys: "n", Action: "clone the selected jail snapshot as a new jail"},
						{Keys: "e", Action: "edit compression, quota, or reservation on an exact jail dataset"},
						{Keys: "x", Action: "refresh the snapshot list"},
						{Keys: "esc", Action: "cancel the current prompt or return to the previous screen"},
					},
					Notes: []string{
						"The selected snapshot details show creation time, used size, and rollback implications.",
						"Rollback preview highlights newer snapshots that would be removed and warns when dependencies may block the rollback.",
					},
				},
			},
		},
		{
			Label: "Upgrade",
			Sections: []helpSection{
				{
					Title:   "Upgrade Wizard",
					Summary: "Choose a supported upgrade workflow for the selected jail and review the execution plan before running it.",
					Shortcuts: []helpShortcut{
						{Keys: "j/k", Action: "select an upgrade workflow"},
						{Keys: "enter", Action: "confirm the workflow or execute the upgrade"},
						{Keys: "esc", Action: "return to workflow selection or leave the wizard"},
					},
					Notes: []string{
						"Classic upgrades patch the jail base with freebsd-update and do not require the jail to be running.",
						"Thin-template upgrades detect the clone origin, patch the template dataset, create a post-upgrade snapshot, then restore readonly mode.",
						"Pkg reinstall upgrades run pkg upgrade -f inside the jail and start the jail first when needed.",
					},
				},
			},
		},
		{
			Label: "Setup",
			Sections: []helpSection{
				{
					Title:   "Initial Config Check",
					Summary: "Runs at startup before the dashboard and can be reopened later from the dashboard.",
					Shortcuts: []helpShortcut{
						{Keys: "y/n or d/c/n", Action: "apply or skip the proposed setup action, depending on the prompt"},
						{Keys: "enter", Action: "continue to the dashboard when the check is complete"},
					},
				},
			},
		},
	}
}

func appendHelpWrappedLines(lines *[]string, width int, text string, style lipgloss.Style, prefix string) {
	width = max(12, width)
	for _, part := range wrapText(strings.TrimSpace(text), width) {
		if prefix != "" {
			part = prefix + part
		}
		*lines = append(*lines, style.Render(part))
	}
}

func helpShortcutLabelWidth(width int) int {
	switch {
	case width >= 100:
		return 28
	case width >= 80:
		return 24
	default:
		return 20
	}
}

func (m model) renderDestroyView() string {
	title := titleStyle.Render("Destroy Jail")
	meta := detailKeyStyle.Render("Selected:") + " " + selectedRowStyle.Render(valueOrDash(m.destroy.target.Name))
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)

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
	bodyHeight := m.pageBodyHeight(header, footer, 0)
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))
	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
}

func (m model) helpBodyHeight() int {
	title := titleStyle.Render("Help / Shortcuts")
	meta := summaryStyle.Render("Tab: " + m.currentHelpTabContent().Label + "  esc: close")
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)
	footer := m.renderFooterWithMessage("left/right or h/l: switch tabs | j/k or pgup/pgdown: scroll | esc/enter: close help | ctrl+c: quit", "", footerStyle)
	return m.pageBodyHeight(header, footer, 0)
}

func (m model) helpContentHeight() int {
	return max(1, m.helpBodyHeight()-2)
}

func (m *model) boundHelpScroll() {
	if m.mode != screenHelp {
		return
	}
	m.ensureHelpState()
	lines := m.helpLines(max(12, m.width-2))
	maxOffset := max(0, len(lines)-m.helpContentHeight())
	if m.helpScroll < 0 {
		m.helpScroll = 0
	}
	if m.helpScroll > maxOffset {
		m.helpScroll = maxOffset
	}
	m.helpTabScrolls[int(m.helpTab)] = m.helpScroll
}

func (m model) renderDashboard() string {
	header := m.renderHeader()
	footer := m.renderFooter()
	bodyHeight := m.pageBodyHeight(header, footer, 0)
	const (
		dashboardMinLeftWidth  = 32
		dashboardMinRightWidth = 24
	)
	leftWidth := max(dashboardMinLeftWidth, m.width/3)
	maxLeftWidth := m.width - dashboardMinRightWidth - 1
	if leftWidth > maxLeftWidth {
		leftWidth = maxLeftWidth
	}
	if leftWidth < dashboardMinLeftWidth {
		leftWidth = m.width
	}
	rightWidth := m.width - leftWidth - 1
	if rightWidth < 0 {
		rightWidth = 0
	}

	listPanel := m.renderJailList(leftWidth, bodyHeight)
	if rightWidth == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, header, "", listPanel, footer)
	}

	detailPanel := m.renderDetailPanel(rightWidth, bodyHeight)
	separator := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		Render(strings.Repeat("|\n", bodyHeight-1) + "|")

	body := lipgloss.JoinHorizontal(lipgloss.Top, listPanel, separator, detailPanel)

	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
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
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)

	hint := "j/k or up/down: scroll | pgup/pgdown | g/G | a: advanced runtime | r: refresh detail"
	if m.detailNoteMode {
		hint = "type to edit note | enter: save | backspace: delete | esc: cancel | ctrl+c: quit"
	} else {
		if detailLooksLikeLinuxJail(m.detail) {
			hint += " | b: retry linux bootstrap"
		}
		hint += " | n: edit note | z: ZFS panel | x: destroy | h: help | esc: back | q: quit"
	}
	message := ""
	if m.detailErr != nil {
		message = "warning: " + m.detailErr.Error()
	} else if m.detailNotice != "" {
		message = m.detailNotice
	} else if m.detailLoading {
		message = "loading detail..."
	}
	footer := m.renderFooterWithMessage(hint, message, footerStyle)
	bodyHeight := m.pageBodyHeight(header, footer, 0)
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

	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
}

func (m model) renderWizardView() string {
	step := m.wizard.currentStep()
	title := titleStyle.Render("Jail Creation Wizard")
	meta := summaryStyle.Render(fmt.Sprintf("Step %d/%d: %s", m.wizard.step+1, len(m.wizard.steps()), step.Title))
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)

	hint := m.wizardFooterHint()
	footer := m.renderFooterWithMessage(hint, m.wizard.message, footerStyle)
	bodyHeight := m.pageBodyHeight(header, footer, 1)
	body := ""
	if leftWidth, rightWidth, ok := m.wizardSplitPaneWidths(); ok {
		leftLines, _ := m.wizardFieldEntryLayout(max(12, leftWidth-2), false)
		offset, end := wizardViewportBounds(len(leftLines), m.wizardScroll, bodyHeight)
		leftPanel := lipgloss.NewStyle().
			Width(leftWidth).
			Height(bodyHeight).
			Padding(1, 1).
			Render(strings.Join(leftLines[offset:end], "\n"))
		rightPanel := lipgloss.NewStyle().
			Width(rightWidth).
			Height(bodyHeight).
			Padding(1, 1).
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

	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
}

func (m model) wizardFooterHint() string {
	hint := "type to edit | tab/shift+tab/up/down: fields | pgup/pgdown: scroll | ctrl+u: userland select | enter/right: next | left: back | ?: help | esc: cancel | ctrl+c: quit"
	if normalizedJailType(m.wizard.values.JailType) == "thin" {
		hint = "type to edit | tab/shift+tab/up/down: fields | pgup/pgdown: scroll | ctrl+u: userland select | ctrl+t: template manager | enter/right: next | left: back | ?: help | esc: cancel | ctrl+c: quit"
	}
	if m.wizard.isConfirmationStep() {
		hint = "j/k or up/down: scroll | pgup/pgdown | p: jail.conf | enter: create | left: back | s/l: tmpl | ?: help | esc: cancel | q: quit | ctrl+c: quit"
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
		hint = "creating jail... j/k or pgup/pgdown: scroll | c: cancel | ctrl+c: quit"
	}
	return hint
}

func (m model) wizardBodyHeight() int {
	step := m.wizard.currentStep()
	title := titleStyle.Render("Jail Creation Wizard")
	meta := summaryStyle.Render(fmt.Sprintf("Step %d/%d: %s", m.wizard.step+1, len(m.wizard.steps()), step.Title))
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)
	footer := m.renderFooterWithMessage(m.wizardFooterHint(), m.wizard.message, footerStyle)
	return m.pageBodyHeight(header, footer, 1)
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

func renderWizardSelectionList(width int, title string, labels []string, cursor int, emptyLines []string, previewTitle string, previewLines []string) []string {
	lines := []string{sectionStyle.Render(title)}
	if len(labels) == 0 {
		lines = append(lines, emptyLines...)
		return lines
	}
	cursor = boundSelectionCursor(cursor, len(labels))
	for idx, label := range labels {
		row := "  " + label
		if idx == cursor {
			row = truncate("> "+label, width)
			row = selectedRowStyle.Width(max(1, width)).Render(row)
			lines = append(lines, row)
			continue
		}
		lines = append(lines, truncate(row, width))
	}
	if len(previewLines) > 0 {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render(previewTitle))
		for _, line := range previewLines {
			lines = append(lines, truncate(line, width))
		}
	}
	return lines
}

func (m model) renderTemplateDatasetCreateView() string {
	title := titleStyle.Render("Template Manager")
	meta := summaryStyle.Render("Reusable ZFS templates for thin jails")
	header := headerBarStyle.Width(m.width).Render(title + "  " + meta)
	hint := "j/k: select | c: create | n: manage snapshots | r: rename | x: destroy | s: ZFS panel | ctrl+r: refresh | ?: help | esc: back | ctrl+c: quit"
	if m.templateCreate.selectMode && m.templateCreate.mode == templateManagerModeBrowse {
		hint = "j/k: select | enter: apply mountpoint | c: create | n: manage snapshots | r: rename | x: destroy | s: ZFS panel | ctrl+r: refresh | ?: help | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.mode == templateManagerModeCreate {
		hint = "type source | p: toggle patch | enter: create | backspace: edit | ctrl+r: refresh preview | ctrl+e: edit parent | esc: back | ctrl+c: quit"
		if m.templateCreate.parentEdit {
			hint = "type parent values | tab/shift+tab: switch field | enter: create parent | esc: stop editing | ctrl+c: quit"
		}
		if m.templateCreate.preview.NeedsParentCreate && !m.templateCreate.parentEdit {
			hint = "type source | p: toggle patch | enter: create proposed parent | ctrl+e: edit parent values | ctrl+r: refresh preview | esc: back | ctrl+c: quit"
		}
	}
	if m.templateCreate.mode == templateManagerModeRename {
		hint = "type new name | enter: rename | backspace: edit | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.mode == templateManagerModeDestroy {
		hint = "enter: destroy dataset | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.mode == templateManagerModeClone {
		hint = "type clone name | up/down: snapshot | enter: clone | ctrl+x: destroy snapshot | backspace: edit | ctrl+r: refresh snapshots | esc: back | ctrl+c: quit"
	}
	if m.templateCreate.mode == templateManagerModeSnapshotDestroy {
		hint = "up/down: snapshot | enter: destroy selected snapshot | ctrl+r: refresh snapshots | esc: back | ctrl+c: quit"
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
		case templateManagerModeSnapshotDestroy:
			hint = "Destroying template snapshot... please wait | ctrl+c: quit"
		default:
			hint = "Creating template dataset... please wait | ctrl+c: quit"
		}
	}
	footer := m.renderFooterWithMessage(hint, m.templateCreate.message, footerStyle)
	bodyHeight := m.pageBodyHeight(header, footer, 0)
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

	return lipgloss.JoinVertical(lipgloss.Left, header, "", body, footer)
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
			lines = append(lines, renderKeyValueLines(width, [2]string{"Derived dataset", preview.Dataset})...)
		}
		if preview.Mountpoint != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Target mountpoint", preview.Mountpoint})...)
		}
		lines = append(lines, renderKeyValueLines(width, [2]string{"Patch to latest level", yesNoText(preview.PatchSelected)})...)
		if preview.PatchRelease != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Patch release", preview.PatchRelease})...)
		}
		lines = append(lines, renderKeyValueLines(width, [2]string{"Readonly after create", "yes"})...)
		if preview.SourceKind != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Source type", preview.SourceKind})...)
		}
		if preview.ResolvedSource != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Resolved source", preview.ResolvedSource})...)
		}
		if preview.Action != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Create action", preview.Action})...)
		}
		if preview.NeedsParentCreate {
			appendWrappedText(&lines, width, "No templates parent dataset was discovered. Create the proposed parent or edit the values first.")
		}
		if strings.TrimSpace(preview.PatchNote) != "" {
			appendWrappedText(&lines, width, preview.PatchNote)
		}
		if preview.Err != nil {
			appendWrappedStyledText(&lines, width, wizardErrorStyle, "Error: "+preview.Err.Error())
		}
	case templateManagerModeRename:
		appendSection(&lines, width, "Rename template dataset")
		preview := m.templateCreate.renamePreview
		current := preview.Current
		if current.Name == "" {
			lines = append(lines, "No template dataset selected.")
		} else {
			lines = append(lines, renderKeyValueLines(width,
				[2]string{"Current dataset", current.Name},
				[2]string{"Current mountpoint", current.Mountpoint},
				[2]string{"Current readonly", yesNoText(current.Readonly)},
			)...)
			lines = append(lines, selectedRowStyle.Width(max(1, width)).Render(truncate("> New name: "+valueOrPlaceholder(m.templateCreate.renameInput, filepath.Base(current.Name)), width)))
			if preview.NewDataset != "" {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Renamed dataset", preview.NewDataset})...)
			}
			if preview.NewMountpoint != "" {
				lines = append(lines, renderKeyValueLines(width, [2]string{"New mountpoint", preview.NewMountpoint})...)
			}
			lines = append(lines, renderKeyValueLines(width, [2]string{"Readonly after rename", yesNoText(preview.ReadonlyPreserved)})...)
			if !current.Readonly {
				appendWrappedStyledText(&lines, width, wizardErrorStyle, "Warning: current template dataset is writable; handbook-style templates should be readonly.")
			}
			if len(preview.UpdatedWizardTemplates) > 0 {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Saved wizard templates updated on rename", strings.Join(preview.UpdatedWizardTemplates, ", ")})...)
			}
			if preview.Err != nil {
				appendWrappedStyledText(&lines, width, wizardErrorStyle, "Error: "+preview.Err.Error())
			}
		}
	case templateManagerModeDestroy:
		appendSection(&lines, width, "Destroy template dataset")
		preview := m.templateCreate.destroyPreview
		current := preview.Current
		if current.Name == "" {
			lines = append(lines, "No template dataset selected.")
		} else {
			lines = append(lines, renderKeyValueLines(width,
				[2]string{"Dataset", current.Name},
				[2]string{"Mountpoint", current.Mountpoint},
				[2]string{"Readonly", yesNoText(preview.Readonly)},
				[2]string{"Destroy scope", "zfs destroy -r " + preview.DestroyScope},
				[2]string{"Snapshots", fmt.Sprintf("%d", current.SnapshotCount)},
				[2]string{"Clone dependents", fmt.Sprintf("%d", len(current.CloneDependents))},
				[2]string{"Saved wizard template refs", fmt.Sprintf("%d", len(preview.ReferencedTemplates))},
			)...)
			if !preview.Readonly {
				appendWrappedStyledText(&lines, width, wizardErrorStyle, "Warning: this template dataset is writable; handbook-style templates should be readonly.")
			}
			if len(current.CloneDependents) > 0 {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Dependents", strings.Join(current.CloneDependents, ", ")})...)
			}
			if len(preview.ReferencedTemplates) > 0 {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Referenced by", strings.Join(preview.ReferencedTemplates, ", ")})...)
			}
			if preview.Err != nil {
				appendWrappedStyledText(&lines, width, wizardErrorStyle, "Error: "+preview.Err.Error())
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
			lines = append(lines, renderKeyValueLines(width, [2]string{"Snapshot", preview.Snapshot})...)
		}
		if preview.NewDataset != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Clone dataset", preview.NewDataset})...)
		}
		if preview.NewMountpoint != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Clone mountpoint", preview.NewMountpoint})...)
		}
		if preview.NewDataset != "" {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Readonly after clone", yesNoText(preview.ReadonlyAfter)})...)
		}
		if preview.Err != nil {
			appendWrappedStyledText(&lines, width, wizardErrorStyle, "Error: "+preview.Err.Error())
		}
	case templateManagerModeSnapshotDestroy:
		appendSection(&lines, width, "Destroy template snapshot")
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			lines = append(lines, "No template dataset selected.")
			break
		}
		lines = append(lines, truncate("Dataset: "+item.Name, width))
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
		preview := m.templateCreate.snapshotDestroyPreview
		if preview.Snapshot != "" {
			lines = append(lines, renderKeyValueLines(width,
				[2]string{"Snapshot", preview.Snapshot},
				[2]string{"Destroy command", "zfs destroy " + preview.Snapshot},
			)...)
		}
		if len(preview.ReferencedJails) > 0 {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Current jails", strings.Join(preview.ReferencedJails, ", ")})...)
		}
		if len(preview.ReferencedClones) > 0 {
			lines = append(lines, renderKeyValueLines(width, [2]string{"Clone dependents", strings.Join(preview.ReferencedClones, ", ")})...)
		}
		if preview.Err != nil {
			appendWrappedStyledText(&lines, width, wizardErrorStyle, "Error: "+preview.Err.Error())
		}
	default:
		appendSection(&lines, width, "Inspect")
		item, ok := m.templateCreate.selectedItem()
		if !ok {
			lines = append(lines, "No template dataset selected.")
		} else {
			lines = append(lines, renderKeyValueLines(width,
				[2]string{"Dataset", item.Name},
				[2]string{"Mountpoint", item.Mountpoint},
				[2]string{"Readonly", yesNoText(item.Readonly)},
				[2]string{"Used", item.Used + "  Avail: " + item.Avail},
				[2]string{"Refer", item.Refer + "  Compression: " + item.Compression},
				[2]string{"Quota", valueOrDash(item.Quota) + "  Reservation: " + valueOrDash(item.Reservation)},
			)...)
			if strings.TrimSpace(item.Origin) != "" && item.Origin != "-" {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Origin", item.Origin})...)
			}
			lines = append(lines, renderKeyValueLines(width,
				[2]string{"Snapshots", fmt.Sprintf("%d", item.SnapshotCount)},
				[2]string{"Child datasets", fmt.Sprintf("%d", len(item.ChildDatasets))},
				[2]string{"Clone dependents", fmt.Sprintf("%d", len(item.CloneDependents))},
				[2]string{"Saved wizard template refs", fmt.Sprintf("%d", len(item.WizardTemplateRefs))},
			)...)
			appendSection(&lines, width, "Safety")
			lines = append(lines, renderKeyValueLines(width,
				[2]string{"Rename allowed", yesNoText(item.RenameAllowed)},
				[2]string{"Destroy allowed", yesNoText(item.DestroyAllowed)},
			)...)
			if len(item.SafetyIssues) > 0 {
				for _, issue := range item.SafetyIssues {
					prefix := "Issue: "
					if strings.Contains(strings.ToLower(issue), "writable") {
						prefix = "Warning: "
					}
					appendWrappedStyledText(&lines, width, wizardErrorStyle, prefix+issue)
				}
			}
			if len(item.CloneDependents) > 0 {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Dependents", strings.Join(item.CloneDependents, ", ")})...)
			}
			if len(item.ChildDatasets) > 0 {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Child datasets", strings.Join(item.ChildDatasets, ", ")})...)
			}
			if len(item.WizardTemplateRefs) > 0 {
				lines = append(lines, renderKeyValueLines(width, [2]string{"Saved template refs", strings.Join(item.WizardTemplateRefs, ", ")})...)
			}
		}
	}
	if len(m.templateCreate.logs) > 0 {
		appendSection(&lines, width, "Execution output")
		for _, line := range m.templateCreate.logs {
			appendWrappedText(&lines, width, line)
		}
	}
	return lines
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ... skipping to wizardLines
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
		labels := make([]string, 0, len(m.wizard.templates))
		for _, template := range m.wizard.templates {
			labels = append(labels, template.Name)
		}
		previewLines := []string(nil)
		if template, ok := m.wizard.selectedTemplate(); ok {
			previewLines = []string{
				"Name: " + template.Name,
				"Destination: " + template.Values.Dataset,
				"Template/Release: " + template.Values.TemplateRelease,
				"IPv4: " + template.Values.IP4,
			}
		}
		return renderWizardSelectionList(width, "Load Template", labels, m.wizard.templateCursor, []string{"No templates available."}, "Selected Template Preview", previewLines)
	}

	if m.wizard.userlandMode {
		labels := make([]string, 0, len(m.wizard.userlandOpts))
		for _, option := range m.wizard.userlandOpts {
			labels = append(labels, option.Label)
		}
		previewLines := []string(nil)
		if option, ok := m.wizard.selectedUserlandOption(); ok {
			previewLines = []string{
				option.Value,
				"Tip: type a custom https URL directly in Template/Release for custom download.",
			}
		}
		return renderWizardSelectionList(width, "Select Userland Source", labels, m.wizard.userlandCursor, []string{"No userland options found."}, "Selected Value", previewLines)
	}

	if m.wizard.thinDatasetMode {
		labels := make([]string, 0, len(m.wizard.thinDatasetOpts))
		for _, option := range m.wizard.thinDatasetOpts {
			labels = append(labels, option.Label)
		}
		emptyLines := []string{
			"No thin template datasets found.",
			"",
			truncate("Press c to create a template dataset from the current Template/Release value.", width),
		}
		previewLines := []string(nil)
		if option, ok := m.wizard.selectedThinDatasetOption(); ok {
			previewLines = []string{
				option.Value,
				"Thin jails require an extracted template dataset mountpoint, not a release tag or archive.",
				"Press c to create a new template dataset from the current Template/Release value.",
			}
		}
		return renderWizardSelectionList(width, "Select Thin Template Dataset", labels, m.wizard.thinDatasetCursor, emptyLines, "Selected Value", previewLines)
	}

	if m.wizard.isConfirmationStep() {
		lines = append(lines, sectionStyle.Render("Summary"))
		for _, line := range m.wizard.summaryLines() {
			lines = append(lines, truncate(line, width))
		}
		if m.wizardApplying {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Status"))
			lines = append(lines, truncate("Creation in progress. Linux bootstrap and package installation can take a while.", width))
			lines = append(lines, truncate("Scroll for the generated jail.conf preview and any execution output.", width))
		}
		if shouldShowNetworkPrereqs(m.wizard.networkPrereqs) {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Network prerequisites"))
			for _, line := range networkWizardPrereqLines(m.wizard.networkPrereqs) {
				appendStyledWizardLine(&lines, line, width)
			}
		}
		if shouldShowRacctPrereqs(m.wizard.racctPrereqs) {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Resource limit prerequisites"))
			for _, line := range racctWizardPrereqLines(m.wizard.racctPrereqs) {
				appendStyledWizardLine(&lines, line, width)
			}
		}
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("jail.conf preview"))
		if m.wizard.showJailConfPreview {
			lines = append(lines, truncate("Press p to hide the generated jail.conf preview.", width))
			lines = append(lines, "")
			for _, line := range m.wizard.jailConfPreviewLines() {
				appendWrappedLiteralLine(&lines, line, width)
			}
		} else {
			lines = append(lines, truncate("Generated jail.conf is available for preview.", width))
			lines = append(lines, truncate("Press p to show it.", width))
		}
		if m.wizardApplying {
			if m.downloading && m.downloadTotal > 0 {
				percent := float64(m.downloadRead) / float64(m.downloadTotal)
				barWidth := width - 20
				if barWidth < 10 {
					barWidth = 10
				}
				filled := int(float64(barWidth) * percent)
				bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
				lines = append(lines, "")
				lines = append(lines, sectionStyle.Render("Downloading Archive"))
				lines = append(lines, fmt.Sprintf(" %s %3.0f%%", bar, percent*100))
				lines = append(lines, fmt.Sprintf(" %s / %s", formatBytes(m.downloadRead), formatBytes(m.downloadTotal)))
			}
		}

		if len(m.wizard.executionLogs) > 0 {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Execution output"))
			for _, line := range m.wizard.executionLogs {
				appendWrappedLiteralLine(&lines, line, width)
			}
		}
		if m.wizard.executionError != "" {
			lines = append(lines, "")
			lines = append(lines, sectionStyle.Render("Execution error"))
			for _, line := range wrapText(m.wizard.executionError, max(1, width)) {
				lines = append(lines, truncate(line, width))
			}
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

	if inlineHelp && shouldShowRacctPrereqs(m.wizard.racctPrereqs) && wizardShowsRacctPrereqs(m.wizard.currentStep()) {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Resource limit prerequisites"))
		for _, line := range racctWizardPrereqLines(m.wizard.racctPrereqs) {
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
	case "note":
		return wizardFieldGuide{
			Purpose: "Optional short description shown in dashboard quick details and the jail detail overview.",
			Format:  fmt.Sprintf("Up to %d characters; keep it short and role-focused.", maxJailNoteLen),
			Examples: []string{
				"nginx reverse proxy",
				"wordpress",
			},
			Notes: []string{
				"Leave blank when the jail name already says enough.",
			},
		}
	case "dataset":
		return wizardFieldGuide{
			Purpose: "Absolute jail root path. The final path must end with the jail name.",
			Format:  "Absolute path only. Shared roots like /usr/local/jails/containers are not valid jail roots by themselves.",
			Examples: []string{
				"/usr/local/jails/containers/web01",
				"/usr/local/jails/containers/db01",
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
	case "patch_base":
		return wizardFieldGuide{
			Purpose: "Controls whether the extracted FreeBSD jail base is patched to the latest level with freebsd-update before first start.",
			Format:  "auto, yes, or no. auto patches only official FreeBSD release tags and recognizable base.txz release archives.",
			Examples: []string{
				"auto to patch official release sources and skip custom roots",
				"yes to require patching for an official release source",
				"no to skip patching even for an official release source",
			},
			Notes: []string{
				"Thin jails do not use this field. Their base should be patched at the template dataset stage instead.",
			},
		}
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
			Notes: []string{
				"Do not enable spanning tree by default for a simple jail bridge. It is only useful when the bridge participates in a Layer 2 loop or multiple redundant paths, and enabling it adds forwarding delay.",
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
	case "vnet_host_setup":
		return wizardFieldGuide{
			Purpose: "Choose whether the host bridge/uplink setup is runtime-only or also persisted in rc.conf.",
			Format:  "runtime or persistent.",
			Examples: []string{
				"runtime to prepare the bridge only for the current host session",
				"persistent to manage rc.conf bridge settings before jail creation",
			},
			Notes: []string{
				"Persistent mode manages cloned_interfaces and bridge settings in rc.conf.",
				"Existing uplink interface settings are preserved instead of being overwritten.",
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
				"Each target path is cleaned before validation. After resolving . and .. segments, it still has to point somewhere under the jail root; paths that escape to locations like / or /etc are rejected.",
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
				"jammy",
				"bookworm",
				"trixie",
			},
			Notes: []string{
				"Bootstrap mode auto only proceeds when the host can verify that debootstrap supports the selected release.",
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
	case "interface", "bridge", "bridge_policy", "vnet_host_setup", "uplink", "ip4", "ip6", "default_router":
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

func wizardShowsRacctPrereqs(step wizardStep) bool {
	for _, field := range step.Fields {
		switch field.ID {
		case "cpu_percent", "memory_limit", "process_limit":
			return true
		}
	}
	return false
}

func shouldShowRacctPrereqs(prereqs RacctWizardPrereqs) bool {
	if !prereqs.HasLimits {
		return false
	}
	if !prereqs.Status.Enabled || prereqs.Status.ReadError != "" {
		return true
	}
	return false
}

func racctWizardPrereqLines(prereqs RacctWizardPrereqs) []string {
	if !prereqs.HasLimits {
		return []string{"No resource limits configured."}
	}
	lines := []string{}
	if prereqs.Status.ReadError != "" {
		lines = append(lines, "Warning: failed to inspect kern.racct.enable: "+prereqs.Status.ReadError)
	} else if !prereqs.Status.Enabled {
		if prereqs.Status.LoaderConfigured {
			lines = append(lines, "Warning: kern.racct.enable is configured but the system requires a manual reboot before rctl limits can be applied.")
		} else {
			lines = append(lines, "Warning: resource limits require kern.racct.enable=1 and a manual reboot before rctl limits can be applied.")
		}
	} else {
		lines = append(lines, "kern.racct.enable is active.")
	}
	return lines
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
		case "interface", "bridge", "vnet_host_setup", "uplink", "ip4", "ip6", "default_router":
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
		*lines = append(*lines, wizardWarningStyle.Render(truncate(text, width)))
		return
	} else if looksLikeErrorText(text) {
		*lines = append(*lines, wizardErrorStyle.Render(truncate(text, width)))
		return
	}
	*lines = append(*lines, truncate(text, width))
}

func appendWrappedStyledWizardLine(lines *[]string, text string, width int) {
	for _, line := range wrapText(text, max(8, width)) {
		if looksLikeWarningText(text) {
			*lines = append(*lines, wizardWarningStyle.Render(line))
			continue
		} else if looksLikeErrorText(text) {
			*lines = append(*lines, wizardErrorStyle.Render(line))
			continue
		}
		*lines = append(*lines, line)
	}
}

func appendWrappedLiteralLine(lines *[]string, text string, width int) {
	if width <= 0 {
		*lines = append(*lines, "")
		return
	}
	indentLen := len(text) - len(strings.TrimLeft(text, " "))
	indent := strings.Repeat(" ", indentLen)
	content := strings.TrimLeft(text, " ")
	contentWidth := max(8, width-indentLen)
	wrapped := wrapText(content, contentWidth)
	if len(wrapped) == 0 {
		*lines = append(*lines, truncate(text, width))
		return
	}
	for _, line := range wrapped {
		*lines = append(*lines, truncate(indent+line, width))
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
	switch prereqs.ReleaseSupport {
	case "supported":
		lines = append(lines, "Bootstrap release support: verified")
	case "unsupported":
		lines = append(lines, "Warning: bootstrap release support: unsupported")
	case "unknown":
		lines = append(lines, "Warning: bootstrap release support could not be verified early")
	}
	if prereqs.ReleaseSupportMsg != "" {
		lines = append(lines, prereqs.ReleaseSupportMsg)
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
	switch prereqs.ReleaseSupport {
	case "supported":
		lines = append(lines, "Bootstrap release support: verified")
	case "unsupported":
		lines = append(lines, "Warning: bootstrap release is not supported by host debootstrap")
	case "unknown":
		lines = append(lines, "Warning: bootstrap release support could not be verified early")
	}
	if prereqs.ReleaseSupportMsg != "" {
		lines = append(lines, prereqs.ReleaseSupportMsg)
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

	appendRenderedSectionWithStyle(&lines, detailSectionStyle, "Overview", renderKeyValueLines(width,
		[2]string{"Name", m.detail.Name},
		[2]string{"State", state},
		[2]string{"Type", valueOrDash(jail.Type)},
		[2]string{"JID", jidText},
		[2]string{"CPU", cpuText},
		[2]string{"Memory", memText},
		[2]string{"Path", m.detail.Path},
		[2]string{"Hostname", m.detail.Hostname},
	))
	lines = append(lines, renderKeyValueLinesWithValueFallback(width, "",
		[2]string{"Note", m.detail.Note},
	)...)

	if m.detailNoteMode || m.detailNoteSaving {
		appendSectionWithStyle(&lines, width, detailSectionStyle, "Note editor")
		lines = append(lines, renderKeyValueLinesWithValueFallback(width, "",
			[2]string{"Note", m.detailNoteInput},
			[2]string{"Length", fmt.Sprintf("%d/%d", jailNoteLength(m.detailNoteInput), maxJailNoteLen)},
		)...)
		lines = append(lines, truncate("Press enter to save, esc to cancel, and leave the field blank to clear the note.", width))
	}

	appendSectionWithStyle(&lines, width, detailSectionStyle, "Configured state")
	lines = append(lines, renderKeyValueLines(width, [2]string{"Source", m.detail.JailConfSource})...)
	if len(m.detail.JailConfValues) == 0 && len(m.detail.JailConfFlags) == 0 {
		lines = append(lines, truncate("No matching jail block found.", width))
	} else {
		for _, key := range sortedKeys(m.detail.JailConfValues) {
			lines = append(lines, renderKeyValueLines(width, [2]string{key, m.detailDisplayConfigValue(m.detail.JailConfValues[key])})...)
		}
		for _, flag := range m.detail.JailConfFlags {
			lines = append(lines, renderKeyValueLines(width, [2]string{flag, "enabled"})...)
		}
	}

	appendSectionWithStyle(&lines, width, detailSectionStyle, "Startup policy")
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

	appendRenderedSectionWithStyle(&lines, detailSectionStyle, "Runtime state", renderKeyValueLines(width,
		[2]string{"State", state},
		[2]string{"CPU", cpuText},
		[2]string{"Memory", memText},
	))
	runtimeNotes := []string{
		"Runtime values come from the running jail and may differ from jail.conf defaults or the configured state shown above.",
	}
	if len(m.detail.RuntimeValues) == 0 {
		runtimeNotes = append(runtimeNotes, "No running runtime record is available for this jail.")
	} else {
		for _, key := range orderedRuntimeKeys(m.detail.RuntimeValues) {
			lines = append(lines, renderKeyValueLines(width, [2]string{key, m.detail.RuntimeValues[key]})...)
		}
	}
	if m.detailShowAdvanced {
		runtimeNotes = append(runtimeNotes, "Advanced runtime/default parameters are shown below; press a to hide them.")
	} else {
		runtimeNotes = append(runtimeNotes, "Advanced runtime/default parameters are hidden; press a to show them.")
	}

	if m.detail.NetworkSummary != nil {
		appendSectionWithStyle(&lines, width, detailSectionStyle, "Network summary")
		networkLabelWidth := 25
		if width < 72 {
			networkLabelWidth = 20
		}
		networkPairs := make([][2]string, 0, len(m.detail.NetworkSummary.Configured)+len(m.detail.NetworkSummary.Runtime))
		for _, key := range orderedNetworkSummaryKeys(m.detail.NetworkSummary.Configured) {
			networkPairs = append(networkPairs, [2]string{key, m.detail.NetworkSummary.Configured[key]})
		}
		for _, key := range orderedNetworkSummaryKeys(m.detail.NetworkSummary.Runtime) {
			networkPairs = append(networkPairs, [2]string{key, m.detail.NetworkSummary.Runtime[key]})
		}
		if len(networkPairs) > 0 {
			lines = append(lines, renderKeyValueLinesWithLabelWidth(width, networkLabelWidth, networkPairs...)...)
		}
		if len(m.detail.NetworkSummary.Validation) > 0 {
			for _, line := range m.detail.NetworkSummary.Validation {
				if looksLikeWarningText(line) {
					lines = append(lines, wizardWarningStyle.Render(truncate(line, max(1, width))))
					continue
				} else if looksLikeErrorText(line) {
					lines = append(lines, wizardErrorStyle.Render(truncate(line, max(1, width))))
					continue
				}
				lines = append(lines, renderInformationalKeyValueWithLabelWidth(width, networkLabelWidth, line)...)
			}
		}
	}

	appendSectionWithStyle(&lines, width, detailSectionStyle, "ZFS dataset")
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

	appendSectionWithStyle(&lines, width, detailSectionStyle, "rctl")
	if m.detail.RctlConfig != nil {
		lines = append(lines, renderKeyValueLines(width,
			[2]string{"Limit mode", valueOrDash(m.detail.RctlConfig.Mode)},
			[2]string{"Configured CPU %", valueOrDash(m.detail.RctlConfig.CPUPercent)},
			[2]string{"Configured memory", valueOrDash(m.detail.RctlConfig.MemoryLimit)},
			[2]string{"Configured max processes", valueOrDash(m.detail.RctlConfig.ProcessLimit)},
			[2]string{"Persistent block in /etc/rctl.conf", yesNoText(m.detail.RctlConfig.Persistent)},
		)...)
		if m.detail.RctlConfig.PersistentErr != "" {
			lines = append(lines, wizardErrorStyle.Render(truncate("rctl.conf check: "+m.detail.RctlConfig.PersistentErr, width)))
		}
	}
	if m.detail.RacctStatus != nil {
		lines = append(lines, renderKeyValueLines(width,
			[2]string{"kern.racct.enable", valueOrDash(m.detail.RacctStatus.EffectiveValue)},
			[2]string{"loader.conf configured", yesNoText(m.detail.RacctStatus.LoaderConfigured)},
		)...)
		if m.detail.RacctStatus.ReadError != "" {
			lines = append(lines, wizardErrorStyle.Render(truncate("racct check: "+m.detail.RacctStatus.ReadError, width)))
		}
	}
	if len(m.detail.RctlRules) == 0 {
		if m.detail.RctlConfig != nil && m.detail.RctlConfig.Mode == "runtime" {
			lines = append(lines, truncate("No live rctl rules. Runtime-only limits apply only while the jail is running.", width))
		} else {
			lines = append(lines, truncate("No matching rctl rules.", width))
		}
	} else {
		for _, rule := range m.detail.RctlRules {
			lines = append(lines, truncate(rule, width))
		}
	}

	if m.detail.LinuxReadiness != nil {
		appendSectionWithStyle(&lines, width, detailSectionStyle, "Linux readiness")
		for _, line := range m.linuxReadinessLines() {
			if looksLikeWarningText(line) {
				lines = append(lines, wizardWarningStyle.Render(truncate(line, max(1, width))))
				continue
			} else if looksLikeErrorText(line) || strings.HasPrefix(strings.ToLower(line), "readiness issue:") {
				lines = append(lines, wizardErrorStyle.Render(truncate(line, max(1, width))))
				continue
			}
			lines = append(lines, truncate(line, width))
		}
	}

	if len(m.detail.SourceErrors) > 0 {
		appendSectionWithStyle(&lines, width, detailSectionStyle, "Source errors")
		for _, source := range sortedKeys(m.detail.SourceErrors) {
			lines = append(lines, wizardErrorStyle.Render(truncate(fmt.Sprintf("%s: %s", source, m.detail.SourceErrors[source]), width)))
		}
	}
	if len(runtimeNotes) > 0 {
		appendSectionWithStyle(&lines, width, detailSectionStyle, "Runtime notes")
		for _, line := range runtimeNotes {
			lines = append(lines, truncate(line, width))
		}
	}
	if m.detailShowAdvanced {
		appendSectionWithStyle(&lines, width, detailSectionStyle, "Advanced runtime parameters")
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
		fmt.Sprintf("Bootstrap release: %s", valueOrDash(readiness.BootstrapRelease)),
		fmt.Sprintf("Compat root: %s", valueOrDash(readiness.CompatRoot)),
		fmt.Sprintf("Bootstrap mode: %s", valueOrDash(readiness.BootstrapMode)),
		fmt.Sprintf("Mirror URL: %s", valueOrDash(readiness.MirrorURL)),
		fmt.Sprintf("Mirror host: %s", valueOrDash(readiness.MirrorHost)),
		fmt.Sprintf("Preflight URL: %s", valueOrDash(readiness.PreflightURL)),
		fmt.Sprintf("Linux userland present: %s", yesNoText(readiness.UserlandPresent)),
	}
	switch readiness.ReleaseSupport {
	case "supported":
		lines = append(lines, "Bootstrap release support: verified")
	case "unsupported":
		lines = append(lines, "Warning: bootstrap release support: unsupported")
	case "unknown":
		lines = append(lines, "Warning: bootstrap release support could not be verified early")
	}
	if readiness.ReleaseSupportDetail != "" {
		lines = append(lines, readiness.ReleaseSupportDetail)
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
	if readiness.RuntimeChecked && readiness.Host.ServicePresent && readiness.Host.ServiceStatusErr != "" && !readiness.Host.ServiceRunning {
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

func (m model) detailDisplayConfigValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	name := strings.TrimSpace(m.detail.Name)
	if name == "" {
		return value
	}
	value = strings.ReplaceAll(value, "${name}", name)
	value = strings.ReplaceAll(value, "$name", name)
	return value
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

func (m model) selectedJailList() []Jail {
	result := make([]Jail, 0, len(m.selectedJails))
	for _, jail := range m.snapshot.Jails {
		if _, ok := m.selectedJails[jail.Name]; ok {
			result = append(result, jail)
		}
	}
	return result
}

func newTemplateDatasetCreateState(sourceInput string, status initialConfigStatus, returnMode screenMode, selectMode bool) templateDatasetCreateState {
	state := templateDatasetCreateState{
		returnMode:  returnMode,
		selectMode:  selectMode,
		sourceInput: strings.TrimSpace(sourceInput),
		patchBase:   "auto",
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
	s.preview = InspectTemplateDatasetCreateWithParent(s.sourceInput, s.parentOverride(), s.patchBase)
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

func (s *templateDatasetCreateState) refreshSnapshotDestroyPreview() {
	item, ok := s.selectedItem()
	if !ok {
		s.snapshotDestroyPreview = TemplateDatasetSnapshotDestroyPreview{}
		return
	}
	s.boundCloneCursor()
	snapshot, ok := s.selectedCloneSnapshot()
	if !ok {
		s.snapshotDestroyPreview = TemplateDatasetSnapshotDestroyPreview{
			Current: item,
			Err:     fmt.Errorf("select a snapshot to destroy"),
		}
		return
	}
	s.snapshotDestroyPreview = InspectTemplateSnapshotDestroy(item.Name, snapshot.Name, s.parentOverride())
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
		return
	}
	if s.mode == templateManagerModeSnapshotDestroy {
		s.refreshSnapshotDestroyPreview()
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
	return headerBarStyle.Width(m.width).Render(title + "  " + summary)
}

func (m model) renderFooter() string {
	hint := "j/k: navigate | enter/d: detail | c: create | s: start/stop | R: restart | u: upgrade | space: select | ctrl+a: all | z: ZFS | x: destroy | t: templates | i: config | h: help | r: refresh | q: quit"
	footerRenderer := footerStyle
	message := m.notice
	if m.err != nil {
		message = "warning: " + m.err.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	}
	if len(m.selectedJails) > 0 && m.err == nil {
		selMsg := fmt.Sprintf("%d selected | s: start/stop | R: restart | esc: clear selection", len(m.selectedJails))
		if m.notice != "" {
			selMsg += " | " + m.notice
		}
		message = selMsg
	}
	return m.renderFooterWithMessage(hint, message, footerRenderer)
}

func (m model) renderFooterWithMessage(hint, message string, footerRenderer lipgloss.Style) string {
	lines := make([]string, 0, 4)
	width := max(1, m.width)

	message = strings.TrimSpace(message)
	if message != "" {
		prefixed := ">> " + message
		renderLine := wizardMessageRenderer(message)
		for _, line := range wrapText(prefixed, max(8, width-2)) {
			lines = append(lines, renderLine(line))
		}
	}

	lines = append(lines, footerRenderer.Width(width).Render(hint))
	return strings.Join(lines, "\n")
}

func (m model) pageBodyHeight(header, footer string, verticalPadding int) int {
	height := m.height - lipgloss.Height(header) - lipgloss.Height(footer) - 1 - verticalPadding*2
	if height < 1 {
		return 1
	}
	return height
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
		sel := " "
		if _, ok := m.selectedJails[jail.Name]; ok {
			sel = "*"
		}
		cursorChar := " "
		if idx == m.cursor {
			cursorChar = ">"
		}
		line := fmt.Sprintf("%s%s %s %s", cursorChar, sel, statusBadge(jail.Running), jail.Name)
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
			[2]string{"Type", valueOrDash(j.Type)},
			[2]string{"Hostname", valueOrDash(j.Hostname)},
			[2]string{"JID", jidText},
			[2]string{"CPU", fmt.Sprintf("%.2f%%", j.CPUPercent)},
			[2]string{"Memory", fmt.Sprintf("%dMB", j.MemoryMB)},
		)...)
		lines = append(lines, renderKeyValueLinesWithValueFallback(max(12, width-2), "(no notes)",
			[2]string{"Note", j.Note},
		)...)
		if strings.TrimSpace(j.QuotaUsage) != "" {
			lines = append(lines, renderKeyValueLines(max(12, width-2),
				[2]string{"Quota", j.QuotaUsage},
			)...)
		}
	}

	return lipgloss.NewStyle().
		Width(width).
		Height(height).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))
}

func (m *model) appendDetailNoteInput(text string) {
	if text == "" {
		return
	}
	for _, r := range text {
		if r == '\n' || r == '\r' {
			continue
		}
		if jailNoteLength(m.detailNoteInput) >= maxJailNoteLen {
			break
		}
		m.detailNoteInput += string(r)
	}
	m.detailErr = nil
	m.detailNotice = ""
}

func (m *model) backspaceDetailNoteInput() {
	runes := []rune(m.detailNoteInput)
	if len(runes) == 0 {
		return
	}
	m.detailNoteInput = string(runes[:len(runes)-1])
	m.detailErr = nil
	m.detailNotice = ""
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
	return wizardMessageRenderer(message)(message)
}

func wizardMessageRenderer(message string) func(string) string {
	lower := strings.ToLower(message)
	if strings.Contains(lower, "applying") ||
		strings.Contains(lower, "creating") ||
		strings.Contains(lower, "refreshing") ||
		strings.Contains(lower, "retrying") ||
		strings.Contains(lower, "rolling back") ||
		strings.Contains(lower, "loading detail") {
		return func(text string) string { return wizardActionStyle.Render(text) }
	}
	if looksLikeWarningText(message) {
		return func(text string) string { return wizardWarningStyle.Render(text) }
	}
	return func(text string) string { return summaryStyle.Render(text) }
}

func summarizeCreationWarning(message string) string {
	trimmed := strings.TrimSpace(message)
	lower := strings.ToLower(trimmed)
	switch {
	case strings.HasPrefix(lower, "linux bootstrap skipped"):
		return "linux bootstrap skipped; use detail view action 'b' after networking is ready"
	case strings.Contains(lower, "linux bootstrap preflight failed"):
		return "linux bootstrap preflight failed; use detail view action 'b' after fixing networking"
	case strings.Contains(lower, "does not support release"):
		if release := firstQuotedValue(trimmed); release != "" {
			return fmt.Sprintf("linux bootstrap failed; debootstrap does not support release %q on this host", release)
		}
		return "linux bootstrap failed; debootstrap on this host does not support the selected release"
	case strings.Contains(lower, "failed to bootstrap") || strings.Contains(lower, "failed to install debootstrap"):
		return "linux bootstrap failed; use detail view action 'b' after fixing package access"
	default:
		return trimmed
	}
}

func firstQuotedValue(text string) string {
	start := strings.IndexByte(text, '"')
	if start < 0 {
		return ""
	}
	rest := text[start+1:]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:end])
}

func looksLikeWarningText(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "warning")
}

func looksLikeErrorText(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "failed") ||
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
	appendSectionWithStyle(lines, width, sectionStyle, title, body...)
}

func appendSectionWithStyle(lines *[]string, width int, style lipgloss.Style, title string, body ...string) {
	if len(*lines) > 0 && (*lines)[len(*lines)-1] != "" {
		*lines = append(*lines, "")
	}
	*lines = append(*lines, style.Render(title))
	for _, line := range body {
		*lines = append(*lines, truncate(line, width))
	}
}

func appendRenderedSection(lines *[]string, title string, body []string) {
	appendRenderedSectionWithStyle(lines, sectionStyle, title, body)
}

func appendRenderedSectionWithStyle(lines *[]string, style lipgloss.Style, title string, body []string) {
	if len(*lines) > 0 && (*lines)[len(*lines)-1] != "" {
		*lines = append(*lines, "")
	}
	*lines = append(*lines, style.Render(title))
	*lines = append(*lines, body...)
}

func appendWrappedText(lines *[]string, width int, text string) {
	for _, line := range wrapText(text, max(8, width)) {
		*lines = append(*lines, line)
	}
}

func appendWrappedStyledText(lines *[]string, width int, style lipgloss.Style, text string) {
	for _, line := range wrapText(text, max(8, width)) {
		*lines = append(*lines, style.Render(line))
	}
}

func renderKeyValueLines(width int, pairs ...[2]string) []string {
	return renderKeyValueLinesWithValueFallback(width, "-", pairs...)
}

func renderKeyValueLinesWithValueFallback(width int, blankFallback string, pairs ...[2]string) []string {
	labelWidth := 25
	if width < 72 {
		labelWidth = 20
	}
	return renderKeyValueLinesWithLabelWidthAndFallback(width, labelWidth, blankFallback, pairs...)
}

func renderKeyValueLinesWithLabelWidth(width, labelWidth int, pairs ...[2]string) []string {
	return renderKeyValueLinesWithLabelWidthAndFallback(width, labelWidth, "-", pairs...)
}

func renderKeyValueLinesWithLabelWidthAndFallback(width, labelWidth int, blankFallback string, pairs ...[2]string) []string {
	lines := make([]string, 0, len(pairs)*2)
	for _, pair := range pairs {
		lines = append(lines, renderKeyValueWithFallback(width, labelWidth, pair[0], pair[1], blankFallback)...)
	}
	return lines
}

func renderKeyValue(width, labelWidth int, label, value string) []string {
	return renderKeyValueWithFallback(width, labelWidth, label, value, "-")
}

func renderKeyValueWithFallback(width, labelWidth int, label, value, blankFallback string) []string {
	label = strings.TrimSpace(label)
	if label == "" {
		return []string{truncate(valueOrPlaceholder(value, blankFallback), width)}
	}
	if labelWidth < 8 {
		labelWidth = 8
	}
	valueWidth := max(8, width-labelWidth-2)
	wrapped := wrapText(valueOrPlaceholder(value, blankFallback), valueWidth)
	prefix := fmt.Sprintf("%-*s", labelWidth, label+":")
	lines := make([]string, 0, len(wrapped))
	if len(wrapped) == 0 {
		return []string{detailKeyStyle.Render(prefix) + " " + blankFallback}
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
