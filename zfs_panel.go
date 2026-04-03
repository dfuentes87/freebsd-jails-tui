package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type ZFSSnapshot struct {
	Name     string
	Creation string
	Used     string
}

type zfsSnapshotListMsg struct {
	snapshots []ZFSSnapshot
	err       error
	message   string
}

type zfsActionMsg struct {
	logs    []string
	err     error
	message string
}

type zfsPanelState struct {
	returnMode       screenMode
	dataset          string
	sourceDetail     JailDetail
	snapshots        []ZFSSnapshot
	cursor           int
	offset           int
	loading          bool
	actionRunning    bool
	inputMode        bool
	inputValue       string
	confirmRollback  bool
	rollbackTarget   string
	cloneMode        bool
	cloneField       int
	cloneName        string
	cloneDestination string
	cloneWriteConfig bool
	clonePreview     JailSnapshotClonePreview
	message          string
	logs             []string
	err              error
}

func newZFSPanelState(dataset string, returnMode screenMode, sourceDetail JailDetail) zfsPanelState {
	panel := zfsPanelState{
		returnMode:       returnMode,
		dataset:          strings.TrimSpace(dataset),
		sourceDetail:     sourceDetail,
		loading:          true,
		cloneWriteConfig: true,
		message:          "Loading snapshots...",
	}
	return panel
}

func listZFSSnapshotsCmd(dataset string) tea.Cmd {
	return func() tea.Msg {
		snapshots, err := listZFSSnapshots(dataset)
		msg := fmt.Sprintf("Loaded %d snapshots.", len(snapshots))
		if err != nil {
			msg = "Failed to load snapshots."
		}
		return zfsSnapshotListMsg{snapshots: snapshots, err: err, message: msg}
	}
}

func createZFSSnapshotCmd(dataset, snapshotName string) tea.Cmd {
	return func() tea.Msg {
		fullName := dataset + "@" + snapshotName
		logs, err := runZFSCommand("zfs", "snapshot", fullName)
		message := "Snapshot created: " + fullName
		if err != nil {
			message = "Snapshot create failed."
		}
		return zfsActionMsg{logs: logs, err: err, message: message}
	}
}

func rollbackZFSSnapshotCmd(snapshot string) tea.Cmd {
	return func() tea.Msg {
		logs, err := runZFSCommand("zfs", "rollback", "-r", snapshot)
		message := "Rollback completed: " + snapshot
		if err != nil {
			message = "Rollback failed."
		}
		return zfsActionMsg{logs: logs, err: err, message: message}
	}
}

func runZFSCommand(name string, args ...string) ([]string, error) {
	logs := []string{"$ " + name + " " + strings.Join(args, " ")}
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	trimmed := strings.TrimSpace(string(out))
	if trimmed != "" {
		for _, line := range strings.Split(trimmed, "\n") {
			logs = append(logs, "  "+line)
		}
	}
	if err != nil {
		return logs, fmt.Errorf("%s failed: %w", logs[0], err)
	}
	return logs, nil
}

func listZFSSnapshots(dataset string) ([]ZFSSnapshot, error) {
	cmd := exec.Command("zfs", "list", "-H", "-t", "snapshot", "-o", "name,creation,used", "-s", "creation", "-r", dataset)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots for %s: %w", dataset, err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	snapshots := make([]ZFSSnapshot, 0, len(lines))
	prefix := dataset + "@"
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			continue
		}
		name := strings.TrimSpace(fields[0])
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		snapshots = append(snapshots, ZFSSnapshot{
			Name:     name,
			Creation: strings.TrimSpace(fields[1]),
			Used:     strings.TrimSpace(fields[2]),
		})
	}
	return snapshots, nil
}

func (m model) updateZFSPanelKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.zfsPanel.inputMode {
		switch msg.String() {
		case "esc", "left":
			if m.zfsPanel.actionRunning {
				return m, nil
			}
			m.zfsPanel.inputMode = false
			m.zfsPanel.inputValue = ""
			m.zfsPanel.message = "Snapshot creation canceled."
			return m, nil
		case "enter":
			if m.zfsPanel.actionRunning {
				return m, nil
			}
			name := strings.TrimSpace(m.zfsPanel.inputValue)
			if name == "" || strings.Contains(name, "@") || strings.ContainsAny(name, " \t") {
				m.zfsPanel.message = "Invalid snapshot name."
				return m, nil
			}
			m.zfsPanel.inputMode = false
			m.zfsPanel.actionRunning = true
			m.zfsPanel.logs = nil
			m.zfsPanel.err = nil
			m.zfsPanel.message = "Creating snapshot..."
			return m, createZFSSnapshotCmd(m.zfsPanel.dataset, name)
		case "backspace", "delete":
			runes := []rune(m.zfsPanel.inputValue)
			if len(runes) == 0 {
				return m, nil
			}
			m.zfsPanel.inputValue = string(runes[:len(runes)-1])
			return m, nil
		}
		if msg.Type == tea.KeyRunes {
			m.zfsPanel.inputValue += string(msg.Runes)
		}
		return m, nil
	}

	if m.zfsPanel.cloneMode {
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		switch msg.String() {
		case "esc", "left":
			m.zfsPanel.cloneMode = false
			m.zfsPanel.message = "Jail snapshot clone canceled."
			return m, nil
		case "tab", "down":
			m.zfsPanel.cloneField++
			if m.zfsPanel.cloneField > 1 {
				m.zfsPanel.cloneField = 0
			}
			return m, nil
		case "shift+tab", "up":
			m.zfsPanel.cloneField--
			if m.zfsPanel.cloneField < 0 {
				m.zfsPanel.cloneField = 1
			}
			return m, nil
		case "j":
			m.zfsPanel.cursor++
			m.zfsPanel.boundCursor(m.zfsListHeight())
			m.zfsPanel.refreshClonePreview()
			return m, nil
		case "k":
			m.zfsPanel.cursor--
			m.zfsPanel.boundCursor(m.zfsListHeight())
			m.zfsPanel.refreshClonePreview()
			return m, nil
		case "g", "home":
			m.zfsPanel.cursor = 0
			m.zfsPanel.boundCursor(m.zfsListHeight())
			m.zfsPanel.refreshClonePreview()
			return m, nil
		case "G", "end":
			m.zfsPanel.cursor = len(m.zfsPanel.snapshots) - 1
			m.zfsPanel.boundCursor(m.zfsListHeight())
			m.zfsPanel.refreshClonePreview()
			return m, nil
		case "t":
			m.zfsPanel.cloneWriteConfig = !m.zfsPanel.cloneWriteConfig
			m.zfsPanel.refreshClonePreview()
			return m, nil
		case "enter":
			if m.zfsPanel.clonePreview.Err != nil {
				m.zfsPanel.message = m.zfsPanel.clonePreview.Err.Error()
				return m, nil
			}
			m.zfsPanel.actionRunning = true
			m.zfsPanel.logs = nil
			m.zfsPanel.err = nil
			m.zfsPanel.message = "Cloning jail snapshot..."
			return m, jailSnapshotCloneCmd(m.zfsPanel.sourceDetail, m.zfsPanel.clonePreview.Snapshot, m.zfsPanel.cloneName, m.zfsPanel.cloneDestination, m.zfsPanel.cloneWriteConfig)
		case "backspace", "delete":
			m.zfsPanel.backspaceCloneField()
			return m, nil
		}
		if msg.Type == tea.KeyRunes {
			m.zfsPanel.appendCloneField(string(msg.Runes))
		}
		return m, nil
	}

	switch msg.String() {
	case "esc", "left":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		if m.zfsPanel.confirmRollback {
			m.zfsPanel.confirmRollback = false
			m.zfsPanel.rollbackTarget = ""
			m.zfsPanel.message = "Rollback canceled."
			return m, nil
		}
		m.mode = m.zfsPanel.returnMode
		return m, nil
	case "x", "X":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.loading = true
		m.zfsPanel.message = "Refreshing snapshots..."
		return m, listZFSSnapshotsCmd(m.zfsPanel.dataset)
	case "c", "C":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.inputMode = true
		m.zfsPanel.confirmRollback = false
		m.zfsPanel.inputValue = time.Now().Format("20060102-150405")
		m.zfsPanel.message = "Enter snapshot name and press enter."
		return m, nil
	case "r":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		snapshot, ok := m.zfsPanel.selectedSnapshot()
		if !ok {
			m.zfsPanel.message = "No snapshot selected."
			return m, nil
		}
		m.zfsPanel.confirmRollback = true
		m.zfsPanel.rollbackTarget = snapshot.Name
		m.zfsPanel.message = "Press enter to confirm rollback to selected snapshot."
		return m, nil
	case "n", "N":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		snapshot, ok := m.zfsPanel.selectedSnapshot()
		if !ok {
			m.zfsPanel.message = "No snapshot selected."
			return m, nil
		}
		baseName := strings.TrimSpace(m.zfsPanel.sourceDetail.Name)
		if baseName == "" {
			baseName = "cloned-jail"
		}
		parentDir := filepath.Dir(strings.TrimSpace(m.zfsPanel.sourceDetail.Path))
		if parentDir == "." || parentDir == "" {
			parentDir = "/usr/local/jails/containers"
		}
		m.zfsPanel.cloneMode = true
		m.zfsPanel.cloneField = 0
		m.zfsPanel.cloneName = baseName + "-clone"
		m.zfsPanel.cloneDestination = filepath.Join(parentDir, m.zfsPanel.cloneName)
		m.zfsPanel.cloneWriteConfig = true
		m.zfsPanel.clonePreview = InspectJailSnapshotClone(m.zfsPanel.sourceDetail, snapshot.Name, m.zfsPanel.cloneName, m.zfsPanel.cloneDestination, m.zfsPanel.cloneWriteConfig)
		m.zfsPanel.message = "Review the jail snapshot clone preview, then press enter to clone it."
		return m, nil
	case "enter":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		if m.zfsPanel.confirmRollback {
			target := m.zfsPanel.rollbackTarget
			if target == "" {
				m.zfsPanel.message = "No rollback target selected."
				m.zfsPanel.confirmRollback = false
				return m, nil
			}
			m.zfsPanel.confirmRollback = false
			m.zfsPanel.actionRunning = true
			m.zfsPanel.logs = nil
			m.zfsPanel.err = nil
			m.zfsPanel.message = "Rolling back snapshot..."
			return m, rollbackZFSSnapshotCmd(target)
		}
	case "j", "down":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.cursor++
	case "k", "up":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.cursor--
	case "g", "home":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.cursor = 0
	case "G", "end":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.cursor = len(m.zfsPanel.snapshots) - 1
	case "pgdown":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.cursor += m.zfsListHeight()
	case "pgup":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.cursor -= m.zfsListHeight()
	}

	m.zfsPanel.boundCursor(m.zfsListHeight())
	return m, nil
}

func (m model) renderZFSPanelView() string {
	title := titleStyle.Render("ZFS Integration Panel")
	meta := summaryStyle.Render("Dataset: " + valueOrDash(m.zfsPanel.dataset))
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := max(5, m.height-4)
	lines := m.zfsPanelLines(max(12, m.width-2), bodyHeight)
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))

	hint := "j/k: select snapshot | c: create snapshot | r: rollback selected | n: clone as jail | x: refresh | esc: back | q: quit"
	if m.zfsPanel.inputMode {
		hint = "Type snapshot name | enter: create snapshot | backspace: edit | esc: cancel"
	}
	if m.zfsPanel.confirmRollback {
		hint = "Rollback confirmation pending: enter to confirm | esc to cancel"
	}
	if m.zfsPanel.cloneMode {
		hint = "type name/destination | tab/shift+tab: field | j/k: snapshot | t: toggle config | enter: clone | esc: cancel | ctrl+c: quit"
	}
	if m.zfsPanel.actionRunning {
		hint = "Executing ZFS action... please wait | ctrl+c: quit"
	}
	footerRenderer := footerStyle
	message := m.zfsPanel.message
	if m.zfsPanel.err != nil {
		message = "error: " + m.zfsPanel.err.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	} else if looksLikeWarningText(m.zfsPanel.message) {
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	}
	footer := m.renderFooterWithMessage(hint, message, footerRenderer)

	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) zfsPanelLines(width, height int) []string {
	lines := []string{sectionStyle.Render("Snapshots")}

	listRows := min(max(3, height-12), 12)
	if len(m.zfsPanel.snapshots) == 0 {
		lines = append(lines, "No snapshots found for dataset.")
	} else {
		start := m.zfsPanel.offset
		end := min(len(m.zfsPanel.snapshots), start+listRows)
		for idx := start; idx < end; idx++ {
			snapshot := m.zfsPanel.snapshots[idx]
			prefix := " "
			if idx == m.zfsPanel.cursor {
				prefix = ">"
			}
			row := fmt.Sprintf(
				"%s %-24s created:%-18s used:%s",
				prefix,
				truncate(snapshotShortName(snapshot.Name), 24),
				truncate(snapshot.Creation, 18),
				snapshot.Used,
			)
			row = truncate(row, width)
			if idx == m.zfsPanel.cursor {
				row = selectedRowStyle.Width(max(1, width)).Render(row)
			}
			lines = append(lines, row)
		}
	}

	if snapshot, ok := m.zfsPanel.selectedSnapshot(); ok {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Selected snapshot"))
		lines = append(lines, truncate("Name: "+snapshot.Name, width))
		lines = append(lines, truncate("Created: "+valueOrDash(snapshot.Creation), width))
		lines = append(lines, truncate("Used: "+valueOrDash(snapshot.Used), width))

		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Rollback implications"))
		for _, line := range m.zfsRollbackImplicationLines(width, snapshot) {
			lines = append(lines, line)
		}
	}

	lines = append(lines, "")
	lines = append(lines, sectionStyle.Render("Actions"))
	lines = append(lines, "c: create snapshot")
	lines = append(lines, "r: rollback selected snapshot")
	lines = append(lines, "n: clone selected snapshot as a new jail")
	lines = append(lines, "x: refresh snapshot list")

	if m.zfsPanel.inputMode {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Create snapshot"))
		lines = append(lines, truncate("Name: "+m.zfsPanel.inputValue, width))
	}

	if m.zfsPanel.confirmRollback {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Confirm rollback"))
		lines = append(lines, truncate("Target: "+m.zfsPanel.rollbackTarget, width))
		lines = append(lines, truncate("Command: zfs rollback -r "+m.zfsPanel.rollbackTarget, width))
	}

	if m.zfsPanel.cloneMode {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Clone jail from snapshot"))
		if snapshot, ok := m.zfsPanel.selectedSnapshot(); ok {
			lines = append(lines, truncate("Source snapshot: "+snapshot.Name, width))
		}
		nameLine := truncate("Clone name: "+valueOrDash(m.zfsPanel.cloneName), width)
		if m.zfsPanel.cloneField == 0 {
			nameLine = selectedRowStyle.Width(max(1, width)).Render(truncate("> Clone name: "+valueOrDash(m.zfsPanel.cloneName), width))
		}
		lines = append(lines, nameLine)
		destLine := truncate("Destination: "+valueOrDash(m.zfsPanel.cloneDestination), width)
		if m.zfsPanel.cloneField == 1 {
			destLine = selectedRowStyle.Width(max(1, width)).Render(truncate("> Destination: "+valueOrDash(m.zfsPanel.cloneDestination), width))
		}
		lines = append(lines, destLine)
		lines = append(lines, truncate("Write jail.conf clone: "+yesNoText(m.zfsPanel.cloneWriteConfig), width))
		if m.zfsPanel.clonePreview.CloneDataset != "" {
			lines = append(lines, truncate("Clone dataset: "+m.zfsPanel.clonePreview.CloneDataset, width))
		}
		if m.zfsPanel.clonePreview.ConfigPath != "" {
			lines = append(lines, truncate("Config path: "+m.zfsPanel.clonePreview.ConfigPath, width))
		}
		if m.zfsPanel.clonePreview.FstabPath != "" {
			lines = append(lines, truncate("Fstab path: "+m.zfsPanel.clonePreview.FstabPath, width))
		}
		if m.zfsPanel.clonePreview.Err != nil {
			for _, line := range wrapText("Error: "+m.zfsPanel.clonePreview.Err.Error(), width) {
				lines = append(lines, wizardErrorStyle.Render(line))
			}
		}
	}

	if len(m.zfsPanel.logs) > 0 {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Last operation"))
		maxLogs := min(8, len(m.zfsPanel.logs))
		for _, line := range m.zfsPanel.logs[len(m.zfsPanel.logs)-maxLogs:] {
			lines = append(lines, truncate(line, width))
		}
	}

	return lines
}

func (m model) zfsRollbackImplicationLines(width int, snapshot ZFSSnapshot) []string {
	newer := m.zfsNewerSnapshots(snapshot.Name)
	lines := []string{
		truncate("Rollback command: zfs rollback -r "+snapshot.Name, width),
		truncate("Dataset contents will revert to the selected snapshot state.", width),
	}
	if len(newer) == 0 {
		lines = append(lines, truncate("No newer snapshots will be destroyed.", width))
		return lines
	}
	lines = append(lines, truncate(fmt.Sprintf("%d newer snapshot(s) will be destroyed on this dataset.", len(newer)), width))
	maxNames := min(4, len(newer))
	names := make([]string, 0, maxNames)
	for _, item := range newer[:maxNames] {
		names = append(names, snapshotShortName(item.Name))
	}
	lines = append(lines, truncate("Newer snapshots: "+strings.Join(names, ", "), width))
	if len(newer) > maxNames {
		lines = append(lines, truncate(fmt.Sprintf("...and %d more newer snapshot(s).", len(newer)-maxNames), width))
	}
	lines = append(lines, truncate("If newer snapshots have dependents, rollback may fail until those dependencies are cleared.", width))
	return lines
}

func (panel *zfsPanelState) refreshClonePreview() {
	snapshot, ok := panel.selectedSnapshot()
	if !ok {
		panel.clonePreview = JailSnapshotClonePreview{Err: fmt.Errorf("select a snapshot to clone")}
		return
	}
	panel.clonePreview = InspectJailSnapshotClone(panel.sourceDetail, snapshot.Name, panel.cloneName, panel.cloneDestination, panel.cloneWriteConfig)
}

func (panel *zfsPanelState) appendCloneField(text string) {
	if text == "" {
		return
	}
	switch panel.cloneField {
	case 0:
		panel.cloneName += text
	case 1:
		panel.cloneDestination += text
	}
	panel.refreshClonePreview()
}

func (panel *zfsPanelState) backspaceCloneField() {
	var ref *string
	switch panel.cloneField {
	case 0:
		ref = &panel.cloneName
	case 1:
		ref = &panel.cloneDestination
	}
	if ref == nil || *ref == "" {
		return
	}
	*ref = (*ref)[:len(*ref)-1]
	panel.refreshClonePreview()
}

func (m model) zfsNewerSnapshots(target string) []ZFSSnapshot {
	if strings.TrimSpace(target) == "" {
		return nil
	}
	for idx, snapshot := range m.zfsPanel.snapshots {
		if snapshot.Name == target {
			if idx+1 >= len(m.zfsPanel.snapshots) {
				return nil
			}
			return append([]ZFSSnapshot(nil), m.zfsPanel.snapshots[idx+1:]...)
		}
	}
	return nil
}

func (m model) zfsListHeight() int {
	return max(3, m.height-16)
}

func (panel *zfsPanelState) selectedSnapshot() (ZFSSnapshot, bool) {
	if len(panel.snapshots) == 0 {
		return ZFSSnapshot{}, false
	}
	idx := panel.cursor
	if idx < 0 {
		idx = 0
	}
	if idx >= len(panel.snapshots) {
		idx = len(panel.snapshots) - 1
	}
	return panel.snapshots[idx], true
}

func (panel *zfsPanelState) boundCursor(visibleRows int) {
	if len(panel.snapshots) == 0 {
		panel.cursor = 0
		panel.offset = 0
		return
	}
	if panel.cursor < 0 {
		panel.cursor = 0
	}
	if panel.cursor >= len(panel.snapshots) {
		panel.cursor = len(panel.snapshots) - 1
	}
	if panel.cursor < panel.offset {
		panel.offset = panel.cursor
	}
	if panel.cursor >= panel.offset+visibleRows {
		panel.offset = panel.cursor - visibleRows + 1
	}
	if panel.offset < 0 {
		panel.offset = 0
	}
	maxOffset := max(0, len(panel.snapshots)-visibleRows)
	if panel.offset > maxOffset {
		panel.offset = maxOffset
	}
}

func snapshotShortName(full string) string {
	if _, name, ok := strings.Cut(full, "@"); ok {
		return name
	}
	return full
}
