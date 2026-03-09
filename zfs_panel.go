package main

import (
	"fmt"
	"os/exec"
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
	dataset         string
	snapshots       []ZFSSnapshot
	cursor          int
	offset          int
	loading         bool
	actionRunning   bool
	inputMode       bool
	inputValue      string
	confirmRollback bool
	rollbackTarget  string
	message         string
	logs            []string
	err             error
}

func newZFSPanelState(dataset string) zfsPanelState {
	return zfsPanelState{
		dataset: strings.TrimSpace(dataset),
		loading: true,
		message: "Loading snapshots...",
	}
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
		m.mode = screenJailDetail
		return m, nil
	case "R":
		if m.zfsPanel.actionRunning {
			return m, nil
		}
		m.zfsPanel.loading = true
		m.zfsPanel.message = "Refreshing snapshots..."
		return m, listZFSSnapshotsCmd(m.zfsPanel.dataset)
	case "n":
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

	hint := "j/k: select snapshot | n: new snapshot | r: rollback selected | R: refresh | esc: back | q: quit"
	if m.zfsPanel.inputMode {
		hint = "Type snapshot name | enter: create snapshot | backspace: edit | esc: cancel"
	}
	if m.zfsPanel.confirmRollback {
		hint = "Rollback confirmation pending: enter to confirm | esc to cancel"
	}
	if m.zfsPanel.actionRunning {
		hint = "Executing ZFS action... please wait | q: quit"
	}
	if m.zfsPanel.message != "" {
		hint += " | " + m.zfsPanel.message
	}
	if m.zfsPanel.err != nil {
		hint += " | error: " + m.zfsPanel.err.Error()
	}
	footer := footerStyle.Width(m.width).Render(hint)

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
				"%s %-28s %-18s used:%s",
				prefix,
				truncate(snapshotShortName(snapshot.Name), 28),
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

	lines = append(lines, "")
	lines = append(lines, sectionStyle.Render("Actions"))
	lines = append(lines, "n: create snapshot")
	lines = append(lines, "r: rollback selected snapshot")
	lines = append(lines, "R: refresh snapshot list")

	if m.zfsPanel.inputMode {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Create snapshot"))
		lines = append(lines, truncate("Name: "+m.zfsPanel.inputValue, width))
	}

	if m.zfsPanel.confirmRollback {
		lines = append(lines, "")
		lines = append(lines, sectionStyle.Render("Confirm rollback"))
		lines = append(lines, truncate("Target: "+m.zfsPanel.rollbackTarget, width))
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
