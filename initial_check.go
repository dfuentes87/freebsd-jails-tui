package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	docJailsPath       = "/usr/local/jails"
	docDatasetBase     = "zroot/jails"
	docDatasetMedia    = "zroot/jails/media"
	docDatasetTemplate = "zroot/jails/templates"
	docDatasetCont     = "zroot/jails/containers"
	initialCheckMarker = "initial-check.done"
)

type initialConfigMsg struct {
	status initialConfigStatus
	err    error
}

type initialActionMsg struct {
	logs    []string
	err     error
	message string
	refresh bool
}

type initialCheckPhase int

const (
	initialPhaseLoading initialCheckPhase = iota
	initialPhaseEnableRCConfirm
	initialPhaseDirsPrompt
	initialPhaseDirsCustomInput
	initialPhaseDatasetsPrompt
	initialPhaseDatasetsCustomInput
	initialPhaseComplete
)

type initialConfigStatus struct {
	JailEnableValue    string
	ParallelStartValue string
	NeedsJailEnable    bool
	NeedsParallelStart bool

	ExistingJailPaths []string
	HasJailPath       bool

	JailDatasets   []string
	HasJailDataset bool

	Errors    []string
	CheckedAt time.Time
}

func (status initialConfigStatus) needsRCFix() bool {
	return status.NeedsJailEnable || status.NeedsParallelStart
}

type initialCheckState struct {
	loading  bool
	applying bool
	phase    initialCheckPhase

	status initialConfigStatus
	err    error

	message string
	logs    []string

	skipRC       bool
	skipDirs     bool
	skipDatasets bool

	customDirPath string

	customDatasetField      int
	customDatasetBase       string
	customDatasetMountpoint string
	customDatasetMedia      string
	customDatasetTemplates  string
	customDatasetContainers string
}

func newInitialCheckState() initialCheckState {
	return initialCheckState{
		loading: true,
		phase:   initialPhaseLoading,
	}
}

func (state *initialCheckState) setPhaseFromStatus() {
	if state.loading || state.applying {
		return
	}
	if state.phase == initialPhaseDirsCustomInput || state.phase == initialPhaseDatasetsCustomInput {
		return
	}
	if state.status.needsRCFix() && !state.skipRC {
		state.phase = initialPhaseEnableRCConfirm
		return
	}
	if !state.status.HasJailPath && !state.skipDirs {
		state.phase = initialPhaseDirsPrompt
		return
	}
	if !state.status.HasJailDataset && !state.skipDatasets {
		state.phase = initialPhaseDatasetsPrompt
		return
	}
	state.phase = initialPhaseComplete
}

func (state *initialCheckState) preferredJailsPath() string {
	for _, path := range state.status.ExistingJailPaths {
		if path == docJailsPath {
			return path
		}
	}
	if len(state.status.ExistingJailPaths) > 0 {
		return state.status.ExistingJailPaths[0]
	}
	return docJailsPath
}

func initialWizardDestination(status initialConfigStatus) string {
	base := ""
	for _, path := range status.ExistingJailPaths {
		if path == docJailsPath {
			base = path
			break
		}
		if base == "" {
			base = path
		}
	}
	if base == "" {
		for _, path := range []string{docJailsPath, "/usr/jail", "/jail"} {
			if info, err := os.Stat(path); err == nil && info.IsDir() {
				base = path
				break
			}
		}
	}
	if base == "" {
		base = docJailsPath
	}
	base = filepath.Clean(base)
	containers := filepath.Join(base, "containers")
	if info, err := os.Stat(containers); err == nil && info.IsDir() {
		return filepath.Join(containers, "new-jail")
	}
	return filepath.Join(base, "new-jail")
}

func suggestTemplateParentDataset(status initialConfigStatus) (string, string, bool) {
	baseDataset := preferredBaseJailDataset(status.JailDatasets)
	if baseDataset == "" {
		return "", "", false
	}

	baseMountpoint := ""
	if mp, err := zfsMountpointForDataset(baseDataset); err == nil {
		baseMountpoint = mp
	}
	if baseMountpoint == "" {
		for _, path := range status.ExistingJailPaths {
			if path == docJailsPath {
				baseMountpoint = path
				break
			}
			if baseMountpoint == "" {
				baseMountpoint = path
			}
		}
	}
	if baseMountpoint == "" || !strings.HasPrefix(baseMountpoint, "/") {
		return "", "", false
	}

	return baseDataset + "/templates", filepath.Join(filepath.Clean(baseMountpoint), "templates"), true
}

func preferredBaseJailDataset(datasets []string) string {
	if len(datasets) == 0 {
		return ""
	}
	for _, dataset := range datasets {
		if dataset == docDatasetBase {
			return dataset
		}
	}
	for _, dataset := range datasets {
		if base := trimJailDatasetRole(dataset); base != "" {
			return base
		}
	}
	for _, dataset := range datasets {
		if strings.Contains(strings.ToLower(filepath.Base(dataset)), "jail") {
			return dataset
		}
	}
	return ""
}

func trimJailDatasetRole(dataset string) string {
	parts := strings.Split(strings.Trim(strings.TrimSpace(dataset), "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	for idx, part := range parts {
		switch strings.ToLower(part) {
		case "media", "templates", "containers":
			if idx == 0 {
				return ""
			}
			return strings.Join(parts[:idx], "/")
		}
	}
	return ""
}

func zfsMountpointForDataset(dataset string) (string, error) {
	out, err := exec.Command("zfs", "list", "-H", "-o", "mountpoint", dataset).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to read mountpoint for %q: %w", dataset, err)
	}
	mountpoint := strings.TrimSpace(strings.Split(string(out), "\n")[0])
	if mountpoint == "" || mountpoint == "-" || mountpoint == "legacy" {
		return "", nil
	}
	return filepath.Clean(mountpoint), nil
}

func initialCheckCompleted() (bool, error) {
	path, err := initialCheckMarkerPath()
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("failed to inspect initial check marker: %w", err)
}

func markInitialCheckCompleted() error {
	path, err := initialCheckMarkerPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}
	if err := os.WriteFile(path, []byte(time.Now().Format(time.RFC3339)+"\n"), 0o644); err != nil {
		return fmt.Errorf("failed to persist initial check marker: %w", err)
	}
	return nil
}

func initialCheckMarkerPath() (string, error) {
	configDir, err := appConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, initialCheckMarker), nil
}

func (state *initialCheckState) beginCustomDirInput() {
	state.phase = initialPhaseDirsCustomInput
	if strings.TrimSpace(state.customDirPath) == "" {
		state.customDirPath = state.preferredJailsPath()
	}
}

func (state *initialCheckState) beginCustomDatasetInput() {
	state.phase = initialPhaseDatasetsCustomInput
	state.customDatasetField = 0
	if strings.TrimSpace(state.customDatasetBase) == "" {
		state.customDatasetBase = docDatasetBase
	}
	if strings.TrimSpace(state.customDatasetMountpoint) == "" {
		state.customDatasetMountpoint = state.preferredJailsPath()
	}
	if strings.TrimSpace(state.customDatasetMedia) == "" {
		state.customDatasetMedia = state.customDatasetBase + "/media"
	}
	if strings.TrimSpace(state.customDatasetTemplates) == "" {
		state.customDatasetTemplates = state.customDatasetBase + "/templates"
	}
	if strings.TrimSpace(state.customDatasetContainers) == "" {
		state.customDatasetContainers = state.customDatasetBase + "/containers"
	}
}

func (state *initialCheckState) resetSkips() {
	state.skipRC = false
	state.skipDirs = false
	state.skipDatasets = false
}

func (state *initialCheckState) datasetFieldRef() *string {
	switch state.customDatasetField {
	case 0:
		return &state.customDatasetBase
	case 1:
		return &state.customDatasetMountpoint
	case 2:
		return &state.customDatasetMedia
	case 3:
		return &state.customDatasetTemplates
	case 4:
		return &state.customDatasetContainers
	default:
		return nil
	}
}

func (state *initialCheckState) datasetFieldLabel() string {
	switch state.customDatasetField {
	case 0:
		return "Base dataset"
	case 1:
		return "Mountpoint path"
	case 2:
		return "Media dataset (optional)"
	case 3:
		return "Templates dataset (optional)"
	case 4:
		return "Containers dataset (optional)"
	default:
		return ""
	}
}

func collectInitialConfigCmd() tea.Cmd {
	return func() tea.Msg {
		status, err := collectInitialConfigStatus(time.Now())
		return initialConfigMsg{status: status, err: err}
	}
}

func collectInitialConfigStatus(now time.Time) (initialConfigStatus, error) {
	status := initialConfigStatus{
		JailEnableValue:    "(unset)",
		ParallelStartValue: "(unset)",
		CheckedAt:          now,
	}
	var errs []error

	jailEnable, err := readRCConfValue("jail_enable")
	if err != nil {
		errs = append(errs, err)
	} else if strings.TrimSpace(jailEnable) != "" {
		status.JailEnableValue = jailEnable
	}
	parallel, err := readRCConfValue("jail_parallel_start")
	if err != nil {
		errs = append(errs, err)
	} else if strings.TrimSpace(parallel) != "" {
		status.ParallelStartValue = parallel
	}

	status.NeedsJailEnable = !isEnabledRCValue(status.JailEnableValue)
	status.NeedsParallelStart = !isEnabledRCValue(status.ParallelStartValue)

	for _, path := range []string{"/jail", "/usr/jail", docJailsPath} {
		info, statErr := os.Stat(path)
		if statErr != nil || !info.IsDir() {
			continue
		}
		status.ExistingJailPaths = append(status.ExistingJailPaths, path)
	}
	status.HasJailPath = len(status.ExistingJailPaths) > 0

	datasets, datasetErr := discoverJailNamedDatasets()
	if datasetErr != nil {
		errs = append(errs, datasetErr)
	} else {
		status.JailDatasets = datasets
		status.HasJailDataset = len(datasets) > 0
	}

	for _, err := range errs {
		if err == nil {
			continue
		}
		status.Errors = append(status.Errors, err.Error())
	}
	return status, errors.Join(errs...)
}

func readRCConfValue(key string) (string, error) {
	out, err := exec.Command("sysrc", "-n", key).CombinedOutput()
	if err == nil {
		return strings.TrimSpace(string(out)), nil
	}
	value, found, parseErr := parseRCConfValue(key)
	if parseErr != nil {
		return "", parseErr
	}
	if found {
		return value, nil
	}
	return "", nil
}

func parseRCConfValue(key string) (string, bool, error) {
	paths := []string{"/etc/rc.conf", "/etc/rc.conf.local"}
	var (
		found bool
		value string
	)

	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", false, fmt.Errorf("failed reading %s: %w", path, err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(stripInlineComment(scanner.Text()))
			if line == "" {
				continue
			}
			left, right, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			if strings.TrimSpace(left) != key {
				continue
			}
			clean := strings.TrimSpace(strings.Trim(right, `"'`))
			value = clean
			found = true
		}
		file.Close()
	}
	return value, found, nil
}

func isEnabledRCValue(value string) bool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "YES", "TRUE", "ON", "1":
		return true
	default:
		return false
	}
}

func discoverJailNamedDatasets() ([]string, error) {
	out, err := exec.Command("zfs", "list", "-H", "-o", "name", "-t", "filesystem").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run zfs list for dataset discovery: %w", err)
	}
	var datasets []string
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		name := strings.TrimSpace(scanner.Text())
		if name == "" {
			continue
		}
		if strings.Contains(strings.ToLower(name), "jail") {
			datasets = append(datasets, name)
		}
	}
	sort.Strings(datasets)
	return datasets, nil
}

func enableRCDefaultsCmd(enableJail, enableParallel bool) tea.Cmd {
	return func() tea.Msg {
		var logs []string
		if enableJail {
			if _, err := runLoggedCommand(&logs, "sysrc", "jail_enable=YES"); err != nil {
				return initialActionMsg{logs: logs, err: err, message: "Failed enabling jail_enable."}
			}
		}
		if enableParallel {
			if _, err := runLoggedCommand(&logs, "sysrc", "jail_parallel_start=YES"); err != nil {
				return initialActionMsg{logs: logs, err: err, message: "Failed enabling jail_parallel_start."}
			}
		}
		if !enableJail && !enableParallel {
			logs = append(logs, "No rc.conf settings required updates.")
		}
		return initialActionMsg{
			logs:    logs,
			message: "rc.conf jail settings updated.",
			refresh: true,
		}
	}
}

func createJailLayoutCmd(basePath string) tea.Cmd {
	return func() tea.Msg {
		validatedPath, err := validateAbsolutePath(basePath, "base path")
		if err != nil {
			return initialActionMsg{
				err:     err,
				message: "Invalid jail path.",
			}
		}
		basePath = validatedPath
		paths := []string{
			basePath,
			filepath.Join(basePath, "media"),
			filepath.Join(basePath, "templates"),
			filepath.Join(basePath, "containers"),
		}
		logs := make([]string, 0, len(paths))
		for _, path := range paths {
			logs = append(logs, "$ mkdir -p "+path)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return initialActionMsg{
					logs:    logs,
					err:     fmt.Errorf("failed creating %s: %w", path, err),
					message: "Failed creating jail directory structure.",
				}
			}
		}
		return initialActionMsg{
			logs:    logs,
			message: "Jail directory structure created.",
			refresh: true,
		}
	}
}

func createDefaultDatasetLayoutCmd() tea.Cmd {
	return createDatasetLayoutCmd(
		docDatasetBase,
		docJailsPath,
		docDatasetMedia,
		docDatasetTemplate,
		docDatasetCont,
	)
}

func createDatasetLayoutCmd(baseDataset, mountpoint, media, templates, containers string) tea.Cmd {
	return func() tea.Msg {
		var err error
		if baseDataset, err = validateZFSDatasetName(baseDataset, "base dataset"); err != nil {
			return initialActionMsg{err: err, message: "Dataset creation failed."}
		}
		if mountpoint, err = validateAbsolutePath(mountpoint, "mountpoint"); err != nil {
			return initialActionMsg{err: err, message: "Dataset creation failed."}
		}
		if media, err = validateOptionalZFSDatasetName(media, "media dataset"); err != nil {
			return initialActionMsg{err: err, message: "Dataset creation failed."}
		}
		if templates, err = validateOptionalZFSDatasetName(templates, "templates dataset"); err != nil {
			return initialActionMsg{err: err, message: "Dataset creation failed."}
		}
		if containers, err = validateOptionalZFSDatasetName(containers, "containers dataset"); err != nil {
			return initialActionMsg{err: err, message: "Dataset creation failed."}
		}

		var logs []string
		if _, err := runLoggedCommand(&logs, "zfs", "create", "-o", "mountpoint="+mountpoint, baseDataset); err != nil {
			return initialActionMsg{
				logs:    logs,
				err:     err,
				message: "Failed creating base jail dataset.",
			}
		}

		for _, dataset := range []string{media, templates, containers} {
			if dataset == "" {
				continue
			}
			if _, err := runLoggedCommand(&logs, "zfs", "create", dataset); err != nil {
				return initialActionMsg{
					logs:    logs,
					err:     err,
					message: "Failed creating child jail datasets.",
				}
			}
		}
		return initialActionMsg{
			logs:    logs,
			message: "Jail dataset layout created.",
			refresh: true,
		}
	}
}

func (m model) updateInitialCheckKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	if m.initCheck.loading || m.initCheck.applying {
		return m, nil
	}

	switch m.initCheck.phase {
	case initialPhaseEnableRCConfirm:
		switch key {
		case "y", "Y", "enter":
			m.initCheck.applying = true
			m.initCheck.message = "Applying rc.conf settings..."
			return m, enableRCDefaultsCmd(m.initCheck.status.NeedsJailEnable, m.initCheck.status.NeedsParallelStart)
		case "n", "N", "s", "S":
			m.initCheck.skipRC = true
			m.initCheck.message = "Skipped rc.conf updates."
			m.initCheck.setPhaseFromStatus()
			return m, nil
		case "r", "R":
			m.initCheck.resetSkips()
			m.initCheck.loading = true
			return m, collectInitialConfigCmd()
		}

	case initialPhaseDirsPrompt:
		switch key {
		case "d", "D", "y", "Y":
			m.initCheck.applying = true
			m.initCheck.message = "Creating documentation default jail path..."
			return m, createJailLayoutCmd(docJailsPath)
		case "c", "C":
			m.initCheck.beginCustomDirInput()
			m.initCheck.message = "Enter custom jail base path."
			return m, nil
		case "n", "N", "s", "S":
			m.initCheck.skipDirs = true
			m.initCheck.message = "Skipped jail directory creation."
			m.initCheck.setPhaseFromStatus()
			return m, nil
		case "r", "R":
			m.initCheck.resetSkips()
			m.initCheck.loading = true
			return m, collectInitialConfigCmd()
		}

	case initialPhaseDirsCustomInput:
		switch key {
		case "esc":
			m.initCheck.phase = initialPhaseDirsPrompt
			m.initCheck.message = "Custom path canceled."
			return m, nil
		case "enter":
			path, err := validateAbsolutePath(m.initCheck.customDirPath, "custom path")
			if err != nil {
				m.initCheck.err = err
				m.initCheck.message = "Enter an absolute path like /usr/local/jails."
				return m, nil
			}
			m.initCheck.applying = true
			m.initCheck.message = "Creating custom jail directory structure..."
			return m, createJailLayoutCmd(path)
		case "backspace", "delete":
			runes := []rune(m.initCheck.customDirPath)
			if len(runes) == 0 {
				return m, nil
			}
			m.initCheck.customDirPath = string(runes[:len(runes)-1])
			return m, nil
		}
		if msg.Type == tea.KeyRunes {
			m.initCheck.customDirPath += string(msg.Runes)
		}
		return m, nil

	case initialPhaseDatasetsPrompt:
		switch key {
		case "d", "D", "y", "Y":
			m.initCheck.applying = true
			m.initCheck.message = "Creating documentation default ZFS datasets..."
			return m, createDefaultDatasetLayoutCmd()
		case "c", "C":
			m.initCheck.beginCustomDatasetInput()
			m.initCheck.message = "Set custom dataset values."
			return m, nil
		case "n", "N", "s", "S":
			m.initCheck.skipDatasets = true
			m.initCheck.message = "Skipped dataset creation."
			m.initCheck.setPhaseFromStatus()
			return m, nil
		case "r", "R":
			m.initCheck.resetSkips()
			m.initCheck.loading = true
			return m, collectInitialConfigCmd()
		}

	case initialPhaseDatasetsCustomInput:
		switch key {
		case "esc":
			m.initCheck.phase = initialPhaseDatasetsPrompt
			m.initCheck.message = "Custom dataset input canceled."
			return m, nil
		case "tab", "down":
			m.initCheck.customDatasetField++
		case "shift+tab", "up":
			m.initCheck.customDatasetField--
		case "g", "home":
			m.initCheck.customDatasetField = 0
		case "G", "end":
			m.initCheck.customDatasetField = 4
		case "enter":
			base, err := validateZFSDatasetName(m.initCheck.customDatasetBase, "base dataset")
			if err != nil {
				m.initCheck.err = err
				m.initCheck.message = "Base dataset is required."
				return m, nil
			}
			mountpoint, err := validateAbsolutePath(m.initCheck.customDatasetMountpoint, "mountpoint")
			if err != nil {
				m.initCheck.err = err
				m.initCheck.message = "Mountpoint must be an absolute path."
				return m, nil
			}
			m.initCheck.applying = true
			m.initCheck.message = "Creating custom dataset layout..."
			return m, createDatasetLayoutCmd(
				base,
				mountpoint,
				m.initCheck.customDatasetMedia,
				m.initCheck.customDatasetTemplates,
				m.initCheck.customDatasetContainers,
			)
		case "backspace", "delete":
			ref := m.initCheck.datasetFieldRef()
			if ref == nil {
				return m, nil
			}
			runes := []rune(*ref)
			if len(runes) == 0 {
				return m, nil
			}
			*ref = string(runes[:len(runes)-1])
		default:
			if msg.Type == tea.KeyRunes {
				ref := m.initCheck.datasetFieldRef()
				if ref != nil {
					*ref += string(msg.Runes)
				}
			}
		}
		if m.initCheck.customDatasetField < 0 {
			m.initCheck.customDatasetField = 0
		}
		if m.initCheck.customDatasetField > 4 {
			m.initCheck.customDatasetField = 4
		}
		return m, nil

	case initialPhaseComplete:
		switch key {
		case "enter", "right":
			if err := markInitialCheckCompleted(); err != nil {
				m.initCheck.err = err
				m.initCheck.message = "Failed to persist initial setup completion."
				return m, nil
			}
			m.mode = screenDashboard
			m.notice = "Initial config check completed."
			return m, pollCmd()
		case "r", "R":
			m.initCheck.resetSkips()
			m.initCheck.loading = true
			m.initCheck.message = "Refreshing checks..."
			return m, collectInitialConfigCmd()
		}
	}
	return m, nil
}

func (m model) renderInitialCheckView() string {
	title := titleStyle.Render("Initial Config Check")
	checked := "pending"
	if !m.initCheck.status.CheckedAt.IsZero() {
		checked = m.initCheck.status.CheckedAt.Format("15:04:05")
	}
	meta := summaryStyle.Render("Checked: " + checked)
	header := lipgloss.NewStyle().Width(m.width).Render(title + "  " + meta)

	bodyHeight := max(5, m.height-3)
	lines := m.initialCheckLines(max(12, m.width-2))
	if len(lines) > bodyHeight {
		lines = lines[:bodyHeight]
	}
	body := lipgloss.NewStyle().
		Width(m.width).
		Height(bodyHeight).
		Padding(0, 1).
		Render(strings.Join(lines, "\n"))

	footerRenderer := footerStyle
	message := m.initCheck.message
	if m.initCheck.err != nil {
		message = "error: " + m.initCheck.err.Error()
		footerRenderer = wizardErrorStyle.Copy().Padding(0, 1)
	}
	footer := m.renderFooterWithMessage(m.initialCheckFooterHint(), message, footerRenderer)
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

func (m model) initialCheckLines(width int) []string {
	lines := make([]string, 0, 64)
	appendLine := func(line string) {
		lines = append(lines, truncate(line, width))
	}

	if m.initCheck.loading {
		appendLine("Running initial checks...")
		return lines
	}
	lines = append(lines, sectionStyle.Render("rc.conf checks"))
	appendLine(fmt.Sprintf("jail_enable = %s (%s)", displayRCValue(m.initCheck.status.JailEnableValue), checkStatusText(!m.initCheck.status.NeedsJailEnable)))
	appendLine(fmt.Sprintf("jail_parallel_start = %s (%s)", displayRCValue(m.initCheck.status.ParallelStartValue), checkStatusText(!m.initCheck.status.NeedsParallelStart)))
	appendLine("")

	lines = append(lines, sectionStyle.Render("Jail path checks"))
	if m.initCheck.status.HasJailPath {
		appendLine("Found jail paths:")
		for _, path := range m.initCheck.status.ExistingJailPaths {
			appendLine("  - " + path)
		}
	} else {
		appendLine("No jail path found at /jail, /usr/jail, or /usr/local/jails.")
	}
	appendLine("")

	lines = append(lines, sectionStyle.Render("ZFS dataset checks"))
	if m.initCheck.status.HasJailDataset {
		appendLine("Datasets containing \"jail\":")
		for _, dataset := range m.initCheck.status.JailDatasets {
			appendLine("  - " + dataset)
		}
	} else {
		appendLine("No ZFS dataset containing \"jail\" was found.")
	}

	if len(m.initCheck.status.Errors) > 0 {
		appendLine("")
		lines = append(lines, sectionStyle.Render("Check warnings"))
		for _, err := range m.initCheck.status.Errors {
			lines = append(lines, wizardErrorStyle.Render(truncate("  - "+err, width)))
		}
	}

	appendLine("")
	lines = append(lines, sectionStyle.Render("Next action"))

	switch m.initCheck.phase {
	case initialPhaseEnableRCConfirm:
		appendLine("Enable missing rc.conf settings now?")
		appendLine("y: yes, set to YES | n: skip for now")
	case initialPhaseDirsPrompt:
		appendLine("No jail directory root found.")
		appendLine("d: create documentation default /usr/local/jails layout")
		appendLine("c: set custom jail base path")
		appendLine("n: skip for now")
	case initialPhaseDirsCustomInput:
		appendLine("Custom jail base path:")
		appendLine("Path: " + m.initCheck.customDirPath)
		appendLine("enter: create path + media/templates/containers subdirs")
	case initialPhaseDatasetsPrompt:
		appendLine("No jail-related ZFS datasets found.")
		appendLine("d: create documentation default zroot/jails layout")
		appendLine("c: set custom dataset values")
		appendLine("n: skip for now")
	case initialPhaseDatasetsCustomInput:
		lines = append(lines, m.initialDatasetCustomLines(width)...)
	case initialPhaseComplete:
		appendLine("Initial check complete.")
		appendLine("Press enter to continue to dashboard.")
	}

	if len(m.initCheck.logs) > 0 {
		appendLine("")
		lines = append(lines, sectionStyle.Render("Last action logs"))
		maxLogs := min(6, len(m.initCheck.logs))
		for _, line := range m.initCheck.logs[len(m.initCheck.logs)-maxLogs:] {
			appendLine(line)
		}
	}

	return lines
}

func (m model) initialDatasetCustomLines(width int) []string {
	lines := make([]string, 0, 16)
	rows := []struct {
		index int
		label string
		value string
	}{
		{0, "Base dataset", m.initCheck.customDatasetBase},
		{1, "Mountpoint path", m.initCheck.customDatasetMountpoint},
		{2, "Media dataset (optional)", m.initCheck.customDatasetMedia},
		{3, "Templates dataset (optional)", m.initCheck.customDatasetTemplates},
		{4, "Containers dataset (optional)", m.initCheck.customDatasetContainers},
	}

	for _, row := range rows {
		prefix := " "
		if row.index == m.initCheck.customDatasetField {
			prefix = ">"
		}
		line := fmt.Sprintf("%s %s: %s", prefix, row.label, row.value)
		line = truncate(line, width)
		if row.index == m.initCheck.customDatasetField {
			line = selectedRowStyle.Width(max(1, width)).Render(line)
		}
		lines = append(lines, line)
	}
	lines = append(lines, truncate("tab/shift+tab: move field | enter: create layout", width))
	return lines
}

func (m model) initialCheckFooterHint() string {
	switch m.initCheck.phase {
	case initialPhaseLoading:
		return "Running initial checks... | q: quit"
	case initialPhaseEnableRCConfirm:
		return "y: enable missing settings | n: skip | r: re-check | q: quit"
	case initialPhaseDirsPrompt:
		return "d: docs default | c: custom path | n: skip | r: re-check | q: quit"
	case initialPhaseDirsCustomInput:
		return "type path | enter: create | esc: back | backspace: edit"
	case initialPhaseDatasetsPrompt:
		return "d: docs default datasets | c: custom values | n: skip | r: re-check | q: quit"
	case initialPhaseDatasetsCustomInput:
		return "type values | tab/shift+tab: field | enter: create | esc: back"
	case initialPhaseComplete:
		return "enter: continue to dashboard | r: re-check | q: quit"
	default:
		return "q: quit"
	}
}

func displayRCValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "(unset)"
	}
	return value
}

func checkStatusText(ok bool) string {
	if ok {
		return "OK"
	}
	return "MISSING"
}
