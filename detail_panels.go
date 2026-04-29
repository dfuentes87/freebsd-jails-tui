package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type detailTab int

const (
	detailTabSummary detailTab = iota
	detailTabConfig
	detailTabRuntime
	detailTabResources
	detailTabDrift
)

func allDetailTabs() []detailTab {
	return []detailTab{
		detailTabSummary,
		detailTabConfig,
		detailTabRuntime,
		detailTabResources,
		detailTabDrift,
	}
}

func (t detailTab) label() string {
	switch t {
	case detailTabConfig:
		return "Config"
	case detailTabRuntime:
		return "Runtime"
	case detailTabResources:
		return "Resources"
	case detailTabDrift:
		return "Drift"
	default:
		return "Summary"
	}
}

func (t detailTab) next() detailTab {
	tabs := allDetailTabs()
	for idx, tab := range tabs {
		if tab != t {
			continue
		}
		return tabs[(idx+1)%len(tabs)]
	}
	return detailTabSummary
}

func (t detailTab) prev() detailTab {
	tabs := allDetailTabs()
	for idx, tab := range tabs {
		if tab != t {
			continue
		}
		next := idx - 1
		if next < 0 {
			next = len(tabs) - 1
		}
		return tabs[next]
	}
	return detailTabSummary
}

func (m model) renderDetailTabBar(width int) string {
	items := make([]string, 0, len(allDetailTabs()))
	for _, tab := range allDetailTabs() {
		style := helpTabStyle
		if tab == m.detailTab {
			style = helpTabActiveStyle
		}
		items = append(items, style.Render(tab.label()))
	}
	line := lipgloss.JoinHorizontal(lipgloss.Top, items...)
	if lipgloss.Width(line) >= width {
		return line
	}
	return line + strings.Repeat(" ", width-lipgloss.Width(line))
}

func (m model) detailLines(width int) []string {
	lines := make([]string, 0, 96)
	m.appendDetailEditLines(&lines, width)

	switch m.detailTab {
	case detailTabConfig:
		m.appendDetailConfiguredState(&lines, width)
		m.appendDetailStartupPolicy(&lines, width)
	case detailTabRuntime:
		m.appendDetailRuntimeState(&lines, width)
		m.appendDetailNetworkSummary(&lines, width)
		m.appendDetailLinuxReadiness(&lines, width)
		m.appendDetailSourceErrors(&lines, width)
	case detailTabResources:
		m.appendDetailZFSSection(&lines, width)
		m.appendDetailRctlSection(&lines, width)
	case detailTabDrift:
		m.appendDetailDriftSection(&lines, width)
	default:
		m.appendDetailOverview(&lines, width)
		m.appendDetailBlockersSection(&lines, width)
		m.appendDetailStartupPolicy(&lines, width)
		m.appendDetailSourceErrors(&lines, width)
	}

	if len(lines) == 0 {
		lines = append(lines, "No detail data is available yet.")
	}
	return lines
}

func (m model) appendDetailOverview(lines *[]string, width int) {
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
			jidText = fmt.Sprintf("%d", jail.JID)
		}
		cpuText = fmt.Sprintf("%.2f%%", jail.CPUPercent)
		memText = fmt.Sprintf("%dMB", jail.MemoryMB)
	}

	appendRenderedSectionWithStyle(lines, detailSectionStyle, "Overview", renderKeyValueLines(width,
		[2]string{"Name", m.detail.Name},
		[2]string{"State", state},
		[2]string{"Type", valueOrDash(jail.Type)},
		[2]string{"JID", jidText},
		[2]string{"CPU", cpuText},
		[2]string{"Memory", memText},
		[2]string{"Path", m.detail.Path},
		[2]string{"Hostname", m.detail.Hostname},
	))
	*lines = append(*lines, renderKeyValueLinesWithValueFallback(width, "",
		[2]string{"Note", m.detail.Note},
	)...)
}

func (m model) appendDetailEditLines(lines *[]string, width int) {
	if !m.detailEdit.active() {
		return
	}
	appendSectionWithStyle(lines, width, detailSectionStyle, m.detailEdit.title())
	fieldPairs := make([][2]string, 0, len(detailEditFieldSpecs(m.detailEdit.kind))+1)
	for idx, field := range detailEditFieldSpecs(m.detailEdit.kind) {
		label := field.label
		if idx == m.detailEdit.field {
			label = "> " + label
		}
		value := ""
		if ref := m.detailEdit.valuePtr(idx); ref != nil {
			value = *ref
		}
		fieldPairs = append(fieldPairs, [2]string{label, valueOrDash(value)})
	}
	if m.detailEdit.kind == detailEditNote {
		fieldPairs = append(fieldPairs, [2]string{"Length", fmt.Sprintf("%d/%d", jailNoteLength(m.detailEdit.values.Note), maxJailNoteLen)})
		*lines = append(*lines, renderKeyValueLinesWithValueFallback(width, "", fieldPairs...)...)
	} else {
		*lines = append(*lines, renderKeyValueLines(width, fieldPairs...)...)
	}
	if m.detailEdit.kind == detailEditRctl {
		for _, line := range racctWizardPrereqLines(collectRacctWizardPrereqs(m.detailEdit.values)) {
			if looksLikeWarningText(line) {
				*lines = append(*lines, wizardWarningStyle.Render(truncate(line, max(1, width))))
				continue
			}
			*lines = append(*lines, truncate(line, width))
		}
		*lines = append(*lines, truncate("Managed edits update jail metadata and /etc/rctl.conf. Leave a field blank to remove that limit.", width))
		if m.detail.JID > 0 {
			*lines = append(*lines, truncate("The jail is running, so live rctl rules will be refreshed after save when kern.racct.enable is active.", width))
		} else {
			*lines = append(*lines, truncate("The jail is stopped, so only persistent configuration will be updated right now.", width))
		}
	} else if m.detailEdit.kind == detailEditLinuxMetadata {
		*lines = append(*lines, truncate("These managed metadata fields guide future Linux bootstrap or retry behavior without changing the jail root immediately.", width))
	}
	*lines = append(*lines, truncate("Press enter to save, esc to cancel, and leave supported fields blank to clear them.", width))
	*lines = append(*lines, "")
}

func (m model) appendDetailBlockersSection(lines *[]string, width int) {
	blockers := detailBlockerSummaryLines(m.detail)
	appendSectionWithStyle(lines, width, detailSectionStyle, "Why blocked?")
	if len(blockers) == 0 {
		*lines = append(*lines, truncate("No active prerequisite blockers were detected from the current config, runtime state, or managed metadata.", width))
		return
	}
	for _, line := range blockers {
		appendWrappedStyledText(lines, width, wizardErrorStyle, line)
	}
}

func (m model) appendDetailConfiguredState(lines *[]string, width int) {
	appendSectionWithStyle(lines, width, detailSectionStyle, "Configured state")
	*lines = append(*lines, renderKeyValueLines(width, [2]string{"Source", m.detail.JailConfSource})...)
	if len(m.detail.JailConfValues) == 0 && len(m.detail.JailConfFlags) == 0 {
		*lines = append(*lines, truncate("No matching jail block found.", width))
		return
	}
	for _, key := range sortedKeys(m.detail.JailConfValues) {
		*lines = append(*lines, renderKeyValueLines(width, [2]string{key, m.detailDisplayConfigValue(m.detail.JailConfValues[key])})...)
	}
	for _, flag := range m.detail.JailConfFlags {
		*lines = append(*lines, renderKeyValueLines(width, [2]string{flag, "enabled"})...)
	}
	metadata := parseTUIMetadata(m.detail.JailConfRaw)
	if len(metadata) > 0 {
		appendSectionWithStyle(lines, width, detailSectionStyle, "Managed metadata")
		for _, key := range sortedKeys(metadata) {
			*lines = append(*lines, renderKeyValueLines(width, [2]string{key, valueOrDash(metadata[key])})...)
		}
	}
}

func (m model) appendDetailStartupPolicy(lines *[]string, width int) {
	appendSectionWithStyle(lines, width, detailSectionStyle, "Startup policy")
	if m.detail.StartupConfig == nil {
		*lines = append(*lines, truncate("Startup policy unavailable.", width))
		return
	}
	if m.detail.StartupConfig.InJailList {
		*lines = append(*lines, renderKeyValueLines(width,
			[2]string{"jail_list position", fmt.Sprintf("%d of %d", m.detail.StartupConfig.Position, len(m.detail.StartupConfig.JailList))},
		)...)
	} else if len(m.detail.StartupConfig.JailList) == 0 {
		*lines = append(*lines, renderKeyValueLines(width, [2]string{"jail_list", "empty"})...)
	} else {
		*lines = append(*lines, renderKeyValueLines(width, [2]string{"jail_list", "not present (manual start required when jail_list is used)"})...)
	}
	*lines = append(*lines, renderKeyValueLines(width, [2]string{"Dependencies", dependencySummary(strings.Join(m.detail.StartupConfig.Dependencies, " "))})...)
	if len(m.detail.StartupConfig.JailList) > 0 {
		*lines = append(*lines, renderKeyValueLines(width, [2]string{"Effective jail_list", strings.Join(m.detail.StartupConfig.JailList, " ")})...)
	}
	if m.detail.StartupConfig.ReadError != "" {
		*lines = append(*lines, wizardErrorStyle.Render(truncate("jail_list read error: "+m.detail.StartupConfig.ReadError, max(1, width))))
	}
}

func (m model) appendDetailRuntimeState(lines *[]string, width int) {
	jail, hasJail := m.detailJail()
	state := "STOPPED"
	cpuText := "0.00%"
	memText := "0MB"
	if hasJail {
		if jail.Running || jail.JID > 0 {
			state = "RUNNING"
		}
		cpuText = fmt.Sprintf("%.2f%%", jail.CPUPercent)
		memText = fmt.Sprintf("%dMB", jail.MemoryMB)
	}
	appendRenderedSectionWithStyle(lines, detailSectionStyle, "Runtime state", renderKeyValueLines(width,
		[2]string{"State", state},
		[2]string{"CPU", cpuText},
		[2]string{"Memory", memText},
	))
	runtimeNotes := []string{
		"Runtime values come from the running jail and may differ from jail.conf defaults or the configured state shown on other tabs.",
	}
	if len(m.detail.RuntimeValues) == 0 {
		runtimeNotes = append(runtimeNotes, "No running runtime record is available for this jail.")
	} else {
		for _, key := range orderedRuntimeKeys(m.detail.RuntimeValues) {
			*lines = append(*lines, renderKeyValueLines(width, [2]string{key, m.detail.RuntimeValues[key]})...)
		}
	}
	if m.detailShowAdvanced {
		runtimeNotes = append(runtimeNotes, "Advanced runtime/default parameters are shown below; press a to hide them.")
	} else {
		runtimeNotes = append(runtimeNotes, "Advanced runtime/default parameters are hidden; press a to show them.")
	}
	appendSectionWithStyle(lines, width, detailSectionStyle, "Runtime notes")
	for _, line := range runtimeNotes {
		*lines = append(*lines, truncate(line, width))
	}
	if m.detailShowAdvanced {
		appendSectionWithStyle(lines, width, detailSectionStyle, "Advanced runtime parameters")
		if len(m.detail.AdvancedRuntimeFields) == 0 {
			*lines = append(*lines, truncate("No additional runtime/default parameters.", width))
		} else {
			for _, key := range sortedKeys(m.detail.AdvancedRuntimeFields) {
				*lines = append(*lines, renderKeyValueLines(width, [2]string{key, m.detail.AdvancedRuntimeFields[key]})...)
			}
		}
	}
}

func (m model) appendDetailNetworkSummary(lines *[]string, width int) {
	if m.detail.NetworkSummary == nil {
		return
	}
	appendSectionWithStyle(lines, width, detailSectionStyle, "Network summary")
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
		*lines = append(*lines, renderKeyValueLinesWithLabelWidth(width, networkLabelWidth, networkPairs...)...)
	}
	if len(m.detail.NetworkSummary.Validation) > 0 {
		appendSectionWithStyle(lines, width, detailSectionStyle, "Validation")
		for _, line := range m.detail.NetworkSummary.Validation {
			appendWrappedStyledWizardLine(lines, line, width)
		}
	}
}

func (m model) appendDetailLinuxReadiness(lines *[]string, width int) {
	if m.detail.LinuxReadiness == nil {
		return
	}
	appendSectionWithStyle(lines, width, detailSectionStyle, "Linux readiness")
	linuxLabelWidth := 24
	if width < 72 {
		linuxLabelWidth = 20
	}
	for _, line := range m.linuxReadinessLines() {
		if looksLikeWarningText(line) {
			*lines = append(*lines, wizardWarningStyle.Render(truncate(line, max(1, width))))
			continue
		} else if looksLikeErrorText(line) || strings.HasPrefix(strings.ToLower(line), "readiness issue:") {
			*lines = append(*lines, wizardErrorStyle.Render(truncate(line, max(1, width))))
			continue
		}
		*lines = append(*lines, renderInformationalKeyValueWithLabelWidth(width, linuxLabelWidth, line)...)
	}
}

func (m model) appendDetailZFSSection(lines *[]string, width int) {
	appendSectionWithStyle(lines, width, detailSectionStyle, "ZFS dataset")
	if m.detail.ZFS == nil {
		*lines = append(*lines, truncate("No dataset matched the jail path.", width))
		return
	}
	*lines = append(*lines, renderKeyValueLines(width,
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

func (m model) appendDetailRctlSection(lines *[]string, width int) {
	appendSectionWithStyle(lines, width, detailSectionStyle, "rctl")
	if m.detail.RctlConfig != nil {
		*lines = append(*lines, renderKeyValueLines(width,
			[2]string{"Limit mode", valueOrDash(m.detail.RctlConfig.Mode)},
			[2]string{"Max CPU %", valueOrDash(m.detail.RctlConfig.CPUPercent)},
			[2]string{"Max memory", valueOrDash(m.detail.RctlConfig.MemoryLimit)},
			[2]string{"Max processes", valueOrDash(m.detail.RctlConfig.ProcessLimit)},
			[2]string{"rctl.conf block", yesNoText(m.detail.RctlConfig.Persistent)},
		)...)
		if m.detail.RctlConfig.PersistentErr != "" {
			*lines = append(*lines, wizardErrorStyle.Render(truncate("rctl.conf check: "+m.detail.RctlConfig.PersistentErr, width)))
		}
	}
	if !(m.detailEdit.active() && m.detailEdit.kind == detailEditRctl) {
		*lines = append(*lines, truncate("Press l to add, change, or remove managed resource limits.", width))
	}
	if m.detail.RacctStatus != nil {
		*lines = append(*lines, renderKeyValueLines(width,
			[2]string{"kern.racct.enable", valueOrDash(m.detail.RacctStatus.EffectiveValue)},
			[2]string{"loader.conf configured", yesNoText(m.detail.RacctStatus.LoaderConfigured)},
		)...)
		if m.detail.RacctStatus.ReadError != "" {
			*lines = append(*lines, wizardErrorStyle.Render(truncate("racct check: "+m.detail.RacctStatus.ReadError, width)))
		}
	}
	displayRules := visibleRctlRules(m.detail.Name, m.detail.RctlRules, m.detail.RctlConfig)
	if len(m.detail.RctlRules) == 0 {
		if m.detail.RctlConfig != nil && m.detail.RctlConfig.Mode == "runtime" {
			*lines = append(*lines, truncate("No live rctl rules. Runtime-only limits apply only while the jail is running.", width))
		} else {
			*lines = append(*lines, truncate("No matching rctl rules.", width))
		}
	} else if len(displayRules) > 0 {
		for idx, rule := range displayRules {
			*lines = append(*lines, renderKeyValueLines(width, [2]string{fmt.Sprintf("Active rule %d", idx+1), rule})...)
		}
	}
}

func (m model) appendDetailSourceErrors(lines *[]string, width int) {
	if len(m.detail.SourceErrors) == 0 {
		return
	}
	appendSectionWithStyle(lines, width, detailSectionStyle, "Source errors")
	for _, source := range sortedKeys(m.detail.SourceErrors) {
		*lines = append(*lines, wizardErrorStyle.Render(truncate(fmt.Sprintf("%s: %s", source, m.detail.SourceErrors[source]), width)))
	}
}

func (m model) appendDetailDriftSection(lines *[]string, width int) {
	appendSectionWithStyle(lines, width, detailSectionStyle, "Drift summary")
	driftItems := detailDriftItems(m.detail)
	if len(driftItems) == 0 {
		*lines = append(*lines, truncate("No obvious drift was detected between configured values, runtime state, and managed metadata.", width))
	} else {
		for _, item := range driftItems {
			*lines = append(*lines, renderKeyValueLines(width, [2]string{item[0], item[1]})...)
		}
	}

	metadata := parseTUIMetadata(m.detail.JailConfRaw)
	appendSectionWithStyle(lines, width, detailSectionStyle, "Managed metadata")
	if len(metadata) == 0 {
		*lines = append(*lines, truncate("No managed freebsd-jails-tui metadata is recorded for this jail.", width))
	} else {
		for _, key := range sortedKeys(metadata) {
			*lines = append(*lines, renderKeyValueLines(width, [2]string{key, valueOrDash(metadata[key])})...)
		}
	}

	blockers := detailBlockerSummaryLines(m.detail)
	appendSectionWithStyle(lines, width, detailSectionStyle, "Current blockers")
	if len(blockers) == 0 {
		*lines = append(*lines, truncate("No active blockers were detected.", width))
		return
	}
	for _, line := range blockers {
		appendWrappedStyledText(lines, width, wizardErrorStyle, line)
	}
}

func detailDriftItems(detail JailDetail) [][2]string {
	items := make([][2]string, 0, 16)
	addMismatch := func(label, configured, runtime string) {
		configured = strings.TrimSpace(configured)
		runtime = strings.TrimSpace(runtime)
		if configured == "" || runtime == "" {
			return
		}
		if normalizedComparisonValue(configured) == normalizedComparisonValue(runtime) {
			return
		}
		items = append(items, [2]string{label, fmt.Sprintf("configured=%s | runtime=%s", configured, runtime)})
	}

	addMismatch("Hostname", detail.JailConfValues["host.hostname"], firstNonEmpty(detail.RuntimeValues["host.hostname"], detail.Hostname))
	addMismatch("Path", detail.JailConfValues["path"], firstNonEmpty(detail.RuntimeValues["Live path"], detail.Path))
	addMismatch("Interface", detail.JailConfValues["interface"], detail.RuntimeValues["Interface"])
	addMismatch("IPv4", detail.JailConfValues["ip4.addr"], detail.RuntimeValues["IPv4"])
	addMismatch("IPv6", detail.JailConfValues["ip6.addr"], detail.RuntimeValues["IPv6"])

	if detail.RctlConfig != nil {
		activeRules := visibleRctlRules(detail.Name, detail.RctlRules, detail.RctlConfig)
		if hasAnyRctlLimits(detailRctlValuesFromConfig(detail.RctlConfig)) && len(activeRules) == 0 && detail.JID > 0 {
			items = append(items, [2]string{"rctl rules", "managed limits exist but no matching active runtime rules were found"})
		}
	}

	metadata := parseTUIMetadata(detail.JailConfRaw)
	if detailLooksLikeLinuxJail(detail) && detail.LinuxReadiness != nil {
		if preset := strings.TrimSpace(metadata["linux_preset"]); preset != "" && preset != detail.LinuxReadiness.BootstrapPreset {
			items = append(items, [2]string{"Linux preset", fmt.Sprintf("metadata=%s | readiness=%s", preset, valueOrDash(detail.LinuxReadiness.BootstrapPreset))})
		}
		if method := strings.TrimSpace(metadata["linux_bootstrap_method"]); method != "" && method != detail.LinuxReadiness.BootstrapMethod {
			items = append(items, [2]string{"Linux method", fmt.Sprintf("metadata=%s | readiness=%s", method, valueOrDash(detail.LinuxReadiness.BootstrapMethod))})
		}
		if effectiveLinuxBootstrapMode(linuxBootstrapConfigFromRawLines(detail.JailConfRaw)) == "auto" && !detail.LinuxReadiness.UserlandPresent {
			items = append(items, [2]string{"Linux userland", "bootstrap mode is auto but the Linux userland is not present"})
		}
	}

	return items
}

func normalizedComparisonValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.Trim(value, "\"")
	return value
}

func wizardBlockerSummaryLines(w jailCreationWizard) []string {
	reasons := make([]string, 0, 16)
	step := w.currentStep()

	if wizardShowsNetworkPrereqs(step) {
		for _, err := range w.networkPrereqs.Errors {
			reasons = append(reasons, "Network: "+err)
		}
	}
	if wizardShowsLinuxPrereqs(step) && normalizedJailType(w.values.JailType) == "linux" {
		reasons = append(reasons, linuxWizardBlockingLines(w.linuxPrereqs)...)
	}
	if wizardShowsRacctPrereqs(step) && w.racctPrereqs.HasLimits {
		if w.racctPrereqs.Status.ReadError != "" {
			reasons = append(reasons, "Resource limits: failed to inspect kern.racct.enable: "+w.racctPrereqs.Status.ReadError)
		} else if !w.racctPrereqs.Status.Enabled {
			if w.racctPrereqs.Status.LoaderConfigured {
				reasons = append(reasons, "Resource limits: kern.racct.enable is configured but not active until the host reboots.")
			} else {
				reasons = append(reasons, "Resource limits: kern.racct.enable is not enabled in loader.conf.")
			}
		}
	}
	return uniqueStrings(reasons)
}

func detailBlockerSummaryLines(detail JailDetail) []string {
	reasons := make([]string, 0, 16)

	networkPrereqs := collectNetworkWizardPrereqs(detailNetworkWizardValues(detail))
	for _, err := range networkPrereqs.Errors {
		reasons = append(reasons, "Network: "+err)
	}

	if detail.RacctStatus != nil && detail.RctlConfig != nil && hasAnyRctlLimits(detailRctlValuesFromConfig(detail.RctlConfig)) {
		if detail.RacctStatus.ReadError != "" {
			reasons = append(reasons, "Resource limits: failed to inspect kern.racct.enable: "+detail.RacctStatus.ReadError)
		} else if !detail.RacctStatus.Enabled {
			if detail.RacctStatus.LoaderConfigured {
				reasons = append(reasons, "Resource limits: kern.racct.enable is configured but not active until the host reboots.")
			} else {
				reasons = append(reasons, "Resource limits: kern.racct.enable is not enabled in loader.conf.")
			}
		}
	}

	if detailLooksLikeLinuxJail(detail) && detail.LinuxReadiness != nil {
		readiness := detail.LinuxReadiness
		if readiness.Host.EnableReadError != "" {
			reasons = append(reasons, "Linux host: failed to inspect linux_enable: "+readiness.Host.EnableReadError)
		} else if !readiness.Host.EnableConfigured {
			reasons = append(reasons, "Linux host: linux_enable is not configured to YES.")
		}
		if readiness.BootstrapMethod == "debootstrap" {
			if !readiness.Debootstrap.Installed {
				reasons = append(reasons, "Linux host: debootstrap is not installed.")
			}
			if !readiness.Debootstrap.ScriptsPresent {
				reasons = append(reasons, "Linux host: debootstrap scripts are missing.")
			}
		}
		for _, err := range readiness.Capabilities.Errors {
			reasons = append(reasons, "Linux host: "+err)
		}
		if readiness.MirrorResolveError != "" {
			reasons = append(reasons, "Linux bootstrap source: "+readiness.MirrorResolveError)
		}
		if effectiveLinuxBootstrapMode(linuxBootstrapConfigFromRawLines(detail.JailConfRaw)) == "auto" && !readiness.UserlandPresent {
			reasons = append(reasons, "Linux userland: bootstrap mode is auto but the Linux userland is not present.")
		}
	}

	return uniqueStrings(reasons)
}

func linuxWizardBlockingLines(prereqs LinuxWizardPrereqs) []string {
	reasons := make([]string, 0, 12)
	if prereqs.Host.EnableReadError != "" {
		reasons = append(reasons, "Linux host: failed to inspect linux_enable: "+prereqs.Host.EnableReadError)
	} else if !prereqs.Host.EnableConfigured {
		reasons = append(reasons, "Linux host: linux_enable is not configured to YES.")
	}
	if prereqs.BootstrapMethod == "debootstrap" {
		if !prereqs.Debootstrap.Installed {
			reasons = append(reasons, "Linux host: debootstrap is not installed.")
		}
		if !prereqs.Debootstrap.ScriptsPresent {
			reasons = append(reasons, "Linux host: debootstrap scripts are missing.")
		}
	}
	for _, err := range prereqs.Capabilities.Errors {
		reasons = append(reasons, "Linux host: "+err)
	}
	if prereqs.ResolveError != "" {
		reasons = append(reasons, "Linux bootstrap source: "+prereqs.ResolveError)
	}
	if prereqs.ReleaseSupport == "unsupported" && prereqs.ReleaseSupportMsg != "" {
		reasons = append(reasons, "Linux release: "+prereqs.ReleaseSupportMsg)
	}
	return uniqueStrings(reasons)
}
