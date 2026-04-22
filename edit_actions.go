package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

type detailEditApplyResult struct {
	Name   string
	Kind   detailEditKind
	Values jailWizardValues
	Logs   []string
	Err    error
}

type bulkActionTargetResult struct {
	Name   string
	Logs   []string
	Err    error
	Reason string
}

type bulkActionResult struct {
	Kind    bulkActionKind
	Input   string
	Results []bulkActionTargetResult
	Logs    []string
	Err     error
}

func ExecuteDetailEdit(detail JailDetail, edit detailEditState) detailEditApplyResult {
	result := detailEditApplyResult{
		Name:   strings.TrimSpace(detail.Name),
		Kind:   edit.kind,
		Values: edit.values,
	}
	switch edit.kind {
	case detailEditNote:
		note, err := normalizeJailNote(edit.values.Note)
		if err != nil {
			result.Err = err
			return result
		}
		noteResult := ExecuteJailNoteUpdate(detail, note)
		result.Values.Note = noteResult.Note
		result.Logs = noteResult.Logs
		result.Err = noteResult.Err
	case detailEditRctl:
		values, _, err := normalizeRctlLimitValues(edit.values)
		if err != nil {
			result.Err = err
			return result
		}
		rctlResult := ExecuteJailRctlUpdate(detail, values)
		result.Values = values
		result.Logs = rctlResult.Logs
		result.Err = rctlResult.Err
	case detailEditHostname:
		hostname, err := normalizeJailHostname(edit.values.Hostname)
		if err != nil {
			result.Err = err
			return result
		}
		hostnameResult := ExecuteJailHostnameUpdate(detail, hostname)
		result.Values.Hostname = hostnameResult.Hostname
		result.Logs = hostnameResult.Logs
		result.Err = hostnameResult.Err
	case detailEditStartupOrder:
		values := jailWizardValues{StartupOrder: strings.TrimSpace(edit.values.StartupOrder)}
		startupResult := ExecuteJailStartupOrderUpdate(detail, values)
		result.Values.StartupOrder = values.StartupOrder
		result.Logs = startupResult.Logs
		result.Err = startupResult.Err
	case detailEditDependencies:
		dependencies, err := validateExistingJailDependencies(edit.values.Dependencies, detail.Name)
		if err != nil {
			result.Err = err
			return result
		}
		value := strings.Join(dependencies, " ")
		dependencyResult := ExecuteJailDependencyUpdate(detail, value)
		result.Values.Dependencies = value
		result.Logs = dependencyResult.Logs
		result.Err = dependencyResult.Err
	case detailEditLinuxMetadata:
		values, err := validateLinuxMetadataValues(edit.values)
		if err != nil {
			result.Err = err
			return result
		}
		linuxResult := ExecuteJailLinuxMetadataUpdate(detail, values)
		result.Values = values
		result.Logs = linuxResult.Logs
		result.Err = linuxResult.Err
	default:
		result.Err = fmt.Errorf("unsupported detail edit")
	}
	return result
}

type JailHostnameUpdateResult struct {
	Name     string
	Hostname string
	Logs     []string
	Err      error
}

type JailDependencyUpdateResult struct {
	Name         string
	Dependencies string
	Logs         []string
	Err          error
}

type JailStartupOrderUpdateResult struct {
	Name         string
	StartupOrder string
	Logs         []string
	Err          error
}

type JailLinuxMetadataUpdateResult struct {
	Name   string
	Values jailWizardValues
	Logs   []string
	Err    error
}

func normalizeJailHostname(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if strings.ContainsAny(value, "\t\n\r") {
		return "", fmt.Errorf("hostname must be a single line")
	}
	return value, nil
}

func validateLinuxMetadataValues(values jailWizardValues) (jailWizardValues, error) {
	values.LinuxPreset = effectiveLinuxBootstrapPreset(values)
	switch values.LinuxPreset {
	case "custom", "alpine", "rocky":
	default:
		return jailWizardValues{}, fmt.Errorf("bootstrap preset must be custom, alpine, or rocky")
	}
	values.LinuxDistro = effectiveLinuxDistro(values)
	if !jailNamePattern.MatchString(values.LinuxDistro) {
		return jailWizardValues{}, fmt.Errorf("bootstrap family must use letters, numbers, dot, underscore, or dash")
	}
	values.LinuxBootstrapMethod = effectiveLinuxBootstrapMethod(values)
	switch values.LinuxBootstrapMethod {
	case "debootstrap", "archive":
	default:
		return jailWizardValues{}, fmt.Errorf("bootstrap method must be debootstrap or archive")
	}
	values.LinuxBootstrap = effectiveLinuxBootstrapMode(values)
	switch values.LinuxBootstrap {
	case "auto", "skip":
	default:
		return jailWizardValues{}, fmt.Errorf("bootstrap mode must be auto or skip")
	}
	values.LinuxMirrorMode = effectiveLinuxMirrorMode(values)
	switch values.LinuxBootstrapMethod {
	case "debootstrap":
		values.LinuxRelease = effectiveLinuxRelease(values)
		if err := validateLinuxBootstrapReleaseValue(values.LinuxRelease); err != nil {
			return jailWizardValues{}, err
		}
		switch values.LinuxMirrorMode {
		case "default", "custom":
		default:
			return jailWizardValues{}, fmt.Errorf("mirror mode must be default or custom")
		}
		if _, err := resolveLinuxBootstrapSource(values); err != nil {
			if values.LinuxMirrorMode == "custom" {
				return jailWizardValues{}, fmt.Errorf("mirror URL: %w", err)
			}
			return jailWizardValues{}, err
		}
	case "archive":
		if _, err := resolveLinuxBootstrapSource(values); err != nil {
			return jailWizardValues{}, err
		}
	}
	return values, nil
}

func ExecuteJailHostnameUpdate(detail JailDetail, hostname string) JailHostnameUpdateResult {
	result := JailHostnameUpdateResult{Name: strings.TrimSpace(detail.Name), Hostname: strings.TrimSpace(hostname)}
	logs := make([]string, 0, 8)
	fail := func(err error) JailHostnameUpdateResult {
		result.Logs = logs
		result.Err = err
		return result
	}
	configPath := strings.TrimSpace(detail.JailConfSource)
	if result.Name == "" || configPath == "" {
		return fail(fmt.Errorf("no jail config source is available for hostname editing"))
	}
	info, err := os.Stat(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to inspect %q: %w", configPath, err))
	}
	if _, err := backupFileForMutation(configPath, "jail-hostname-"+result.Name, &logs); err != nil {
		return fail(err)
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to read %q: %w", configPath, err))
	}
	updated, err := updateJailConfigValue(string(content), result.Name, "host.hostname", quotedConfigValue(result.Hostname))
	if err != nil {
		return fail(err)
	}
	logs = append(logs, "$ write "+configPath)
	if err := writeFileAtomicReplace(configPath, []byte(updated), info.Mode().Perm()); err != nil {
		return fail(fmt.Errorf("failed to update %q: %w", configPath, err))
	}
	result.Logs = logs
	return result
}

func ExecuteJailDependencyUpdate(detail JailDetail, dependencies string) JailDependencyUpdateResult {
	result := JailDependencyUpdateResult{Name: strings.TrimSpace(detail.Name), Dependencies: strings.TrimSpace(dependencies)}
	logs := make([]string, 0, 8)
	fail := func(err error) JailDependencyUpdateResult {
		result.Logs = logs
		result.Err = err
		return result
	}
	configPath := strings.TrimSpace(detail.JailConfSource)
	if result.Name == "" || configPath == "" {
		return fail(fmt.Errorf("no jail config source is available for dependency editing"))
	}
	info, err := os.Stat(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to inspect %q: %w", configPath, err))
	}
	if _, err := backupFileForMutation(configPath, "jail-depend-"+result.Name, &logs); err != nil {
		return fail(err)
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to read %q: %w", configPath, err))
	}
	updated, err := updateJailConfigValue(string(content), result.Name, "depend", rawConfigValue(strings.Join(mustParseJailDependencyNames(result.Dependencies), ", ")))
	if err != nil {
		return fail(err)
	}
	logs = append(logs, "$ write "+configPath)
	if err := writeFileAtomicReplace(configPath, []byte(updated), info.Mode().Perm()); err != nil {
		return fail(fmt.Errorf("failed to update %q: %w", configPath, err))
	}
	result.Logs = logs
	return result
}

func ExecuteJailStartupOrderUpdate(detail JailDetail, values jailWizardValues) JailStartupOrderUpdateResult {
	result := JailStartupOrderUpdateResult{Name: strings.TrimSpace(detail.Name), StartupOrder: strings.TrimSpace(values.StartupOrder)}
	logs := make([]string, 0, 8)
	if result.Name == "" {
		result.Err = fmt.Errorf("jail name is required")
		return result
	}
	if _, err := parseStartupOrderValue(values.StartupOrder); err != nil {
		result.Err = err
		return result
	}
	_, err := updateJailStartupConfig(context.Background(), result.Name, values, &logs)
	result.Logs = logs
	result.Err = err
	return result
}

func ExecuteJailLinuxMetadataUpdate(detail JailDetail, values jailWizardValues) JailLinuxMetadataUpdateResult {
	result := JailLinuxMetadataUpdateResult{Name: strings.TrimSpace(detail.Name), Values: values}
	logs := make([]string, 0, 8)
	fail := func(err error) JailLinuxMetadataUpdateResult {
		result.Logs = logs
		result.Err = err
		return result
	}
	configPath := strings.TrimSpace(detail.JailConfSource)
	if result.Name == "" || configPath == "" {
		return fail(fmt.Errorf("no jail config source is available for linux metadata editing"))
	}
	info, err := os.Stat(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to inspect %q: %w", configPath, err))
	}
	if _, err := backupFileForMutation(configPath, "jail-linux-meta-"+result.Name, &logs); err != nil {
		return fail(err)
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to read %q: %w", configPath, err))
	}
	updated, err := updateJailConfigLinuxMetadata(string(content), result.Name, result.Values)
	if err != nil {
		return fail(err)
	}
	logs = append(logs, "$ write "+configPath)
	if err := writeFileAtomicReplace(configPath, []byte(updated), info.Mode().Perm()); err != nil {
		return fail(fmt.Errorf("failed to update %q: %w", configPath, err))
	}
	result.Logs = logs
	return result
}

type configValueMode int

const (
	configValueRaw configValueMode = iota
	configValueQuoted
)

type configValueUpdate struct {
	mode  configValueMode
	value string
}

func rawConfigValue(value string) configValueUpdate {
	return configValueUpdate{mode: configValueRaw, value: strings.TrimSpace(value)}
}

func quotedConfigValue(value string) configValueUpdate {
	return configValueUpdate{mode: configValueQuoted, value: strings.TrimSpace(value)}
}

func updateJailConfigValue(content, jailName, key string, update configValueUpdate) (string, error) {
	lines := strings.Split(content, "\n")
	start, end, found := findJailBlockBounds(lines, jailName)
	if !found {
		return "", fmt.Errorf("jail %q was not found in %q", jailName, jailConfigPathForName(jailName))
	}
	if end <= start {
		return "", fmt.Errorf("failed to locate the end of jail %q in its config", jailName)
	}
	updatedBlock := upsertJailConfigValue(lines[start+1:end], key, update)
	updatedLines := make([]string, 0, len(lines)-((end-start)-1)+len(updatedBlock))
	updatedLines = append(updatedLines, lines[:start+1]...)
	updatedLines = append(updatedLines, updatedBlock...)
	updatedLines = append(updatedLines, lines[end:]...)
	return strings.Join(updatedLines, "\n"), nil
}

func upsertJailConfigValue(blockLines []string, key string, update configValueUpdate) []string {
	cleaned := make([]string, 0, len(blockLines)+1)
	insertIdx := -1
	prefix := key + " ="
	for _, line := range blockLines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, prefix) || strings.HasPrefix(trimmed, key+"=") {
			if insertIdx < 0 {
				insertIdx = len(cleaned)
			}
			continue
		}
		if insertIdx < 0 && (strings.HasPrefix(trimmed, "host.hostname =") || strings.HasPrefix(trimmed, "path =") || strings.HasPrefix(trimmed, "# freebsd-jails-tui:")) {
			insertIdx = len(cleaned) + 1
		}
		cleaned = append(cleaned, line)
	}
	if strings.TrimSpace(update.value) == "" {
		return cleaned
	}
	if insertIdx < 0 {
		insertIdx = len(cleaned)
	}
	rendered := renderConfigValueLine(key, update)
	cleaned = append(cleaned, "")
	copy(cleaned[insertIdx+1:], cleaned[insertIdx:])
	cleaned[insertIdx] = rendered
	return cleaned
}

func renderConfigValueLine(key string, update configValueUpdate) string {
	if update.mode == configValueQuoted {
		return fmt.Sprintf("  %s = %q;", key, update.value)
	}
	return fmt.Sprintf("  %s = %s;", key, update.value)
}

func updateJailConfigLinuxMetadata(content, jailName string, values jailWizardValues) (string, error) {
	lines := strings.Split(content, "\n")
	start, end, found := findJailBlockBounds(lines, jailName)
	if !found {
		return "", fmt.Errorf("jail %q was not found in %q", jailName, jailConfigPathForName(jailName))
	}
	if end <= start {
		return "", fmt.Errorf("failed to locate the end of jail %q in its config", jailName)
	}
	updatedBlock := upsertJailLinuxMetadata(lines[start+1:end], values)
	updatedLines := make([]string, 0, len(lines)-((end-start)-1)+len(updatedBlock))
	updatedLines = append(updatedLines, lines[:start+1]...)
	updatedLines = append(updatedLines, updatedBlock...)
	updatedLines = append(updatedLines, lines[end:]...)
	return strings.Join(updatedLines, "\n"), nil
}

func upsertJailLinuxMetadata(blockLines []string, values jailWizardValues) []string {
	cleaned := make([]string, 0, len(blockLines)+1)
	insertIdx := -1
	keys := []string{"linux_preset", "linux_distro", "linux_bootstrap_method", "linux_release", "linux_bootstrap", "linux_mirror_mode", "linux_mirror_url", "linux_archive_url"}
	for _, line := range blockLines {
		updated := line
		for _, key := range keys {
			if stripped, changed := stripTUIMetadataKey(updated, key); changed {
				updated = stripped
			}
		}
		if strings.TrimSpace(updated) == "" {
			continue
		}
		if insertIdx < 0 {
			trimmed := strings.TrimSpace(updated)
			if strings.HasPrefix(trimmed, "# freebsd-jails-tui:") {
				insertIdx = len(cleaned)
			}
		}
		cleaned = append(cleaned, updated)
	}
	if insertIdx < 0 {
		insertIdx = 0
		for idx, line := range cleaned {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "host.hostname =") || strings.HasPrefix(trimmed, "path =") {
				insertIdx = idx + 1
			}
		}
	}
	metaLine := fmt.Sprintf("  # freebsd-jails-tui: linux_preset=%s linux_distro=%s linux_bootstrap_method=%s linux_release=%s linux_bootstrap=%s linux_mirror_mode=%s linux_mirror_url=%s linux_archive_url=%s;",
		effectiveLinuxBootstrapPreset(values),
		effectiveLinuxDistro(values),
		effectiveLinuxBootstrapMethod(values),
		effectiveLinuxRelease(values),
		effectiveLinuxBootstrapMode(values),
		effectiveLinuxMirrorMode(values),
		linuxMirrorMetadataValue(values),
		linuxArchiveMetadataValue(values),
	)
	cleaned = append(cleaned, "")
	copy(cleaned[insertIdx+1:], cleaned[insertIdx:])
	cleaned[insertIdx] = metaLine
	return cleaned
}

func ExecuteBulkNoteUpdate(targets []Jail, note string) bulkActionResult {
	result := bulkActionResult{Kind: bulkActionNote, Input: strings.TrimSpace(note)}
	logs := make([]string, 0, len(targets))
	note, err := normalizeJailNote(note)
	if err != nil {
		result.Err = err
		return result
	}
	for _, target := range targets {
		item := bulkActionTargetResult{Name: target.Name}
		detail, detailErr := CollectJailDetail(target.Name, target.JID, target.Path, time.Now())
		if detailErr != nil {
			item.Err = detailErr
			result.Results = append(result.Results, item)
			continue
		}
		update := ExecuteJailNoteUpdate(detail, note)
		item.Logs = update.Logs
		item.Err = update.Err
		result.Results = append(result.Results, item)
		logs = append(logs, update.Logs...)
	}
	result.Logs = logs
	return result
}

func ExecuteBulkSnapshotCreate(targets []Jail, snapshotName string) bulkActionResult {
	result := bulkActionResult{Kind: bulkActionSnapshot, Input: strings.TrimSpace(snapshotName)}
	name := strings.TrimSpace(snapshotName)
	if name == "" || strings.Contains(name, "@") || strings.ContainsAny(name, " \t") {
		result.Err = fmt.Errorf("invalid snapshot name")
		return result
	}
	for _, target := range targets {
		item := bulkActionTargetResult{Name: target.Name}
		detail, err := CollectJailDetail(target.Name, target.JID, target.Path, time.Now())
		if err != nil {
			item.Err = err
			result.Results = append(result.Results, item)
			continue
		}
		if detail.ZFS == nil || detail.ZFS.MatchType != "exact" || strings.TrimSpace(detail.ZFS.Name) == "" {
			item.Reason = "no exact ZFS dataset"
			result.Results = append(result.Results, item)
			continue
		}
		var logs []string
		fullName := detail.ZFS.Name + "@" + name
		if _, err := runLoggedCommand(context.Background(), &logs, "zfs", "snapshot", fullName); err != nil {
			item.Logs = logs
			item.Err = err
			result.Results = append(result.Results, item)
			continue
		}
		item.Logs = logs
		result.Results = append(result.Results, item)
	}
	return result
}
