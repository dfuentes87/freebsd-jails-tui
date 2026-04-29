package main

import (
	"context"
	"fmt"
	"os"
	"strings"
)

type JailNoteUpdateResult struct {
	Name string
	Note string
	Logs []string
	Err  error
}

type JailRctlUpdateResult struct {
	Name   string
	Config *JailRctlConfig
	Logs   []string
	Err    error
}

func ExecuteJailNoteUpdate(detail JailDetail, note string) JailNoteUpdateResult {
	result := JailNoteUpdateResult{
		Name: strings.TrimSpace(detail.Name),
	}
	logs := make([]string, 0, 8)
	fail := func(err error) JailNoteUpdateResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	if result.Name == "" {
		return fail(fmt.Errorf("jail name is required"))
	}
	note, err := normalizeJailNote(note)
	if err != nil {
		return fail(err)
	}
	configPath := strings.TrimSpace(detail.JailConfSource)
	if configPath == "" {
		return fail(fmt.Errorf("no jail config source is available for note editing"))
	}
	info, err := os.Stat(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to inspect %q: %w", configPath, err))
	}
	if _, err := backupFileForMutation(configPath, "jail-note-"+result.Name, &logs); err != nil {
		return fail(err)
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to read %q: %w", configPath, err))
	}
	updated, err := updateJailConfigNote(string(content), result.Name, note)
	if err != nil {
		return fail(err)
	}
	logs = append(logs, fmt.Sprintf("$ write %s", configPath))
	if err := writeFileAtomicReplace(configPath, []byte(updated), info.Mode().Perm()); err != nil {
		return fail(fmt.Errorf("failed to update %q: %w", configPath, err))
	}
	result.Note = note
	result.Logs = logs
	return result
}

func updateJailConfigNote(content, jailName, note string) (string, error) {
	lines := strings.Split(content, "\n")
	start, end, found := findJailBlockBounds(lines, jailName)
	if !found {
		return "", fmt.Errorf("jail %q was not found in %q", jailName, jailConfigPathForName(jailName))
	}
	if end <= start {
		return "", fmt.Errorf("failed to locate the end of jail %q in its config", jailName)
	}

	updatedBlock := upsertJailNoteMetadata(lines[start+1:end], note)
	updatedLines := make([]string, 0, len(lines)-((end-start)-1)+len(updatedBlock))
	updatedLines = append(updatedLines, lines[:start+1]...)
	updatedLines = append(updatedLines, updatedBlock...)
	updatedLines = append(updatedLines, lines[end:]...)
	return strings.Join(updatedLines, "\n"), nil
}

func ExecuteJailRctlUpdate(detail JailDetail, values jailWizardValues) JailRctlUpdateResult {
	result := JailRctlUpdateResult{
		Name: strings.TrimSpace(detail.Name),
	}
	logs := make([]string, 0, 16)
	fail := func(err error) JailRctlUpdateResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	if result.Name == "" {
		return fail(fmt.Errorf("jail name is required"))
	}
	normalized, _, err := normalizeRctlLimitValues(values)
	if err != nil {
		return fail(err)
	}
	configPath := strings.TrimSpace(detail.JailConfSource)
	if configPath == "" {
		return fail(fmt.Errorf("no jail config source is available for resource limit editing"))
	}
	info, err := os.Stat(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to inspect %q: %w", configPath, err))
	}
	configBackup, err := backupFileForMutation(configPath, "jail-rctl-"+result.Name, &logs)
	if err != nil {
		return fail(err)
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fail(fmt.Errorf("failed to read %q: %w", configPath, err))
	}
	updated, err := updateJailConfigRctlMetadata(string(content), result.Name, normalized)
	if err != nil {
		return fail(err)
	}
	logs = append(logs, fmt.Sprintf("$ write %s", configPath))
	if err := writeFileAtomicReplace(configPath, []byte(updated), info.Mode().Perm()); err != nil {
		return fail(fmt.Errorf("failed to update %q: %w", configPath, err))
	}

	restoreConfig := func() {
		if err := restoreFileMutationBackup(configBackup, &logs); err != nil {
			logs = append(logs, "rollback warning: "+err.Error())
		}
	}

	persistentCleanup, err := syncPersistentJailRctlRules(context.Background(), normalized, result.Name, &logs)
	if err != nil {
		restoreConfig()
		return fail(err)
	}
	if detail.JID > 0 && collectRacctStatus().Enabled {
		if err := replaceLiveJailRctlRules(context.Background(), result.Name, detail.JID, normalized, &logs); err != nil {
			if persistentCleanup != nil {
				persistentCleanup()
			}
			restoreConfig()
			return fail(err)
		}
	}

	if hasAnyRctlLimits(normalized) {
		result.Config = &JailRctlConfig{
			Mode:         "persistent",
			CPUPercent:   normalized.CPUPercent,
			MemoryLimit:  normalized.MemoryLimit,
			ProcessLimit: normalized.ProcessLimit,
			Persistent:   true,
		}
	}
	result.Logs = logs
	return result
}

func updateJailConfigRctlMetadata(content, jailName string, values jailWizardValues) (string, error) {
	lines := strings.Split(content, "\n")
	start, end, found := findJailBlockBounds(lines, jailName)
	if !found {
		return "", fmt.Errorf("jail %q was not found in %q", jailName, jailConfigPathForName(jailName))
	}
	if end <= start {
		return "", fmt.Errorf("failed to locate the end of jail %q in its config", jailName)
	}

	updatedBlock := upsertJailRctlMetadata(lines[start+1:end], values)
	updatedLines := make([]string, 0, len(lines)-((end-start)-1)+len(updatedBlock))
	updatedLines = append(updatedLines, lines[:start+1]...)
	updatedLines = append(updatedLines, updatedBlock...)
	updatedLines = append(updatedLines, lines[end:]...)
	return strings.Join(updatedLines, "\n"), nil
}

func upsertJailNoteMetadata(blockLines []string, note string) []string {
	cleaned := make([]string, 0, len(blockLines)+1)
	insertIdx := -1
	for _, line := range blockLines {
		updated, changed := stripTUIMetadataKey(line, "note")
		if changed {
			line = updated
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		if insertIdx < 0 {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "# freebsd-jails-tui:") {
				insertIdx = len(cleaned)
			}
		}
		cleaned = append(cleaned, line)
	}
	if strings.TrimSpace(note) == "" {
		return cleaned
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
	noteLine := fmt.Sprintf("  # freebsd-jails-tui: note=%s;", encodeTUIMetadataValue(note))
	cleaned = append(cleaned, "")
	copy(cleaned[insertIdx+1:], cleaned[insertIdx:])
	cleaned[insertIdx] = noteLine
	return cleaned
}

func upsertJailRctlMetadata(blockLines []string, values jailWizardValues) []string {
	cleaned := make([]string, 0, len(blockLines)+1)
	insertIdx := -1
	for _, line := range blockLines {
		updated := line
		for _, key := range []string{"rctl_mode", "cpu_percent", "memory_limit", "process_limit"} {
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
	if !hasAnyRctlLimits(values) {
		return cleaned
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
	metaLine := fmt.Sprintf("  # freebsd-jails-tui: rctl_mode=persistent cpu_percent=%s memory_limit=%s process_limit=%s;", metadataDashValue(values.CPUPercent), metadataDashValue(values.MemoryLimit), metadataDashValue(values.ProcessLimit))
	cleaned = append(cleaned, "")
	copy(cleaned[insertIdx+1:], cleaned[insertIdx:])
	cleaned[insertIdx] = metaLine
	return cleaned
}

func stripTUIMetadataKey(line, key string) (string, bool) {
	idx := strings.Index(line, "freebsd-jails-tui:")
	if idx < 0 {
		return line, false
	}
	prefix := line[:idx+len("freebsd-jails-tui:")]
	payload := strings.TrimSpace(strings.TrimSuffix(line[idx+len("freebsd-jails-tui:"):], ";"))
	fields := strings.Fields(payload)
	if len(fields) == 0 {
		return line, false
	}
	kept := make([]string, 0, len(fields))
	removed := false
	for _, field := range fields {
		fieldKey, _, ok := strings.Cut(field, "=")
		if ok && strings.TrimSpace(fieldKey) == key {
			removed = true
			continue
		}
		kept = append(kept, field)
	}
	if !removed {
		return line, false
	}
	if len(kept) == 0 {
		return "", true
	}
	return prefix + " " + strings.Join(kept, " ") + ";", true
}

func replaceLiveJailRctlRules(ctx context.Context, jailName string, jid int, values jailWizardValues, logs *[]string) error {
	existing, err := discoverRctlRules(jailName, jid)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		if _, err := runLoggedCommand(ctx, logs, "rctl", "-r", "jail:"+jailName); err != nil {
			return fmt.Errorf("failed to clear existing rctl rules: %w", err)
		}
	}
	if err := applyRctlLimits(ctx, values, jailName, logs); err != nil {
		return err
	}
	return nil
}

func applyRctlLimits(ctx context.Context, values jailWizardValues, jailName string, logs *[]string) error {
	if !hasAnyRctlLimits(values) {
		return nil
	}
	if !collectRacctStatus().Enabled {
		return nil
	}
	if strings.TrimSpace(values.CPUPercent) != "" {
		if _, err := runLoggedCommand(ctx, logs, "rctl", "-a", fmt.Sprintf("jail:%s:pcpu:deny=%s", jailName, strings.TrimSpace(values.CPUPercent))); err != nil {
			return fmt.Errorf("failed to apply CPU rctl limit: %w", err)
		}
	}
	if strings.TrimSpace(values.MemoryLimit) != "" {
		if _, err := runLoggedCommand(ctx, logs, "rctl", "-a", fmt.Sprintf("jail:%s:memoryuse:deny=%s", jailName, strings.ToUpper(strings.TrimSpace(values.MemoryLimit)))); err != nil {
			return fmt.Errorf("failed to apply memory rctl limit: %w", err)
		}
	}
	if strings.TrimSpace(values.ProcessLimit) != "" {
		if _, err := runLoggedCommand(ctx, logs, "rctl", "-a", fmt.Sprintf("jail:%s:maxproc:deny=%s", jailName, strings.TrimSpace(values.ProcessLimit))); err != nil {
			return fmt.Errorf("failed to apply process rctl limit: %w", err)
		}
	}
	return nil
}
