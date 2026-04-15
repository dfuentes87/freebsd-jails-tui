package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const (
	loaderConfPath = "/boot/loader.conf"
	rctlConfPath   = "/etc/rctl.conf"
)

type RacctStatus struct {
	Enabled          bool
	EffectiveValue   string
	LoaderConfigured bool
	LoaderValue      string
	ReadError        string
}

type JailRctlConfig struct {
	Mode          string
	CPUPercent    string
	MemoryLimit   string
	ProcessLimit  string
	Persistent    bool
	PersistentErr string
}

type managedRctlBlockMalformedError struct {
	JailName string
}

func (e managedRctlBlockMalformedError) Error() string {
	return fmt.Sprintf("managed rctl block for jail %q is missing end marker in %s; refusing to rewrite it", e.JailName, rctlConfPath)
}

func isManagedRctlBlockMalformedError(err error) bool {
	var target managedRctlBlockMalformedError
	return errors.As(err, &target)
}

func hasAnyRctlLimits(values jailWizardValues) bool {
	return strings.TrimSpace(values.CPUPercent) != "" ||
		strings.TrimSpace(values.MemoryLimit) != "" ||
		strings.TrimSpace(values.ProcessLimit) != ""
}

func effectiveRctlLimitMode(values jailWizardValues) string {
	return "persistent"
}

func collectRacctStatus() RacctStatus {
	status := RacctStatus{}
	out, err := exec.Command("sysctl", "-n", "kern.racct.enable").CombinedOutput()
	if err != nil {
		status.ReadError = strings.TrimSpace(string(out))
		if status.ReadError == "" {
			status.ReadError = err.Error()
		}
	} else {
		status.EffectiveValue = strings.TrimSpace(string(out))
		status.Enabled = status.EffectiveValue == "1"
	}

	value, readErr := readLoaderConfValue("kern.racct.enable")
	if readErr != nil {
		if status.ReadError == "" {
			status.ReadError = readErr.Error()
		}
	} else {
		status.LoaderValue = value
		status.LoaderConfigured = strings.TrimSpace(value) == "1"
	}
	return status
}

func readLoaderConfValue(key string) (string, error) {
	file, err := os.Open(loaderConfPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("failed reading %s: %w", loaderConfPath, err)
	}
	defer file.Close()

	var last string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(stripInlineComment(scanner.Text()))
		if line == "" {
			continue
		}
		left, right, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(left) != key {
			continue
		}
		last = strings.TrimSpace(strings.Trim(right, `"'`))
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("failed scanning %s: %w", loaderConfPath, err)
	}
	return last, nil
}

func validateRacctPreflight(values jailWizardValues) error {
	if !hasAnyRctlLimits(values) {
		return nil
	}
	status := collectRacctStatus()
	if !status.Enabled {
		return fmt.Errorf("resource limits require kern.racct.enable=1 and a reboot before rctl limits can be applied")
	}
	return nil
}

func managedRctlRulesForJail(values jailWizardValues, jailName string) []string {
	jailName = strings.TrimSpace(jailName)
	if jailName == "" || !hasAnyRctlLimits(values) {
		return nil
	}
	rules := make([]string, 0, 3)
	if cpu := strings.TrimSpace(values.CPUPercent); cpu != "" {
		rules = append(rules, fmt.Sprintf("jail:%s:pcpu:deny=%s", jailName, cpu))
	}
	if memory := strings.TrimSpace(values.MemoryLimit); memory != "" {
		rules = append(rules, fmt.Sprintf("jail:%s:memoryuse:deny=%s", jailName, strings.ToUpper(memory)))
	}
	if maxproc := strings.TrimSpace(values.ProcessLimit); maxproc != "" {
		rules = append(rules, fmt.Sprintf("jail:%s:maxproc:deny=%s", jailName, maxproc))
	}
	return rules
}

func syncPersistentJailRctlRules(ctx context.Context, values jailWizardValues, jailName string, logs *[]string) (func(), error) {
	return rewriteManagedJailRctlBlock(jailName, managedRctlRulesForJail(values, jailName), logs)
}

func removePersistentJailRctlRules(jailName string, logs *[]string) (func(), error) {
	return rewriteManagedJailRctlBlock(jailName, nil, logs)
}

func rewriteManagedJailRctlBlock(jailName string, rules []string, logs *[]string) (func(), error) {
	jailName = strings.TrimSpace(jailName)
	if jailName == "" {
		return nil, nil
	}
	existing, err := readRctlConfLines()
	if err != nil {
		return nil, err
	}
	updated, changed, err := replaceManagedRctlBlock(existing, jailName, rules)
	if err != nil {
		return nil, err
	}
	if !changed {
		return nil, nil
	}
	backup, err := backupFileForMutation(rctlConfPath, "rctl-conf", logs)
	if err != nil {
		return nil, err
	}
	if err := writeRctlConfLines(updated, logs); err != nil {
		return nil, err
	}
	return func() {
		if err := restoreFileMutationBackup(backup, logs); err != nil {
			*logs = append(*logs, "rollback warning: "+err.Error())
		}
	}, nil
}

func readRctlConfLines() ([]string, error) {
	data, err := os.ReadFile(rctlConfPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read %s: %w", rctlConfPath, err)
	}
	return strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n"), nil
}

func writeRctlConfLines(lines []string, logs *[]string) error {
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	content := strings.Join(lines, "\n")
	if content != "" {
		content += "\n"
	}
	if logs != nil {
		*logs = append(*logs, "$ write "+rctlConfPath)
	}
	if err := writeFileAtomicReplace(rctlConfPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write %s: %w", rctlConfPath, err)
	}
	return nil
}

func replaceManagedRctlBlock(lines []string, jailName string, rules []string) ([]string, bool, error) {
	begin := fmt.Sprintf("# freebsd-jails-tui: rctl=%s begin", jailName)
	end := fmt.Sprintf("# freebsd-jails-tui: rctl=%s end", jailName)
	updated := make([]string, 0, len(lines)+len(rules)+4)
	inBlock := false
	removed := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == begin {
			inBlock = true
			removed = true
			continue
		}
		if inBlock {
			if trimmed == end {
				inBlock = false
			}
			continue
		}
		updated = append(updated, line)
	}
	if inBlock {
		return nil, false, managedRctlBlockMalformedError{JailName: jailName}
	}
	for len(updated) > 0 && strings.TrimSpace(updated[len(updated)-1]) == "" {
		updated = updated[:len(updated)-1]
	}
	if len(rules) == 0 {
		return updated, removed, nil
	}
	if len(updated) > 0 {
		updated = append(updated, "")
	}
	updated = append(updated, begin)
	updated = append(updated, rules...)
	updated = append(updated, end)
	return updated, true, nil
}

func rctlConfigFromRawLines(lines []string) *JailRctlConfig {
	cfg := &JailRctlConfig{
		Mode: "runtime",
	}
	jailName := cfgJailNameFromLines(lines)
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if !strings.Contains(line, "freebsd-jails-tui:") {
			continue
		}
		parts := strings.SplitN(line, "freebsd-jails-tui:", 2)
		if len(parts) != 2 {
			continue
		}
		meta := strings.TrimSpace(strings.TrimSuffix(parts[1], ";"))
		for _, token := range strings.Fields(meta) {
			key, value, ok := strings.Cut(token, "=")
			if !ok {
				continue
			}
			switch strings.TrimSpace(key) {
			case "rctl_mode":
				if strings.TrimSpace(value) == "persistent" {
					cfg.Mode = "persistent"
				} else {
					cfg.Mode = "runtime"
				}
			case "cpu_percent":
				if strings.TrimSpace(value) != "-" {
					cfg.CPUPercent = strings.TrimSpace(value)
				}
			case "memory_limit":
				if strings.TrimSpace(value) != "-" {
					cfg.MemoryLimit = strings.TrimSpace(value)
				}
			case "process_limit":
				if strings.TrimSpace(value) != "-" {
					cfg.ProcessLimit = strings.TrimSpace(value)
				}
			}
		}
	}
	if cfg.Mode == "runtime" && cfg.CPUPercent == "" && cfg.MemoryLimit == "" && cfg.ProcessLimit == "" {
		return nil
	}
	cfg.Persistent, cfg.PersistentErr = hasManagedRctlBlock(jailName)
	return cfg
}

func cfgJailNameFromLines(lines []string) string {
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || line == "}" {
			continue
		}
		if strings.HasSuffix(line, "{") {
			return strings.TrimSpace(strings.TrimSuffix(line, "{"))
		}
	}
	return ""
}

func hasManagedRctlBlock(jailName string) (bool, string) {
	jailName = strings.TrimSpace(jailName)
	if jailName == "" {
		return false, ""
	}
	lines, err := readRctlConfLines()
	if err != nil {
		return false, err.Error()
	}
	begin := fmt.Sprintf("# freebsd-jails-tui: rctl=%s begin", jailName)
	for _, line := range lines {
		if strings.TrimSpace(line) == begin {
			return true, ""
		}
	}
	return false, ""
}
