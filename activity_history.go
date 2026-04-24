package main

import (
	"path/filepath"
	"strings"
)

type activityLogSummary struct {
	Commands         []string
	FilesTouched     []string
	RollbackWarnings []string
}

func summarizeActivityLogs(logs []string) activityLogSummary {
	summary := activityLogSummary{}
	commandSeen := map[string]struct{}{}
	fileSeen := map[string]struct{}{}
	rollbackSeen := map[string]struct{}{}

	appendUnique := func(list *[]string, seen map[string]struct{}, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		*list = append(*list, value)
	}

	for _, raw := range logs {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "$ ") {
			command := strings.TrimSpace(strings.TrimPrefix(line, "$ "))
			appendUnique(&summary.Commands, commandSeen, command)
			if strings.HasPrefix(command, "write ") {
				path := strings.TrimSpace(strings.TrimPrefix(command, "write "))
				appendUnique(&summary.FilesTouched, fileSeen, filepath.Clean(path))
			}
			continue
		}
		if strings.HasPrefix(line, "restore: ") {
			appendUnique(&summary.RollbackWarnings, rollbackSeen, line)
			path := strings.TrimSpace(strings.TrimPrefix(line, "restore: "))
			if original, _, ok := strings.Cut(path, "<-"); ok {
				appendUnique(&summary.FilesTouched, fileSeen, filepath.Clean(strings.TrimSpace(original)))
			}
			continue
		}
		if strings.Contains(strings.ToLower(line), "rollback warning:") {
			appendUnique(&summary.RollbackWarnings, rollbackSeen, line)
		}
	}

	return summary
}

func buildPostCreateChecklist(values jailWizardValues, warnings []string, success bool) []string {
	if normalizedJailType(values.JailType) != "linux" {
		return nil
	}

	actions := []string{
		"Open the jail detail view and review the Summary or Drift tab for current blockers and Linux readiness.",
	}

	method := effectiveLinuxBootstrapMethod(values)
	mode := effectiveLinuxBootstrapMode(values)

	if mode == "skip" {
		if method == "archive" {
			actions = append(actions, "Stop the jail when ready, then use detail view action 'b' to run archive bootstrap.")
		} else {
			actions = append(actions, "Use detail view action 'b' to run Linux bootstrap after networking is ready inside the jail.")
		}
	} else {
		for _, warning := range warnings {
			summary := summarizeCreationWarning(warning)
			if strings.TrimSpace(summary) == "" {
				continue
			}
			actions = append(actions, firstUpper(summary))
		}
	}

	if success {
		actions = append(actions, "Review the Runtime tab to confirm DNS, package manager, and init/service health after bootstrap.")
	}

	return uniqueStrings(actions)
}

func firstUpper(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.ToUpper(value[:1]) + value[1:]
}
