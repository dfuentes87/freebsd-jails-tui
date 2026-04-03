package main

import (
	"fmt"
	"strings"
)

type JailStartupConfig struct {
	Dependencies []string
	JailList     []string
	InJailList   bool
	Position     int
	ReadError    string
}

func collectJailStartupConfig(detail JailDetail) *JailStartupConfig {
	config := &JailStartupConfig{
		Dependencies: mustParseJailDependencyNames(detail.JailConfValues["depend"]),
	}
	value, err := readRCConfValue("jail_list")
	if err != nil {
		config.ReadError = err.Error()
		return config
	}
	config.JailList = parseJailListValue(value)
	for idx, item := range config.JailList {
		if item != detail.Name {
			continue
		}
		config.InJailList = true
		config.Position = idx + 1
		break
	}
	return config
}

func parseJailListValue(raw string) []string {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return nil
	}
	list := make([]string, 0, len(fields))
	seen := map[string]struct{}{}
	for _, field := range fields {
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		list = append(list, field)
	}
	return list
}

func formatJailListValue(list []string) string {
	if len(list) == 0 {
		return ""
	}
	return strings.Join(list, " ")
}

func updateJailStartupConfig(name string, values jailWizardValues, logs *[]string) (func(), error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("jail name is required for startup order")
	}
	position, err := parseStartupOrderValue(values.StartupOrder)
	if err != nil {
		return nil, err
	}

	oldValue, oldErr := readRCConfValue("jail_list")
	if oldErr != nil {
		return nil, oldErr
	}
	oldList := parseJailListValue(oldValue)
	if len(oldList) == 0 && position == 0 {
		return nil, nil
	}
	baseList := append([]string(nil), oldList...)
	if len(baseList) == 0 && position > 0 {
		baseList = discoverConfiguredJails()
	}
	newList := applyJailListPosition(baseList, name, position)
	if formatJailListValue(oldList) == formatJailListValue(newList) {
		return nil, nil
	}

	newValue := formatJailListValue(newList)
	if _, err := runLoggedCommand(logs, "sysrc", "jail_list="+newValue); err != nil {
		return nil, fmt.Errorf("failed to update jail_list: %w", err)
	}

	restoreValue := formatJailListValue(oldList)
	return func() {
		if restoreValue == "" {
			if _, err := runLoggedCommand(logs, "sysrc", "-x", "jail_list"); err != nil {
				*logs = append(*logs, "  rollback warning: failed to clear jail_list: "+err.Error())
			}
			return
		}
		if _, err := runLoggedCommand(logs, "sysrc", "jail_list="+restoreValue); err != nil {
			*logs = append(*logs, "  rollback warning: failed to restore jail_list: "+err.Error())
		}
	}, nil
}

func applyJailListPosition(list []string, name string, position int) []string {
	filtered := make([]string, 0, len(list)+1)
	for _, item := range list {
		if item == name {
			continue
		}
		filtered = append(filtered, item)
	}
	if position <= 0 || position > len(filtered)+1 {
		return append(filtered, name)
	}
	idx := position - 1
	filtered = append(filtered[:idx], append([]string{name}, filtered[idx:]...)...)
	return filtered
}
