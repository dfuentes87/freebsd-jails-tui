package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type RCSettingSource struct {
	Path  string
	Value string
}

type RCSettingStatus struct {
	Key          string
	Expected     string
	Effective    string
	SourceValues []RCSettingSource
	DriftReasons []string
	ReadError    string
}

func collectRCSettingStatus(key, expected string) RCSettingStatus {
	status := RCSettingStatus{
		Key:      strings.TrimSpace(key),
		Expected: strings.TrimSpace(expected),
	}
	value, err := readRCConfValue(status.Key)
	if err != nil {
		status.ReadError = err.Error()
	} else {
		status.Effective = strings.TrimSpace(value)
	}

	sourceValues, sourceErr := collectRCConfSourceValues(status.Key)
	if sourceErr != nil {
		if status.ReadError == "" {
			status.ReadError = sourceErr.Error()
		}
	} else {
		status.SourceValues = sourceValues
	}

	if status.Expected != "" && !matchesExpectedRCValue(status.Effective, status.Expected) {
		status.DriftReasons = append(status.DriftReasons, fmt.Sprintf("effective value is %s, expected %s", displayRCValue(status.Effective), status.Expected))
	}

	if rcSettingHasConflictingSources(status) {
		status.DriftReasons = append(status.DriftReasons, "conflicting values are defined across rc.conf files")
	}
	if status.ReadError != "" {
		status.DriftReasons = append(status.DriftReasons, status.ReadError)
	}
	return status
}

func rcSettingHasConflictingSources(status RCSettingStatus) bool {
	unique := map[string]struct{}{}
	for _, source := range status.SourceValues {
		unique[strings.ToUpper(strings.TrimSpace(source.Value))] = struct{}{}
	}
	return len(unique) > 1
}

func ensureRCSettingSafeToMutate(key string) error {
	status := collectRCSettingStatus(strings.TrimSpace(key), "")
	if status.ReadError != "" {
		return fmt.Errorf("failed to inspect %s before update: %s", status.Key, status.ReadError)
	}
	if rcSettingHasConflictingSources(status) {
		return fmt.Errorf("%s has conflicting values across /etc/rc.conf and /etc/rc.conf.local; resolve them manually before the TUI updates it", status.Key)
	}
	return nil
}

func matchesExpectedRCValue(value, expected string) bool {
	value = strings.TrimSpace(value)
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return true
	}
	if strings.EqualFold(expected, "YES") {
		return isEnabledRCValue(value)
	}
	return strings.EqualFold(value, expected)
}

func collectRCConfSourceValues(key string) ([]RCSettingSource, error) {
	paths := []string{"/etc/rc.conf", "/etc/rc.conf.local"}
	values := make([]RCSettingSource, 0, len(paths))
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("failed reading %s: %w", path, err)
		}
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
			values = append(values, RCSettingSource{
				Path:  path,
				Value: strings.TrimSpace(strings.Trim(right, `"'`)),
			})
		}
		file.Close()
	}
	return values, nil
}
