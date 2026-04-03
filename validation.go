package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

var zfsDatasetComponentPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

func normalizeAbsolutePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return filepath.Clean(value)
}

func containsControlOrNewline(value string) bool {
	for _, r := range value {
		if r == '\n' || r == '\r' || unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func validateAbsolutePath(value, field string) (string, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if containsControlOrNewline(raw) {
		return "", fmt.Errorf("%s contains invalid control characters", field)
	}
	clean := filepath.Clean(raw)
	if !strings.HasPrefix(clean, "/") {
		return "", fmt.Errorf("%s must be an absolute path", field)
	}
	if clean == "." || clean == "" || clean == "/" {
		return "", fmt.Errorf("%s must not be /", field)
	}
	return clean, nil
}

func validateOptionalAbsolutePath(value, field string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	return validateAbsolutePath(value, field)
}

func validateJailDestinationPath(destination, jailName string) (string, error) {
	clean, err := validateAbsolutePath(destination, "destination")
	if err != nil {
		return "", err
	}
	if jailName = strings.TrimSpace(jailName); jailName != "" && filepath.Base(clean) != jailName {
		return "", fmt.Errorf("destination must end with /%s", jailName)
	}
	return clean, nil
}

func validateAccessibleAbsolutePath(value, field string) (string, error) {
	clean, err := validateAbsolutePath(value, field)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(clean); err != nil {
		return "", fmt.Errorf("%s %q is not accessible", field, clean)
	}
	return clean, nil
}

func validateZFSDatasetName(value, field string) (string, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if containsControlOrNewline(raw) {
		return "", fmt.Errorf("%s contains invalid control characters", field)
	}
	if strings.HasPrefix(raw, "/") || strings.HasSuffix(raw, "/") {
		return "", fmt.Errorf("%s %q is invalid", field, raw)
	}
	parts := strings.Split(raw, "/")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("%s %q is invalid", field, raw)
		}
		if !zfsDatasetComponentPattern.MatchString(part) {
			return "", fmt.Errorf("%s %q is invalid; allowed characters are letters, numbers, ., _, :, and -", field, raw)
		}
	}
	return strings.Join(parts, "/"), nil
}

func validateOptionalZFSDatasetName(value, field string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	return validateZFSDatasetName(value, field)
}

func validateTemplateRenameLeafName(value string) (string, error) {
	name := strings.TrimSpace(value)
	if name == "" {
		return "", fmt.Errorf("new template name is required")
	}
	if containsControlOrNewline(name) {
		return "", fmt.Errorf("new template name contains invalid control characters")
	}
	if !templateDatasetLeafPattern.MatchString(name) {
		return "", fmt.Errorf("new template name %q is invalid; allowed characters are letters, numbers, ., _, -", name)
	}
	return name, nil
}

func validateMountTarget(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("mount target is required")
	}
	if containsControlOrNewline(target) {
		return "", fmt.Errorf("mount target contains invalid control characters")
	}
	clean := filepath.Clean("/" + strings.TrimPrefix(target, "/"))
	if clean == "." || clean == "/" {
		return "", fmt.Errorf("mount target must not be /")
	}
	if !strings.HasPrefix(clean, "/") {
		return "", fmt.Errorf("mount target %q is invalid", target)
	}
	return clean, nil
}

func validateMountTargetPath(jailPath, target string) (string, string, error) {
	cleanTarget, err := validateMountTarget(target)
	if err != nil {
		return "", "", err
	}
	cleanJailPath, err := validateAbsolutePath(jailPath, "jail path")
	if err != nil {
		return "", "", err
	}
	targetPath := filepath.Clean(filepath.Join(cleanJailPath, strings.TrimPrefix(cleanTarget, "/")))
	if targetPath == cleanJailPath || !strings.HasPrefix(targetPath, cleanJailPath+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("mount target %q escapes jail root %q", target, cleanJailPath)
	}
	return cleanTarget, targetPath, nil
}
