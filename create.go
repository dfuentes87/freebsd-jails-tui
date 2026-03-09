package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var releaseValuePattern = regexp.MustCompile(`^[0-9]+\.[0-9]+-RELEASE`)

type JailCreationResult struct {
	Name       string
	ConfigPath string
	FstabPath  string
	JailPath   string
	Logs       []string
	Err        error
}

func ExecuteJailCreation(values jailWizardValues) JailCreationResult {
	result := JailCreationResult{
		Name: strings.TrimSpace(values.Name),
	}
	logs := make([]string, 0, 32)
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	fail := func(err error) JailCreationResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	validator := newJailCreationWizard()
	validator.values = values
	if err := validator.validateAll(); err != nil {
		return fail(err)
	}
	if result.Name == "" {
		return fail(fmt.Errorf("jail name is required"))
	}
	for _, existing := range discoverConfiguredJails() {
		if existing == result.Name {
			return fail(fmt.Errorf("jail %q already exists in discovered config", result.Name))
		}
	}

	destination := strings.TrimSpace(values.Dataset)
	if destination == "" {
		return fail(fmt.Errorf("destination is required"))
	}

	result.ConfigPath = jailConfigPathForName(result.Name)
	logf("Starting jail creation for %s", result.Name)

	jailPath, err := ensureDestinationJailPath(destination, &logs)
	if err != nil {
		return fail(err)
	}
	result.JailPath = jailPath

	if err := provisionJailRoot(jailPath, strings.TrimSpace(values.TemplateRelease), &logs); err != nil {
		return fail(err)
	}

	mountSpecs := parseMountPointSpecs(values.MountPoints)
	fstabPath, err := configureMountPoints(result.Name, jailPath, mountSpecs, &logs)
	if err != nil {
		return fail(err)
	}
	result.FstabPath = fstabPath

	configLines := buildJailConfBlock(values, jailPath, fstabPath)
	if err := writeJailConfigFile(result.ConfigPath, configLines, &logs); err != nil {
		return fail(err)
	}

	if _, err := runLoggedCommand(&logs, "service", "jail", "start", result.Name); err != nil {
		return fail(err)
	}
	if err := applyRctlLimits(values, result.Name, &logs); err != nil {
		return fail(err)
	}

	logf("Jail %s created successfully.", result.Name)
	result.Logs = logs
	return result
}

func ensureDestinationJailPath(destination string, logs *[]string) (string, error) {
	destination = strings.TrimSpace(destination)
	if strings.HasPrefix(destination, "/") {
		jailPath := filepath.Clean(destination)
		*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", jailPath))
		if err := os.MkdirAll(jailPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create destination path %q: %w", jailPath, err)
		}
		return jailPath, nil
	}

	// Backward compatibility: treat non-absolute values as ZFS dataset names.
	if _, err := runLoggedCommand(logs, "zfs", "list", "-H", "-o", "name", destination); err != nil {
		if _, createErr := runLoggedCommand(logs, "zfs", "create", "-p", destination); createErr != nil {
			return "", fmt.Errorf("failed to ensure dataset %q: %w", destination, createErr)
		}
	}

	mountpointOut, err := runLoggedCommand(logs, "zfs", "list", "-H", "-o", "mountpoint", destination)
	if err != nil {
		return "", fmt.Errorf("failed to discover mountpoint for %q: %w", destination, err)
	}
	mountpoint := strings.TrimSpace(strings.Split(mountpointOut, "\n")[0])
	if mountpoint == "" || mountpoint == "-" || mountpoint == "legacy" {
		mountpoint = "/" + strings.Trim(destination, "/")
	}

	*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", mountpoint))
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return "", fmt.Errorf("failed to create jail path %q: %w", mountpoint, err)
	}
	return mountpoint, nil
}

func provisionJailRoot(jailPath, templateRelease string, logs *[]string) error {
	entries, err := os.ReadDir(jailPath)
	if err != nil {
		return fmt.Errorf("failed to read jail path %q: %w", jailPath, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("jail path %q is not empty; refusing to overwrite existing root", jailPath)
	}

	if templateRelease == "" {
		return fmt.Errorf("template/release is required")
	}

	if info, err := os.Stat(templateRelease); err == nil {
		if info.IsDir() {
			_, cpErr := runLoggedCommand(logs, "cp", "-a", templateRelease+"/.", jailPath+"/")
			if cpErr != nil {
				return fmt.Errorf("failed to copy template directory %q: %w", templateRelease, cpErr)
			}
			return nil
		}
		_, tarErr := runLoggedCommand(logs, "tar", "-xf", templateRelease, "-C", jailPath)
		if tarErr != nil {
			return fmt.Errorf("failed to extract template archive %q: %w", templateRelease, tarErr)
		}
		return nil
	}

	localBaseArchive := "/usr/freebsd-dist/base.txz"
	if releaseValuePattern.MatchString(strings.ToUpper(templateRelease)) {
		if _, err := os.Stat(localBaseArchive); err == nil {
			_, tarErr := runLoggedCommand(logs, "tar", "-xf", localBaseArchive, "-C", jailPath)
			if tarErr != nil {
				return fmt.Errorf("failed to extract %s: %w", localBaseArchive, tarErr)
			}
			return nil
		}
	}

	return fmt.Errorf(
		"template/release %q not found; provide a template directory, archive path, or install /usr/freebsd-dist/base.txz for release-based bootstrap",
		templateRelease,
	)
}

func configureMountPoints(name, jailPath string, specs []mountPointSpec, logs *[]string) (string, error) {
	if len(specs) == 0 {
		return "", nil
	}

	fstabLines := make([]string, 0, len(specs))
	for _, spec := range specs {
		if spec.Target == "" {
			continue
		}
		targetPath := filepath.Join(jailPath, strings.TrimPrefix(spec.Target, "/"))
		*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", targetPath))
		if err := os.MkdirAll(targetPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create mount target %q: %w", targetPath, err)
		}
		if spec.Source == "" {
			continue
		}
		if _, err := os.Stat(spec.Source); err != nil {
			return "", fmt.Errorf("mount source %q is not accessible: %w", spec.Source, err)
		}
		fstabLines = append(fstabLines, fmt.Sprintf("%s %s nullfs rw 0 0", spec.Source, targetPath))
	}

	if len(fstabLines) == 0 {
		return "", nil
	}

	fstabPath := jailFstabPathForName(name)
	if _, err := os.Stat(fstabPath); err == nil {
		return "", fmt.Errorf("fstab file %q already exists", fstabPath)
	}
	content := strings.Join(fstabLines, "\n") + "\n"
	*logs = append(*logs, fmt.Sprintf("$ write %s", fstabPath))
	if err := os.WriteFile(fstabPath, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("failed to write %q: %w", fstabPath, err)
	}
	return fstabPath, nil
}

func writeJailConfigFile(configPath string, lines []string, logs *[]string) error {
	if _, err := os.Stat(configPath); err == nil {
		return fmt.Errorf("config file %q already exists", configPath)
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return fmt.Errorf("failed to create config directory for %q: %w", configPath, err)
	}
	content := strings.Join(lines, "\n") + "\n"
	*logs = append(*logs, fmt.Sprintf("$ write %s", configPath))
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write %q: %w", configPath, err)
	}
	return nil
}

func applyRctlLimits(values jailWizardValues, jailName string, logs *[]string) error {
	if strings.TrimSpace(values.CPUPercent) != "" {
		if _, err := runLoggedCommand(logs, "rctl", "-a", fmt.Sprintf("jail:%s:pcpu:deny=%s", jailName, strings.TrimSpace(values.CPUPercent))); err != nil {
			return fmt.Errorf("failed to apply CPU rctl limit: %w", err)
		}
	}
	if strings.TrimSpace(values.MemoryLimit) != "" {
		if _, err := runLoggedCommand(logs, "rctl", "-a", fmt.Sprintf("jail:%s:memoryuse:deny=%s", jailName, strings.ToUpper(strings.TrimSpace(values.MemoryLimit)))); err != nil {
			return fmt.Errorf("failed to apply memory rctl limit: %w", err)
		}
	}
	if strings.TrimSpace(values.ProcessLimit) != "" {
		if _, err := runLoggedCommand(logs, "rctl", "-a", fmt.Sprintf("jail:%s:maxproc:deny=%s", jailName, strings.TrimSpace(values.ProcessLimit))); err != nil {
			return fmt.Errorf("failed to apply process rctl limit: %w", err)
		}
	}
	return nil
}

func runLoggedCommand(logs *[]string, name string, args ...string) (string, error) {
	command := name
	if len(args) > 0 {
		command += " " + strings.Join(args, " ")
	}
	*logs = append(*logs, "$ "+command)

	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(output))
	if text != "" {
		for _, line := range strings.Split(text, "\n") {
			*logs = append(*logs, "  "+line)
		}
	}
	if err != nil {
		return text, fmt.Errorf("%s: %w", command, err)
	}
	return text, nil
}
