package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	defaultJailConfPath = "/etc/jail.conf"
	jailConfDInclude    = `.include "/etc/jail.conf.d/*.conf";`
)

var (
	hostFreeBSDVersionPattern   = regexp.MustCompile(`(?i)(\d+)\.(\d+)-[A-Za-z0-9]+`)
	sourceFreeBSDReleasePattern = regexp.MustCompile(`(?i)(\d+\.\d+-RELEASE)`)
)

type jailConfIncludeStatus struct {
	ConfigPath     string
	FileExists     bool
	IncludePresent bool
	ReadError      string
}

type freeBSDVersion struct {
	Major int
	Minor int
	Raw   string
}

type jailBaseCompatibility struct {
	HostVersion   string
	SourceVersion string
	Warning       string
	Err           error
}

func validateJailCreateHostPreflight(values jailWizardValues) (string, error) {
	if err := jailConfDIncludePreflightError(); err != nil {
		return "", err
	}
	compatibility := collectJailBaseCompatibility(values)
	if compatibility.Err != nil {
		return "template_release", compatibility.Err
	}
	if err := validateRacctPreflight(values); err != nil {
		return "cpu_percent", err
	}
	return "", nil
}

func collectJailConfDIncludeStatus() jailConfIncludeStatus {
	status := jailConfIncludeStatus{
		ConfigPath: defaultJailConfPath,
	}
	info, err := os.Stat(status.ConfigPath)
	if err != nil {
		if os.IsNotExist(err) {
			return status
		}
		status.ReadError = fmt.Sprintf("failed to inspect %s: %v", status.ConfigPath, err)
		return status
	}
	if info.IsDir() {
		status.ReadError = fmt.Sprintf("%s is a directory", status.ConfigPath)
		return status
	}
	status.FileExists = true
	content, err := os.ReadFile(status.ConfigPath)
	if err != nil {
		status.ReadError = fmt.Sprintf("failed to read %s: %v", status.ConfigPath, err)
		return status
	}
	status.IncludePresent = hasJailConfDIncludeLine(string(content))
	return status
}

func hasJailConfDIncludeLine(content string) bool {
	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimSpace(stripInlineComment(rawLine))
		if line == jailConfDInclude {
			return true
		}
	}
	return false
}

func (status jailConfIncludeStatus) needsFix() bool {
	return strings.TrimSpace(status.ReadError) == "" && !status.IncludePresent
}

func jailConfDIncludePreflightError() error {
	status := collectJailConfDIncludeStatus()
	if strings.TrimSpace(status.ReadError) != "" {
		return fmt.Errorf("jail.conf include preflight failed: %s", status.ReadError)
	}
	if status.IncludePresent {
		return nil
	}
	return fmt.Errorf("%s is missing %s; rerun initial config or add it before creating jails", status.ConfigPath, jailConfDInclude)
}

func ensureJailConfDInclude(logs *[]string) error {
	status := collectJailConfDIncludeStatus()
	if strings.TrimSpace(status.ReadError) != "" {
		return fmt.Errorf("failed to prepare %s: %s", status.ConfigPath, status.ReadError)
	}
	if status.IncludePresent {
		if logs != nil {
			*logs = append(*logs, "No /etc/jail.conf include changes required.")
		}
		return nil
	}

	if _, err := backupFileForMutation(status.ConfigPath, "initial-check-jail-conf", logs); err != nil {
		return err
	}

	content := ""
	if status.FileExists {
		existing, err := os.ReadFile(status.ConfigPath)
		if err != nil {
			return fmt.Errorf("failed to read %s before update: %w", status.ConfigPath, err)
		}
		content = strings.TrimRight(string(existing), "\n")
		if content != "" {
			content += "\n"
		}
	}
	content += jailConfDInclude + "\n"

	if err := os.MkdirAll(filepath.Dir(status.ConfigPath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", status.ConfigPath, err)
	}
	if logs != nil {
		if status.FileExists {
			*logs = append(*logs, "$ update "+status.ConfigPath)
		} else {
			*logs = append(*logs, "$ write "+status.ConfigPath)
		}
	}
	if err := writeFileAtomicReplace(status.ConfigPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write %s: %w", status.ConfigPath, err)
	}
	return nil
}

func collectJailBaseCompatibility(values jailWizardValues) jailBaseCompatibility {
	hostVersion, err := detectHostFreeBSDVersion()
	if err != nil {
		return jailBaseCompatibility{
			Err: fmt.Errorf("could not determine host FreeBSD version: %w", err),
		}
	}
	status := jailBaseCompatibility{
		HostVersion: hostVersion.Raw,
	}

	sourceVersion, sourceText, ok := inferRequestedJailBaseVersion(strings.TrimSpace(values.TemplateRelease))
	if !ok {
		status.Warning = fmt.Sprintf("could not determine the FreeBSD jail base version from %q; verify it is not newer than host %s", strings.TrimSpace(values.TemplateRelease), hostVersion.Raw)
		return status
	}
	status.SourceVersion = sourceText
	if compareFreeBSDVersions(sourceVersion, hostVersion) > 0 {
		status.Err = fmt.Errorf("requested FreeBSD jail base %s is newer than host %s", sourceText, hostVersion.Raw)
	}
	return status
}

func detectHostFreeBSDVersion() (freeBSDVersion, error) {
	for _, candidate := range [][]string{
		{"freebsd-version"},
		{"uname", "-r"},
	} {
		out, err := exec.Command(candidate[0], candidate[1:]...).CombinedOutput()
		if err != nil {
			continue
		}
		text := strings.TrimSpace(strings.Split(string(out), "\n")[0])
		if text == "" {
			continue
		}
		version, parseErr := parseHostFreeBSDVersion(text)
		if parseErr == nil {
			return version, nil
		}
	}
	return freeBSDVersion{}, fmt.Errorf("freebsd-version and uname -r did not return a parseable value")
}

func parseHostFreeBSDVersion(raw string) (freeBSDVersion, error) {
	match := hostFreeBSDVersionPattern.FindStringSubmatch(strings.TrimSpace(raw))
	if len(match) != 3 {
		return freeBSDVersion{}, fmt.Errorf("unrecognized host version %q", raw)
	}
	major, err := strconv.Atoi(match[1])
	if err != nil {
		return freeBSDVersion{}, fmt.Errorf("invalid host major version %q", raw)
	}
	minor, err := strconv.Atoi(match[2])
	if err != nil {
		return freeBSDVersion{}, fmt.Errorf("invalid host minor version %q", raw)
	}
	return freeBSDVersion{
		Major: major,
		Minor: minor,
		Raw:   strings.TrimSpace(raw),
	}, nil
}

func inferRequestedJailBaseVersion(input string) (freeBSDVersion, string, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return freeBSDVersion{}, "", false
	}

	candidates := []string{input}
	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		candidates = append(candidates, source)
	}
	for _, candidate := range candidates {
		if version, matched, ok := extractFreeBSDReleaseVersion(candidate); ok {
			return version, matched, true
		}
	}
	return freeBSDVersion{}, "", false
}

func extractFreeBSDReleaseVersion(raw string) (freeBSDVersion, string, bool) {
	match := sourceFreeBSDReleasePattern.FindString(strings.TrimSpace(raw))
	if match == "" {
		return freeBSDVersion{}, "", false
	}
	parts := strings.SplitN(strings.TrimSuffix(strings.ToUpper(match), "-RELEASE"), ".", 2)
	if len(parts) != 2 {
		return freeBSDVersion{}, "", false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return freeBSDVersion{}, "", false
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return freeBSDVersion{}, "", false
	}
	return freeBSDVersion{
		Major: major,
		Minor: minor,
		Raw:   strings.ToUpper(match),
	}, strings.ToUpper(match), true
}

func compareFreeBSDVersions(a, b freeBSDVersion) int {
	switch {
	case a.Major < b.Major:
		return -1
	case a.Major > b.Major:
		return 1
	case a.Minor < b.Minor:
		return -1
	case a.Minor > b.Minor:
		return 1
	default:
		return 0
	}
}
