package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var releaseValuePattern = regexp.MustCompile(`^[0-9]+\.[0-9]+-RELEASE`)

const (
	defaultUserlandDir  = "/usr/local/jails/media"
	defaultDownloadHost = "https://download.freebsd.org"
)

type JailCreationResult struct {
	Name       string
	ConfigPath string
	FstabPath  string
	JailPath   string
	Logs       []string
	Err        error
}

func ExecuteJailCreation(values jailWizardValues) JailCreationResult {
	if strings.TrimSpace(values.JailType) == "" {
		values.JailType = "thick"
	}
	if strings.TrimSpace(values.Interface) == "" {
		values.Interface = "em0"
	}

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

	validator := newJailCreationWizard("")
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
	if err := ensureJailConfigDoesNotExist(result.ConfigPath); err != nil {
		return fail(err)
	}
	logf("Starting jail creation for %s", result.Name)

	jailPath, err := prepareJailPath(values, destination, &logs)
	if err != nil {
		return fail(err)
	}
	result.JailPath = jailPath

	if err := provisionJailRoot(values, jailPath, &logs); err != nil {
		return fail(err)
	}
	if normalizedJailType(values.JailType) == "linux" {
		if err := ensureLinuxHostReady(&logs); err != nil {
			return fail(err)
		}
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

func prepareJailPath(values jailWizardValues, destination string, logs *[]string) (string, error) {
	if normalizedJailType(values.JailType) == "thin" {
		return ensureThinDestinationPath(destination, logs)
	}
	return ensureDestinationJailPath(destination, logs)
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

func provisionJailRoot(values jailWizardValues, jailPath string, logs *[]string) error {
	switch normalizedJailType(values.JailType) {
	case "thin":
		return provisionThinJailRoot(values, jailPath, logs)
	case "linux":
		if err := provisionStandardJailRoot(jailPath, strings.TrimSpace(values.TemplateRelease), logs); err != nil {
			return err
		}
		return ensureLinuxCompatPaths(jailPath, logs)
	default:
		if err := provisionStandardJailRoot(jailPath, strings.TrimSpace(values.TemplateRelease), logs); err != nil {
			return err
		}
		return seedGuestBaseFiles(jailPath, logs)
	}
}

func provisionStandardJailRoot(jailPath, templateRelease string, logs *[]string) error {
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

	sourcePath, cleanup, err := resolveTemplateSource(strings.TrimSpace(templateRelease), logs)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	if info, err := os.Stat(sourcePath); err == nil {
		if info.IsDir() {
			_, cpErr := runLoggedCommand(logs, "cp", "-a", sourcePath+"/.", jailPath+"/")
			if cpErr != nil {
				return fmt.Errorf("failed to copy template directory %q: %w", sourcePath, cpErr)
			}
			return nil
		}
		_, tarErr := runLoggedCommand(logs, "tar", "-xf", sourcePath, "-C", jailPath)
		if tarErr != nil {
			return fmt.Errorf("failed to extract template archive %q: %w", sourcePath, tarErr)
		}
		return nil
	}
	return fmt.Errorf("resolved template/release %q is not accessible", sourcePath)
}

func ensureThinDestinationPath(destination string, logs *[]string) (string, error) {
	jailPath := filepath.Clean(strings.TrimSpace(destination))
	if jailPath == "" || !strings.HasPrefix(jailPath, "/") {
		return "", fmt.Errorf("thin jails require an absolute destination path")
	}
	parent := filepath.Dir(jailPath)
	*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", parent))
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("failed to create thin jail parent path %q: %w", parent, err)
	}
	if _, err := os.Stat(jailPath); err == nil {
		return "", fmt.Errorf("thin jail destination %q already exists", jailPath)
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to inspect thin jail destination %q: %w", jailPath, err)
	}
	return jailPath, nil
}

func provisionThinJailRoot(values jailWizardValues, jailPath string, logs *[]string) error {
	sourcePath, cleanup, err := resolveTemplateSource(strings.TrimSpace(values.TemplateRelease), logs)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("thin jail template %q is not accessible: %w", sourcePath, err)
	}
	if !sourceInfo.IsDir() {
		return fmt.Errorf("thin jails require a template directory backed by ZFS; archives and release tags should be extracted into a template dataset first")
	}

	sourceDataset, err := exactZFSDatasetForPath(sourcePath)
	if err != nil {
		return fmt.Errorf("thin jail template path must be an exact ZFS dataset mountpoint: %w", err)
	}
	parentDataset, err := parentZFSDatasetForPath(jailPath)
	if err != nil {
		return fmt.Errorf("thin jail destination must be inside a ZFS dataset: %w", err)
	}

	snapshot := sourceDataset.Name + "@freebsd-jails-tui-base"
	if _, err := runLoggedCommand(logs, "zfs", "list", "-H", "-t", "snapshot", "-o", "name", snapshot); err != nil {
		if _, snapshotErr := runLoggedCommand(logs, "zfs", "snapshot", snapshot); snapshotErr != nil {
			return fmt.Errorf("failed to create thin jail template snapshot %q: %w", snapshot, snapshotErr)
		}
	}

	cloneDataset := parentDataset.Name + "/" + filepath.Base(jailPath)
	if _, err := runLoggedCommand(logs, "zfs", "list", "-H", "-o", "name", cloneDataset); err == nil {
		return fmt.Errorf("thin jail dataset %q already exists", cloneDataset)
	}
	if _, err := runLoggedCommand(logs, "zfs", "clone", snapshot, cloneDataset); err != nil {
		return fmt.Errorf("failed to clone thin jail dataset %q from %q: %w", cloneDataset, snapshot, err)
	}
	expectedMount := filepath.Join(filepath.Clean(parentDataset.Mountpoint), filepath.Base(jailPath))
	if filepath.Clean(expectedMount) != filepath.Clean(jailPath) {
		if _, err := runLoggedCommand(logs, "zfs", "set", "mountpoint="+jailPath, cloneDataset); err != nil {
			return fmt.Errorf("failed to set mountpoint for thin jail dataset %q: %w", cloneDataset, err)
		}
	}
	return seedGuestBaseFiles(jailPath, logs)
}

func exactZFSDatasetForPath(path string) (*ZFSDatasetInfo, error) {
	info, err := discoverZFSDataset(path)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("no ZFS dataset matched %q", path)
	}
	if filepath.Clean(info.Mountpoint) != filepath.Clean(path) {
		return nil, fmt.Errorf("%q resolves to parent dataset %q mounted at %q", path, info.Name, info.Mountpoint)
	}
	return info, nil
}

func parentZFSDatasetForPath(path string) (*ZFSDatasetInfo, error) {
	parent := filepath.Clean(filepath.Dir(path))
	info, err := discoverZFSDataset(parent)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("no parent ZFS dataset matched %q", parent)
	}
	return info, nil
}

func seedGuestBaseFiles(jailPath string, logs *[]string) error {
	copyList := []struct {
		src string
		dst string
	}{
		{src: "/etc/resolv.conf", dst: filepath.Join(jailPath, "etc", "resolv.conf")},
		{src: "/etc/localtime", dst: filepath.Join(jailPath, "etc", "localtime")},
	}
	for _, item := range copyList {
		if _, err := os.Stat(item.src); err != nil {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(item.dst), 0o755); err != nil {
			return fmt.Errorf("failed to prepare guest file path %q: %w", item.dst, err)
		}
		*logs = append(*logs, fmt.Sprintf("$ cp %s %s", item.src, item.dst))
		if _, err := runLoggedCommand(logs, "cp", "-f", item.src, item.dst); err != nil {
			return fmt.Errorf("failed to copy %q into jail: %w", item.src, err)
		}
	}
	return nil
}

func ensureLinuxHostReady(logs *[]string) error {
	if _, err := runLoggedCommand(logs, "sysrc", "linux_enable=YES"); err != nil {
		return fmt.Errorf("failed to enable linux ABI on host: %w", err)
	}
	if _, err := runLoggedCommand(logs, "service", "linux", "start"); err != nil {
		return fmt.Errorf("failed to start linux ABI service on host: %w", err)
	}
	return nil
}

func ensureLinuxCompatPaths(jailPath string, logs *[]string) error {
	paths := []string{
		filepath.Join(jailPath, "compat", "ubuntu", "dev", "shm"),
		filepath.Join(jailPath, "compat", "ubuntu", "dev", "fd"),
		filepath.Join(jailPath, "compat", "ubuntu", "proc"),
		filepath.Join(jailPath, "compat", "ubuntu", "sys"),
		filepath.Join(jailPath, "compat", "ubuntu", "tmp"),
		filepath.Join(jailPath, "compat", "ubuntu", "home"),
	}
	for _, path := range paths {
		*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", path))
		if err := os.MkdirAll(path, 0o755); err != nil {
			return fmt.Errorf("failed to prepare Linux compatibility path %q: %w", path, err)
		}
	}
	return seedGuestBaseFiles(jailPath, logs)
}

func ensureJailConfigDoesNotExist(configPath string) error {
	if _, err := os.Stat(configPath); err == nil {
		return fmt.Errorf("config file %q already exists", configPath)
	}
	return nil
}

func resolveTemplateSource(input string, logs *[]string) (string, func(), error) {
	if input == "" {
		return "", nil, fmt.Errorf("template/release is required")
	}

	// Explicit filesystem path wins.
	if _, err := os.Stat(input); err == nil {
		return input, nil, nil
	}

	// Shortcut: entry name from userland media directory.
	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		return source, nil, nil
	}

	// Full URL: download and extract.
	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		return downloadArchiveToTemp(input, logs)
	}

	// Release tag: local archive, then media dir, then download.freebsd.org.
	if releaseValuePattern.MatchString(strings.ToUpper(input)) {
		localBaseArchive := "/usr/freebsd-dist/base.txz"
		if _, err := os.Stat(localBaseArchive); err == nil {
			return localBaseArchive, nil, nil
		}
		if source, ok := findReleaseArchiveInUserland(defaultUserlandDir, input); ok {
			return source, nil, nil
		}

		return downloadReleaseArchiveToTemp(input, logs)
	}

	return "", nil, fmt.Errorf(
		"template/release %q not found; use a local path, an entry from %s, a release tag, or a custom URL",
		input,
		defaultUserlandDir,
	)
}

func defaultReleaseBaseURLs(release string) ([]string, error) {
	archOut, err := exec.Command("uname", "-m").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to detect system arch for release download: %w", err)
	}
	arch := strings.TrimSpace(string(archOut))
	if arch == "" {
		arch = "amd64"
	}
	release = strings.ToUpper(strings.TrimSpace(release))
	urls := []string{
		// FreeBSD release directory layout commonly uses arch/arch/<release>/base.txz.
		fmt.Sprintf("%s/ftp/releases/%s/%s/%s/base.txz", defaultDownloadHost, arch, arch, release),
		// Compatibility fallback.
		fmt.Sprintf("%s/ftp/releases/%s/%s/base.txz", defaultDownloadHost, arch, release),
	}
	return urls, nil
}

func downloadReleaseArchiveToTemp(release string, logs *[]string) (string, func(), error) {
	urls, err := defaultReleaseBaseURLs(release)
	if err != nil {
		return "", nil, err
	}
	var lastErr error
	for _, url := range urls {
		path, cleanup, err := downloadArchiveToTemp(url, logs)
		if err == nil {
			return path, cleanup, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("release download failed")
	}
	return "", nil, fmt.Errorf("unable to download release %s: %w", release, lastErr)
}

func downloadArchiveToTemp(url string, logs *[]string) (string, func(), error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return "", nil, fmt.Errorf("download URL is empty")
	}
	*logs = append(*logs, "$ fetch "+url)
	resp, err := http.Get(url) // #nosec G107 user-provided URL is intentional
	if err != nil {
		return "", nil, fmt.Errorf("failed downloading %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", nil, fmt.Errorf("download failed from %s: http %d", url, resp.StatusCode)
	}
	tmp, err := os.CreateTemp("", "freebsd-jail-userland-*.txz")
	if err != nil {
		return "", nil, fmt.Errorf("failed creating temp archive: %w", err)
	}
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", nil, fmt.Errorf("failed writing temp archive: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", nil, fmt.Errorf("failed closing temp archive: %w", err)
	}
	*logs = append(*logs, "  downloaded to "+tmp.Name())
	cleanup := func() {
		_ = os.Remove(tmp.Name())
	}
	return tmp.Name(), cleanup, nil
}

func discoverUserlandSources(userlandDir string) ([]string, error) {
	entries, err := os.ReadDir(userlandDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed reading userland directory %q: %w", userlandDir, err)
	}
	var sources []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		fullPath := filepath.Join(userlandDir, entry.Name())
		if entry.IsDir() {
			baseArchive := filepath.Join(fullPath, "base.txz")
			if _, err := os.Stat(baseArchive); err == nil {
				sources = append(sources, baseArchive)
				continue
			}
			sources = append(sources, fullPath)
			continue
		}
		sources = append(sources, fullPath)
	}
	sort.Strings(sources)
	return sources, nil
}

func findNamedUserlandSource(userlandDir, input string) (string, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", false
	}
	sources, err := discoverUserlandSources(userlandDir)
	if err != nil {
		return "", false
	}
	lowerInput := strings.ToLower(input)
	for _, source := range sources {
		base := strings.ToLower(filepath.Base(source))
		noExt := strings.TrimSuffix(base, filepath.Ext(base))
		if base == lowerInput || noExt == lowerInput {
			return source, true
		}
		parent := strings.ToLower(filepath.Base(filepath.Dir(source)))
		if parent == lowerInput {
			return source, true
		}
	}
	return "", false
}

func findReleaseArchiveInUserland(userlandDir, release string) (string, bool) {
	sources, err := discoverUserlandSources(userlandDir)
	if err != nil {
		return "", false
	}
	release = strings.ToLower(strings.TrimSpace(release))
	for _, source := range sources {
		text := strings.ToLower(source)
		if strings.Contains(text, release) {
			return source, true
		}
	}
	return "", false
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
