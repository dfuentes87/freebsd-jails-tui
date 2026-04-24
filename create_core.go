package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var releaseValuePattern = regexp.MustCompile(`^[0-9]+\.[0-9]+-RELEASE`)

var errTemplateDatasetParentMissing = errors.New("template parent dataset missing")

var debugLogEnabled bool

func init() {
	if os.Getenv("FREEBSD_JAILS_TUI_DEBUG") != "" {
		debugLogEnabled = true
	}
}

func debugLog(component, message string) {
	if !debugLogEnabled {
		return
	}
	f, err := os.OpenFile("/var/log/freebsd-jails-tui-debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		defer f.Close()
		ts := time.Now().Format(time.RFC3339)
		f.WriteString(fmt.Sprintf("[%s] %s: %s\n", ts, component, message))
	}
}

const (
	defaultUserlandDir     = "/usr/local/jails/media"
	defaultDownloadHost    = "https://download.freebsd.org"
	archiveDownloadTimeout = 30 * time.Minute
)

type createProgressContextKey struct{}

type JailCreationResult struct {
	Name        string
	ConfigPath  string
	FstabPath   string
	JailPath    string
	Logs        []string
	Warnings    []string
	NextActions []string
	Err         error
}

func withCreateProgress(ctx context.Context, ch chan<- downloadProgressMsg) context.Context {
	if ch == nil {
		return ctx
	}
	return context.WithValue(ctx, createProgressContextKey{}, ch)
}

func createProgressChan(ctx context.Context) chan<- downloadProgressMsg {
	if ctx == nil {
		return nil
	}
	ch, _ := ctx.Value(createProgressContextKey{}).(chan<- downloadProgressMsg)
	return ch
}

func reportCreatePhase(ctx context.Context, step, total int, phase, detail string) {
	ch := createProgressChan(ctx)
	if ch == nil {
		return
	}
	select {
	case ch <- downloadProgressMsg{Step: step, StepTotal: total, Phase: strings.TrimSpace(phase), Detail: strings.TrimSpace(detail)}:
	default:
	}
}

func reportCreateLog(ctx context.Context, line string) {
	ch := createProgressChan(ctx)
	if ch == nil || strings.TrimSpace(line) == "" {
		return
	}
	select {
	case ch <- downloadProgressMsg{LogLine: line}:
	default:
	}
}

func ExecuteJailCreation(ctx context.Context, values jailWizardValues, progressChan chan<- downloadProgressMsg) JailCreationResult {
	ctx = withCreateProgress(ctx, progressChan)
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
		msg := fmt.Sprintf(format, args...)
		logs = append(logs, msg)
		debugLog("JailCreation", msg)
		reportCreateLog(ctx, msg)
	}
	cleanups := make([]func(), 0, 8)
	addCleanup := func(fn func()) {
		if fn != nil {
			cleanups = append(cleanups, fn)
		}
	}
	warnings := make([]string, 0, 4)
	fail := func(err error) JailCreationResult {
		debugLog("JailCreation", fmt.Sprintf("Failed: %v. Rolling back...", err))
		if len(warnings) > 0 {
			for idx, warning := range warnings {
				debugLog("JailCreation", fmt.Sprintf("Warning[%d]: %s", idx, warning))
			}
		}
		for idx := len(cleanups) - 1; idx >= 0; idx-- {
			cleanups[idx]()
		}
		result.Logs = logs
		result.Warnings = warnings
		result.NextActions = buildPostCreateChecklist(values, warnings, err == nil)
		result.Err = err
		return result
	}

	validator := newJailCreationWizard("")
	validator.values = values
	if err := validator.validateAll(); err != nil {
		return fail(err)
	}
	validatedDependencies, err := validateExistingJailDependencies(values.Dependencies, result.Name)
	if err != nil {
		return fail(err)
	}
	values.Dependencies = strings.Join(validatedDependencies, " ")
	if result.Name == "" {
		return fail(fmt.Errorf("jail name is required"))
	}
	if _, err := validateJailCreateHostPreflight(values); err != nil {
		return fail(err)
	}
	if compatibility := collectJailBaseCompatibility(values); strings.TrimSpace(compatibility.Warning) != "" {
		logf("warning: %s", compatibility.Warning)
	}
	if normalizedJailType(values.JailType) == "linux" {
		releaseSupport := collectLinuxBootstrapReleaseSupport(values)
		if effectiveLinuxBootstrapMethod(values) == "debootstrap" && releaseSupport.Status == "unknown" && strings.TrimSpace(releaseSupport.Detail) != "" && effectiveLinuxBootstrapMode(values) == "auto" {
			logf("warning: %s", releaseSupport.Detail)
		}
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
	if strings.HasPrefix(destination, "/") {
		cleanDestination, err := validateJailDestinationPath(destination, result.Name)
		if err != nil {
			return fail(err)
		}
		destination = cleanDestination
		values.Dataset = cleanDestination
	} else {
		cleanDataset, err := validateZFSDatasetName(destination, "destination dataset")
		if err != nil {
			return fail(err)
		}
		destination = cleanDataset
		values.Dataset = cleanDataset
	}

	result.ConfigPath = jailConfigPathForName(result.Name)
	if err := ensureJailConfigDoesNotExist(result.ConfigPath); err != nil {
		return fail(err)
	}
	logf("Starting jail creation for %s", result.Name)
	patchDecision := resolveFreeBSDPatchDecision(values.TemplateRelease, values.PatchBase)
	if patchDecision.Err != nil {
		return fail(patchDecision.Err)
	}

	phases := []struct {
		title  string
		detail string
	}{
		{"Prepare host requirements", "Validating host state and preparing networking."},
		{"Prepare jail path", "Creating or validating the destination path."},
		{"Provision jail root", "Extracting, cloning, or copying the FreeBSD base."},
	}
	if patchDecision.Effective {
		phases = append(phases, struct {
			title  string
			detail string
		}{"Patch FreeBSD base", "Running freebsd-update inside the new jail root."})
	}
	if normalizedJailType(values.JailType) == "linux" {
		phases = append(phases, struct {
			title  string
			detail string
		}{"Prepare Linux host support", "Ensuring Linux ABI and required host support are available."})
	}
	if normalizedJailType(values.JailType) == "linux" && effectiveLinuxBootstrapMethod(values) == "archive" {
		phases = append(phases, struct {
			title  string
			detail string
		}{"Bootstrap Linux userland", "Running host-side archive bootstrap before the jail starts."})
	}
	phases = append(phases,
		struct {
			title  string
			detail string
		}{"Write jail configuration", "Writing jail.conf, mount points, startup order, and limits."},
		struct {
			title  string
			detail string
		}{"Start jail", "Starting the jail and waiting for it to come up."},
	)
	if normalizedJailType(values.JailType) == "linux" && effectiveLinuxBootstrapMethod(values) != "archive" {
		phases = append(phases, struct {
			title  string
			detail string
		}{"Bootstrap Linux userland", "Running preflight and the selected Linux bootstrap method inside the jail."})
	}
	phases = append(phases, struct {
		title  string
		detail string
	}{"Finalize limits", "Applying live rctl limits when configured."})
	phaseIdx := 0
	nextPhase := func() {
		if phaseIdx >= len(phases) {
			return
		}
		phaseIdx++
		reportCreatePhase(ctx, phaseIdx, len(phases), phases[phaseIdx-1].title, phases[phaseIdx-1].detail)
	}

	nextPhase()
	hostNetworkCleanup, err := ensureHostNetworkReady(ctx, values, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(hostNetworkCleanup)

	nextPhase()
	jailPath, pathCleanup, err := prepareJailPath(ctx, values, destination, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(pathCleanup)
	result.JailPath = jailPath

	nextPhase()
	rootCleanup, err := provisionJailRoot(ctx, values, jailPath, &logs, progressChan)
	if err != nil {
		return fail(err)
	}
	addCleanup(rootCleanup)
	if patchDecision.Effective {
		nextPhase()
		if err := patchFreeBSDRoot(ctx, jailPath, &logs); err != nil {
			return fail(err)
		}
	}
	if normalizedJailType(values.JailType) == "linux" {
		nextPhase()
		linuxCleanup, err := ensureLinuxHostReady(ctx, &logs)
		if err != nil {
			return fail(err)
		}
		addCleanup(linuxCleanup)
	}
	if normalizedJailType(values.JailType) == "linux" && effectiveLinuxBootstrapMethod(values) == "archive" {
		nextPhase()
		bootstrapWarnings, err := maybeBootstrapLinuxUserland(ctx, values, result.Name, jailPath, &logs)
		if err != nil {
			return fail(err)
		}
		warnings = append(warnings, bootstrapWarnings...)
	}

	mountSpecs := parseMountPointSpecs(values.MountPoints)
	fstabPath, err := configureMountPoints(ctx, result.Name, jailPath, mountSpecs, &logs)
	if err != nil {
		return fail(err)
	}
	result.FstabPath = fstabPath
	addCleanup(func() {
		if err := removeFileIfExists(fstabPath, &logs); err != nil {
			logs = append(logs, "rollback warning: "+err.Error())
		}
	})

	nextPhase()
	configLines := buildJailConfBlock(values, jailPath, fstabPath)
	if err := writeJailConfigFile(result.ConfigPath, configLines, &logs); err != nil {
		return fail(err)
	}
	addCleanup(func() {
		if err := removeFileIfExists(result.ConfigPath, &logs); err != nil {
			logs = append(logs, "rollback warning: "+err.Error())
		}
	})
	startupCleanup, err := updateJailStartupConfig(ctx, result.Name, values, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(startupCleanup)
	persistentRctlCleanup, err := syncPersistentJailRctlRules(ctx, values, result.Name, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(persistentRctlCleanup)

	nextPhase()
	if _, err := runLoggedCommand(ctx, &logs, "service", "jail", "start", result.Name); err != nil {
		return fail(err)
	}
	addCleanup(func() {
		if _, err := runLoggedCommand(ctx, &logs, "service", "jail", "stop", result.Name); err != nil {
			logs = append(logs, fmt.Sprintf("rollback warning: failed to stop jail %q: %v", result.Name, err))
		}
	})
	if normalizedJailType(values.JailType) == "linux" && effectiveLinuxBootstrapMethod(values) != "archive" {
		nextPhase()
		bootstrapWarnings, err := maybeBootstrapLinuxUserland(ctx, values, result.Name, jailPath, &logs)
		if err != nil {
			return fail(err)
		}
		warnings = append(warnings, bootstrapWarnings...)
	}
	nextPhase()
	if err := applyRctlLimits(ctx, values, result.Name, &logs); err != nil {
		return fail(err)
	}
	addCleanup(func() {
		removeJailRctlRules(ctx, result.Name, &logs)
	})

	if len(warnings) > 0 {
		for idx, warning := range warnings {
			debugLog("JailCreation", fmt.Sprintf("Warning[%d]: %s", idx, warning))
		}
		logf("Jail %s created and started with warnings.", result.Name)
	} else {
		logf("Jail %s created successfully.", result.Name)
	}
	result.Logs = logs
	result.Warnings = warnings
	result.NextActions = buildPostCreateChecklist(values, warnings, true)
	return result
}

func prepareJailPath(ctx context.Context, values jailWizardValues, destination string, logs *[]string) (string, func(), error) {
	if normalizedJailType(values.JailType) == "thin" {
		return ensureThinDestinationPath(ctx, destination, logs)
	}
	return ensureDestinationJailPath(ctx, destination, logs)
}

func ensureDestinationJailPath(ctx context.Context, destination string, logs *[]string) (string, func(), error) {
	destination = strings.TrimSpace(destination)
	if strings.HasPrefix(destination, "/") {
		jailPath, err := validateAbsolutePath(destination, "destination")
		if err != nil {
			return "", nil, err
		}
		info, err := os.Stat(jailPath)
		existed := err == nil && info.IsDir()
		existedEmpty := false
		if existed {
			entries, readErr := os.ReadDir(jailPath)
			if readErr != nil {
				return "", nil, fmt.Errorf("failed to inspect destination path %q: %w", jailPath, readErr)
			}
			existedEmpty = len(entries) == 0
		}
		*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", jailPath))
		if err := os.MkdirAll(jailPath, 0o755); err != nil {
			return "", nil, fmt.Errorf("failed to create destination path %q: %w", jailPath, err)
		}
		if !existed {
			return jailPath, func() {
				*logs = append(*logs, "$ rm -rf "+jailPath)
				if err := os.RemoveAll(jailPath); err != nil {
					*logs = append(*logs, "  rollback warning: failed to remove "+jailPath+": "+err.Error())
				}
			}, nil
		}
		if existedEmpty {
			return jailPath, func() {
				rollbackPreexistingEmptyDir(jailPath, logs)
			}, nil
		}
		return jailPath, nil, nil
	}

	var err error
	destination, err = validateZFSDatasetName(destination, "destination dataset")
	if err != nil {
		return "", nil, err
	}
	createdDataset := false
	if !zfsDatasetExists(destination) {
		if _, createErr := runLoggedCommand(ctx, logs, "zfs", "create", "-p", destination); createErr != nil {
			return "", nil, fmt.Errorf("failed to ensure dataset %q: %w", destination, createErr)
		}
		createdDataset = true
	}

	mountpointOut, err := runLoggedCommand(ctx, logs, "zfs", "list", "-H", "-o", "mountpoint", destination)
	if err != nil {
		return "", nil, fmt.Errorf("failed to discover mountpoint for %q: %w", destination, err)
	}
	mountpoint := strings.TrimSpace(strings.Split(mountpointOut, "\n")[0])
	if mountpoint == "" || mountpoint == "-" || mountpoint == "legacy" {
		mountpoint = "/" + strings.Trim(destination, "/")
	}
	info, statErr := os.Stat(mountpoint)
	mountExisted := statErr == nil && info.IsDir()
	mountExistedEmpty := false
	if mountExisted {
		entries, readErr := os.ReadDir(mountpoint)
		if readErr != nil {
			return "", nil, fmt.Errorf("failed to inspect jail path %q: %w", mountpoint, readErr)
		}
		mountExistedEmpty = len(entries) == 0
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return "", nil, fmt.Errorf("failed to inspect jail path %q: %w", mountpoint, statErr)
	}

	*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", mountpoint))
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return "", nil, fmt.Errorf("failed to create jail path %q: %w", mountpoint, err)
	}
	if createdDataset {
		return mountpoint, func() {
			if _, err := runLoggedCommand(ctx, logs, "zfs", "destroy", "-r", destination); err != nil {
				*logs = append(*logs, "  rollback warning: failed to destroy dataset "+destination+": "+err.Error())
			}
		}, nil
	}
	if mountExistedEmpty {
		return mountpoint, func() {
			rollbackPreexistingEmptyDir(mountpoint, logs)
		}, nil
	}
	return mountpoint, nil, nil
}

func provisionJailRoot(ctx context.Context, values jailWizardValues, jailPath string, logs *[]string, progressChan chan<- downloadProgressMsg) (func(), error) {
	switch normalizedJailType(values.JailType) {
	case "thin":
		return provisionThinJailRoot(ctx, values, jailPath, logs, progressChan)
	case "linux":
		if err := provisionStandardJailRoot(ctx, jailPath, strings.TrimSpace(values.TemplateRelease), logs, progressChan); err != nil {
			return nil, err
		}
		return nil, ensureLinuxCompatPaths(ctx, jailPath, values, logs)
	default:
		if err := provisionStandardJailRoot(ctx, jailPath, strings.TrimSpace(values.TemplateRelease), logs, progressChan); err != nil {
			return nil, err
		}
		return nil, seedGuestBaseFiles(ctx, jailPath, logs)
	}
}

func provisionStandardJailRoot(ctx context.Context, jailPath, templateRelease string, logs *[]string, progressChan chan<- downloadProgressMsg) error {
	entries, err := os.ReadDir(jailPath)
	if err != nil {
		return fmt.Errorf("failed to read jail path %q: %w", jailPath, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("jail path %q is not empty; please manually investigate or remove it", jailPath)
	}

	if templateRelease == "" {
		return fmt.Errorf("template/release is required")
	}

	sourcePath, cleanup, err := resolveTemplateSource(ctx, strings.TrimSpace(templateRelease), logs, progressChan)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	if info, err := os.Stat(sourcePath); err == nil {
		if info.IsDir() {
			_, cpErr := runLoggedCommand(ctx, logs, "cp", "-a", sourcePath+"/.", jailPath+"/")
			if cpErr != nil {
				return fmt.Errorf("failed to copy template directory %q: %w", sourcePath, cpErr)
			}
			return nil
		}
		_, tarErr := runLoggedCommand(ctx, logs, "tar", "-xf", sourcePath, "-C", jailPath)
		if tarErr != nil {
			return fmt.Errorf("failed to extract template archive %q: %w", sourcePath, tarErr)
		}
		return nil
	}
	return fmt.Errorf("resolved template/release %q is not accessible", sourcePath)
}

func ensureThinDestinationPath(ctx context.Context, destination string, logs *[]string) (string, func(), error) {
	jailPath := filepath.Clean(strings.TrimSpace(destination))
	if jailPath == "" || !strings.HasPrefix(jailPath, "/") {
		return "", nil, fmt.Errorf("thin jails require an absolute destination path")
	}
	parent := filepath.Dir(jailPath)
	*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", parent))
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", nil, fmt.Errorf("failed to create thin jail parent path %q: %w", parent, err)
	}
	if _, err := os.Stat(jailPath); err == nil {
		return "", nil, fmt.Errorf("thin jail destination %q already exists", jailPath)
	} else if !os.IsNotExist(err) {
		return "", nil, fmt.Errorf("failed to inspect thin jail destination %q: %w", jailPath, err)
	}
	return jailPath, nil, nil
}

func provisionThinJailRoot(ctx context.Context, values jailWizardValues, jailPath string, logs *[]string, progressChan chan<- downloadProgressMsg) (func(), error) {
	sourcePath, cleanup, err := resolveTemplateSource(ctx, strings.TrimSpace(values.TemplateRelease), logs, progressChan)
	if err != nil {
		return nil, err
	}
	if cleanup != nil {
		defer cleanup()
	}

	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("thin jail template %q is not accessible: %w", sourcePath, err)
	}
	if !sourceInfo.IsDir() {
		return nil, fmt.Errorf("thin jails require a template directory backed by ZFS; archives and release tags should be extracted into a template dataset first")
	}

	sourceDataset, err := exactZFSDatasetForPath(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("thin jail template path must be an exact ZFS dataset mountpoint: %w", err)
	}
	parentDataset, err := parentZFSDatasetForPath(jailPath)
	if err != nil {
		return nil, fmt.Errorf("thin jail destination must be inside a ZFS dataset: %w", err)
	}

	snapshot := sourceDataset.Name + "@freebsd-jails-tui-base"
	if !zfsSnapshotExists(snapshot) {
		if _, snapshotErr := runLoggedCommand(ctx, logs, "zfs", "snapshot", snapshot); snapshotErr != nil {
			return nil, fmt.Errorf("failed to create thin jail template snapshot %q: %w", snapshot, snapshotErr)
		}
	}

	cloneDataset := parentDataset.Name + "/" + filepath.Base(jailPath)
	if zfsDatasetExists(cloneDataset) {
		return nil, fmt.Errorf("thin jail dataset %q already exists", cloneDataset)
	}
	if _, err := runLoggedCommand(ctx, logs, "zfs", "clone", snapshot, cloneDataset); err != nil {
		return nil, fmt.Errorf("failed to clone thin jail dataset %q from %q: %w", cloneDataset, snapshot, err)
	}
	expectedMount := filepath.Join(filepath.Clean(parentDataset.Mountpoint), filepath.Base(jailPath))
	if filepath.Clean(expectedMount) != filepath.Clean(jailPath) {
		if _, err := runLoggedCommand(ctx, logs, "zfs", "set", "mountpoint="+jailPath, cloneDataset); err != nil {
			return nil, fmt.Errorf("failed to set mountpoint for thin jail dataset %q: %w", cloneDataset, err)
		}
	}
	cleanupClone := func() {
		if _, err := runLoggedCommand(ctx, logs, "zfs", "destroy", "-r", cloneDataset); err != nil {
			*logs = append(*logs, "  rollback warning: failed to destroy thin jail dataset "+cloneDataset+": "+err.Error())
		}
	}
	return cleanupClone, seedGuestBaseFiles(ctx, jailPath, logs)
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

func seedGuestBaseFiles(ctx context.Context, jailPath string, logs *[]string) error {
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
		if _, err := runLoggedCommand(ctx, logs, "cp", "-f", item.src, item.dst); err != nil {
			return fmt.Errorf("failed to copy %q into jail: %w", item.src, err)
		}
	}
	return nil
}

func configureMountPoints(ctx context.Context, name, jailPath string, specs []mountPointSpec, logs *[]string) (string, error) {
	if len(specs) == 0 {
		return "", nil
	}

	fstabLines := make([]string, 0, len(specs))
	seenTargets := map[string]struct{}{}
	for _, spec := range specs {
		if spec.Target == "" {
			continue
		}
		cleanTarget, targetPath, err := validateMountTargetPath(jailPath, spec.Target)
		if err != nil {
			return "", err
		}
		if _, exists := seenTargets[cleanTarget]; exists {
			return "", fmt.Errorf("mount target %q is duplicated", cleanTarget)
		}
		seenTargets[cleanTarget] = struct{}{}
		*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", targetPath))
		if err := os.MkdirAll(targetPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create mount target %q: %w", targetPath, err)
		}
		if spec.Source == "" {
			continue
		}
		source, err := validateAccessibleAbsoluteDirectory(spec.Source, "mount source")
		if err != nil {
			return "", err
		}
		fstabLines = append(fstabLines, fmt.Sprintf("%s %s nullfs rw 0 0", source, targetPath))
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
	if err := writeFileAtomicExclusive(fstabPath, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("failed to write %q: %w", fstabPath, err)
	}
	return fstabPath, nil
}

func clearDirectoryContents(path string, logs *[]string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("failed to read %q for rollback cleanup: %w", path, err)
	}
	for _, entry := range entries {
		target := filepath.Join(path, entry.Name())
		*logs = append(*logs, "$ rm -rf "+target)
		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("failed to remove %q during rollback cleanup: %w", target, err)
		}
	}
	return nil
}

func rollbackPreexistingEmptyDir(path string, logs *[]string) {
	if err := clearDestroyPathFlags(path, logs); err != nil {
		*logs = append(*logs, "  rollback warning: "+err.Error())
	}
	if err := clearDirectoryContents(path, logs); err != nil {
		*logs = append(*logs, "  rollback warning: "+err.Error())
	}
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
	if err := writeFileAtomicExclusive(configPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write %q: %w", configPath, err)
	}
	return nil
}

func runLoggedCommand(ctx context.Context, logs *[]string, name string, args ...string) (string, error) {
	command := name
	if len(args) > 0 {
		command += " " + strings.Join(args, " ")
	}
	*logs = append(*logs, "$ "+command)
	debugLog("Command", command)
	reportCreateLog(ctx, "$ "+command)

	cmd := exec.CommandContext(ctx, name, args...)
	reader, writer, err := os.Pipe()
	if err != nil {
		return "", fmt.Errorf("pipe setup failed: %w", err)
	}
	cmd.Stdout = writer
	cmd.Stderr = writer

	outputLines := make([]string, 0, 16)
	scanDone := make(chan error, 1)
	go func() {
		defer reader.Close()
		scanner := bufio.NewScanner(reader)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			outputLines = append(outputLines, line)
			*logs = append(*logs, "  "+line)
			debugLog("Command", "  "+line)
			reportCreateLog(ctx, "  "+line)
		}
		scanDone <- scanner.Err()
	}()

	if err := cmd.Start(); err != nil {
		_ = writer.Close()
		_ = reader.Close()
		return "", fmt.Errorf("%s: %w", command, err)
	}
	waitErr := cmd.Wait()
	closeErr := writer.Close()
	scanErr := <-scanDone
	text := strings.TrimSpace(strings.Join(outputLines, "\n"))

	if closeErr != nil {
		return text, fmt.Errorf("failed to close command output for %s: %w", command, closeErr)
	}
	if scanErr != nil {
		return text, fmt.Errorf("failed to read command output for %s: %w", command, scanErr)
	}
	if waitErr != nil {
		return text, fmt.Errorf("%s: %w", command, waitErr)
	}
	return text, nil
}

func zfsDatasetExists(dataset string) bool {
	dataset = strings.TrimSpace(dataset)
	if dataset == "" {
		return false
	}
	return exec.Command("zfs", "list", "-H", "-o", "name", dataset).Run() == nil
}

func zfsSnapshotExists(snapshot string) bool {
	snapshot = strings.TrimSpace(snapshot)
	if snapshot == "" {
		return false
	}
	return exec.Command("zfs", "list", "-H", "-t", "snapshot", "-o", "name", snapshot).Run() == nil
}

func jailExecutableExists(jailName, path string) bool {
	jailName = strings.TrimSpace(jailName)
	path = strings.TrimSpace(path)
	if jailName == "" || path == "" {
		return false
	}
	return exec.Command("jexec", jailName, "test", "-x", path).Run() == nil
}
