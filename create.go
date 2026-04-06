package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var releaseValuePattern = regexp.MustCompile(`^[0-9]+\.[0-9]+-RELEASE`)

var errTemplateDatasetParentMissing = errors.New("template parent dataset missing")

const (
	defaultUserlandDir     = "/usr/local/jails/media"
	defaultDownloadHost    = "https://download.freebsd.org"
	archiveDownloadTimeout = 30 * time.Minute
)

type JailCreationResult struct {
	Name       string
	ConfigPath string
	FstabPath  string
	JailPath   string
	Logs       []string
	Warnings   []string
	Err        error
}

type TemplateDatasetResult struct {
	Dataset    string
	Mountpoint string
	Parent     string
	Logs       []string
	Err        error
}

type TemplateDatasetPreview struct {
	SourceInput       string
	SourceKind        string
	ResolvedSource    string
	Action            string
	PatchSelected     bool
	PatchEligible     bool
	PatchRelease      string
	PatchNote         string
	ParentDataset     string
	ParentMountpoint  string
	Dataset           string
	Mountpoint        string
	NeedsParentCreate bool
	Err               error
}

type TemplateParentDatasetResult struct {
	Dataset    string
	Mountpoint string
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
	cleanups := make([]func(), 0, 8)
	addCleanup := func(fn func()) {
		if fn != nil {
			cleanups = append(cleanups, fn)
		}
	}
	warnings := make([]string, 0, 4)
	fail := func(err error) JailCreationResult {
		for idx := len(cleanups) - 1; idx >= 0; idx-- {
			cleanups[idx]()
		}
		result.Logs = logs
		result.Warnings = warnings
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

	hostNetworkCleanup, err := ensureHostNetworkReady(values, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(hostNetworkCleanup)

	jailPath, pathCleanup, err := prepareJailPath(values, destination, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(pathCleanup)
	result.JailPath = jailPath

	rootCleanup, err := provisionJailRoot(values, jailPath, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(rootCleanup)
	patchDecision := resolveFreeBSDPatchDecision(values.TemplateRelease, values.PatchBase)
	if patchDecision.Err != nil {
		return fail(patchDecision.Err)
	}
	if patchDecision.Effective {
		if err := patchFreeBSDRoot(jailPath, &logs); err != nil {
			return fail(err)
		}
	}
	if normalizedJailType(values.JailType) == "linux" {
		linuxCleanup, err := ensureLinuxHostReady(&logs)
		if err != nil {
			return fail(err)
		}
		addCleanup(linuxCleanup)
	}

	mountSpecs := parseMountPointSpecs(values.MountPoints)
	fstabPath, err := configureMountPoints(result.Name, jailPath, mountSpecs, &logs)
	if err != nil {
		return fail(err)
	}
	result.FstabPath = fstabPath
	addCleanup(func() {
		if err := removeFileIfExists(fstabPath, &logs); err != nil {
			logs = append(logs, "rollback warning: "+err.Error())
		}
	})

	configLines := buildJailConfBlock(values, jailPath, fstabPath)
	if err := writeJailConfigFile(result.ConfigPath, configLines, &logs); err != nil {
		return fail(err)
	}
	addCleanup(func() {
		if err := removeFileIfExists(result.ConfigPath, &logs); err != nil {
			logs = append(logs, "rollback warning: "+err.Error())
		}
	})
	startupCleanup, err := updateJailStartupConfig(result.Name, values, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(startupCleanup)
	persistentRctlCleanup, err := syncPersistentJailRctlRules(values, result.Name, &logs)
	if err != nil {
		return fail(err)
	}
	addCleanup(persistentRctlCleanup)

	if _, err := runLoggedCommand(&logs, "service", "jail", "start", result.Name); err != nil {
		return fail(err)
	}
	addCleanup(func() {
		if _, err := runLoggedCommand(&logs, "service", "jail", "stop", result.Name); err != nil {
			logs = append(logs, fmt.Sprintf("rollback warning: failed to stop jail %q: %v", result.Name, err))
		}
	})
	if normalizedJailType(values.JailType) == "linux" {
		bootstrapWarnings, err := maybeBootstrapLinuxUserland(values, result.Name, &logs)
		if err != nil {
			return fail(err)
		}
		warnings = append(warnings, bootstrapWarnings...)
	}
	if err := applyRctlLimits(values, result.Name, &logs); err != nil {
		return fail(err)
	}
	addCleanup(func() {
		removeJailRctlRules(result.Name, &logs)
	})

	logf("Jail %s created successfully.", result.Name)
	result.Logs = logs
	result.Warnings = warnings
	return result
}

func prepareJailPath(values jailWizardValues, destination string, logs *[]string) (string, func(), error) {
	if normalizedJailType(values.JailType) == "thin" {
		return ensureThinDestinationPath(destination, logs)
	}
	return ensureDestinationJailPath(destination, logs)
}

func ensureDestinationJailPath(destination string, logs *[]string) (string, func(), error) {
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
				*logs = append(*logs, "  rollback note: destination existed before create; leaving "+jailPath+" in place for manual review")
			}, nil
		}
		return jailPath, nil, nil
	}

	// Backward compatibility: treat non-absolute values as ZFS dataset names.
	var err error
	destination, err = validateZFSDatasetName(destination, "destination dataset")
	if err != nil {
		return "", nil, err
	}
	createdDataset := false
	if !zfsDatasetExists(destination) {
		if _, createErr := runLoggedCommand(logs, "zfs", "create", "-p", destination); createErr != nil {
			return "", nil, fmt.Errorf("failed to ensure dataset %q: %w", destination, createErr)
		}
		createdDataset = true
	}

	mountpointOut, err := runLoggedCommand(logs, "zfs", "list", "-H", "-o", "mountpoint", destination)
	if err != nil {
		return "", nil, fmt.Errorf("failed to discover mountpoint for %q: %w", destination, err)
	}
	mountpoint := strings.TrimSpace(strings.Split(mountpointOut, "\n")[0])
	if mountpoint == "" || mountpoint == "-" || mountpoint == "legacy" {
		mountpoint = "/" + strings.Trim(destination, "/")
	}

	*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", mountpoint))
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return "", nil, fmt.Errorf("failed to create jail path %q: %w", mountpoint, err)
	}
	if createdDataset {
		return mountpoint, func() {
			if _, err := runLoggedCommand(logs, "zfs", "destroy", "-r", destination); err != nil {
				*logs = append(*logs, "  rollback warning: failed to destroy dataset "+destination+": "+err.Error())
			}
		}, nil
	}
	return mountpoint, nil, nil
}

func provisionJailRoot(values jailWizardValues, jailPath string, logs *[]string) (func(), error) {
	switch normalizedJailType(values.JailType) {
	case "thin":
		return provisionThinJailRoot(values, jailPath, logs)
	case "linux":
		if err := provisionStandardJailRoot(jailPath, strings.TrimSpace(values.TemplateRelease), logs); err != nil {
			return nil, err
		}
		return nil, ensureLinuxCompatPaths(jailPath, values, logs)
	default:
		if err := provisionStandardJailRoot(jailPath, strings.TrimSpace(values.TemplateRelease), logs); err != nil {
			return nil, err
		}
		return nil, seedGuestBaseFiles(jailPath, logs)
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

func ensureThinDestinationPath(destination string, logs *[]string) (string, func(), error) {
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

func provisionThinJailRoot(values jailWizardValues, jailPath string, logs *[]string) (func(), error) {
	sourcePath, cleanup, err := resolveTemplateSource(strings.TrimSpace(values.TemplateRelease), logs)
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
		if _, snapshotErr := runLoggedCommand(logs, "zfs", "snapshot", snapshot); snapshotErr != nil {
			return nil, fmt.Errorf("failed to create thin jail template snapshot %q: %w", snapshot, snapshotErr)
		}
	}

	cloneDataset := parentDataset.Name + "/" + filepath.Base(jailPath)
	if zfsDatasetExists(cloneDataset) {
		return nil, fmt.Errorf("thin jail dataset %q already exists", cloneDataset)
	}
	if _, err := runLoggedCommand(logs, "zfs", "clone", snapshot, cloneDataset); err != nil {
		return nil, fmt.Errorf("failed to clone thin jail dataset %q from %q: %w", cloneDataset, snapshot, err)
	}
	expectedMount := filepath.Join(filepath.Clean(parentDataset.Mountpoint), filepath.Base(jailPath))
	if filepath.Clean(expectedMount) != filepath.Clean(jailPath) {
		if _, err := runLoggedCommand(logs, "zfs", "set", "mountpoint="+jailPath, cloneDataset); err != nil {
			return nil, fmt.Errorf("failed to set mountpoint for thin jail dataset %q: %w", cloneDataset, err)
		}
	}
	cleanupClone := func() {
		if _, err := runLoggedCommand(logs, "zfs", "destroy", "-r", cloneDataset); err != nil {
			*logs = append(*logs, "  rollback warning: failed to destroy thin jail dataset "+cloneDataset+": "+err.Error())
		}
	}
	return cleanupClone, seedGuestBaseFiles(jailPath, logs)
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

func ensureLinuxHostReady(logs *[]string) (func(), error) {
	if _, err := os.Stat("/etc/rc.d/linux"); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("linux ABI host service script /etc/rc.d/linux is not present")
		}
		return nil, fmt.Errorf("failed to inspect linux host service script: %w", err)
	}

	previousEnable, err := readRCConfValue("linux_enable")
	if err != nil {
		return nil, fmt.Errorf("failed to inspect host linux_enable setting: %w", err)
	}
	previousEnable = strings.TrimSpace(previousEnable)
	previouslyEnabled := isEnabledRCValue(previousEnable)
	previouslyRunning := exec.Command("service", "linux", "onestatus").Run() == nil

	rcConfPaths := []string{"/etc/rc.conf", "/etc/rc.conf.local"}
	var rcConfBackups []managedPathBackup
	backupsReady := false
	ensureBackups := func() error {
		if backupsReady {
			return nil
		}
		backups, err := backupPathsForMutation(rcConfPaths, "linux-host-rcconf", logs)
		if err != nil {
			return err
		}
		rcConfBackups = backups
		backupsReady = true
		return nil
	}
	restoreBackups := func() {
		if !backupsReady {
			return
		}
		restorePathMutationBackups(rcConfBackups, logs)
	}

	changedEnable := false
	if !previouslyEnabled {
		if err := ensureRCSettingSafeToMutate("linux_enable"); err != nil {
			return nil, err
		}
		if err := ensureBackups(); err != nil {
			return nil, err
		}
		if _, err := runLoggedCommand(logs, "sysrc", "linux_enable=YES"); err != nil {
			restoreBackups()
			return nil, fmt.Errorf("failed to enable linux ABI on host: %w", err)
		}
		changedEnable = true
	}
	startedService := false
	if !previouslyRunning {
		if _, err := runLoggedCommand(logs, "service", "linux", "start"); err != nil {
			if changedEnable {
				restoreBackups()
			}
			return nil, fmt.Errorf("failed to start linux ABI service on host: %w", err)
		}
		startedService = true
	}

	if !changedEnable && !startedService {
		return nil, nil
	}
	return func() {
		if startedService {
			if _, err := runLoggedCommand(logs, "service", "linux", "stop"); err != nil {
				*logs = append(*logs, "  rollback warning: failed to stop linux ABI service: "+err.Error())
			}
		}
		if changedEnable {
			restoreBackups()
		}
	}, nil
}

func ensureLinuxCompatPaths(jailPath string, values jailWizardValues, logs *[]string) error {
	compatRoot := linuxCompatRoot(jailPath, values)
	paths := []string{
		filepath.Join(compatRoot, "dev", "shm"),
		filepath.Join(compatRoot, "dev", "fd"),
		filepath.Join(compatRoot, "proc"),
		filepath.Join(compatRoot, "sys"),
		filepath.Join(compatRoot, "tmp"),
		filepath.Join(compatRoot, "home"),
	}
	for _, path := range paths {
		*logs = append(*logs, fmt.Sprintf("$ mkdir -p %s", path))
		if err := os.MkdirAll(path, 0o755); err != nil {
			return fmt.Errorf("failed to prepare Linux compatibility path %q: %w", path, err)
		}
	}
	return seedGuestBaseFiles(jailPath, logs)
}

func bootstrapLinuxUserland(values jailWizardValues, jailName string, logs *[]string) error {
	distro := effectiveLinuxDistro(values)
	release := effectiveLinuxRelease(values)
	target := filepath.ToSlash(filepath.Join("/compat", distro))
	mirrorInfo, err := resolveLinuxMirror(values)
	if err != nil {
		return err
	}
	mirror := mirrorInfo.BaseURL

	if jailExecutableExists(jailName, filepath.ToSlash(filepath.Join(target, "bin", "sh"))) {
		*logs = append(*logs, "Linux userland already present under "+target+"; skipping debootstrap.")
		return nil
	}
	if _, err := runLoggedCommand(logs, "jexec", jailName, "env", "ASSUME_ALWAYS_YES=yes", "pkg", "bootstrap", "-f"); err != nil {
		return fmt.Errorf("failed to bootstrap pkg inside linux jail: %w", err)
	}
	if _, err := runLoggedCommand(logs, "jexec", jailName, "env", "ASSUME_ALWAYS_YES=yes", "pkg", "install", "-y", "debootstrap"); err != nil {
		return fmt.Errorf("failed to install debootstrap inside linux jail: %w", err)
	}
	if _, err := runLoggedCommand(logs, "jexec", jailName, "debootstrap", "--arch", hostArch(), release, target, mirror); err != nil {
		return fmt.Errorf("failed to bootstrap %s %s inside linux jail: %w", distro, release, err)
	}
	return nil
}

func maybeBootstrapLinuxUserland(values jailWizardValues, jailName string, logs *[]string) ([]string, error) {
	if effectiveLinuxBootstrapMode(values) == "skip" {
		return []string{"Linux bootstrap skipped by wizard setting. Use detail view action 'b' after networking is ready."}, nil
	}
	if err := preflightLinuxBootstrap(values, jailName, logs); err != nil {
		return []string{err.Error() + " Use detail view action 'b' to retry after fixing networking."}, nil
	}
	if err := bootstrapLinuxUserland(values, jailName, logs); err != nil {
		return []string{err.Error() + " Use detail view action 'b' to retry after fixing networking or package access."}, nil
	}
	return nil, nil
}

func preflightLinuxBootstrap(values jailWizardValues, jailName string, logs *[]string) error {
	mirrorInfo, err := resolveLinuxMirror(values)
	if err != nil {
		return err
	}
	hasIPv4Route := checkLinuxRouteFamily(jailName, "inet", logs)
	hasIPv6Route := checkLinuxRouteFamily(jailName, "inet6", logs)
	if !hasIPv4Route && !hasIPv6Route {
		if err := checkLinuxGenericFetchReachability(mirrorInfo.PreflightURL, jailName, logs); err == nil {
			*logs = append(*logs, "Linux bootstrap preflight: shared-stack fetch succeeded without an explicit default route probe.")
			return nil
		}
		return fmt.Errorf("linux bootstrap preflight failed: no IPv4 or IPv6 default route inside the jail")
	}
	host := mirrorInfo.Host
	if host == "" {
		return fmt.Errorf("linux bootstrap preflight failed: could not determine mirror host")
	}
	hasIPv4DNS, hasIPv6DNS, err := checkLinuxDNSFamilies(jailName, host, logs)
	if err != nil {
		return err
	}
	if hasIPv4Route && !hasIPv4DNS && hasIPv6Route && !hasIPv6DNS {
		return fmt.Errorf("linux bootstrap preflight failed: DNS returned no IPv4 or IPv6 answers for %s", host)
	}
	if hasIPv4Route && !hasIPv4DNS && !hasIPv6Route {
		return fmt.Errorf("linux bootstrap preflight failed: DNS returned no IPv4 answers for %s", host)
	}
	if hasIPv6Route && !hasIPv6DNS && !hasIPv4Route {
		return fmt.Errorf("linux bootstrap preflight failed: DNS returned no IPv6 answers for %s", host)
	}
	if err := checkLinuxFetchReachability(mirrorInfo.PreflightURL, jailName, hasIPv4Route && hasIPv4DNS, hasIPv6Route && hasIPv6DNS, logs); err != nil {
		return err
	}
	return nil
}

func checkLinuxRouteFamily(jailName, family string, logs *[]string) bool {
	args := []string{"jexec", jailName, "route", "-n", "get"}
	switch family {
	case "inet6":
		args = append(args, "-inet6")
	default:
		args = append(args, "-inet")
	}
	args = append(args, "default")
	_, err := runLoggedCommand(logs, args[0], args[1:]...)
	return err == nil
}

func checkLinuxFetchReachability(preflightURL, jailName string, hasIPv4Route, hasIPv6Route bool, logs *[]string) error {
	var failures []string
	if hasIPv4Route {
		if _, err := runLoggedCommand(logs, "jexec", jailName, "fetch", "-4", "-qo", "/dev/null", preflightURL); err == nil {
			return nil
		} else {
			failures = append(failures, "IPv4 fetch failed")
		}
	}
	if hasIPv6Route {
		if _, err := runLoggedCommand(logs, "jexec", jailName, "fetch", "-6", "-qo", "/dev/null", preflightURL); err == nil {
			return nil
		} else {
			failures = append(failures, "IPv6 fetch failed")
		}
	}
	if len(failures) == 0 {
		failures = append(failures, "no usable IP family available for fetch")
	}
	return fmt.Errorf("linux bootstrap preflight failed: could not fetch %s (%s)", preflightURL, strings.Join(failures, ", "))
}

func checkLinuxGenericFetchReachability(preflightURL, jailName string, logs *[]string) error {
	if strings.TrimSpace(preflightURL) == "" {
		return fmt.Errorf("linux bootstrap preflight failed: no preflight URL available")
	}
	_, err := runLoggedCommand(logs, "jexec", jailName, "fetch", "-qo", "/dev/null", preflightURL)
	if err != nil {
		return fmt.Errorf("linux bootstrap preflight failed: generic fetch to %s failed: %w", preflightURL, err)
	}
	return nil
}

func checkLinuxDNSFamilies(jailName, host string, logs *[]string) (bool, bool, error) {
	out, err := runLoggedCommand(logs, "jexec", jailName, "getent", "hosts", host)
	if err != nil {
		return false, false, fmt.Errorf("linux bootstrap preflight failed: DNS lookup failed for %s", host)
	}
	var hasIPv4, hasIPv6 bool
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 0 {
			continue
		}
		ip := net.ParseIP(fields[0])
		if ip == nil {
			continue
		}
		if ip.To4() != nil {
			hasIPv4 = true
			continue
		}
		if ip.To16() != nil {
			hasIPv6 = true
		}
	}
	return hasIPv4, hasIPv6, nil
}

func hostArch() string {
	out, err := exec.Command("uname", "-m").Output()
	if err != nil {
		return "amd64"
	}
	arch := strings.TrimSpace(string(out))
	if arch == "" {
		return "amd64"
	}
	return arch
}

func ExecuteTemplateDatasetCreate(sourceInput string) TemplateDatasetResult {
	return ExecuteTemplateDatasetCreateWithParent(sourceInput, nil, "auto")
}

func ExecuteTemplateDatasetCreateWithParent(sourceInput string, parentOverride *templateDatasetParent, patchPreference string) TemplateDatasetResult {
	result := TemplateDatasetResult{}
	logs := make([]string, 0, 24)
	fail := func(err error) TemplateDatasetResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	sourceInput = strings.TrimSpace(sourceInput)
	if sourceInput == "" {
		return fail(fmt.Errorf("template/release is required before a template dataset can be created"))
	}

	parent, err := resolveTemplateDatasetParent(parentOverride)
	if err != nil {
		return fail(err)
	}
	templateName := suggestTemplateDatasetName(sourceInput)
	if templateName == "" {
		return fail(fmt.Errorf("could not derive a template dataset name from %q", sourceInput))
	}

	sourcePath, cleanup, err := resolveTemplateSource(sourceInput, &logs)
	if err != nil {
		return fail(err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	patchDecision := resolveFreeBSDPatchDecision(sourceInput, patchPreference)
	if patchDecision.Err != nil {
		return fail(patchDecision.Err)
	}

	childDataset := parent.Name + "/" + templateName
	childMountpoint := filepath.Join(parent.Mountpoint, templateName)
	validatedChildDataset, err := validateZFSDatasetName(childDataset, "template dataset")
	if err != nil {
		return fail(err)
	}
	validatedChildMountpoint, err := validateAbsolutePath(childMountpoint, "template mountpoint")
	if err != nil {
		return fail(err)
	}
	if validatedChildMountpoint, err = validateUnusedMountpointPath(validatedChildMountpoint, "template mountpoint"); err != nil {
		return fail(err)
	}
	result.Parent = parent.Name
	result.Dataset = validatedChildDataset
	result.Mountpoint = validatedChildMountpoint

	if zfsDatasetExists(result.Dataset) {
		return fail(fmt.Errorf("template dataset %q already exists", result.Dataset))
	}

	if _, err := runLoggedCommand(&logs, "zfs", "create", "-o", "mountpoint="+result.Mountpoint, result.Dataset); err != nil {
		return fail(fmt.Errorf("failed to create template dataset %q: %w", result.Dataset, err))
	}

	success := false
	defer func() {
		if success {
			return
		}
		_, _ = runLoggedCommand(&logs, "zfs", "destroy", "-r", result.Dataset)
	}()

	info, err := os.Stat(sourcePath)
	if err != nil {
		return fail(fmt.Errorf("template source %q is not accessible: %w", sourcePath, err))
	}
	if info.IsDir() {
		if _, err := runLoggedCommand(&logs, "cp", "-a", sourcePath+"/.", result.Mountpoint+"/"); err != nil {
			return fail(fmt.Errorf("failed to copy template source into %q: %w", result.Dataset, err))
		}
	} else {
		if _, err := runLoggedCommand(&logs, "tar", "-xf", sourcePath, "-C", result.Mountpoint); err != nil {
			return fail(fmt.Errorf("failed to extract template archive into %q: %w", result.Dataset, err))
		}
	}
	if patchDecision.Effective {
		if err := patchFreeBSDRoot(result.Mountpoint, &logs); err != nil {
			return fail(err)
		}
	}
	if err := finalizeTemplateDatasetReadonly(result.Dataset, &logs); err != nil {
		return fail(err)
	}

	success = true
	result.Logs = logs
	return result
}

func InspectTemplateDatasetCreate(sourceInput string) TemplateDatasetPreview {
	return InspectTemplateDatasetCreateWithParent(sourceInput, nil, "auto")
}

func InspectTemplateDatasetCreateWithParent(sourceInput string, parentOverride *templateDatasetParent, patchPreference string) TemplateDatasetPreview {
	preview := TemplateDatasetPreview{
		SourceInput: strings.TrimSpace(sourceInput),
	}
	patchDecision := resolveFreeBSDPatchDecision(preview.SourceInput, patchPreference)
	preview.PatchSelected = patchDecision.Effective
	preview.PatchEligible = patchDecision.Eligible
	preview.PatchRelease = patchDecision.ReleaseVersion
	preview.PatchNote = patchDecision.Note
	if patchDecision.Err != nil {
		preview.Err = patchDecision.Err
		return preview
	}

	parent, err := resolveTemplateDatasetParent(parentOverride)
	if err != nil {
		if errors.Is(err, errTemplateDatasetParentMissing) {
			preview.NeedsParentCreate = true
		}
		preview.Err = err
		return preview
	}
	preview.ParentDataset = parent.Name
	preview.ParentMountpoint = parent.Mountpoint

	if preview.SourceInput == "" {
		return preview
	}

	templateName := suggestTemplateDatasetName(preview.SourceInput)
	if templateName == "" {
		preview.Err = fmt.Errorf("could not derive a template dataset name from %q", preview.SourceInput)
		return preview
	}
	preview.Dataset = parent.Name + "/" + templateName
	preview.Mountpoint = filepath.Join(parent.Mountpoint, templateName)
	if preview.Dataset, err = validateZFSDatasetName(preview.Dataset, "template dataset"); err != nil {
		preview.Err = err
		return preview
	}
	if preview.Mountpoint, err = validateAbsolutePath(preview.Mountpoint, "template mountpoint"); err != nil {
		preview.Err = err
		return preview
	}
	if preview.Mountpoint, err = validateUnusedMountpointPath(preview.Mountpoint, "template mountpoint"); err != nil {
		preview.Err = err
		return preview
	}

	sourceKind, resolvedSource, action, err := inspectTemplateSourceInput(preview.SourceInput)
	preview.SourceKind = sourceKind
	preview.ResolvedSource = resolvedSource
	preview.Action = action
	if err != nil {
		preview.Err = err
		return preview
	}

	if _, err := exec.Command("zfs", "list", "-H", "-o", "name", preview.Dataset).Output(); err == nil {
		preview.Err = fmt.Errorf("template dataset %q already exists", preview.Dataset)
	}

	return preview
}

func ExecuteTemplateParentDatasetCreate(dataset, mountpoint string) TemplateParentDatasetResult {
	result := TemplateParentDatasetResult{
		Dataset:    strings.TrimSpace(dataset),
		Mountpoint: strings.TrimSpace(mountpoint),
	}
	var logs []string
	fail := func(err error) TemplateParentDatasetResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	var err error
	if result.Dataset, err = validateZFSDatasetName(result.Dataset, "parent dataset"); err != nil {
		return fail(err)
	}
	if result.Mountpoint, err = validateAbsolutePath(result.Mountpoint, "parent mountpoint"); err != nil {
		return fail(err)
	}

	out, err := exec.Command("zfs", "list", "-H", "-o", "mountpoint", result.Dataset).CombinedOutput()
	if err == nil {
		existing := strings.TrimSpace(strings.Split(string(out), "\n")[0])
		if existing == "" || existing == "-" || existing == "legacy" {
			return fail(fmt.Errorf("dataset %q already exists with unusable mountpoint %q; set a real mountpoint or choose a different dataset", result.Dataset, existing))
		}
		if existing != "" && existing != "-" && existing != "legacy" && filepath.Clean(existing) != result.Mountpoint {
			return fail(fmt.Errorf("dataset %q already exists with mountpoint %q", result.Dataset, existing))
		}
		result.Logs = logs
		return result
	}

	if _, err := runLoggedCommand(&logs, "zfs", "create", "-o", "mountpoint="+result.Mountpoint, result.Dataset); err != nil {
		return fail(fmt.Errorf("failed to create template parent dataset %q: %w", result.Dataset, err))
	}
	result.Logs = logs
	return result
}

func linuxBootstrapConfigFromRawLines(lines []string) jailWizardValues {
	values := jailWizardValues{}
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
			case "linux_distro":
				values.LinuxDistro = strings.TrimSpace(value)
			case "linux_release":
				values.LinuxRelease = strings.TrimSpace(value)
			case "linux_bootstrap":
				values.LinuxBootstrap = strings.TrimSpace(value)
			case "linux_mirror_mode":
				values.LinuxMirrorMode = strings.TrimSpace(value)
			case "linux_mirror_url":
				if strings.TrimSpace(value) != "-" {
					values.LinuxMirrorURL = strings.TrimSpace(value)
				}
			}
		}
	}
	if strings.TrimSpace(values.LinuxDistro) == "" {
		re := regexp.MustCompile(`/compat/([^/\s"]+)`)
		for _, raw := range lines {
			matches := re.FindStringSubmatch(raw)
			if len(matches) == 2 {
				values.LinuxDistro = matches[1]
				break
			}
		}
	}
	return values
}

type templateDatasetParent struct {
	Name       string
	Mountpoint string
}

func discoverTemplateDatasetParent() (*templateDatasetParent, error) {
	out, err := exec.Command("zfs", "list", "-H", "-o", "name,mountpoint", "-t", "filesystem").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to discover templates dataset: %w", err)
	}

	var fallback *templateDatasetParent
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			fields = strings.Fields(line)
		}
		if len(fields) < 2 {
			continue
		}
		name := strings.TrimSpace(fields[0])
		mountpoint := strings.TrimSpace(fields[1])
		if name == "" || mountpoint == "" || mountpoint == "-" || mountpoint == "legacy" {
			continue
		}
		parent := &templateDatasetParent{Name: name, Mountpoint: mountpoint}
		if name == docDatasetTemplate || filepath.Clean(mountpoint) == filepath.Join(docJailsPath, "templates") {
			return parent, nil
		}
		if filepath.Base(name) == "templates" || filepath.Base(mountpoint) == "templates" {
			if fallback == nil {
				fallback = parent
			}
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("%w: no templates dataset found; create one first in the initial config check or under your jail dataset layout", errTemplateDatasetParentMissing)
}

func resolveTemplateDatasetParent(parentOverride *templateDatasetParent) (*templateDatasetParent, error) {
	if parentOverride != nil {
		name := strings.TrimSpace(parentOverride.Name)
		mountpoint := strings.TrimSpace(parentOverride.Mountpoint)
		if name != "" && mountpoint != "" {
			validatedName, err := validateZFSDatasetName(name, "parent dataset")
			if err != nil {
				return nil, err
			}
			validatedMountpoint, err := validateAbsolutePath(mountpoint, "parent mountpoint")
			if err != nil {
				return nil, err
			}
			return &templateDatasetParent{Name: validatedName, Mountpoint: validatedMountpoint}, nil
		}
	}
	return discoverTemplateDatasetParent()
}

func inspectTemplateSourceInput(input string) (string, string, string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", "", fmt.Errorf("template/release is required")
	}

	if info, err := os.Stat(input); err == nil {
		cleanInput, pathErr := validateAbsolutePath(input, "template/release path")
		if pathErr != nil {
			return "", "", "", pathErr
		}
		if info.IsDir() {
			return "local directory", cleanInput, "copy directory contents into the new dataset", nil
		}
		return "local archive", cleanInput, "extract archive into the new dataset", nil
	}

	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		info, err := os.Stat(source)
		if err != nil {
			return "", "", "", fmt.Errorf("named userland source %q is not accessible: %w", source, err)
		}
		if info.IsDir() {
			return "named userland directory", source, "copy directory contents into the new dataset", nil
		}
		return "named userland archive", source, "extract archive into the new dataset", nil
	}

	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		parsed, err := neturl.Parse(input)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return "", "", "", fmt.Errorf("template/release URL %q is invalid", input)
		}
		return "custom URL", input, "download and extract archive into the new dataset", nil
	}

	if releaseValuePattern.MatchString(strings.ToUpper(input)) {
		localBaseArchive := "/usr/freebsd-dist/base.txz"
		if _, err := os.Stat(localBaseArchive); err == nil {
			return "release tag", localBaseArchive, "extract archive into the new dataset", nil
		}
		if source, ok := findReleaseArchiveInUserland(defaultUserlandDir, input); ok {
			return "release tag", source, "extract archive into the new dataset", nil
		}
		urls, err := defaultReleaseBaseURLs(input)
		if err != nil {
			return "", "", "", err
		}
		if len(urls) == 0 {
			return "", "", "", fmt.Errorf("could not resolve a download URL for release %q", input)
		}
		return "release tag", urls[0], "download and extract archive into the new dataset", nil
	}

	return "", "", "", fmt.Errorf(
		"template/release %q not found; use a local path, an entry from %s, a release tag, or a custom URL",
		input,
		defaultUserlandDir,
	)
}

func suggestTemplateDatasetName(sourceInput string) string {
	input := strings.TrimSpace(sourceInput)
	if input == "" {
		return ""
	}
	upper := strings.ToUpper(input)
	if releaseValuePattern.MatchString(upper) {
		return sanitizeTemplateDatasetName(strings.ToLower(input))
	}
	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		if parsed, err := neturl.Parse(input); err == nil {
			base := pathBaseNoExt(parsed.Path)
			parent := pathBaseNoExt(filepath.Dir(parsed.Path))
			if strings.EqualFold(base, "base") && releaseValuePattern.MatchString(strings.ToUpper(parent)) {
				return sanitizeTemplateDatasetName(strings.ToLower(parent))
			}
			return sanitizeTemplateDatasetName(strings.ToLower(base))
		}
	}
	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		return suggestTemplateDatasetName(source)
	}
	base := filepath.Base(input)
	parent := filepath.Base(filepath.Dir(input))
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if strings.EqualFold(base, "base") && parent != "." && parent != string(filepath.Separator) {
		base = parent
	}
	return sanitizeTemplateDatasetName(strings.ToLower(base))
}

func sanitizeTemplateDatasetName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteRune('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func pathBaseNoExt(raw string) string {
	base := filepath.Base(strings.TrimSpace(raw))
	return strings.TrimSuffix(base, filepath.Ext(base))
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
		cleanInput, pathErr := validateAbsolutePath(input, "template/release path")
		if pathErr != nil {
			return "", nil, pathErr
		}
		return cleanInput, nil, nil
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
	arch := hostArch()
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
	client := &http.Client{Timeout: archiveDownloadTimeout}
	resp, err := client.Get(url) // #nosec G107 user-provided URL is intentional
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
		source, err := validateAccessibleAbsolutePath(spec.Source, "mount source")
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

func resolveMountTargetPath(jailPath, target string) (string, error) {
	_, targetPath, err := validateMountTargetPath(jailPath, target)
	if err != nil {
		return "", err
	}
	return targetPath, nil
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

func applyRctlLimits(values jailWizardValues, jailName string, logs *[]string) error {
	if !hasAnyRctlLimits(values) {
		return nil
	}
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

func finalizeTemplateDatasetReadonly(dataset string, logs *[]string) error {
	dataset = strings.TrimSpace(dataset)
	if dataset == "" {
		return fmt.Errorf("template dataset is required")
	}
	if _, err := runLoggedCommand(logs, "zfs", "set", "readonly=on", dataset); err != nil {
		return fmt.Errorf("failed to finalize template dataset %q as readonly: %w", dataset, err)
	}
	return nil
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
