package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func ensureLinuxHostReady(ctx context.Context, logs *[]string) (func(), error) {
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
		if _, err := runLoggedCommand(ctx, logs, "sysrc", "linux_enable=YES"); err != nil {
			restoreBackups()
			return nil, fmt.Errorf("failed to enable linux ABI on host: %w", err)
		}
		changedEnable = true
	}
	startedService := false
	if !previouslyRunning {
		if _, err := runLoggedCommand(ctx, logs, "service", "linux", "start"); err != nil {
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
			if _, err := runLoggedCommand(ctx, logs, "service", "linux", "stop"); err != nil {
				*logs = append(*logs, "  rollback warning: failed to stop linux ABI service: "+err.Error())
			}
		}
		if changedEnable {
			restoreBackups()
		}
	}, nil
}

func ensureLinuxCompatPaths(ctx context.Context, jailPath string, values jailWizardValues, logs *[]string) error {
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
	return seedGuestBaseFiles(ctx, jailPath, logs)
}

func bootstrapLinuxUserland(ctx context.Context, values jailWizardValues, jailName, jailPath string, logs *[]string) error {
	distro := effectiveLinuxDistro(values)
	release := effectiveLinuxRelease(values)
	target := filepath.ToSlash(filepath.Join("/compat", distro))
	sourceInfo, err := resolveLinuxBootstrapSource(values)
	if err != nil {
		return err
	}

	switch effectiveLinuxBootstrapMethod(values) {
	case "archive":
		if strings.TrimSpace(jailPath) == "" {
			return fmt.Errorf("jail path is required for archive bootstrap")
		}
		targetHostPath := linuxCompatRoot(jailPath, values)
		if linuxShellPathPresent(targetHostPath) {
			*logs = append(*logs, "Linux userland already present under "+target+"; skipping bootstrap.")
			return nil
		}
		if err := bootstrapLinuxArchiveUserland(ctx, values, jailName, jailPath, sourceInfo, logs); err != nil {
			return fmt.Errorf("failed to bootstrap %s from %s inside linux jail: %w", distro, sourceInfo.URL, err)
		}
	case "debootstrap":
		if jailExecutableExists(jailName, filepath.ToSlash(filepath.Join(target, "bin", "sh"))) {
			*logs = append(*logs, "Linux userland already present under "+target+"; skipping bootstrap.")
			return nil
		}
		if _, err := runLoggedCommand(ctx, logs, "jexec", jailName, "env", "ASSUME_ALWAYS_YES=yes", "pkg", "bootstrap", "-f"); err != nil {
			return fmt.Errorf("failed to bootstrap pkg inside linux jail: %w", err)
		}
		if _, err := runLoggedCommand(ctx, logs, "jexec", jailName, "env", "ASSUME_ALWAYS_YES=yes", "pkg", "install", "-y", "debootstrap"); err != nil {
			return fmt.Errorf("failed to install debootstrap inside linux jail: %w", err)
		}
		if err := validateJailLinuxDebootstrapReleaseSupport(ctx, jailName, release, logs); err != nil {
			return fmt.Errorf("failed to bootstrap %s %s inside linux jail: %w", distro, release, err)
		}
		if _, err := runLoggedCommand(ctx, logs, "jexec", jailName, "debootstrap", "--arch", hostArch(), release, target, sourceInfo.URL); err != nil {
			return fmt.Errorf("failed to bootstrap %s %s inside linux jail: %w", distro, release, err)
		}
	default:
		return fmt.Errorf("unsupported linux bootstrap method %q", effectiveLinuxBootstrapMethod(values))
	}
	return nil
}

func maybeBootstrapLinuxUserland(ctx context.Context, values jailWizardValues, jailName, jailPath string, logs *[]string) ([]string, error) {
	if effectiveLinuxBootstrapMode(values) == "skip" {
		if effectiveLinuxBootstrapMethod(values) == "archive" {
			return []string{"Linux bootstrap skipped by wizard setting. Use detail view action 'b' while the jail is stopped."}, nil
		}
		return []string{"Linux bootstrap skipped by wizard setting. Use detail view action 'b' after networking is ready."}, nil
	}
	if err := preflightLinuxBootstrap(ctx, values, jailName, logs); err != nil {
		if effectiveLinuxBootstrapMethod(values) == "archive" {
			return []string{err.Error() + " Use detail view action 'b' while the jail is stopped after fixing archive access."}, nil
		}
		return []string{err.Error() + " Use detail view action 'b' to retry after fixing networking."}, nil
	}
	if err := bootstrapLinuxUserland(ctx, values, jailName, jailPath, logs); err != nil {
		return []string{err.Error() + linuxBootstrapRetryGuidance(err)}, nil
	}
	return nil, nil
}

func linuxBootstrapRetryGuidance(err error) string {
	lower := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(lower, "does not support release"):
		return ""
	case strings.Contains(lower, "failed to install debootstrap") || strings.Contains(lower, "failed to bootstrap pkg"):
		return " Use detail view action 'b' to retry after fixing package access."
	case strings.Contains(lower, "failed to fetch archive bootstrap") || strings.Contains(lower, "failed to extract archive bootstrap"):
		return " Use detail view action 'b' to retry after fixing networking or archive access."
	case strings.Contains(lower, "requires the jail to be stopped"):
		return " Stop the jail and use detail view action 'b' again."
	default:
		return " Use detail view action 'b' to retry after fixing networking or bootstrap access."
	}
}

func validateJailLinuxDebootstrapReleaseSupport(ctx context.Context, jailName, release string, logs *[]string) error {
	release = strings.TrimSpace(release)
	if release == "" {
		return fmt.Errorf("bootstrap release is required")
	}
	scriptPath := filepath.ToSlash(filepath.Join(debootstrapScriptsDir, release))
	if _, err := runLoggedCommand(ctx, logs, "jexec", jailName, "test", "-f", scriptPath); err != nil {
		return fmt.Errorf("installed debootstrap does not support release %q on this host; choose a supported bootstrap release or update debootstrap", release)
	}
	return nil
}

func preflightLinuxBootstrap(ctx context.Context, values jailWizardValues, jailName string, logs *[]string) error {
	sourceInfo, err := resolveLinuxBootstrapSource(values)
	if err != nil {
		return err
	}
	if effectiveLinuxBootstrapMethod(values) == "archive" {
		*logs = append(*logs, "Linux bootstrap preflight: archive bootstrap uses host-side source access; jail route, DNS, and fetch checks are skipped.")
		return nil
	}
	if sourceInfo.IsLocal {
		*logs = append(*logs, "Linux bootstrap preflight: local archive source selected; skipping route, DNS, and fetch checks.")
		return nil
	}
	hasIPv4Route := checkLinuxRouteFamily(ctx, jailName, "inet", logs)
	hasIPv6Route := checkLinuxRouteFamily(ctx, jailName, "inet6", logs)
	if !hasIPv4Route && !hasIPv6Route {
		if err := checkLinuxGenericFetchReachability(ctx, sourceInfo.PreflightURL, jailName, logs); err == nil {
			*logs = append(*logs, "Linux bootstrap preflight: shared-stack fetch succeeded without an explicit default route probe.")
			return nil
		}
		return fmt.Errorf("linux bootstrap preflight failed: no IPv4 or IPv6 default route inside the jail")
	}
	host := sourceInfo.Host
	if host == "" {
		return fmt.Errorf("linux bootstrap preflight failed: could not determine bootstrap source host")
	}
	hasIPv4DNS, hasIPv6DNS, err := checkLinuxDNSFamilies(ctx, jailName, host, logs)
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
	if err := checkLinuxFetchReachability(ctx, sourceInfo.PreflightURL, jailName, hasIPv4Route && hasIPv4DNS, hasIPv6Route && hasIPv6DNS, logs); err != nil {
		return err
	}
	return nil
}

func bootstrapLinuxArchiveUserland(ctx context.Context, values jailWizardValues, jailName, jailPath string, sourceInfo linuxBootstrapSourceInfo, logs *[]string) error {
	if strings.TrimSpace(jailPath) == "" {
		return fmt.Errorf("jail path is required for archive bootstrap")
	}
	targetHostPath := linuxCompatRoot(jailPath, values)
	if mountedPaths, err := linuxMountedDescendants(targetHostPath); err != nil {
		return fmt.Errorf("failed to inspect active mounts under %s: %w", targetHostPath, err)
	} else if len(mountedPaths) > 0 {
		return fmt.Errorf("archive bootstrap requires the jail to be stopped before modifying %s; active mounts: %s", targetHostPath, strings.Join(mountedPaths, ", "))
	}
	stagePath := targetHostPath + ".bootstrap-stage"
	if err := removePathAllLogged(stagePath, logs); err != nil {
		return err
	}
	archiveHostPath, archiveCleanup, err := prepareLinuxArchiveSource(ctx, sourceInfo, jailName, jailPath, values, logs)
	if err != nil {
		return err
	}
	if archiveCleanup != nil {
		defer archiveCleanup()
	}
	if err := extractLinuxArchiveToStage(ctx, archiveHostPath, stagePath, logs); err != nil {
		return err
	}
	extractedRoot, cleanupStage, err := detectLinuxArchiveRoot(stagePath)
	if err != nil {
		_ = removePathAllLogged(stagePath, logs)
		return err
	}
	if cleanupStage != nil {
		defer cleanupStage()
	}
	if err := pruneLinuxArchiveStage(extractedRoot, logs); err != nil {
		_ = removePathAllLogged(stagePath, logs)
		return err
	}
	if err := clearLinuxCompatInstallTarget(targetHostPath, logs); err != nil {
		_ = removePathAllLogged(stagePath, logs)
		return err
	}
	if err := copyLinuxArchiveIntoCompat(ctx, extractedRoot, targetHostPath, logs); err != nil {
		_ = clearLinuxCompatInstallTarget(targetHostPath, logs)
		_ = removePathAllLogged(stagePath, logs)
		return err
	}
	if err := ensureLinuxCompatPaths(ctx, jailPath, values, logs); err != nil {
		_ = clearLinuxCompatInstallTarget(targetHostPath, logs)
		return fmt.Errorf("failed to prepare compatibility mount paths after archive bootstrap: %w", err)
	}
	if !linuxShellPathPresent(targetHostPath) {
		_ = clearLinuxCompatInstallTarget(targetHostPath, logs)
		return fmt.Errorf("extracted archive did not provide %s", filepath.ToSlash(filepath.Join(targetHostPath, "bin", "sh")))
	}
	if err := removePathAllLogged(stagePath, logs); err != nil {
		return err
	}
	return nil
}

func prepareLinuxArchiveSource(ctx context.Context, sourceInfo linuxBootstrapSourceInfo, jailName, jailPath string, values jailWizardValues, logs *[]string) (string, func(), error) {
	if sourceInfo.IsLocal {
		return sourceInfo.LocalPath, nil, nil
	}
	hostPath := filepath.Join(jailPath, "tmp", linuxArchiveDownloadName(values))
	if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
		return "", nil, fmt.Errorf("failed to create archive download directory %q: %w", filepath.Dir(hostPath), err)
	}
	if _, err := runLoggedCommand(ctx, logs, "fetch", "-o", hostPath, sourceInfo.URL); err != nil {
		return "", nil, fmt.Errorf("failed to fetch archive bootstrap from %s: %w", sourceInfo.URL, err)
	}
	cleanup := func() {
		_ = removePathAllLogged(hostPath, logs)
	}
	return hostPath, cleanup, nil
}

func extractLinuxArchiveToStage(ctx context.Context, archivePath, stagePath string, logs *[]string) error {
	*logs = append(*logs, "$ mkdir -p "+stagePath)
	if err := os.MkdirAll(stagePath, 0o755); err != nil {
		return fmt.Errorf("failed to create archive staging path %q: %w", stagePath, err)
	}
	if _, err := runLoggedCommand(ctx, logs, "tar", "--no-xattrs", "-xf", archivePath, "-C", stagePath); err != nil {
		return fmt.Errorf("failed to extract archive bootstrap from %s: %w", archivePath, err)
	}
	return nil
}

// Accept either bin/sh at the archive root or under exactly one top-level directory.
func detectLinuxArchiveRoot(stagePath string) (string, func(), error) {
	stagePath = filepath.Clean(strings.TrimSpace(stagePath))
	if stagePath == "" {
		return "", nil, fmt.Errorf("archive staging path is required")
	}
	if linuxShellPathPresent(stagePath) {
		return stagePath, nil, nil
	}
	entries, err := os.ReadDir(stagePath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to inspect extracted archive staging path %q: %w", stagePath, err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry.Name())
		}
	}
	if len(dirs) == 1 {
		nestedRoot := filepath.Join(stagePath, dirs[0])
		if linuxShellPathPresent(nestedRoot) {
			return nestedRoot, func() {}, nil
		}
		return "", nil, fmt.Errorf("archive extracted into a top-level subdirectory %q, but %s was not found there", dirs[0], filepath.ToSlash(filepath.Join(dirs[0], "bin", "sh")))
	}
	return "", nil, fmt.Errorf("archive bootstrap layout is unsupported: expected bin/sh at the archive root or under a single top-level directory")
}

func pruneLinuxArchiveStage(rootPath string, logs *[]string) error {
	for _, name := range linuxCompatReservedTopLevelNames() {
		path := filepath.Join(rootPath, name)
		if err := removePathAllLogged(path, logs); err != nil {
			return err
		}
	}
	return nil
}

func clearLinuxCompatInstallTarget(targetPath string, logs *[]string) error {
	targetPath = strings.TrimSpace(targetPath)
	if targetPath == "" {
		return fmt.Errorf("target path is required")
	}
	if _, err := os.Stat(targetPath); err != nil {
		if os.IsNotExist(err) {
			*logs = append(*logs, "$ mkdir -p "+targetPath)
			return os.MkdirAll(targetPath, 0o755)
		}
		return fmt.Errorf("failed to inspect install target %q: %w", targetPath, err)
	}
	entries, err := os.ReadDir(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read install target %q: %w", targetPath, err)
	}
	reserved := linuxCompatReservedTopLevelNameSet()
	for _, entry := range entries {
		if _, ok := reserved[entry.Name()]; ok {
			continue
		}
		if err := removePathAllLogged(filepath.Join(targetPath, entry.Name()), logs); err != nil {
			return err
		}
	}
	return nil
}

func copyLinuxArchiveIntoCompat(ctx context.Context, sourcePath, targetPath string, logs *[]string) error {
	sourcePath = strings.TrimSpace(sourcePath)
	targetPath = strings.TrimSpace(targetPath)
	if sourcePath == "" || targetPath == "" {
		return fmt.Errorf("source and target paths are required for archive install")
	}
	args := linuxArchiveCopyArgs(sourcePath, targetPath)
	if _, err := runLoggedCommand(ctx, logs, "cp", args...); err != nil {
		return fmt.Errorf("failed to install extracted archive into %s: %w", targetPath, err)
	}
	return nil
}

func linuxArchiveCopyArgs(sourcePath, targetPath string) []string {
	return []string{
		"-a",
		filepath.ToSlash(strings.TrimSpace(sourcePath)) + "/.",
		filepath.ToSlash(strings.TrimSpace(targetPath)),
	}
}

func linuxCompatReservedTopLevelNames() []string {
	return []string{"dev", "proc", "sys", "tmp", "home"}
}

func linuxCompatReservedTopLevelNameSet() map[string]struct{} {
	names := linuxCompatReservedTopLevelNames()
	values := make(map[string]struct{}, len(names))
	for _, name := range names {
		values[name] = struct{}{}
	}
	return values
}

func linuxMountedDescendants(rootPath string) ([]string, error) {
	rootPath = filepath.Clean(strings.TrimSpace(rootPath))
	if rootPath == "" {
		return nil, fmt.Errorf("root path is required")
	}
	out, err := exec.Command("mount", "-p").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("mount -p failed: %w", err)
	}
	return mountedDescendantsFromLines(strings.Split(strings.TrimSpace(string(out)), "\n"), rootPath), nil
}

func mountedDescendantsFromLines(lines []string, rootPath string) []string {
	rootPath = filepath.Clean(strings.TrimSpace(rootPath))
	if rootPath == "" {
		return nil
	}
	prefix := rootPath + string(filepath.Separator)
	var mounts []string
	seen := map[string]struct{}{}
	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		mountpoint := filepath.Clean(fields[1])
		if mountpoint == rootPath || strings.HasPrefix(mountpoint, prefix) {
			if _, ok := seen[mountpoint]; ok {
				continue
			}
			seen[mountpoint] = struct{}{}
			mounts = append(mounts, mountpoint)
		}
	}
	sort.Strings(mounts)
	return mounts
}

func removePathAllLogged(path string, logs *[]string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to inspect path %q: %w", path, err)
	}
	*logs = append(*logs, "$ rm -rf "+path)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("failed to remove path %q: %w", path, err)
	}
	return nil
}

func checkLinuxRouteFamily(ctx context.Context, jailName, family string, logs *[]string) bool {
	args := []string{"jexec", jailName, "route", "-n", "get"}
	switch family {
	case "inet6":
		args = append(args, "-inet6")
	default:
		args = append(args, "-inet")
	}
	args = append(args, "default")
	_, err := runLoggedCommand(ctx, logs, args[0], args[1:]...)
	return err == nil
}

func checkLinuxFetchReachability(ctx context.Context, preflightURL, jailName string, hasIPv4Route, hasIPv6Route bool, logs *[]string) error {
	var failures []string
	if hasIPv4Route {
		if _, err := runLoggedCommand(ctx, logs, "jexec", jailName, "fetch", "-4", "-qo", "/dev/null", preflightURL); err == nil {
			return nil
		} else {
			failures = append(failures, "IPv4 fetch failed")
		}
	}
	if hasIPv6Route {
		if _, err := runLoggedCommand(ctx, logs, "jexec", jailName, "fetch", "-6", "-qo", "/dev/null", preflightURL); err == nil {
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

func checkLinuxGenericFetchReachability(ctx context.Context, preflightURL, jailName string, logs *[]string) error {
	if strings.TrimSpace(preflightURL) == "" {
		return fmt.Errorf("linux bootstrap preflight failed: no preflight URL available")
	}
	_, err := runLoggedCommand(ctx, logs, "jexec", jailName, "fetch", "-qo", "/dev/null", preflightURL)
	if err != nil {
		return fmt.Errorf("linux bootstrap preflight failed: generic fetch to %s failed: %w", preflightURL, err)
	}
	return nil
}

func checkLinuxDNSFamilies(ctx context.Context, jailName, host string, logs *[]string) (bool, bool, error) {
	out, err := runLoggedCommand(ctx, logs, "jexec", jailName, "getent", "hosts", host)
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
