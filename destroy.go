package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type JailDestroyResult struct {
	Name string
	Logs []string
	Err  error
}

type jailDestroyPlan struct {
	Name             string
	JailPath         string
	ConfigPath       string
	FstabPath        string
	ZFSDataset       string
	ZFSMatch         string
	ZFSPrefixDataset string
	ZFSPrefixMount   string
	Running          bool
}

func ExecuteJailDestroy(target Jail) JailDestroyResult {
	result := JailDestroyResult{Name: strings.TrimSpace(target.Name)}
	logs := make([]string, 0, 32)
	var persistentRctlCleanup func()
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	fail := func(err error) JailDestroyResult {
		if persistentRctlCleanup != nil {
			persistentRctlCleanup()
		}
		result.Logs = logs
		result.Err = err
		return result
	}

	if result.Name == "" {
		return fail(fmt.Errorf("jail name is required"))
	}

	logf("Starting jail destroy for %s", result.Name)
	plan := buildJailDestroyPlan(target, &logs)

	if plan.ConfigPath != "" && !isManagedJailConfigPath(plan.ConfigPath, result.Name) {
		return fail(fmt.Errorf("refusing to remove shared config file %q; remove the jail block manually first", plan.ConfigPath))
	}
	if plan.JailPath != "" {
		if err := validateDestroyPath(plan.JailPath); err != nil {
			return fail(err)
		}
		if err := validateFallbackDestroyPlan(plan); err != nil {
			return fail(err)
		}
	}
	if err := preflightDestroyPlan(plan); err != nil {
		return fail(err)
	}

	configBackup, err := backupFileForMutation(plan.ConfigPath, "destroy-"+result.Name, &logs)
	if err != nil {
		return fail(err)
	}
	if _, err := backupFileForMutation(plan.FstabPath, "destroy-"+result.Name, &logs); err != nil {
		return fail(err)
	}

	if plan.Running {
		if _, err := runLoggedCommand(&logs, "service", "jail", "stop", result.Name); err != nil {
			return fail(fmt.Errorf("failed to stop jail %q: %w", result.Name, err))
		}
	}

	removeJailRctlRules(result.Name, &logs)
	persistentRctlCleanup, err = removePersistentJailRctlRules(result.Name, &logs)
	if err != nil {
		if isManagedRctlBlockMalformedError(err) {
			logs = append(logs, "warning: unable to remove managed /etc/rctl.conf block automatically: "+err.Error())
			logs = append(logs, "warning: leaving /etc/rctl.conf unchanged; repair the managed block manually if needed")
		} else {
			return fail(err)
		}
	}

	if plan.ZFSDataset != "" {
		if _, err := runLoggedCommand(&logs, "zfs", "destroy", "-r", plan.ZFSDataset); err != nil {
			return fail(fmt.Errorf("failed to destroy dataset %q: %w", plan.ZFSDataset, err))
		}
	} else if plan.JailPath != "" {
		if err := clearDestroyPathFlags(plan.JailPath, &logs); err != nil {
			return fail(err)
		}
		logs = append(logs, "$ rm -rf "+plan.JailPath)
		if err := os.RemoveAll(plan.JailPath); err != nil {
			return fail(fmt.Errorf("failed to remove jail path %q: %w", plan.JailPath, err))
		}
	}

	configRemoved := false
	if err := removeFileIfExists(plan.ConfigPath, &logs); err != nil {
		return fail(err)
	}
	if configBackup != nil {
		configRemoved = true
	}
	if err := removeFileIfExists(plan.FstabPath, &logs); err != nil {
		if configRemoved {
			if restoreErr := restoreFileMutationBackup(configBackup, &logs); restoreErr != nil {
				logs = append(logs, "rollback warning: "+restoreErr.Error())
			}
		}
		return fail(err)
	}

	logf("Jail %s destroyed successfully.", result.Name)
	persistentRctlCleanup = nil
	result.Logs = logs
	return result
}

func buildJailDestroyPlan(target Jail, logs *[]string) jailDestroyPlan {
	name := strings.TrimSpace(target.Name)
	plan := jailDestroyPlan{
		Name:      name,
		JailPath:  strings.TrimSpace(target.Path),
		FstabPath: jailFstabPathForName(name),
		Running:   target.Running || target.JID > 0,
	}

	if fields, err := discoverRunningJailFields(name); err == nil {
		if jid, _ := strconv.Atoi(fields["jid"]); jid > 0 {
			plan.Running = true
		}
		if plan.JailPath == "" {
			plan.JailPath = strings.TrimSpace(fields["path"])
		}
	} else {
		*logs = append(*logs, "warning: unable to inspect running jail fields: "+err.Error())
	}

	if conf, err := discoverJailConf(name); err == nil {
		if plan.JailPath == "" {
			plan.JailPath = strings.TrimSpace(conf.Values["path"])
		}
		plan.ConfigPath = strings.TrimSpace(conf.SourcePath)
	} else {
		*logs = append(*logs, "warning: unable to inspect jail config: "+err.Error())
	}

	defaultConfigPath := jailConfigPathForName(name)
	if plan.ConfigPath == "" {
		if _, err := os.Stat(defaultConfigPath); err == nil {
			plan.ConfigPath = defaultConfigPath
		}
	}

	if plan.JailPath != "" {
		if zfsInfo, err := discoverZFSDataset(plan.JailPath); err == nil && zfsInfo != nil {
			plan.ZFSMatch = strings.TrimSpace(zfsInfo.MatchType)
			if strings.EqualFold(zfsInfo.MatchType, "exact") {
				plan.ZFSDataset = strings.TrimSpace(zfsInfo.Name)
			} else {
				plan.ZFSPrefixDataset = strings.TrimSpace(zfsInfo.Name)
				plan.ZFSPrefixMount = strings.TrimSpace(zfsInfo.Mountpoint)
				*logs = append(*logs, fmt.Sprintf(
					"warning: refusing recursive ZFS destroy because jail path %q only matched parent dataset %q (%s match)",
					plan.JailPath,
					strings.TrimSpace(zfsInfo.Name),
					strings.TrimSpace(zfsInfo.MatchType),
				))
			}
		} else if err != nil {
			*logs = append(*logs, "warning: unable to inspect ZFS dataset: "+err.Error())
		}
	}

	return plan
}

func isManagedJailConfigPath(path, name string) bool {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" {
		return false
	}
	base := name + ".conf"
	if filepath.Base(path) != base {
		return false
	}
	dir := filepath.Dir(path)
	return dir == "/etc/jail.conf.d" || dir == "/usr/local/etc/jail.conf.d"
}

func validateDestroyPath(path string) error {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" {
		return nil
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("refusing to destroy non-absolute jail path %q", path)
	}
	disallowed := map[string]struct{}{
		"/":                           {},
		"/jail":                       {},
		"/usr":                        {},
		"/usr/jail":                   {},
		"/usr/local":                  {},
		"/usr/local/jails":            {},
		"/usr/local/jails/containers": {},
		"/usr/local/jails/media":      {},
		"/usr/local/jails/templates":  {},
		"/etc":                        {},
		"/var":                        {},
	}
	if _, blocked := disallowed[path]; blocked {
		return fmt.Errorf("refusing to destroy shared root path %q", path)
	}
	return nil
}

func validateFallbackDestroyPlan(plan jailDestroyPlan) error {
	if plan.JailPath == "" || plan.ZFSDataset != "" || !strings.EqualFold(plan.ZFSMatch, "prefix") {
		return nil
	}

	jailPath := filepath.Clean(strings.TrimSpace(plan.JailPath))
	name := strings.TrimSpace(plan.Name)
	if name == "" {
		return fmt.Errorf("refusing fallback path destroy for %q: jail name is empty", jailPath)
	}
	if filepath.Base(jailPath) != name {
		return fmt.Errorf("refusing fallback path destroy for %q: basename does not match jail name %q", jailPath, name)
	}

	prefixMount := filepath.Clean(strings.TrimSpace(plan.ZFSPrefixMount))
	if prefixMount == "" || prefixMount == "." || prefixMount == "/" {
		return fmt.Errorf("refusing fallback path destroy for %q: matched dataset mountpoint is not specific enough", jailPath)
	}
	if !strings.HasPrefix(jailPath, prefixMount+"/") {
		return fmt.Errorf("refusing fallback path destroy for %q: path is not under matched dataset mountpoint %q", jailPath, prefixMount)
	}

	rel, err := filepath.Rel(prefixMount, jailPath)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("refusing fallback path destroy for %q: could not derive a safe relative path from %q", jailPath, prefixMount)
	}
	segments := strings.Split(filepath.ToSlash(rel), "/")
	if len(segments) == 0 {
		return fmt.Errorf("refusing fallback path destroy for %q: relative path is empty", jailPath)
	}
	switch strings.ToLower(segments[0]) {
	case "media", "templates":
		return fmt.Errorf("refusing fallback path destroy for %q: path is inside shared %q tree", jailPath, segments[0])
	}

	return nil
}

func preflightDestroyPlan(plan jailDestroyPlan) error {
	actionable := false
	if dataset := strings.TrimSpace(plan.ZFSDataset); dataset != "" {
		if !zfsDatasetExists(dataset) {
			return fmt.Errorf("matched ZFS dataset %q no longer exists; refresh before destroying", dataset)
		}
		actionable = true
	} else if path := strings.TrimSpace(plan.JailPath); path != "" {
		if _, err := os.Stat(path); err == nil {
			actionable = true
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to inspect jail path %q: %w", path, err)
		}
	}
	for _, candidate := range []string{plan.ConfigPath, plan.FstabPath} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			actionable = true
			continue
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to inspect %q before destroy: %w", candidate, err)
		}
	}
	if !actionable {
		return fmt.Errorf("nothing exists to destroy for jail %q", strings.TrimSpace(plan.Name))
	}
	return nil
}

func clearDestroyPathFlags(path string, logs *[]string) error {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to inspect jail path %q before clearing file flags: %w", path, err)
	}
	if _, err := runLoggedCommand(logs, "chflags", "-R", "0", path); err != nil {
		return fmt.Errorf("failed to clear file flags under %q: %w", path, err)
	}
	return nil
}

func removeJailRctlRules(name string, logs *[]string) {
	if _, err := runLoggedCommand(logs, "rctl", "-r", "jail:"+name); err != nil {
		*logs = append(*logs, "warning: unable to remove rctl rules: "+err.Error())
	}
}

func removeFileIfExists(path string, logs *[]string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to inspect %q: %w", path, err)
	}
	*logs = append(*logs, "$ rm -f "+path)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove %q: %w", path, err)
	}
	return nil
}

func buildDestroyPreview(target Jail) []string {
	plan := buildJailDestroyPlan(target, &[]string{})
	name := strings.TrimSpace(target.Name)
	lines := []string{
		"Destroying a jail will make irreversible changes:",
		"1. Stop the jail if it is currently running.",
		"2. Remove jail-specific rctl rules and any managed /etc/rctl.conf block for this jail.",
		"3. Delete the ZFS dataset only when it is dedicated to this jail and not shared with others.",
		"4. Otherwise remove the jail root path.",
		fmt.Sprintf("5. Remove /etc/jail.conf.d/%s.conf and /etc/fstab.%s if present.", name, name),
		"",
		fmt.Sprintf("Selected jail: %s", target.Name),
		fmt.Sprintf("Current JID: %s", valueOrDash(jailJIDText(target))),
		fmt.Sprintf("Current path: %s", valueOrDash(strings.TrimSpace(plan.JailPath))),
	}
	if plan.ZFSDataset != "" {
		lines = append(lines, fmt.Sprintf("Matched ZFS dataset: %s (exact)", plan.ZFSDataset))
	} else if plan.ZFSPrefixDataset != "" {
		lines = append(lines, fmt.Sprintf("Matched ZFS dataset: %s (prefix match only; dataset destroy will be skipped)", plan.ZFSPrefixDataset))
	} else {
		lines = append(lines, "Matched ZFS dataset: none")
	}
	if strings.TrimSpace(target.Hostname) != "" {
		lines = append(lines, fmt.Sprintf("Current hostname: %s", target.Hostname))
	}
	return lines
}

func newDestroyState(target Jail, returnMode screenMode) destroyState {
	target = buildDestroyTarget(target)
	return destroyState{
		returnMode: returnMode,
		target:     target,
		preview:    buildDestroyPreview(target),
		message:    "Press enter to destroy this jail, or esc to cancel.",
	}
}

func jailJIDText(target Jail) string {
	if target.JID <= 0 {
		return ""
	}
	return strconv.Itoa(target.JID)
}

func destroyResultMessage(result JailDestroyResult) string {
	if result.Err != nil {
		return "Destroy failed. Review execution output before retrying."
	}
	return fmt.Sprintf("Jail %s destroyed successfully.", result.Name)
}

func destroyTargetFromDetail(detail JailDetail) Jail {
	return Jail{
		Name:     detail.Name,
		JID:      detail.JID,
		Path:     detail.Path,
		Hostname: detail.Hostname,
		Running:  detail.JID > 0,
	}
}

func buildDestroyTarget(target Jail) Jail {
	target.Name = strings.TrimSpace(target.Name)
	target.Path = strings.TrimSpace(target.Path)
	target.Hostname = strings.TrimSpace(target.Hostname)
	if target.JID > 0 {
		target.Running = true
	}
	if target.Name == "" {
		return target
	}
	plan := buildJailDestroyPlan(target, &[]string{})
	if target.Path == "" {
		target.Path = strings.TrimSpace(plan.JailPath)
	}
	if target.Hostname == "" {
		if conf, err := discoverJailConf(target.Name); err == nil {
			target.Hostname = strings.TrimSpace(conf.Values["host.hostname"])
		}
	}
	return target
}

func timestampedDestroyNotice(name string) string {
	return fmt.Sprintf("Jail %s destroyed at %s.", name, time.Now().Format("15:04:05"))
}
