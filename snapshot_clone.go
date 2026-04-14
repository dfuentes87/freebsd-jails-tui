package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type TemplateDatasetSnapshotClonePreview struct {
	Current       TemplateDatasetInfo
	Snapshot      string
	NewName       string
	NewDataset    string
	NewMountpoint string
	ReadonlyAfter bool
	Err           error
}

type TemplateDatasetSnapshotCloneResult struct {
	Dataset    string
	Mountpoint string
	Logs       []string
	Err        error
}

type TemplateDatasetSnapshotDestroyPreview struct {
	Current          TemplateDatasetInfo
	Snapshot         string
	ReferencedJails  []string
	ReferencedClones []string
	Err              error
}

type TemplateDatasetSnapshotDestroyResult struct {
	Dataset  string
	Snapshot string
	Logs     []string
	Err      error
}

type JailSnapshotClonePreview struct {
	Source       JailDetail
	Snapshot     string
	NewName      string
	Destination  string
	CloneDataset string
	WriteConfig  bool
	ConfigPath   string
	FstabPath    string
	Err          error
}

type JailSnapshotCloneResult struct {
	Name        string
	Dataset     string
	Destination string
	ConfigPath  string
	Logs        []string
	Err         error
}

func InspectTemplateSnapshotClone(dataset, snapshot, newName string, parentOverride *templateDatasetParent) TemplateDatasetSnapshotClonePreview {
	preview := TemplateDatasetSnapshotClonePreview{
		Snapshot: strings.TrimSpace(snapshot),
		NewName:  strings.TrimSpace(newName),
	}
	info, err := CollectTemplateDatasetDetail(dataset, parentOverride)
	if err != nil {
		preview.Err = err
		return preview
	}
	preview.Current = info
	if preview.Snapshot == "" || !strings.HasPrefix(preview.Snapshot, info.Name+"@") {
		preview.Err = fmt.Errorf("select a snapshot from the current template dataset")
		return preview
	}
	validatedName, err := validateTemplateRenameLeafName(preview.NewName)
	if err != nil {
		preview.Err = err
		return preview
	}
	preview.NewName = validatedName
	preview.ReadonlyAfter = true
	preview.NewDataset = info.ParentDataset + "/" + preview.NewName
	preview.NewMountpoint = filepath.Join(info.ParentMountpoint, preview.NewName)
	if preview.NewDataset, err = validateZFSDatasetName(preview.NewDataset, "template dataset"); err != nil {
		preview.Err = err
		return preview
	}
	if preview.NewMountpoint, err = validateAbsolutePath(preview.NewMountpoint, "template mountpoint"); err != nil {
		preview.Err = err
		return preview
	}
	if preview.NewMountpoint, err = validateUnusedMountpointPath(preview.NewMountpoint, "template mountpoint"); err != nil {
		preview.Err = err
		return preview
	}
	if zfsDatasetExists(preview.NewDataset) {
		preview.Err = fmt.Errorf("template dataset %q already exists", preview.NewDataset)
	}
	return preview
}

func ExecuteTemplateSnapshotClone(dataset, snapshot, newName string, parentOverride *templateDatasetParent) TemplateDatasetSnapshotCloneResult {
	result := TemplateDatasetSnapshotCloneResult{}
	logs := make([]string, 0, 24)
	fail := func(err error) TemplateDatasetSnapshotCloneResult {
		result.Logs = logs
		result.Err = err
		return result
	}
	preview := InspectTemplateSnapshotClone(dataset, snapshot, newName, parentOverride)
	if preview.Err != nil {
		return fail(preview.Err)
	}
	result.Dataset = preview.NewDataset
	result.Mountpoint = preview.NewMountpoint
	if _, err := runLoggedCommand(context.Background(), &logs, "zfs", "clone", preview.Snapshot, preview.NewDataset); err != nil {
		return fail(fmt.Errorf("failed to clone template snapshot %q: %w", preview.Snapshot, err))
	}
	if _, err := runLoggedCommand(context.Background(), &logs, "zfs", "set", "mountpoint="+preview.NewMountpoint, preview.NewDataset); err != nil {
		_, _ = runLoggedCommand(context.Background(), &logs, "zfs", "destroy", "-r", preview.NewDataset)
		return fail(fmt.Errorf("failed to set mountpoint for %q: %w", preview.NewDataset, err))
	}
	if err := finalizeTemplateDatasetReadonly(context.Background(), preview.NewDataset, &logs); err != nil {
		_, _ = runLoggedCommand(context.Background(), &logs, "zfs", "destroy", "-r", preview.NewDataset)
		return fail(err)
	}
	result.Logs = logs
	return result
}

func InspectTemplateSnapshotDestroy(dataset, snapshot string, parentOverride *templateDatasetParent) TemplateDatasetSnapshotDestroyPreview {
	preview := TemplateDatasetSnapshotDestroyPreview{
		Snapshot: strings.TrimSpace(snapshot),
	}
	info, err := CollectTemplateDatasetDetail(dataset, parentOverride)
	if err != nil {
		preview.Err = err
		return preview
	}
	preview.Current = info
	if preview.Snapshot == "" || !strings.HasPrefix(preview.Snapshot, info.Name+"@") {
		preview.Err = fmt.Errorf("select a snapshot from the current template dataset")
		return preview
	}

	if !zfsSnapshotExists(preview.Snapshot) {
		preview.Err = fmt.Errorf("snapshot %q does not exist", preview.Snapshot)
		return preview
	}

	currentJails := collectCurrentJailDatasetReferences()
	out, err := exec.Command("zfs", "list", "-H", "-o", "name,origin", "-t", "filesystem").Output()
	if err != nil {
		preview.Err = fmt.Errorf("failed to list zfs filesystems to check dependencies: %w", err)
		return preview
	}

	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) != 2 {
			continue
		}
		dependent := strings.TrimSpace(fields[0])
		if dependent == "" || strings.TrimSpace(fields[1]) != preview.Snapshot {
			continue
		}
		preview.ReferencedClones = append(preview.ReferencedClones, dependent)
		preview.ReferencedJails = append(preview.ReferencedJails, currentJails[dependent]...)
	}
	if err := scanner.Err(); err != nil {
		preview.Err = fmt.Errorf("failed to scan zfs dependency list: %w", err)
		return preview
	}

	preview.ReferencedClones = uniqueSortedStrings(preview.ReferencedClones)
	preview.ReferencedJails = uniqueSortedStrings(preview.ReferencedJails)
	if len(preview.ReferencedJails) > 0 {
		preview.Err = fmt.Errorf("snapshot %q is in use by %d current jail(s)", preview.Snapshot, len(preview.ReferencedJails))
		return preview
	}
	if len(preview.ReferencedClones) > 0 {
		preview.Err = fmt.Errorf("snapshot %q has %d clone dependent(s)", preview.Snapshot, len(preview.ReferencedClones))
	}

	return preview
}

func ExecuteTemplateSnapshotDestroy(dataset, snapshot string, parentOverride *templateDatasetParent) TemplateDatasetSnapshotDestroyResult {
	result := TemplateDatasetSnapshotDestroyResult{}
	logs := make([]string, 0, 8)
	fail := func(err error) TemplateDatasetSnapshotDestroyResult {
		result.Logs = logs
		result.Err = err
		return result
	}
	preview := InspectTemplateSnapshotDestroy(dataset, snapshot, parentOverride)
	if preview.Err != nil {
		return fail(preview.Err)
	}

	result.Dataset = preview.Current.Name
	result.Snapshot = preview.Snapshot

	if _, err := runLoggedCommand(context.Background(), &logs, "zfs", "destroy", preview.Snapshot); err != nil {
		return fail(fmt.Errorf("failed to destroy template snapshot %q: %w", preview.Snapshot, err))
	}

	result.Logs = logs
	return result
}

func collectCurrentJailDatasetReferences() map[string][]string {
	refs := map[string][]string{}
	runningByName := map[string]runningJail{}
	names := map[string]struct{}{}

	running, _ := discoverRunningJails()
	for _, jail := range running {
		name := strings.TrimSpace(jail.Name)
		if name == "" {
			continue
		}
		runningByName[name] = jail
		names[name] = struct{}{}
	}
	for _, name := range discoverConfiguredJails() {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		names[name] = struct{}{}
	}

	for name := range names {
		path := strings.TrimSpace(runningByName[name].Path)
		if path == "" {
			conf, err := discoverJailConf(name)
			if err == nil {
				path = strings.TrimSpace(conf.Values["path"])
			}
		}
		if path == "" {
			continue
		}
		info, err := discoverZFSDataset(path)
		if err != nil || info == nil || info.MatchType != "exact" {
			continue
		}
		dataset := strings.TrimSpace(info.Name)
		if dataset == "" {
			continue
		}
		refs[dataset] = append(refs[dataset], name)
	}

	for dataset, names := range refs {
		refs[dataset] = uniqueSortedStrings(names)
	}
	return refs
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.ToLower(result[i]) < strings.ToLower(result[j])
	})
	return result
}

func InspectJailSnapshotClone(detail JailDetail, snapshot, newName, destination string, writeConfig bool) JailSnapshotClonePreview {
	preview := JailSnapshotClonePreview{
		Source:      detail,
		Snapshot:    strings.TrimSpace(snapshot),
		NewName:     strings.TrimSpace(newName),
		Destination: strings.TrimSpace(destination),
		WriteConfig: writeConfig,
	}
	if detail.ZFS == nil || detail.ZFS.MatchType != "exact" || strings.TrimSpace(detail.ZFS.Name) == "" {
		preview.Err = fmt.Errorf("jail snapshot cloning requires an exact jail ZFS dataset")
		return preview
	}
	if preview.Snapshot == "" || !strings.HasPrefix(preview.Snapshot, detail.ZFS.Name+"@") {
		preview.Err = fmt.Errorf("select a snapshot from the current jail dataset")
		return preview
	}
	if preview.NewName == "" || !jailNamePattern.MatchString(preview.NewName) {
		preview.Err = fmt.Errorf("invalid jail name")
		return preview
	}
	cleanDestination, err := validateJailDestinationPath(preview.Destination, preview.NewName)
	if err != nil {
		preview.Err = err
		return preview
	}
	preview.Destination = cleanDestination
	if _, err := os.Stat(preview.Destination); err == nil {
		preview.Err = fmt.Errorf("destination %q already exists", preview.Destination)
		return preview
	}
	parentDataset := filepath.Dir(detail.ZFS.Name)
	if parentDataset == "." || parentDataset == "/" || parentDataset == "" {
		preview.Err = fmt.Errorf("could not derive a destination dataset parent from %q", detail.ZFS.Name)
		return preview
	}
	preview.CloneDataset = parentDataset + "/" + preview.NewName
	if preview.CloneDataset, err = validateZFSDatasetName(preview.CloneDataset, "clone dataset"); err != nil {
		preview.Err = err
		return preview
	}
	if zfsDatasetExists(preview.CloneDataset) {
		preview.Err = fmt.Errorf("clone dataset %q already exists", preview.CloneDataset)
		return preview
	}
	if writeConfig {
		if len(detail.JailConfRaw) == 0 {
			preview.Err = fmt.Errorf("source jail config could not be read")
			return preview
		}
		preview.ConfigPath = jailConfigPathForName(preview.NewName)
		if _, err := os.Stat(preview.ConfigPath); err == nil {
			preview.Err = fmt.Errorf("config file %q already exists", preview.ConfigPath)
			return preview
		}
		if fstabSource := strings.TrimSpace(detail.JailConfValues["mount.fstab"]); fstabSource != "" {
			if _, err := os.Stat(fstabSource); err != nil {
				preview.Err = fmt.Errorf("source fstab %q is not accessible", fstabSource)
				return preview
			}
			preview.FstabPath = jailFstabPathForName(preview.NewName)
			if _, err := os.Stat(preview.FstabPath); err == nil {
				preview.Err = fmt.Errorf("fstab file %q already exists", preview.FstabPath)
				return preview
			} else if !os.IsNotExist(err) {
				preview.Err = fmt.Errorf("failed to inspect fstab file %q: %w", preview.FstabPath, err)
				return preview
			}
		}
	}
	return preview
}

func ExecuteJailSnapshotClone(detail JailDetail, snapshot, newName, destination string, writeConfig bool) JailSnapshotCloneResult {
	result := JailSnapshotCloneResult{}
	logs := make([]string, 0, 32)
	cleanups := make([]func(), 0, 4)
	addCleanup := func(fn func()) {
		if fn != nil {
			cleanups = append(cleanups, fn)
		}
	}
	fail := func(err error) JailSnapshotCloneResult {
		for idx := len(cleanups) - 1; idx >= 0; idx-- {
			cleanups[idx]()
		}
		result.Logs = logs
		result.Err = err
		return result
	}

	preview := InspectJailSnapshotClone(detail, snapshot, newName, destination, writeConfig)
	if preview.Err != nil {
		return fail(preview.Err)
	}
	result.Name = preview.NewName
	result.Dataset = preview.CloneDataset
	result.Destination = preview.Destination
	result.ConfigPath = preview.ConfigPath

	if _, err := runLoggedCommand(context.Background(), &logs, "zfs", "clone", preview.Snapshot, preview.CloneDataset); err != nil {
		return fail(fmt.Errorf("failed to clone jail snapshot %q: %w", preview.Snapshot, err))
	}
	addCleanup(func() {
		_, _ = runLoggedCommand(context.Background(), &logs, "zfs", "destroy", "-r", preview.CloneDataset)
	})
	if _, err := runLoggedCommand(context.Background(), &logs, "zfs", "set", "mountpoint="+preview.Destination, preview.CloneDataset); err != nil {
		return fail(fmt.Errorf("failed to set mountpoint for %q: %w", preview.CloneDataset, err))
	}
	if !preview.WriteConfig {
		result.Logs = logs
		return result
	}
	var fstabPath string
	if preview.FstabPath != "" {
		fstabPath = preview.FstabPath
		if err := copyFile(detail.JailConfValues["mount.fstab"], fstabPath); err != nil {
			return fail(err)
		}
		addCleanup(func() {
			_ = removeFileIfExists(fstabPath, &logs)
		})
	}
	lines, err := clonedJailConfigLines(detail, preview.NewName, preview.Destination, fstabPath)
	if err != nil {
		return fail(err)
	}
	if err := writeJailConfigFile(preview.ConfigPath, lines, &logs); err != nil {
		return fail(err)
	}
	addCleanup(func() {
		_ = removeFileIfExists(preview.ConfigPath, &logs)
	})
	result.Logs = logs
	return result
}

func clonedJailConfigLines(detail JailDetail, newName, destination, newFstabPath string) ([]string, error) {
	if len(detail.JailConfRaw) == 0 {
		return nil, fmt.Errorf("source jail config could not be read")
	}
	lines := []string{fmt.Sprintf("%s {", newName)}
	hasFstabLine := false
	for _, raw := range detail.JailConfRaw {
		trimmed := strings.TrimSpace(stripInlineComment(raw))
		if trimmed == "" {
			continue
		}
		trimmed = strings.TrimSuffix(trimmed, ";")
		if key, _, ok := strings.Cut(trimmed, "="); ok {
			key = strings.TrimSpace(key)
			switch key {
			case "path":
				lines = append(lines, fmt.Sprintf("  path = %q;", destination))
				continue
			case "host.hostname":
				lines = append(lines, fmt.Sprintf("  host.hostname = %q;", newName))
				continue
			case "mount.fstab":
				hasFstabLine = true
				if newFstabPath != "" {
					lines = append(lines, fmt.Sprintf("  mount.fstab = %q;", newFstabPath))
				}
				continue
			}
		}
		lines = append(lines, raw)
	}
	if newFstabPath != "" && !hasFstabLine {
		lines = append(lines, fmt.Sprintf("  mount.fstab = %q;", newFstabPath))
	}
	lines = append(lines, "}")
	return lines, nil
}

func copyFile(src, dst string) error {
	src = strings.TrimSpace(src)
	dst = strings.TrimSpace(dst)
	if src == "" || dst == "" {
		return fmt.Errorf("source and destination files are required")
	}
	content, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("failed to read %q: %w", src, err)
	}
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to inspect %q: %w", src, err)
	}
	if err := writeFileAtomicExclusive(dst, content, info.Mode().Perm()); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("refusing to overwrite existing file %q", dst)
		}
		return fmt.Errorf("failed to write %q: %w", dst, err)
	}
	return nil
}
