package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var templateDatasetLeafPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

type TemplateDatasetInfo struct {
	Name               string
	Mountpoint         string
	ParentDataset      string
	ParentMountpoint   string
	Used               string
	Avail              string
	Refer              string
	Compression        string
	Quota              string
	Reservation        string
	Origin             string
	SnapshotCount      int
	ChildDatasets      []string
	CloneDependents    []string
	WizardTemplateRefs []string
	SafeLeaf           bool
	RenameAllowed      bool
	DestroyAllowed     bool
	SafetyIssues       []string
}

type TemplateDatasetRenamePreview struct {
	Current                TemplateDatasetInfo
	NewName                string
	NewDataset             string
	NewMountpoint          string
	UpdatedWizardTemplates []string
	Err                    error
}

type TemplateDatasetRenameResult struct {
	Dataset                string
	Mountpoint             string
	UpdatedWizardTemplates []string
	Logs                   []string
	Err                    error
}

type TemplateDatasetDestroyPreview struct {
	Current             TemplateDatasetInfo
	ReferencedTemplates []string
	DestroyScope        string
	Err                 error
}

type TemplateDatasetDestroyResult struct {
	Dataset string
	Logs    []string
	Err     error
}

type zfsFilesystemRow struct {
	Name        string
	Mountpoint  string
	Used        string
	Avail       string
	Refer       string
	Compression string
	Quota       string
	Reservation string
	Origin      string
}

func ListTemplateDatasets(parentOverride *templateDatasetParent) ([]TemplateDatasetInfo, *templateDatasetParent, error) {
	parent, err := resolveTemplateDatasetParent(parentOverride)
	if err != nil {
		return nil, nil, err
	}

	rows, err := listZFSFilesystems()
	if err != nil {
		return nil, parent, err
	}
	snapshots, err := listZFSSnapshotCounts()
	if err != nil {
		return nil, parent, err
	}
	refMap, err := collectWizardTemplateReferenceMap()
	if err != nil {
		return nil, parent, err
	}

	items := make([]TemplateDatasetInfo, 0)
	for _, row := range rows {
		if !isDirectChildDataset(parent.Name, row.Name) {
			continue
		}
		item := TemplateDatasetInfo{
			Name:             row.Name,
			Mountpoint:       row.Mountpoint,
			ParentDataset:    parent.Name,
			ParentMountpoint: parent.Mountpoint,
			Used:             row.Used,
			Avail:            row.Avail,
			Refer:            row.Refer,
			Compression:      row.Compression,
			Quota:            row.Quota,
			Reservation:      row.Reservation,
			Origin:           row.Origin,
			SnapshotCount:    snapshots[row.Name],
		}
		item.ChildDatasets = childDatasetsFor(row.Name, rows)
		item.CloneDependents = cloneDependentsFor(row.Name, rows)
		item.WizardTemplateRefs = append(item.WizardTemplateRefs, refMap[filepath.Clean(row.Mountpoint)]...)
		sort.Strings(item.ChildDatasets)
		sort.Strings(item.CloneDependents)
		sort.Slice(item.WizardTemplateRefs, func(i, j int) bool {
			return strings.ToLower(item.WizardTemplateRefs[i]) < strings.ToLower(item.WizardTemplateRefs[j])
		})
		applyTemplateDatasetSafety(&item)
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
	})
	return items, parent, nil
}

func CollectTemplateDatasetDetail(dataset string, parentOverride *templateDatasetParent) (TemplateDatasetInfo, error) {
	items, _, err := ListTemplateDatasets(parentOverride)
	if err != nil {
		return TemplateDatasetInfo{}, err
	}
	dataset = strings.TrimSpace(dataset)
	for _, item := range items {
		if item.Name == dataset {
			return item, nil
		}
	}
	return TemplateDatasetInfo{}, fmt.Errorf("template dataset %q was not found under the templates parent dataset", dataset)
}

func InspectTemplateDatasetRename(dataset, newName string, parentOverride *templateDatasetParent) TemplateDatasetRenamePreview {
	preview := TemplateDatasetRenamePreview{
		NewName: strings.TrimSpace(newName),
	}
	info, err := CollectTemplateDatasetDetail(dataset, parentOverride)
	if err != nil {
		preview.Err = err
		return preview
	}
	preview.Current = info
	preview.UpdatedWizardTemplates = append(preview.UpdatedWizardTemplates, info.WizardTemplateRefs...)
	if !info.RenameAllowed {
		preview.Err = fmt.Errorf("template dataset %q cannot be renamed: %s", info.Name, strings.Join(info.SafetyIssues, "; "))
		return preview
	}
	validatedName, err := validateTemplateRenameLeafName(preview.NewName)
	if err != nil {
		preview.Err = err
		return preview
	}
	preview.NewName = validatedName
	if preview.NewName == filepath.Base(info.Name) {
		preview.Err = fmt.Errorf("new template name must differ from the current name")
		return preview
	}

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
	if _, err := exec.Command("zfs", "list", "-H", "-o", "name", preview.NewDataset).Output(); err == nil {
		preview.Err = fmt.Errorf("template dataset %q already exists", preview.NewDataset)
	}
	return preview
}

func ExecuteTemplateDatasetRename(dataset, newName string, parentOverride *templateDatasetParent) TemplateDatasetRenameResult {
	result := TemplateDatasetRenameResult{}
	logs := make([]string, 0, 24)
	fail := func(err error) TemplateDatasetRenameResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	preview := InspectTemplateDatasetRename(dataset, newName, parentOverride)
	if preview.Err != nil {
		return fail(preview.Err)
	}

	result.Dataset = preview.NewDataset
	result.Mountpoint = preview.NewMountpoint
	result.UpdatedWizardTemplates = append(result.UpdatedWizardTemplates, preview.UpdatedWizardTemplates...)

	var templateBackup *fileMutationBackup
	var err error
	if len(preview.UpdatedWizardTemplates) > 0 {
		templateBackup, err = backupWizardTemplateStore(&logs)
		if err != nil {
			return fail(err)
		}
	}

	if _, err := runLoggedCommand(&logs, "zfs", "rename", preview.Current.Name, preview.NewDataset); err != nil {
		return fail(fmt.Errorf("failed to rename template dataset %q: %w", preview.Current.Name, err))
	}
	if _, err := runLoggedCommand(&logs, "zfs", "set", "mountpoint="+preview.NewMountpoint, preview.NewDataset); err != nil {
		_, _ = runLoggedCommand(&logs, "zfs", "rename", preview.NewDataset, preview.Current.Name)
		return fail(fmt.Errorf("failed to set mountpoint for %q: %w", preview.NewDataset, err))
	}
	if _, err := rewriteWizardTemplateReleaseReferences(preview.Current.Mountpoint, preview.NewMountpoint); err != nil {
		_, _ = runLoggedCommand(&logs, "zfs", "set", "mountpoint="+preview.Current.Mountpoint, preview.NewDataset)
		_, _ = runLoggedCommand(&logs, "zfs", "rename", preview.NewDataset, preview.Current.Name)
		if restoreErr := restoreWizardTemplateStoreBackup(templateBackup, &logs); restoreErr != nil {
			logs = append(logs, "rollback warning: "+restoreErr.Error())
		}
		return fail(fmt.Errorf("failed to update saved wizard templates after rename: %w", err))
	}

	result.Logs = logs
	return result
}

func InspectTemplateDatasetDestroy(dataset string, parentOverride *templateDatasetParent) TemplateDatasetDestroyPreview {
	preview := TemplateDatasetDestroyPreview{}
	info, err := CollectTemplateDatasetDetail(dataset, parentOverride)
	if err != nil {
		preview.Err = err
		return preview
	}
	preview.Current = info
	preview.ReferencedTemplates = append(preview.ReferencedTemplates, info.WizardTemplateRefs...)
	preview.DestroyScope = info.Name
	if !info.DestroyAllowed {
		preview.Err = fmt.Errorf("template dataset %q cannot be destroyed: %s", info.Name, strings.Join(info.SafetyIssues, "; "))
	}
	return preview
}

func ExecuteTemplateDatasetDestroy(dataset string, parentOverride *templateDatasetParent) TemplateDatasetDestroyResult {
	result := TemplateDatasetDestroyResult{}
	logs := make([]string, 0, 24)
	fail := func(err error) TemplateDatasetDestroyResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	preview := InspectTemplateDatasetDestroy(dataset, parentOverride)
	if preview.Err != nil {
		return fail(preview.Err)
	}
	result.Dataset = preview.Current.Name
	if _, err := runLoggedCommand(&logs, "zfs", "destroy", "-r", preview.Current.Name); err != nil {
		return fail(fmt.Errorf("failed to destroy template dataset %q: %w", preview.Current.Name, err))
	}
	result.Logs = logs
	return result
}

func listZFSFilesystems() ([]zfsFilesystemRow, error) {
	out, err := exec.Command(
		"zfs",
		"list",
		"-H",
		"-o",
		"name,mountpoint,used,avail,refer,compression,quota,reservation,origin",
		"-t",
		"filesystem",
	).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to list ZFS filesystems: %w", err)
	}

	rows := make([]zfsFilesystemRow, 0)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 9 {
			fields = strings.Fields(line)
		}
		if len(fields) < 9 {
			continue
		}
		row := zfsFilesystemRow{
			Name:        strings.TrimSpace(fields[0]),
			Mountpoint:  strings.TrimSpace(fields[1]),
			Used:        strings.TrimSpace(fields[2]),
			Avail:       strings.TrimSpace(fields[3]),
			Refer:       strings.TrimSpace(fields[4]),
			Compression: strings.TrimSpace(fields[5]),
			Quota:       strings.TrimSpace(fields[6]),
			Reservation: strings.TrimSpace(fields[7]),
			Origin:      strings.TrimSpace(fields[8]),
		}
		if row.Name == "" || row.Mountpoint == "" || row.Mountpoint == "-" || row.Mountpoint == "legacy" {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func listZFSSnapshotCounts() (map[string]int, error) {
	out, err := exec.Command("zfs", "list", "-H", "-o", "name", "-t", "snapshot").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to list ZFS snapshots: %w", err)
	}
	counts := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		dataset, _, ok := strings.Cut(line, "@")
		if !ok {
			continue
		}
		counts[dataset]++
	}
	return counts, nil
}

func collectWizardTemplateReferenceMap() (map[string][]string, error) {
	templates, err := loadWizardTemplates()
	if err != nil {
		return nil, err
	}
	refs := map[string][]string{}
	for _, tmpl := range templates {
		value := strings.TrimSpace(tmpl.Values.TemplateRelease)
		if value == "" || !strings.HasPrefix(value, "/") {
			continue
		}
		clean := filepath.Clean(value)
		refs[clean] = append(refs[clean], tmpl.Name)
	}
	return refs, nil
}

func isDirectChildDataset(parent, child string) bool {
	parent = strings.TrimSpace(parent)
	child = strings.TrimSpace(child)
	if parent == "" || child == "" || !strings.HasPrefix(child, parent+"/") {
		return false
	}
	rest := strings.TrimPrefix(child, parent+"/")
	return rest != "" && !strings.Contains(rest, "/")
}

func childDatasetsFor(dataset string, rows []zfsFilesystemRow) []string {
	children := make([]string, 0)
	for _, row := range rows {
		if strings.HasPrefix(row.Name, dataset+"/") {
			children = append(children, row.Name)
		}
	}
	return children
}

func cloneDependentsFor(dataset string, rows []zfsFilesystemRow) []string {
	dependents := make([]string, 0)
	prefix := dataset + "@"
	for _, row := range rows {
		if row.Name == dataset {
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(row.Origin), prefix) {
			dependents = append(dependents, row.Name)
		}
	}
	return dependents
}

func applyTemplateDatasetSafety(item *TemplateDatasetInfo) {
	item.SafeLeaf = len(item.ChildDatasets) == 0
	issues := make([]string, 0, 4)
	if !item.SafeLeaf {
		issues = append(issues, "has child datasets")
	}
	if item.SnapshotCount > 0 {
		issues = append(issues, fmt.Sprintf("has %d snapshots", item.SnapshotCount))
	}
	if len(item.CloneDependents) > 0 {
		issues = append(issues, fmt.Sprintf("has %d clone dependents", len(item.CloneDependents)))
	}
	item.RenameAllowed = len(issues) == 0
	if len(item.WizardTemplateRefs) > 0 {
		issues = append(issues, fmt.Sprintf("referenced by %d saved wizard templates", len(item.WizardTemplateRefs)))
	}
	item.DestroyAllowed = len(issues) == 0
	item.SafetyIssues = issues
}
