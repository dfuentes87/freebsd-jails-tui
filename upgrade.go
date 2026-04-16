package main

import "context"

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

type upgradeWorkflow int

const (
	upgradeWorkflowClassic      upgradeWorkflow = iota
	upgradeWorkflowThinTemplate upgradeWorkflow = iota
	upgradeWorkflowPkgReinstall upgradeWorkflow = iota
)

var upgradeWorkflowAll = []upgradeWorkflow{
	upgradeWorkflowClassic,
	upgradeWorkflowThinTemplate,
	upgradeWorkflowPkgReinstall,
}

type upgradeState struct {
	returnMode screenMode
	target     Jail
	workflow   upgradeWorkflow
	cursor     int
	selecting  bool
	preview    []string
	applying   bool
	logs       []string
	err        error
	message    string
}

type upgradeApplyMsg struct {
	name     string
	workflow upgradeWorkflow
	logs     []string
	err      error
}

func newUpgradeState(target Jail, returnMode screenMode) upgradeState {
	return upgradeState{
		returnMode: returnMode,
		target:     target,
		selecting:  true,
		cursor:     0,
	}
}

func upgradeWorkflowLabel(w upgradeWorkflow) string {
	switch w {
	case upgradeWorkflowClassic:
		return "Classic jail base upgrade (freebsd-update)"
	case upgradeWorkflowThinTemplate:
		return "Thin jail template upgrade (patch template dataset)"
	case upgradeWorkflowPkgReinstall:
		return "Post-major-version package reinstall (pkg upgrade -f)"
	default:
		return "Unknown workflow"
	}
}

func upgradeWorkflowDescriptionLines(w upgradeWorkflow, jail Jail) []string {
	switch w {
	case upgradeWorkflowClassic:
		return []string{
			"Applies to: classic (thick) jails with a dedicated root filesystem.",
			"Steps:",
			"  1. Run: env PAGER=cat freebsd-update --not-running-from-cron -b <jailPath> fetch install",
			"     This patches the jail base to the latest security level.",
			"  2. The jail does not need to be running.",
			"  3. Start the jail and run pkg upgrade manually if packages need updating.",
			"",
			"Jail path: " + valueOrDash(jail.Path),
		}
	case upgradeWorkflowThinTemplate:
		return []string{
			"Applies to: thin jails whose root is a ZFS clone of a template dataset.",
			"Steps:",
			"  1. Detect the template dataset from the jail's ZFS clone origin.",
			"  2. Make the template dataset temporarily writable.",
			"  3. Run: env PAGER=cat freebsd-update --not-running-from-cron -b <templateMountpoint> fetch install",
			"  4. Create a post-upgrade snapshot of the template dataset.",
			"  5. Restore the template dataset to readonly.",
			"",
			"Note: existing thin jail clones are not re-cloned automatically.",
			"      Re-clone them from the new snapshot to pick up base changes.",
			"",
			"Jail path: " + valueOrDash(jail.Path),
		}
	case upgradeWorkflowPkgReinstall:
		return []string{
			"Applies to: jails that need packages rebuilt after a major FreeBSD version bump.",
			"Steps:",
			"  1. Start the jail if it is not already running.",
			"  2. Run: pkg -j <jailName> upgrade -f",
			"     The -f flag forces reinstallation even for unchanged package versions.",
			"  3. Stop the jail if it was started in step 1.",
			"",
			"This does not patch the FreeBSD base. Use the classic or thin-template",
			"workflow first, then run this workflow to reconcile installed packages.",
			"",
			"Jail: " + valueOrDash(jail.Name),
		}
	default:
		return []string{"No description available."}
	}
}

func ExecuteJailUpgrade(target Jail, workflow upgradeWorkflow) upgradeApplyMsg {
	name := strings.TrimSpace(target.Name)
	logs := make([]string, 0, 32)
	result := upgradeApplyMsg{
		name:     name,
		workflow: workflow,
	}
	fail := func(err error) upgradeApplyMsg {
		result.logs = logs
		result.err = err
		return result
	}

	switch workflow {
	case upgradeWorkflowClassic:
		if err := executeClassicUpgrade(target, &logs); err != nil {
			return fail(err)
		}
	case upgradeWorkflowThinTemplate:
		if err := executeThinTemplateUpgrade(target, &logs); err != nil {
			return fail(err)
		}
	case upgradeWorkflowPkgReinstall:
		if err := executePkgReinstall(target, &logs); err != nil {
			return fail(err)
		}
	default:
		return fail(fmt.Errorf("unknown upgrade workflow %d", workflow))
	}

	result.logs = logs
	return result
}

func executeClassicUpgrade(jail Jail, logs *[]string) error {
	path := strings.TrimSpace(jail.Path)
	if path == "" {
		return fmt.Errorf("jail path is required for classic upgrade")
	}
	return patchFreeBSDRoot(context.Background(), path, logs)
}

func executeThinTemplateUpgrade(jail Jail, logs *[]string) error {
	path := strings.TrimSpace(jail.Path)
	if path == "" {
		return fmt.Errorf("jail path is required to detect thin template dataset")
	}
	dataset, mountpoint, err := detectThinTemplateDataset(path)
	if err != nil {
		return err
	}
	if _, err := runLoggedCommand(context.Background(), logs, "zfs", "set", "readonly=off", dataset); err != nil {
		return fmt.Errorf("failed to make template dataset %q writable: %w", dataset, err)
	}
	patchErr := patchFreeBSDRoot(context.Background(), mountpoint, logs)
	if _, setErr := runLoggedCommand(context.Background(), logs, "zfs", "set", "readonly=on", dataset); setErr != nil {
		*logs = append(*logs, "warning: failed to restore readonly on template dataset: "+setErr.Error())
	}
	if patchErr != nil {
		return patchErr
	}
	snap := dataset + "@post-upgrade-" + time.Now().Format("20060102T150405")
	if _, err := runLoggedCommand(context.Background(), logs, "zfs", "snapshot", snap); err != nil {
		*logs = append(*logs, "warning: post-upgrade snapshot failed: "+err.Error())
	}
	return nil
}

func executePkgReinstall(jail Jail, logs *[]string) error {
	name := strings.TrimSpace(jail.Name)
	if name == "" {
		return fmt.Errorf("jail name is required for pkg reinstall")
	}
	started := false
	if !jail.Running {
		if _, err := runLoggedCommand(context.Background(), logs, "service", "jail", "start", name); err != nil {
			return fmt.Errorf("failed to start jail %q: %w", name, err)
		}
		started = true
	}
	_, pkgErr := runLoggedCommand(context.Background(), logs, "pkg", "-j", name, "upgrade", "-f", "-y")
	if started {
		if _, err := runLoggedCommand(context.Background(), logs, "service", "jail", "stop", name); err != nil {
			*logs = append(*logs, "warning: failed to stop jail after pkg reinstall: "+err.Error())
		}
	}
	if pkgErr != nil {
		return fmt.Errorf("pkg upgrade -f failed: %w", pkgErr)
	}
	return nil
}

func detectThinTemplateDataset(jailPath string) (dataset, mountpoint string, err error) {
	jailPath = strings.TrimSpace(jailPath)
	if jailPath == "" {
		return "", "", fmt.Errorf("jail path is required")
	}
	info, dsErr := exactZFSDatasetForPath(jailPath)
	if dsErr != nil || info == nil || strings.TrimSpace(info.Name) == "" {
		return "", "", fmt.Errorf("no exact ZFS dataset found for jail path %q: %w", jailPath, dsErr)
	}
	jailDataset := info.Name
	out, err := exec.Command("zfs", "get", "-H", "-o", "value", "origin", jailDataset).Output()
	if err != nil {
		return "", "", fmt.Errorf("failed to get ZFS origin of %q: %w", jailDataset, err)
	}
	origin := strings.TrimSpace(string(out))
	if origin == "" || origin == "-" {
		return "", "", fmt.Errorf("jail dataset %q has no clone origin; not a ZFS-backed thin jail", jailDataset)
	}
	templateDataset, _, ok := strings.Cut(origin, "@")
	if !ok || strings.TrimSpace(templateDataset) == "" {
		return "", "", fmt.Errorf("could not parse template dataset from origin %q", origin)
	}
	out2, err := exec.Command("zfs", "get", "-H", "-o", "value", "mountpoint", templateDataset).Output()
	if err != nil {
		return "", "", fmt.Errorf("failed to get mountpoint of template dataset %q: %w", templateDataset, err)
	}
	mnt := strings.TrimSpace(string(out2))
	if mnt == "" || mnt == "-" || mnt == "legacy" {
		return "", "", fmt.Errorf("template dataset %q has no usable mountpoint", templateDataset)
	}
	return templateDataset, mnt, nil
}
