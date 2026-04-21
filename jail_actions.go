package main

import "context"

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

type jailServiceResult struct {
	Name   string
	Action string
	Logs   []string
	Err    error
}

type linuxBootstrapResult struct {
	Name     string
	Logs     []string
	Warnings []string
	Err      error
}

type zfsOpenResult struct {
	Detail JailDetail
	Err    error
}

func ExecuteJailServiceAction(target Jail, action string) jailServiceResult {
	result := jailServiceResult{
		Name:   strings.TrimSpace(target.Name),
		Action: strings.TrimSpace(action),
	}
	logs := make([]string, 0, 8)
	fail := func(err error) jailServiceResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	if result.Name == "" {
		return fail(fmt.Errorf("jail name is required"))
	}
	if result.Action != "start" && result.Action != "stop" && result.Action != "restart" {
		return fail(fmt.Errorf("unsupported jail action %q", result.Action))
	}

	command := "service jail " + result.Action + " " + result.Name
	logs = append(logs, "$ "+command)
	cmd := exec.Command("service", "jail", result.Action, result.Name)
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if text != "" {
		for _, line := range strings.Split(text, "\n") {
			logs = append(logs, "  "+line)
		}
	}
	if err != nil {
		return fail(fmt.Errorf("%s: %w", command, err))
	}

	result.Logs = logs
	return result
}

func resolveZFSPanelTarget(target Jail) zfsOpenResult {
	detail, err := CollectJailDetail(target.Name, target.JID, target.Path, time.Now())
	return zfsOpenResult{
		Detail: detail,
		Err:    err,
	}
}

func ExecuteLinuxBootstrapAction(detail JailDetail) linuxBootstrapResult {
	result := linuxBootstrapResult{
		Name: strings.TrimSpace(detail.Name),
	}
	logs := make([]string, 0, 16)
	fail := func(err error) linuxBootstrapResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	if result.Name == "" {
		return fail(fmt.Errorf("jail name is required"))
	}
	if !detailLooksLikeLinuxJail(detail) {
		return fail(fmt.Errorf("linux bootstrap retry is only available for linux jails"))
	}
	if detail.JID <= 0 {
		return fail(fmt.Errorf("linux bootstrap retry requires the jail to be running"))
	}

	values := linuxBootstrapConfigFromRawLines(detail.JailConfRaw)
	values.LinuxBootstrap = "auto"
	if err := preflightLinuxBootstrap(context.Background(), values, result.Name, &logs); err != nil {
		return fail(err)
	}
	if err := bootstrapLinuxUserland(context.Background(), values, result.Name, detail.Path, &logs); err != nil {
		return fail(err)
	}

	result.Logs = logs
	return result
}

// ExecuteBulkJailServiceAction runs the given action on each target sequentially.
// action must be "start", "stop", or "restart".
// Each target is processed independently; errors do not abort the remaining targets.
func ExecuteBulkJailServiceAction(targets []Jail, action string) []jailServiceResult {
	results := make([]jailServiceResult, 0, len(targets))
	for _, t := range targets {
		results = append(results, ExecuteJailServiceAction(t, action))
	}
	return results
}

func detailLooksLikeLinuxJail(detail JailDetail) bool {
	for _, flag := range detail.JailConfFlags {
		switch strings.TrimSpace(flag) {
		case "allow.mount.linprocfs", "allow.mount.linsysfs":
			return true
		}
	}
	for _, raw := range detail.JailConfRaw {
		if strings.Contains(raw, "/compat/") || strings.Contains(raw, "linux_distro=") {
			return true
		}
	}
	return false
}
