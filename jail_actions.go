package main

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
	if result.Action != "start" && result.Action != "stop" {
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
