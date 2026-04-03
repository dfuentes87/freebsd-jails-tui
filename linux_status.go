package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type LinuxHostStatus struct {
	EnableValue      string
	EnableConfigured bool
	EnableReadError  string
	ServicePresent   bool
	ServiceRunning   bool
	ServiceStatusErr string
}

type LinuxWizardPrereqs struct {
	Host         LinuxHostStatus
	MirrorHost   string
	PreflightURL string
}

type LinuxReadiness struct {
	Host            LinuxHostStatus
	CompatRoot      string
	BootstrapMode   string
	MirrorHost      string
	PreflightURL    string
	UserlandPresent bool
	RuntimeChecked  bool
	IPv4Route       bool
	IPv6Route       bool
	IPv4DNS         bool
	IPv6DNS         bool
	IPv4Fetch       bool
	IPv6Fetch       bool
	RuntimeError    string
}

func collectLinuxHostStatus() LinuxHostStatus {
	status := LinuxHostStatus{}
	value, err := readRCConfValue("linux_enable")
	if err != nil {
		status.EnableReadError = err.Error()
	} else {
		status.EnableValue = strings.TrimSpace(value)
		status.EnableConfigured = strings.EqualFold(status.EnableValue, "YES")
	}

	if _, err := os.Stat("/etc/rc.d/linux"); err == nil {
		status.ServicePresent = true
		runErr := exec.Command("service", "linux", "onestatus").Run()
		if runErr == nil {
			status.ServiceRunning = true
		} else {
			status.ServiceStatusErr = runErr.Error()
		}
	}
	return status
}

func collectLinuxWizardPrereqs(values jailWizardValues) LinuxWizardPrereqs {
	return LinuxWizardPrereqs{
		Host:         collectLinuxHostStatus(),
		MirrorHost:   linuxMirrorHost(values),
		PreflightURL: linuxPreflightURL(values),
	}
}

func collectLinuxReadiness(detail JailDetail) *LinuxReadiness {
	if !detailLooksLikeLinuxJail(detail) {
		return nil
	}

	values := linuxBootstrapConfigFromRawLines(detail.JailConfRaw)
	readiness := &LinuxReadiness{
		Host:          collectLinuxHostStatus(),
		BootstrapMode: effectiveLinuxBootstrapMode(values),
		MirrorHost:    linuxMirrorHost(values),
		PreflightURL:  linuxPreflightURL(values),
	}
	if strings.TrimSpace(detail.Path) != "" {
		readiness.CompatRoot = linuxCompatRoot(detail.Path, values)
		readiness.UserlandPresent = linuxUserlandPresent(detail.Path, values)
	}

	if strings.TrimSpace(detail.Name) == "" || detail.JID <= 0 {
		return readiness
	}

	readiness.RuntimeChecked = true
	readiness.IPv4Route = linuxRouteFamilyAvailable(detail.Name, "inet")
	readiness.IPv6Route = linuxRouteFamilyAvailable(detail.Name, "inet6")
	if !readiness.IPv4Route && !readiness.IPv6Route {
		readiness.RuntimeError = "No IPv4 or IPv6 default route inside the jail."
		return readiness
	}
	if readiness.MirrorHost == "" {
		readiness.RuntimeError = "Could not determine Linux bootstrap mirror host."
		return readiness
	}

	ipv4DNS, ipv6DNS, err := linuxDNSFamiliesAvailable(detail.Name, readiness.MirrorHost)
	if err != nil {
		readiness.RuntimeError = err.Error()
		return readiness
	}
	readiness.IPv4DNS = ipv4DNS
	readiness.IPv6DNS = ipv6DNS

	if readiness.IPv4Route && readiness.IPv4DNS {
		readiness.IPv4Fetch = linuxFetchReachable(detail.Name, readiness.PreflightURL, "-4")
	}
	if readiness.IPv6Route && readiness.IPv6DNS {
		readiness.IPv6Fetch = linuxFetchReachable(detail.Name, readiness.PreflightURL, "-6")
	}
	if !readiness.IPv4Fetch && !readiness.IPv6Fetch && readiness.PreflightURL != "" {
		readiness.RuntimeError = fmt.Sprintf("Could not fetch %s with a usable route/DNS family.", readiness.PreflightURL)
	}
	return readiness
}

func linuxUserlandPresent(jailPath string, values jailWizardValues) bool {
	jailPath = strings.TrimSpace(jailPath)
	if jailPath == "" {
		return false
	}
	compatRoot := linuxCompatRoot(jailPath, values)
	_, err := os.Stat(filepath.Join(compatRoot, "bin", "sh"))
	return err == nil
}

func linuxRouteFamilyAvailable(jailName, family string) bool {
	args := []string{"jexec", jailName, "route", "-n", "get"}
	switch family {
	case "inet6":
		args = append(args, "-inet6")
	default:
		args = append(args, "-inet")
	}
	args = append(args, "default")
	return exec.Command(args[0], args[1:]...).Run() == nil
}

func linuxDNSFamiliesAvailable(jailName, host string) (bool, bool, error) {
	out, err := exec.Command("jexec", jailName, "getent", "hosts", host).CombinedOutput()
	if err != nil {
		return false, false, fmt.Errorf("DNS lookup failed for %s", host)
	}
	var hasIPv4, hasIPv6 bool
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 0 {
			continue
		}
		if strings.Count(fields[0], ".") == 3 {
			hasIPv4 = true
			continue
		}
		if strings.Contains(fields[0], ":") {
			hasIPv6 = true
		}
	}
	return hasIPv4, hasIPv6, nil
}

func linuxFetchReachable(jailName, url, familyFlag string) bool {
	if strings.TrimSpace(url) == "" {
		return false
	}
	return exec.Command("jexec", jailName, "fetch", familyFlag, "-qo", "/dev/null", url).Run() == nil
}
