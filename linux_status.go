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
	EnableDrift      []string
	ServicePresent   bool
	ServiceRunning   bool
	ServiceStatusErr string
}

type LinuxWizardPrereqs struct {
	Host              LinuxHostStatus
	MirrorURL         string
	MirrorHost        string
	PreflightURL      string
	ResolveError      string
	ReleaseSupport    string
	ReleaseSupportMsg string
}

type LinuxReadiness struct {
	Host                 LinuxHostStatus
	BootstrapFamily      string
	BootstrapRelease     string
	CompatRoot           string
	BootstrapMode        string
	MirrorURL            string
	MirrorHost           string
	PreflightURL         string
	MirrorResolveError   string
	ReleaseSupport       string
	ReleaseSupportDetail string
	UserlandPresent      bool
	RuntimeChecked       bool
	IPv4Route            bool
	IPv6Route            bool
	IPv4DNS              bool
	IPv6DNS              bool
	IPv4Fetch            bool
	IPv6Fetch            bool
	RuntimeError         string
	HealthChecked        bool
	PackageManagerOK     bool
	PackageManagerStatus string
	DNSWorks             bool
	DNSStatus            string
	InitPresent          bool
	InitStatus           string
	ServiceStatus        string
}

func collectLinuxHostStatus() LinuxHostStatus {
	status := LinuxHostStatus{}
	rcStatus := collectRCSettingStatus("linux_enable", "YES")
	status.EnableValue = strings.TrimSpace(rcStatus.Effective)
	status.EnableConfigured = strings.EqualFold(status.EnableValue, "YES")
	status.EnableReadError = rcStatus.ReadError
	status.EnableDrift = append(status.EnableDrift, rcStatus.DriftReasons...)

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
	mirror, err := resolveLinuxMirror(values)
	support := collectLinuxBootstrapReleaseSupport(values)
	return LinuxWizardPrereqs{
		Host:              collectLinuxHostStatus(),
		MirrorURL:         mirror.BaseURL,
		MirrorHost:        mirror.Host,
		PreflightURL:      mirror.PreflightURL,
		ResolveError:      errorText(err),
		ReleaseSupport:    support.Status,
		ReleaseSupportMsg: support.Detail,
	}
}

func collectLinuxReadiness(detail JailDetail) *LinuxReadiness {
	if !detailLooksLikeLinuxJail(detail) {
		return nil
	}

	values := linuxBootstrapConfigFromRawLines(detail.JailConfRaw)
	readiness := &LinuxReadiness{
		Host:             collectLinuxHostStatus(),
		BootstrapFamily:  effectiveLinuxDistro(values),
		BootstrapRelease: effectiveLinuxRelease(values),
		BootstrapMode:    effectiveLinuxBootstrapMode(values),
	}
	support := collectLinuxBootstrapReleaseSupport(values)
	mirror, err := resolveLinuxMirror(values)
	readiness.MirrorURL = mirror.BaseURL
	readiness.MirrorHost = mirror.Host
	readiness.PreflightURL = mirror.PreflightURL
	readiness.MirrorResolveError = errorText(err)
	readiness.ReleaseSupport = support.Status
	readiness.ReleaseSupportDetail = support.Detail
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
		if readiness.PreflightURL != "" && linuxGenericFetchReachable(detail.Name, readiness.PreflightURL) {
			populateLinuxHealth(readiness, detail, values)
			return readiness
		}
		readiness.RuntimeError = "No IPv4 or IPv6 default route inside the jail."
		return readiness
	}
	if readiness.MirrorHost == "" {
		if readiness.MirrorResolveError != "" {
			readiness.RuntimeError = readiness.MirrorResolveError
		} else {
			readiness.RuntimeError = "Could not determine Linux bootstrap mirror host."
		}
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
	populateLinuxHealth(readiness, detail, values)
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

func linuxGenericFetchReachable(jailName, url string) bool {
	if strings.TrimSpace(url) == "" {
		return false
	}
	return exec.Command("jexec", jailName, "fetch", "-qo", "/dev/null", url).Run() == nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func populateLinuxHealth(readiness *LinuxReadiness, detail JailDetail, values jailWizardValues) {
	if readiness == nil || !readiness.UserlandPresent || strings.TrimSpace(detail.Name) == "" || detail.JID <= 0 {
		return
	}
	readiness.HealthChecked = true
	readiness.DNSWorks = readiness.IPv4DNS || readiness.IPv6DNS
	if readiness.DNSWorks {
		readiness.DNSStatus = "Resolver returned at least one usable A/AAAA answer."
	} else if readiness.RuntimeChecked {
		readiness.DNSStatus = "Resolver did not return any usable A/AAAA answers."
	}

	hostCompatRoot := linuxCompatRoot(detail.Path, values)
	readiness.InitPresent = fileExists(filepath.Join(hostCompatRoot, "sbin", "init")) || fileExists(filepath.Join(hostCompatRoot, "lib", "systemd", "systemd"))
	if readiness.InitPresent {
		readiness.InitStatus = "Linux init binary is present."
	} else {
		readiness.InitStatus = "Linux init binary was not found under the compat root."
	}

	distro := effectiveLinuxDistro(values)
	switch {
	case fileExists(filepath.Join(hostCompatRoot, "usr", "bin", "apt-get")):
		output, err := runLinuxChrootCommand(detail.Name, distro, "/usr/bin/apt-get", "--version")
		if err == nil {
			readiness.PackageManagerOK = true
			readiness.PackageManagerStatus = firstOutputLine(output, "apt-get is available.")
		} else {
			readiness.PackageManagerStatus = trimmedErrorOutput(output, err)
		}
	case fileExists(filepath.Join(hostCompatRoot, "usr", "bin", "dpkg-query")):
		output, err := runLinuxChrootCommand(detail.Name, distro, "/usr/bin/dpkg-query", "--version")
		if err == nil {
			readiness.PackageManagerOK = true
			readiness.PackageManagerStatus = firstOutputLine(output, "dpkg-query is available.")
		} else {
			readiness.PackageManagerStatus = trimmedErrorOutput(output, err)
		}
	default:
		readiness.PackageManagerStatus = "No Linux package manager binary was detected in the compat root."
	}

	switch {
	case fileExists(filepath.Join(hostCompatRoot, "bin", "systemctl")) || fileExists(filepath.Join(hostCompatRoot, "usr", "bin", "systemctl")):
		output, err := runLinuxChrootCommand(detail.Name, distro, "/bin/systemctl", "is-system-running")
		if err != nil {
			altOutput, altErr := runLinuxChrootCommand(detail.Name, distro, "/usr/bin/systemctl", "is-system-running")
			if altErr == nil {
				readiness.ServiceStatus = firstOutputLine(altOutput, "systemctl is available.")
			} else {
				readiness.ServiceStatus = trimmedErrorOutput(altOutput, altErr)
			}
		} else {
			readiness.ServiceStatus = firstOutputLine(output, "systemctl is available.")
		}
	case fileExists(filepath.Join(hostCompatRoot, "usr", "sbin", "service")) || fileExists(filepath.Join(hostCompatRoot, "sbin", "service")):
		output, err := runLinuxChrootCommand(detail.Name, distro, "/usr/sbin/service", "--status-all")
		if err != nil {
			altOutput, altErr := runLinuxChrootCommand(detail.Name, distro, "/sbin/service", "--status-all")
			if altErr == nil {
				readiness.ServiceStatus = "service --status-all succeeded."
			} else {
				readiness.ServiceStatus = trimmedErrorOutput(altOutput, altErr)
			}
		} else {
			readiness.ServiceStatus = firstOutputLine(output, "service --status-all succeeded.")
		}
	default:
		readiness.ServiceStatus = "No Linux service manager command was detected."
	}
}

func runLinuxChrootCommand(jailName, distro string, args ...string) (string, error) {
	target := filepath.ToSlash(filepath.Join("/compat", distro))
	baseArgs := []string{
		jailName,
		"chroot",
		target,
		"/usr/bin/env",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
	baseArgs = append(baseArgs, args...)
	out, err := exec.Command("jexec", baseArgs...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func firstOutputLine(output, fallback string) string {
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return fallback
}

func trimmedErrorOutput(output string, err error) string {
	text := strings.TrimSpace(output)
	if text != "" {
		return text
	}
	if err != nil {
		return err.Error()
	}
	return "unavailable"
}
