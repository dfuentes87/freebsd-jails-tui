package main

import (
	"strings"
	"testing"
)

func TestTemplateDatasetJailRefs(t *testing.T) {
	refs := templateDatasetJailRefs(
		"/usr/local/jails/templates/13.4-release",
		[]string{"zroot/jails/app01", "zroot/jails/app02"},
		map[string]string{
			"zroot/jails/app01": "/usr/local/jails/app01",
			"zroot/jails/app02": "/usr/local/jails/app02",
		},
		map[string][]string{
			"/usr/local/jails/templates/13.4-release": {"template-reader"},
			"/usr/local/jails/app01":                  {"app01"},
			"/usr/local/jails/app02":                  {"app02", "app02-alt"},
		},
	)

	got := strings.Join(refs, ",")
	want := "app01,app02,app02-alt,template-reader"
	if got != want {
		t.Fatalf("templateDatasetJailRefs() = %q, want %q", got, want)
	}
}

func TestDetailDriftItems(t *testing.T) {
	detail := JailDetail{
		Name:        "linux01",
		Path:        "/usr/local/jails/linux01-live",
		Hostname:    "linux01-live",
		JailConfRaw: []string{"linux01 {", "  # freebsd-jails-tui: linux_preset=rocky linux_distro=rocky linux_bootstrap_method=archive linux_bootstrap=auto;", "}"},
		JailConfValues: map[string]string{
			"path":          "/usr/local/jails/linux01",
			"host.hostname": "linux01.example",
			"interface":     "vtnet0",
			"ip4.addr":      "192.0.2.10",
		},
		RuntimeValues: map[string]string{
			"Live path": "/usr/local/jails/linux01-live",
			"Interface": "epair0b",
			"IPv4":      "192.0.2.11",
		},
		JID: 1,
		RctlConfig: &JailRctlConfig{
			Mode:         "persistent",
			CPUPercent:   "25",
			MemoryLimit:  "2G",
			ProcessLimit: "256",
		},
		LinuxReadiness: &LinuxReadiness{
			BootstrapPreset: "alpine",
			BootstrapMethod: "debootstrap",
			UserlandPresent: false,
		},
	}

	items := detailDriftItems(detail)
	joined := strings.Join(flattenDriftItems(items), "\n")
	for _, want := range []string{
		"Hostname",
		"Path",
		"Interface",
		"IPv4",
		"rctl rules",
		"Linux preset",
		"Linux method",
		"Linux userland",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("detailDriftItems() missing %q in:\n%s", want, joined)
		}
	}
}

func TestLinuxWizardBlockingLines(t *testing.T) {
	lines := linuxWizardBlockingLines(LinuxWizardPrereqs{
		Host: LinuxHostStatus{
			EnableConfigured: false,
		},
		Debootstrap: HostDebootstrapStatus{
			Installed:      false,
			ScriptsPresent: false,
		},
		Capabilities: LinuxHostCapabilityStatus{
			Errors: []string{"linux64 support is not available on the host"},
		},
		BootstrapMethod:   "debootstrap",
		ResolveError:      "mirror lookup failed",
		ReleaseSupport:    "unsupported",
		ReleaseSupportMsg: "release is not supported",
	})

	joined := strings.Join(lines, "\n")
	for _, want := range []string{
		"linux_enable is not configured",
		"debootstrap is not installed",
		"debootstrap scripts are missing",
		"linux64 support is not available on the host",
		"mirror lookup failed",
		"release is not supported",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("linuxWizardBlockingLines() missing %q in:\n%s", want, joined)
		}
	}
}

func flattenDriftItems(items [][2]string) []string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, item[0]+" "+item[1])
	}
	return lines
}
