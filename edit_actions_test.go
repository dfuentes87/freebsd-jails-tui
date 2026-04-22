package main

import (
	"strings"
	"testing"
)

func TestUpdateJailConfigValueHostnameAndDependencies(t *testing.T) {
	original := strings.Join([]string{
		"web01 {",
		"  host.hostname = \"web01\";",
		"  path = \"/usr/local/jails/web01\";",
		"  # freebsd-jails-tui: note=frontend;",
		"  persist;",
		"}",
		"",
	}, "\n")

	updatedHostname, err := updateJailConfigValue(original, "web01", "host.hostname", quotedConfigValue("web01.internal"))
	if err != nil {
		t.Fatalf("updateJailConfigValue(hostname) returned error: %v", err)
	}
	if !strings.Contains(updatedHostname, "host.hostname = \"web01.internal\";") {
		t.Fatalf("updated hostname missing from config:\n%s", updatedHostname)
	}
	if !strings.Contains(updatedHostname, "# freebsd-jails-tui: note=frontend;") {
		t.Fatalf("hostname update should preserve unrelated metadata:\n%s", updatedHostname)
	}

	withDepend, err := updateJailConfigValue(updatedHostname, "web01", "depend", rawConfigValue("db01, cache01"))
	if err != nil {
		t.Fatalf("updateJailConfigValue(depend) returned error: %v", err)
	}
	if !strings.Contains(withDepend, "depend = db01, cache01;") {
		t.Fatalf("dependency line missing from config:\n%s", withDepend)
	}

	clearedDepend, err := updateJailConfigValue(withDepend, "web01", "depend", rawConfigValue(""))
	if err != nil {
		t.Fatalf("clearing depend returned error: %v", err)
	}
	if strings.Contains(clearedDepend, "depend =") {
		t.Fatalf("dependency line should be removed when cleared:\n%s", clearedDepend)
	}
}

func TestUpdateJailConfigLinuxMetadata(t *testing.T) {
	original := strings.Join([]string{
		"linux01 {",
		"  host.hostname = \"linux01\";",
		"  path = \"/usr/local/jails/linux01\";",
		"  # freebsd-jails-tui: note=build-node linux_preset=alpine linux_distro=alpine linux_bootstrap_method=archive linux_release=3.20 linux_bootstrap=auto linux_mirror_mode=default linux_mirror_url=- linux_archive_url=https%3A%2F%2Fexample.test%2Falpine.tar;",
		"  persist;",
		"}",
		"",
	}, "\n")

	values := jailWizardValues{
		LinuxPreset:          "custom",
		LinuxDistro:          "debian",
		LinuxBootstrapMethod: "debootstrap",
		LinuxRelease:         "bookworm",
		LinuxBootstrap:       "skip",
		LinuxMirrorMode:      "custom",
		LinuxMirrorURL:       "https://deb.example.test/debian",
	}
	updated, err := updateJailConfigLinuxMetadata(original, "linux01", values)
	if err != nil {
		t.Fatalf("updateJailConfigLinuxMetadata returned error: %v", err)
	}
	if !strings.Contains(updated, "linux_preset=custom") ||
		!strings.Contains(updated, "linux_distro=debian") ||
		!strings.Contains(updated, "linux_bootstrap_method=debootstrap") ||
		!strings.Contains(updated, "linux_release=bookworm") {
		t.Fatalf("updated linux metadata missing expected values:\n%s", updated)
	}
	if !strings.Contains(updated, "note=build-node") {
		t.Fatalf("linux metadata update should preserve unrelated note metadata:\n%s", updated)
	}
	if strings.Contains(updated, "alpine.tar") {
		t.Fatalf("old archive metadata should have been removed:\n%s", updated)
	}
}
