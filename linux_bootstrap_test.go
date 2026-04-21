package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEffectiveLinuxBootstrapMethodDefaultsToDebootstrap(t *testing.T) {
	if got := effectiveLinuxBootstrapMethod(jailWizardValues{}); got != "debootstrap" {
		t.Fatalf("effectiveLinuxBootstrapMethod() = %q, want %q", got, "debootstrap")
	}
}

func TestResolveLinuxBootstrapSourceArchive(t *testing.T) {
	values := jailWizardValues{
		JailType:             "linux",
		LinuxDistro:          "alpine",
		LinuxBootstrapMethod: "archive",
		LinuxArchiveURL:      "https://dl-cdn.alpinelinux.org/alpine/v3.23/releases/x86_64/alpine-minirootfs-3.23.0-x86_64.tar.gz",
	}

	info, err := resolveLinuxBootstrapSource(values)
	if err != nil {
		t.Fatalf("resolveLinuxBootstrapSource() error = %v", err)
	}
	if info.Method != "archive" {
		t.Fatalf("info.Method = %q, want %q", info.Method, "archive")
	}
	if info.URL != values.LinuxArchiveURL {
		t.Fatalf("info.URL = %q, want %q", info.URL, values.LinuxArchiveURL)
	}
	if info.Host != "dl-cdn.alpinelinux.org" {
		t.Fatalf("info.Host = %q, want %q", info.Host, "dl-cdn.alpinelinux.org")
	}
	if info.PreflightURL != values.LinuxArchiveURL {
		t.Fatalf("info.PreflightURL = %q, want %q", info.PreflightURL, values.LinuxArchiveURL)
	}
}

func TestResolveLinuxArchiveSourceRejectsUnsupportedExtension(t *testing.T) {
	_, err := resolveLinuxArchiveSource("https://example.invalid/rootfs.zip")
	if err == nil {
		t.Fatal("resolveLinuxArchiveSource() error = nil, want non-nil")
	}
}

func TestResolveLinuxBootstrapSourceLocalArchive(t *testing.T) {
	tempDir := t.TempDir()
	archivePath := filepath.Join(tempDir, "rocky rootfs.tar.xz")
	if err := os.WriteFile(archivePath, []byte("test"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	values := jailWizardValues{
		JailType:             "linux",
		LinuxDistro:          "rockylinux",
		LinuxBootstrapMethod: "archive",
		LinuxArchiveURL:      archivePath,
	}

	info, err := resolveLinuxBootstrapSource(values)
	if err != nil {
		t.Fatalf("resolveLinuxBootstrapSource() error = %v", err)
	}
	if !info.IsLocal {
		t.Fatal("info.IsLocal = false, want true")
	}
	if info.LocalPath != archivePath {
		t.Fatalf("info.LocalPath = %q, want %q", info.LocalPath, archivePath)
	}
	if info.PreflightURL != "" {
		t.Fatalf("info.PreflightURL = %q, want empty for local archive", info.PreflightURL)
	}
}

func TestResolveLinuxBootstrapSourceDebootstrapDefaultMirror(t *testing.T) {
	values := jailWizardValues{
		JailType:        "linux",
		LinuxDistro:     "ubuntu",
		LinuxRelease:    "jammy",
		LinuxMirrorMode: "default",
	}

	info, err := resolveLinuxBootstrapSource(values)
	if err != nil {
		t.Fatalf("resolveLinuxBootstrapSource() error = %v", err)
	}
	if info.Method != "debootstrap" {
		t.Fatalf("info.Method = %q, want %q", info.Method, "debootstrap")
	}
	if info.URL != "https://archive.ubuntu.com/ubuntu" {
		t.Fatalf("info.URL = %q, want ubuntu default mirror", info.URL)
	}
	if info.PreflightURL != "https://archive.ubuntu.com/ubuntu/dists/jammy/Release" {
		t.Fatalf("info.PreflightURL = %q, want ubuntu Release URL", info.PreflightURL)
	}
}

func TestLinuxBootstrapConfigRoundTripArchiveMetadata(t *testing.T) {
	values := jailWizardValues{
		JailType:             "linux",
		Name:                 "alpine01",
		Hostname:             "alpine01.local",
		Interface:            "em0",
		LinuxDistro:          "alpine",
		LinuxBootstrapMethod: "archive",
		LinuxBootstrap:       "auto",
		LinuxArchiveURL:      "https://dl-cdn.alpinelinux.org/alpine/v3.23/releases/x86_64/alpine-minirootfs-3.23.0-x86_64.tar.gz",
	}

	lines := buildJailConfBlock(values, "/usr/local/jails/containers/alpine01", "")
	parsed := linuxBootstrapConfigFromRawLines(lines)

	if parsed.LinuxDistro != values.LinuxDistro {
		t.Fatalf("parsed.LinuxDistro = %q, want %q", parsed.LinuxDistro, values.LinuxDistro)
	}
	if effectiveLinuxBootstrapMethod(parsed) != "archive" {
		t.Fatalf("effectiveLinuxBootstrapMethod(parsed) = %q, want %q", effectiveLinuxBootstrapMethod(parsed), "archive")
	}
	if parsed.LinuxArchiveURL != values.LinuxArchiveURL {
		t.Fatalf("parsed.LinuxArchiveURL = %q, want %q", parsed.LinuxArchiveURL, values.LinuxArchiveURL)
	}
}

func TestLinuxBootstrapConfigRoundTripLocalArchiveMetadata(t *testing.T) {
	values := jailWizardValues{
		JailType:             "linux",
		Name:                 "rocky01",
		Hostname:             "rocky01.local",
		Interface:            "em0",
		LinuxPreset:          "rocky",
		LinuxDistro:          "rockylinux",
		LinuxBootstrapMethod: "archive",
		LinuxBootstrap:       "auto",
		LinuxArchiveURL:      "/usr/local/jails/media/rocky rootfs.tar.xz",
	}

	lines := buildJailConfBlock(values, "/usr/local/jails/containers/rocky01", "")
	parsed := linuxBootstrapConfigFromRawLines(lines)

	if parsed.LinuxPreset != values.LinuxPreset {
		t.Fatalf("parsed.LinuxPreset = %q, want %q", parsed.LinuxPreset, values.LinuxPreset)
	}
	if parsed.LinuxArchiveURL != values.LinuxArchiveURL {
		t.Fatalf("parsed.LinuxArchiveURL = %q, want %q", parsed.LinuxArchiveURL, values.LinuxArchiveURL)
	}
}

func TestLinuxBootstrapConfigBackwardCompatibleWithoutMethod(t *testing.T) {
	lines := []string{
		`testlinux {`,
		`  # freebsd-jails-tui: linux_distro=ubuntu linux_release=jammy linux_bootstrap=auto linux_mirror_mode=default linux_mirror_url=-;`,
		`  mount += "/compat/ubuntu";`,
		`}`,
	}

	parsed := linuxBootstrapConfigFromRawLines(lines)
	if effectiveLinuxBootstrapMethod(parsed) != "debootstrap" {
		t.Fatalf("effectiveLinuxBootstrapMethod(parsed) = %q, want %q", effectiveLinuxBootstrapMethod(parsed), "debootstrap")
	}
}

func TestLinuxBootstrapVisibleFieldsArchive(t *testing.T) {
	w := newJailCreationWizard("/usr/local/jails/containers")
	w.values.JailType = "linux"
	w.values.LinuxBootstrapMethod = "archive"
	w.step = 1

	ids := visibleFieldIDs(w.visibleFields())
	if !containsString(ids, "linux_preset") {
		t.Fatalf("visible fields %v do not include linux_preset", ids)
	}
	if !containsString(ids, "linux_archive_url") {
		t.Fatalf("visible fields %v do not include linux_archive_url", ids)
	}
	if containsString(ids, "linux_release") {
		t.Fatalf("visible fields %v unexpectedly include linux_release", ids)
	}
	if containsString(ids, "linux_mirror_mode") {
		t.Fatalf("visible fields %v unexpectedly include linux_mirror_mode", ids)
	}
}

func TestLinuxBootstrapVisibleFieldsDebootstrap(t *testing.T) {
	w := newJailCreationWizard("/usr/local/jails/containers")
	w.values.JailType = "linux"
	w.values.LinuxBootstrapMethod = "debootstrap"
	w.values.LinuxMirrorMode = "custom"
	w.step = 1

	ids := visibleFieldIDs(w.visibleFields())
	if !containsString(ids, "linux_release") {
		t.Fatalf("visible fields %v do not include linux_release", ids)
	}
	if !containsString(ids, "linux_mirror_mode") {
		t.Fatalf("visible fields %v do not include linux_mirror_mode", ids)
	}
	if !containsString(ids, "linux_mirror_url") {
		t.Fatalf("visible fields %v do not include linux_mirror_url", ids)
	}
	if containsString(ids, "linux_archive_url") {
		t.Fatalf("visible fields %v unexpectedly include linux_archive_url", ids)
	}
}

func TestValidateLinuxBootstrapReleaseSupportArchiveSkipsDebootstrapChecks(t *testing.T) {
	values := jailWizardValues{
		JailType:             "linux",
		LinuxDistro:          "rockylinux",
		LinuxBootstrapMethod: "archive",
		LinuxArchiveURL:      "https://images.linuxcontainers.org/images/rockylinux/9/amd64/default/20260421_03%3A17/rootfs.tar.xz",
	}

	if err := validateLinuxBootstrapReleaseSupport(values); err != nil {
		t.Fatalf("validateLinuxBootstrapReleaseSupport() error = %v, want nil", err)
	}
}

func TestSummarizeCreationWarningArchive(t *testing.T) {
	got := summarizeCreationWarning("failed to fetch archive bootstrap for alpine from https://example.invalid/rootfs.tar.gz: fetch failed")
	want := "linux bootstrap failed; use detail view action 'b' after fixing networking or archive access"
	if got != want {
		t.Fatalf("summarizeCreationWarning() = %q, want %q", got, want)
	}
}

func TestApplyLinuxBootstrapPresetAlpineLeavesArchiveSourceBlank(t *testing.T) {
	w := newJailCreationWizard("/usr/local/jails/containers")
	w.values.JailType = "linux"
	w.values.LinuxPreset = "alpine"

	w.applyLinuxBootstrapPreset()

	if w.values.LinuxDistro != "alpine" {
		t.Fatalf("w.values.LinuxDistro = %q, want %q", w.values.LinuxDistro, "alpine")
	}
	if effectiveLinuxBootstrapMethod(w.values) != "archive" {
		t.Fatalf("effectiveLinuxBootstrapMethod() = %q, want %q", effectiveLinuxBootstrapMethod(w.values), "archive")
	}
	if w.values.LinuxArchiveURL != "" {
		t.Fatalf("w.values.LinuxArchiveURL = %q, want empty archive source", w.values.LinuxArchiveURL)
	}
}

func TestApplyLinuxBootstrapPresetPreservesManualArchiveSource(t *testing.T) {
	w := newJailCreationWizard("/usr/local/jails/containers")
	w.values.JailType = "linux"
	w.values.LinuxPreset = "rocky"
	w.values.LinuxArchiveURL = "/srv/mirror/rocky-custom.tar.xz"

	w.applyLinuxBootstrapPreset()

	if w.values.LinuxDistro != "rockylinux" {
		t.Fatalf("w.values.LinuxDistro = %q, want %q", w.values.LinuxDistro, "rockylinux")
	}
	if w.values.LinuxArchiveURL != "/srv/mirror/rocky-custom.tar.xz" {
		t.Fatalf("w.values.LinuxArchiveURL = %q, want manual value preserved", w.values.LinuxArchiveURL)
	}
}

func TestArchiveSourceFieldGuideHasSingleAlpineURLExample(t *testing.T) {
	m := model{}
	guide := m.wizardFieldGuide(wizardField{ID: "linux_archive_url"})
	if len(guide.Examples) != 1 {
		t.Fatalf("len(guide.Examples) = %d, want 1", len(guide.Examples))
	}
	want := "https://dl-cdn.alpinelinux.org/alpine/v3.23/releases/x86_64/alpine-minirootfs-3.23.0-x86_64.tar.gz"
	if guide.Examples[0] != want {
		t.Fatalf("guide.Examples[0] = %q, want %q", guide.Examples[0], want)
	}
}

func TestDetectLinuxArchiveRootSingleTopLevelDirectory(t *testing.T) {
	stageDir := t.TempDir()
	rootDir := filepath.Join(stageDir, "rootfs")
	binDir := filepath.Join(rootDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("os.MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	root, _, err := detectLinuxArchiveRoot(stageDir)
	if err != nil {
		t.Fatalf("detectLinuxArchiveRoot() error = %v", err)
	}
	if root != rootDir {
		t.Fatalf("detectLinuxArchiveRoot() = %q, want %q", root, rootDir)
	}
}

func TestDetectLinuxArchiveRootRejectsUnsupportedLayout(t *testing.T) {
	stageDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stageDir, "README"), []byte("no rootfs here"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	_, _, err := detectLinuxArchiveRoot(stageDir)
	if err == nil {
		t.Fatal("detectLinuxArchiveRoot() error = nil, want non-nil")
	}
}

func TestMountedDescendantsFromLines(t *testing.T) {
	root := "/usr/local/jails/containers/rocky/compat/rockylinux"
	lines := []string{
		"devfs " + root + "/dev devfs rw 0 0",
		"tmpfs " + root + "/dev/shm tmpfs rw 0 0",
		"/tmp " + root + "/tmp nullfs rw 0 0",
		"/home " + root + "/home nullfs rw 0 0",
		"zroot/home /home zfs rw 0 0",
	}

	got := mountedDescendantsFromLines(lines, root)
	want := []string{
		root + "/dev",
		root + "/dev/shm",
		root + "/home",
		root + "/tmp",
	}
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %q, want %q (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestMountedDescendantsFromLinesIgnoresParentAndUnrelatedMounts(t *testing.T) {
	root := "/usr/local/jails/containers/rocky/compat/rockylinux"
	lines := []string{
		"zroot/jails /usr/local/jails zfs rw 0 0",
		"zroot/home /home zfs rw 0 0",
		"zroot/other /usr/local/jails/containers/other zfs rw 0 0",
	}

	got := mountedDescendantsFromLines(lines, root)
	if len(got) != 0 {
		t.Fatalf("mountedDescendantsFromLines() = %v, want empty", got)
	}
}

func visibleFieldIDs(fields []wizardField) []string {
	ids := make([]string, 0, len(fields))
	for _, field := range fields {
		ids = append(ids, field.ID)
	}
	return ids
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
