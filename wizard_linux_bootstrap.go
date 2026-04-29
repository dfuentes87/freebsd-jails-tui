package main

import (
	"fmt"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
)

func effectiveLinuxDistro(values jailWizardValues) string {
	distro := strings.ToLower(strings.TrimSpace(values.LinuxDistro))
	if distro == "" {
		return "ubuntu"
	}
	return distro
}

func effectiveLinuxRelease(values jailWizardValues) string {
	release := strings.TrimSpace(values.LinuxRelease)
	if release != "" {
		return release
	}
	switch effectiveLinuxDistro(values) {
	case "debian":
		return "bookworm"
	default:
		return "jammy"
	}
}

func effectiveLinuxBootstrapPreset(values jailWizardValues) string {
	preset := strings.ToLower(strings.TrimSpace(values.LinuxPreset))
	if preset == "" {
		return "custom"
	}
	return preset
}

func effectiveLinuxBootstrapMethod(values jailWizardValues) string {
	method := strings.ToLower(strings.TrimSpace(values.LinuxBootstrapMethod))
	if method == "" {
		return "debootstrap"
	}
	return method
}

func effectiveLinuxBootstrapMode(values jailWizardValues) string {
	mode := strings.ToLower(strings.TrimSpace(values.LinuxBootstrap))
	if mode == "" {
		return "auto"
	}
	return mode
}

func effectiveLinuxMirrorMode(values jailWizardValues) string {
	mode := strings.ToLower(strings.TrimSpace(values.LinuxMirrorMode))
	if mode == "" {
		return "default"
	}
	return mode
}

type linuxMirrorInfo struct {
	BaseURL      string
	Host         string
	PreflightURL string
}

type linuxBootstrapSourceInfo struct {
	Method       string
	URL          string
	Host         string
	PreflightURL string
	LocalPath    string
	IsLocal      bool
}

func resolveLinuxMirror(values jailWizardValues) (linuxMirrorInfo, error) {
	mode := effectiveLinuxMirrorMode(values)
	baseURL := ""
	switch mode {
	case "default":
		switch effectiveLinuxDistro(values) {
		case "debian":
			baseURL = "https://deb.debian.org/debian"
		case "ubuntu":
			baseURL = "https://archive.ubuntu.com/ubuntu"
		default:
			return linuxMirrorInfo{}, fmt.Errorf("default mirror mode only supports bootstrap families ubuntu or debian; use custom mirror mode for %q", effectiveLinuxDistro(values))
		}
	case "custom":
		raw := strings.TrimSpace(values.LinuxMirrorURL)
		if raw == "" {
			return linuxMirrorInfo{}, fmt.Errorf("mirror URL is required when mirror mode is custom")
		}
		parsed, err := neturl.ParseRequestURI(raw)
		if err != nil {
			return linuxMirrorInfo{}, fmt.Errorf("mirror URL must be a valid http or https URL")
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return linuxMirrorInfo{}, fmt.Errorf("mirror URL must use http or https")
		}
		if strings.TrimSpace(parsed.Host) == "" {
			return linuxMirrorInfo{}, fmt.Errorf("mirror URL must include a host")
		}
		baseURL = strings.TrimRight(parsed.String(), "/")
	default:
		return linuxMirrorInfo{}, fmt.Errorf("mirror mode must be default or custom")
	}

	parsed, err := neturl.Parse(baseURL)
	if err != nil || strings.TrimSpace(parsed.Host) == "" {
		return linuxMirrorInfo{}, fmt.Errorf("failed to resolve effective Linux mirror URL")
	}
	info := linuxMirrorInfo{
		BaseURL:      strings.TrimRight(baseURL, "/"),
		Host:         parsed.Hostname(),
		PreflightURL: strings.TrimRight(baseURL, "/") + "/dists/" + effectiveLinuxRelease(values) + "/Release",
	}
	return info, nil
}

func resolveLinuxArchiveSource(raw string) (linuxBootstrapSourceInfo, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source is required")
	}
	if filepath.IsAbs(raw) {
		return resolveLinuxArchiveLocalPath(raw)
	}
	if strings.HasPrefix(raw, "file://") {
		parsed, err := neturl.Parse(raw)
		if err != nil {
			return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source must be a valid file URL")
		}
		if !filepath.IsAbs(parsed.Path) {
			return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source file URL must contain an absolute path")
		}
		return resolveLinuxArchiveLocalPath(parsed.Path)
	}
	parsed, err := neturl.ParseRequestURI(raw)
	if err != nil {
		return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source must be an absolute local path or a valid http or https URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source URL must use http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source URL must include a host")
	}
	path := strings.ToLower(strings.TrimSpace(parsed.Path))
	if err := validateLinuxArchivePath(path); err != nil {
		return linuxBootstrapSourceInfo{}, err
	}
	return linuxBootstrapSourceInfo{
		Method:       "archive",
		URL:          parsed.String(),
		Host:         parsed.Hostname(),
		PreflightURL: parsed.String(),
	}, nil
}

func resolveLinuxArchiveLocalPath(raw string) (linuxBootstrapSourceInfo, error) {
	raw = filepath.Clean(strings.TrimSpace(raw))
	if raw == "" {
		return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source is required")
	}
	info, err := os.Stat(raw)
	if err != nil {
		if os.IsNotExist(err) {
			return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source path %q does not exist", raw)
		}
		return linuxBootstrapSourceInfo{}, fmt.Errorf("failed to inspect archive source path %q: %v", raw, err)
	}
	if info.IsDir() {
		return linuxBootstrapSourceInfo{}, fmt.Errorf("archive source path %q must be a file, not a directory", raw)
	}
	if err := validateLinuxArchivePath(strings.ToLower(raw)); err != nil {
		return linuxBootstrapSourceInfo{}, err
	}
	return linuxBootstrapSourceInfo{
		Method:    "archive",
		URL:       raw,
		LocalPath: raw,
		IsLocal:   true,
	}, nil
}

func validateLinuxArchivePath(path string) error {
	switch {
	case strings.HasSuffix(path, ".tar"),
		strings.HasSuffix(path, ".tar.gz"),
		strings.HasSuffix(path, ".tgz"),
		strings.HasSuffix(path, ".tar.xz"):
	default:
		return fmt.Errorf("archive source must point to a supported tar archive (.tar, .tar.gz, .tgz, .tar.xz)")
	}
	return nil
}

func resolveLinuxBootstrapSource(values jailWizardValues) (linuxBootstrapSourceInfo, error) {
	switch effectiveLinuxBootstrapMethod(values) {
	case "archive":
		return resolveLinuxArchiveSource(values.LinuxArchiveURL)
	case "debootstrap":
		mirror, err := resolveLinuxMirror(values)
		if err != nil {
			return linuxBootstrapSourceInfo{}, err
		}
		return linuxBootstrapSourceInfo{
			Method:       "debootstrap",
			URL:          mirror.BaseURL,
			Host:         mirror.Host,
			PreflightURL: mirror.PreflightURL,
		}, nil
	default:
		return linuxBootstrapSourceInfo{}, fmt.Errorf("bootstrap method must be debootstrap or archive")
	}
}

func linuxMirrorMetadataValue(values jailWizardValues) string {
	if effectiveLinuxBootstrapMethod(values) != "debootstrap" {
		return "-"
	}
	if effectiveLinuxMirrorMode(values) != "custom" {
		return "-"
	}
	info, err := resolveLinuxMirror(values)
	if err != nil {
		return encodeTUIMetadataValue(strings.TrimSpace(values.LinuxMirrorURL))
	}
	return encodeTUIMetadataValue(info.BaseURL)
}

func linuxArchiveMetadataValue(values jailWizardValues) string {
	if effectiveLinuxBootstrapMethod(values) != "archive" {
		return "-"
	}
	info, err := resolveLinuxArchiveSource(values.LinuxArchiveURL)
	if err != nil {
		return encodeTUIMetadataValue(strings.TrimSpace(values.LinuxArchiveURL))
	}
	return encodeTUIMetadataValue(info.URL)
}

func effectiveLinuxMirrorSummary(values jailWizardValues) string {
	info, err := resolveLinuxMirror(values)
	if err != nil {
		if effectiveLinuxMirrorMode(values) == "custom" {
			return valueOrDash(strings.TrimSpace(values.LinuxMirrorURL))
		}
		return "-"
	}
	return info.BaseURL
}

func effectiveLinuxSourceSummary(values jailWizardValues) string {
	info, err := resolveLinuxBootstrapSource(values)
	if err != nil {
		switch effectiveLinuxBootstrapMethod(values) {
		case "archive":
			return valueOrDash(strings.TrimSpace(values.LinuxArchiveURL))
		case "debootstrap":
			return effectiveLinuxMirrorSummary(values)
		default:
			return "-"
		}
	}
	return info.URL
}

func linuxBootstrapUsesLocalSource(method, host, preflight string) bool {
	return strings.TrimSpace(method) == "archive" && strings.TrimSpace(host) == "" && strings.TrimSpace(preflight) == ""
}

func linuxArchiveDownloadName(values jailWizardValues) string {
	raw := strings.TrimSpace(values.LinuxArchiveURL)
	if raw == "" {
		return effectiveLinuxDistro(values) + "-rootfs.tar"
	}
	if filepath.IsAbs(raw) {
		if base := filepath.Base(raw); base != "" && base != "." && base != string(filepath.Separator) {
			return base
		}
	}
	parsed, err := neturl.Parse(raw)
	if err == nil {
		if base := pathBase(parsed.Path); base != "" && base != "." && base != "/" {
			return base
		}
	}
	return effectiveLinuxDistro(values) + "-rootfs.tar"
}

func linuxCompatRoot(jailPath string, values jailWizardValues) string {
	return filepath.Join(jailPath, "compat", effectiveLinuxDistro(values))
}

func pathBase(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "/")
	return parts[len(parts)-1]
}

func (w *jailCreationWizard) applyLinuxBootstrapPreset() {
	switch effectiveLinuxBootstrapPreset(w.values) {
	case "alpine":
		w.values.LinuxDistro = "alpine"
		w.values.LinuxBootstrapMethod = "archive"
	case "rocky":
		w.values.LinuxDistro = "rockylinux"
		w.values.LinuxBootstrapMethod = "archive"
	}
}
