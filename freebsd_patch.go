package main

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

type freeBSDPatchDecision struct {
	SourceKind     string
	ResolvedSource string
	ReleaseVersion string
	Eligible       bool
	Effective      bool
	Preference     string
	Note           string
	Err            error
}

func normalizeFreeBSDPatchPreference(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "auto":
		return "auto", nil
	case "yes", "no":
		return strings.ToLower(strings.TrimSpace(raw)), nil
	default:
		return "", fmt.Errorf("patch FreeBSD base must be auto, yes, or no")
	}
}

func resolveFreeBSDPatchDecision(sourceInput, preference string) freeBSDPatchDecision {
	decision := freeBSDPatchDecision{}
	normalizedPreference, err := normalizeFreeBSDPatchPreference(preference)
	if err != nil {
		decision.Err = err
		return decision
	}
	decision.Preference = normalizedPreference
	if strings.TrimSpace(sourceInput) == "" {
		return decision
	}

	sourceKind, resolvedSource, _, inspectErr := inspectTemplateSourceInput(strings.TrimSpace(sourceInput))
	if inspectErr == nil {
		decision.SourceKind = sourceKind
		decision.ResolvedSource = resolvedSource
	}

	if _, matched, ok := inferRequestedJailBaseVersion(strings.TrimSpace(sourceInput)); ok {
		decision.ReleaseVersion = matched
	} else if version, matched, ok := extractFreeBSDReleaseVersion(resolvedSource); ok {
		_ = version
		decision.ReleaseVersion = matched
	}

	decision.Eligible = isFreeBSDPatchEligible(strings.TrimSpace(sourceInput), decision.SourceKind, decision.ResolvedSource, decision.ReleaseVersion)
	if !decision.Eligible {
		decision.Note = "Only official FreeBSD release tags and recognizable base.txz release archives are patched automatically."
	}

	switch decision.Preference {
	case "yes":
		if !decision.Eligible {
			decision.Err = fmt.Errorf("patching is only available for official FreeBSD release tags and recognizable base.txz release archives")
			return decision
		}
		decision.Effective = true
	case "no":
		decision.Effective = false
	default:
		decision.Effective = decision.Eligible
	}
	return decision
}

func isFreeBSDPatchEligible(input, sourceKind, resolvedSource, releaseVersion string) bool {
	if strings.TrimSpace(releaseVersion) == "" {
		return false
	}
	if releaseValuePattern.MatchString(strings.ToUpper(strings.TrimSpace(input))) {
		return true
	}
	switch sourceKind {
	case "local directory", "named userland directory":
		return false
	}
	if !looksLikeFreeBSDBaseArchive(input) && !looksLikeFreeBSDBaseArchive(resolvedSource) {
		return false
	}
	if strings.EqualFold(filepath.Clean(strings.TrimSpace(resolvedSource)), "/usr/freebsd-dist/base.txz") {
		return true
	}
	if looksLikeOfficialFreeBSDReleaseURL(input) || looksLikeOfficialFreeBSDReleaseURL(resolvedSource) {
		return true
	}
	return true
}

func looksLikeFreeBSDBaseArchive(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	lower := strings.ToLower(raw)
	return strings.HasSuffix(lower, "/base.txz") || strings.HasSuffix(lower, string(filepath.Separator)+"base.txz") || strings.EqualFold(filepath.Base(raw), "base.txz")
}

func looksLikeOfficialFreeBSDReleaseURL(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	lower := strings.ToLower(raw)
	return strings.Contains(lower, "download.freebsd.org/ftp/releases/") && strings.Contains(lower, "/base.txz")
}

func patchFreeBSDRoot(ctx context.Context, rootPath string, logs *[]string) error {
	rootPath = strings.TrimSpace(rootPath)
	if rootPath == "" {
		return fmt.Errorf("patch root path is required")
	}
	if _, err := exec.LookPath("freebsd-update"); err != nil {
		return fmt.Errorf("freebsd-update is not available on this host")
	}
	if _, err := runLoggedCommand(ctx, logs, "freebsd-update", "-b", rootPath, "fetch", "install"); err != nil {
		return fmt.Errorf("failed to patch FreeBSD base under %q: %w", rootPath, err)
	}
	return nil
}
