package main

import (
	"fmt"
	"net/netip"
	neturl "net/url"
	"os"
	"strings"
)

func (w jailCreationWizard) validateCurrentStepDetailed() (string, error) {
	if w.isConfirmationStep() {
		return "", nil
	}
	jailType := strings.ToLower(strings.TrimSpace(w.values.JailType))
	if jailType == "" {
		return "jail_type", fmt.Errorf("jail type is required (thick, thin, vnet, linux)")
	}
	switch jailType {
	case "thick", "thin", "vnet", "linux":
	default:
		return "jail_type", fmt.Errorf("jail type must be one of: thick, thin, vnet, linux")
	}
	w.values.JailType = jailType
	if w.currentStepHasField("name") && strings.TrimSpace(w.values.Name) == "" {
		return "name", fmt.Errorf("jail name is required")
	}
	if w.currentStepHasField("name") && !jailNamePattern.MatchString(strings.TrimSpace(w.values.Name)) {
		return "name", fmt.Errorf("invalid jail name")
	}
	if w.currentStepHasField("note") {
		note, err := normalizeJailNote(w.values.Note)
		if err != nil {
			return "note", err
		}
		w.values.Note = note
	}
	if w.currentStepHasField("dataset") && strings.TrimSpace(w.values.Dataset) == "" {
		return "dataset", fmt.Errorf("destination is required: enter full path like /usr/local/jails/containers/%s", strings.TrimSpace(w.values.Name))
	}
	if w.currentStepHasField("dataset") {
		if _, err := validateJailDestinationPath(w.values.Dataset, w.values.Name); err != nil {
			if strings.Contains(err.Error(), "is required") {
				return "dataset", fmt.Errorf("destination is required: enter full path like /usr/local/jails/containers/%s", strings.TrimSpace(w.values.Name))
			}
			return "dataset", err
		}
		if checkJailRootExistsAndNotEmpty(w.values) {
			return "dataset", fmt.Errorf("destination directory already exists and is not empty; please manually investigate or remove it")
		}
	}
	if w.currentStepHasField("template_release") && strings.TrimSpace(w.values.TemplateRelease) == "" {
		return "template_release", fmt.Errorf("template/release is required (local path, release tag, or https URL)")
	}
	if w.currentStepHasField("template_release") {
		if err := validateTemplateReleaseInput(w.values); err != nil {
			return "template_release", err
		}
		if compatibility := collectJailBaseCompatibility(w.values); compatibility.Err != nil {
			return "template_release", compatibility.Err
		}
	}
	if w.currentStepHasField("patch_base") {
		decision := resolveFreeBSDPatchDecision(w.values.TemplateRelease, w.values.PatchBase)
		if decision.Err != nil {
			return "patch_base", decision.Err
		}
	}
	if w.currentStepHasField("name") && hasConflictingJailConfig(w.values.Name) {
		return "name", fmt.Errorf("config already exists: %s", jailConfigPathForName(w.values.Name))
	}
	if w.currentStepHasField("bridge") || w.currentStepHasField("interface") || w.currentStepHasField("ip4") || w.currentStepHasField("ip6") || w.currentStepHasField("default_router") || w.currentStepHasField("startup_order") || w.currentStepHasField("cpu_percent") || w.currentStepHasField("mount_points") {
		if jailType == "vnet" {
			bridge, err := validateNetworkInterfaceName(w.values.Bridge, "bridge")
			if err != nil {
				return "bridge", err
			}
			w.values.Bridge = bridge
			if strings.TrimSpace(w.values.Bridge) == "" {
				return "bridge", fmt.Errorf("bridge is required for vnet jails")
			}
			if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(w.values.Bridge)), "bridge") {
				return "bridge", fmt.Errorf("vnet jails require a bridge such as bridge0")
			}
			policy := effectiveBridgePolicy(w.values)
			switch policy {
			case "auto-create", "require-existing":
				w.values.BridgePolicy = policy
			default:
				return "bridge_policy", fmt.Errorf("bridge policy must be auto-create or require-existing")
			}
			switch effectiveVNETHostSetup(w.values) {
			case "runtime", "persistent":
				w.values.VNETHostSetup = effectiveVNETHostSetup(w.values)
			default:
				return "vnet_host_setup", fmt.Errorf("host setup must be runtime or persistent")
			}
			if strings.TrimSpace(w.values.Uplink) != "" {
				uplink, err := validateOptionalNetworkInterfaceName(w.values.Uplink, "uplink")
				if err != nil {
					return "uplink", err
				}
				w.values.Uplink = uplink
			}
		} else {
			iface, err := validateNetworkInterfaceName(w.values.Interface, "interface")
			if err != nil {
				return "interface", err
			}
			w.values.Interface = iface
			if strings.TrimSpace(w.values.Interface) == "" {
				return "interface", fmt.Errorf("interface is required")
			}
		}
		if strings.TrimSpace(w.values.IP4) == "" {
			return "ip4", fmt.Errorf("IPv4 is required")
		}
		if err := validateJailIPValue(strings.TrimSpace(w.values.IP4), true, "IPv4", jailType != "vnet"); err != nil {
			return "ip4", err
		}
		if err := validateJailIPValue(strings.TrimSpace(w.values.IP6), false, "IPv6", jailType != "vnet"); err != nil {
			return "ip6", err
		}
		if jailType == "vnet" {
			if strings.EqualFold(strings.TrimSpace(w.values.IP4), "inherit") {
				return "ip4", fmt.Errorf("vnet jails cannot use IPv4 inherit; switch jail type or enter an explicit IPv4 address")
			}
			if strings.EqualFold(strings.TrimSpace(w.values.IP6), "inherit") {
				return "ip6", fmt.Errorf("vnet jails cannot use IPv6 inherit; switch jail type or enter an explicit IPv6 address")
			}
		}
		if value := strings.TrimSpace(w.values.DefaultRouter); value != "" {
			if _, err := netip.ParseAddr(value); err != nil {
				return "default_router", fmt.Errorf("default router must be a valid IPv4 or IPv6 address")
			}
		}
		if _, err := parseStartupOrderValue(w.values.StartupOrder); err != nil {
			return "startup_order", err
		}
		dependencies, err := validateExistingJailDependencies(w.values.Dependencies, w.values.Name)
		if err != nil {
			return "dependencies", err
		}
		w.values.Dependencies = strings.Join(dependencies, " ")
		normalized, fieldID, err := normalizeRctlLimitValues(w.values)
		if err != nil {
			return fieldID, err
		}
		w.values.CPUPercent = normalized.CPUPercent
		w.values.MemoryLimit = normalized.MemoryLimit
		w.values.ProcessLimit = normalized.ProcessLimit
		if err := validateMountPointInput(w.values.MountPoints); err != nil {
			return "mount_points", err
		}
		w.refreshNetworkPrereqs()
		if err := w.networkPrereqs.blockingError(); err != nil {
			if len(w.networkPrereqs.RCConfErrors) > 0 {
				return "vnet_host_setup", err
			}
			return blockingPrereqFieldID(w.values), err
		}
	}
	if (w.currentStepHasField("linux_preset") || w.currentStepHasField("linux_distro") || w.currentStepHasField("linux_bootstrap_method") || w.currentStepHasField("linux_release") || w.currentStepHasField("linux_bootstrap") || w.currentStepHasField("linux_mirror_mode") || w.currentStepHasField("linux_mirror_url") || w.currentStepHasField("linux_archive_url")) && jailType == "linux" {
		preset := effectiveLinuxBootstrapPreset(w.values)
		switch preset {
		case "custom", "alpine", "rocky":
		default:
			return "linux_preset", fmt.Errorf("bootstrap preset must be custom, alpine, or rocky")
		}
		w.applyLinuxBootstrapPreset()
		family := strings.ToLower(strings.TrimSpace(w.values.LinuxDistro))
		if family == "" {
			return "linux_distro", fmt.Errorf("bootstrap family is required")
		}
		if !jailNamePattern.MatchString(family) {
			return "linux_distro", fmt.Errorf("bootstrap family must use letters, numbers, dot, underscore, or dash")
		}
		w.values.LinuxDistro = family
		method := effectiveLinuxBootstrapMethod(w.values)
		switch method {
		case "debootstrap", "archive":
		default:
			return "linux_bootstrap_method", fmt.Errorf("bootstrap method must be debootstrap or archive")
		}
		mode := effectiveLinuxBootstrapMode(w.values)
		switch mode {
		case "auto", "skip":
		default:
			return "linux_bootstrap", fmt.Errorf("bootstrap mode must be auto or skip")
		}
		switch method {
		case "debootstrap":
			if strings.TrimSpace(w.values.LinuxRelease) == "" {
				return "linux_release", fmt.Errorf("bootstrap release is required")
			}
			if err := validateLinuxBootstrapReleaseValue(w.values.LinuxRelease); err != nil {
				return "linux_release", err
			}
			mirrorMode := effectiveLinuxMirrorMode(w.values)
			switch mirrorMode {
			case "default", "custom":
			default:
				return "linux_mirror_mode", fmt.Errorf("mirror mode must be default or custom")
			}
			if _, err := resolveLinuxBootstrapSource(w.values); err != nil {
				if mirrorMode == "custom" {
					return "linux_mirror_url", err
				}
				return "linux_mirror_mode", err
			}
			if err := validateLinuxBootstrapReleaseSupport(w.values); err != nil {
				return "linux_release", err
			}
		case "archive":
			if _, err := resolveLinuxBootstrapSource(w.values); err != nil {
				return "linux_archive_url", err
			}
		}
	}
	return "", nil
}

func validateTemplateReleaseInput(values jailWizardValues) error {
	input := strings.TrimSpace(values.TemplateRelease)
	if input == "" {
		return fmt.Errorf("template/release is required (local path, release tag, or https URL)")
	}

	if strings.HasPrefix(input, "/") {
		cleanInput, err := validateAbsolutePath(input, "template/release path")
		if err != nil {
			return err
		}
		input = cleanInput
	}

	if info, err := os.Stat(input); err == nil {
		if normalizedJailType(values.JailType) == "thin" {
			if !info.IsDir() {
				return fmt.Errorf("thin jails require a template dataset mountpoint; use ctrl+t to select one or press c in the selector to create one")
			}
			if _, err := exactZFSDatasetForPath(input); err != nil {
				return fmt.Errorf("thin jails require a template dataset mountpoint; use ctrl+t to select one or press c in the selector to create one")
			}
		}
		return nil
	}

	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		if normalizedJailType(values.JailType) == "thin" {
			info, err := os.Stat(source)
			if err != nil || !info.IsDir() {
				return fmt.Errorf("thin jails require a template dataset mountpoint; use ctrl+t to select one or press c in the selector to create one")
			}
			if _, err := exactZFSDatasetForPath(source); err != nil {
				return fmt.Errorf("thin jails require a template dataset mountpoint; use ctrl+t to select one or press c in the selector to create one")
			}
		}
		return nil
	}

	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		if _, err := neturl.ParseRequestURI(input); err != nil {
			return fmt.Errorf("template/release URL is invalid")
		}
		if normalizedJailType(values.JailType) == "thin" {
			return fmt.Errorf("thin jails require a template dataset mountpoint; use ctrl+t to select one or press c in the selector to create one")
		}
		return nil
	}

	if releaseValuePattern.MatchString(strings.ToUpper(input)) {
		if normalizedJailType(values.JailType) == "thin" {
			return fmt.Errorf("thin jails require a template dataset mountpoint; use ctrl+t to select one or press c in the selector to create one")
		}
		return nil
	}

	if strings.HasPrefix(input, "/") {
		return fmt.Errorf("template/release path %q was not found", input)
	}
	return fmt.Errorf("template/release %q not found; use a local path, an entry from %s, a release tag, or a custom URL", input, defaultUserlandDir)
}

func validateJailIPValue(value string, ipv4 bool, fieldName string, allowInherit bool) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if strings.EqualFold(value, "inherit") {
		if allowInherit {
			return nil
		}
		return fmt.Errorf("%s inherit is only valid for non-vnet jails", fieldName)
	}

	if prefix, err := netip.ParsePrefix(value); err == nil {
		if ipv4 && prefix.Addr().Is4() {
			return nil
		}
		if !ipv4 && prefix.Addr().Is6() {
			return nil
		}
		return fmt.Errorf("%s must match the correct IP family", fieldName)
	}
	if addr, err := netip.ParseAddr(value); err == nil {
		if ipv4 && addr.Is4() {
			return nil
		}
		if !ipv4 && addr.Is6() {
			return nil
		}
		return fmt.Errorf("%s must match the correct IP family", fieldName)
	}

	if ipv4 {
		return fmt.Errorf("%s must be a valid IPv4 address, IPv4 CIDR, or 'inherit'", fieldName)
	}
	return fmt.Errorf("%s must be a valid IPv6 address, IPv6 CIDR, or 'inherit'", fieldName)
}

func validateMountPointInput(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	splitter := strings.NewReplacer("\n", ",", ";", ",")
	chunks := strings.Split(splitter.Replace(raw), ",")
	targets := map[string]string{}
	for _, chunk := range chunks {
		item := strings.TrimSpace(chunk)
		if item == "" {
			continue
		}
		source, target, hasSeparator := strings.Cut(item, ":")
		if hasSeparator {
			source = strings.TrimSpace(source)
			if source == "" {
				return fmt.Errorf("mount source is required before ':'")
			}
			cleanSource, err := validateAccessibleAbsoluteDirectory(source, "mount source")
			if err != nil {
				return err
			}
			source = cleanSource
			cleanTarget, err := validateMountTarget(target)
			if err != nil {
				return err
			}
			if prior, exists := targets[cleanTarget]; exists {
				return fmt.Errorf("mount target %q is duplicated (%s and %s)", cleanTarget, prior, item)
			}
			targets[cleanTarget] = item
			continue
		}
		cleanTarget, err := validateMountTarget(item)
		if err != nil {
			return err
		}
		if prior, exists := targets[cleanTarget]; exists {
			return fmt.Errorf("mount target %q is duplicated (%s and %s)", cleanTarget, prior, item)
		}
		targets[cleanTarget] = item
	}
	return nil
}

func (w jailCreationWizard) validateAll() error {
	_, _, err := w.validateAllDetailed()
	return err
}

func (w jailCreationWizard) validateAllDetailed() (int, string, error) {
	steps := w.steps()
	for idx := 0; idx < len(steps)-1; idx++ {
		test := w
		test.step = idx
		test.normalizeField()
		if fieldID, err := test.validateCurrentStepDetailed(); err != nil {
			return idx, fieldID, err
		}
	}
	if fieldID, err := validateJailCreateHostPreflight(w.values); err != nil {
		return len(steps) - 1, fieldID, err
	}
	return -1, "", nil
}

func blockingPrereqFieldID(values jailWizardValues) string {
	if normalizedJailType(values.JailType) == "vnet" {
		if strings.TrimSpace(values.Bridge) != "" {
			return "bridge"
		}
		return "ip4"
	}
	return "interface"
}
