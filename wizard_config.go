package main

import (
	"fmt"
	"hash/fnv"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

func parseMountPointSpecs(raw string) []mountPointSpec {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	splitter := strings.NewReplacer("\n", ",", ";", ",")
	chunks := strings.Split(splitter.Replace(raw), ",")
	specs := make([]mountPointSpec, 0, len(chunks))
	for _, chunk := range chunks {
		item := strings.TrimSpace(chunk)
		if item == "" {
			continue
		}
		source, target, hasSeparator := strings.Cut(item, ":")
		if hasSeparator {
			source = strings.TrimSpace(source)
			target, _ = validateMountTarget(target)
			if source == "" || target == "" {
				continue
			}
			specs = append(specs, mountPointSpec{Source: source, Target: target})
			continue
		}
		target, _ = validateMountTarget(item)
		if target == "" {
			continue
		}
		specs = append(specs, mountPointSpec{Target: target})
	}
	return specs
}

func jailConfigPathForName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "/etc/jail.conf.d/new-jail.conf"
	}
	return filepath.Join("/etc/jail.conf.d", name+".conf")
}

func jailFstabPathForName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "/etc/fstab.new-jail"
	}
	return filepath.Join("/etc", "fstab."+name)
}

func checkJailRootExistsAndNotEmpty(values jailWizardValues) bool {
	if normalizedJailType(values.JailType) == "thin" {
		return false
	}
	destination := strings.TrimSpace(values.Dataset)
	if !strings.HasPrefix(destination, "/") {
		// Try to resolve ZFS dataset mountpoint
		out, err := exec.Command("zfs", "list", "-H", "-o", "mountpoint", destination).Output()
		if err == nil {
			mountpoint := strings.TrimSpace(strings.Split(string(out), "\n")[0])
			if mountpoint != "" && mountpoint != "-" && mountpoint != "legacy" {
				destination = mountpoint
			} else {
				destination = "/" + strings.Trim(destination, "/")
			}
		} else {
			destination = "/" + strings.Trim(destination, "/")
		}
	}

	entries, err := os.ReadDir(destination)
	if err != nil {
		return false
	}
	return len(entries) > 0
}

func defaultJailPathForValues(values jailWizardValues) string {
	destination := strings.TrimSpace(values.Dataset)
	if strings.HasPrefix(destination, "/") {
		return filepath.Clean(destination)
	}
	dataset := strings.Trim(destination, "/")
	if dataset != "" {
		return "/" + dataset
	}
	name := strings.TrimSpace(values.Name)
	if name == "" {
		name = "new-jail"
	}
	return filepath.Join("/usr/jails", name)
}

func buildJailConfBlock(values jailWizardValues, jailPath, fstabPath string) []string {
	name := strings.TrimSpace(values.Name)
	if name == "" {
		name = "new-jail"
	}
	jailType := normalizedJailType(values.JailType)
	lines := []string{
		fmt.Sprintf("%s {", name),
		fmt.Sprintf("  exec.consolelog = \"/var/log/jail_console_%s.log\";", name),
		"  allow.raw_sockets;",
		"  exec.clean;",
		"  mount.devfs;",
		fmt.Sprintf("  host.hostname = %q;", effectiveJailHostname(values)),
		fmt.Sprintf("  path = %q;", jailPath),
	}

	switch jailType {
	case "vnet":
		lines = append(lines,
			fmt.Sprintf("  # freebsd-jails-tui: bridge=%s bridge_policy=%s host_setup=%s uplink=%s default_router=%s;", strings.TrimSpace(values.Bridge), effectiveBridgePolicy(values), effectiveVNETHostSetup(values), strings.TrimSpace(values.Uplink), strings.TrimSpace(values.DefaultRouter)),
		)
		lines = append(lines, buildVNETJailConfig(values)...)
	case "linux":
		lines = append(lines,
			fmt.Sprintf("  # freebsd-jails-tui: linux_preset=%s linux_distro=%s linux_bootstrap_method=%s linux_release=%s linux_bootstrap=%s linux_mirror_mode=%s linux_mirror_url=%s linux_archive_url=%s;", effectiveLinuxBootstrapPreset(values), effectiveLinuxDistro(values), effectiveLinuxBootstrapMethod(values), effectiveLinuxRelease(values), effectiveLinuxBootstrapMode(values), effectiveLinuxMirrorMode(values), linuxMirrorMetadataValue(values), linuxArchiveMetadataValue(values)),
		)
		lines = append(lines, buildLinuxJailConfig(values, jailPath)...)
	default:
		lines = append(lines,
			"  exec.start = \"/bin/sh /etc/rc\";",
			"  exec.stop = \"/bin/sh /etc/rc.shutdown\";",
		)
		lines = append(lines, fmt.Sprintf("  interface = %q;", strings.TrimSpace(values.Interface)))
		appendJailIPConfig(&lines, "ip4", strings.TrimSpace(values.IP4))
		appendJailIPConfig(&lines, "ip6", strings.TrimSpace(values.IP6))
	}
	if hasAnyRctlLimits(values) {
		lines = append(lines,
			fmt.Sprintf("  # freebsd-jails-tui: rctl_mode=persistent cpu_percent=%s memory_limit=%s process_limit=%s;", metadataDashValue(values.CPUPercent), metadataDashValue(values.MemoryLimit), metadataDashValue(values.ProcessLimit)),
		)
	}
	if note, err := normalizeJailNote(values.Note); err == nil && note != "" {
		lines = append(lines, fmt.Sprintf("  # freebsd-jails-tui: note=%s;", encodeTUIMetadataValue(note)))
	}

	if strings.TrimSpace(values.DefaultRouter) != "" {
		if jailType != "vnet" {
			lines = append(lines, fmt.Sprintf("  defaultrouter = %q;", strings.TrimSpace(values.DefaultRouter)))
		}
	}
	if strings.TrimSpace(fstabPath) != "" {
		lines = append(lines, fmt.Sprintf("  mount.fstab = %q;", fstabPath))
	}
	if dependencies := strings.Join(mustParseJailDependencyNames(values.Dependencies), ", "); dependencies != "" {
		lines = append(lines, fmt.Sprintf("  depend = %s;", dependencies))
	}
	lines = append(lines, "  persist;")
	lines = append(lines, "}")
	return lines
}

func effectiveJailHostname(values jailWizardValues) string {
	hostname := strings.TrimSpace(values.Hostname)
	if hostname != "" {
		return hostname
	}
	name := strings.TrimSpace(values.Name)
	if name != "" {
		return name
	}
	return "new-jail"
}

func normalizedJailType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "thick"
	}
	return value
}

func appendJailIPConfig(lines *[]string, family, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if strings.EqualFold(value, "inherit") {
		*lines = append(*lines, fmt.Sprintf("  %s = %q;", family, "inherit"))
		return
	}
	*lines = append(*lines, fmt.Sprintf("  %s.addr = %q;", family, value))
}

func buildVNETJailConfig(values jailWizardValues) []string {
	epair := vnetEpairName(values.Name)
	bridge := strings.TrimSpace(values.Bridge)
	lines := []string{
		"  vnet;",
		"  devfs_ruleset = 5;",
		fmt.Sprintf("  vnet.interface = %q;", epair+"b"),
		fmt.Sprintf("  exec.prestart = \"/sbin/ifconfig %s create up\";", epair),
		fmt.Sprintf("  exec.prestart += \"/sbin/ifconfig %sa up descr jail:${name}\";", epair),
		fmt.Sprintf("  exec.prestart += \"/sbin/ifconfig %s addm %sa up\";", bridge, epair),
	}
	if ip4 := strings.TrimSpace(values.IP4); ip4 != "" {
		lines = append(lines, fmt.Sprintf("  exec.start = \"/sbin/ifconfig %sb inet %s up\";", epair, ip4))
	} else {
		lines = append(lines, fmt.Sprintf("  exec.start = \"/bin/true\";"))
	}
	if ip6 := strings.TrimSpace(values.IP6); ip6 != "" {
		lines = append(lines, fmt.Sprintf("  exec.start += \"/sbin/ifconfig %sb inet6 %s up\";", epair, ip6))
	}
	if router := strings.TrimSpace(values.DefaultRouter); router != "" {
		routeCmd := "/sbin/route add default " + router
		if strings.Contains(router, ":") {
			routeCmd = "/sbin/route -6 add default " + router
		}
		lines = append(lines, fmt.Sprintf("  exec.start += %q;", routeCmd))
	}
	lines = append(lines,
		"  exec.start += \"/bin/sh /etc/rc\";",
		"  exec.stop = \"/bin/sh /etc/rc.shutdown\";",
		fmt.Sprintf("  exec.poststop = \"/sbin/ifconfig %s deletem %sa\";", bridge, epair),
		fmt.Sprintf("  exec.poststop += \"/sbin/ifconfig %sa destroy\";", epair),
	)
	return lines
}

func buildLinuxJailConfig(values jailWizardValues, jailPath string) []string {
	compatRoot := linuxCompatRoot(jailPath, values)
	lines := []string{
		"  exec.start = \"/bin/sh /etc/rc\";",
		"  exec.stop = \"/bin/sh /etc/rc.shutdown\";",
		"  devfs_ruleset = 4;",
		"  allow.mount;",
		"  allow.mount.devfs;",
		"  allow.mount.fdescfs;",
		"  allow.mount.procfs;",
		"  allow.mount.linprocfs;",
		"  allow.mount.linsysfs;",
		"  allow.mount.tmpfs;",
		"  enforce_statfs = 1;",
		fmt.Sprintf("  interface = %q;", strings.TrimSpace(values.Interface)),
	}
	appendJailIPConfig(&lines, "ip4", strings.TrimSpace(values.IP4))
	appendJailIPConfig(&lines, "ip6", strings.TrimSpace(values.IP6))
	lines = append(lines,
		fmt.Sprintf("  mount += %q;", fmt.Sprintf("devfs     %s/dev     devfs     rw  0 0", compatRoot)),
		fmt.Sprintf("  mount += %q;", fmt.Sprintf("tmpfs     %s/dev/shm tmpfs     rw,size=1g,mode=1777  0 0", compatRoot)),
		fmt.Sprintf("  mount += %q;", fmt.Sprintf("fdescfs   %s/dev/fd  fdescfs   rw,linrdlnk 0 0", compatRoot)),
		fmt.Sprintf("  mount += %q;", fmt.Sprintf("linprocfs %s/proc    linprocfs rw  0 0", compatRoot)),
		fmt.Sprintf("  mount += %q;", fmt.Sprintf("linsysfs  %s/sys     linsysfs  rw  0 0", compatRoot)),
		fmt.Sprintf("  mount += %q;", fmt.Sprintf("/tmp      %s/tmp     nullfs    rw  0 0", compatRoot)),
		fmt.Sprintf("  mount += %q;", fmt.Sprintf("/home     %s/home    nullfs    rw  0 0", compatRoot)),
	)
	return lines
}

func effectiveBridgePolicy(values jailWizardValues) string {
	policy := strings.ToLower(strings.TrimSpace(values.BridgePolicy))
	if policy == "" {
		return "auto-create"
	}
	return policy
}

func parseStartupOrderValue(raw string) (int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	position, err := strconv.Atoi(value)
	if err != nil || position <= 0 {
		return 0, fmt.Errorf("startup order must be a positive integer or blank to append")
	}
	return position, nil
}

func parseJailDependencyNames(raw, self string) ([]string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, nil
	}
	replacer := strings.NewReplacer(",", " ", "\n", " ", ";", " ", "\t", " ")
	tokens := strings.Fields(replacer.Replace(value))
	deps := make([]string, 0, len(tokens))
	seen := map[string]struct{}{}
	self = strings.TrimSpace(self)
	for _, token := range tokens {
		if !jailNamePattern.MatchString(token) {
			return nil, fmt.Errorf("dependency %q is not a valid jail name", token)
		}
		if self != "" && token == self {
			return nil, fmt.Errorf("dependency %q cannot reference the jail itself", token)
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		deps = append(deps, token)
	}
	sort.Strings(deps)
	return deps, nil
}

func validateExistingJailDependencies(raw, self string) ([]string, error) {
	deps, err := parseJailDependencyNames(raw, self)
	if err != nil {
		return nil, err
	}
	if len(deps) == 0 {
		return nil, nil
	}
	configured := discoverConfiguredJails()
	known := make(map[string]struct{}, len(configured))
	for _, name := range configured {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		known[name] = struct{}{}
	}
	for _, dep := range deps {
		if _, ok := known[dep]; !ok {
			return nil, fmt.Errorf("dependency %q is not a configured jail", dep)
		}
	}
	return deps, nil
}

func mustParseJailDependencyNames(raw string) []string {
	deps, _ := parseJailDependencyNames(raw, "")
	return deps
}

func startupOrderSummary(raw string) string {
	position, err := parseStartupOrderValue(raw)
	if err != nil || position == 0 {
		return "append to jail_list"
	}
	return fmt.Sprintf("position %d in jail_list", position)
}

func dependencySummary(raw string) string {
	deps := mustParseJailDependencyNames(raw)
	if len(deps) == 0 {
		return "-"
	}
	return strings.Join(deps, " ")
}

func metadataDashValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func normalizeJailNote(value string) (string, error) {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if utf8.RuneCountInString(value) > maxJailNoteLen {
		return "", fmt.Errorf("note must be %d characters or fewer", maxJailNoteLen)
	}
	return value, nil
}

func jailNoteLength(value string) int {
	return utf8.RuneCountInString(value)
}

func encodeTUIMetadataValue(value string) string {
	return neturl.PathEscape(strings.TrimSpace(value))
}

func decodeTUIMetadataValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	decoded, err := neturl.PathUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

func freeBSDPatchSummary(sourceInput, preference string) string {
	decision := resolveFreeBSDPatchDecision(sourceInput, preference)
	if decision.Err != nil {
		return "invalid"
	}
	switch decision.Preference {
	case "yes":
		return "yes"
	case "no":
		return "no"
	default:
		if decision.Effective {
			return "auto (yes)"
		}
		return "auto (no)"
	}
}

func effectiveVNETHostSetup(values jailWizardValues) string {
	switch strings.ToLower(strings.TrimSpace(values.VNETHostSetup)) {
	case "persistent":
		return "persistent"
	default:
		return "runtime"
	}
}

func vnetEpairName(name string) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(strings.TrimSpace(name)))
	return fmt.Sprintf("epair%d", 100+(hasher.Sum32()%9000))
}

func wizardSectionForField(id string) string {
	switch id {
	case "jail_type":
		return "Type"
	case "name", "hostname":
		return "Identity"
	case "dataset", "template_release":
		return "Root filesystem"
	case "patch_base":
		return "Root filesystem"
	case "interface", "bridge", "bridge_policy", "vnet_host_setup", "uplink", "ip4", "ip6", "default_router":
		return "Networking"
	case "startup_order", "dependencies":
		return "Startup"
	case "cpu_percent", "memory_limit", "process_limit":
		return "Resource limits"
	case "mount_points":
		return "Mount points"
	case "linux_preset", "linux_distro", "linux_bootstrap_method", "linux_release", "linux_bootstrap", "linux_mirror_mode", "linux_mirror_url", "linux_archive_url":
		return "Linux bootstrap"
	default:
		return ""
	}
}

func hasConflictingJailConfig(name string) bool {
	configPath := jailConfigPathForName(strings.TrimSpace(name))
	if _, err := os.Stat(configPath); err == nil {
		return true
	}
	return false
}
