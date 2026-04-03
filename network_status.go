package main

import (
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"sort"
	"strings"
)

type hostInterfaceState struct {
	Name     string
	IsBridge bool
	Members  []string
	IPv4     []netip.Addr
	IPv6     []netip.Addr
}

type HostNetworkState struct {
	Interfaces   map[string]hostInterfaceState
	InspectError string
}

type NetworkWizardPrereqs struct {
	JailType           string
	InspectError       string
	Interface          string
	InterfaceExists    bool
	Bridge             string
	BridgePolicy       string
	BridgeExists       bool
	BridgeIsBridge     bool
	BridgeCreateNeeded bool
	Uplink             string
	UplinkExists       bool
	UplinkAttached     bool
	UplinkAttachNeeded bool
	IP4                string
	IP4Conflicts       []string
	IP4OverlapWarnings []string
	IP6                string
	IP6Conflicts       []string
	IP6OverlapWarnings []string
	DefaultRouter      string
	RouterStatus       string
	Warnings           []string
	Errors             []string
}

func collectHostNetworkState() HostNetworkState {
	state := HostNetworkState{
		Interfaces: make(map[string]hostInterfaceState),
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		state.InspectError = err.Error()
		return state
	}
	for _, iface := range ifaces {
		info := hostInterfaceState{
			Name: iface.Name,
		}
		for _, addr := range collectInterfaceAddrs(iface) {
			if addr.Is4() {
				info.IPv4 = append(info.IPv4, addr)
				continue
			}
			if addr.Is6() {
				info.IPv6 = append(info.IPv6, addr)
			}
		}
		if output, err := exec.Command("ifconfig", iface.Name).CombinedOutput(); err == nil {
			parseIfconfigDetails(&info, string(output))
		}
		info.Members = uniqueStrings(info.Members)
		state.Interfaces[iface.Name] = info
	}
	return state
}

func collectInterfaceAddrs(iface net.Interface) []netip.Addr {
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}
	out := make([]netip.Addr, 0, len(addrs))
	for _, addr := range addrs {
		parsed, ok := parseAddrString(addr.String())
		if !ok {
			continue
		}
		out = append(out, parsed)
	}
	return out
}

func parseAddrString(raw string) (netip.Addr, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return netip.Addr{}, false
	}
	if host, _, ok := strings.Cut(raw, "/"); ok {
		raw = host
	}
	raw = strings.TrimSpace(raw)
	if idx := strings.Index(raw, "%"); idx >= 0 {
		raw = raw[:idx]
	}
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

func parseIfconfigDetails(info *hostInterfaceState, output string) {
	nameLower := strings.ToLower(strings.TrimSpace(info.Name))
	if strings.HasPrefix(nameLower, "bridge") {
		info.IsBridge = true
	}
	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "member:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				info.Members = append(info.Members, fields[1])
				info.IsBridge = true
			}
			continue
		}
		if strings.HasPrefix(line, "groups:") && strings.Contains(" "+line+" ", " bridge ") {
			info.IsBridge = true
		}
	}
}

func collectNetworkWizardPrereqs(values jailWizardValues) NetworkWizardPrereqs {
	prereqs := NetworkWizardPrereqs{
		JailType:      normalizedJailType(values.JailType),
		Interface:     strings.TrimSpace(values.Interface),
		Bridge:        strings.TrimSpace(values.Bridge),
		BridgePolicy:  effectiveBridgePolicy(values),
		Uplink:        strings.TrimSpace(values.Uplink),
		IP4:           strings.TrimSpace(values.IP4),
		IP6:           strings.TrimSpace(values.IP6),
		DefaultRouter: strings.TrimSpace(values.DefaultRouter),
	}
	state := collectHostNetworkState()
	runningJails, runningErr := collectRunningJailNetworkState()
	prereqs.InspectError = strings.TrimSpace(state.InspectError)
	if prereqs.InspectError == "" && runningErr != nil {
		prereqs.InspectError = runningErr.Error()
	}
	if prereqs.InspectError != "" {
		prereqs.Errors = append(prereqs.Errors, "host network inspection failed: "+prereqs.InspectError)
		return prereqs
	}

	switch prereqs.JailType {
	case "vnet":
		if prereqs.Bridge != "" {
			if info, ok := state.Interfaces[prereqs.Bridge]; ok {
				prereqs.BridgeExists = true
				prereqs.BridgeIsBridge = info.IsBridge
				if !info.IsBridge {
					prereqs.Errors = append(prereqs.Errors, fmt.Sprintf("bridge %q exists but is not a bridge interface", prereqs.Bridge))
				}
			} else {
				if prereqs.BridgePolicy == "require-existing" {
					prereqs.Errors = append(prereqs.Errors, fmt.Sprintf("bridge %q does not exist and bridge policy requires an existing bridge", prereqs.Bridge))
				} else {
					prereqs.BridgeCreateNeeded = true
					prereqs.Warnings = append(prereqs.Warnings, fmt.Sprintf("bridge %q will be created on the host before jail start", prereqs.Bridge))
				}
			}
		}
		if prereqs.Uplink != "" {
			if info, ok := state.Interfaces[prereqs.Uplink]; ok {
				prereqs.UplinkExists = true
				if prereqs.Uplink == prereqs.Bridge {
					prereqs.Errors = append(prereqs.Errors, "uplink must not be the same interface as the bridge")
				} else if prereqs.BridgeExists && prereqs.BridgeIsBridge {
					prereqs.UplinkAttached = stringSliceContains(infoIfBridgeMembers(state, prereqs.Bridge), prereqs.Uplink)
					prereqs.UplinkAttachNeeded = !prereqs.UplinkAttached
					if prereqs.UplinkAttachNeeded {
						prereqs.Warnings = append(prereqs.Warnings, fmt.Sprintf("uplink %q will be attached to %q before jail start", prereqs.Uplink, prereqs.Bridge))
					}
				} else if prereqs.BridgeCreateNeeded {
					prereqs.UplinkAttachNeeded = true
					prereqs.Warnings = append(prereqs.Warnings, fmt.Sprintf("uplink %q will be attached to %q after the bridge is created", prereqs.Uplink, prereqs.Bridge))
				}
				_ = info
			} else {
				prereqs.Errors = append(prereqs.Errors, fmt.Sprintf("uplink %q was not found on the host", prereqs.Uplink))
			}
		} else {
			prereqs.Warnings = append(prereqs.Warnings, "no uplink selected; the bridge will stay isolated until you connect it manually")
		}
	default:
		if prereqs.Interface != "" {
			_, prereqs.InterfaceExists = state.Interfaces[prereqs.Interface]
			if !prereqs.InterfaceExists {
				prereqs.Errors = append(prereqs.Errors, fmt.Sprintf("interface %q was not found on the host", prereqs.Interface))
			}
		}
	}

	prereqs.IP4Conflicts = append(prereqs.IP4Conflicts, hostAddressConflicts(state, prereqs.IP4)...)
	prereqs.IP4Conflicts = append(prereqs.IP4Conflicts, runningJailAddressConflicts(runningJails, prereqs.IP4)...)
	prereqs.IP4Conflicts = uniqueStrings(prereqs.IP4Conflicts)
	if len(prereqs.IP4Conflicts) > 0 {
		prereqs.Errors = append(prereqs.Errors, fmt.Sprintf("IPv4 %q is already assigned on the host or a running jail: %s", prereqs.IP4, strings.Join(prereqs.IP4Conflicts, ", ")))
	}
	prereqs.IP4OverlapWarnings = runningJailSubnetOverlapWarnings(runningJails, prereqs.IP4, true)
	if len(prereqs.IP4OverlapWarnings) > 0 {
		prereqs.Warnings = append(prereqs.Warnings, fmt.Sprintf("IPv4 subnet %q overlaps running jail addresses/subnets: %s", prereqs.IP4, strings.Join(prereqs.IP4OverlapWarnings, ", ")))
	}
	prereqs.IP6Conflicts = append(prereqs.IP6Conflicts, hostAddressConflicts(state, prereqs.IP6)...)
	prereqs.IP6Conflicts = append(prereqs.IP6Conflicts, runningJailAddressConflicts(runningJails, prereqs.IP6)...)
	prereqs.IP6Conflicts = uniqueStrings(prereqs.IP6Conflicts)
	if len(prereqs.IP6Conflicts) > 0 {
		prereqs.Errors = append(prereqs.Errors, fmt.Sprintf("IPv6 %q is already assigned on the host or a running jail: %s", prereqs.IP6, strings.Join(prereqs.IP6Conflicts, ", ")))
	}
	prereqs.IP6OverlapWarnings = runningJailSubnetOverlapWarnings(runningJails, prereqs.IP6, false)
	if len(prereqs.IP6OverlapWarnings) > 0 {
		prereqs.Warnings = append(prereqs.Warnings, fmt.Sprintf("IPv6 subnet %q overlaps running jail addresses/subnets: %s", prereqs.IP6, strings.Join(prereqs.IP6OverlapWarnings, ", ")))
	}
	prereqs.RouterStatus = evaluateDefaultRouterStatus(prereqs)
	return prereqs
}

type runningJailNetworkState struct {
	Name         string
	IPv4         []netip.Addr
	IPv6         []netip.Addr
	IPv4Prefixes []netip.Prefix
	IPv6Prefixes []netip.Prefix
}

func collectRunningJailNetworkState() ([]runningJailNetworkState, error) {
	out, err := exec.Command("jls", "-n").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run jls -n: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	result := make([]runningJailNetworkState, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := parseKVFields(line)
		name := strings.TrimSpace(fields["name"])
		if name == "" {
			continue
		}
		result = append(result, runningJailNetworkState{
			Name:         name,
			IPv4:         parseJailAddressField(fields["ip4.addr"], fields["ip4"]),
			IPv6:         parseJailAddressField(fields["ip6.addr"], fields["ip6"]),
			IPv4Prefixes: parseJailPrefixField(fields["ip4.addr"], fields["ip4"]),
			IPv6Prefixes: parseJailPrefixField(fields["ip6.addr"], fields["ip6"]),
		})
	}
	return result, nil
}

func parseJailAddressField(values ...string) []netip.Addr {
	var result []netip.Addr
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || strings.EqualFold(value, "inherit") {
			continue
		}
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			addr, ok := explicitJailAddr(part)
			if !ok {
				continue
			}
			result = append(result, addr)
		}
	}
	return result
}

func parseJailPrefixField(values ...string) []netip.Prefix {
	var result []netip.Prefix
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || strings.EqualFold(value, "inherit") {
			continue
		}
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			prefix, ok := explicitJailPrefix(part)
			if !ok {
				continue
			}
			result = append(result, prefix)
		}
	}
	return result
}

func runningJailAddressConflicts(jails []runningJailNetworkState, value string) []string {
	addr, ok := explicitJailAddr(value)
	if !ok {
		return nil
	}
	conflicts := make([]string, 0, 2)
	for _, jail := range jails {
		for _, jailAddr := range jail.IPv4 {
			if jailAddr == addr {
				conflicts = append(conflicts, "jail:"+jail.Name)
				goto nextJail
			}
		}
		for _, jailAddr := range jail.IPv6 {
			if jailAddr == addr {
				conflicts = append(conflicts, "jail:"+jail.Name)
				goto nextJail
			}
		}
	nextJail:
	}
	return conflicts
}

func runningJailSubnetOverlapWarnings(jails []runningJailNetworkState, value string, ipv4 bool) []string {
	requestedPrefix, hasRequestedPrefix := explicitJailPrefix(value)
	if !hasRequestedPrefix {
		return nil
	}
	warnings := make([]string, 0, 2)
	for _, jail := range jails {
		var jailAddrs []netip.Addr
		var jailPrefixes []netip.Prefix
		if ipv4 {
			jailAddrs = jail.IPv4
			jailPrefixes = jail.IPv4Prefixes
		} else {
			jailAddrs = jail.IPv6
			jailPrefixes = jail.IPv6Prefixes
		}
		for _, jailPrefix := range jailPrefixes {
			if prefixesOverlap(requestedPrefix, jailPrefix) {
				warnings = append(warnings, fmt.Sprintf("jail:%s (%s)", jail.Name, jailPrefix.String()))
				goto nextJail
			}
		}
		for _, jailAddr := range jailAddrs {
			if requestedPrefix.Contains(jailAddr) {
				warnings = append(warnings, fmt.Sprintf("jail:%s (%s)", jail.Name, jailAddr.String()))
				goto nextJail
			}
		}
	nextJail:
	}
	return uniqueStrings(warnings)
}

func prefixesOverlap(a, b netip.Prefix) bool {
	a = a.Masked()
	b = b.Masked()
	return a.Contains(b.Addr()) || b.Contains(a.Addr())
}

func infoIfBridgeMembers(state HostNetworkState, bridge string) []string {
	info, ok := state.Interfaces[bridge]
	if !ok {
		return nil
	}
	return info.Members
}

func hostAddressConflicts(state HostNetworkState, value string) []string {
	addr, ok := explicitJailAddr(value)
	if !ok {
		return nil
	}
	conflicts := make([]string, 0, 2)
	for name, iface := range state.Interfaces {
		for _, hostAddr := range iface.IPv4 {
			if hostAddr == addr {
				conflicts = append(conflicts, name)
				goto nextIface
			}
		}
		for _, hostAddr := range iface.IPv6 {
			if hostAddr == addr {
				conflicts = append(conflicts, name)
				goto nextIface
			}
		}
	nextIface:
	}
	sort.Strings(conflicts)
	return conflicts
}

func explicitJailAddr(value string) (netip.Addr, bool) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "inherit") {
		return netip.Addr{}, false
	}
	if prefix, err := netip.ParsePrefix(value); err == nil {
		return prefix.Addr().Unmap(), true
	}
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

func explicitJailPrefix(value string) (netip.Prefix, bool) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "inherit") {
		return netip.Prefix{}, false
	}
	prefix, err := netip.ParsePrefix(value)
	if err != nil {
		return netip.Prefix{}, false
	}
	return prefix.Masked(), true
}

func evaluateDefaultRouterStatus(prereqs NetworkWizardPrereqs) string {
	router := strings.TrimSpace(prereqs.DefaultRouter)
	if router == "" {
		return ""
	}
	addr, err := netip.ParseAddr(router)
	if err != nil {
		return "invalid router address"
	}
	familyValue := prereqs.IP4
	family := "IPv4"
	if addr.Is6() {
		familyValue = prereqs.IP6
		family = "IPv6"
	}
	familyValue = strings.TrimSpace(familyValue)
	switch {
	case familyValue == "":
		return fmt.Sprintf("%s default router is set, but no %s jail address is configured", family, strings.ToLower(family))
	case strings.EqualFold(familyValue, "inherit"):
		return fmt.Sprintf("%s default router cannot be verified against inherited addressing", family)
	}
	if prefix, ok := explicitJailPrefix(familyValue); ok {
		if prefix.Contains(addr.Unmap()) {
			return fmt.Sprintf("%s default router is inside the configured jail subnet", family)
		}
		return fmt.Sprintf("%s default router is outside the configured jail subnet", family)
	}
	if familyAddr, ok := explicitJailAddr(familyValue); ok {
		if familyAddr.Is4() == addr.Is4() {
			return fmt.Sprintf("%s default router matches the configured address family", family)
		}
	}
	return fmt.Sprintf("%s default router does not match the configured address family", family)
}

func (p NetworkWizardPrereqs) blockingError() error {
	if len(p.Errors) == 0 {
		if strings.Contains(strings.ToLower(p.RouterStatus), "outside the configured jail subnet") ||
			strings.Contains(strings.ToLower(p.RouterStatus), "does not match the configured address family") ||
			strings.Contains(strings.ToLower(p.RouterStatus), "no ipv4 jail address") ||
			strings.Contains(strings.ToLower(p.RouterStatus), "no ipv6 jail address") {
			return fmt.Errorf("%s", p.RouterStatus)
		}
		return nil
	}
	return fmt.Errorf("%s", p.Errors[0])
}

func networkWizardPrereqLines(prereqs NetworkWizardPrereqs) []string {
	lines := make([]string, 0, 16)
	if prereqs.JailType == "vnet" {
		lines = append(lines,
			fmt.Sprintf("Bridge: %s", valueOrDash(prereqs.Bridge)),
			fmt.Sprintf("Bridge policy: %s", valueOrDash(prereqs.BridgePolicy)),
			fmt.Sprintf("Bridge exists: %s", yesNoText(prereqs.BridgeExists)),
		)
		if prereqs.BridgeExists {
			lines = append(lines, fmt.Sprintf("Bridge type valid: %s", yesNoText(prereqs.BridgeIsBridge)))
		}
		if prereqs.BridgeCreateNeeded {
			lines = append(lines, "Bridge setup: bridge will be created automatically before jail create.")
		}
		if prereqs.Uplink != "" {
			lines = append(lines,
				fmt.Sprintf("Uplink: %s", prereqs.Uplink),
				fmt.Sprintf("Uplink exists: %s", yesNoText(prereqs.UplinkExists)),
				fmt.Sprintf("Uplink already attached: %s", yesNoText(prereqs.UplinkAttached)),
			)
			if prereqs.UplinkAttachNeeded {
				lines = append(lines, "Uplink setup: uplink will be attached to the bridge before jail start.")
			}
		} else {
			lines = append(lines, "Uplink: none selected")
		}
	} else {
		lines = append(lines,
			fmt.Sprintf("Interface: %s", valueOrDash(prereqs.Interface)),
			fmt.Sprintf("Interface exists: %s", yesNoText(prereqs.InterfaceExists)),
		)
	}

	if strings.TrimSpace(prereqs.IP4) != "" && !strings.EqualFold(strings.TrimSpace(prereqs.IP4), "inherit") {
		if len(prereqs.IP4Conflicts) == 0 {
			lines = append(lines, "Host IPv4 conflict: no")
		} else {
			lines = append(lines, "Host IPv4 conflict: "+strings.Join(prereqs.IP4Conflicts, ", "))
		}
	}
	if strings.TrimSpace(prereqs.IP6) != "" && !strings.EqualFold(strings.TrimSpace(prereqs.IP6), "inherit") {
		if len(prereqs.IP6Conflicts) == 0 {
			lines = append(lines, "Host IPv6 conflict: no")
		} else {
			lines = append(lines, "Host IPv6 conflict: "+strings.Join(prereqs.IP6Conflicts, ", "))
		}
	}
	if prereqs.RouterStatus != "" {
		lines = append(lines, "Default router: "+prereqs.RouterStatus)
	}
	for _, warning := range prereqs.Warnings {
		lines = append(lines, "Warning: "+warning)
	}
	for _, err := range prereqs.Errors {
		lines = append(lines, "Error: "+err)
	}
	return lines
}

func ensureHostNetworkReady(values jailWizardValues, logs *[]string) (func(), error) {
	prereqs := collectNetworkWizardPrereqs(values)
	for _, warning := range prereqs.Warnings {
		*logs = append(*logs, "network preflight: "+warning)
	}
	if prereqs.RouterStatus != "" {
		*logs = append(*logs, "network preflight: "+prereqs.RouterStatus)
	}
	if err := prereqs.blockingError(); err != nil {
		*logs = append(*logs, "network preflight failed: "+err.Error())
		return nil, err
	}
	if normalizedJailType(values.JailType) != "vnet" {
		return nil, nil
	}
	return ensureVNETHostReady(prereqs, logs)
}

func ensureVNETHostReady(prereqs NetworkWizardPrereqs, logs *[]string) (func(), error) {
	if strings.TrimSpace(prereqs.Bridge) == "" {
		return nil, fmt.Errorf("bridge is required for vnet jails")
	}

	createdBridge := false
	attachedUplink := false
	if prereqs.BridgeCreateNeeded {
		if out, err := runLoggedCommand(logs, "ifconfig", prereqs.Bridge, "create"); err != nil {
			if !strings.Contains(strings.ToLower(out), "file exists") {
				return nil, fmt.Errorf("failed to create bridge %q: %w", prereqs.Bridge, err)
			}
		} else {
			createdBridge = true
		}
	}
	if _, err := runLoggedCommand(logs, "ifconfig", prereqs.Bridge, "up"); err != nil {
		return nil, fmt.Errorf("failed to bring bridge %q up: %w", prereqs.Bridge, err)
	}
	if prereqs.UplinkAttachNeeded && strings.TrimSpace(prereqs.Uplink) != "" {
		if out, err := runLoggedCommand(logs, "ifconfig", prereqs.Bridge, "addm", prereqs.Uplink, "up"); err != nil {
			if !strings.Contains(strings.ToLower(out), "file exists") {
				return nil, fmt.Errorf("failed to attach uplink %q to bridge %q: %w", prereqs.Uplink, prereqs.Bridge, err)
			}
		} else {
			attachedUplink = true
		}
	}

	return func() {
		if attachedUplink && !createdBridge {
			if _, err := runLoggedCommand(logs, "ifconfig", prereqs.Bridge, "deletem", prereqs.Uplink); err != nil {
				*logs = append(*logs, fmt.Sprintf("  rollback warning: failed to detach uplink %q from bridge %q: %v", prereqs.Uplink, prereqs.Bridge, err))
			}
		}
		if createdBridge {
			if _, err := runLoggedCommand(logs, "ifconfig", prereqs.Bridge, "destroy"); err != nil {
				*logs = append(*logs, fmt.Sprintf("  rollback warning: failed to destroy bridge %q: %v", prereqs.Bridge, err))
			}
		}
	}, nil
}

func stringSliceContains(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
