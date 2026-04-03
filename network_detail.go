package main

import (
	"fmt"
	"regexp"
	"strings"
)

type JailNetworkSummary struct {
	Configured map[string]string
	Runtime    map[string]string
	Validation []string
}

var (
	vnetBridgePattern = regexp.MustCompile(`ifconfig\s+([A-Za-z0-9_.:-]+)\s+addm\s+epair[0-9]+a\s+up`)
	vnetUplinkPattern = regexp.MustCompile(`ifconfig\s+([A-Za-z0-9_.:-]+)\s+addm\s+([A-Za-z0-9_.:-]+)\s+up`)
	vnetRoutePattern  = regexp.MustCompile(`route(?:\s+-6)?\s+add\s+default\s+([^\s"']+)`)
)

func collectJailNetworkSummary(detail JailDetail) *JailNetworkSummary {
	values := detailNetworkWizardValues(detail)
	summary := &JailNetworkSummary{
		Configured: map[string]string{},
		Runtime:    map[string]string{},
	}
	jailType := normalizedJailType(values.JailType)
	summary.Configured["Type"] = jailType
	if jailType == "vnet" {
		summary.Configured["Bridge"] = valueOrDash(values.Bridge)
		summary.Configured["Bridge policy"] = valueOrDash(effectiveBridgePolicy(values))
		summary.Configured["Uplink"] = valueOrDash(values.Uplink)
	} else {
		summary.Configured["Interface"] = valueOrDash(values.Interface)
	}
	summary.Configured["IPv4"] = valueOrDash(values.IP4)
	summary.Configured["IPv6"] = valueOrDash(values.IP6)
	summary.Configured["Default router"] = valueOrDash(values.DefaultRouter)
	summary.Configured["Hostname"] = valueOrDash(firstNonEmpty(strings.TrimSpace(values.Hostname), strings.TrimSpace(detail.JailConfValues["host.hostname"]), strings.TrimSpace(detail.Hostname)))

	state := "stopped"
	if detail.JID > 0 {
		state = "running"
	}
	summary.Runtime["State"] = state
	summary.Runtime["JID"] = valueOrDash(firstNonEmpty(detail.RuntimeValues["JID"], intString(detail.JID)))
	summary.Runtime["Network mode"] = valueOrDash(detail.RuntimeValues["Network mode"])
	summary.Runtime["Interface"] = valueOrDash(detail.RuntimeValues["Interface"])
	summary.Runtime["IPv4"] = valueOrDash(detail.RuntimeValues["IPv4"])
	summary.Runtime["IPv6"] = valueOrDash(detail.RuntimeValues["IPv6"])
	summary.Runtime["Live hostname"] = valueOrDash(firstNonEmpty(detail.RuntimeValues["Live hostname"], detail.Hostname))

	prereqs := collectNetworkWizardPrereqs(values)
	summary.Validation = append(summary.Validation, networkWizardPrereqLines(prereqs)...)
	return summary
}

func detailNetworkWizardValues(detail JailDetail) jailWizardValues {
	values := jailWizardValues{
		Interface: firstNonEmpty(strings.TrimSpace(detail.JailConfValues["interface"]), strings.TrimSpace(detail.RuntimeValues["Interface"])),
		IP4:       firstNonEmpty(strings.TrimSpace(detail.JailConfValues["ip4.addr"]), strings.TrimSpace(detail.JailConfValues["ip4"])),
		IP6:       firstNonEmpty(strings.TrimSpace(detail.JailConfValues["ip6.addr"]), strings.TrimSpace(detail.JailConfValues["ip6"])),
		Hostname:  firstNonEmpty(strings.TrimSpace(detail.JailConfValues["host.hostname"]), strings.TrimSpace(detail.Hostname)),
	}
	metadata := parseTUIMetadata(detail.JailConfRaw)
	if detailLooksLikeLinuxJail(detail) {
		values.JailType = "linux"
	} else if containsFlag(detail.JailConfFlags, "vnet") {
		values.JailType = "vnet"
	} else {
		values.JailType = "thick"
	}
	if values.JailType == "vnet" {
		values.Bridge = firstNonEmpty(metadata["bridge"], parseBridgeFromRaw(detail.JailConfRaw))
		values.BridgePolicy = firstNonEmpty(metadata["bridge_policy"], "auto-create")
		values.Uplink = firstNonEmpty(metadata["uplink"], parseUplinkFromRaw(detail.JailConfRaw, values.Bridge))
		values.DefaultRouter = firstNonEmpty(metadata["default_router"], parseDefaultRouterFromRaw(detail.JailConfRaw))
	} else {
		values.DefaultRouter = strings.TrimSpace(detail.JailConfValues["defaultrouter"])
	}
	return values
}

func parseTUIMetadata(lines []string) map[string]string {
	values := map[string]string{}
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		idx := strings.Index(line, "freebsd-jails-tui:")
		if idx < 0 {
			continue
		}
		payload := strings.TrimSpace(strings.TrimSuffix(line[idx+len("freebsd-jails-tui:"):], ";"))
		for _, field := range strings.Fields(payload) {
			key, value, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	return values
}

func parseBridgeFromRaw(lines []string) string {
	for _, raw := range lines {
		match := vnetBridgePattern.FindStringSubmatch(raw)
		if len(match) == 2 {
			return match[1]
		}
	}
	return ""
}

func parseUplinkFromRaw(lines []string, bridge string) string {
	bridge = strings.TrimSpace(bridge)
	for _, raw := range lines {
		match := vnetUplinkPattern.FindStringSubmatch(raw)
		if len(match) != 3 {
			continue
		}
		if bridge != "" && match[1] != bridge {
			continue
		}
		if strings.HasPrefix(match[2], "epair") {
			continue
		}
		return match[2]
	}
	return ""
}

func parseDefaultRouterFromRaw(lines []string) string {
	for _, raw := range lines {
		match := vnetRoutePattern.FindStringSubmatch(raw)
		if len(match) == 2 {
			return match[1]
		}
	}
	return ""
}

func containsFlag(flags []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, flag := range flags {
		if strings.TrimSpace(flag) == target {
			return true
		}
	}
	return false
}

func orderedNetworkSummaryKeys(values map[string]string) []string {
	order := []string{
		"Type",
		"Bridge",
		"Bridge policy",
		"Uplink",
		"Interface",
		"IPv4",
		"IPv6",
		"Default router",
		"Hostname",
		"State",
		"JID",
		"Network mode",
		"Live hostname",
	}
	keys := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, key := range order {
		if _, ok := values[key]; ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}
	for _, key := range sortedKeys(values) {
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func intString(value int) string {
	if value <= 0 {
		return ""
	}
	return fmt.Sprintf("%d", value)
}
