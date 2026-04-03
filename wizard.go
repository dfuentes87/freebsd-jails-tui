package main

import (
	"fmt"
	"hash/fnv"
	"net/netip"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	jailNamePattern    = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	memoryLimitPattern = regexp.MustCompile(`^[0-9]+[KMGTP]?$`)
)

type wizardField struct {
	ID          string
	Label       string
	Placeholder string
	Help        string
}

type wizardStep struct {
	Title       string
	Description string
	Fields      []wizardField
}

type wizardTemplateMode int

const (
	wizardTemplateModeNone wizardTemplateMode = iota
	wizardTemplateModeSave
	wizardTemplateModeLoad
)

type wizardTemplate struct {
	Name   string           `json:"name"`
	Values jailWizardValues `json:"values"`
}

type userlandOption struct {
	Label string
	Value string
}

type templateDatasetOption struct {
	Label string
	Value string
}

var wizardBaseSteps = []wizardStep{
	{
		Title:       "0. Jail Type",
		Description: "Select the jail type to create.",
		Fields: []wizardField{
			{ID: "jail_type", Label: "Type", Placeholder: "thick", Help: "Options: thick, thin, vnet, linux"},
		},
	},
	{
		Title:       "1-5. Configuration",
		Description: "Fill in name, destination, release/template, networking, limits, and mounts on this page.",
		Fields: []wizardField{
			{ID: "name", Label: "Jail name", Placeholder: "web01", Help: "Allowed: letters, numbers, ., _, -"},
			{ID: "dataset", Label: "Destination", Placeholder: "/usr/local/jails/containers/web01", Help: "Use full destination path where jail root will be created"},
			{ID: "template_release", Label: "Template/Release", Placeholder: "15.0-RELEASE", Help: "Local path, release tag, or custom https URL (downloads supported)"},
			{ID: "interface", Label: "Interface", Placeholder: "em0", Help: "Jail interface name for thick, thin, and linux jails"},
			{ID: "bridge", Label: "Bridge", Placeholder: "bridge0", Help: "Required for vnet jails"},
			{ID: "bridge_policy", Label: "Bridge policy", Placeholder: "auto-create", Help: "Options: auto-create or require-existing"},
			{ID: "uplink", Label: "Uplink", Placeholder: "em0", Help: "Optional host uplink to attach to the bridge before jail start"},
			{ID: "ip4", Label: "IPv4", Placeholder: "192.168.1.20/24", Help: "CIDR or 'inherit' (inherit only for non-vnet)"},
			{ID: "ip6", Label: "IPv6", Placeholder: "2001:db8::10/64", Help: "CIDR or 'inherit' (inherit only for non-vnet)"},
			{ID: "default_router", Label: "Default router", Placeholder: "192.168.1.1", Help: "Optional"},
			{ID: "hostname", Label: "Hostname", Placeholder: "web01.example.internal", Help: "Optional, defaults to jail name"},
			{ID: "cpu_percent", Label: "CPU %", Placeholder: "50", Help: ""},
			{ID: "memory_limit", Label: "Memory", Placeholder: "2G"},
			{ID: "process_limit", Label: "Max processes", Placeholder: "512", Help: ""},
			{ID: "mount_points", Label: "Mount points (optional)", Placeholder: "/data,/logs", Help: "Example: /mnt/shared,/var/cache/pkg"},
		},
	},
	{
		Title:       "6. Linux Bootstrap",
		Description: "Choose the Linux distro and release to bootstrap inside /compat when creating a linux jail.",
		Fields: []wizardField{
			{ID: "linux_distro", Label: "Linux distro", Placeholder: "ubuntu", Help: "Supported: ubuntu or debian"},
			{ID: "linux_release", Label: "Linux release", Placeholder: "jammy", Help: "Ubuntu codename or Debian suite"},
			{ID: "linux_bootstrap", Label: "Bootstrap mode", Placeholder: "auto", Help: "Options: auto or skip"},
		},
	},
	{
		Title:       "7. Confirmation",
		Description: "Review the generated jail.conf and creation plan.",
	},
}

type jailWizardValues struct {
	JailType        string
	Name            string
	Dataset         string
	TemplateRelease string
	Interface       string
	Bridge          string
	BridgePolicy    string
	Uplink          string
	IP4             string
	IP6             string
	DefaultRouter   string
	Hostname        string
	LinuxDistro     string
	LinuxRelease    string
	LinuxBootstrap  string
	CPUPercent      string
	MemoryLimit     string
	ProcessLimit    string
	MountPoints     string
}

type mountPointSpec struct {
	Source string
	Target string
}

type jailCreationWizard struct {
	step                 int
	field                int
	values               jailWizardValues
	linuxPrereqs         LinuxWizardPrereqs
	linuxPrereqKey       string
	linuxPrereqCached    bool
	networkPrereqs       NetworkWizardPrereqs
	networkPrereqKey     string
	networkPrereqCached  bool
	templateMode         wizardTemplateMode
	templateInput        string
	templates            []wizardTemplate
	templateCursor       int
	userlandMode         bool
	userlandOpts         []userlandOption
	userlandCursor       int
	thinDatasetMode      bool
	thinDatasetOpts      []templateDatasetOption
	thinDatasetCursor    int
	datasetCreateRunning bool
	message              string
	executionLogs        []string
	executionError       string
}

func newJailCreationWizard(defaultDestination string) jailCreationWizard {
	w := jailCreationWizard{
		values: jailWizardValues{
			Dataset:   strings.TrimSpace(defaultDestination),
			Interface: "em0",
		},
	}
	w.refreshLinuxPrereqs()
	w.refreshNetworkPrereqs()
	return w
}

func (w jailCreationWizard) steps() []wizardStep {
	if normalizedJailType(w.values.JailType) == "linux" {
		return wizardBaseSteps
	}
	return []wizardStep{wizardBaseSteps[0], wizardBaseSteps[1], wizardBaseSteps[3]}
}

func (w jailCreationWizard) currentStep() wizardStep {
	steps := w.steps()
	if len(steps) == 0 {
		return wizardStep{}
	}
	if w.step < 0 || w.step >= len(steps) {
		return steps[0]
	}
	return steps[w.step]
}

func (w jailCreationWizard) isConfirmationStep() bool {
	return w.step == len(w.steps())-1
}

func (w *jailCreationWizard) nextField() {
	fields := w.visibleFields()
	if len(fields) == 0 {
		return
	}
	w.field++
	if w.field >= len(fields) {
		w.field = 0
	}
}

func (w *jailCreationWizard) prevField() {
	fields := w.visibleFields()
	if len(fields) == 0 {
		return
	}
	w.field--
	if w.field < 0 {
		w.field = len(fields) - 1
	}
}

func (w *jailCreationWizard) nextStep() error {
	if err := w.validateCurrentStep(); err != nil {
		w.message = err.Error()
		return err
	}
	if w.step < len(w.steps())-1 {
		w.step++
		w.field = 0
		w.message = ""
		w.executionLogs = nil
		w.executionError = ""
	}
	w.refreshLinuxPrereqs()
	w.refreshNetworkPrereqs()
	return nil
}

func (w *jailCreationWizard) prevStep() {
	if w.step > 0 {
		w.step--
		w.field = 0
		w.message = ""
		w.executionLogs = nil
		w.executionError = ""
	}
	w.refreshLinuxPrereqs()
	w.refreshNetworkPrereqs()
}

func (w *jailCreationWizard) beginTemplateSave() {
	w.templateMode = wizardTemplateModeSave
	w.userlandMode = false
	w.thinDatasetMode = false
	if strings.TrimSpace(w.templateInput) == "" {
		w.templateInput = strings.TrimSpace(w.values.Name)
	}
	w.message = ""
}

func (w *jailCreationWizard) beginTemplateLoad() error {
	templates, err := loadWizardTemplates()
	if err != nil {
		return err
	}
	w.userlandMode = false
	w.thinDatasetMode = false
	w.templates = templates
	w.templateCursor = 0
	w.templateMode = wizardTemplateModeLoad
	w.templateInput = ""
	w.message = ""
	return nil
}

func (w *jailCreationWizard) endTemplateMode() {
	w.templateMode = wizardTemplateModeNone
	w.templateInput = ""
	w.boundTemplateCursor()
}

func (w *jailCreationWizard) beginUserlandSelect() error {
	options, err := discoverWizardUserlandOptions()
	if err != nil {
		return err
	}
	if len(options) == 0 {
		return fmt.Errorf("no userland entries found in %s", defaultUserlandDir)
	}
	w.templateMode = wizardTemplateModeNone
	w.userlandMode = true
	w.thinDatasetMode = false
	w.userlandOpts = options
	w.userlandCursor = 0
	w.message = ""
	return nil
}

func (w *jailCreationWizard) endUserlandSelect() {
	w.userlandMode = false
	w.userlandOpts = nil
	w.userlandCursor = 0
}

func (w *jailCreationWizard) beginThinDatasetSelect() error {
	options, err := discoverThinTemplateDatasets()
	if err != nil {
		return err
	}
	w.templateMode = wizardTemplateModeNone
	w.userlandMode = false
	w.thinDatasetMode = true
	w.thinDatasetOpts = options
	w.thinDatasetCursor = 0
	w.datasetCreateRunning = false
	w.message = ""
	return nil
}

func (w *jailCreationWizard) endThinDatasetSelect() {
	w.thinDatasetMode = false
	w.thinDatasetOpts = nil
	w.thinDatasetCursor = 0
	w.datasetCreateRunning = false
}

func (w *jailCreationWizard) boundUserlandCursor() {
	if len(w.userlandOpts) == 0 {
		w.userlandCursor = 0
		return
	}
	if w.userlandCursor < 0 {
		w.userlandCursor = 0
	}
	if w.userlandCursor >= len(w.userlandOpts) {
		w.userlandCursor = len(w.userlandOpts) - 1
	}
}

func (w *jailCreationWizard) selectedUserlandOption() (userlandOption, bool) {
	if len(w.userlandOpts) == 0 {
		return userlandOption{}, false
	}
	w.boundUserlandCursor()
	return w.userlandOpts[w.userlandCursor], true
}

func (w *jailCreationWizard) boundThinDatasetCursor() {
	if len(w.thinDatasetOpts) == 0 {
		w.thinDatasetCursor = 0
		return
	}
	if w.thinDatasetCursor < 0 {
		w.thinDatasetCursor = 0
	}
	if w.thinDatasetCursor >= len(w.thinDatasetOpts) {
		w.thinDatasetCursor = len(w.thinDatasetOpts) - 1
	}
}

func (w *jailCreationWizard) selectedThinDatasetOption() (templateDatasetOption, bool) {
	if len(w.thinDatasetOpts) == 0 {
		return templateDatasetOption{}, false
	}
	w.boundThinDatasetCursor()
	return w.thinDatasetOpts[w.thinDatasetCursor], true
}

func (w *jailCreationWizard) selectedTemplate() (wizardTemplate, bool) {
	if len(w.templates) == 0 {
		return wizardTemplate{}, false
	}
	w.boundTemplateCursor()
	return w.templates[w.templateCursor], true
}

func (w *jailCreationWizard) boundTemplateCursor() {
	if len(w.templates) == 0 {
		w.templateCursor = 0
		return
	}
	if w.templateCursor < 0 {
		w.templateCursor = 0
	}
	if w.templateCursor >= len(w.templates) {
		w.templateCursor = len(w.templates) - 1
	}
}

func (w *jailCreationWizard) appendTemplateInput(input string) {
	if input == "" {
		return
	}
	w.templateInput += input
	w.message = ""
}

func (w *jailCreationWizard) backspaceTemplateInput() {
	runes := []rune(w.templateInput)
	if len(runes) == 0 {
		return
	}
	w.templateInput = string(runes[:len(runes)-1])
	w.message = ""
}

func (w *jailCreationWizard) appendToActive(input string) {
	field, ok := w.activeField()
	if !ok {
		return
	}
	ref := w.valueRef(field.ID)
	if ref == nil {
		return
	}
	*ref += input
	w.message = ""
	w.refreshLinuxPrereqs()
	w.refreshNetworkPrereqs()
}

func (w *jailCreationWizard) backspaceActive() {
	field, ok := w.activeField()
	if !ok {
		return
	}
	ref := w.valueRef(field.ID)
	if ref == nil {
		return
	}
	runes := []rune(*ref)
	if len(runes) == 0 {
		return
	}
	*ref = string(runes[:len(runes)-1])
	w.message = ""
	w.refreshLinuxPrereqs()
	w.refreshNetworkPrereqs()
}

func (w jailCreationWizard) activeField() (wizardField, bool) {
	fields := w.visibleFields()
	if len(fields) == 0 {
		return wizardField{}, false
	}
	idx := w.field
	if idx < 0 {
		idx = 0
	}
	if idx >= len(fields) {
		idx = len(fields) - 1
	}
	return fields[idx], true
}

func (w *jailCreationWizard) normalizeField() {
	fields := w.visibleFields()
	if len(fields) == 0 {
		w.field = 0
		return
	}
	if w.field < 0 {
		w.field = 0
	}
	if w.field >= len(fields) {
		w.field = len(fields) - 1
	}
}

func (w *jailCreationWizard) refreshLinuxPrereqs() {
	key := strings.Join([]string{
		normalizedJailType(w.values.JailType),
		effectiveLinuxDistro(w.values),
		effectiveLinuxRelease(w.values),
		effectiveLinuxBootstrapMode(w.values),
	}, "|")
	if w.linuxPrereqCached && w.linuxPrereqKey == key {
		return
	}
	w.linuxPrereqs = collectLinuxWizardPrereqs(w.values)
	w.linuxPrereqKey = key
	w.linuxPrereqCached = true
}

func (w *jailCreationWizard) refreshNetworkPrereqs() {
	key := strings.Join([]string{
		normalizedJailType(w.values.JailType),
		strings.TrimSpace(w.values.Interface),
		strings.TrimSpace(w.values.Bridge),
		effectiveBridgePolicy(w.values),
		strings.TrimSpace(w.values.Uplink),
		strings.TrimSpace(w.values.IP4),
		strings.TrimSpace(w.values.IP6),
		strings.TrimSpace(w.values.DefaultRouter),
	}, "|")
	if w.networkPrereqCached && w.networkPrereqKey == key {
		return
	}
	w.networkPrereqs = collectNetworkWizardPrereqs(w.values)
	w.networkPrereqKey = key
	w.networkPrereqCached = true
}

func (w *jailCreationWizard) normalizeStep() {
	steps := w.steps()
	if len(steps) == 0 {
		w.step = 0
		w.field = 0
		return
	}
	if w.step < 0 {
		w.step = 0
	}
	if w.step >= len(steps) {
		w.step = len(steps) - 1
	}
	w.normalizeField()
}

func (w jailCreationWizard) visibleFields() []wizardField {
	step := w.currentStep()
	jailType := normalizedJailType(w.values.JailType)
	fields := make([]wizardField, 0, len(step.Fields))
	for _, field := range step.Fields {
		switch field.ID {
		case "interface":
			if jailType == "vnet" {
				continue
			}
		case "bridge", "bridge_policy", "uplink":
			if jailType != "vnet" {
				continue
			}
		case "linux_distro", "linux_release":
			if jailType != "linux" {
				continue
			}
		case "linux_bootstrap":
			if jailType != "linux" {
				continue
			}
		}
		fields = append(fields, field)
	}
	return fields
}

func (w *jailCreationWizard) valueRef(id string) *string {
	switch id {
	case "jail_type":
		return &w.values.JailType
	case "name":
		return &w.values.Name
	case "dataset":
		return &w.values.Dataset
	case "template_release":
		return &w.values.TemplateRelease
	case "interface":
		return &w.values.Interface
	case "bridge":
		return &w.values.Bridge
	case "bridge_policy":
		return &w.values.BridgePolicy
	case "uplink":
		return &w.values.Uplink
	case "ip4":
		return &w.values.IP4
	case "ip6":
		return &w.values.IP6
	case "default_router":
		return &w.values.DefaultRouter
	case "hostname":
		return &w.values.Hostname
	case "linux_distro":
		return &w.values.LinuxDistro
	case "linux_release":
		return &w.values.LinuxRelease
	case "linux_bootstrap":
		return &w.values.LinuxBootstrap
	case "cpu_percent":
		return &w.values.CPUPercent
	case "memory_limit":
		return &w.values.MemoryLimit
	case "process_limit":
		return &w.values.ProcessLimit
	case "mount_points":
		return &w.values.MountPoints
	default:
		return nil
	}
}

func (w jailCreationWizard) valueByID(id string) string {
	switch id {
	case "jail_type":
		return w.values.JailType
	case "name":
		return w.values.Name
	case "dataset":
		return w.values.Dataset
	case "template_release":
		return w.values.TemplateRelease
	case "interface":
		return w.values.Interface
	case "bridge":
		return w.values.Bridge
	case "bridge_policy":
		return w.values.BridgePolicy
	case "uplink":
		return w.values.Uplink
	case "ip4":
		return w.values.IP4
	case "ip6":
		return w.values.IP6
	case "default_router":
		return w.values.DefaultRouter
	case "hostname":
		return w.values.Hostname
	case "linux_distro":
		return w.values.LinuxDistro
	case "linux_release":
		return w.values.LinuxRelease
	case "linux_bootstrap":
		return w.values.LinuxBootstrap
	case "cpu_percent":
		return w.values.CPUPercent
	case "memory_limit":
		return w.values.MemoryLimit
	case "process_limit":
		return w.values.ProcessLimit
	case "mount_points":
		return w.values.MountPoints
	default:
		return ""
	}
}

func (w jailCreationWizard) validateCurrentStep() error {
	if w.isConfirmationStep() {
		return nil
	}
	jailType := strings.ToLower(strings.TrimSpace(w.values.JailType))
	if jailType == "" {
		return fmt.Errorf("jail type is required (thick, thin, vnet, linux)")
	}
	switch jailType {
	case "thick", "thin", "vnet", "linux":
	default:
		return fmt.Errorf("jail type must be one of: thick, thin, vnet, linux")
	}
	w.values.JailType = jailType

	if w.step == 0 {
		return nil
	}
	if strings.TrimSpace(w.values.Name) == "" {
		return fmt.Errorf("jail name is required")
	}
	if !jailNamePattern.MatchString(strings.TrimSpace(w.values.Name)) {
		return fmt.Errorf("invalid jail name")
	}
	if strings.TrimSpace(w.values.Dataset) == "" {
		return fmt.Errorf("destination is required: enter full path like /usr/local/jails/containers/%s", strings.TrimSpace(w.values.Name))
	}
	if _, err := validateJailDestinationPath(w.values.Dataset, w.values.Name); err != nil {
		if strings.Contains(err.Error(), "is required") {
			return fmt.Errorf("destination is required: enter full path like /usr/local/jails/containers/%s", strings.TrimSpace(w.values.Name))
		}
		return err
	}
	if strings.TrimSpace(w.values.TemplateRelease) == "" {
		return fmt.Errorf("template/release is required (local path, release tag, or https URL)")
	}
	if err := validateTemplateReleaseInput(w.values); err != nil {
		return err
	}
	if hasConflictingJailConfig(w.values.Name) {
		return fmt.Errorf("config already exists: %s", jailConfigPathForName(w.values.Name))
	}
	if jailType == "vnet" {
		bridge, err := validateNetworkInterfaceName(w.values.Bridge, "bridge")
		if err != nil {
			return err
		}
		w.values.Bridge = bridge
		if strings.TrimSpace(w.values.Bridge) == "" {
			return fmt.Errorf("bridge is required for vnet jails")
		}
		if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(w.values.Bridge)), "bridge") {
			return fmt.Errorf("vnet jails require a bridge such as bridge0")
		}
		policy := effectiveBridgePolicy(w.values)
		switch policy {
		case "auto-create", "require-existing":
			w.values.BridgePolicy = policy
		default:
			return fmt.Errorf("bridge policy must be auto-create or require-existing")
		}
		if strings.TrimSpace(w.values.Uplink) != "" {
			uplink, err := validateOptionalNetworkInterfaceName(w.values.Uplink, "uplink")
			if err != nil {
				return err
			}
			w.values.Uplink = uplink
		}
	} else {
		iface, err := validateNetworkInterfaceName(w.values.Interface, "interface")
		if err != nil {
			return err
		}
		w.values.Interface = iface
		if strings.TrimSpace(w.values.Interface) == "" {
			return fmt.Errorf("interface is required")
		}
	}
	if strings.TrimSpace(w.values.IP4) == "" {
		return fmt.Errorf("IPv4 is required")
	}
	if err := validateJailIPValue(strings.TrimSpace(w.values.IP4), true, "IPv4", jailType != "vnet"); err != nil {
		return err
	}
	if err := validateJailIPValue(strings.TrimSpace(w.values.IP6), false, "IPv6", jailType != "vnet"); err != nil {
		return err
	}
	if jailType == "vnet" {
		if strings.EqualFold(strings.TrimSpace(w.values.IP4), "inherit") {
			return fmt.Errorf("vnet jails cannot use IPv4 inherit; switch jail type or enter an explicit IPv4 address")
		}
		if strings.EqualFold(strings.TrimSpace(w.values.IP6), "inherit") {
			return fmt.Errorf("vnet jails cannot use IPv6 inherit; switch jail type or enter an explicit IPv6 address")
		}
	}
	if value := strings.TrimSpace(w.values.DefaultRouter); value != "" {
		if _, err := netip.ParseAddr(value); err != nil {
			return fmt.Errorf("default router must be a valid IPv4 or IPv6 address")
		}
	}
	if value := strings.TrimSpace(w.values.CPUPercent); value != "" {
		cpu, err := strconv.Atoi(value)
		if err != nil || cpu <= 0 || cpu > 100 {
			return fmt.Errorf("CPU %% must be between 1 and 100")
		}
	}
	if value := strings.TrimSpace(w.values.MemoryLimit); value != "" {
		if !memoryLimitPattern.MatchString(strings.ToUpper(value)) {
			return fmt.Errorf("memory must look like 512M or 2G")
		}
	}
	if value := strings.TrimSpace(w.values.ProcessLimit); value != "" {
		procs, err := strconv.Atoi(value)
		if err != nil || procs <= 0 {
			return fmt.Errorf("max processes must be a positive integer")
		}
	}
	if err := validateMountPointInput(w.values.MountPoints); err != nil {
		return err
	}
	w.refreshNetworkPrereqs()
	if err := w.networkPrereqs.blockingError(); err != nil {
		return err
	}
	if jailType == "linux" {
		distro := strings.ToLower(strings.TrimSpace(w.values.LinuxDistro))
		switch distro {
		case "", "ubuntu", "debian":
		default:
			return fmt.Errorf("linux distro must be ubuntu or debian")
		}
		mode := effectiveLinuxBootstrapMode(w.values)
		switch mode {
		case "auto", "skip":
		default:
			return fmt.Errorf("bootstrap mode must be auto or skip")
		}
	}
	return nil
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
			cleanSource, err := validateAccessibleAbsolutePath(source, "mount source")
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
	steps := w.steps()
	for idx := 0; idx < len(steps)-1; idx++ {
		test := w
		test.step = idx
		test.normalizeField()
		if err := test.validateCurrentStep(); err != nil {
			return err
		}
	}
	return nil
}

func (w jailCreationWizard) summaryLines() []string {
	lines := []string{
		fmt.Sprintf("Type: %s", valueOrDash(w.values.JailType)),
		fmt.Sprintf("Name: %s", w.values.Name),
		fmt.Sprintf("Destination: %s", w.values.Dataset),
		fmt.Sprintf("Template/Release: %s", w.values.TemplateRelease),
		fmt.Sprintf("IPv4: %s", w.values.IP4),
		fmt.Sprintf("IPv6: %s", valueOrDash(w.values.IP6)),
		fmt.Sprintf("Default router: %s", valueOrDash(w.values.DefaultRouter)),
		fmt.Sprintf("Hostname: %s", valueOrDash(w.values.Hostname)),
		fmt.Sprintf("CPU %%: %s", valueOrDash(w.values.CPUPercent)),
		fmt.Sprintf("Memory limit: %s", valueOrDash(w.values.MemoryLimit)),
		fmt.Sprintf("Process limit: %s", valueOrDash(w.values.ProcessLimit)),
	}
	if normalizedJailType(w.values.JailType) == "vnet" {
		lines = append(lines,
			fmt.Sprintf("Bridge: %s", valueOrDash(w.values.Bridge)),
			fmt.Sprintf("Bridge policy: %s", effectiveBridgePolicy(w.values)),
			fmt.Sprintf("Uplink: %s", valueOrDash(w.values.Uplink)),
		)
	} else {
		lines = append(lines, fmt.Sprintf("Interface: %s", w.values.Interface))
	}
	if normalizedJailType(w.values.JailType) == "linux" {
		lines = append(lines,
			fmt.Sprintf("Linux distro: %s", effectiveLinuxDistro(w.values)),
			fmt.Sprintf("Linux release: %s", effectiveLinuxRelease(w.values)),
			fmt.Sprintf("Bootstrap mode: %s", effectiveLinuxBootstrapMode(w.values)),
		)
	}
	mounts := w.mountPointList()
	if len(mounts) == 0 {
		lines = append(lines, "Mount points: -")
	} else {
		lines = append(lines, "Mount points:")
		for _, mount := range mounts {
			lines = append(lines, "  - "+mount)
		}
	}
	return lines
}

func (w jailCreationWizard) jailConfPreviewLines() []string {
	jailPath := defaultJailPathForValues(w.values)
	fstabPath := ""
	for _, spec := range parseMountPointSpecs(w.values.MountPoints) {
		if spec.Source != "" {
			fstabPath = jailFstabPathForName(w.values.Name)
			break
		}
	}
	return buildJailConfBlock(w.values, jailPath, fstabPath)
}

func (w jailCreationWizard) commandPlanLines() []string {
	destination := strings.TrimSpace(w.values.Dataset)
	jailType := normalizedJailType(w.values.JailType)
	lines := []string{}
	step := 1
	addStep := func(title string) {
		lines = append(lines, fmt.Sprintf("%d. %s", step, title))
		step++
	}
	addDetail := func(text string) {
		lines = append(lines, text)
	}
	switch jailType {
	case "thin":
		addStep("Prepare thin-jail destination from a ZFS template dataset:")
		addDetail(fmt.Sprintf("   # source template: %s", w.values.TemplateRelease))
		addDetail(fmt.Sprintf("   # destination: %s", destination))
		addDetail("   zfs snapshot <template-dataset>@freebsd-jails-tui-base")
		addDetail(fmt.Sprintf("   zfs clone <template-dataset>@freebsd-jails-tui-base <parent-dataset>/%s", strings.TrimSpace(w.values.Name)))
	case "linux":
		addStep("Ensure Linux ABI is enabled on the host:")
		addDetail("   sysrc linux_enable=YES")
		addDetail("   service linux start")
		addStep("Ensure destination path exists:")
		addDetail(fmt.Sprintf("   mkdir -p %s", destination))
		addStep("Provision jail root from selected template/release:")
		addDetail(fmt.Sprintf("   # source: %s", w.values.TemplateRelease))
	default:
		addStep("Ensure destination path exists:")
		addDetail(fmt.Sprintf("   mkdir -p %s", destination))
		addStep("Provision jail root from selected template/release:")
		addDetail(fmt.Sprintf("   # source: %s", w.values.TemplateRelease))
	}

	switch jailType {
	case "vnet":
		addStep("Ensure VNET host bridge setup is ready:")
		addDetail(fmt.Sprintf("   # bridge policy: %s", effectiveBridgePolicy(w.values)))
		if w.networkPrereqs.BridgeCreateNeeded {
			addDetail(fmt.Sprintf("   ifconfig %s create", strings.TrimSpace(w.values.Bridge)))
		}
		addDetail(fmt.Sprintf("   ifconfig %s up", strings.TrimSpace(w.values.Bridge)))
		if w.networkPrereqs.UplinkAttachNeeded && strings.TrimSpace(w.values.Uplink) != "" {
			addDetail(fmt.Sprintf("   ifconfig %s addm %s up", strings.TrimSpace(w.values.Bridge), strings.TrimSpace(w.values.Uplink)))
		}
		addStep(fmt.Sprintf("VNET jail hooks: create %s and attach it to %s", vnetEpairName(w.values.Name), strings.TrimSpace(w.values.Bridge)))
		addStep(fmt.Sprintf("VNET start config: assign %s inside the jail", strings.TrimSpace(w.values.IP4)))
	case "linux":
		addStep(fmt.Sprintf("Linux compatibility mounts are configured under %s", linuxCompatRoot(destination, w.values)))
		if effectiveLinuxBootstrapMode(w.values) == "skip" {
			addStep("Skip Linux bootstrap for now.")
			addDetail("   # use detail view action 'b' later after networking is ready")
		} else {
			addStep("Preflight Linux bootstrap networking inside the jail:")
			addDetail("   jexec <jail> route -n get -inet default || route -n get -inet6 default")
			addDetail(fmt.Sprintf("   jexec <jail> getent hosts %s  # confirm A/AAAA answers for usable route families", linuxMirrorHost(w.values)))
			addDetail(fmt.Sprintf("   jexec <jail> fetch -4/-6 -qo /dev/null %s", linuxPreflightURL(w.values)))
			addStep("Bootstrap Linux userland inside the jail:")
			addDetail("   jexec <jail> pkg bootstrap -f")
			addDetail("   jexec <jail> pkg install -y debootstrap")
			addDetail(fmt.Sprintf("   jexec <jail> debootstrap %s /compat/%s %s", effectiveLinuxRelease(w.values), effectiveLinuxDistro(w.values), linuxMirrorURL(w.values)))
		}
	}

	addStep("Write jail config: " + jailConfigPathForName(w.values.Name))
	addStep(fmt.Sprintf("Start jail: service jail start %s", w.values.Name))

	if strings.TrimSpace(w.values.CPUPercent) != "" ||
		strings.TrimSpace(w.values.MemoryLimit) != "" ||
		strings.TrimSpace(w.values.ProcessLimit) != "" {
		addStep("Apply rctl limits:")
		if strings.TrimSpace(w.values.CPUPercent) != "" {
			addDetail(fmt.Sprintf("   rctl -a jail:%s:pcpu:deny=%s", w.values.Name, w.values.CPUPercent))
		}
		if strings.TrimSpace(w.values.MemoryLimit) != "" {
			addDetail(fmt.Sprintf("   rctl -a jail:%s:memoryuse:deny=%s", w.values.Name, strings.ToUpper(w.values.MemoryLimit)))
		}
		if strings.TrimSpace(w.values.ProcessLimit) != "" {
			addDetail(fmt.Sprintf("   rctl -a jail:%s:maxproc:deny=%s", w.values.Name, w.values.ProcessLimit))
		}
	}

	mounts := w.mountPointList()
	if len(mounts) > 0 {
		addStep("Configure mount points:")
		for _, mount := range mounts {
			addDetail("   mountpoint: " + mount)
		}
	}
	return lines
}

func (w jailCreationWizard) mountPointList() []string {
	specs := parseMountPointSpecs(w.values.MountPoints)
	var mounts []string
	for _, spec := range specs {
		if spec.Source == "" {
			if spec.Target == "" {
				continue
			}
			mounts = append(mounts, spec.Target)
			continue
		}
		mounts = append(mounts, spec.Source+":"+spec.Target)
	}
	return mounts
}

func (w *jailCreationWizard) clearExecutionResult() {
	w.executionLogs = nil
	w.executionError = ""
}

func (w *jailCreationWizard) setExecutionResult(result JailCreationResult) {
	w.executionLogs = append([]string(nil), result.Logs...)
	if result.Err != nil {
		w.executionError = result.Err.Error()
		w.message = "Creation failed. Review execution output and adjust values."
		return
	}
	w.executionError = ""
	if len(result.Warnings) > 0 {
		w.message = "Creation completed with warnings."
		return
	}
	w.message = "Creation completed successfully."
}

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

func normalizeMountTarget(target string) string {
	clean, err := validateMountTarget(target)
	if err != nil {
		return ""
	}
	return clean
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
		"  exec.consolelog = \"/var/log/jail_console_${name}.log\";",
		"  allow.raw_sockets;",
		"  exec.clean;",
		"  mount.devfs;",
		fmt.Sprintf("  host.hostname = %q;", effectiveJailHostname(values)),
		fmt.Sprintf("  path = %q;", jailPath),
	}

	switch jailType {
	case "vnet":
		lines = append(lines, buildVNETJailConfig(values)...)
	case "linux":
		lines = append(lines,
			fmt.Sprintf("  # freebsd-jails-tui: linux_distro=%s linux_release=%s linux_bootstrap=%s;", effectiveLinuxDistro(values), effectiveLinuxRelease(values), effectiveLinuxBootstrapMode(values)),
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

	if strings.TrimSpace(values.DefaultRouter) != "" {
		if jailType != "vnet" {
			lines = append(lines, fmt.Sprintf("  defaultrouter = %q;", strings.TrimSpace(values.DefaultRouter)))
		}
	}
	if strings.TrimSpace(fstabPath) != "" {
		lines = append(lines, fmt.Sprintf("  mount.fstab = %q;", fstabPath))
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

func effectiveLinuxDistro(values jailWizardValues) string {
	distro := strings.ToLower(strings.TrimSpace(values.LinuxDistro))
	if distro == "" {
		return "ubuntu"
	}
	return distro
}

func effectiveBridgePolicy(values jailWizardValues) string {
	policy := strings.ToLower(strings.TrimSpace(values.BridgePolicy))
	if policy == "" {
		return "auto-create"
	}
	return policy
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

func effectiveLinuxBootstrapMode(values jailWizardValues) string {
	mode := strings.ToLower(strings.TrimSpace(values.LinuxBootstrap))
	if mode == "" {
		return "auto"
	}
	return mode
}

func linuxMirrorURL(values jailWizardValues) string {
	switch effectiveLinuxDistro(values) {
	case "debian":
		return "https://deb.debian.org/debian"
	default:
		return "https://archive.ubuntu.com/ubuntu"
	}
}

func linuxMirrorHost(values jailWizardValues) string {
	mirror := linuxMirrorURL(values)
	mirror = strings.TrimPrefix(mirror, "https://")
	mirror = strings.TrimPrefix(mirror, "http://")
	if idx := strings.IndexByte(mirror, '/'); idx >= 0 {
		mirror = mirror[:idx]
	}
	return mirror
}

func linuxPreflightURL(values jailWizardValues) string {
	return strings.TrimRight(linuxMirrorURL(values), "/") + "/dists/" + effectiveLinuxRelease(values) + "/Release"
}

func linuxCompatRoot(jailPath string, values jailWizardValues) string {
	return filepath.Join(jailPath, "compat", effectiveLinuxDistro(values))
}

func vnetEpairName(name string) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(strings.TrimSpace(name)))
	return fmt.Sprintf("epair%d", 100+(hasher.Sum32()%9000))
}

func wizardSectionForField(id string) string {
	switch id {
	case "jail_type":
		return "0. Jail Type"
	case "name", "dataset":
		return "1. Name & Destination"
	case "template_release":
		return "2. Template or release"
	case "interface", "bridge", "bridge_policy", "uplink", "ip4", "ip6", "default_router", "hostname":
		return "3. Networking"
	case "cpu_percent", "memory_limit", "process_limit":
		return "4. Resource Limits (optional)"
	case "mount_points":
		return "5. Mount points"
	case "linux_distro", "linux_release", "linux_bootstrap":
		return "6. Linux Bootstrap"
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

func discoverWizardUserlandOptions() ([]userlandOption, error) {
	sources, err := discoverUserlandSources(defaultUserlandDir)
	if err != nil {
		return nil, err
	}
	options := make([]userlandOption, 0, len(sources)+4)
	for _, source := range sources {
		label := filepath.Base(source)
		parent := filepath.Base(filepath.Dir(source))
		if strings.EqualFold(label, "base.txz") {
			label = parent + "/base.txz"
		}
		options = append(options, userlandOption{
			Label: "local: " + label,
			Value: source,
		})
	}
	// Download options from the official mirror.
	for _, release := range []string{"15.0-RELEASE", "14.2-RELEASE", "13.4-RELEASE"} {
		options = append(options, userlandOption{
			Label: "download: " + release + " (from " + defaultDownloadHost + ")",
			Value: release,
		})
	}
	sort.Slice(options, func(i, j int) bool {
		return strings.ToLower(options[i].Label) < strings.ToLower(options[j].Label)
	})
	return options, nil
}

func discoverThinTemplateDatasets() ([]templateDatasetOption, error) {
	out, err := exec.Command("zfs", "list", "-H", "-o", "name,mountpoint", "-t", "filesystem").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to list ZFS datasets for thin templates: %w", err)
	}

	var options []templateDatasetOption
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			fields = strings.Fields(line)
		}
		if len(fields) < 2 {
			continue
		}
		name := strings.TrimSpace(fields[0])
		mountpoint := strings.TrimSpace(fields[1])
		if name == "" || mountpoint == "" || mountpoint == "-" || mountpoint == "legacy" {
			continue
		}
		lowerName := strings.ToLower(name)
		lowerMount := strings.ToLower(mountpoint)
		if filepath.Base(lowerName) == "templates" || filepath.Base(lowerMount) == "templates" {
			continue
		}
		if !strings.Contains(lowerName, "template") && !strings.Contains(lowerMount, "/templates/") && !strings.Contains(lowerMount, "/template/") {
			continue
		}
		options = append(options, templateDatasetOption{
			Label: fmt.Sprintf("%s -> %s", name, mountpoint),
			Value: mountpoint,
		})
	}
	sort.Slice(options, func(i, j int) bool {
		return strings.ToLower(options[i].Label) < strings.ToLower(options[j].Label)
	})
	return options, nil
}
