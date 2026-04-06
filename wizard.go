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
		Title:       "Create Jail",
		Description: "Set the jail type, identity, root filesystem source, networking, startup behavior, limits, and mounts.",
		Fields: []wizardField{
			{ID: "jail_type", Label: "Type", Placeholder: "thick", Help: "Options: thick, thin, vnet, linux"},
			{ID: "name", Label: "Jail name", Placeholder: "web01", Help: "Allowed: letters, numbers, ., _, -"},
			{ID: "hostname", Label: "Hostname", Placeholder: "web01.example.internal", Help: "Optional, defaults to jail name"},
			{ID: "dataset", Label: "Destination", Placeholder: "/usr/local/jails/containers/web01", Help: "Full jail root path"},
			{ID: "template_release", Label: "Template/Release", Placeholder: "15.0-RELEASE", Help: "Local path, release tag, or custom https URL"},
			{ID: "patch_base", Label: "Patch FreeBSD base", Placeholder: "auto", Help: "auto, yes, or no"},
			{ID: "interface", Label: "Interface", Placeholder: "em0", Help: "Used by thick, thin, and linux"},
			{ID: "bridge", Label: "Bridge", Placeholder: "bridge0", Help: "Required for vnet jails"},
			{ID: "bridge_policy", Label: "Bridge policy", Placeholder: "auto-create", Help: "Options: auto-create or require-existing"},
			{ID: "vnet_host_setup", Label: "Host setup", Placeholder: "runtime", Help: "runtime or persistent"},
			{ID: "uplink", Label: "Uplink", Placeholder: "em0", Help: "Optional host uplink"},
			{ID: "ip4", Label: "IPv4", Placeholder: "192.168.1.20/24", Help: "CIDR or 'inherit' (inherit only for non-vnet)"},
			{ID: "ip6", Label: "IPv6", Placeholder: "2001:db8::10/64", Help: "CIDR or 'inherit' (inherit only for non-vnet)"},
			{ID: "default_router", Label: "Default router", Placeholder: "192.168.1.1", Help: "Optional"},
			{ID: "startup_order", Label: "Startup order", Placeholder: "append", Help: "Optional jail_list position"},
			{ID: "dependencies", Label: "Dependencies", Placeholder: "db01 cache01", Help: "Optional jail names"},
			{ID: "cpu_percent", Label: "CPU %", Placeholder: "50", Help: ""},
			{ID: "memory_limit", Label: "Memory", Placeholder: "2G"},
			{ID: "process_limit", Label: "Max processes", Placeholder: "512", Help: ""},
			{ID: "mount_points", Label: "Mount points (optional)", Placeholder: "/data,/logs", Help: "Example: /mnt/shared,/var/cache/pkg"},
		},
	},
	{
		Title:       "Linux Bootstrap",
		Description: "Choose the bootstrap family, release, and mirror used to populate /compat inside a linux jail.",
		Fields: []wizardField{
			{ID: "linux_distro", Label: "Bootstrap family", Placeholder: "ubuntu", Help: "Free-form family name; Ubuntu and Debian have built-in default mirrors"},
			{ID: "linux_release", Label: "Bootstrap release", Placeholder: "noble", Help: "Free-form codename, suite, or release string passed to debootstrap"},
			{ID: "linux_bootstrap", Label: "Bootstrap mode", Placeholder: "auto", Help: "Options: auto or skip"},
			{ID: "linux_mirror_mode", Label: "Mirror mode", Placeholder: "default", Help: "Options: default or custom"},
			{ID: "linux_mirror_url", Label: "Mirror URL", Placeholder: "https://mirror.example.invalid/repo", Help: "Custom Linux package mirror base URL"},
		},
	},
	{
		Title:       "Confirmation",
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
	VNETHostSetup   string
	Uplink          string
	IP4             string
	IP6             string
	DefaultRouter   string
	Hostname        string
	PatchBase       string
	StartupOrder    string
	Dependencies    string
	LinuxDistro     string
	LinuxRelease    string
	LinuxBootstrap  string
	LinuxMirrorMode string
	LinuxMirrorURL  string
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
	validationField      string
	validationError      string
	message              string
	executionLogs        []string
	executionError       string
}

func (w jailCreationWizard) currentStepHasField(id string) bool {
	for _, field := range w.visibleFields() {
		if field.ID == id {
			return true
		}
	}
	return false
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
	return []wizardStep{wizardBaseSteps[0], wizardBaseSteps[2]}
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
	fieldID, err := w.validateCurrentStepDetailed()
	if err != nil {
		w.applyValidationError(fieldID, err)
		return err
	}
	if w.step < len(w.steps())-1 {
		w.step++
		w.field = 0
		w.clearValidationError()
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
		w.clearValidationError()
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
	w.clearValidationIfFieldMatches(field.ID)
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
	w.clearValidationIfFieldMatches(field.ID)
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
		effectiveLinuxMirrorMode(w.values),
		strings.TrimSpace(w.values.LinuxMirrorURL),
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
		case "patch_base":
			if jailType == "thin" {
				continue
			}
		case "bridge", "bridge_policy", "vnet_host_setup", "uplink":
			if jailType != "vnet" {
				continue
			}
		case "linux_distro", "linux_release", "linux_mirror_mode":
			if jailType != "linux" {
				continue
			}
		case "linux_bootstrap":
			if jailType != "linux" {
				continue
			}
		case "linux_mirror_url":
			if jailType != "linux" {
				continue
			}
			if effectiveLinuxMirrorMode(w.values) != "custom" {
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
	case "vnet_host_setup":
		return &w.values.VNETHostSetup
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
	case "patch_base":
		return &w.values.PatchBase
	case "startup_order":
		return &w.values.StartupOrder
	case "dependencies":
		return &w.values.Dependencies
	case "linux_distro":
		return &w.values.LinuxDistro
	case "linux_release":
		return &w.values.LinuxRelease
	case "linux_bootstrap":
		return &w.values.LinuxBootstrap
	case "linux_mirror_mode":
		return &w.values.LinuxMirrorMode
	case "linux_mirror_url":
		return &w.values.LinuxMirrorURL
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
	case "vnet_host_setup":
		return w.values.VNETHostSetup
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
	case "patch_base":
		return w.values.PatchBase
	case "startup_order":
		return w.values.StartupOrder
	case "dependencies":
		return w.values.Dependencies
	case "linux_distro":
		return w.values.LinuxDistro
	case "linux_release":
		return w.values.LinuxRelease
	case "linux_bootstrap":
		return w.values.LinuxBootstrap
	case "linux_mirror_mode":
		return w.values.LinuxMirrorMode
	case "linux_mirror_url":
		return w.values.LinuxMirrorURL
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
	_, err := w.validateCurrentStepDetailed()
	return err
}

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
		if value := strings.TrimSpace(w.values.CPUPercent); value != "" {
			cpu, err := strconv.Atoi(value)
			if err != nil || cpu <= 0 || cpu > 100 {
				return "cpu_percent", fmt.Errorf("CPU %% must be between 1 and 100")
			}
		}
		if value := strings.TrimSpace(w.values.MemoryLimit); value != "" {
			if !memoryLimitPattern.MatchString(strings.ToUpper(value)) {
				return "memory_limit", fmt.Errorf("memory must look like 512M or 2G")
			}
		}
		if value := strings.TrimSpace(w.values.ProcessLimit); value != "" {
			procs, err := strconv.Atoi(value)
			if err != nil || procs <= 0 {
				return "process_limit", fmt.Errorf("max processes must be a positive integer")
			}
		}
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
	if (w.currentStepHasField("linux_distro") || w.currentStepHasField("linux_release") || w.currentStepHasField("linux_bootstrap") || w.currentStepHasField("linux_mirror_mode") || w.currentStepHasField("linux_mirror_url")) && jailType == "linux" {
		family := strings.ToLower(strings.TrimSpace(w.values.LinuxDistro))
		if family == "" {
			return "linux_distro", fmt.Errorf("bootstrap family is required")
		}
		if !jailNamePattern.MatchString(family) {
			return "linux_distro", fmt.Errorf("bootstrap family must use letters, numbers, dot, underscore, or dash")
		}
		w.values.LinuxDistro = family
		if strings.TrimSpace(w.values.LinuxRelease) == "" {
			return "linux_release", fmt.Errorf("bootstrap release is required")
		}
		mode := effectiveLinuxBootstrapMode(w.values)
		switch mode {
		case "auto", "skip":
		default:
			return "linux_bootstrap", fmt.Errorf("bootstrap mode must be auto or skip")
		}
		mirrorMode := effectiveLinuxMirrorMode(w.values)
		switch mirrorMode {
		case "default", "custom":
		default:
			return "linux_mirror_mode", fmt.Errorf("mirror mode must be default or custom")
		}
		if _, err := resolveLinuxMirror(w.values); err != nil {
			if mirrorMode == "custom" {
				return "linux_mirror_url", err
			}
			return "linux_mirror_mode", err
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

func (w *jailCreationWizard) clearValidationError() {
	w.validationField = ""
	w.validationError = ""
}

func (w *jailCreationWizard) clearValidationIfFieldMatches(fieldID string) {
	if strings.TrimSpace(fieldID) == "" {
		return
	}
	if w.validationField == fieldID {
		w.clearValidationError()
	}
}

func (w *jailCreationWizard) applyValidationError(fieldID string, err error) {
	if err == nil {
		w.message = ""
		w.clearValidationError()
		return
	}
	fieldID = strings.TrimSpace(fieldID)
	if fieldID == "" {
		w.validationField = ""
		w.validationError = ""
		w.message = err.Error()
		return
	}
	if fieldID != "" {
		w.focusField(fieldID)
	}
	w.validationField = fieldID
	w.validationError = err.Error()
	w.message = ""
}

func (w *jailCreationWizard) focusField(fieldID string) {
	fields := w.visibleFields()
	for idx, field := range fields {
		if field.ID == fieldID {
			w.field = idx
			return
		}
	}
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

func (w jailCreationWizard) summaryLines() []string {
	templateLabel := "Template/Release"
	if normalizedJailType(w.values.JailType) == "linux" {
		templateLabel = "FreeBSD Base/Release"
	}
	lines := []string{
		fmt.Sprintf("Type: %s", valueOrDash(w.values.JailType)),
		fmt.Sprintf("Name: %s", w.values.Name),
		fmt.Sprintf("Destination: %s", w.values.Dataset),
		fmt.Sprintf("%s: %s", templateLabel, w.values.TemplateRelease),
		fmt.Sprintf("IPv4: %s", w.values.IP4),
		fmt.Sprintf("IPv6: %s", valueOrDash(w.values.IP6)),
		fmt.Sprintf("Default router: %s", valueOrDash(w.values.DefaultRouter)),
		fmt.Sprintf("Hostname: %s", valueOrDash(w.values.Hostname)),
		fmt.Sprintf("Startup order: %s", startupOrderSummary(w.values.StartupOrder)),
		fmt.Sprintf("Dependencies: %s", dependencySummary(w.values.Dependencies)),
		fmt.Sprintf("CPU %%: %s", valueOrDash(w.values.CPUPercent)),
		fmt.Sprintf("Memory limit: %s", valueOrDash(w.values.MemoryLimit)),
		fmt.Sprintf("Process limit: %s", valueOrDash(w.values.ProcessLimit)),
	}
	if normalizedJailType(w.values.JailType) != "thin" {
		lines = append(lines, fmt.Sprintf("Patch FreeBSD base: %s", freeBSDPatchSummary(w.values.TemplateRelease, w.values.PatchBase)))
	}
	if normalizedJailType(w.values.JailType) == "vnet" {
		lines = append(lines,
			fmt.Sprintf("Bridge: %s", valueOrDash(w.values.Bridge)),
			fmt.Sprintf("Bridge policy: %s", effectiveBridgePolicy(w.values)),
			fmt.Sprintf("Host setup: %s", effectiveVNETHostSetup(w.values)),
			fmt.Sprintf("Uplink: %s", valueOrDash(w.values.Uplink)),
		)
	} else {
		lines = append(lines, fmt.Sprintf("Interface: %s", w.values.Interface))
	}
	if normalizedJailType(w.values.JailType) == "linux" {
		lines = append(lines,
			fmt.Sprintf("Bootstrap family: %s", effectiveLinuxDistro(w.values)),
			fmt.Sprintf("Bootstrap release: %s", effectiveLinuxRelease(w.values)),
			fmt.Sprintf("Bootstrap mode: %s", effectiveLinuxBootstrapMode(w.values)),
			fmt.Sprintf("Mirror mode: %s", effectiveLinuxMirrorMode(w.values)),
			fmt.Sprintf("Mirror URL: %s", effectiveLinuxMirrorSummary(w.values)),
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
	if normalizedJailType(w.values.JailType) != "thin" {
		patchDecision := resolveFreeBSDPatchDecision(w.values.TemplateRelease, w.values.PatchBase)
		if patchDecision.Effective {
			addStep("Patch extracted FreeBSD base to latest level:")
			addDetail(fmt.Sprintf("   freebsd-update -b %s fetch install", destination))
		}
	}

	switch jailType {
	case "vnet":
		addStep("Ensure VNET host bridge setup is ready:")
		addDetail(fmt.Sprintf("   # bridge policy: %s", effectiveBridgePolicy(w.values)))
		addDetail(fmt.Sprintf("   # host setup: %s", effectiveVNETHostSetup(w.values)))
		if effectiveVNETHostSetup(w.values) == "persistent" {
			addStep("Persist VNET host networking in rc.conf:")
			addDetail(fmt.Sprintf("   sysrc cloned_interfaces+=\" %s\"", strings.TrimSpace(w.values.Bridge)))
			bridgeConfig := "up"
			if strings.TrimSpace(w.values.Uplink) != "" {
				bridgeConfig = fmt.Sprintf("addm %s up", strings.TrimSpace(w.values.Uplink))
			}
			addDetail(fmt.Sprintf("   sysrc ifconfig_%s=%q", strings.TrimSpace(w.values.Bridge), bridgeConfig))
			if strings.TrimSpace(w.values.Uplink) != "" {
				addDetail(fmt.Sprintf("   sysrc ifconfig_%s=up", strings.TrimSpace(w.values.Uplink)))
			}
		}
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
	jailListAction := startupOrderPlanLine(w.values)
	if jailListAction != "" {
		addStep("Update rc.conf jail_list for startup order:")
		addDetail("   " + jailListAction)
	}
	addStep(fmt.Sprintf("Start jail: service jail start %s", w.values.Name))

	if strings.TrimSpace(w.values.CPUPercent) != "" ||
		strings.TrimSpace(w.values.MemoryLimit) != "" ||
		strings.TrimSpace(w.values.ProcessLimit) != "" {
		addStep("Write managed jail limits to /etc/rctl.conf and apply them immediately:")
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
		lines = append(lines,
			fmt.Sprintf("  # freebsd-jails-tui: bridge=%s bridge_policy=%s host_setup=%s uplink=%s default_router=%s;", strings.TrimSpace(values.Bridge), effectiveBridgePolicy(values), effectiveVNETHostSetup(values), strings.TrimSpace(values.Uplink), strings.TrimSpace(values.DefaultRouter)),
		)
		lines = append(lines, buildVNETJailConfig(values)...)
	case "linux":
		lines = append(lines,
			fmt.Sprintf("  # freebsd-jails-tui: linux_distro=%s linux_release=%s linux_bootstrap=%s linux_mirror_mode=%s linux_mirror_url=%s;", effectiveLinuxDistro(values), effectiveLinuxRelease(values), effectiveLinuxBootstrapMode(values), effectiveLinuxMirrorMode(values), linuxMirrorMetadataValue(values)),
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

func startupOrderPlanLine(values jailWizardValues) string {
	position, _ := parseStartupOrderValue(values.StartupOrder)
	if position <= 0 {
		return fmt.Sprintf("if jail_list is already set: append %s; otherwise keep implicit 'start all configured jails' behavior", strings.TrimSpace(values.Name))
	}
	return fmt.Sprintf("sysrc jail_list=\"... %s ...\"  # place %s at position %d", strings.TrimSpace(values.Name), strings.TrimSpace(values.Name), position)
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

func linuxMirrorMetadataValue(values jailWizardValues) string {
	if effectiveLinuxMirrorMode(values) != "custom" {
		return "-"
	}
	info, err := resolveLinuxMirror(values)
	if err != nil {
		return strings.TrimSpace(values.LinuxMirrorURL)
	}
	return info.BaseURL
}

func metadataDashValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
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

func linuxMirrorURL(values jailWizardValues) string {
	mirror, _ := resolveLinuxMirror(values)
	return mirror.BaseURL
}

func linuxMirrorHost(values jailWizardValues) string {
	mirror, _ := resolveLinuxMirror(values)
	return mirror.Host
}

func linuxPreflightURL(values jailWizardValues) string {
	mirror, _ := resolveLinuxMirror(values)
	return mirror.PreflightURL
}

func linuxCompatRoot(jailPath string, values jailWizardValues) string {
	return filepath.Join(jailPath, "compat", effectiveLinuxDistro(values))
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
	case "linux_distro", "linux_release", "linux_bootstrap", "linux_mirror_mode", "linux_mirror_url":
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
