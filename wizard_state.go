package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	jailNamePattern    = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	memoryLimitPattern = regexp.MustCompile(`^[0-9]+[KMGTP]?$`)
)

const maxJailNoteLen = 120

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
			{ID: "note", Label: "Note", Placeholder: "nginx reverse proxy", Help: "Optional short dashboard note"},
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
			{ID: "mount_points", Label: "Mount points", Placeholder: "/data,/logs", Help: "Example: /mnt/shared,/var/cache/pkg"},
		},
	},
	{
		Title:       "Linux Bootstrap",
		Description: "Choose the bootstrap family, method, and source used to populate /compat inside a linux jail.",
		Fields: []wizardField{
			{ID: "linux_preset", Label: "Bootstrap preset", Placeholder: "custom", Help: "Options: custom, alpine, or rocky"},
			{ID: "linux_distro", Label: "Bootstrap family", Placeholder: "ubuntu", Help: "Free-form family name used for the compat root name"},
			{ID: "linux_bootstrap_method", Label: "Bootstrap method", Placeholder: "debootstrap", Help: "Options: debootstrap or archive"},
			{ID: "linux_release", Label: "Bootstrap release", Placeholder: "jammy", Help: "Codename, suite, or release string passed to debootstrap"},
			{ID: "linux_bootstrap", Label: "Bootstrap mode", Placeholder: "auto", Help: "Options: auto or skip"},
			{ID: "linux_mirror_mode", Label: "Mirror mode", Placeholder: "default", Help: "Options: default or custom"},
			{ID: "linux_mirror_url", Label: "Mirror URL", Placeholder: "https://mirror.example.com/repo", Help: "Custom Linux package mirror base URL"},
			{ID: "linux_archive_url", Label: "Archive source", Placeholder: "URL or local file", Help: "Full URL or absolute local path to a rootfs tar archive"},
		},
	},
	{
		Title:       "Confirmation",
		Description: "Review the generated jail.conf and creation plan.",
	},
}

type jailWizardValues struct {
	JailType             string
	Name                 string
	Dataset              string
	TemplateRelease      string
	Interface            string
	Bridge               string
	BridgePolicy         string
	VNETHostSetup        string
	Uplink               string
	IP4                  string
	IP6                  string
	DefaultRouter        string
	Hostname             string
	Note                 string
	PatchBase            string
	StartupOrder         string
	Dependencies         string
	LinuxPreset          string
	LinuxDistro          string
	LinuxBootstrapMethod string
	LinuxRelease         string
	LinuxBootstrap       string
	LinuxMirrorMode      string
	LinuxMirrorURL       string
	LinuxArchiveURL      string
	CPUPercent           string
	MemoryLimit          string
	ProcessLimit         string
	MountPoints          string
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
	racctPrereqs         RacctWizardPrereqs
	racctPrereqKey       string
	racctPrereqCached    bool
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
	showJailConfPreview  bool
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
		w.showJailConfPreview = false
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
		w.showJailConfPreview = false
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

func boundSelectionCursor(cursor, count int) int {
	if count <= 0 {
		return 0
	}
	if cursor < 0 {
		return 0
	}
	if cursor >= count {
		return count - 1
	}
	return cursor
}

func (w *jailCreationWizard) endThinDatasetSelect() {
	w.thinDatasetMode = false
	w.thinDatasetOpts = nil
	w.thinDatasetCursor = 0
	w.datasetCreateRunning = false
}

func (w *jailCreationWizard) boundUserlandCursor() {
	w.userlandCursor = boundSelectionCursor(w.userlandCursor, len(w.userlandOpts))
}

func (w *jailCreationWizard) selectedUserlandOption() (userlandOption, bool) {
	if len(w.userlandOpts) == 0 {
		return userlandOption{}, false
	}
	w.boundUserlandCursor()
	return w.userlandOpts[w.userlandCursor], true
}

func (w *jailCreationWizard) boundThinDatasetCursor() {
	w.thinDatasetCursor = boundSelectionCursor(w.thinDatasetCursor, len(w.thinDatasetOpts))
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
	w.templateCursor = boundSelectionCursor(w.templateCursor, len(w.templates))
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
	w.applyLinuxBootstrapPreset()
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
	w.applyLinuxBootstrapPreset()
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
		effectiveLinuxBootstrapPreset(w.values),
		effectiveLinuxDistro(w.values),
		effectiveLinuxBootstrapMethod(w.values),
		effectiveLinuxRelease(w.values),
		effectiveLinuxBootstrapMode(w.values),
		effectiveLinuxMirrorMode(w.values),
		strings.TrimSpace(w.values.LinuxMirrorURL),
		strings.TrimSpace(w.values.LinuxArchiveURL),
	}, "|")
	if w.linuxPrereqCached && w.linuxPrereqKey == key {
		return
	}
	w.linuxPrereqs = collectLinuxWizardPrereqs(w.values)
	w.linuxPrereqKey = key
	w.linuxPrereqCached = true
}

func (w *jailCreationWizard) refreshRacctPrereqs() {
	key := strings.Join([]string{
		strings.TrimSpace(w.values.CPUPercent),
		strings.TrimSpace(w.values.MemoryLimit),
		strings.TrimSpace(w.values.ProcessLimit),
	}, "|")
	if w.racctPrereqCached && w.racctPrereqKey == key {
		return
	}
	w.racctPrereqs = collectRacctWizardPrereqs(w.values)
	w.racctPrereqKey = key
	w.racctPrereqCached = true
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
	w.refreshRacctPrereqs()
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
		case "linux_preset", "linux_distro", "linux_bootstrap_method", "linux_bootstrap":
			if jailType != "linux" {
				continue
			}
		case "linux_release", "linux_mirror_mode":
			if jailType != "linux" {
				continue
			}
			if effectiveLinuxBootstrapMethod(w.values) != "debootstrap" {
				continue
			}
		case "linux_mirror_url":
			if jailType != "linux" {
				continue
			}
			if effectiveLinuxBootstrapMethod(w.values) != "debootstrap" {
				continue
			}
			if effectiveLinuxMirrorMode(w.values) != "custom" {
				continue
			}
		case "linux_archive_url":
			if jailType != "linux" {
				continue
			}
			if effectiveLinuxBootstrapMethod(w.values) != "archive" {
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
	case "note":
		return &w.values.Note
	case "patch_base":
		return &w.values.PatchBase
	case "startup_order":
		return &w.values.StartupOrder
	case "dependencies":
		return &w.values.Dependencies
	case "linux_preset":
		return &w.values.LinuxPreset
	case "linux_distro":
		return &w.values.LinuxDistro
	case "linux_bootstrap_method":
		return &w.values.LinuxBootstrapMethod
	case "linux_release":
		return &w.values.LinuxRelease
	case "linux_bootstrap":
		return &w.values.LinuxBootstrap
	case "linux_mirror_mode":
		return &w.values.LinuxMirrorMode
	case "linux_mirror_url":
		return &w.values.LinuxMirrorURL
	case "linux_archive_url":
		return &w.values.LinuxArchiveURL
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
	case "note":
		return w.values.Note
	case "patch_base":
		return w.values.PatchBase
	case "startup_order":
		return w.values.StartupOrder
	case "dependencies":
		return w.values.Dependencies
	case "linux_preset":
		return w.values.LinuxPreset
	case "linux_distro":
		return w.values.LinuxDistro
	case "linux_bootstrap_method":
		return w.values.LinuxBootstrapMethod
	case "linux_release":
		return w.values.LinuxRelease
	case "linux_bootstrap":
		return w.values.LinuxBootstrap
	case "linux_mirror_mode":
		return w.values.LinuxMirrorMode
	case "linux_mirror_url":
		return w.values.LinuxMirrorURL
	case "linux_archive_url":
		return w.values.LinuxArchiveURL
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
	focused := false
	if fieldID != "" {
		focused = w.focusField(fieldID)
	}
	w.validationField = fieldID
	w.validationError = err.Error()
	if focused {
		w.message = ""
		return
	}
	w.message = err.Error()
}

func (w *jailCreationWizard) focusField(fieldID string) bool {
	fields := w.visibleFields()
	for idx, field := range fields {
		if field.ID == fieldID {
			w.field = idx
			return true
		}
	}
	return false
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
		fmt.Sprintf("Note: %s", valueOrDash(w.values.Note)),
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
			fmt.Sprintf("Bootstrap preset: %s", effectiveLinuxBootstrapPreset(w.values)),
			fmt.Sprintf("Bootstrap family: %s", effectiveLinuxDistro(w.values)),
			fmt.Sprintf("Bootstrap method: %s", effectiveLinuxBootstrapMethod(w.values)),
			fmt.Sprintf("Bootstrap mode: %s", effectiveLinuxBootstrapMode(w.values)),
		)
		if effectiveLinuxBootstrapMethod(w.values) == "debootstrap" {
			lines = append(lines,
				fmt.Sprintf("Bootstrap release: %s", effectiveLinuxRelease(w.values)),
				fmt.Sprintf("Mirror mode: %s", effectiveLinuxMirrorMode(w.values)),
			)
		}
		lines = append(lines,
			fmt.Sprintf("Bootstrap source: %s", effectiveLinuxSourceSummary(w.values)),
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
