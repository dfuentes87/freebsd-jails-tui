package main

import (
	"fmt"
	"os"
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

var wizardSteps = []wizardStep{
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
			{ID: "interface", Label: "Interface", Placeholder: "em0", Help: "Bridge or jail interface name"},
			{ID: "ip4", Label: "IPv4", Placeholder: "192.168.1.20/24", Help: "CIDR or 'inherit'"},
			{ID: "ip6", Label: "IPv6", Placeholder: "2001:db8::10/64", Help: "CIDR or 'inherit'"},
			{ID: "default_router", Label: "Default router", Placeholder: "192.168.1.1", Help: "Optional"},
			{ID: "cpu_percent", Label: "CPU %", Placeholder: "50", Help: ""},
			{ID: "memory_limit", Label: "Memory", Placeholder: "2G", Help: "Examples: 512M, 2G"},
			{ID: "process_limit", Label: "Max processes", Placeholder: "512", Help: ""},
			{ID: "mount_points", Label: "Mount points (optional)", Placeholder: "/data,/logs", Help: "Example: /mnt/shared,/var/cache/pkg"},
		},
	},
	{
		Title:       "6. Confirmation",
		Description: "Review the generated jail.conf and creation plan.",
	},
}

type jailWizardValues struct {
	JailType        string
	Name            string
	Dataset         string
	TemplateRelease string
	Interface       string
	IP4             string
	IP6             string
	DefaultRouter   string
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
	step           int
	field          int
	values         jailWizardValues
	templateMode   wizardTemplateMode
	templateInput  string
	templates      []wizardTemplate
	templateCursor int
	userlandMode   bool
	userlandOpts   []userlandOption
	userlandCursor int
	message        string
	executionLogs  []string
	executionError string
}

func newJailCreationWizard(defaultDestination string) jailCreationWizard {
	return jailCreationWizard{
		values: jailWizardValues{
			Dataset:   strings.TrimSpace(defaultDestination),
			Interface: "em0",
		},
	}
}

func (w jailCreationWizard) currentStep() wizardStep {
	if w.step < 0 || w.step >= len(wizardSteps) {
		return wizardSteps[0]
	}
	return wizardSteps[w.step]
}

func (w jailCreationWizard) isConfirmationStep() bool {
	return w.step == len(wizardSteps)-1
}

func (w *jailCreationWizard) nextField() {
	step := w.currentStep()
	if len(step.Fields) == 0 {
		return
	}
	w.field++
	if w.field >= len(step.Fields) {
		w.field = 0
	}
}

func (w *jailCreationWizard) prevField() {
	step := w.currentStep()
	if len(step.Fields) == 0 {
		return
	}
	w.field--
	if w.field < 0 {
		w.field = len(step.Fields) - 1
	}
}

func (w *jailCreationWizard) nextStep() error {
	if err := w.validateCurrentStep(); err != nil {
		w.message = err.Error()
		return err
	}
	if w.step < len(wizardSteps)-1 {
		w.step++
		w.field = 0
		w.message = ""
		w.executionLogs = nil
		w.executionError = ""
	}
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
}

func (w *jailCreationWizard) beginTemplateSave() {
	w.templateMode = wizardTemplateModeSave
	w.userlandMode = false
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
}

func (w jailCreationWizard) activeField() (wizardField, bool) {
	step := w.currentStep()
	if len(step.Fields) == 0 {
		return wizardField{}, false
	}
	idx := w.field
	if idx < 0 {
		idx = 0
	}
	if idx >= len(step.Fields) {
		idx = len(step.Fields) - 1
	}
	return step.Fields[idx], true
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
	case "ip4":
		return &w.values.IP4
	case "ip6":
		return &w.values.IP6
	case "default_router":
		return &w.values.DefaultRouter
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
	case "ip4":
		return w.values.IP4
	case "ip6":
		return w.values.IP6
	case "default_router":
		return w.values.DefaultRouter
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
	if !strings.HasPrefix(strings.TrimSpace(w.values.Dataset), "/") {
		return fmt.Errorf("destination must be an absolute path, e.g. /usr/local/jails/containers/%s", strings.TrimSpace(w.values.Name))
	}
	if strings.TrimSpace(w.values.TemplateRelease) == "" {
		return fmt.Errorf("template/release is required (local path, release tag, or https URL)")
	}
	if hasConflictingJailConfig(w.values.Name) {
		return fmt.Errorf("config already exists: %s", jailConfigPathForName(w.values.Name))
	}
	if strings.TrimSpace(w.values.Interface) == "" {
		return fmt.Errorf("interface is required")
	}
	if strings.TrimSpace(w.values.IP4) == "" {
		return fmt.Errorf("IPv4 is required")
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
	return nil
}

func (w jailCreationWizard) validateAll() error {
	for idx := 0; idx < len(wizardSteps)-1; idx++ {
		test := w
		test.step = idx
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
		fmt.Sprintf("Interface: %s", w.values.Interface),
		fmt.Sprintf("IPv4: %s", w.values.IP4),
		fmt.Sprintf("IPv6: %s", valueOrDash(w.values.IP6)),
		fmt.Sprintf("Default router: %s", valueOrDash(w.values.DefaultRouter)),
		fmt.Sprintf("CPU %%: %s", valueOrDash(w.values.CPUPercent)),
		fmt.Sprintf("Memory limit: %s", valueOrDash(w.values.MemoryLimit)),
		fmt.Sprintf("Process limit: %s", valueOrDash(w.values.ProcessLimit)),
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
	lines := []string{
		"1. Ensure destination path exists:",
		fmt.Sprintf("   mkdir -p %s", destination),
		"2. Provision jail root from selected template/release:",
		fmt.Sprintf("   # source: %s", w.values.TemplateRelease),
		fmt.Sprintf("3. Write jail config: %s", jailConfigPathForName(w.values.Name)),
		fmt.Sprintf("4. Start jail: service jail start %s", w.values.Name),
	}

	if strings.TrimSpace(w.values.CPUPercent) != "" ||
		strings.TrimSpace(w.values.MemoryLimit) != "" ||
		strings.TrimSpace(w.values.ProcessLimit) != "" {
		lines = append(lines, "5. Apply rctl limits:")
		if strings.TrimSpace(w.values.CPUPercent) != "" {
			lines = append(lines, fmt.Sprintf("   rctl -a jail:%s:pcpu:deny=%s", w.values.Name, w.values.CPUPercent))
		}
		if strings.TrimSpace(w.values.MemoryLimit) != "" {
			lines = append(lines, fmt.Sprintf("   rctl -a jail:%s:memoryuse:deny=%s", w.values.Name, strings.ToUpper(w.values.MemoryLimit)))
		}
		if strings.TrimSpace(w.values.ProcessLimit) != "" {
			lines = append(lines, fmt.Sprintf("   rctl -a jail:%s:maxproc:deny=%s", w.values.Name, w.values.ProcessLimit))
		}
	}

	mounts := w.mountPointList()
	if len(mounts) > 0 {
		lines = append(lines, "6. Configure mount points:")
		for _, mount := range mounts {
			lines = append(lines, "   mountpoint: "+mount)
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
			target = normalizeMountTarget(target)
			if source == "" || target == "" {
				continue
			}
			specs = append(specs, mountPointSpec{Source: source, Target: target})
			continue
		}
		target = normalizeMountTarget(item)
		if target == "" {
			continue
		}
		specs = append(specs, mountPointSpec{Target: target})
	}
	return specs
}

func normalizeMountTarget(target string) string {
	target = "/" + strings.Trim(strings.TrimSpace(target), "/")
	if target == "/" {
		return ""
	}
	return target
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
	lines := []string{
		fmt.Sprintf("%s {", name),
		fmt.Sprintf("  host.hostname = %q;", name),
		fmt.Sprintf("  path = %q;", jailPath),
		"  vnet;",
		fmt.Sprintf("  vnet.interface = %q;", strings.TrimSpace(values.Interface)),
		fmt.Sprintf("  ip4.addr = %q;", strings.TrimSpace(values.IP4)),
	}
	if strings.TrimSpace(values.IP6) != "" {
		lines = append(lines, fmt.Sprintf("  ip6.addr = %q;", strings.TrimSpace(values.IP6)))
	}
	lines = append(lines,
		"  exec.start = \"/bin/sh /etc/rc\";",
		"  exec.stop = \"/bin/sh /etc/rc.shutdown\";",
		"  persist;",
	)
	if strings.TrimSpace(values.DefaultRouter) != "" {
		lines = append(lines, fmt.Sprintf("  defaultrouter = %q;", strings.TrimSpace(values.DefaultRouter)))
	}
	if strings.TrimSpace(fstabPath) != "" {
		lines = append(lines, fmt.Sprintf("  mount.fstab = %q;", fstabPath))
	}
	lines = append(lines, "}")
	return lines
}

func wizardSectionForField(id string) string {
	switch id {
	case "jail_type":
		return "0. Jail Type"
	case "name", "dataset":
		return "1. Name & Destination"
	case "template_release":
		return "2. Template or release"
	case "interface", "ip4", "ip6", "default_router":
		return "3. Networking"
	case "cpu_percent", "memory_limit", "process_limit":
		return "4. Resource Limits (optional)"
	case "mount_points":
		return "5. Mount points"
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
