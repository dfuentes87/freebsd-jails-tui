package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Jail struct {
	Name       string
	JID        int
	Path       string
	Hostname   string
	QuotaUsage string
	Running    bool
	CPUPercent float64
	MemoryMB   int
}

type DashboardSnapshot struct {
	Jails           []Jail
	RunningCount    int
	StoppedCount    int
	TotalCPUPercent float64
	TotalMemoryMB   int
	LastUpdated     time.Time
}

type JailDetail struct {
	Name                  string
	JID                   int
	Path                  string
	Hostname              string
	JLSFields             map[string]string
	RuntimeValues         map[string]string
	AdvancedRuntimeFields map[string]string
	NetworkSummary        *JailNetworkSummary
	StartupConfig         *JailStartupConfig
	JailConfSource        string
	JailConfRaw           []string
	JailConfValues        map[string]string
	JailConfFlags         []string
	ZFS                   *ZFSDatasetInfo
	RctlRules             []string
	RctlConfig            *JailRctlConfig
	RacctStatus           *RacctStatus
	LinuxReadiness        *LinuxReadiness
	SourceErrors          map[string]string
	LastUpdated           time.Time
}

type ZFSDatasetInfo struct {
	Name        string
	Mountpoint  string
	Used        string
	Avail       string
	Refer       string
	Compression string
	Quota       string
	Reservation string
	MatchType   string
}

type jailMetric struct {
	CPUPercent float64
	RSSKB      int
}

type runningJail struct {
	Name     string
	JID      int
	Path     string
	Hostname string
}

type jailConfData struct {
	SourcePath string
	RawLines   []string
	Values     map[string]string
	Flags      []string
}

func CollectSnapshot(now time.Time) (DashboardSnapshot, error) {
	var (
		errs      []error
		snapshot  DashboardSnapshot
		nameSet   = map[string]struct{}{}
		runningBy = map[string]runningJail{}
	)

	configured := discoverConfiguredJails()
	for _, name := range configured {
		nameSet[name] = struct{}{}
	}

	running, err := discoverRunningJails()
	if err != nil {
		errs = append(errs, err)
	}
	for _, jail := range running {
		runningBy[jail.Name] = jail
		nameSet[jail.Name] = struct{}{}
	}

	metrics, metricErr := discoverJailMetrics()
	if metricErr != nil {
		errs = append(errs, metricErr)
	}
	zfsRows, zfsErr := listZFSFilesystems()
	if zfsErr != nil {
		errs = append(errs, zfsErr)
	}

	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		j := Jail{Name: name}
		if run, ok := runningBy[name]; ok {
			j.Running = run.JID > 0
			j.JID = run.JID
			j.Path = run.Path
			j.Hostname = run.Hostname
		}
		if conf, err := discoverJailConf(name); err == nil {
			if j.Path == "" {
				j.Path = strings.TrimSpace(conf.Values["path"])
			}
			if j.Hostname == "" {
				j.Hostname = strings.TrimSpace(conf.Values["host.hostname"])
			}
		}
		if info := discoverZFSDatasetFromRows(zfsRows, j.Path); info != nil {
			j.QuotaUsage = formatQuotaUsage(info)
		}
		if metric, ok := metrics[j.JID]; ok {
			j.CPUPercent = metric.CPUPercent
			j.MemoryMB = metric.RSSKB / 1024
		}
		if j.Running {
			snapshot.RunningCount++
		} else {
			snapshot.StoppedCount++
		}
		snapshot.TotalCPUPercent += j.CPUPercent
		snapshot.TotalMemoryMB += j.MemoryMB
		snapshot.Jails = append(snapshot.Jails, j)
	}

	sort.Slice(snapshot.Jails, func(i, j int) bool {
		if snapshot.Jails[i].Running != snapshot.Jails[j].Running {
			return snapshot.Jails[i].Running
		}
		return snapshot.Jails[i].Name < snapshot.Jails[j].Name
	})

	snapshot.LastUpdated = now
	return snapshot, errors.Join(errs...)
}

func CollectJailDetail(name string, jid int, pathHint string, now time.Time) (JailDetail, error) {
	detail := JailDetail{
		Name:                  name,
		JID:                   jid,
		Path:                  strings.TrimSpace(pathHint),
		JLSFields:             map[string]string{},
		RuntimeValues:         map[string]string{},
		AdvancedRuntimeFields: map[string]string{},
		JailConfValues:        map[string]string{},
		SourceErrors:          map[string]string{},
	}

	var errs []error
	addErr := func(source string, err error) {
		if err == nil {
			return
		}
		errs = append(errs, err)
		detail.SourceErrors[source] = err.Error()
	}

	jlsFields, jlsErr := discoverRunningJailFields(name)
	addErr("jls", jlsErr)
	if len(jlsFields) > 0 {
		detail.JLSFields = jlsFields
		if detail.JID <= 0 {
			detail.JID, _ = strconv.Atoi(jlsFields["jid"])
		}
		if detail.Path == "" {
			detail.Path = jlsFields["path"]
		}
		if detail.Hostname == "" {
			detail.Hostname = jlsFields["host.hostname"]
		}
	}

	conf, confErr := discoverJailConf(name)
	addErr("jail.conf", confErr)
	if conf.SourcePath != "" {
		detail.JailConfSource = conf.SourcePath
		detail.JailConfRaw = conf.RawLines
		detail.JailConfValues = conf.Values
		detail.JailConfFlags = conf.Flags
		if detail.Path == "" {
			detail.Path = conf.Values["path"]
		}
		if detail.Hostname == "" {
			detail.Hostname = conf.Values["host.hostname"]
		}
	}

	if detail.Path != "" {
		zfsInfo, zfsErr := discoverZFSDataset(detail.Path)
		addErr("zfs", zfsErr)
		detail.ZFS = zfsInfo
	}

	rules, rctlErr := discoverRctlRules(name, detail.JID)
	addErr("rctl", rctlErr)
	detail.RctlRules = rules
	detail.RctlConfig = rctlConfigFromRawLines(detail.JailConfRaw)
	racct := collectRacctStatus()
	detail.RacctStatus = &racct
	detail.RuntimeValues, detail.AdvancedRuntimeFields = classifyDetailRuntime(detail.JLSFields, detail.JailConfValues)
	detail.NetworkSummary = collectJailNetworkSummary(detail)
	detail.StartupConfig = collectJailStartupConfig(detail)
	detail.LinuxReadiness = collectLinuxReadiness(detail)

	detail.LastUpdated = now
	return detail, errors.Join(errs...)
}

func classifyDetailRuntime(jlsFields, confValues map[string]string) (map[string]string, map[string]string) {
	runtime := map[string]string{}
	advanced := map[string]string{}

	get := func(key string) string {
		return strings.TrimSpace(jlsFields[key])
	}

	if jid := get("jid"); jid != "" {
		runtime["JID"] = jid
	}
	if path := get("path"); path != "" {
		runtime["Live path"] = path
	}
	if host := get("host.hostname"); host != "" {
		runtime["Live hostname"] = host
	}

	switch strings.ToLower(get("vnet")) {
	case "new", "on", "true":
		runtime["Network mode"] = "VNET"
	case "inherit":
		runtime["Network mode"] = "Shared stack"
	}
	if iface := strings.TrimSpace(firstNonEmpty(get("vnet.interface"), get("interface"))); iface != "" {
		runtime["Interface"] = iface
	}
	if ip4 := strings.TrimSpace(firstNonEmpty(get("ip4.addr"), get("ip4"))); ip4 != "" {
		runtime["IPv4"] = ip4
	}
	if ip6 := strings.TrimSpace(firstNonEmpty(get("ip6.addr"), get("ip6"))); ip6 != "" {
		runtime["IPv6"] = ip6
	}

	curatedKeys := map[string]struct{}{
		"name":           {},
		"jid":            {},
		"path":           {},
		"host.hostname":  {},
		"vnet":           {},
		"vnet.interface": {},
		"interface":      {},
		"ip4":            {},
		"ip4.addr":       {},
		"ip6":            {},
		"ip6.addr":       {},
	}

	configuredKeys := map[string]struct{}{}
	for key := range confValues {
		configuredKeys[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}

	for key, value := range jlsFields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := curatedKeys[key]; ok {
			continue
		}
		if _, ok := configuredKeys[strings.ToLower(key)]; ok {
			continue
		}
		advanced[key] = value
	}

	return runtime, advanced
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func discoverConfiguredJails() []string {
	found := map[string]struct{}{}
	re := regexp.MustCompile(`^\s*([a-zA-Z0-9_.-]+)\s*\{`)
	for _, path := range discoverJailConfigFiles() {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			matches := re.FindStringSubmatch(line)
			if len(matches) == 2 {
				found[matches[1]] = struct{}{}
			}
		}
		file.Close()
	}
	names := make([]string, 0, len(found))
	for name := range found {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func discoverJailConf(name string) (jailConfData, error) {
	var (
		firstReadablePath string
		foundAnyPath      bool
	)

	for _, path := range discoverJailConfigFiles() {
		content, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		foundAnyPath = true
		if firstReadablePath == "" {
			firstReadablePath = path
		}
		rawLines, ok := extractJailBlock(string(content), name)
		if !ok {
			continue
		}
		values, flags := parseJailBlockLines(rawLines)
		return jailConfData{
			SourcePath: path,
			RawLines:   rawLines,
			Values:     values,
			Flags:      flags,
		}, nil
	}

	if !foundAnyPath {
		return jailConfData{}, fmt.Errorf("no readable jail config found in /etc/jail.conf, /usr/local/etc/jail.conf, or /etc/jail.conf.d")
	}
	return jailConfData{}, fmt.Errorf("jail %q not found in discovered jail config files (first readable: %s)", name, firstReadablePath)
}

func discoverJailConfigFiles() []string {
	primaryFiles := []string{
		"/etc/jail.conf",
		"/usr/local/etc/jail.conf",
	}
	configDirs := []string{
		"/etc/jail.conf.d",
		"/usr/local/etc/jail.conf.d",
	}

	seen := map[string]struct{}{}
	files := make([]string, 0, 16)
	appendFile := func(path string) {
		if _, exists := seen[path]; exists {
			return
		}
		seen[path] = struct{}{}
		files = append(files, path)
	}

	for _, path := range primaryFiles {
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			continue
		}
		appendFile(path)
	}

	for _, dir := range configDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasPrefix(name, ".") {
				continue
			}
			full := filepath.Join(dir, name)
			appendFile(full)
		}
	}
	sort.Strings(files)
	return files
}

func discoverRunningJails() ([]runningJail, error) {
	out, err := exec.Command("jls", "-n").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run jls -n: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	result := make([]runningJail, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := parseKVFields(line)
		name := fields["name"]
		if name == "" {
			continue
		}
		jid, _ := strconv.Atoi(fields["jid"])
		result = append(result, runningJail{
			Name:     name,
			JID:      jid,
			Path:     fields["path"],
			Hostname: fields["host.hostname"],
		})
	}
	return result, nil
}

func discoverRunningJailFields(name string) (map[string]string, error) {
	out, err := exec.Command("jls", "-n").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run jls -n: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := parseKVFields(line)
		if fields["name"] == name {
			return fields, nil
		}
	}
	return map[string]string{}, nil
}

func discoverJailMetrics() (map[int]jailMetric, error) {
	out, err := exec.Command("ps", "-axo", "jid=,pcpu=,rss=").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run ps for jail metrics: %w", err)
	}
	metrics := map[int]jailMetric{}
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			continue
		}
		jid, err := strconv.Atoi(fields[0])
		if err != nil || jid <= 0 {
			continue
		}
		cpu, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		rssKB, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		current := metrics[jid]
		current.CPUPercent += cpu
		current.RSSKB += rssKB
		metrics[jid] = current
	}
	return metrics, nil
}

func discoverZFSDataset(jailPath string) (*ZFSDatasetInfo, error) {
	rows, err := listZFSFilesystems()
	if err != nil {
		return nil, err
	}
	return discoverZFSDatasetFromRows(rows, jailPath), nil
}

func discoverZFSDatasetFromRows(rows []zfsFilesystemRow, jailPath string) *ZFSDatasetInfo {
	jailPath = strings.TrimSpace(jailPath)
	if jailPath == "" {
		return nil
	}
	jailPath = filepath.Clean(jailPath)
	var (
		best      *ZFSDatasetInfo
		bestScore = -1
	)
	for _, row := range rows {
		mountpoint := strings.TrimSpace(row.Mountpoint)
		if mountpoint == "" || mountpoint == "-" || mountpoint == "legacy" {
			continue
		}

		matchType := ""
		score := -1
		if jailPath == mountpoint {
			matchType = "exact"
			score = len(mountpoint) + 1000
		} else if mountpoint == "/" && strings.HasPrefix(jailPath, "/") {
			matchType = "prefix"
			score = 1
		} else if strings.HasPrefix(jailPath, mountpoint+"/") {
			matchType = "prefix"
			score = len(mountpoint)
		}
		if score <= bestScore {
			continue
		}
		bestScore = score
		best = &ZFSDatasetInfo{
			Name:        row.Name,
			Mountpoint:  mountpoint,
			Used:        row.Used,
			Avail:       row.Avail,
			Refer:       row.Refer,
			Compression: row.Compression,
			Quota:       row.Quota,
			Reservation: row.Reservation,
			MatchType:   matchType,
		}
	}
	return best
}

func formatQuotaUsage(info *ZFSDatasetInfo) string {
	if info == nil {
		return ""
	}
	quota := strings.TrimSpace(info.Quota)
	if quota == "" || quota == "-" || strings.EqualFold(quota, "none") || strings.EqualFold(quota, "inherit") {
		return ""
	}
	used := strings.TrimSpace(info.Used)
	if used == "" || used == "-" {
		return quota
	}
	return used + " / " + quota
}

func discoverRctlRules(name string, jid int) ([]string, error) {
	out, err := exec.Command("rctl").CombinedOutput()
	if err != nil {
		text := strings.TrimSpace(string(out))
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			lower := strings.ToLower(text)
			if text == "" ||
				strings.Contains(lower, "racct") ||
				strings.Contains(lower, "rctl support not present") ||
				strings.Contains(lower, "not supported") {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to run rctl: %s", text)
		}
		return nil, fmt.Errorf("failed to run rctl: %w", err)
	}

	namePattern := "jail:" + name
	jidPattern := ""
	if jid > 0 {
		jidPattern = "jail:" + strconv.Itoa(jid)
	}

	seen := map[string]struct{}{}
	var rules []string
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.Contains(line, namePattern) || (jidPattern != "" && strings.Contains(line, jidPattern)) {
			if _, exists := seen[line]; exists {
				continue
			}
			seen[line] = struct{}{}
			rules = append(rules, line)
		}
	}
	return rules, nil
}

func parseKVFields(line string) map[string]string {
	fields := map[string]string{}
	for _, token := range strings.Fields(line) {
		key, value, ok := strings.Cut(token, "=")
		if !ok {
			continue
		}
		fields[key] = strings.Trim(value, `"`)
	}
	return fields
}

func extractJailBlock(content, jailName string) ([]string, bool) {
	lines := strings.Split(content, "\n")
	openPattern := regexp.MustCompile(`^\s*` + regexp.QuoteMeta(jailName) + `\s*\{`)
	start := -1
	depth := 0

	for idx, line := range lines {
		if !openPattern.MatchString(line) {
			continue
		}
		start = idx
		depth = strings.Count(line, "{") - strings.Count(line, "}")
		if depth <= 0 {
			depth = 1
		}
		break
	}
	if start < 0 {
		return nil, false
	}

	var block []string
	for idx := start + 1; idx < len(lines); idx++ {
		line := lines[idx]
		nextDepth := depth + strings.Count(line, "{") - strings.Count(line, "}")
		if nextDepth <= 0 {
			break
		}
		block = append(block, line)
		depth = nextDepth
	}
	return block, true
}

func parseJailBlockLines(lines []string) (map[string]string, []string) {
	values := map[string]string{}
	var flags []string

	for _, raw := range lines {
		trimmed := strings.TrimSpace(stripInlineComment(raw))
		if trimmed == "" {
			continue
		}
		trimmed = strings.TrimSuffix(trimmed, ";")
		if key, val, ok := strings.Cut(trimmed, "="); ok {
			key = strings.TrimSpace(key)
			val = strings.TrimSpace(val)
			values[key] = strings.Trim(val, `"`)
			continue
		}
		flags = append(flags, trimmed)
	}
	sort.Strings(flags)
	return values, flags
}

func stripInlineComment(line string) string {
	inQuotes := false
	for idx := 0; idx < len(line); idx++ {
		switch line[idx] {
		case '"':
			inQuotes = !inQuotes
		case '#':
			if !inQuotes {
				return line[:idx]
			}
		}
	}
	return line
}
