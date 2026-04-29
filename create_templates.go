package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type TemplateDatasetResult struct {
	Dataset    string
	Mountpoint string
	Parent     string
	Logs       []string
	Err        error
}

type TemplateDatasetPreview struct {
	SourceInput       string
	SourceKind        string
	ResolvedSource    string
	Action            string
	PatchSelected     bool
	PatchEligible     bool
	PatchRelease      string
	PatchNote         string
	ParentDataset     string
	ParentMountpoint  string
	Dataset           string
	Mountpoint        string
	NeedsParentCreate bool
	Err               error
}

type TemplateParentDatasetResult struct {
	Dataset    string
	Mountpoint string
	Logs       []string
	Err        error
}

func hostArch() string {
	out, err := exec.Command("uname", "-m").Output()
	if err != nil {
		return "amd64"
	}
	arch := strings.TrimSpace(string(out))
	if arch == "" {
		return "amd64"
	}
	return arch
}

func ExecuteTemplateDatasetCreateWithParent(ctx context.Context, sourceInput string, parentOverride *templateDatasetParent, patchPreference string) TemplateDatasetResult {
	result := TemplateDatasetResult{}
	logs := make([]string, 0, 24)
	fail := func(err error) TemplateDatasetResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	sourceInput = strings.TrimSpace(sourceInput)
	if sourceInput == "" {
		return fail(fmt.Errorf("template/release is required before a template dataset can be created"))
	}

	parent, err := resolveTemplateDatasetParent(parentOverride)
	if err != nil {
		return fail(err)
	}
	templateName := suggestTemplateDatasetName(sourceInput)
	if templateName == "" {
		return fail(fmt.Errorf("could not derive a template dataset name from %q", sourceInput))
	}

	sourcePath, cleanup, err := resolveTemplateSource(ctx, sourceInput, &logs, nil)
	if err != nil {
		return fail(err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	patchDecision := resolveFreeBSDPatchDecision(sourceInput, patchPreference)
	if patchDecision.Err != nil {
		return fail(patchDecision.Err)
	}

	childDataset := parent.Name + "/" + templateName
	childMountpoint := filepath.Join(parent.Mountpoint, templateName)
	validatedChildDataset, err := validateZFSDatasetName(childDataset, "template dataset")
	if err != nil {
		return fail(err)
	}
	validatedChildMountpoint, err := validateAbsolutePath(childMountpoint, "template mountpoint")
	if err != nil {
		return fail(err)
	}
	if validatedChildMountpoint, err = validateUnusedMountpointPath(validatedChildMountpoint, "template mountpoint"); err != nil {
		return fail(err)
	}
	result.Parent = parent.Name
	result.Dataset = validatedChildDataset
	result.Mountpoint = validatedChildMountpoint

	if zfsDatasetExists(result.Dataset) {
		return fail(fmt.Errorf("template dataset %q already exists", result.Dataset))
	}

	if _, err := runLoggedCommand(ctx, &logs, "zfs", "create", "-o", "mountpoint="+result.Mountpoint, result.Dataset); err != nil {
		return fail(fmt.Errorf("failed to create template dataset %q: %w", result.Dataset, err))
	}

	success := false
	defer func() {
		if success {
			return
		}
		_, _ = runLoggedCommand(ctx, &logs, "zfs", "destroy", "-r", result.Dataset)
	}()

	info, err := os.Stat(sourcePath)
	if err != nil {
		return fail(fmt.Errorf("template source %q is not accessible: %w", sourcePath, err))
	}
	if info.IsDir() {
		if _, err := runLoggedCommand(ctx, &logs, "cp", "-a", sourcePath+"/.", result.Mountpoint+"/"); err != nil {
			return fail(fmt.Errorf("failed to copy template source into %q: %w", result.Dataset, err))
		}
	} else {
		if _, err := runLoggedCommand(ctx, &logs, "tar", "-xf", sourcePath, "-C", result.Mountpoint); err != nil {
			return fail(fmt.Errorf("failed to extract template archive into %q: %w", result.Dataset, err))
		}
	}
	if patchDecision.Effective {
		if err := patchFreeBSDRoot(ctx, result.Mountpoint, &logs); err != nil {
			return fail(err)
		}
	}
	if err := finalizeTemplateDatasetReadonly(ctx, result.Dataset, &logs); err != nil {
		return fail(err)
	}

	success = true
	result.Logs = logs
	return result
}

func InspectTemplateDatasetCreateWithParent(sourceInput string, parentOverride *templateDatasetParent, patchPreference string) TemplateDatasetPreview {
	preview := TemplateDatasetPreview{
		SourceInput: strings.TrimSpace(sourceInput),
	}
	patchDecision := resolveFreeBSDPatchDecision(preview.SourceInput, patchPreference)
	preview.PatchSelected = patchDecision.Effective
	preview.PatchEligible = patchDecision.Eligible
	preview.PatchRelease = patchDecision.ReleaseVersion
	preview.PatchNote = patchDecision.Note
	if patchDecision.Err != nil {
		preview.Err = patchDecision.Err
		return preview
	}

	parent, err := resolveTemplateDatasetParent(parentOverride)
	if err != nil {
		if errors.Is(err, errTemplateDatasetParentMissing) {
			preview.NeedsParentCreate = true
		}
		preview.Err = err
		return preview
	}
	preview.ParentDataset = parent.Name
	preview.ParentMountpoint = parent.Mountpoint

	if preview.SourceInput == "" {
		return preview
	}

	templateName := suggestTemplateDatasetName(preview.SourceInput)
	if templateName == "" {
		preview.Err = fmt.Errorf("could not derive a template dataset name from %q", preview.SourceInput)
		return preview
	}
	preview.Dataset = parent.Name + "/" + templateName
	preview.Mountpoint = filepath.Join(parent.Mountpoint, templateName)
	if preview.Dataset, err = validateZFSDatasetName(preview.Dataset, "template dataset"); err != nil {
		preview.Err = err
		return preview
	}
	if preview.Mountpoint, err = validateAbsolutePath(preview.Mountpoint, "template mountpoint"); err != nil {
		preview.Err = err
		return preview
	}
	if preview.Mountpoint, err = validateUnusedMountpointPath(preview.Mountpoint, "template mountpoint"); err != nil {
		preview.Err = err
		return preview
	}

	sourceKind, resolvedSource, action, err := inspectTemplateSourceInput(preview.SourceInput)
	preview.SourceKind = sourceKind
	preview.ResolvedSource = resolvedSource
	preview.Action = action
	if err != nil {
		preview.Err = err
		return preview
	}

	if _, err := exec.Command("zfs", "list", "-H", "-o", "name", preview.Dataset).Output(); err == nil {
		preview.Err = fmt.Errorf("template dataset %q already exists", preview.Dataset)
	}

	return preview
}

func ExecuteTemplateParentDatasetCreate(ctx context.Context, dataset, mountpoint string) TemplateParentDatasetResult {
	result := TemplateParentDatasetResult{
		Dataset:    strings.TrimSpace(dataset),
		Mountpoint: strings.TrimSpace(mountpoint),
	}
	var logs []string
	fail := func(err error) TemplateParentDatasetResult {
		result.Logs = logs
		result.Err = err
		return result
	}

	var err error
	if result.Dataset, err = validateZFSDatasetName(result.Dataset, "parent dataset"); err != nil {
		return fail(err)
	}
	if result.Mountpoint, err = validateAbsolutePath(result.Mountpoint, "parent mountpoint"); err != nil {
		return fail(err)
	}

	out, err := exec.Command("zfs", "list", "-H", "-o", "mountpoint", result.Dataset).CombinedOutput()
	if err == nil {
		existing := strings.TrimSpace(strings.Split(string(out), "\n")[0])
		if existing == "" || existing == "-" || existing == "legacy" {
			return fail(fmt.Errorf("dataset %q already exists with unusable mountpoint %q; set a real mountpoint or choose a different dataset", result.Dataset, existing))
		}
		if existing != "" && existing != "-" && existing != "legacy" && filepath.Clean(existing) != result.Mountpoint {
			return fail(fmt.Errorf("dataset %q already exists with mountpoint %q", result.Dataset, existing))
		}
		result.Logs = logs
		return result
	}
	if zfsDatasetExists(result.Dataset) {
		text := strings.TrimSpace(string(out))
		if text == "" {
			text = err.Error()
		}
		return fail(fmt.Errorf("failed to inspect mountpoint for existing dataset %q: %s", result.Dataset, text))
	}
	if result.Mountpoint, err = validateUnusedMountpointPath(result.Mountpoint, "parent mountpoint"); err != nil {
		return fail(err)
	}

	if _, err := runLoggedCommand(ctx, &logs, "zfs", "create", "-o", "mountpoint="+result.Mountpoint, result.Dataset); err != nil {
		return fail(fmt.Errorf("failed to create template parent dataset %q: %w", result.Dataset, err))
	}
	result.Logs = logs
	return result
}

func linuxBootstrapConfigFromRawLines(lines []string) jailWizardValues {
	values := jailWizardValues{}
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if !strings.Contains(line, "freebsd-jails-tui:") {
			continue
		}
		parts := strings.SplitN(line, "freebsd-jails-tui:", 2)
		if len(parts) != 2 {
			continue
		}
		meta := strings.TrimSpace(strings.TrimSuffix(parts[1], ";"))
		for _, token := range strings.Fields(meta) {
			key, value, ok := strings.Cut(token, "=")
			if !ok {
				continue
			}
			switch strings.TrimSpace(key) {
			case "linux_preset":
				values.LinuxPreset = strings.TrimSpace(value)
			case "linux_distro":
				values.LinuxDistro = strings.TrimSpace(value)
			case "linux_bootstrap_method":
				values.LinuxBootstrapMethod = strings.TrimSpace(value)
			case "linux_release":
				values.LinuxRelease = strings.TrimSpace(value)
			case "linux_bootstrap":
				values.LinuxBootstrap = strings.TrimSpace(value)
			case "linux_mirror_mode":
				values.LinuxMirrorMode = strings.TrimSpace(value)
			case "linux_mirror_url":
				if strings.TrimSpace(value) != "-" {
					values.LinuxMirrorURL = decodeTUIMetadataValue(strings.TrimSpace(value))
				}
			case "linux_archive_url":
				if strings.TrimSpace(value) != "-" {
					values.LinuxArchiveURL = decodeTUIMetadataValue(strings.TrimSpace(value))
				}
			}
		}
	}
	if strings.TrimSpace(values.LinuxDistro) == "" {
		re := regexp.MustCompile(`/compat/([^/\s"]+)`)
		for _, raw := range lines {
			matches := re.FindStringSubmatch(raw)
			if len(matches) == 2 {
				values.LinuxDistro = matches[1]
				break
			}
		}
	}
	return values
}

type templateDatasetParent struct {
	Name       string
	Mountpoint string
}

func discoverTemplateDatasetParent() (*templateDatasetParent, error) {
	out, err := exec.Command("zfs", "list", "-H", "-o", "name,mountpoint", "-t", "filesystem").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to discover templates dataset: %w", err)
	}

	var fallback *templateDatasetParent
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
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
		parent := &templateDatasetParent{Name: name, Mountpoint: mountpoint}
		if name == docDatasetTemplate || filepath.Clean(mountpoint) == filepath.Join(docJailsPath, "templates") {
			return parent, nil
		}
		if filepath.Base(name) == "templates" || filepath.Base(mountpoint) == "templates" {
			if fallback == nil {
				fallback = parent
			}
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("%w: no templates dataset found; create one first in the initial config check or under your jail dataset layout", errTemplateDatasetParentMissing)
}

func resolveTemplateDatasetParent(parentOverride *templateDatasetParent) (*templateDatasetParent, error) {
	if parentOverride != nil {
		name := strings.TrimSpace(parentOverride.Name)
		mountpoint := strings.TrimSpace(parentOverride.Mountpoint)
		if name != "" && mountpoint != "" {
			validatedName, err := validateZFSDatasetName(name, "parent dataset")
			if err != nil {
				return nil, err
			}
			validatedMountpoint, err := validateAbsolutePath(mountpoint, "parent mountpoint")
			if err != nil {
				return nil, err
			}
			return &templateDatasetParent{Name: validatedName, Mountpoint: validatedMountpoint}, nil
		}
	}
	return discoverTemplateDatasetParent()
}

func inspectTemplateSourceInput(input string) (string, string, string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", "", fmt.Errorf("template/release is required")
	}

	if info, err := os.Stat(input); err == nil {
		cleanInput, pathErr := validateAbsolutePath(input, "template/release path")
		if pathErr != nil {
			return "", "", "", pathErr
		}
		if info.IsDir() {
			return "local directory", cleanInput, "copy directory contents into the new dataset", nil
		}
		return "local archive", cleanInput, "extract archive into the new dataset", nil
	}

	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		info, err := os.Stat(source)
		if err != nil {
			return "", "", "", fmt.Errorf("named userland source %q is not accessible: %w", source, err)
		}
		if info.IsDir() {
			return "named userland directory", source, "copy directory contents into the new dataset", nil
		}
		return "named userland archive", source, "extract archive into the new dataset", nil
	}

	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		parsed, err := neturl.Parse(input)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return "", "", "", fmt.Errorf("template/release URL %q is invalid", input)
		}
		return "custom URL", input, "download and extract archive into the new dataset", nil
	}

	if releaseValuePattern.MatchString(strings.ToUpper(input)) {
		localBaseArchive := "/usr/freebsd-dist/base.txz"
		if _, err := os.Stat(localBaseArchive); err == nil {
			return "release tag", localBaseArchive, "extract archive into the new dataset", nil
		}
		if source, ok := findReleaseArchiveInUserland(defaultUserlandDir, input); ok {
			return "release tag", source, "extract archive into the new dataset", nil
		}
		urls, err := defaultReleaseBaseURLs(input)
		if err != nil {
			return "", "", "", err
		}
		if len(urls) == 0 {
			return "", "", "", fmt.Errorf("could not resolve a download URL for release %q", input)
		}
		return "release tag", urls[0], "download and extract archive into the new dataset", nil
	}

	return "", "", "", fmt.Errorf(
		"template/release %q not found; use a local path, an entry from %s, a release tag, or a custom URL",
		input,
		defaultUserlandDir,
	)
}

func suggestTemplateDatasetName(sourceInput string) string {
	input := strings.TrimSpace(sourceInput)
	if input == "" {
		return ""
	}
	upper := strings.ToUpper(input)
	if releaseValuePattern.MatchString(upper) {
		return sanitizeTemplateDatasetName(strings.ToLower(input))
	}
	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		if parsed, err := neturl.Parse(input); err == nil {
			base := pathBaseNoExt(parsed.Path)
			parent := pathBaseNoExt(filepath.Dir(parsed.Path))
			if strings.EqualFold(base, "base") && releaseValuePattern.MatchString(strings.ToUpper(parent)) {
				return sanitizeTemplateDatasetName(strings.ToLower(parent))
			}
			return sanitizeTemplateDatasetName(strings.ToLower(base))
		}
	}
	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		return suggestTemplateDatasetName(source)
	}
	base := filepath.Base(input)
	parent := filepath.Base(filepath.Dir(input))
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if strings.EqualFold(base, "base") && parent != "." && parent != string(filepath.Separator) {
		base = parent
	}
	return sanitizeTemplateDatasetName(strings.ToLower(base))
}

func sanitizeTemplateDatasetName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteRune('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func pathBaseNoExt(raw string) string {
	base := filepath.Base(strings.TrimSpace(raw))
	return strings.TrimSuffix(base, filepath.Ext(base))
}

func ensureJailConfigDoesNotExist(configPath string) error {
	if _, err := os.Stat(configPath); err == nil {
		return fmt.Errorf("config file %q already exists", configPath)
	}
	return nil
}

func resolveTemplateSource(ctx context.Context, input string, logs *[]string, progressChan chan<- downloadProgressMsg) (string, func(), error) {
	if input == "" {
		return "", nil, fmt.Errorf("template/release is required")
	}

	// Explicit filesystem path wins.
	if _, err := os.Stat(input); err == nil {
		cleanInput, pathErr := validateAbsolutePath(input, "template/release path")
		if pathErr != nil {
			return "", nil, pathErr
		}
		return cleanInput, nil, nil
	}

	// Shortcut: entry name from userland media directory.
	if source, ok := findNamedUserlandSource(defaultUserlandDir, input); ok {
		return source, nil, nil
	}

	// Full URL: download and extract.
	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		parsed, _ := neturl.Parse(input)
		filename := filepath.Base(parsed.Path)
		if filename == "" || filename == "/" || filename == "." {
			filename = "downloaded.txz"
		}
		targetPath := filepath.Join(defaultUserlandDir, "custom", filename)
		return downloadArchiveToPath(ctx, input, targetPath, logs, progressChan)
	}

	// Release tag: local archive, then media dir, then download.freebsd.org.
	if releaseValuePattern.MatchString(strings.ToUpper(input)) {
		localBaseArchive := "/usr/freebsd-dist/base.txz"
		if _, err := os.Stat(localBaseArchive); err == nil {
			return localBaseArchive, nil, nil
		}
		if source, ok := findReleaseArchiveInUserland(defaultUserlandDir, input); ok {
			return source, nil, nil
		}

		return downloadReleaseArchiveToTemp(ctx, input, logs, progressChan)
	}

	return "", nil, fmt.Errorf(
		"template/release %q not found; use a local path, an entry from %s, a release tag, or a custom URL",
		input,
		defaultUserlandDir,
	)
}

func defaultReleaseBaseURLs(release string) ([]string, error) {
	arch := hostArch()
	release = strings.ToUpper(strings.TrimSpace(release))
	urls := []string{
		// FreeBSD release directory layout commonly uses arch/arch/<release>/base.txz.
		fmt.Sprintf("%s/ftp/releases/%s/%s/%s/base.txz", defaultDownloadHost, arch, arch, release),
		// Compatibility fallback.
		fmt.Sprintf("%s/ftp/releases/%s/%s/base.txz", defaultDownloadHost, arch, release),
	}
	return urls, nil
}

func downloadReleaseArchiveToTemp(ctx context.Context, release string, logs *[]string, progressChan chan<- downloadProgressMsg) (string, func(), error) {
	urls, err := defaultReleaseBaseURLs(release)
	if err != nil {
		return "", nil, err
	}

	targetPath := filepath.Join(defaultUserlandDir, release, "base.txz")

	var lastErr error
	for _, url := range urls {
		path, cleanup, err := downloadArchiveToPath(ctx, url, targetPath, logs, progressChan)
		if err == nil {
			return path, cleanup, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("release download failed")
	}
	return "", nil, fmt.Errorf("unable to download release %s: %w", release, lastErr)
}

type progressReader struct {
	io.Reader
	total int64
	read  int64
	ch    chan<- downloadProgressMsg
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.Reader.Read(p)
	if n > 0 {
		pr.read += int64(n)
		if pr.ch != nil {
			select {
			case pr.ch <- downloadProgressMsg{Read: pr.read, Total: pr.total}:
			default:
			}
		}
	}
	return n, err
}

func downloadArchiveToPath(ctx context.Context, url string, destPath string, logs *[]string, progressChan chan<- downloadProgressMsg) (string, func(), error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return "", nil, fmt.Errorf("download URL is empty")
	}
	*logs = append(*logs, "$ fetch "+url)
	client := &http.Client{Timeout: archiveDownloadTimeout}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil) // #nosec G107 user-provided URL is intentional
	if err != nil {
		return "", nil, fmt.Errorf("failed creating request for %s: %w", url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("failed downloading %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", nil, fmt.Errorf("download failed from %s: http %d", url, resp.StatusCode)
	}

	destDir := filepath.Dir(destPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", nil, fmt.Errorf("failed creating media directory %q: %w", destDir, err)
	}

	// Use a temporary file for the download to avoid corrupted partial files
	tmpPath := destPath + ".part"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed creating temp archive: %w", err)
	}

	reader := &progressReader{
		Reader: resp.Body,
		total:  resp.ContentLength,
		ch:     progressChan,
	}

	if _, err := io.Copy(tmp, reader); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", nil, fmt.Errorf("failed writing temp archive: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return "", nil, fmt.Errorf("failed closing temp archive: %w", err)
	}

	// Rename partial file to final destination
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return "", nil, fmt.Errorf("failed renaming downloaded archive: %w", err)
	}

	*logs = append(*logs, "  downloaded to "+destPath)
	// Return a nil cleanup function so the downloaded archive is kept even if jail creation fails
	return destPath, nil, nil
}

func discoverUserlandSources(userlandDir string) ([]string, error) {
	entries, err := os.ReadDir(userlandDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed reading userland directory %q: %w", userlandDir, err)
	}
	var sources []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		fullPath := filepath.Join(userlandDir, entry.Name())
		if entry.IsDir() {
			baseArchive := filepath.Join(fullPath, "base.txz")
			if _, err := os.Stat(baseArchive); err == nil {
				sources = append(sources, baseArchive)
				continue
			}
			sources = append(sources, fullPath)
			continue
		}
		sources = append(sources, fullPath)
	}
	sort.Strings(sources)
	return sources, nil
}

func findNamedUserlandSource(userlandDir, input string) (string, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", false
	}
	sources, err := discoverUserlandSources(userlandDir)
	if err != nil {
		return "", false
	}
	lowerInput := strings.ToLower(input)
	for _, source := range sources {
		base := strings.ToLower(filepath.Base(source))
		noExt := strings.TrimSuffix(base, filepath.Ext(base))
		if base == lowerInput || noExt == lowerInput {
			return source, true
		}
		parent := strings.ToLower(filepath.Base(filepath.Dir(source)))
		if parent == lowerInput {
			return source, true
		}
	}
	return "", false
}

func findReleaseArchiveInUserland(userlandDir, release string) (string, bool) {
	sources, err := discoverUserlandSources(userlandDir)
	if err != nil {
		return "", false
	}
	release = strings.ToLower(strings.TrimSpace(release))
	for _, source := range sources {
		text := strings.ToLower(source)
		if strings.Contains(text, release) {
			return source, true
		}
	}
	return "", false
}

func finalizeTemplateDatasetReadonly(ctx context.Context, dataset string, logs *[]string) error {
	dataset = strings.TrimSpace(dataset)
	if dataset == "" {
		return fmt.Errorf("template dataset is required")
	}
	if _, err := runLoggedCommand(ctx, logs, "zfs", "set", "readonly=on", dataset); err != nil {
		return fmt.Errorf("failed to finalize template dataset %q as readonly: %w", dataset, err)
	}
	return nil
}
