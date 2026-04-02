package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type wizardTemplateStore struct {
	Templates []wizardTemplate `json:"templates"`
}

func saveWizardTemplate(name string, values jailWizardValues) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("template name is required")
	}

	templates, err := loadWizardTemplates()
	if err != nil {
		return err
	}

	entry := wizardTemplate{
		Name:   name,
		Values: values,
	}
	if strings.TrimSpace(entry.Values.JailType) == "" {
		entry.Values.JailType = "thick"
	}
	if strings.TrimSpace(entry.Values.Interface) == "" {
		entry.Values.Interface = "em0"
	}
	if normalizedJailType(entry.Values.JailType) == "vnet" && strings.TrimSpace(entry.Values.Bridge) == "" {
		entry.Values.Bridge = strings.TrimSpace(entry.Values.Interface)
	}

	replaced := false
	for idx := range templates {
		if templates[idx].Name == name {
			templates[idx] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		templates = append(templates, entry)
	}
	sort.Slice(templates, func(i, j int) bool {
		return strings.ToLower(templates[i].Name) < strings.ToLower(templates[j].Name)
	})
	return writeWizardTemplates(templates)
}

func loadWizardTemplates() ([]wizardTemplate, error) {
	path, err := wizardTemplateFilePath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []wizardTemplate{}, nil
		}
		return nil, fmt.Errorf("failed to read template file: %w", err)
	}

	var store wizardTemplateStore
	if err := json.Unmarshal(data, &store); err != nil {
		// Backward-compatible fallback for raw array format.
		var list []wizardTemplate
		if legacyErr := json.Unmarshal(data, &list); legacyErr != nil {
			return nil, fmt.Errorf("failed to parse template file: %w", err)
		}
		store.Templates = list
	}

	templates := make([]wizardTemplate, 0, len(store.Templates))
	for _, t := range store.Templates {
		t.Name = strings.TrimSpace(t.Name)
		if t.Name == "" {
			continue
		}
		if strings.TrimSpace(t.Values.JailType) == "" {
			t.Values.JailType = "thick"
		}
		if strings.TrimSpace(t.Values.Interface) == "" {
			t.Values.Interface = "em0"
		}
		if normalizedJailType(t.Values.JailType) == "vnet" && strings.TrimSpace(t.Values.Bridge) == "" {
			t.Values.Bridge = strings.TrimSpace(t.Values.Interface)
		}
		templates = append(templates, t)
	}
	sort.Slice(templates, func(i, j int) bool {
		return strings.ToLower(templates[i].Name) < strings.ToLower(templates[j].Name)
	})
	return templates, nil
}

func wizardTemplateFilePath() (string, error) {
	configDir, err := appConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, "templates.json"), nil
}

func writeWizardTemplates(templates []wizardTemplate) error {
	path, err := wizardTemplateFilePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("failed to create template directory: %w", err)
	}

	store := wizardTemplateStore{Templates: templates}
	payload, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize templates: %w", err)
	}
	payload = append(payload, '\n')

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return fmt.Errorf("failed to write template file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("failed to finalize template file: %w", err)
	}
	return nil
}

func findWizardTemplateReleaseReferences(mountpoint string) ([]string, error) {
	templates, err := loadWizardTemplates()
	if err != nil {
		return nil, err
	}
	target := filepath.Clean(strings.TrimSpace(mountpoint))
	if target == "." || target == "" {
		return nil, nil
	}
	refs := make([]string, 0)
	for _, tmpl := range templates {
		value := strings.TrimSpace(tmpl.Values.TemplateRelease)
		if value == "" || !strings.HasPrefix(value, "/") {
			continue
		}
		if filepath.Clean(value) == target {
			refs = append(refs, tmpl.Name)
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		return strings.ToLower(refs[i]) < strings.ToLower(refs[j])
	})
	return refs, nil
}

func rewriteWizardTemplateReleaseReferences(oldMountpoint, newMountpoint string) ([]string, error) {
	templates, err := loadWizardTemplates()
	if err != nil {
		return nil, err
	}
	oldClean := filepath.Clean(strings.TrimSpace(oldMountpoint))
	newClean := filepath.Clean(strings.TrimSpace(newMountpoint))
	if oldClean == "." || oldClean == "" {
		return nil, fmt.Errorf("old template mountpoint is required")
	}
	if newClean == "." || newClean == "" {
		return nil, fmt.Errorf("new template mountpoint is required")
	}
	updated := make([]string, 0)
	for idx := range templates {
		value := strings.TrimSpace(templates[idx].Values.TemplateRelease)
		if value == "" || !strings.HasPrefix(value, "/") {
			continue
		}
		if filepath.Clean(value) != oldClean {
			continue
		}
		templates[idx].Values.TemplateRelease = newClean
		updated = append(updated, templates[idx].Name)
	}
	if len(updated) == 0 {
		return nil, nil
	}
	if err := writeWizardTemplates(templates); err != nil {
		return nil, err
	}
	sort.Slice(updated, func(i, j int) bool {
		return strings.ToLower(updated[i]) < strings.ToLower(updated[j])
	})
	return updated, nil
}

func appConfigDir() (string, error) {
	if xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); xdg != "" {
		return filepath.Join(xdg, "freebsd-jails-tui"), nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("failed to locate user config directory: %w", err)
	}
	return filepath.Join(dir, "freebsd-jails-tui"), nil
}
