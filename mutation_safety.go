package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type fileMutationBackup struct {
	OriginalPath string
	BackupPath   string
}

func backupFileForMutation(path, category string, logs *[]string) (*fileMutationBackup, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to inspect %q before backup: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("refusing to back up non-regular file %q", path)
	}

	backupDir, err := mutationBackupDir(category)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory %q: %w", backupDir, err)
	}

	backupPath := filepath.Join(backupDir, filepath.Base(path))
	if err := copyFileContents(path, backupPath, info.Mode().Perm()); err != nil {
		return nil, fmt.Errorf("failed to create backup for %q: %w", path, err)
	}
	if logs != nil {
		*logs = append(*logs, fmt.Sprintf("backup: %s -> %s", path, backupPath))
	}
	return &fileMutationBackup{
		OriginalPath: path,
		BackupPath:   backupPath,
	}, nil
}

func restoreFileMutationBackup(backup *fileMutationBackup, logs *[]string) error {
	if backup == nil || strings.TrimSpace(backup.OriginalPath) == "" || strings.TrimSpace(backup.BackupPath) == "" {
		return nil
	}
	info, err := os.Stat(backup.BackupPath)
	if err != nil {
		return fmt.Errorf("failed to inspect backup %q: %w", backup.BackupPath, err)
	}
	if err := copyFileContents(backup.BackupPath, backup.OriginalPath, info.Mode().Perm()); err != nil {
		return fmt.Errorf("failed to restore %q from backup %q: %w", backup.OriginalPath, backup.BackupPath, err)
	}
	if logs != nil {
		*logs = append(*logs, fmt.Sprintf("restore: %s <- %s", backup.OriginalPath, backup.BackupPath))
	}
	return nil
}

func mutationBackupDir(category string) (string, error) {
	configDir, err := appConfigDir()
	if err != nil {
		return "", err
	}
	category = sanitizeBackupCategory(category)
	if category == "" {
		category = "general"
	}
	return filepath.Join(configDir, "backups", category, time.Now().Format("20060102-150405")), nil
}

func sanitizeBackupCategory(category string) string {
	category = strings.TrimSpace(strings.ToLower(category))
	if category == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range category {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	return strings.Trim(b.String(), "-")
}

func copyFileContents(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func writeFileAtomicReplace(dst string, content []byte, mode os.FileMode) error {
	dst = strings.TrimSpace(dst)
	if dst == "" {
		return fmt.Errorf("destination path is required")
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(dst)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func writeFileAtomicExclusive(dst string, content []byte, mode os.FileMode) error {
	dst = strings.TrimSpace(dst)
	if dst == "" {
		return fmt.Errorf("destination path is required")
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(dst)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpPath, dst); err != nil {
		if os.IsExist(err) {
			return os.ErrExist
		}
		return err
	}
	cleanup = false
	_ = os.Remove(tmpPath)
	return nil
}
