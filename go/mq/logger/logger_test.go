package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitWritesIntoDirectoryOutputPath(t *testing.T) {
	logDir := t.TempDir()

	err := Init(Config{
		Level:      InfoLevel,
		OutputPath: logDir,
		Format:     "json",
	})
	if err != nil {
		t.Fatalf("init logger with directory output path: %v", err)
	}
	defer func() {
		_ = Sync()
		Logger = nil
		Sugar = nil
	}()

	Info("directory target works")
	_ = Sync()

	logFile := filepath.Join(logDir, defaultLogFilename)
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(content), "directory target works") {
		t.Fatalf("expected log message in %s, got %q", logFile, string(content))
	}
}

func TestInitCreatesParentDirectoriesForFileOutputPath(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "nested", "server.log")

	err := Init(Config{
		Level:      InfoLevel,
		OutputPath: logFile,
		Format:     "json",
	})
	if err != nil {
		t.Fatalf("init logger with nested file path: %v", err)
	}
	defer func() {
		_ = Sync()
		Logger = nil
		Sugar = nil
	}()

	Info("nested file target works")
	_ = Sync()

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(content), "nested file target works") {
		t.Fatalf("expected log message in %s, got %q", logFile, string(content))
	}
}
