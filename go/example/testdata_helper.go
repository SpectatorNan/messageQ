package example

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const debugCleanup = false

// cleanTestData is controlled by environment variable
var cleanTestData = func() bool {
	v := os.Getenv("MSGQ_CLEAN_TESTDATA")
	if v == "" {
		return debugCleanup
	}
	switch strings.ToLower(v) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}()

// getTestDataDir returns a test data directory. When cleanTestData is true, a temp dir is used.
// When false, a unique directory under ./testdata is created and preserved for inspection.
func getTestDataDir(t *testing.T, name string) string {
	t.Helper()
	if cleanTestData {
		return t.TempDir()
	}
	base := "./testdata"
	_ = os.MkdirAll(base, 0o755)
	ts := time.Now().Format("20060102_150405.000000000")
	dir := filepath.Join(base, name+"_"+ts)
	_ = os.MkdirAll(dir, 0o755)
	t.Logf("test data dir: %s", dir)
	return dir
}
