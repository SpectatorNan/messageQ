package example

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// getTestDataDir returns a test data directory. When cleanTestData is true, a temp dir is used.
// When false, a unique directory under ./testdata is created and preserved for inspection.
func getTestDataDir(t *testing.T, name string) string {
	t.Helper()
	if cleanTestData {
		return t.TempDir()
	}
	base := "./testdata"
	_ = os.MkdirAll(base, 0o755)
	dir := filepath.Join(base, name+"_"+time.Now().Format("20060102_150405.000000000"))
	_ = os.MkdirAll(dir, 0o755)
	t.Logf("test data dir: %s", dir)
	return dir
}
