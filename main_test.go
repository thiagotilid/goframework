package goframework

import (
	"os"
	"testing"
	"time"
)

// NewGoFramework sets time.Local to UTC. Doing it once up front keeps later calls from
// writing the global while goroutines left over by earlier tests are reading it.
func TestMain(m *testing.M) {
	time.Local = time.UTC
	os.Exit(m.Run())
}
