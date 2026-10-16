package goframework

import (
	"os"
	"path/filepath"
	"testing"
)

func newConfigDir(t *testing.T, content string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "configs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "configs", "test.json"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	t.Setenv("env", "test")
}

func TestViperReadsFromFileWhenEnvIsAbsent(t *testing.T) {
	newConfigDir(t, `{"kafka":{"groupid":"file"},"redis":{"db":3}}`)

	v := initializeViper()

	if got := v.GetString("kafka.groupid"); got != "file" {
		t.Fatalf("kafka.groupid = %q, want %q", got, "file")
	}
	if got := v.GetInt("redis.db"); got != 3 {
		t.Fatalf("redis.db = %d, want 3", got)
	}
}

func TestViperEnvOverridesFile(t *testing.T) {
	newConfigDir(t, `{"kafka":{"groupid":"file"}}`)
	t.Setenv("KAFKA_GROUPID", "env")

	v := initializeViper()

	if got := v.GetString("kafka.groupid"); got != "env" {
		t.Fatalf("kafka.groupid = %q, want %q", got, "env")
	}
}

// Secrets can be removed from the file and provided only through the environment.
func TestViperEnvOnlyKeyWithoutFileEntry(t *testing.T) {
	newConfigDir(t, `{"redis":{"hosts":["localhost:6379"]}}`)
	t.Setenv("REDIS_PASS", "secret")

	v := initializeViper()

	if got := v.GetString("redis.pass"); got != "secret" {
		t.Fatalf("redis.pass = %q, want %q", got, "secret")
	}
	if got := v.GetStringSlice("redis.hosts"); len(got) != 1 || got[0] != "localhost:6379" {
		t.Fatalf("redis.hosts = %v, want file value", got)
	}
}

func TestViperMissingConfigFileIsTolerated(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("env", "missing")
	t.Setenv("KAFKA_GROUPID", "env")

	v := initializeViper()

	if got := v.GetString("kafka.groupid"); got != "env" {
		t.Fatalf("kafka.groupid = %q, want %q", got, "env")
	}
}

func TestViperInvalidConfigFilePanics(t *testing.T) {
	newConfigDir(t, `{invalid`)

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for malformed config file")
		}
	}()
	initializeViper()
}
