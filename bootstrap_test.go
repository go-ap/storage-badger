package badger

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
)

func createForbiddenDir(t *testing.T) string {
	forbiddenPath := filepath.Join(t.TempDir(), "forbidden")
	err := os.MkdirAll(forbiddenPath, 0o000)
	if err != nil {
		t.Fatalf("unable to create forbidden test path %s: %s", forbiddenPath, err)
	}
	return forbiddenPath
}

func TestBootstrap(t *testing.T) {
	forbiddenPath := createForbiddenDir(t)
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: os.ErrNotExist,
		},
		{
			name: "temp",
			arg:  Config{Path: filepath.Join(t.TempDir())},
		},
		{
			name:    "deeper than forbidden",
			arg:     Config{Path: filepath.Join(forbiddenPath, "should-fail")},
			wantErr: &fs.PathError{Op: "stat", Path: filepath.Join(forbiddenPath, "should-fail"), Err: syscall.EACCES},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Bootstrap(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Bootstrap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClean(t *testing.T) {
	forbiddenPath := createForbiddenDir(t)
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: os.ErrNotExist,
		},
		{
			name:    "temp - exists, but empty",
			arg:     Config{Path: t.TempDir()},
			wantErr: nil,
		},
		{
			name:    "temp - does not exists",
			arg:     Config{Path: filepath.Join(t.TempDir(), "test")},
			wantErr: nil,
		},
		{
			name:    "invalid path " + os.DevNull,
			arg:     Config{Path: os.DevNull},
			wantErr: errors.Errorf("path exists, and is not a folder %s", os.DevNull),
		},
		{
			name:    "forbidden",
			arg:     Config{Path: filepath.Join(forbiddenPath, "should-fail")},
			wantErr: &fs.PathError{Op: "stat", Path: filepath.Join(forbiddenPath, "should-fail"), Err: syscall.EACCES},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Clean(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Clean() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
		})
	}
}
