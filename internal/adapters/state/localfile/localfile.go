// Package localfile implements StateStore as a single JSON file, written via
// temp-file-then-rename for atomicity (NFR-9), guarded by an advisory flock.
// See docs/planning/02-architecture.md §4.3 and §7.
package localfile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/rezarajan/platformctl/internal/ports/state"
)

type Store struct {
	Path string
}

func New(path string) *Store { return &Store{Path: path} }

func (s *Store) Load(_ context.Context) (state.State, error) {
	st := state.State{Version: state.CurrentVersion}
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		st.Normalize()
		return st, nil
	}
	if err != nil {
		return st, fmt.Errorf("load state %s: %w", s.Path, err)
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, fmt.Errorf("parse state %s: %w", s.Path, err)
	}
	if st.Version > state.CurrentVersion {
		return st, fmt.Errorf("state file %s has version %d, newer than this binary supports (%d) — upgrade platformctl", s.Path, st.Version, state.CurrentVersion)
	}
	st.Normalize()
	return st, nil
}

func (s *Store) Save(_ context.Context, st state.State) error {
	st.Version = state.CurrentVersion
	st.Flatten()
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".state-*.json.tmp")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state file: %w", err)
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		return fmt.Errorf("rename temp state file into place: %w", err)
	}
	// Fsync the directory so the rename itself is durable — without this a
	// crash between rename and the next directory flush can lose the new
	// entry even though the file's own data was synced
	// (docs/planning/07 §1.4). Best-effort where the platform disallows
	// opening directories.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Lock takes an advisory flock on a sibling .lock file for the duration of
// plan/apply/destroy. A held lock fails fast with a recovery instruction.
func (s *Store) Lock(_ context.Context) (func() error, error) {
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir %s: %w", dir, err)
	}
	lockPath := s.Path + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("state file %s is locked by another platformctl process (lock: %s); if that process died, remove the lock file and retry", s.Path, lockPath)
	}
	// Record holder PID for stale-lock diagnosis.
	_ = f.Truncate(0)
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
	return func() error {
		defer f.Close()
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			return fmt.Errorf("unlock %s: %w", lockPath, err)
		}
		return os.Remove(lockPath)
	}, nil
}
