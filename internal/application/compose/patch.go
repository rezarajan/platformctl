package compose

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// piece is one candidate resource a Plan function assembles before
// resolving it into a FileOp (the collision/idempotency check): kind+name
// identify it, content is its already-rendered YAML text.
type piece struct {
	kind, name, content string
}

// FileOp is one file a Patch would write.
type FileOp struct {
	Path      string // relative to the target dir
	Content   string
	New       bool // the file does not exist yet
	Identical bool // exists already, byte-identical to Content -> no-op
}

// EnvAppend is one KEY=VALUE line proposed for the target dir's .env.
// Pending reports whether the key is not already present (a second run
// with identical answers must not duplicate or clobber a filled-in
// value — the idempotent-regeneration bar, ADR 024).
type EnvAppend struct {
	Key     string
	Default string
	Comment string
	Pending bool
}

// Patch is the manifest patch a compose command proposes: files to write
// and .env keys to append, plus warnings/notes for human display. Nothing
// else — ADR 024's one rule ("composition compiles to manifests; it never
// bypasses them").
type Patch struct {
	Command    string // e.g. "add pipeline", "wire cdc", "expose Source/app-db"
	Dir        string
	Files      []FileOp
	EnvAppends []EnvAppend
	Warnings   []string
	Notes      []string // human-facing reuse notes ("reusing broker Provider \"broker\"")
}

// HasChanges reports whether applying this Patch would write or modify
// anything — the idempotent-regeneration bar: re-running a compose command
// with unchanged answers must report zero changes.
func (p Patch) HasChanges() bool {
	for _, f := range p.Files {
		if f.New || !f.Identical {
			return true
		}
	}
	for _, a := range p.EnvAppends {
		if a.Pending {
			return true
		}
	}
	return false
}

// FilePath is the deterministic destination path compose writes a
// generated resource to: one file per Kind+name, so two composites never
// collide on filename and a second run of the same composite with the
// same name always targets the same file (the idempotency mechanism below
// depends on this).
func FilePath(kind, name string) string {
	prefix := map[string]string{
		"Provider":        "provider",
		"Source":          "source",
		"EventStream":     "eventstream",
		"Binding":         "binding",
		"Dataset":         "dataset",
		"SecretReference": "secret",
		"Connection":      "connection",
		"Catalog":         "catalog",
	}[kind]
	if prefix == "" {
		prefix = strings.ToLower(kind)
	}
	return fmt.Sprintf("%s-%s.yaml", prefix, name)
}

// resolveFile decides whether writing content for kind/name is a new file,
// an already-satisfied no-op, or a collision — and it is a collision by
// construction whenever anything would be silently overwritten, per ADR
// 024's naming rule. Content must be a pure, deterministic function of the
// Plan's inputs (it always is, in this package: no timestamps, no random
// names) so that regenerating from identical flags reproduces
// byte-identical output and this resolves to Identical rather than a
// spurious collision.
func resolveFile(dir string, snap Snapshot, kind, name, content string) (FileOp, error) {
	path := FilePath(kind, name)
	full := filepath.Join(dir, path)
	existing, err := os.ReadFile(full)
	switch {
	case err == nil:
		if string(existing) == content {
			return FileOp{Path: path, Content: content, Identical: true}, nil
		}
		return FileOp{}, fmt.Errorf("%s %q: %s already exists with different content — nothing is ever overwritten; choose a different name", kind, name, path)
	case os.IsNotExist(err):
		if snap.NameExists(kind, name) {
			return FileOp{}, fmt.Errorf("%s %q already exists in the manifest set (in a different file) — choose a different name", kind, name)
		}
		return FileOp{Path: path, Content: content, New: true}, nil
	default:
		return FileOp{}, fmt.Errorf("%s %q: reading %s: %w", kind, name, path, err)
	}
}

// readEnvKeys reads dir/.env (if present; a missing file is not an error)
// and returns the set of keys already assigned, so pendingEnvAppends can
// skip anything already filled in.
func readEnvKeys(dir string) (map[string]bool, error) {
	keys := map[string]bool{}
	data, err := os.ReadFile(filepath.Join(dir, ".env"))
	if err != nil {
		if os.IsNotExist(err) {
			return keys, nil
		}
		return nil, err
	}
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, _, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		keys[strings.TrimSpace(key)] = true
	}
	return keys, sc.Err()
}

// resolveEnvAppends marks each proposed EnvAppend Pending against dir's
// current .env contents.
func resolveEnvAppends(dir string, appends []EnvAppend) ([]EnvAppend, error) {
	existing, err := readEnvKeys(dir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", filepath.Join(dir, ".env"), err)
	}
	out := make([]EnvAppend, len(appends))
	for i, a := range appends {
		a.Pending = !existing[a.Key]
		out[i] = a
	}
	return out, nil
}

// Write applies a Patch to disk: writes every New file, and appends every
// still-Pending EnvAppend to dir/.env under a provenance-commented section.
// Files/keys that already resolved as Identical/non-pending are left
// untouched. Returns the paths written and env keys appended, both sorted,
// for CLI reporting.
func Write(patch Patch) (filesWritten []string, envKeysAppended []string, err error) {
	for _, f := range patch.Files {
		if !f.New {
			continue
		}
		full := filepath.Join(patch.Dir, f.Path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return nil, nil, fmt.Errorf("creating %s: %w", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(f.Content), 0o644); err != nil {
			return nil, nil, fmt.Errorf("writing %s: %w", full, err)
		}
		filesWritten = append(filesWritten, f.Path)
	}

	var pending []EnvAppend
	for _, a := range patch.EnvAppends {
		if a.Pending {
			pending = append(pending, a)
		}
	}
	if len(pending) > 0 {
		envPath := filepath.Join(patch.Dir, ".env")
		f, openErr := os.OpenFile(envPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if openErr != nil {
			return nil, nil, fmt.Errorf("opening %s: %w", envPath, openErr)
		}
		var b strings.Builder
		fmt.Fprintf(&b, "\n# appended by platformctl %s; fill in real values (docs/planning/08 E9)\n", patch.Command)
		for _, a := range pending {
			if a.Comment != "" {
				fmt.Fprintf(&b, "# %s\n", a.Comment)
			}
			fmt.Fprintf(&b, "%s=%s\n", a.Key, a.Default)
			envKeysAppended = append(envKeysAppended, a.Key)
		}
		if _, writeErr := f.WriteString(b.String()); writeErr != nil {
			_ = f.Close() // a different error is already being returned; this is best-effort cleanup
			return nil, nil, fmt.Errorf("appending to %s: %w", envPath, writeErr)
		}
		// Check the final Close, not just the write: matches
		// localfile.go's convention (docs/planning/11 B4) — a failed
		// Close after a successful Write can still mean the data never
		// made it to disk.
		if err := f.Close(); err != nil {
			return nil, nil, fmt.Errorf("closing %s: %w", envPath, err)
		}
	}

	sort.Strings(filesWritten)
	sort.Strings(envKeysAppended)
	return filesWritten, envKeysAppended, nil
}
