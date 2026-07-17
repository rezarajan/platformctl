# F-001: Machine-output contract violations — `graph -o json`, `validate -o json`, `inventory --for -o json`

**Severity:** Medium (CI/tooling consumers get unparseable output; a Gate 0
stage-gate completion claim is unsupported as written).
**Status:** Confirmed, reproduced at `ae99505`.

## Claim audited

`docs/planning/07-production-grade-docker-runtime-gap-analysis.md`:

- Gate 0 stage-gate: "[x] Machine-readable command output is valid JSON/YAML
  for every command".
- §0.5 required work: "For `json` and `yaml`, stdout must contain exactly
  one parseable document for every exit path."

## Evidence (reproduced)

```
$ platformctl graph examples/cdc-attendance/ -o json | head -1
DATA FLOW                          # ← not JSON; graph ignores -o entirely

$ platformctl validate examples/cdc-attendance/ -o json
14 resource(s) valid               # ← not JSON (success path of the gate command)

$ platformctl inventory examples/cdc-attendance/ --for spark -o json
# spark-defaults.conf — ...        # ← prose config snippet, not JSON
```

Piping each through `python3 -c "import json,sys; json.load(sys.stdin)"`
fails. By contrast `plan|apply|destroy|drift|status|import|inventory`
(without `--for`) emit exactly one parseable document (verified at the same
revision).

## Root cause

`-o|--output` is a root **persistent** flag (`cmd/platformctl/root.go`,
`newRootCmd`, `root.PersistentFlags().StringVarP(&a.output, ...)`), so every
subcommand accepts it. Three code paths never consult `a.output`:

1. `newGraphCmd` renders via its own `--format` flag and writes tree text to
   stdout regardless of `-o` (`cmd/platformctl/root.go`, `newGraphCmd`).
2. `newValidateCmd` prints `fmt.Fprintf(..., "%d resource(s) valid\n")`
   unconditionally (`cmd/platformctl/root.go`, `newValidateCmd`).
3. The `--for` branch of `newInventoryCmd` calls `renderToolConfig` on
   stdout unconditionally (`cmd/platformctl/root.go`, `newInventoryCmd`;
   `cmd/platformctl/toolconfig.go`).

## Required behavior

For every command, when `-o json` or `-o yaml` is in effect, stdout must
carry exactly one parseable document on every exit path (§0.5). Prose goes
to stderr or inside the payload. Specifically:

1. **graph**: when `isStructured(a.output)` is true, emit the JSON rendering
   of the view (the existing `view.Render(w, "json")` path; for `-o yaml`,
   marshal the same structure as YAML via `cliutil.WriteOutput`). The
   `--format` flag keeps governing the non-structured (`-o table`, default)
   presentation. If both `-o json|yaml` and a non-default `--format` are
   given, `-o` wins and a warning goes to stderr.
2. **validate**: when structured, emit `{"valid": true, "resources": N}` via
   `cliutil.WriteOutput`; keep the prose line for the default output. The
   error path already emits structured errors via `writeStructuredError`
   (`cmd/platformctl/main.go`) — do not change it.
3. **inventory --for**: when structured, emit
   `{"tool": "<tool>", "config": "<rendered snippet as one string>"}` via
   `cliutil.WriteOutput`; prose snippet stays the default-output behavior.

## Exact files and symbols

- `cmd/platformctl/root.go`: `newGraphCmd`, `newValidateCmd`,
  `newInventoryCmd` (the `forTool != ""` branch), helpers `isStructured`,
  `humanWriter`, `cliutil.WriteOutput` (already exist — reuse, do not
  reimplement).
- `cmd/platformctl/toolconfig.go`: `renderToolConfig` may gain a
  `renderToolConfigString(tool string, f toolFacts) (string, error)` wrapper
  (render into a `bytes.Buffer`); do not change renderer signatures.

## Implementation constraints

- Do not change the default (`-o table`) output of any of the three
  commands — existing tests and the README quickstart depend on it.
- Do not add new output formats or flags; only honor the existing contract.
- `graph --format json` (existing behavior) must keep working unchanged.
- No changes outside `cmd/platformctl/`.

## Tests to add

In `cmd/platformctl` (unit, no Docker needed — follow the style of
`toolconfig_test.go` / `inventory_test.go` which build cobra commands with
a temp state file):

1. `graph -o json` on a valid example dir → stdout parses as JSON and
   contains `"nodes"`.
2. `graph -o yaml` → parses as YAML.
3. `validate -o json` success → parses, `valid == true`, `resources == N`.
4. `inventory --for spark -o json` (empty state) → parses, has `tool` and
   `config` keys.
5. Regression: `graph` with no `-o` still starts with `DATA FLOW`.

## Validation commands

```
go test ./cmd/platformctl/ -run 'TestGraphStructuredOutput|TestValidateStructuredOutput|TestToolConfig'
go build -o /tmp/pctl ./cmd/platformctl
/tmp/pctl graph examples/cdc-attendance/ -o json | python3 -c "import json,sys; json.load(sys.stdin)"
/tmp/pctl validate examples/cdc-attendance/ -o json | python3 -c "import json,sys; json.load(sys.stdin)"
```

## Dependencies / ordering

None. Independent of all other findings.

## Risk

Low — additive branches on already-isolated code paths. The one behavioral
overlap is `graph -o json` for users who relied on it emitting tree text;
that output was never a documented contract and contradicts the global
`-o` help.

## Escalation conditions

Escalate (stop and ask) if: honoring `-o` in `graph` requires touching
`internal/application/archview` (it should not — `Render(w, "json")`
exists); or if `cliutil.WriteOutput` cannot render a plain struct to YAML
(it can — `drift`/`plan` already use it).

## Doc correction required

`docs/planning/07`: Gate 0 stage-gate item "Machine-readable command output
is valid JSON/YAML for every command" must be annotated (or unchecked) until
this fix lands; §0.5's resolved list must gain `graph`/`validate`/`--for`
entries when it does.
