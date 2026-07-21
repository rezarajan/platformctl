#!/usr/bin/env bash
# PreToolUse hook: guard docs/planning/*.md — these files are the contract
# the codebase is checked against, so edits that *alter or remove* existing
# contract text are blocked. Two shapes pass through automatically because
# neither changes what the already-committed text asserts:
#
#   1. Checking off a completed checklist item (toggling "- [ ]" to
#      "- [x]"/"- [X]" and back) with no other text change — bookkeeping on
#      work already done elsewhere.
#   2. A purely *additive* edit: every original line survives verbatim and in
#      order, and the only difference is inserted new lines. Documenting a
#      newly-observed per-runtime difference or a limit of shipped behavior is
#      additive; it records a fact, it does not revise the plan.
#
# Two further shapes are permitted:
#
#   3. Creating a NEW file under docs/planning/ (Write to a path that does
#      not exist yet) — a new document cannot alter what existing contract
#      text asserts, so it is additive by definition.
#   4. A maintenance unlock: while the marker file
#      `.claude/planning-docs-unlock` exists at the repo root, all edits
#      pass. This is the explicit, auditable bypass for user-authorized
#      documentation-maintenance passes (e.g. the 2026-07-20 docs
#      consolidation); remove the marker to restore protection. The marker
#      is intentionally not checked into git.
#
# Everything else — modifying or deleting an existing line — is blocked
# outright. There is no retry-with-justification path; changing what an
# existing contract statement says needs a human to make it directly (or to
# grant the maintenance unlock above).
set -euo pipefail

input=$(cat)
tool_name=$(echo "$input" | jq -r '.tool_name // empty')
file_path=$(echo "$input" | jq -r '.tool_input.file_path // empty')

if [[ "$file_path" != *docs/planning/*.md ]]; then
  exit 0
fi

# Maintenance unlock (shape 4): a marker file at the repo root suspends the
# guard for a user-authorized documentation-maintenance pass.
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ -f "$repo_root/.claude/planning-docs-unlock" ]]; then
  exit 0
fi

block_reason='docs/planning/ is the planning contract. This hook blocks edits that modify or delete existing text; only two shapes pass automatically: (a) toggling a checklist item'"'"'s "- [ ]"/"- [x]" marker with no other change, and (b) a purely additive edit that inserts new lines while preserving every existing line verbatim and in order. Your edit changed or removed existing text — that needs a human to make it directly in the file (there is no automated confirm-and-retry path). If you meant only to add content, re-issue the edit so no existing line is altered.'

# normalize collapses every checkbox marker to a single placeholder so a
# pure marker toggle (the only content difference) diffs as identical.
normalize() {
  sed -E 's/\[[ xX]\]/[_]/g'
}

only_checkbox_toggle() {
  local old="$1" new="$2"
  [[ "$old" != "$new" ]] || return 1
  local old_norm new_norm
  old_norm=$(printf '%s' "$old" | normalize)
  new_norm=$(printf '%s' "$new" | normalize)
  [[ "$old_norm" == "$new_norm" ]]
}

# only_additions passes when new differs from old solely by inserted lines:
# a line-based diff reports no old-only ("<") lines, so nothing existing was
# modified (a modification shows the line as both "<" and ">") or deleted.
only_additions() {
  local old="$1" new="$2"
  [[ "$old" != "$new" ]] || return 1
  # Append a trailing newline to both sides so diff never reports the final
  # line as "changed" merely because it gained a newline when content was
  # inserted after it (the "\ No newline at end of file" artifact).
  local d
  d=$(diff <(printf '%s\n' "$old") <(printf '%s\n' "$new") || true)
  ! printf '%s' "$d" | grep -q '^< '
}

case "$tool_name" in
  Edit)
    old_string=$(echo "$input" | jq -r '.tool_input.old_string // empty')
    new_string=$(echo "$input" | jq -r '.tool_input.new_string // empty')
    if only_checkbox_toggle "$old_string" "$new_string" || only_additions "$old_string" "$new_string"; then
      exit 0
    fi
    ;;
  Write)
    new_content=$(echo "$input" | jq -r '.tool_input.content // empty')
    if [[ ! -f "$file_path" ]]; then
      # Shape 3: a brand-new document is additive by definition.
      exit 0
    fi
    old_content=$(cat "$file_path")
    if only_checkbox_toggle "$old_content" "$new_content" || only_additions "$old_content" "$new_content"; then
      exit 0
    fi
    ;;
esac

jq -n --arg reason "$block_reason" '{"decision": "block", "reason": $reason}'
exit 0
