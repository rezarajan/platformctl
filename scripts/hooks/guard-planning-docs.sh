#!/usr/bin/env bash
# PreToolUse hook: guard docs/planning/*.md — these files are the contract
# the codebase is checked against, so most edits are blocked. The one
# exception is checking off a completed checklist item (toggling
# "- [ ]" to "- [x]"/"- [X]" and back) with no other text change: that's
# bookkeeping on work already done elsewhere, not a contract change, so it
# passes through automatically. Everything else is blocked outright — there
# is no retry-with-justification path; a real plan/content change needs a
# human to make it directly.
set -euo pipefail

input=$(cat)
tool_name=$(echo "$input" | jq -r '.tool_name // empty')
file_path=$(echo "$input" | jq -r '.tool_input.file_path // empty')

if [[ "$file_path" != *docs/planning/*.md ]]; then
  exit 0
fi

block_reason='docs/planning/ is the planning contract and this hook blocks edits to it unconditionally, except for toggling a checklist item'"'"'s "- [ ]"/"- [x]" marker with no other change. A substantive edit (a plan was wrong, a phase revealed a gap) needs a human to make it directly in the file — there is no automated confirm-and-retry path.'

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

case "$tool_name" in
  Edit)
    old_string=$(echo "$input" | jq -r '.tool_input.old_string // empty')
    new_string=$(echo "$input" | jq -r '.tool_input.new_string // empty')
    if only_checkbox_toggle "$old_string" "$new_string"; then
      exit 0
    fi
    ;;
  Write)
    new_content=$(echo "$input" | jq -r '.tool_input.content // empty')
    if [[ -f "$file_path" ]]; then
      old_content=$(cat "$file_path")
      if only_checkbox_toggle "$old_content" "$new_content"; then
        exit 0
      fi
    fi
    ;;
esac

jq -n --arg reason "$block_reason" '{"decision": "block", "reason": $reason}'
exit 0
