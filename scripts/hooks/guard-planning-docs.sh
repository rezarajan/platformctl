#!/usr/bin/env bash
# PreToolUse hook: warn when editing docs/planning/*.md — these files are the
# contract the codebase is checked against. Non-blocking: prints a reminder so
# the edit is deliberate, not incidental.
set -euo pipefail

input=$(cat)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // empty')

if [[ "$file_path" == *docs/planning/*.md ]]; then
  echo '{"decision": "block", "reason": "docs/planning/ is the planning contract. If this edit is deliberate (plan was wrong, phase revealed a gap), state the reason and retry — the edit will be allowed on explicit confirmation. Incidental edits should go elsewhere."}'
  exit 0
fi

exit 0
