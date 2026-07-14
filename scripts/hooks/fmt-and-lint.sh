#!/usr/bin/env bash
# PostToolUse hook: format (and lint, when available) a Go file after Edit/Write.
# Reads the tool input JSON from stdin per Claude Code hook contract.
set -euo pipefail

input=$(cat)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // empty')

# Only act on Go files that exist.
[[ "$file_path" == *.go && -f "$file_path" ]] || exit 0

gofmt -w "$file_path"

if command -v golangci-lint >/dev/null 2>&1; then
  (cd "$(dirname "$file_path")" && golangci-lint run --fix . 2>/dev/null) || true
fi

exit 0
