#!/usr/bin/env bash
# PreToolUse hook: subagent spawns must use a cost-efficient model.
#
# Every Agent/Task spawn must either:
#   1. Pass an explicit cost-efficient model override (sonnet or haiku), or
#   2. Use a project agent whose definition already pins an efficient model
#      in its frontmatter (.claude/agents/*.md — see the allowlist below), or
#   3. Run while the gitignored marker file .claude/agent-model-unlock exists
#      at the repo root — the explicit, auditable bypass for a session where
#      the user has authorized a bigger model (e.g. a deep architectural
#      review). Remove the marker to restore the guard.
#
# Everything else — no model (inherits the session model, typically the most
# expensive tier) or an explicit opus/fable — is denied with instructions,
# so tokens are never burnt on a big-model agent by accident.
set -euo pipefail

input=$(cat)
tool_name=$(echo "$input" | jq -r '.tool_name // empty')
model=$(echo "$input" | jq -r '.tool_input.model // empty')
subagent=$(echo "$input" | jq -r '.tool_input.subagent_type // empty')

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ -f "$repo_root/.claude/agent-model-unlock" ]]; then
  exit 0
fi

case "$model" in
  sonnet|haiku)
    exit 0
    ;;
esac

# Project agents that pin sonnet/haiku in their own frontmatter — spawning
# them without a model override is already cost-efficient.
case "$subagent" in
  provider-implementer|compatibility-reviewer|integration-test-runner|docker-verifier|schema-doc-sync)
    exit 0
    ;;
esac

reason="Cost guard: subagent spawns in this repo must use an efficient model. Set model: \"sonnet\" (or \"haiku\" for high-volume/low-judgment work) on the Agent call — omitting it inherits the expensive session model. The project agents (provider-implementer, compatibility-reviewer, integration-test-runner, docker-verifier, schema-doc-sync) already pin efficient models and pass without an override. If a bigger model is genuinely needed, ask the user; with their authorization, touch .claude/agent-model-unlock (gitignored) and remove it afterward."

jq -n --arg reason "$reason" '{"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny", "permissionDecisionReason": $reason}}'
exit 0
