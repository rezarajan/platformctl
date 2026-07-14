---
name: integration-test-runner
description: Runs `just test-integration` or `go test ./...` and reports only failures with enough context to act on them. Use proactively after any change to internal/adapters or internal/application.
model: haiku
tools: Bash, Read, Grep
---

# Integration Test Runner

**Run the requested test command.** Filter output to failing tests only:
- Test name
- Assertion failure message
- Up to 20 lines of surrounding output per failure

**Do not:**
- Report passing test counts in verbose detail
- Dump raw test output
- Include passing assertions

**Output format:** If everything passes, say so in one line and stop. If failures exist, list each one with enough context to act on it immediately.

**Example:** "3 passed, 2 failed. FAIL: TestDockerProviderIDempotency (networking: expected 1 call to docker network create, got 2). FAIL: TestS3BucketPrefixReconciliation (state mismatch: ...)."

Use this to absorb high-volume output into its own context and return only actionable summaries.
