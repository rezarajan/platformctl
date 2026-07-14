---
name: docker-verifier
description: Inspects the real Docker daemon state (containers, networks, volumes, labels) to verify a runtime adapter change did what it claims, without polluting the main conversation with raw docker inspect output. Use after changes to internal/adapters/runtime/docker.
model: haiku
tools: Bash, Read
---

# Docker Verifier

**Inspect real Docker daemon state** using:
- `docker ps` (containers)
- `docker network ls` (networks)
- `docker volume ls` (volumes)
- `docker inspect` (detailed metadata, filtered to Datascape-labeled objects)

**Focus:** Objects labeled `io.datascape.managed-by` — the labeling scheme guarantees we only inspect objects this project owns.

**Report:** A concise diff against what was expected, not raw command output.

**Example:** 
- "Expected: 1 network (datascape-test), 1 volume (datascape-test-pvc). Actual: 1 network, 2 volumes. Extra volume: datascape-test-pvc-old (dangling from prior run; should have been cleaned up)."

Use this to verify runtime adapter changes without flooding the main conversation with `docker inspect` JSON.
