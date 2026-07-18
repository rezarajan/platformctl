#!/usr/bin/env bash
# Resolves every image in scripts/pinned-images.txt to its current registry
# digest and rewrites each occurrence in tracked Go/YAML/Dockerfile files — a
# quoted Go string literal ("repo:tag"), a YAML `image: repo:tag`, or a
# Dockerfile `FROM repo:tag` — to repo:tag@sha256:<digest>, replacing any
# stale digest already pinned there. Idempotent: re-running against an
# unchanged upstream digest edits nothing.
#
# Requires a Docker daemon reachable by the current user (used only to
# resolve digests; nothing here mutates a running deployment).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
list="$root/scripts/pinned-images.txt"
cd "$root"

if [[ ! -f "$list" ]]; then
  echo "missing $list" >&2
  exit 1
fi

while IFS= read -r raw; do
  image="${raw%%#*}"
  # shellcheck disable=SC2001 # trim: portable across the sed on macOS/BSD too
  image="$(echo "$image" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')"
  [[ -z "$image" ]] && continue

  echo "==> resolving $image" >&2
  docker pull --quiet "$image" >/dev/null
  ref="$(docker inspect --format='{{index .RepoDigests 0}}' "$image")"
  digest="${ref##*@}"
  if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "could not resolve a digest for $image (docker inspect returned %q)" "$ref" >&2
    exit 1
  fi
  pinned="${image}@${digest}"

  # $image/$pinned travel via the environment, not shell-interpolated into
  # the -e source: the image ref itself contains "/" characters that would
  # otherwise collide with perl's s/// delimiter once substituted into the
  # script text before perl ever parses it.
  while IFS= read -r -d '' file; do
    case "$file" in
    *.go)
      IMG="$image" PINNED="$pinned" perl -0pi -e '
        my $image = quotemeta($ENV{IMG});
        my $pinned = $ENV{PINNED};
        s/"$image(\@sha256:[0-9a-f]{64})?"/"$pinned"/g;
      ' "$file"
      ;;
    *.yaml | *.yml)
      IMG="$image" PINNED="$pinned" perl -0pi -e '
        my $image = quotemeta($ENV{IMG});
        my $pinned = $ENV{PINNED};
        s/(image:\s*)$image(\@sha256:[0-9a-f]{64})?/${1}$pinned/g;
      ' "$file"
      ;;
    */Dockerfile | Dockerfile)
      IMG="$image" PINNED="$pinned" perl -0pi -e '
        my $image = quotemeta($ENV{IMG});
        my $pinned = $ENV{PINNED};
        s/(FROM\s+)$image(\@sha256:[0-9a-f]{64})?/${1}$pinned/g;
      ' "$file"
      ;;
    esac
  done < <(git ls-files -z -- '*.go' '*.yaml' '*.yml' '*Dockerfile*')
done <"$list"

if git diff --quiet -- '*.go' '*.yaml' '*.yml' '*Dockerfile*'; then
  echo "digests already current" >&2
else
  echo "digests updated:" >&2
  git diff --stat -- '*.go' '*.yaml' '*.yml' '*Dockerfile*' >&2
fi
