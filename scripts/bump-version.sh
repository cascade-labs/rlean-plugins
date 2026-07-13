#!/usr/bin/env bash
# Bump the workspace version, open a "Release v<newver>" PR against main.
#
# Usage: ./scripts/bump-version.sh [patch|minor|major]   (default: patch)
#
# On merge, .github/workflows/auto-tag.yml tags v<newver> and fires the universal
# multi-platform release. This script only prepares the PR — it never tags.
set -euo pipefail

COMPONENT="${1:-patch}"
case "$COMPONENT" in
  patch|minor|major) ;;
  *) echo "usage: $0 [patch|minor|major]" >&2; exit 2 ;;
esac

# Run from the repo root regardless of the caller's cwd.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ ! -f Cargo.toml ]; then
  echo "ERROR: no Cargo.toml at repo root ($REPO_ROOT)" >&2
  exit 1
fi

# Refuse to run on a dirty working tree.
if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: working tree is dirty — commit or stash first." >&2
  git status --short >&2
  exit 1
fi

# --- 1. Read current [workspace.package] version -----------------------------
read_workspace_version() {
  awk '
    /^\[workspace\.package\]/ {inpkg=1; next}
    /^\[/ {inpkg=0}
    inpkg && /^version[[:space:]]*=/ {
      gsub(/[",]/, "", $0); print $NF; exit
    }' Cargo.toml
}
CUR="$(read_workspace_version)"
if [ -z "$CUR" ]; then
  echo "ERROR: could not find [workspace.package] version in Cargo.toml" >&2
  exit 1
fi
case "$CUR" in
  *[!0-9.]*|"")
    echo "ERROR: current version '$CUR' is not a plain X.Y.Z semver" >&2
    exit 1 ;;
esac

# --- 2. Compute the new version ----------------------------------------------
IFS='.' read -r MAJOR MINOR PATCH <<EOF
$CUR
EOF
: "${MAJOR:=0}" "${MINOR:=0}" "${PATCH:=0}"
case "$COMPONENT" in
  major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
  minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
  patch) PATCH=$((PATCH + 1)) ;;
esac
NEW="${MAJOR}.${MINOR}.${PATCH}"
echo "Bumping workspace version: ${CUR} -> ${NEW}"

# --- 3. Rewrite the version line in [workspace.package] -----------------------
# Only the version line inside the [workspace.package] section is touched.
awk -v new="$NEW" '
  /^\[workspace\.package\]/ {inpkg=1; print; next}
  /^\[/ {inpkg=0}
  inpkg && /^version[[:space:]]*=/ && !done {
    print "version = \"" new "\""; done=1; next
  }
  {print}
' Cargo.toml > Cargo.toml.tmp
mv Cargo.toml.tmp Cargo.toml

# Verify every member crate inherits the workspace version.
bad_members=""
while IFS= read -r member_toml; do
  [ -f "$member_toml" ] || continue
  # A member is compliant if it has `version.workspace = true` and no hardcoded
  # `version = "..."` in its [package] section.
  if ! grep -qE '^[[:space:]]*version\.workspace[[:space:]]*=[[:space:]]*true' "$member_toml"; then
    bad_members="${bad_members} ${member_toml}"
  fi
done < <(find crates data_providers brokerages custom_data -mindepth 2 -maxdepth 2 -name Cargo.toml 2>/dev/null)
if [ -n "$bad_members" ]; then
  echo "ERROR: these member crates do not inherit version.workspace = true:" >&2
  for m in $bad_members; do echo "  $m" >&2; done
  echo "Fix them to 'version.workspace = true' before releasing." >&2
  exit 1
fi

# --- 4. Refresh Cargo.lock (only if one is tracked in git) -------------------
if git ls-files --error-unmatch Cargo.lock >/dev/null 2>&1; then
  # Bump the workspace-member entries textually: each member package block is
  #   name = "<crate>"
  #   version = "<old>"
  # Bumping the line after a matching name avoids a network `cargo update` (the
  # plugin repos' git deps would try to fetch ssh://.../rlean, which is not
  # available from the release worktree).
  # Collect member crate names from the workspace Cargo.tomls.
  crate_names=""
  while IFS= read -r member_toml; do
    nm=$(awk -F'"' '/^name[[:space:]]*=/ {print $2; exit}' "$member_toml")
    [ -n "$nm" ] && crate_names="${crate_names} ${nm}"
  done < <(find crates data_providers brokerages custom_data -mindepth 2 -maxdepth 2 -name Cargo.toml 2>/dev/null)

  if command -v cargo >/dev/null 2>&1 && cargo update --workspace --offline >/dev/null 2>&1; then
    echo "Refreshed Cargo.lock via cargo update --workspace --offline"
  else
    echo "cargo update unavailable/offline — bumping member versions in Cargo.lock textually"
    for nm in $crate_names; do
      awk -v crate="$nm" -v newver="$NEW" '
        prev_is_name && /^version = "/ { sub(/"[^"]*"/, "\"" newver "\""); prev_is_name=0 }
        { if ($0 == "name = \"" crate "\"") prev_is_name=1; else if ($0 !~ /^version = "/) prev_is_name=0 }
        {print}
      ' Cargo.lock > Cargo.lock.tmp
      mv Cargo.lock.tmp Cargo.lock
    done
  fi
fi

# --- 5. Branch, commit, push, open PR ----------------------------------------
BRANCH="release/v${NEW}"
git checkout -b "$BRANCH"
git add Cargo.toml
if git ls-files --error-unmatch Cargo.lock >/dev/null 2>&1; then
  git add Cargo.lock
fi
git commit -m "Release v${NEW}"
git push -u origin "$BRANCH"

PR_URL=$(gh pr create --base main --head "$BRANCH" \
  --title "Release v${NEW}" \
  --body "Bumps the workspace version to \`${NEW}\`.

Merging this PR auto-tags \`v${NEW}\` and fires the universal multi-platform release (one semver tag -> artifacts for every supported triple + manifest.json).")

echo "Opened release PR: ${PR_URL}"
