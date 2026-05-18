#!/usr/bin/env bash
# import-public-pr.sh — Import a PR from the public lensesio/stream-reactor mirror
# into this lensesio-dev/stream-reactor fork, preserving authorship, and open a PR.
#
# Usage:
#   ./import-public-pr.sh <public-pr-number>
#
# Optional env overrides:
#   PUBLIC_REPO   — public GitHub repo slug (default: lensesio/stream-reactor)
#   DEV_REMOTE    — git remote name for the dev repo (default: origin)
#   BASE_BRANCH   — target branch for the new PR (default: master)

set -euo pipefail

# ---------------------------------------------------------------------------
# Config / defaults
# ---------------------------------------------------------------------------
PR="${1:-}"
PUBLIC_REPO="${PUBLIC_REPO:-lensesio/stream-reactor}"
DEV_REMOTE="${DEV_REMOTE:-origin}"
BASE_BRANCH="${BASE_BRANCH:-master}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
die() { echo "ERROR: $*" >&2; exit 1; }
info() { echo "==> $*"; }

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
[[ -n "$PR" ]] || die "Usage: $0 <public-pr-number>"
[[ "$PR" =~ ^[0-9]+$ ]] || die "PR number must be a positive integer, got: $PR"

for cmd in git gh jq; do
  command -v "$cmd" >/dev/null 2>&1 || die "'$cmd' is required but not found in PATH."
done

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "Must be run from inside a git work tree."

if [[ -n "$(git status --porcelain | grep -v '^??')" ]]; then
  die "Working tree has staged or modified tracked files. Please commit or stash your changes before importing."
fi

# Resolve the dev repo slug from the remote URL
DEV_REMOTE_URL="$(git remote get-url "$DEV_REMOTE")"
# Handle both HTTPS and SSH URL formats
DEV_REPO_SLUG="$(echo "$DEV_REMOTE_URL" \
  | sed -E 's|^git@github\.com:||; s|^https://github\.com/||; s|\.git$||')"
info "Dev repo: $DEV_REPO_SLUG"

# ---------------------------------------------------------------------------
# Fetch PR metadata from the public repo
# ---------------------------------------------------------------------------
info "Fetching metadata for $PUBLIC_REPO #$PR ..."
PR_JSON="$(gh pr view "$PR" --repo "$PUBLIC_REPO" \
  --json number,title,author,headRefName,headRefOid,baseRefName,body,url,commits)"

PR_TITLE="$(echo "$PR_JSON"   | jq -r '.title')"
PR_AUTHOR="$(echo "$PR_JSON"  | jq -r '.author.login')"
PR_HEAD_REF="$(echo "$PR_JSON" | jq -r '.headRefName')"
PR_URL="$(echo "$PR_JSON"     | jq -r '.url')"
PR_BODY="$(echo "$PR_JSON"    | jq -r '.body')"
# Build newline-separated list of commit OIDs in order
COMMIT_OIDS="$(echo "$PR_JSON" | jq -r '.commits[].oid')"

info "Title  : $PR_TITLE"
info "Author : $PR_AUTHOR"
info "Branch : $PR_HEAD_REF"
info "Commits: $(echo "$COMMIT_OIDS" | wc -l | tr -d ' ')"

# ---------------------------------------------------------------------------
# Guard: refuse to overwrite an existing local or remote branch
# ---------------------------------------------------------------------------
if git show-ref --verify --quiet "refs/heads/$PR_HEAD_REF"; then
  die "Local branch '$PR_HEAD_REF' already exists. Delete it first:\n  git branch -D '$PR_HEAD_REF'"
fi
if git ls-remote --exit-code "$DEV_REMOTE" "refs/heads/$PR_HEAD_REF" >/dev/null 2>&1; then
  die "Remote branch '$PR_HEAD_REF' already exists on $DEV_REMOTE. Delete it first:\n  git push $DEV_REMOTE --delete '$PR_HEAD_REF'"
fi

# ---------------------------------------------------------------------------
# Temp dir — always cleaned up on exit
# ---------------------------------------------------------------------------
TMP="$(mktemp -d)"
REMOTE_ADDED=false
BRANCH_CREATED=false
ORIG_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

cleanup() {
  local exit_code=$?
  info "Cleaning up ..."

  if [[ "$BRANCH_CREATED" == true ]] && [[ $exit_code -ne 0 ]]; then
    info "Removing partially created branch '$PR_HEAD_REF' due to error ..."
    git checkout "$ORIG_BRANCH" --quiet 2>/dev/null || true
    git branch -D "$PR_HEAD_REF" 2>/dev/null || true
  fi

  if [[ "$REMOTE_ADDED" == true ]]; then
    git remote remove public-import 2>/dev/null || true
  fi

  rm -rf "$TMP"
  info "Cleanup done."
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Clone the public repo into the temp dir and fetch the PR ref
# ---------------------------------------------------------------------------
info "Cloning public repo $PUBLIC_REPO into temp dir ..."
git clone --no-tags --quiet "https://github.com/${PUBLIC_REPO}.git" "$TMP/public"

info "Fetching pull/$PR/head from public repo ..."
git -C "$TMP/public" fetch --quiet origin "pull/${PR}/head:pr-${PR}"

# ---------------------------------------------------------------------------
# Add temp clone as a remote in the dev repo and fetch the commits
# ---------------------------------------------------------------------------
info "Adding public-import remote ..."
git remote add public-import "$TMP/public"
REMOTE_ADDED=true

git fetch --quiet public-import "pr-${PR}:refs/remotes/public-import/pr-${PR}"

# ---------------------------------------------------------------------------
# Create target branch off of origin/master
# ---------------------------------------------------------------------------
info "Fetching $DEV_REMOTE/$BASE_BRANCH ..."
git fetch --quiet "$DEV_REMOTE" "$BASE_BRANCH"

info "Creating branch '$PR_HEAD_REF' from $DEV_REMOTE/$BASE_BRANCH ..."
git checkout -b "$PR_HEAD_REF" "$DEV_REMOTE/$BASE_BRANCH" --quiet
BRANCH_CREATED=true

# ---------------------------------------------------------------------------
# Cherry-pick commits in order
# ---------------------------------------------------------------------------
info "Cherry-picking $(echo "$COMMIT_OIDS" | wc -l | tr -d ' ') commit(s) ..."
while IFS= read -r oid; do
  info "  cherry-pick $oid ..."
  if ! git cherry-pick -x "$oid"; then
    echo ""
    echo "CONFLICT during cherry-pick of $oid."
    echo "Resolve conflicts, then run:"
    echo "  git cherry-pick --continue"
    echo "  git push -u $DEV_REMOTE $PR_HEAD_REF"
    echo "  gh pr create --repo $DEV_REPO_SLUG --base $BASE_BRANCH --head $PR_HEAD_REF \\"
    echo "    --title \"[import #$PR] $PR_TITLE\" --body \"Imports $PR_URL\""
    # Don't let cleanup delete the branch — user needs it to resolve
    BRANCH_CREATED=false
    exit 1
  fi
done <<< "$COMMIT_OIDS"

# ---------------------------------------------------------------------------
# Push the branch
# ---------------------------------------------------------------------------
info "Pushing '$PR_HEAD_REF' to $DEV_REMOTE ..."
git push --quiet -u "$DEV_REMOTE" "$PR_HEAD_REF"

# ---------------------------------------------------------------------------
# Open the PR
# ---------------------------------------------------------------------------
info "Opening PR against $DEV_REPO_SLUG:$BASE_BRANCH ..."

PR_IMPORT_TITLE="[import #${PR}] ${PR_TITLE}"

PR_IMPORT_BODY="$(cat <<EOF
Imports ${PR_URL}

**Original author:** @${PR_AUTHOR}
**Commits cherry-picked:**
$(echo "$COMMIT_OIDS" | sed 's/^/- /')

---

${PR_BODY}
EOF
)"

NEW_PR_URL="$(gh pr create \
  --repo "$DEV_REPO_SLUG" \
  --base "$BASE_BRANCH" \
  --head "$PR_HEAD_REF" \
  --title "$PR_IMPORT_TITLE" \
  --body "$PR_IMPORT_BODY")"

# Success — don't roll back branch on cleanup
BRANCH_CREATED=false

echo ""
echo "Done! New PR: $NEW_PR_URL"
