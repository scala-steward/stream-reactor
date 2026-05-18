#!/usr/bin/env bash
# import-public-pr.sh — Import a PR from the public lensesio/stream-reactor mirror
# into the lensesio-dev/stream-reactor private repo, preserving all commits and
# authorship exactly, and open a PR for review.
#
# Usage:
#   ./import-public-pr.sh <public-pr-number>
#
# Optional env overrides:
#   PUBLIC_REPO   — public GitHub repo slug  (default: lensesio/stream-reactor)
#   DEV_REMOTE    — remote name in the current working copy whose URL is used
#                   as the SSH URL of the private repo  (default: origin)
#   BASE_BRANCH   — base branch for the new PR  (default: master)
#
# Process (no cherry-pick — commits are preserved verbatim):
#   1. Clone the private repo to a temp dir.
#   2. Add the contributor's fork as a second remote ("prsource").
#   3. Checkout the PR branch as public-pr-<N>.
#   4. Push public-pr-<N> to the private repo.
#   5. Delete the temp dir (EXIT trap).
#   6. Open the PR via gh.

set -euo pipefail

# ---------------------------------------------------------------------------
# Config / defaults
# ---------------------------------------------------------------------------
PR="${1:-}"
PUBLIC_REPO="${PUBLIC_REPO:-lensesio/stream-reactor}"
DEV_REMOTE="${DEV_REMOTE:-origin}"
BASE_BRANCH="${BASE_BRANCH:-master}"
IMPORT_BRANCH="public-pr-${PR}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
die()  { echo "ERROR: $*" >&2; exit 1; }
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
  die "Working tree has staged or modified tracked files. Please commit or stash before importing."
fi

# Resolve the SSH URL of the private repo from the current working copy's remote
DEV_SSH_URL="$(git remote get-url "$DEV_REMOTE")"
DEV_REPO_SLUG="$(echo "$DEV_SSH_URL" \
  | sed -E 's|^git@github\.com:||; s|^https://github\.com/||; s|\.git$||')"
info "Private repo : $DEV_REPO_SLUG  ($DEV_SSH_URL)"

# ---------------------------------------------------------------------------
# Fetch PR metadata from the public repo
# ---------------------------------------------------------------------------
info "Fetching metadata for $PUBLIC_REPO #$PR ..."
PR_JSON="$(gh pr view "$PR" --repo "$PUBLIC_REPO" \
  --json number,title,author,headRefName,headRepositoryOwner,headRepository,url,body,commits)"

PR_TITLE="$(echo  "$PR_JSON" | jq -r '.title')"
PR_AUTHOR="$(echo "$PR_JSON" | jq -r '.author.login')"
PR_HEAD_REF="$(echo "$PR_JSON" | jq -r '.headRefName')"
PR_FORK_OWNER="$(echo "$PR_JSON" | jq -r '.headRepositoryOwner.login')"
PR_FORK_NAME="$(echo "$PR_JSON" | jq -r '.headRepository.name')"
PR_URL="$(echo   "$PR_JSON" | jq -r '.url')"
PR_BODY="$(echo  "$PR_JSON" | jq -r '.body')"
COMMIT_OIDS="$(echo "$PR_JSON" | jq -r '.commits[].oid')"

FORK_URL="https://github.com/${PR_FORK_OWNER}/${PR_FORK_NAME}.git"

info "Title        : $PR_TITLE"
info "Author       : $PR_AUTHOR"
info "Fork         : $FORK_URL"
info "Fork branch  : $PR_HEAD_REF"
info "Import branch: $IMPORT_BRANCH"
info "Commits      : $(echo "$COMMIT_OIDS" | wc -l | tr -d ' ')"

# ---------------------------------------------------------------------------
# Guard: refuse if the import branch already exists locally or on remote
# ---------------------------------------------------------------------------
if git show-ref --verify --quiet "refs/heads/$IMPORT_BRANCH"; then
  die "Local branch '$IMPORT_BRANCH' already exists. Delete it first:
  git branch -D '$IMPORT_BRANCH'"
fi
if git ls-remote --exit-code "$DEV_REMOTE" "refs/heads/$IMPORT_BRANCH" >/dev/null 2>&1; then
  die "Remote branch '$IMPORT_BRANCH' already exists on $DEV_REMOTE. Delete it first:
  git push $DEV_REMOTE --delete '$IMPORT_BRANCH'"
fi

# ---------------------------------------------------------------------------
# Temp dir — always deleted on exit (success or failure)
# ---------------------------------------------------------------------------
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

CLONE_DIR="$TMP/clone"

# ---------------------------------------------------------------------------
# Clone the private repo into the temp dir
# ---------------------------------------------------------------------------
info "Cloning private repo into temp dir ..."
git clone --quiet "$DEV_SSH_URL" "$CLONE_DIR"

# ---------------------------------------------------------------------------
# Add the contributor's fork and fetch
# ---------------------------------------------------------------------------
info "Adding fork remote: $FORK_URL ..."
git -C "$CLONE_DIR" remote add prsource "$FORK_URL"

info "Fetching from fork ..."
git -C "$CLONE_DIR" fetch --quiet prsource

# ---------------------------------------------------------------------------
# Create the import branch from the fork's PR branch
# ---------------------------------------------------------------------------
info "Creating branch '$IMPORT_BRANCH' from prsource/$PR_HEAD_REF ..."
git -C "$CLONE_DIR" checkout -b "$IMPORT_BRANCH" "prsource/$PR_HEAD_REF" --quiet

# ---------------------------------------------------------------------------
# Push the import branch to the private repo
# (master is never checked out — structural guarantee)
# ---------------------------------------------------------------------------
info "Pushing '$IMPORT_BRANCH' to private repo ..."
git -C "$CLONE_DIR" push --quiet --set-upstream origin "$IMPORT_BRANCH"

# ---------------------------------------------------------------------------
# Temp dir is removed here by the EXIT trap
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Open the PR against the private repo
# ---------------------------------------------------------------------------
info "Opening PR against $DEV_REPO_SLUG:$BASE_BRANCH ..."

PR_IMPORT_TITLE="[import #${PR}] ${PR_TITLE}"

PR_IMPORT_BODY="$(cat <<EOF
Imports ${PR_URL}

**Original author:** @${PR_AUTHOR}
**Fork branch:** \`${PR_FORK_OWNER}:${PR_HEAD_REF}\`
**Commits:**
$(echo "$COMMIT_OIDS" | sed 's/^/- /')

---

${PR_BODY}
EOF
)"

NEW_PR_URL="$(gh pr create \
  --repo "$DEV_REPO_SLUG" \
  --base "$BASE_BRANCH" \
  --head "$IMPORT_BRANCH" \
  --title "$PR_IMPORT_TITLE" \
  --body "$PR_IMPORT_BODY")"

echo ""
echo "Done! New PR: $NEW_PR_URL"
