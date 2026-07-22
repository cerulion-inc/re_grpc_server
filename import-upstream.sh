#!/usr/bin/env bash
#
# import-upstream.sh — refresh the `upstream` branch of this sparse fork to a
# given crates.io release of `re_grpc_server`, then tag it.
#
# Usage:   ./import-upstream.sh <VERSION>
# Example: ./import-upstream.sh 0.35.0
#
# What it does:
#   1. Downloads the crates.io tarball for <VERSION>.
#   2. Checks out the `upstream` branch and replaces its tree with the tarball
#      contents verbatim (stripping crates-io packaging artifacts
#      .cargo_vcs_info.json / Cargo.toml.orig).
#   3. Commits and tags `upstream/<VERSION>`.
#
# After this, bring the Cerulion patch forward:
#   git checkout main && git merge upstream
# (resolve any conflicts in src/lib.rs), rebuild, retest, and update the pinned
# rev in cerulion-base's root Cargo.toml [patch.crates-io]. See CERULION-PATCH.md.

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <VERSION>   e.g. $0 0.35.0" >&2
    exit 2
fi

VERSION="$1"
CRATE="re_grpc_server"
# crates.io requires a descriptive User-Agent (data-access policy).
UA="cerulion-re_grpc_server-fork import-upstream.sh (opensource@cerulion.com)"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
    echo "error: working tree is dirty; commit or stash first." >&2
    exit 1
fi

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo ">> downloading ${CRATE} ${VERSION} from crates.io ..."
curl -fsSL -A "$UA" \
    "https://crates.io/api/v1/crates/${CRATE}/${VERSION}/download" \
    -o "$TMPDIR/${CRATE}-${VERSION}.crate"

echo ">> extracting ..."
tar -xzf "$TMPDIR/${CRATE}-${VERSION}.crate" -C "$TMPDIR"
SRC="$TMPDIR/${CRATE}-${VERSION}"
if [ ! -d "$SRC" ]; then
    echo "error: expected extracted dir $SRC not found." >&2
    exit 1
fi

# Strip crates-io packaging artifacts — never part of the source tree.
rm -f "$SRC/.cargo_vcs_info.json" "$SRC/Cargo.toml.orig"

echo ">> switching to the upstream branch ..."
git checkout upstream

# Replace the whole tracked tree (except .git) with the tarball contents.
echo ">> replacing the upstream tree ..."
git rm -rq --ignore-unmatch .
cp -R "$SRC/." .
git add -A

if git diff --cached --quiet; then
    echo ">> no changes; ${CRATE} ${VERSION} is already on upstream. Tagging anyway."
else
    git commit -q -m "upstream: ${CRATE} ${VERSION} (crates.io tarball, verbatim)"
fi

git tag -f "upstream/${VERSION}"
echo ">> done. upstream is now ${CRATE} ${VERSION}, tagged upstream/${VERSION}."
echo ">> next: git checkout main && git merge upstream   (then rebuild + retest)."
