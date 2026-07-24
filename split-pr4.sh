#!/usr/bin/env bash
# Break PR4 (split/4-code) into 7 independent sub-PRs, each based on split/3-utils.
#
# Why fresh branches (not rebasing split/4-code): split/4-code is stale — it still
# carries the pre-review versions of .gitignore / docs / utils.R and debug.log,
# because it was never rebased after PR1/2/3 review changes. The CODE files were
# untouched by those reviews, so they're identical across split/4-code and
# read-write-fresh-run-r-table. We extract code files only, off the current PR3 tip.
#
# Each sub-PR branches directly off split/3-utils (independent — disjoint files,
# no mutual conflicts, any merge order after PR3 lands).
#
# Run from repo root in Git Bash:  sh split-pr4.sh
# Does NOT push. Review, then push + open PRs (all targeting split/3-utils).

set -euo pipefail

BASE_NAME="split/3-utils"
SRC_NAME="read-write-fresh-run-r-table"   # canonical PR #89 head; code identical to split/4-code

echo "==> Sanity checks"
git fetch origin --quiet
for n in "$BASE_NAME" "$SRC_NAME"; do
  if ! git rev-parse --verify --quiet "$n" >/dev/null \
     && ! git rev-parse --verify --quiet "origin/$n" >/dev/null; then
    echo "ERROR: cannot find '$n'." >&2; exit 1
  fi
done
# promote remote-only branches to local if needed
git rev-parse --verify --quiet "$BASE_NAME" >/dev/null || git branch "$BASE_NAME" "origin/$BASE_NAME" >/dev/null
git rev-parse --verify --quiet "$SRC_NAME"  >/dev/null || git branch "$SRC_NAME"  "origin/$SRC_NAME"  >/dev/null
BASE="$BASE_NAME"; SRC="$SRC_NAME"
echo "   BASE = $BASE ($(git rev-parse --short "$BASE"))"
echo "   SRC  = $SRC  ($(git rev-parse --short "$SRC"))"

# --- recover: drop back to BASE, delete any prior sub-PR branches -------------
recover() {
  git merge --abort 2>/dev/null || true
  git checkout -q "$BASE"
  for b in split/4a-input-renames split/4b-loader-logic split/4c-orchestrators \
           split/4d-prep-scripts split/4e-census split/4f-modules-01-02 \
           split/4g-modules-03-08; do
    git branch -D "$b" 2>/dev/null || true
  done
}
recover

if ! git diff-index --quiet HEAD --; then
  echo "ERROR: working tree not clean on $BASE." >&2; git status --short; exit 1
fi

# --- build one sub-PR: branch off BASE, extract files from SRC, commit --------
#   $1 = branch name   $2 = commit message   $3.. = files
build() {
  local branch="$1"; local msg="$2"; shift 2
  echo; echo "==> $branch"
  git checkout -q "$BASE"
  git checkout -q -b "$branch"
  git checkout "$SRC" -- "$@"
  git add -- "$@"
  if git diff --cached --quiet; then
    echo "   (no changes — deleting empty branch)"
    git checkout -q "$BASE"; git branch -D "$branch" >/dev/null
    return
  fi
  git commit -q -m "$msg"
  echo "   committed $(git log -1 --format=%h)  ($(git diff --cached HEAD^ --name-only | wc -l) files before commit)"
}

build split/4a-input-renames \
  "feat(loaders): align cohort/loader inputs with _r table convention" \
  R/load-cohort-appso.R R/load-cohort-bgs.R R/load-cohort-dacso.R R/load-cohort-trd.R \
  R/load-graduate-projections.R R/load-near-completers-ttrain.R

build split/4b-loader-logic \
  "feat(loaders): program/occupation projection logic, INFOWARE lookups, StatCan loads" \
  R/load-program-projections.R R/load-occupation-projections.R \
  R/load-infoware-lookups.R R/load-custom-stats-can.R

build split/4c-orchestrators \
  "feat(orchestration): add run-data-loading and run-data-preprocessing drivers" \
  R/run-data-loading.R R/run-data-preprocessing.R

build split/4d-prep-scripts \
  "feat(prep): fresh-run/QI prep driven by second_schema and _r tables" \
  R/prep-for-fresh-run.R R/prep-for-qi-run.R

build split/4e-census \
  "feat(census): labour-supply and occupation distribution census scripts to _r convention" \
  R/labour-supply-dists-census-data.R R/occ-dists-census-data.R

build split/4f-modules-01-02 \
  "feat(modules 01-02): SQL-to-R refactor of enrolment/credential preprocessing and program matching" \
  R/01a-enrolment-preprocessing.R R/01b-credential-preprocessing.R R/01c-credential-analysis.R \
  R/01d-enrolment-analysis.R R/01e-stp-distributions.R \
  R/02a-appso-programs.R R/02a-bgs-program-matching.R R/02a-dacso-program-matching.R \
  R/02a-update-cred-non-dup.R \
  R/02b-1-pssm-cohorts.R R/02b-2-pssm-cohorts-new-labour-supply.R R/02b-3-pssm-cohorts-occupation-distributions.R

build split/4g-modules-03-08 \
  "feat(modules 03-08): SQL-to-R refactor of near-completers, projections, and final reports" \
  R/03-near-completers-ttrain-refactor.R R/03-near-completers-ttrain.R \
  R/04-graduate-projections.R \
  R/06-historic-cohort-program-distribution.R R/06-program-projections.R \
  R/07-occupation-projections.R R/08-create-final-reports.R

# --- summary + verification --------------------------------------------------
echo; echo "==> Sub-PRs created (each targets $BASE):"
for b in split/4a-input-renames split/4b-loader-logic split/4c-orchestrators \
         split/4d-prep-scripts split/4e-census split/4f-modules-01-02 \
         split/4g-modules-03-08; do
  git rev-parse --verify --quiet "$b" >/dev/null || continue
  printf "   %-26s %s\n" "$b" "$(git log -1 --format='%h  %s' "$b")"
done

echo; echo "==> Verify: union of all sub-PR files vs full PR4 code diff"
ALL_FILES=$(git diff --name-only "$BASE".."$SRC" -- 'R/*.R' | sort)
echo "   code files in $BASE..$SRC : $(echo "$ALL_FILES" | grep -c .)"
COLLECTED=""
for b in split/4a-input-renames split/4b-loader-logic split/4c-orchestrators \
         split/4d-prep-scripts split/4e-census split/4f-modules-01-02 split/4g-modules-03-08; do
  COLLECTED="$COLLECTED
$(git diff --name-only "$BASE".."$b")"
done
COLLECTED=$(echo "$COLLECTED" | sed '/^$/d' | sort -u)
echo "   code files across sub-PRs : $(echo "$COLLECTED" | grep -c .)"
MISSING=$(comm -23 <(echo "$ALL_FILES") <(echo "$COLLECTED"))
if [ -z "$MISSING" ]; then
  echo "   ✅ every code file is covered by exactly one sub-PR"
else
  echo "   ⚠️  NOT covered:" >&2
  echo "$MISSING" | sed 's/^      /   /' >&2
fi

echo; echo "Per-PR diff sizes:"
for b in split/4a-input-renames split/4b-loader-logic split/4c-orchestrators \
         split/4d-prep-scripts split/4e-census split/4f-modules-01-02 split/4g-modules-03-08; do
  printf "   %-26s " "$b"; git diff --shortstat "$BASE".."$b" | sed 's/^/  /'
done

git checkout -q "$BASE"
echo; echo "Done. Push when ready:"
echo "  git push -u origin split/4a-input-renames split/4b-loader-logic split/4c-orchestrators \\"
echo "                 split/4d-prep-scripts split/4e-census split/4f-modules-01-02 split/4g-modules-03-08"
echo "Open 7 PRs, each targeting split/3-utils. After PR3 merges, GitHub retargets them to 01-refactor."
echo "Then close the original PR4 (split/4-code) and PR #89."
