#!/usr/bin/env bash
# Publish one scorecard entry to the `scorecard` branch (used by the nightly
# workflow; runnable by hand).
#
#   ./chaos/publish-scorecard.sh <entry.json> [repo-dir] [--no-push]
#
# Adds results/<date>-<commit>.json, rewrites latest.json (a shields.io
# endpoint badge) and SCORECARD.md (last 30 runs), commits and pushes.
set -euo pipefail
entry=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
repo=${2:-.}
push=yes
[ "${3:-}" = --no-push ] && push=no
runs=$(jq -r '.runs_per_scenario // "?"' "$entry")
commit=$(jq -r '.commit // "unknown"' "$entry")

cd "$repo"
wt=$(mktemp -d)
trap 'git worktree remove --force "$wt" >/dev/null 2>&1 || true' EXIT
git fetch origin scorecard >/dev/null 2>&1 || true
if git rev-parse --verify -q origin/scorecard >/dev/null; then
    git worktree add -q -B scorecard "$wt" origin/scorecard
elif git rev-parse --verify -q scorecard >/dev/null; then
    git worktree add -q "$wt" scorecard
else
    git worktree add -q --detach "$wt"
    (cd "$wt" && git checkout -q --orphan scorecard && { git rm -rq --cached . >/dev/null 2>&1 || true; } && find . -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +)
fi
cd "$wt"
mkdir -p results
cp "$entry" "results/$(date -u +%Y%m%d-%H%M%S)-$commit.json"

pass=$(jq -r '.pass' "$entry")
maxrto=$(jq -r '[.scenarios[] | .max_rto_ms // empty] | max // 0' "$entry")
lost=$(jq -r '[.scenarios[] | .acked_missing] | add // 0' "$entry")
if [ "$pass" = true ]; then
    msg="pass, max RTO $((maxrto / 1000))s, 0 acked rows lost"; color=brightgreen
else
    msg="FAIL ($lost acked rows lost)"; color=red
fi
jq -n --arg m "$msg" --arg c "$color" '{schemaVersion: 1, label: "worlber_ha chaos", message: $m, color: $c}' > latest.json

{
    echo "# worlber_ha nightly scorecard"
    echo
    echo "Every nightly run is listed, pass or fail. $runs runs per scenario on a GitHub-hosted"
    echo "runner (Docker lab, 3 nodes, failover_timeout 10s, 8 writer clients). RTO is the"
    echo "maximum over the runs. Last update: $(date -u +%F)."
    echo
    echo "| date | commit | pass | S1 RTO | S2 RTO | S4 RTO | S11 RTO | S12 RTO | acked rows lost |"
    echo "|---|---|---|---|---|---|---|---|---|"
    for f in $(ls results/*.json | sort -r | head -30); do
        jq -r '
          def r(k): (.scenarios[k].max_rto_ms // null) | if . == null then "-" else "\((. / 100 | floor) / 10)s" end;
          "| \(.date[0:10]) | \(.commit) | \(if .pass then "yes" else "**no**" end) | \(r("s1")) | \(r("s2")) | \(r("s4")) | \(r("s11")) | \(r("s12")) | \([.scenarios[] | .acked_missing] | add // 0) |"' "$f"
    done
} > SCORECARD.md

git add -A results latest.json SCORECARD.md
git -c user.name="worlber-ha scorecard" -c user.email="actions@users.noreply.github.com" \
    commit -q -m "scorecard: $(date -u +%F) $commit pass=$pass" || { echo "nothing to publish"; exit 0; }
if [ "$push" = yes ]; then
    git push -q origin scorecard
fi
echo "published: $msg"
