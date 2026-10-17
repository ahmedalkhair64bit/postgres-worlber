#!/usr/bin/env bash
# Summarise chaos results into one scorecard entry.
#
#   ./chaos/report.sh [results-dir] [stamp-filter]  > run.json
#
# Reads results/<scenario>-<stamp>-<n>.json (only files whose stamp starts
# with stamp-filter, if given) and prints JSON:
#   {date, commit, scenarios: {s1: {runs, passed, max_rto_ms, acked,
#    acked_missing, old_only_acked}, ...}, pass}
# "pass" is true only when every run of every scenario passed.
set -euo pipefail
dir=${1:-$(dirname "$0")/results}
filter=${2:-}
shopt -s nullglob
files=()
for f in "$dir"/*.json; do
    b=$(basename "$f")
    [[ "$b" =~ ^s[0-9a-z]+-[0-9]{8}-[0-9]{6}-[0-9]+\.json$ ]] || continue
    [ -z "$filter" ] || [[ "$b" == *"-$filter"* ]] || continue
    files+=("$f")
done
if [ ${#files[@]} -eq 0 ]; then
    jq -n --arg d "$(date -u +%FT%TZ)" '{date: $d, scenarios: {}, pass: false, error: "no results"}'
    exit 0
fi
commit=$(git -C "$(dirname "$0")" rev-parse --short HEAD 2>/dev/null || echo unknown)
jq -s --arg d "$(date -u +%FT%TZ)" --arg c "$commit" '
    group_by(.scenario)
    | map({key: .[0].scenario, value: {
        runs: length,
        passed: (map(select(.pass == true)) | length),
        max_rto_ms: (map(.verdict.summary.rto_ms // null) | map(select(. != null)) | max),
        acked: (map(.verdict.acked // 0) | add),
        acked_missing: (map(.verdict.acked_missing // 0) | add),
        old_only_acked: (map(.verdict.old_only_acked // 0) | add)
      }})
    | from_entries as $s
    | {date: $d, commit: $c, scenarios: $s,
       pass: ($s | to_entries | all(.value.runs == .value.passed))}
' "${files[@]}"
