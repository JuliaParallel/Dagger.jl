#!/usr/bin/env bash
# Buildkite entrypoint for the benchmark regression job.
#
# Runs benchmark/ci.jl (which compares the current checkout against `master`,
# prints a table, renders plots, and exits non-zero on a >25% regression), then
# uploads the results as artifacts, posts a Buildkite annotation with the table
# and inline plots, and optionally comments the table on the GitHub PR.
#
# The job's pass/fail status mirrors ci.jl's exit code.

set -uo pipefail

BASE_REV="${BENCHMARK_BASE_REV:-master}"
OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-benchmark_results}"

# Make the base revision resolvable from the (possibly shallow) checkout.
echo "--- Fetching $BASE_REV"
git fetch -f origin "$BASE_REV:$BASE_REV" || git fetch origin "$BASE_REV" || true

echo "--- Running benchmarks (current vs $BASE_REV)"
julia --color=yes "$(dirname "$0")/../benchmark/ci.jl"
CODE=$?

echo "--- Publishing results"
if command -v buildkite-agent >/dev/null 2>&1; then
    buildkite-agent artifact upload "${OUTPUT_DIR}/**/*" || true

    if [ -f "${OUTPUT_DIR}/report.md" ]; then
        STYLE="success"
        [ "$CODE" -ne 0 ] && STYLE="error"
        buildkite-agent annotate --style "$STYLE" --context benchmarks \
            < "${OUTPUT_DIR}/report.md" || true
    fi
fi

# Optional: comment the table on the GitHub PR. Requires a GITHUB_TOKEN with
# pull-request write access; silently skipped otherwise. Plots are not inlined
# here (GitHub can't render Buildkite `artifact://` refs) -- we link to the
# Buildkite build instead, where the annotation shows the plots inline.
if [ -n "${GITHUB_TOKEN:-}" ] && [ "${BUILDKITE_PULL_REQUEST:-false}" != "false" ]; then
    echo "--- Commenting on GitHub PR #${BUILDKITE_PULL_REQUEST}"
    REPO_SLUG="$(echo "${BUILDKITE_REPO:-}" | sed -E 's#.*github.com[:/]##; s#\.git$##')"
    if [ -n "$REPO_SLUG" ] && [ -f "${OUTPUT_DIR}/report.md" ]; then
        BODY_FILE="$(mktemp)"
        grep -v 'artifact://' "${OUTPUT_DIR}/report.md" > "$BODY_FILE"
        {
            echo
            echo "[Full results and plots on Buildkite](${BUILDKITE_BUILD_URL:-})"
        } >> "$BODY_FILE"
        # jq-free JSON encoding of the comment body via python.
        PAYLOAD="$(python3 -c 'import json,sys; print(json.dumps({"body": open(sys.argv[1]).read()}))' "$BODY_FILE")"
        curl -sS -X POST \
            -H "Authorization: token ${GITHUB_TOKEN}" \
            -H "Accept: application/vnd.github+json" \
            "https://api.github.com/repos/${REPO_SLUG}/issues/${BUILDKITE_PULL_REQUEST}/comments" \
            -d "$PAYLOAD" >/dev/null || echo "PR comment failed (non-fatal)"
        rm -f "$BODY_FILE"
    fi
fi

exit "$CODE"
