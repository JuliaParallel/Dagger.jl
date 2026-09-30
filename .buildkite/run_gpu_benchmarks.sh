#!/usr/bin/env bash
set -euo pipefail
# A conditionally skipped test satisfies depends_on. Require actual success.
IFS=',' read -ra required <<< "$BENCHMARK_TEST_STEPS"
for step in "${required[@]}"; do
    outcome=$(buildkite-agent step get outcome --step "$step")
    if [[ "$outcome" != "passed" ]]; then
        echo "GPU benchmarks skipped: $step outcome is $outcome"
        exit 0
    fi
done
export BENCHMARK="$1:dagger+$BENCHMARK_GPU"
# One report writer aggregates all GPU jobs; per-suite jobs only upload artifacts.
export BENCHMARK_SKIP_COMMENT=1
bash .buildkite/run_benchmarks.sh
