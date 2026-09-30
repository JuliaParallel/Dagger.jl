#!/usr/bin/env bash
set -euo pipefail
mkdir -p gpu_results
# Failed jobs can still have usable summaries; blocked/skipped jobs have none.
buildkite-agent artifact download 'benchmark-results-*/summary.json' gpu_results || true
python3 .buildkite/gpu_benchmark_report.py gpu_results
buildkite-agent artifact upload 'gpu_results/report.md'
buildkite-agent annotate --context gpu-benchmarks --style info < gpu_results/report.md
