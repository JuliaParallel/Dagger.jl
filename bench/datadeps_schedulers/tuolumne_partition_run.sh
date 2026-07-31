#!/bin/bash
#
# tuolumne_partition_run.sh
#
# Submits the Tuolumne MI300a benchmark as SIX partitioned Flux jobs — one
# per (regime × workload) combination — so any single walltime kill only
# loses that one partition rather than the whole run. This is the safe
# alternative to running tuolumne_oneshot.jl as a single 24h+ session
# (which is what killed the previous attempt).
#
# Each job runs `tuolumne_oneshot.jl` but with TUOLUMNE_REGIME_FILTER and
# TUOLUMNE_WORKLOAD_FILTER env vars set so only that partition's cells
# are covered. TUOLUMNE_OUTPUT_TAG tags every output CSV/TXT with the
# partition name so parallel jobs never clobber each other.
#
# Prereq (run ONCE on a login node with network access; skip if already done):
#
#   cd <repo root>
#   julia --project=bench/datadeps_schedulers/tuolumne_env -e \
#     'using Pkg; Pkg.develop(path="."); Pkg.instantiate()'
#
# Usage:
#
#   cd <repo root>
#   bash bench/datadeps_schedulers/tuolumne_partition_run.sh
#
# Environment overrides:
#   NODES        nodes per job (default 4 so m1+m2+m4 topology fits in every job)
#   TIME_LIMIT   Flux walltime per job (default 24h)
#   JULIA_BIN    julia executable (default: julia)
#   FLUX_QUEUE   Flux queue (default: unset; Flux picks default)
#
# After the jobs finish, six sets of CSVs will be in $PWD:
#   tuolumne_regime_m1_1node_<tag>.csv
#   tuolumne_regime_m2_2node_<tag>.csv
#   tuolumne_regime_m4_4node_<tag>.csv
#   tuolumne_optgap_<tag>.csv
#   tuolumne_cache_<tag>.csv         (only for tags that include cholesky)
#   tuolumne_medians_<tag>.csv
#   tuolumne_run_summary_<tag>.txt
# where <tag> is one of: m1_cholesky, m1_matmul, m2_cholesky, m2_matmul,
# m4_cholesky, m4_matmul. Concatenate the same-schema CSVs post-hoc.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ONESHOT="$SCRIPT_DIR/tuolumne_oneshot.jl"
[[ -f "$ONESHOT" ]] || { echo "FATAL: $ONESHOT not found" >&2; exit 1; }

: "${NODES:=4}"
: "${TIME_LIMIT:=24h}"
: "${JULIA_BIN:=julia}"

FLUX_QUEUE_ARG=()
if [[ -n "${FLUX_QUEUE:-}" ]]; then
    FLUX_QUEUE_ARG=(--queue "$FLUX_QUEUE")
fi

# (regime, workload) partitions. Rationale:
#   - Regime partition: m1 (1 node) is fastest, m4 (4 nodes) slowest;
#     splitting lets each finish independently.
#   - Workload partition: cholesky and matmul have very different tile-count
#     compute profiles; splitting halves each job's runtime.
#   - 6 jobs total, each with NODES=4 allocation so the topology detection
#     inside tuolumne_oneshot.jl sees enough nodes for its regime (m1/m2/m4
#     just scope to a subset of the allocation; the same physical allocation
#     serves all three per job).
PARTITIONS=(
    "m1 cholesky"
    "m1 matmul"
    "m2 cholesky"
    "m2 matmul"
    "m4 cholesky"
    "m4 matmul"
)

echo "=== tuolumne_partition_run: submitting ${#PARTITIONS[@]} jobs ==="
echo "  julia=$JULIA_BIN nodes=$NODES time=$TIME_LIMIT"
[[ -n "${FLUX_QUEUE:-}" ]] && echo "  flux queue=$FLUX_QUEUE"

for part in "${PARTITIONS[@]}"; do
    read -r regime workload <<< "$part"
    tag="${regime}_${workload}"

    echo "→ flux batch --nodes=$NODES --time-limit=$TIME_LIMIT  regime=$regime workload=$workload  tag=$tag"

    flux batch \
        --nodes="$NODES" \
        --time-limit="$TIME_LIMIT" \
        --job-name="tuo_${tag}" \
        --output="tuolumne_partition_${tag}.out" \
        --error="tuolumne_partition_${tag}.err" \
        --env=ALL \
        --env=TUOLUMNE_REGIME_FILTER="$regime" \
        --env=TUOLUMNE_WORKLOAD_FILTER="$workload" \
        --env=TUOLUMNE_OUTPUT_TAG="$tag" \
        "${FLUX_QUEUE_ARG[@]}" \
        --wrap "$JULIA_BIN $ONESHOT"
done

echo "=== tuolumne_partition_run: all jobs submitted. Monitor with:  flux jobs -a ==="
