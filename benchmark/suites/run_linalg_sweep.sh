#!/usr/bin/env bash
# Blocksize / assignment sweep over the integration benches.
# AWS only — do not run on the Dagger-linalg-ultra workstation.
#
# Sweeps knobs Dagger already exposes (Blocks size, distribute assignment,
# 1-D vs 2-D tiling, Dagger.scope / ProcessScope). No new assignment API.
#
# Best-config numbers must wait for linalg/blas1-fastpath on
# origin/Dagger-linalg-ultra. This driver is safe to land before that merge.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export LINALG_BENCH_SWEEP=1
export LINALG_BENCH_SWEEP_KEYS="${LINALG_BENCH_SWEEP_KEYS:-dense_gemm,sparse_spmv,krylov_cg,krylov_blockjacobi}"
export LINALG_BENCH_TILES="${LINALG_BENCH_TILES:-256,512,1024,2048}"
export LINALG_BENCH_ASSIGNS="${LINALG_BENCH_ASSIGNS:-arbitrary,blockrow,cyclicrow}"
export LINALG_BENCH_LAYOUTS="${LINALG_BENCH_LAYOUTS:-2d,1drow}"
export LINALG_BENCH_SCOPES="${LINALG_BENCH_SCOPES:-default}"
# Match published Krylov methodology (warmup 5, samples 3) unless overridden.
export LINALG_BENCH_WARMUP="${LINALG_BENCH_WARMUP:-5}"
export LINALG_BENCH_SAMPLES="${LINALG_BENCH_SAMPLES:-3}"
export LINALG_BENCH_OUT="${LINALG_BENCH_OUT:-$ROOT/results}"

exec "$ROOT/benchmark/suites/run_linalg_integration.sh" "$@"
