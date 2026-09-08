#!/usr/bin/env bash
# AWS / local-VM runner for benchmark/suites/linalg_integration.jl
# Do not run on the Dagger-linalg-ultra workstation.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MODE="${LINALG_BENCH_MODE:-mt}"
THREADS="${THREADS:-16}"
OUT="${LINALG_BENCH_OUT:-$ROOT/results}"
export PATH="${HOME}/.juliaup/bin:${PATH}"
export JULIA_PKG_PRECOMPILE_AUTO="${JULIA_PKG_PRECOMPILE_AUTO:-1}"

PROJ="${LBENCH_PROJECT:-$ROOT/benchmark/linalg_integration}"
mkdir -p "$PROJ" "$OUT"

if [[ ! -f "$PROJ/Project.toml" ]]; then
  DAGGER_ROOT="$ROOT" julia --project="$PROJ" -e 'using Pkg
    Pkg.develop(path=ENV["DAGGER_ROOT"])
    Pkg.add(["Krylov", "LinearSolve", "AlgebraicMultigrid", "IncompleteLU",
             "PureKLU", "PureUMFPACK", "SparseArrays"])
  '
fi

export LINALG_BENCH_MODE="$MODE"
export LINALG_BENCH_OUT="$OUT"

if [[ "$MODE" == "mpi" ]]; then
  HOSTFILE="${HOSTFILE:-${WORK:-/home/ubuntu/work}/hostfile}"
  N="${NRANKS:-4}"
  exec mpiexec --hostfile "$HOSTFILE" -n "$N" \
    julia --project="$PROJ" -t "${THREADS}" \
      "$ROOT/benchmark/suites/linalg_integration.jl"
else
  exec julia --project="$PROJ" -t "${THREADS}" \
    "$ROOT/benchmark/suites/linalg_integration.jl"
fi
