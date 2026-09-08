#!/usr/bin/env bash
# AWS / local-VM runner for benchmark/suites/linalg_integration.jl
# Do not run on the Dagger-linalg-ultra workstation.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MODE="${LINALG_BENCH_MODE:-mt}"
OUT="${LINALG_BENCH_OUT:-$ROOT/results}"
export PATH="${HOME}/.juliaup/bin:${PATH}"
export JULIA_PKG_PRECOMPILE_AUTO="${JULIA_PKG_PRECOMPILE_AUTO:-1}"
JULIA="${JULIA:-$(command -v julia)}"

PROJ="${LBENCH_PROJECT:-$ROOT/benchmark/linalg_integration}"
mkdir -p "$PROJ" "$OUT"

if [[ ! -f "$PROJ/Project.toml" ]]; then
  DAGGER_ROOT="$ROOT" "$JULIA" --project="$PROJ" -e 'using Pkg
    Pkg.develop(path=ENV["DAGGER_ROOT"])
    pkgs = ["Krylov", "LinearSolve", "AlgebraicMultigrid", "IncompleteLU",
            "PureKLU", "PureUMFPACK", "SparseArrays", "SparseMatricesCSR"]
    if get(ENV, "LINALG_BENCH_MODE", "mt") == "mpi" || !isempty(get(ENV, "LINALG_BENCH_MPI_DEPS", ""))
        append!(pkgs, ["MPI", "MPIPreferences"])
    end
    Pkg.add(pkgs)
  '
  if [[ "$MODE" == "mpi" ]]; then
    "$JULIA" --project="$PROJ" -e 'using MPIPreferences; MPIPreferences.use_system_binary()'
  fi
fi

export LINALG_BENCH_MODE="$MODE"
export LINALG_BENCH_OUT="$OUT"

if [[ "$MODE" == "mpi" ]]; then
  # One rank per VM. The batchctl hostfile has slots=<vcpus>, and OpenMPI's
  # default-by-slot mapping would otherwise pack every rank onto node 0.
  HOSTFILE="${HOSTFILE:-${WORK:-/home/ubuntu/work}/hostfile}"
  N="${NRANKS:-4}"
  THREADS="${THREADS:-4}"
  RANKFILE="${HOSTFILE}.ranks"
  awk '{print $1, "slots=1"}' "$HOSTFILE" > "$RANKFILE"
  export JULIA_MPI_BINARY="${JULIA_MPI_BINARY:-system}"
  export OMPI_MCA_btl_vader_single_copy_mechanism="${OMPI_MCA_btl_vader_single_copy_mechanism:-none}"
  exec mpiexec --hostfile "$RANKFILE" -n "$N" \
    --map-by ppr:1:node --bind-to none \
    -x PATH -x HOME -x JULIA_MPI_BINARY \
    -x LINALG_BENCH_MODE -x LINALG_BENCH_OUT -x LINALG_BENCH_SCALE \
    -x LINALG_BENCH_WARMUP -x LINALG_BENCH_SAMPLES \
    -x LINALG_BENCH_INSTANCE -x LINALG_BENCH_ONLY \
    -x DAGGER_MPI_DEADLOCK_TIMEOUT \
    -x OMPI_MCA_btl_vader_single_copy_mechanism \
    "$JULIA" --project="$PROJ" -t "${THREADS}" \
      "$ROOT/benchmark/suites/linalg_integration.jl"
else
  THREADS="${THREADS:-16}"
  exec "$JULIA" --project="$PROJ" -t "${THREADS}" \
    "$ROOT/benchmark/suites/linalg_integration.jl"
fi
