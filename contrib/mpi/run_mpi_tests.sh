#!/usr/bin/env bash
# Run the full Dagger MPI test suite (CPU; GPU optional).
#
# This script lives at contrib/mpi/run_mpi_tests.sh and locates the repo root
# relative to its own path, so it can be run from anywhere:
#   contrib/mpi/run_mpi_tests.sh
#   N=4 THREADS=2 contrib/mpi/run_mpi_tests.sh
#   SKIP_GPU=1 contrib/mpi/run_mpi_tests.sh      # skip mpi_cuda.jl and mpi_rocm.jl
#   SKIP_CUDA=1 contrib/mpi/run_mpi_tests.sh     # skip mpi_cuda.jl only (e.g. AMD/ROCm systems)
#   SKIP_ROCM=1 contrib/mpi/run_mpi_tests.sh     # skip mpi_rocm.jl only (e.g. NVIDIA/CUDA systems)
#   CUDA_GPU_PROJECT=test/cudaenv contrib/mpi/run_mpi_tests.sh
#   ROCM_GPU_PROJECT=test/rocmenv contrib/mpi/run_mpi_tests.sh
#
# Override the Julia binary with JULIA_BIN if needed.

set -euo pipefail

# Repo root is two levels up from this script (contrib/mpi/).
DAGGER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
N="${N:-4}"
N_GPU="${N_GPU:-2}"
THREADS="${THREADS:-2}"
CUDA_GPU_PROJECT="${CUDA_GPU_PROJECT:-${DAGGER_DIR}/test/cudaenv}"
ROCM_GPU_PROJECT="${ROCM_GPU_PROJECT:-${DAGGER_DIR}/test/rocmenv}"

if [[ -z "${JULIA_BIN:-}" ]]; then
    JULIA_BIN="$(ls -d "${HOME}"/.julia/juliaup/julia-*+0.x64.linux.gnu/bin/julia 2>/dev/null | sort -V | tail -1)"
fi

if [[ ! -x "${JULIA_BIN}" ]]; then
    echo "ERROR: Julia binary not found. Set JULIA_BIN to your julia executable." >&2
    exit 1
fi

if ! command -v mpiexec >/dev/null 2>&1; then
    echo "ERROR: mpiexec not found on PATH." >&2
    exit 1
fi

run_mpi() {
    local label="$1"
    local nproc="$2"
    local project="$3"
    local script="$4"
    shift 4

    echo ""
    echo "======================================================================"
    echo "MPI suite: ${label}"
    echo "  ranks:   ${nproc}"
    echo "  project: ${project}"
    echo "  script:  ${script}"
    echo "======================================================================"
    mpiexec -n "${nproc}" "${JULIA_BIN}" --project="${project}" --threads="${THREADS}" "${script}" "$@"
}

failed=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; failed=1; }

echo "Dagger MPI test runner"
echo "  julia:   ${JULIA_BIN}"
echo "  mpiexec: $(command -v mpiexec)"
echo "  N:       ${N}"
echo "  threads: ${THREADS}"

# --- Dagger CPU MPI suite (mpiexec) ---
if run_mpi "mpi" "${N}" "${DAGGER_DIR}" "${DAGGER_DIR}/test/mpi.jl"; then
    pass "mpi"
else
    fail "mpi"
fi

# --- MPI GPU (CUDA, optional) ---
if [[ "${SKIP_GPU:-0}" == "1" || "${SKIP_CUDA:-0}" == "1" ]]; then
    echo ""
    echo "SKIP: mpi_cuda (SKIP_GPU=${SKIP_GPU:-0}, SKIP_CUDA=${SKIP_CUDA:-0})"
elif [[ ! -f "${CUDA_GPU_PROJECT}/Project.toml" ]]; then
    echo ""
    echo "SKIP: mpi_cuda (no Project.toml at CUDA_GPU_PROJECT=${CUDA_GPU_PROJECT})"
else
    if run_mpi "mpi_cuda" "${N_GPU}" "${CUDA_GPU_PROJECT}" "${DAGGER_DIR}/test/mpi_cuda.jl"; then
        pass "mpi_cuda"
    else
        fail "mpi_cuda"
    fi
fi

# --- MPI GPU (ROCm, optional) ---
if [[ "${SKIP_GPU:-0}" == "1" || "${SKIP_ROCM:-0}" == "1" ]]; then
    echo ""
    echo "SKIP: mpi_rocm (SKIP_GPU=${SKIP_GPU:-0}, SKIP_ROCM=${SKIP_ROCM:-0})"
elif [[ ! -f "${ROCM_GPU_PROJECT}/Project.toml" ]]; then
    echo ""
    echo "SKIP: mpi_rocm (no Project.toml at ROCM_GPU_PROJECT=${ROCM_GPU_PROJECT})"
else
    if run_mpi "mpi_rocm" "${N_GPU}" "${ROCM_GPU_PROJECT}" "${DAGGER_DIR}/test/mpi_rocm.jl"; then
        pass "mpi_rocm"
    else
        fail "mpi_rocm"
    fi
fi

echo ""
if [[ "${failed}" -eq 0 ]]; then
    echo "All MPI suites passed."
    exit 0
else
    echo "One or more MPI suites failed." >&2
    exit 1
fi
