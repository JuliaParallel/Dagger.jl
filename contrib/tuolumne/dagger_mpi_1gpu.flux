#!/bin/bash
# Full Dagger MPI test suite on Tuolumne — single GPU (1 MPI rank for ROCm tests).
#
# Before submit:
#   1. source ~/tuolumne_dagger.profile
#   2. Replace YOUR_BANK below (or set FLUX_BANK in profile and use it here)
#   3. flux batch scripts/tuolumne/dagger_mpi_1gpu.flux
#
#flux: -N 1
#flux: -q pdebug
#flux: -t 2h
#flux: --setattr=bank=YOUR_BANK
#flux: --job-name=dagger-mpi-1gpu
#flux: --output=dagger-mpi-1gpu.o{{id}}
#flux: --error=dagger-mpi-1gpu.e{{id}}

set -euo pipefail
source "${HOME}/tuolumne_dagger.profile"
cd "${DAGGER_DIR}"

# One Flux task on an exclusive node; mpiexec inside run_mpi_tests.sh.
# N=4     -> CPU MPI suites (mpi_datadeps, cholesky, lu_smoke)
# N_GPU=1 -> mpi_rocm.jl with 1 rank (single MI300A APU)
flux run -N1 -x -n 1 -- \
  env SKIP_CUDA=1 N=4 N_GPU=1 THREADS=4 contrib/mpi/run_mpi_tests.sh
