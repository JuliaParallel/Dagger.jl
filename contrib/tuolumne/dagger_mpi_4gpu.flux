#!/bin/bash
# Full Dagger MPI test suite on Tuolumne — single node, all 4 MI300A APUs.
#
# Before submit:
#   1. source ~/tuolumne_dagger.profile
#   2. Replace YOUR_BANK below
#   3. flux batch scripts/tuolumne/dagger_mpi_4gpu.flux
#
#flux: -N 1
#flux: -q pdebug
#flux: -t 2h
#flux: --setattr=bank=YOUR_BANK
#flux: --job-name=dagger-mpi-4gpu
#flux: --output=dagger-mpi-4gpu.o{{id}}
#flux: --error=dagger-mpi-4gpu.e{{id}}

set -euo pipefail
source "${HOME}/tuolumne_dagger.profile"
cd "${DAGGER_DIR}"

# N_GPU=4 -> one MPI rank per APU (Tuolumne has 4 MI300A per node)
flux run -N1 -x -n 1 -- \
  env SKIP_CUDA=1 N=4 N_GPU=4 THREADS=4 contrib/mpi/run_mpi_tests.sh
