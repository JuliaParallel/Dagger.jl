#!/bin/bash
#SBATCH --job-name=dagger-ms-bench
#SBATCH --partition=small
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=/home/ftome_local/Dagger.jl-with-Multistreams/logs/unified_%j.out
#SBATCH --error=/home/ftome_local/Dagger.jl-with-Multistreams/logs/unified_%j.err

set -euo pipefail

ROOT="/home/ftome_local/Dagger.jl-with-Multistreams"
JULIA="${JULIA:-/home/ftome_local/julia-1.12.4/bin/julia}"
export PATH="$(dirname "$JULIA"):$PATH"

# Prefer CUDA 12.8 if available (CUDA.jl tooling); fall back to default module.
if command -v module >/dev/null 2>&1; then
  module purge >/dev/null 2>&1 || true
  module load cuda/12.8.0-y5ulor7 2>/dev/null || module load cuda >/dev/null 2>&1 || true
fi

cd "$ROOT"
mkdir -p logs test/benchresults

echo "=== Dagger multi-stream unified bench ==="
echo "host=$(hostname)  date=$(date -Is)  job=${SLURM_JOB_ID:-none}"
echo "CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"
echo "SLURM_JOB_GPUS=${SLURM_JOB_GPUS:-unset}  (physical device ids; CVD is remapped)"
nvidia-smi --query-gpu=index,name,memory.used,memory.free,memory.total,utilization.gpu --format=csv || true
echo "Julia: $($JULIA --version)"
echo "module cuda: $(command -v nvcc || true)"

# dgx01 GPU 0 is known-slower; Slurm remaps the allocated device to CVD=0, so use
# SLURM_JOB_GPUS (physical index) — refuse and requeue rather than burn a bad slot.
phys_gpus="${SLURM_JOB_GPUS:-}"
if [[ -n "${phys_gpus}" ]]; then
  for g in ${phys_gpus//,/ }; do
    if [[ "$g" == "0" ]]; then
      echo "ERROR: allocated physical GPU 0 (SLURM_JOB_GPUS=${phys_gpus}), which is slower on this node." >&2
      echo "Requeue with: sbatch scripts/slurm_unified_bench.sbatch" >&2
      exit 43
    fi
  done
  echo "Physical GPU(s) OK: ${phys_gpus}"
fi

# Abort early if the assigned GPU is already nearly full (shared-node interference).
free_mib="$(nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits | head -n1 | tr -d ' ')"
echo "GPU memory free (MiB, first visible device): ${free_mib}"
if [[ "${free_mib}" =~ ^[0-9]+$ ]] && (( free_mib < 20000 )); then
  echo "ERROR: assigned GPU has only ${free_mib} MiB free (<20 GiB). Refusing to run." >&2
  nvidia-smi || true
  exit 42
fi

# Prefer CUDA.jl's default memory pool (do not force a possibly-invalid name).
unset JULIA_CUDA_MEMORY_POOL || true

# Resolve / develop local Dagger into the test environment (once per workspace).
echo "=== Pkg.instantiate / develop ==="
"$JULIA" --project=test -e '
  using Pkg
  Pkg.develop(path=".")
  # Prefer CUDA-only resolve for this H200 run; AMDGPU stays in Project but unused.
  Pkg.instantiate()
  Pkg.precompile()
  println("project ready")
'

echo "=== Running unified benchmark (CUDA) ==="
export DAGGER_BENCH_BACKEND=CUDA
# Keep BLAS single-threaded so host-side work doesn't steal cores from Julia
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export JULIA_NUM_THREADS="${JULIA_NUM_THREADS:-16}"

"$JULIA" --project=test test/benchmark.jl

echo "=== Done $(date -Is) ==="
ls -lt test/benchresults/unified_CUDA_*.csv | head -5
