# Tuolumne Agent Testing Guide — Dagger MPI (`fda/mpi-spmd-datadeps`)

This document is written for an **automated agent** (or human operator) validating the Dagger MPI stack on **Tuolumne** at LLNL. Follow the steps in order. Report pass/fail for each checkpoint with log excerpts on failure.

## Mission

Validate the full MPI test suite on Tuolumne in two GPU configurations:

| ID | Configuration | Flux allocation | Key env vars |
|----|---------------|-----------------|--------------|
| **T1** | Single GPU, single node | 1 node, exclusive | `N_GPU=1`, `N=4` |
| **T2** | Multi-GPU, single node | 1 node, exclusive | `N_GPU=4`, `N=4` |

Both runs execute `contrib/mpi/run_mpi_tests.sh`, which covers CPU MPI tests and ROCm GPU tests (`test/mpi_rocm.jl`). CUDA tests are skipped (`SKIP_CUDA=1`).

## System facts (Tuolumne)

- **Hostname alias:** `tuo` (`ssh tuo.llnl.gov`)
- **Scheduler:** [Flux](https://hpc.llnl.gov/banks-jobs/running-jobs/flux-quick-start-guide)
- **Node GPU:** 4× AMD MI300A APU per compute node (1 GPU per APU)
- **Queues:** `pdebug` (short/debug, 44 nodes), `pbatch` (batch, 1100 nodes)
- **Filesystem:** use `/p/lustre5/${USER}` for clone, Julia depot, and job I/O — **not** `$HOME` (NFS)
- **GPU stack:** ROCm only — **no CUDA**
- **Binding:** use Flux `--exclusive` (`-x`) so [mpibind](https://hpc.llnl.gov/documentation/user-guides/using-el-capitan-systems/running-jobs-flux-and-mpi) maps MPI ranks to APUs

References:

- [Tuolumne hardware](https://hpc.llnl.gov/hardware/compute-platforms/tuolumne)
- [El Capitan / Tuolumne quickstart](https://hpc.llnl.gov/documentation/user-guides/using-el-capitan-systems/quickstart)

## Repository

| Item | Value |
|------|-------|
| Remote | `git@github.com:JuliaParallel/Dagger.jl.git` |
| Branch | `fda/mpi-spmd-datadeps` |
| Test runner | `contrib/mpi/run_mpi_tests.sh` |
| ROCm env | `test/rocmenv/Project.toml` |
| Tuolumne scripts | `contrib/tuolumne/` |

## Preconditions (agent checklist)

Before testing, confirm:

- [ ] SSH access to `tuo` with LC account
- [ ] Flux bank available (`flux banks` — set `FLUX_BANK` in profile)
- [ ] `git` and network access to GitHub
- [ ] `mpiexec` available **on compute nodes** (Cray MPICH via system modules)
- [ ] Julia ≥ 1.9 installed to Lustre (`JULIA_BIN` in profile)
- [ ] ROCm module loads (`module avail rocm`)

**Do not** run `contrib/mpi/run_mpi_tests.sh` or heavy `Pkg.instantiate()` on login nodes except for initial dependency install.

---

## Phase 0 — Environment setup (login node)

### 0.1 Create workspace on Lustre

```bash
ssh tuo.llnl.gov
export PROJDIR=/p/lustre5/${USER}/dagger-mpi
mkdir -p "${PROJDIR}"
cd "${PROJDIR}"
```

### 0.2 Clone and checkout branch

```bash
if [[ ! -d Dagger.jl ]]; then
  git clone git@github.com:JuliaParallel/Dagger.jl.git
fi
cd Dagger.jl
git fetch origin
git checkout fda/mpi-spmd-datadeps
git pull origin fda/mpi-spmd-datadeps
```

Record the commit SHA:

```bash
git rev-parse HEAD
```

### 0.3 Install profile

```bash
cp contrib/tuolumne/tuolumne_dagger.profile.example ~/tuolumne_dagger.profile
```

Edit `~/tuolumne_dagger.profile`:

1. Set `FLUX_BANK` to a valid bank (from `flux banks`)
2. Set `JULIA_BIN` to your Lustre Julia binary
3. Set `DAGGER_DIR` to `${PROJDIR}/Dagger.jl`
4. Verify `module load rocm/...` version with `module avail rocm`

```bash
source ~/tuolumne_dagger.profile
```

### 0.4 Install Julia (skip if `JULIA_BIN` already works)

```bash
mkdir -p /p/lustre5/${USER}/julia/bin
# Example: juliaup into Lustre (adjust if site provides Julia modules)
curl -fsSL https://install.julialang.org | sh -s -- --yes
# Point JULIA_BIN in profile to the installed julia on Lustre
${JULIA_BIN} --version
```

### 0.5 Instantiate Julia projects (login node OK)

```bash
cd "${DAGGER_DIR}"

${JULIA_BIN} --project=. -e 'using Pkg; Pkg.instantiate()'
${JULIA_BIN} --project=test/rocmenv -e 'using Pkg; Pkg.develop(path=pwd()); Pkg.instantiate()'
```

**Checkpoint 0.5:** both `Pkg.instantiate()` commands exit 0.

If MPI.jl fails to build, try [JuliaParallel/JUHPC](https://github.com/JuliaParallel/JUHPC) with Cray + ROCm modules, or `MPIPreferences.use_system_binary()` for Cray MPICH. Escalate to LC Hotline if blocked.

### 0.6 Configure Flux batch scripts

Replace `YOUR_BANK` in:

- `contrib/tuolumne/dagger_mpi_1gpu.flux`
- `contrib/tuolumne/dagger_mpi_4gpu.flux`

Or ensure `FLUX_BANK` in profile matches the `#flux: --setattr=bank=...` line.

---

## Phase 1 — Sanity checks (compute node via pdebug)

Allocate an interactive node:

```bash
source ~/tuolumne_dagger.profile
flux run -q pdebug -N 1 -t 1h -x --setattr=bank=${FLUX_BANK} -- bash -l
```

Inside the compute shell:

```bash
source ~/tuolumne_dagger.profile
cd "${DAGGER_DIR}"

which mpiexec
${JULIA_BIN} --version
${JULIA_BIN} --project=test/rocmenv -e 'using AMDGPU; println("ROCm functional: ", AMDGPU.functional())'
```

**Checkpoint 1:**

| Check | Expected |
|-------|----------|
| `mpiexec` found | path on PATH |
| Julia runs | version ≥ 1.9 |
| `AMDGPU.functional()` | `true` |

If `AMDGPU.functional()` is `false`, stop — GPU tests will fail. Check ROCm modules and that you are on a **compute** node.

---

## Phase 2 — Test T1: single GPU, single node

### 2.1 Interactive (optional dry run)

On a pdebug compute node:

```bash
source ~/tuolumne_dagger.profile
cd "${DAGGER_DIR}"
SKIP_CUDA=1 N=4 N_GPU=1 THREADS=4 contrib/mpi/run_mpi_tests.sh
```

### 2.2 Batch (primary)

On login node:

```bash
source ~/tuolumne_dagger.profile
cd "${DAGGER_DIR}"
flux batch contrib/tuolumne/dagger_mpi_1gpu.flux
```

Monitor:

```bash
flux jobs -a
# When complete:
cat dagger-mpi-1gpu.o<JOBID>
cat dagger-mpi-1gpu.e<JOBID>   # should be empty or warnings only
```

**Checkpoint 2 — T1 pass criteria:**

- [ ] Exit code 0
- [ ] Output contains `PASS: mpi`
- [ ] Output contains `SKIP: mpi_cuda` (CUDA skipped)
- [ ] Output contains `PASS: mpi_rocm`
- [ ] Final line: `All MPI suites passed.`
- [ ] All ranks print `[<rank>] MPI ROCm suite OK` (or single rank for `N_GPU=1`)

---

## Phase 3 — Test T2: single node, 4 GPUs (all APUs)

### 3.1 Interactive (optional)

```bash
SKIP_CUDA=1 N=4 N_GPU=4 THREADS=4 contrib/mpi/run_mpi_tests.sh
```

### 3.2 Batch (primary)

```bash
flux batch contrib/tuolumne/dagger_mpi_4gpu.flux
```

**Checkpoint 3 — T2 pass criteria:** same as T2, plus:

- [ ] `mpi_rocm` ran with 4 MPI ranks (`ranks: 4` in runner log)
- [ ] Output shows `[0]`, `[1]`, `[2]`, `[3]` `MPI ROCm suite OK` lines

---

## What `contrib/mpi/run_mpi_tests.sh` executes

Order of suites:

| # | Suite | Ranks var | Project | Notes |
|---|-------|-----------|---------|-------|
| 1 | `test/mpi.jl` | `N` (default 4) | repo root | CPU MPI suite (datadeps + linalg smokes) |
| 2 | `test/mpi_cuda.jl` | `N_GPU` | `test/cudaenv` | **Skipped on Tuolumne** (`SKIP_CUDA=1`) |
| 3 | `test/mpi_rocm.jl` | `N_GPU` | `test/rocmenv` | ROCm GPU datadeps |

### Environment variables

| Variable | T1 value | T2 value | Purpose |
|----------|----------|----------|---------|
| `SKIP_CUDA` | `1` | `1` | Skip NVIDIA CUDA suite |
| `N` | `4` | `4` | MPI ranks for CPU suites |
| `N_GPU` | `1` | `4` | MPI ranks for `mpi_rocm.jl` |
| `THREADS` | `4` | `4` | Julia threads per MPI rank |
| `JULIA_BIN` | from profile | from profile | Julia executable |
| `ROCM_GPU_PROJECT` | `test/rocmenv` | `test/rocmenv` | ROCm Julia env |
| `SKIP_GPU` | unset | unset | Set `1` to skip all GPU tests |

---

## Phase 4 — Report template (agent output)

Deliver a summary in this format:

```markdown
## Tuolumne Dagger MPI Test Report

- **Date:**
- **Commit:** `<git rev-parse HEAD>`
- **User:**
- **Flux bank:**
- **ROCm module:** `<module list | grep rocm>`
- **Julia:** `<julia --version>`

### T1 — Single GPU (N_GPU=1)
- **Job ID:**
- **Result:** PASS / FAIL
- **Log:** `dagger-mpi-1gpu.o<JOBID>`

### T2 — 4 GPU / node (N_GPU=4)
- **Job ID:**
- **Result:** PASS / FAIL
- **Log:** `dagger-mpi-4gpu.o<JOBID>`

### Failures (if any)
- Suite name:
- Error excerpt:
- Suggested next step:
```

---

## Troubleshooting

### `Julia binary not found`

Set `JULIA_BIN` in `~/tuolumne_dagger.profile` to a Lustre path. Do not rely on login-node-only installs.

### `mpiexec not found`

Load Cray/programming environment modules before running. Verify on a **compute** node: `which mpiexec`.

### `mpi_gpu` fails on Tuolumne

Tuolumne has no NVIDIA GPUs. Ensure `SKIP_CUDA=1` (or `export SKIP_CUDA=1` in profile). Do **not** set `CUDA_GPU_PROJECT` to `test/cudaenv` on Tuolumne.

### `ROCExt failed to load` / `AMDGPU.functional() == false`

- Confirm on compute node, not login
- `module load rocm/<version>`
- Do not point `ROCM_PATH` at a different ROCm than the loaded module ([LC pro-tip](https://hpc.llnl.gov/documentation/user-guides/using-el-capitan-systems/introduction-and-quickstart/pro-tips))

### MPI.jl / `mpiexec` hangs or fails

- Ensure job uses `flux run ... -x` (exclusive)
- For Cray MPICH + Julia, may need JUHPC or `MPIPreferences.use_system_binary()`
- Try `export JULIA_MPIEXEC_EXECUTABLE=$(which mpiexec)`

### Flux job pending forever

- Check bank: `flux banks`
- `pdebug` is small — try off-peak or use `pbatch` with longer walltime
- `flux jobs -a` for state

### Depots on NFS cause slow/failed precompile

Set in profile:

```bash
export JULIA_DEPOT_PATH=/p/lustre5/${USER}/.julia_depot:${JULIA_DEPOT_PATH:-}
```

---

## Optional extensions (out of scope unless requested)

- **CPU-only** quick smoke: `SKIP_GPU=1 contrib/mpi/run_mpi_tests.sh`
- **Individual suite:** `mpiexec -n 4 ${JULIA_BIN} --project=. --threads=4 test/mpi.jl`
- **Long batch queue:** change `#flux: -q pdebug` → `#flux: -q pbatch` in flux scripts
- **Multi-node MPI:** not required for this mission; current suites target single-node GPU configs

---

## File index

```
Dagger.jl/
├── contrib/mpi/run_mpi_tests.sh  # Main orchestrator
├── test/
│   ├── mpi.jl                    # CPU MPI suite (datadeps + linalg smokes)
│   ├── mpi_gpu_suite.jl          # Shared GPU suite (parametrized per backend)
│   ├── mpi_cuda.jl               # CUDA only — skipped on Tuolumne
│   ├── mpi_rocm.jl               # ROCm GPU suite (Tuolumne)
│   ├── rocmenv/Project.toml
│   └── cudaenv/Project.toml
└── contrib/tuolumne/
    ├── tuolumne_dagger.profile.example
    ├── dagger_mpi_1gpu.flux      # T1 batch script
    └── dagger_mpi_4gpu.flux      # T2 batch script
```

---

## Quick command reference

```bash
# Setup (once)
ssh tuo
export PROJDIR=/p/lustre5/${USER}/dagger-mpi
git clone git@github.com:JuliaParallel/Dagger.jl.git ${PROJDIR}/Dagger.jl
cd ${PROJDIR}/Dagger.jl && git checkout fda/mpi-spmd-datadeps
cp contrib/tuolumne/tuolumne_dagger.profile.example ~/tuolumne_dagger.profile
# edit profile, then:
source ~/tuolumne_dagger.profile
${JULIA_BIN} --project=. -e 'using Pkg; Pkg.instantiate()'
${JULIA_BIN} --project=test/rocmenv -e 'using Pkg; Pkg.develop(path=pwd()); Pkg.instantiate()'

# Submit tests
flux batch contrib/tuolumne/dagger_mpi_1gpu.flux   # T1
flux batch contrib/tuolumne/dagger_mpi_4gpu.flux   # T2
flux jobs -a
```
