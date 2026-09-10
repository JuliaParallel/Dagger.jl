# Dagger linear-algebra integration

Living status document for the multi-agent linalg program. **This file is the
source of truth.** Sibling agents implement on their own worktrees and feature
branches; they do **not** merge into this workspace branch, and they do **not**
edit other agents' rows here. Report status in your final message so the
coordinator can update the table.

Last coordinator pass: 2026-09-09 (`linalg/blas1-fastpath`, `linalg/einsum`, `linalg/bsr` merged; blocksize/assignment sweep published from job `f74175e59a65e3ec` at SHA `6bf9aa2b`). Identify AWS jobs by **job id**, not EC2 `Name`/`vmbench-label` (see AWS note below).

---

## Goal and API rules

Close the PETSc-shaped gaps in Dagger's distributed linear algebra **by
extending Base, LinearAlgebra, SparseArrays, and ecosystem packages via
multiple dispatch on `DArray` / `DMatrix` / `DVector`.**

Hard rules:

1. **No novel `Dagger.xyz` API** unless a Base/LinearAlgebra/ecosystem generic
   cannot express the operation. Existing unavoidable names (`Dagger.klu`,
   `Dagger.splu`, `Dagger.AMGPreconditioner`, …) stay; do not add more without
   a written reason in the workstream notes.
2. **Do not break existing APIs or change existing Dagger semantics.** Dense
   tiled `lu` / `cholesky` / Krylov / `sprand` / `spzeros` must keep working.
3. **Never densify a sparse operator** on a path that is supposed to stay
   sparse (`\`, `lu`, `factorize`, assembly, preconditioner apply).
4. **Never run Dagger test suites or benchmarks on the local host**
   (`/home/jpsamaroo/.julia/dev/Dagger-linalg-ultra` and sibling worktrees).
   That machine is resource-constrained. Tests and benches go to AWS VMs
   (recipe below).
5. **Never force-push `main`/`master`.** Never rebase shared branches
   destructively.

What already exists (do not reimplement; extend):

- Dense tiled LU, Cholesky, QR, SVD, `ldiv!`, BLAS-1 (`src/array/lu.jl`,
  `cholesky.jl`, `linalg.jl`, …).
- Sparse tiles (`DSparseArray`), `sprand`/`spzeros`/`distribute` with `Blocks`
  (`ext/SparseArraysExt.jl`, `docs/src/sparse-arrays.md`).
- Krylov.jl matrix-free solvers on `DArray` (`ext/KrylovExt.jl`,
  `docs/src/iterative-solving.md`).
- Per-tile block Jacobi / ILU / AMG (`src/array/iterativesolvers.jl`).
  Per-tile AMG is **not** global AMG — see AGENTS.md lesson 19.
- Geometric V-cycle: `GeometricMultigrid` (matrix-based R/P/RAP; not `@stencil`).
- Gathered sparse direct: `Dagger.klu` / `Dagger.splu` (`src/array/sparsedirect.jl`).

---

## AWS how-to (sibling agents copy this)

There is **no AWS MCP namespace** in the current Cursor session
(`GetDynamicTools` catalog is only `cursor` + `cursor-app-control`). Drive
everything through the `batchctl` CLI. The `awsmcp` MCP server exists at
`/home/jpsamaroo/awsmcp/` and is documented in `BATCH.md` / `src/awsmcp/server.py`;
use those tools only if your session actually has the `awsmcp` server loaded.
Handles are interchangeable: a `batchctl submit` handle works with MCP
`exec_command` and vice versa, provided `$AWSMCP_HOME` is consistent
(default `~/.local/share/awsmcp`).

### Verified 2026-09-07

| Item | Value |
|---|---|
| Auth | AWS SSO profile `lab` works (`aws sts get-caller-identity --profile lab`) |
| Account / region | `730335443380` / `us-east-1` |
| batchd | Running, authenticated, `vcpu_cap=256` |
| CLI | `/home/jpsamaroo/awsmcp/batchctl` (`batchctl` is **not** on `PATH`) |
| Smoke job | `3b37b310cd5c53f1` → `c6i.large`, AMI `ami-025d99823a4caad37`, Ubuntu 24.04.4, exec `uname -a` ok, then `done` |
| Image | Auto-resolved Ubuntu 24.04 from SSM (`/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id`). Do not pass `--ami` unless you have a reason. |
| Julia on AMI | **Not installed.** You must install it (recipe below). |
| Work dir | `/home/ubuntu/work` |
| `$AWSMCP_HOME` | Unset → `~/.local/share/awsmcp`. Do not override unless every process agrees. |

`batchd` is a queue in front of `vmbench.py`. It enforces the account vCPU cap
across agents. **CPU instances only** through `batchctl submit`. GPU jobs must
use `vmbench.py` / `awsmcp.launch_vm` directly (separate EC2 quota).

### One-time / sprint (coordinator; already done for this sprint)

```bash
# Only if batchd is down or unauthenticated:
/home/jpsamaroo/awsmcp/batchctl login --profile lab   # relay URL+code to the user; blocks
/home/jpsamaroo/awsmcp/batchctl start --profile lab
/home/jpsamaroo/awsmcp/batchctl queue                 # expect authenticated + vcpu_cap set
```

If `vcpu_cap` is `null`, submission is refused (503) until
`batchctl start --vcpu-cap N` or login/permissions are fixed.

### Per-agent: submit → wait → work → done

**Never share a job handle with another agent.** It is a bearer token.

```bash
BATCH=/home/jpsamaroo/awsmcp/batchctl

# Typical CPU test box (resolves to c6i.2xlarge = 8 vCPU / 16 GiB).
# --disk 40: Julia + depot + Dagger test deps will not fit on the default 10 GiB
# (a 10 GiB volume only yields ~8.7 GiB on / after the Ubuntu partition table).
JOB=$($BATCH submit --cpus 8 --memory 16 --disk 40 --timeout 7200 --label linalg-<workstream>)
$BATCH wait "$JOB"          # blocks until ready / failed / done

# MPI across a real network (all-or-nothing group of N VMs):
# JOB=$($BATCH submit --cpus 8 --memory 16 --disk 40 --count 4 --timeout 7200 --label linalg-<ws>-mpi)
# $BATCH wait "$JOB"
# $BATCH nodes "$JOB"       # private IPs; $WORK/hostfile is already on every node
```

Always pass `--timeout` (seconds; default 7200). That is the VM sleep-timer
safety net, **not** the way you release capacity. Call `done` when finished.
`reset-timeout` extends the timer (max 24h).

Drive the VM (these go SSH-direct; the daemon is not in the path):

```bash
$BATCH exec "$JOB" -- uname -a
$BATCH exec "$JOB" --cwd /home/ubuntu/work -- bash -lc '…'
$BATCH exec "$JOB" --background --timeout 30 -- ./run_long.sh   # prints bg job_id
$BATCH output "$JOB" <bg_job_id> --tail 80
$BATCH write "$JOB" src/foo.jl --content-file ./local_foo.jl
$BATCH read  "$JOB" results/out.txt
$BATCH ls    "$JOB" .
$BATCH upload   "$JOB" /local/path  remote/path
$BATCH download "$JOB" remote/path  /local/dir
$BATCH reset-timeout "$JOB" 7200
$BATCH info "$JOB"          # live AWS state + remaining sleep-timer
```

`--node N` (default 0) targets a node of a `--count>1` job.

**When done (required):**

```bash
$BATCH done "$JOB"
```

The VM is never reused. `done` initiates terminate and frees the vCPU budget
immediately. Abandoned VMs self-terminate at `--timeout`; a reconciliation
loop also marks vanished instances `done`.

Watch the shared budget: `$BATCH queue`. Do not `batchctl stop` unless you
are the coordinator ending the sprint — that does not terminate ready VMs.

### Install Julia and run Dagger tests on the VM

The AMI is bare Ubuntu 24.04. Suggested once-per-VM bootstrap (adjust the
branch to **your** `linalg/<name>` after you have pushed it):

```bash
$BATCH exec "$JOB" --timeout 600 -- bash -lc '
set -euxo pipefail
curl -fsSL https://install.julialang.org | sh -s -- --yes
export PATH="$HOME/.juliaup/bin:$PATH"
# OpenMPI only if you submitted --count>1
# sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openmpi-bin libopenmpi-dev
git clone --depth 1 --branch linalg/<name> https://github.com/JuliaParallel/Dagger.jl.git
cd Dagger.jl
julia --project=test -e "using Pkg; Pkg.instantiate()"
'

# Targeted suite (example). Full `Pkg.test()` is too heavy for most workstreams.
$BATCH exec "$JOB" --timeout 3600 -- bash -lc '
export PATH="$HOME/.juliaup/bin:$PATH"
cd /home/ubuntu/work/Dagger.jl
julia --project=test test/runtests.jl --test array/linalg
'

# Fetch logs / results, then release the VM
$BATCH download "$JOB" Dagger.jl/test  /tmp/linalg-<name>-test
$BATCH done "$JOB"
```

Useful `--test` names (see `test/runtests.jl`): `array/linalg/sparsedirect`,
`array/linalg/iterativesolvers`, `array/linalg/lu`, `array/linalg/cholesky`,
`array/linalg/solve`, `array/linalg/core`. Directory prefix `array/linalg`
runs the whole linalg group.

If you have not pushed yet, upload a worktree instead of `git clone`:

```bash
$BATCH upload "$JOB" /home/jpsamaroo/.julia/dev/Dagger-linalg-<name> Dagger.jl
```

`--script` / `--run` / `--upload` at `submit` time run on every node after
boot (handy for the juliaup install).

### MCP tools (only if `awsmcp` is actually connected)

`aws_login`, `launch_vm`, `get_status`, `exec_command`, `get_command_output`,
`read_file`, `write_file`, `list_dir`, `remove_path`, `mkdir`, `upload`,
`download`, `reset_timeout`, `terminate_vm`. After `batchctl status "$JOB"`
you have a `handle` for these. GPU: `launch_vm(gpu="nvidia"|"amd")` — not
`batchctl submit`.

Full queue semantics, multi-VM MPI setup (`$WORK/hostfile`, cluster SSH key,
shared SG), and safety nets: `/home/jpsamaroo/awsmcp/BATCH.md`.

---

## Sibling-agent rules

1. **Worktree, not this workspace.** Create a worktree off this repo / commit:
   ```bash
   git -C /home/jpsamaroo/.julia/dev/Dagger-linalg-ultra worktree add \
     /home/jpsamaroo/.julia/dev/Dagger-linalg-<name> -b linalg/<name>
   ```
   Branch name **must** be `linalg/<name>` matching the workstream id.
2. **Push `linalg/<name>` to `origin`.** Do **not** merge into
   `Dagger-linalg-ultra`, `master`, or `main`. The coordinator merges.
3. **Do not edit other rows** of the workstream table, the changelog, or the
   AWS recipe unless you are fixing a fact you just verified. Prefer reporting
   status in your final message.
4. **Do not discard user work** in any worktree. Do not `git reset --hard` or
   clean dirty trees you did not create.
5. **Tests and benches: AWS only.** See above. Never `Pkg.test()` / runtests /
   benchmarks on the local machine.
6. **Handles are private.** One agent, one job (or one `--count N` group).
7. **Call `batchctl done`** when you finish or fail. Do not leave VMs.
8. **API rule reminder:** extend Base/LinearAlgebra/ecosystem; no new
   `Dagger.xyz` unless unavoidable; no semantic changes to existing methods.

### Performance note

If a workstream's performance is unideal, benchmark on top of
`jps/datadeps-region-async` (32 commits ahead of current `master` as of this
writing; local tip `fb40efa3`, origin tip `b108fb25` — they have diverged).
**Rebase that branch onto current `master` on a private fork branch.** Never
rebase shared branches (`origin/jps/datadeps-region-async`, `origin/master`,
anyone else's `linalg/*`) destructively.

---

## Workstreams

Status values: `not started` | `in progress` | `blocked` | `done` | `failed`.

Priority: **P0 done** / **P1 done** = merged onto `Dagger-linalg-ultra`. **P2** = completeness (API holes vs PETSc/HYPRE/LinearAlgebra), not performance.

| Pri | Workstream | Status | Branch | Owner notes | Last update |
|---|---|---|---|---|---|
| P0 | sparse-solve-dispatch | done | `linalg/sparse-solve-dispatch` @ `57c7414b` | Sparse `A\b` / `lu` / `factorize` route off dense LU via `is_sparse_backed`. AWS: array/linalg pass. Numeric refactor skipped — see `numeric-refactor`. `inv` on sparse uses the sparse factor (`factorize` + `ldiv!` into `I`). | 2026-09-07 |
| P0 | linearsolve | done | `linalg/linearsolve` @ `55c38361` | `LinearSolve.solve` / `defaultalg` on `DArray` (direct `KrylovJL_GMRES` / `PureKLU` / `PureUMFPACK`, never `DefaultLinearSolver`). AWS: linearsolve 25/25, full array/linalg pass. AlgebraicMultigrid compat `"1, 2"`. | 2026-09-07 |
| P0 | assembly | done | `linalg/assembly` @ `6018c6f4` | `sparse` / `sparse!` + `Blocks` tile-routed COO; regular `@spawn`, not datadeps (`SparseCOOBucket` has no `move!`). AWS: assembly 64/64, array/linalg pass. | 2026-09-07 |
| P0 | overlapping-asm | done | `linalg/overlapping-asm` @ `5a2102f7` | `AdditiveSchwarzPreconditioner` (`PC_ASM_RESTRICT`). RAS is not SPD — use GMRES. Overlap benefit is problem-dependent (no `niter` drop on well-conditioned 1-D Laplacian with large tiles). Symmetric leftover: `ras-symmetric`. AWS: iterativesolvers 346, array/linalg pass. | 2026-09-07 |
| P0 | sparse-chol-ic | done | `linalg/sparse-chol-ic` @ `5c1f9991` | Sparse `cholesky` via `_cholesky` / `cholesky!` (never `_chol!`); per-tile `BlockICPreconditioner` / `ichol`. AWS: sparse chol 55, array/linalg pass. | 2026-09-07 |
| P0 | global-amg | done | `linalg/global-amg` @ `c7779f81` | `GlobalAMG` / `SmoothedAggregationPreconditioner` / `RugeStubenPreconditioner`. Default `P` is tiled (`global-amg-distributed-P`). Check `‖Ax−b‖`, not only `stats.solved`. AWS: global_amg 113, then 112/112 after distributed-`P`. | 2026-09-07 |
| P0 | operator-types | done | `linalg/operator-types` @ `9158cb47` | `Projected` (nullspace; constructor orthonormalizes) + `BlockOperator` / `BlockDiagonalPC`. AWS: iterativesolvers 346, array/linalg pass. | 2026-09-07 |
| P0 leftover | numeric-refactor | done | `linalg/numeric-refactor` @ `f4265d49` | `lu!(F::DaggerSparseLU, A)` / `cholesky!(F::DaggerSparseCholesky, A)` reuse KLU/CHOLMOD symbolic; PureUMFPACK cannot cheap-refactor (rebuilds `splu` into the same box). Disambiguated `DaggerSparseLU \ DVector`. AWS: global_amg 113, sparsedirect 349, linearsolve 33, assembly 64. | 2026-09-07 |
| P0 leftover | global-amg-distributed-P | done | `linalg/amg-distributed-p` @ `058c489c` | Tiled GlobalAMG `P` (per-tile SA/RS + leftover interface matching + Jacobi smooth). Default setup does not collect `A`. Per-tile `AMGPreconditioner` unchanged. Kept ultra `nullspace=` / `_solve_pinned_dvector`. Incoming lesson 35 is 42. Interface-aggregate merge was tried and reverted. Remaining gathers: coarsest LU, one row of tiles per coarsen, header fetch, GPU tile host-stage; `nullspace=N` still gathers `A`+`N`. AWS GlobalAMG 112/112 with true residuals. | 2026-09-07 |
| P0 leftover | ras-symmetric | done | `linalg/ras-symmetric` @ `a552987b` | `AdditiveSchwarzPreconditioner(; type=:restrict|:basic)`. Default restrict unchanged. `:basic` is PETSc `PC_ASM_BASIC` (SPD-preserving, for CG). AWS: iterativesolvers 419/419, full array/linalg pass. | 2026-09-07 |
| P1 | stencil-gmg | done | `linalg/stencil-gmg` @ `f78eae25` | Matrix-based `GeometricMultigrid` (injection/full-weighting R, linear/bilinear P, Galerkin RAP, V-cycle `mul!`). **FLAG:** `jps/sparse-stencil` is same-idx halo only — not usable for restriction/prolongation; GMG is matrix-based. AWS: gmg 158/158, full array/linalg pass, GlobalAMG 113 unchanged. | 2026-09-07 |
| P1 | csr-bsr | done | `linalg/csr-bsr` @ `775d4d4a` | Host CSR tiles via SparseMatricesCSR (`distribute`, `sparsecsr`, `spzeros`). CSC unchanged; GPU stays CSC. **BSR deferred** (no host ecosystem type; `BlockArrays` is dense mortar, `CuSparseMatrixBSR` is device-only — do not invent `Dagger.BSR`). Incoming lesson 35 is 44; incoming lesson 36 already lesson 36. Kept ultra `_solve_pinned_dvector`. AWS CSR 83, full array/linalg green after LU `\` disambiguation. | 2026-09-07 |
| P1 | graph-partition | done | `linalg/graph-partition` @ `21fd1755` | `repartition` / `distribute` with `partitioner=Metis` or `perm=` (no `Dagger.metis`). Schur LU still uses `_nested_dissection_partition`. AWS: graph partition 295, sparsedirect 334. | 2026-09-07 |
| P1 | sparse-eigen | done | `linalg/sparse-eigen` @ `10594d72` | `LinearAlgebra.eigen` / `eigvals` on `DMatrix` via LOBPCG (`nev=1`, `which=:SR` default). Dense SVD unchanged. Kept ultra `_solve_pinned_dvector`; incoming lesson 35 is 43 (incoming 36 already lesson 36). AWS: Eigen 34, SVD 419. | 2026-09-07 |
| P1 | mixed-precision | done | `linalg/mixed-precision` @ `e4420917` | Mixed-eltype `mul!` / GEMM / GEMV; FP32 PC + FP64 Krylov apply (tile-local convert). Kept ultra `_solve_pinned_dvector`; incoming lesson 35 is 41. AWS: matmul 662, iterativesolvers 401, GlobalAMG 113. | 2026-09-07 |
| P1 | gpu-pc | done | `linalg/gpu-pc` @ `430aa1b2` | Device-side block-PC apply (Jacobi / ILU / AMG / RAS) keeps GPU Krylov vectors in VRAM (`memory_space_scope`, vendor LU / `DeviceILU0`). CPU `array/linalg/iterativesolvers` 404 passed; ROCm RX 6800 XT `gpu_pc_defs` 14/14. CUDA / MPI×GPU / Metal / OpenCL / oneAPI not device-validated. | 2026-09-07 |
| P1 | near-nullspace | done | `linalg/near-nullspace` @ `d2277c01` | `SmoothedAggregationPreconditioner(A; nullspace=N)` and `GlobalAMG(Projected(A, N))`. `AMGPreconditioner` unchanged. Default SA setup is now tiled (`global-amg-distributed-P`); `nullspace=N` still gathers `A`+`N` so coarse levels get `R`. AWS: near-nullspace 40, GlobalAMG 113, iterativesolvers 395. | 2026-09-07 |
| P1 | block-krylov | done | `linalg/block-krylov` @ `22cc9ae0` | Sparse `A \\ B` / `ldiv!` uses Krylov `block_gmres` (not a column loop). LinearSolve GMRES/MINRES accept `DMatrix` RHS. Kept ultra `_solve_pinned_dvector`; incoming lesson 35 is 45. AWS: iterativesolvers 421, LinearSolve 34. | 2026-09-07 |
| P1 | slicing | done | `linalg/slicing` @ `48bc09d4` | Range `getindex` / `view` / `setindex!` match Base and stay tiled (one task per tile pair). **`view` remains `SubArray`** (intentional; not a DArray-valued view). StepRange `copyto!` still throws. AWS: indexing 251, copyto 650. | 2026-09-07 |
| P2 | symm-hemm | done | `linalg/symm-hemm` @ `ce57f916` | Dense `BLAS.symm!` / `BLAS.hemm!` and `mul!` of `Symmetric`/`Hermitian` `DMatrix`. Typed α/β match stdlib; `Number` converts (`1+0im`). AWS matmul 718/718 on job `2d19db10113920ac`. | 2026-09-08 |
| P2 | sparse-qr | done | `linalg/sparse-qr` @ `0f93b02e` | Sparse-backed `qr` / `qr!` gather-then-SPQR (`DaggerSparseQR`). Multi-RHS uses `_direct_solve` (tall `n×p`). Lesson 46. AWS: sparseqr 18/18, dense qr 184/184. | 2026-09-08 |
| P2 | matrix-io | done | `linalg/matrix-io` @ `d2a05594` | `MatrixMarket.mmread` / `mmwrite` and `DelimitedFiles.readdlm` / `writedlm` on `DArray` / `Blocks`. Sparse write via `_collect_sparse_dmatrix`. AWS: matrixio 9/9 (`Pkg.resolve()` on the test project). | 2026-09-08 |
| P2 | dense-schur | done | `linalg/dense-schur` @ `2c1e32b9` | `schur(::DMatrix)` gather-then-LAPACK for dense tiles; sparse-backed throws. Does **not** replace LOBPCG `eigen`. Lesson 47. AWS: schur 8/8, eigen 34/34. | 2026-09-08 |
| P2 | einsum | done | `linalg/einsum` @ `245f68df` | User-approved `Dagger.@einsum` (tiled Datadeps, `@stencil` style). Lesson 49. **FLAG:** TensorOperations / OMEinsum / Tullio still need a `DArray` tensor backend (second invention; not added). AWS: einsum 16/16 on job `a332fb32a54319f5`; tail after known NNS 39/40. | 2026-09-09 |
| P2 | mpi-vecghost | skipped | — | **FLAG:** MPI chunks are already rank-owned; `HaloArray` / `@stencil` already express ghosts. A public `Dagger.VecGhost` would be a new type. Discuss only — do not ship. | 2026-09-08 |
| P2 | sparse-inv | skipped | — | Already `factorize` + `ldiv!` into `I`. Result is a dense inverse; that is LinearAlgebra's `inv`, not an API hole. | 2026-09-08 |
| P2 | bsr-tiles | done | `linalg/bsr` @ `5cfa88cb` | User-approved host `SparseMatrixBSR` (block-CSR; `Dagger.BSR` is only an alias). CSC default; host CSR unchanged; GPU stays CSC. `mul!` / `*` / `distribute` / `spzeros(SparseMatrixBSR,…)`. Lesson 50. Assembly via `sparsebsr(I,J,V,…,part)`; block PCs still collect a tile to CSC. AWS: BSR 27/27 + CSR 83 on job `d951c6a41f5c60d6`. | 2026-09-09 |
| P2 | stencil-gmg-xfer | skipped | — | **FLAG:** `jps/sparse-stencil` cannot express restriction/prolongation (lesson 37). Matrix `GeometricMultigrid` already landed. Do not grow the stencil stack. | 2026-09-08 |
| P0 leftover | blas1-fastpath | done | `linalg/blas1-fastpath` @ `69c88672` | Local `ThreadProc`+CPURAM BLAS-1 (`dot`/`axpy!`/`axpby!`/`norm`/`copyto!`/`fill!`/`rmul!`/`lmul!`) skips `spawn_datadeps`; MPI/remote/GPU stay on Datadeps (no second MPI path). Lesson 48. **Sweep may start.** AWS job `50a5dd19ede8a63e`: core 43 (incl. local vs Datadeps), rest green except known NNS 39/40. | 2026-09-09 |
| P0 | boomeramg-alike | in progress | `linalg/boomeramg-alike` | PMIS coarsening, deeper RAP, Jacobi/ℓ1/Chebyshev/hybrid-GS/ILU/RAS level smoothers, W/F-cycle, `blocksize`, NNS gathers `N` only. Lesson 51. Per-tile `AMGPreconditioner` unchanged. | 2026-09-10 |

---

## Changelog (merges onto `Dagger-linalg-ultra`)

| Date | Merge | Notes |
|---|---|---|
| 2026-09-07 | tracking doc only | Integration branch created at `origin/master` (`39528ab3`). |
| 2026-09-07 | `b31d56c0` `linalg/sparse-solve-dispatch` @ `57c7414b` | No conflicts. |
| 2026-09-07 | `f12a2f2a` `linalg/linearsolve` @ `55c38361` | Conflict: `AGENTS.md` (kept both lesson 27s as 27–28). |
| 2026-09-07 | `08074b4e` `linalg/assembly` @ `6018c6f4` | Conflicts: `AGENTS.md` (lesson 29), `test/runtests.jl` (kept LinearSolve + assembly entries). |
| 2026-09-07 | `bd3943d3` `linalg/overlapping-asm` @ `5a2102f7` | Conflict: `AGENTS.md` (lesson 30). Docs/ext auto-merged. |
| 2026-09-07 | `0e3c0e48` `linalg/sparse-chol-ic` @ `5c1f9991` | Conflicts: `AGENTS.md` (lesson 31), `ext/SparseArraysExt.jl` (union assembly + IC0). `runtests.jl` auto-kept `sparsechol.jl`. |
| 2026-09-07 | `79fa35f5` `linalg/global-amg` @ `c7779f81` | Conflicts: `AGENTS.md` (lesson 32), `docs/src/iterative-solving.md` (kept ASM prose + GlobalAMG warning). |
| 2026-09-07 | `e5e7eaaa` `linalg/operator-types` @ `9158cb47` | Conflict: `AGENTS.md` (lessons 33–34; Projected orthonormalize folded into 33). |
| 2026-09-07 | tracking doc only | Assigned P0 leftovers (`numeric-refactor`, `global-amg-distributed-P`, `ras-symmetric`) and nine P1 workstreams to sibling agents. No feature-branch merges. |
| 2026-09-07 | `eefca27c` `linalg/gpu-pc` @ `430aa1b2` | No conflicts (branch was based on integration HEAD). CPU iterativesolvers 404; ROCm `gpu_pc_defs` 14/14; CUDA unvalidated. |
| 2026-09-07 | `405b3a69` `linalg/numeric-refactor` @ `f4265d49` | Conflict: `AGENTS.md` (kept GPU-PC lesson 35; numeric-refactor is 36). Docs/ext/tests auto-merged. AWS: global_amg 113, sparsedirect 349, linearsolve 33, assembly 64. PureUMFPACK cannot cheap-refactor. |
| 2026-09-07 | `0ab8911b` `linalg/stencil-gmg` @ `f78eae25` | Conflict: `AGENTS.md` (kept GPU-PC 35 + numeric-refactor 36; GMG is 37). Docs/`runtests`/`SparseArraysExt` auto-merged. Auto-merge also duplicated `DaggerSparseLU \ DVector`; kept the numeric-refactor concrete methods (also covers Cholesky) and dropped the incoming `invoke`. **FLAG:** `jps/sparse-stencil` is same-idx halo only — not usable for restriction/prolongation; GMG is matrix-based. AWS: gmg 158/158, full array/linalg pass, GlobalAMG 113 unchanged. |
| 2026-09-07 | `3fddd5f6` `linalg/ras-symmetric` @ `a552987b` | Conflicts: `docs/src/index.md`, `docs/src/iterative-solving.md` (union: incoming `:restrict`/`:basic` plus HEAD GPU ILU / GMG wording). AGENTS/src/tests auto-merged (lesson 30 now documents `:basic`). Auto-merge also duplicated `DaggerSparseLU \ DVector`; kept the numeric-refactor `_solve_pinned_dvector` methods and dropped the incoming `invoke`. AWS: iterativesolvers 419/419, full array/linalg pass. |
| 2026-09-07 | `f3128a64` `linalg/slicing` @ `48bc09d4` | Conflict: `AGENTS.md` (kept GPU-PC 35 + numeric-refactor 36 + GMG 37; slicing `view`/`SubArray` is 38). FEATURES_ROADMAP / `darray.md` / indexing / copy / tests auto-merged. **`view` stays `SubArray`** (intentional). StepRange `copyto!` still throws. No `DaggerSparseLU \ DVector` change. AWS: indexing 251, copyto 650. |
| 2026-09-07 | `10a08071` `linalg/near-nullspace` @ `d2277c01` | Conflicts: `AGENTS.md` (kept GPU-PC 35 + numeric-refactor 36 + GMG 37 + slicing 38; near-nullspace is 39), `docs/src/iterative-solving.md` (union: HEAD GMG wording + incoming elasticity/`nullspace=`), `test/runtests.jl` (kept GMG + near-nullspace). Auto-merge also duplicated `DaggerSparseLU \ DVector`; kept the numeric-refactor `_solve_pinned_dvector` methods and dropped the incoming inline duplicate. Incoming lesson 36 was already lesson 36. AWS: near-nullspace 40, GlobalAMG 113, iterativesolvers 395. |
| 2026-09-07 | `d94c971b` `linalg/graph-partition` @ `21fd1755` | Conflicts: `AGENTS.md` (kept GPU-PC 35 + numeric-refactor 36 + GMG 37 + slicing 38 + near-nullspace 39; incoming graph-partition lesson 35 is 40), `src/array/sparsedirect.jl` (kept ultra `_solve_pinned_dvector`; unioned the comment to mention GMG + Schur). `FEATURES_ROADMAP.md` unchanged (Performance-table / Indexing-Slicing row kept). Docs/`copy.jl`/`darray.jl`/`MetisExt`/`runtests` auto-merged. No `Dagger.metis`. Schur still `_nested_dissection_partition`. AWS: graph partition 295, sparsedirect 334. |
| 2026-09-07 | `77f3c9be` `linalg/mixed-precision` @ `e4420917` | Conflicts: `AGENTS.md` (kept GPU-PC 35 + numeric-refactor 36 + GMG 37 + slicing 38 + near-nullspace 39 + graph-partition 40; incoming mixed-precision lesson 35 is 41; dropped incoming lesson 36 as already lesson 36), `docs/src/darray.md` (union: mixed-eltype `mul!` plus sparse `lu!`/`cholesky!` notes), `src/array/iterativesolvers.jl` (union: GPU `_supports_device_apply` plus `_apply_inverse_mixed!`), `src/array/sparsedirect.jl` (kept ultra `_solve_pinned_dvector`; dropped incoming `_pinned_factor_solve` / duplicate `DaggerSparseLU \ DVector`). `FEATURES_ROADMAP.md` unchanged. `mul.jl` / iterative-solving docs / tests auto-merged. AWS: matmul 662, iterativesolvers 401, GlobalAMG 113. |
| 2026-09-07 | `ad859bd6` `linalg/amg-distributed-p` @ `058c489c` | Conflicts: `AGENTS.md` (kept 35–41; incoming tiled-`P` lesson 35 is 42), `docs/src/iterative-solving.md` (union: incoming tiled `P` plus HEAD `nullspace=` / GMG), `ext/AlgebraicMultigridExt.jl` / `src/array/amg.jl` (union: incoming tiled interpolation plus ultra `nullspace=` / `nmodes` / `Projected`; default SA is tiled, `nullspace=N` still gathers). `sparsedirect.jl` auto-merged `COLLECT_SPARSE_DMATRIX_MAXSIZE`; dropped incoming `invoke` `DaggerSparseLU \ DVector` and kept ultra `_solve_pinned_dvector`. `FEATURES_ROADMAP.md` unchanged. Tests auto-merged (no-collect-`A` + drop 5% Jacobi margin). Interface-aggregate merge was tried and reverted. Remaining gathers: coarsest LU, one row of tiles per coarsen, header fetch, GPU tile host-stage; `nullspace=N` still gathers. AWS GlobalAMG 112/112 with true residuals. |
| 2026-09-07 | `167f047d` `linalg/sparse-eigen` @ `10594d72` | Conflicts: `AGENTS.md` (kept 35–42; incoming eigen lesson 35 is 43; incoming lesson 36 already lesson 36), `src/array/sparsedirect.jl` (kept ultra `_solve_pinned_dvector`; dropped incoming `_pinned_solve` / Union `\\` / duplicate `DaggerSparseLU \\ DVector`). `FEATURES_ROADMAP.md` unchanged (Performance-table / Indexing-Slicing row kept). Docs/`Dagger.jl`/`runtests` auto-merged; new `eigen.jl` + tests. Dense `svd.jl` untouched. AWS: Eigen 34, SVD 419. |
| 2026-09-07 | `d266fe02` `linalg/csr-bsr` @ `775d4d4a` | Conflicts: `AGENTS.md` (kept 35–43 after sparse-eigen; incoming CSR lesson 35 is 44; incoming lesson 36 already lesson 36). Auto-merge also duplicated `DaggerSparseLU \ DVector`; kept ultra `_solve_pinned_dvector` (incoming method dropped; `sparsedirect.jl` therefore unchanged vs HEAD). `FEATURES_ROADMAP.md` unchanged (Performance-table / Indexing-Slicing row kept). Docs/`sparse.jl`/`Project.toml`/`runtests` auto-merged; new `SparseMatricesCSRExt` + `matmul_csr.jl`. CSC path in `SparseArraysExt` unchanged. **BSR deferred** (no host ecosystem type). AWS CSR 83, full array/linalg green. |
| 2026-09-07 | `c835b8ab` `linalg/block-krylov` @ `22cc9ae0` | Conflicts: `AGENTS.md` (kept 35–44; incoming block-krylov lesson 35 is 45), `ext/LinearSolveExt.jl` (union: incoming `_require_darray_rhs` / `DMatrix` RHS plus HEAD `lu!` reuse), `src/array/mul.jl` (kept HEAD mixed-eltype GEMM; took incoming host×`DMatrix` GEMM), `test/array/linalg/linearsolve.jl` (kept both `lu!` reuse and multi-RHS testsets). Auto-merge also duplicated `DaggerSparseLU \\ DVector`; kept ultra `_solve_pinned_dvector` and dropped the incoming `invoke`. `FEATURES_ROADMAP.md` / Performance tables unchanged. AWS: iterativesolvers 421, LinearSolve 34. |
| 2026-09-08 | tracking doc + profile harness | `linalg_profile.jl` (`LINALG_BENCH_PROFILE=…`) and a Bottlenecks section from AWS MT profiles at `862841e5`. No Dagger API changes. |
| 2026-09-08 | tracking doc only | P2 backlog: accept `symm-hemm`, `sparse-qr`, `matrix-io`, `dense-schur`. Flag/skip einsum, VecGhost, sparse `inv`, BSR, stencil GMG transfers. No feature-branch merges yet. |
| 2026-09-08 | `ced9fdd0` `linalg/symm-hemm` @ `ce57f916` | No conflicts. AWS matmul 718/718. |
| 2026-09-08 | `7186a439` `linalg/sparse-qr` @ `0f93b02e` | No conflicts. Lesson 46. AWS sparseqr 18, qr 184. Kept ultra `_solve_pinned_dvector`. |
| 2026-09-08 | `026385e6` `linalg/dense-schur` @ `2c1e32b9` | Conflict: `AGENTS.md` (kept sparse-QR 46; incoming schur is 47). Docs/`runtests` auto-merged. AWS schur 8, eigen 34. |
| 2026-09-08 | `0770052b` `linalg/matrix-io` @ `d2a05594` | No conflicts. AWS matrixio 9/9. |
| 2026-09-08 | tracking + full suite | Job `2d19db10113920ac`: P2 suites green; full `array/linalg` (no Finch) green except NNS 39/40 (`24>25` P-width, twice). Remaining after NNS: sparsedirect 349, linearsolve 42, assembly 64, matrixio 9, partition 295. |
| 2026-09-09 | docs + tracker | `@stencil` cannot express GMG transfers (`docs/src/stencils.md` `stencil-no-gmg`); AMG vs BoomerAMG coverage table (this file + shorter `iterative-solving.md`). Best-config sweep numbers wait for `linalg/blas1-fastpath`. |
| 2026-09-09 | sweep harness | `run_linalg_sweep.sh` + `LINALG_BENCH_SWEEP` in `linalg_integration.jl`. Pending BLAS-1 before measured best-config tables. |
| 2026-09-09 | `d81d17ff` `linalg/blas1-fastpath` @ `69c88672` | No conflicts. Lesson 48. AWS `50a5dd19ede8a63e`: core 43; known NNS 39/40 (`24>25`); tail sparsedirect 349, linearsolve 42, assembly 64, matrixio 9, partition 295. **BLAS-1 is on origin — sweep can start.** |
| 2026-09-09 | `d2784096` `linalg/bsr` @ `5cfa88cb` | Conflict: `AGENTS.md` (kept BLAS-1 48; incoming BSR is 50). Lesson 50. Kept `_solve_pinned_dvector`. AWS `d951c6a41f5c60d6`: BSR 27, CSR 83, same known NNS leftover + green tail. |
| 2026-09-09 | `3bfcebf6` `linalg/einsum` @ `245f68df` | Conflicts: `AGENTS.md` (inserted lesson 49 between 48 and 50), `docs/src/darray.md` / `index.md` (union BLAS-1 + stencil-no-gmg + einsum). **FLAG:** no TensorOperations/OMEinsum/Tullio backend. AWS `a332fb32a54319f5`: einsum 16/16 after n-ary `*` + `LinearAlgebra.transpose` fixes. |
| 2026-09-09 | sweep numbers | Job `f74175e59a65e3ec` (`c6i.4xlarge`) measured SHA `6bf9aa2b` (post BLAS-1 / BSR / einsum). Best-config + compact 2-D grid in Performance. 1drow SpMV OOM; rest of 1drow/1dcol/auto aborted. MPI sweep not launched. |

## Remaining follow-ups

P0 leftovers are all merged. Honest remaining gathers on GlobalAMG setup (do not treat these as “`P` still collects `A`”):

- coarsest LU (`_gather_sparse`, not `_collect_sparse_dmatrix`)
- one row of tiles per coarsen task (stays on the worker; PMIS iterates on headers)
- membership / header fetch (`nagg` + C/F / aggregate ids), not `A`
- GPU tile host-stage
- `nullspace=N` gathers `N` only so coarse levels get `R` from `fit_candidates`

Unassigned leftover from P0:

- **`inv` on a sparse-backed `DMatrix`** uses the sparse factor (`factorize` + `ldiv!` into `I`) rather than a dedicated sparse inverse. The result is still a dense `I` solve. **P2: skipped** — not an API hole.

Unassigned leftover from `csr-bsr`:

- **BSR tiles** — **done** (`SparseMatrixBSR`, lesson 50). Still flagged: GPU vendor BSR; block PCs collect a tile to CSC; `allocate_tiled` still makes CSC zeros (convert back with `sparsebsr(A, part, blocksize)`).

P2 flags (completeness, not scheduled):

- **Einsum ecosystem backends** — `Dagger.@einsum` landed (lesson 49). TensorOperations / OMEinsum / Tullio still need a `DArray` tensor backend (second invention; not added).
- **MPI owned+ghost / `VecGhost`** — rank-owned `Chunk` + `HaloArray` / `@stencil` already cover this. A new public vector type is out of scope.
- **`@stencil` restriction/prolongation** — not usable for GMG (lesson 37).
  Matrix `GeometricMultigrid` is the transfer path. User-facing write-up:
  `docs/src/stencils.md` (`stencil-no-gmg`), plus the GMG / BoomerAMG
  sections in `docs/src/iterative-solving.md`. Short recap below.
- **Full dense geev / ScaLAPACK Schur** — not required. `eigen` stays LOBPCG; P2 `schur` is gather-then-LAPACK for dense tiles only.
- **Near-nullspace Q1 elasticity `P`-width assert** — leftover `24 > 25` was gathered-SA vs tiled-SA using different aggregate counts. `linalg/boomeramg-alike` uses the same tiled PMIS `AggOp` for scalar and `nullspace=N` (`fit_candidates` injects `nmodes` columns). Do not weaken the assert.

AWS labeling (2026-09-08 `vmbench.py` working-tree tweak): EC2 `Name` is now the launch `--label` (was always `vmbench`), plus `vmbench-label` / `vmbench-pid` / `vmbench-started`. `batchd` still calls `provision_vm` without `label=`, so new `batchctl` VMs would tag `Name=vmbench`. Pre-tweak instances (including `i-021ef9ff800fc17a4`) have no `vmbench-label` tag. Filter/teardown by **job id**. Reserved `dagger-distributed` (`2f5b7c978c2a0b3a`) and `dagger-mpi` (`a1e9f3af2f347b8d`) are already `done` in batchd — do not `done` them again.

`AGENTS.md` lessons 27–51 are the union (through dense `schur` 47; BLAS-1 is 48; `@einsum` is 49; BSR is 50; BoomerAMG-alike PMIS is 51). Lesson 20 remains unused (pre-existing gap). Lesson 35 is GPU-PC; do not reuse that number.

---

## Design notes

### Why `@stencil` cannot do GMG restriction / prolongation

`@stencil` (including `origin/jps/sparse-stencil`) is a **same-`idx`,
same-size, same-chunk** halo sweep. Lowering in `src/array/stencil.jl`
(`macro stencil`) requires `r_idx == write_idx` — a neighborhood at any
other index throws `ArgumentError` — and spawns one task per destination
chunk that reads the **same** `chunk_idx` from every operand. Halos are
`{−1,0,+1}` same-grid neighbors (`select_neighborhood_info`); the kernel
(`cpu_stencil_sweep!`) iterates `axes(output)`. Operands must share shape
and layout.

Geometric restriction / prolongation maps a fine grid of size `n` onto a
coarse grid of size `n/2` (injection / full-weighting / linear / bilinear).
That is a **different index and a different array size**. We did **not**
grow a competing stencil stack. `GeometricMultigrid` is
matrix-based: `_gmg_restriction_coo` / `_gmg_prolongation_coo`
(`src/array/gmg.jl`) assembled as sparse `DMatrix`s
(`sparse(I, J, V, …, Blocks)` in `ext/SparseArraysExt.jl`), Galerkin
`Ac = R A P` via existing `mul!`, V-cycle `_gmg_vcycle!`. Making
`@stencil` do this would need mapped indices, different-sized operands,
and a gather of source chunks that overlap the mapped neighborhood — a
new language. Not scheduled. User-facing: `docs/src/stencils.md`
(`stencil-no-gmg`).

### AMG vs HYPRE BoomerAMG

Not PETSc/HYPRE parity. Judge AMG quality by `‖Ax−b‖`, never by Krylov
`stats.solved` (lesson 19: per-tile AMG can look solved with a huge true
residual; lesson 32: one Jacobi sweep each side of a V-cycle can lose to
Jacobi-only).

| BoomerAMG feature | Dagger | Notes |
|---|---|---|
| Global V-cycle over a distributed operator | **Yes** | `GlobalAMG` / `SmoothedAggregationPreconditioner` / `RugeStubenPreconditioner`: tiled PMIS `P` (lessons 42 / 51), Galerkin `P'AP`, V/W/F-cycle. Recurses with distributed RAP until `max_coarse` / `max_levels` (default 10). Gathered LU only at the true coarsest. |
| Per-subdomain AMG | **Different meaning** | `AMGPreconditioner` is **block-diagonal Schwarz** (one AlgebraicMultigrid.jl hierarchy per diagonal tile). `Blocks(n,n)` is “global” only because there is one tile. Lesson 19. |
| Geometric / PFMG transfers | **Partial** | `GeometricMultigrid`: injection / full-weighting `R`, linear / bilinear `P`, Galerkin `RAP`, Jacobi V-cycle. 1-D and 2-D only. Not `@stencil`. |
| Overlapping Schwarz | **As its own PC** | `AdditiveSchwarzPreconditioner` (`:restrict` = `PC_ASM_RESTRICT`, `:basic` = `PC_ASM_BASIC`). Not a BoomerAMG smoother. |
| Near-nullspace / rigid-body modes | **Partial** | `SmoothedAggregationPreconditioner(A; nullspace=N)` / `GlobalAMG(Projected(A, N))` via `fit_candidates` on the tiled PMIS `AggOp`. RS rejects `nullspace`. `AMGPreconditioner` does not take it. Gathers `N` only (lesson 39 / 51). `blocksize` / `nvars` is HYPRE `NumFunctions`. |
| Coarsening: HMIS / PMIS / Falgout / CLJP / CGC / aggressive | **Partial** | Default `coarsen=:pmis` (tiled parallel independent set). `:hmis` freezes local SA then PMIS on leftovers. `:standard` is the old leftover-pair path. No Falgout / CLJP / CGC / aggressive. Do not merge already-assigned interface aggregates (tried; residual worse than Jacobi). |
| Interpolation: classical / extended / ext+i / FF / AIR / multipass | **Partial** | Tiled SA tentative `P` + Jacobi smooth; RS classical distance-1 from the row graph. No AIR, no ext+i, no FF, no multipass (`interp=:extended` throws). |
| Smoothers: hybrid GS, Schwarz, Chebyshev, ILU, FSAI, ℓ1-Jacobi | **Partial** | `smoother=:jacobi` (default, `relax=2/3`), `:l1jacobi`, `:chebyshev`, `:hybrid_gs` (local GS + Jacobi off-tile), `:ilu` / `:ras` as V-cycle level smoothers. No FSAI. |
| Cycle types: W, F, additive / mult-additive AMG | **Partial** | `cycle=:v` (default), `:w`, `:f`. No additive AMG. |
| Strength threshold / truncation / `Pmax` / non-Galerkin drop | **Partial** | AlgebraicMultigrid.jl `strength=` / `aggregate=` pass through on the tiled path; no HYPRE `Pmax` or non-Galerkin sparsification. |
| Complex arithmetic | **Missing** | Real `DMatrix` path. |
| Nodal / unknown-based systems | **Partial** | `blocksize` / `nvars` on SA (per-unknown constants when `nullspace` is omitted). Elasticity still wants `nullspace=N`. |
| Native GPU AMG setup / apply | **Missing** | AlgebraicMultigrid.jl is host. GPU-PC (lesson 35) keeps *vector* chunks on-device for some block PCs; the AMG hierarchy itself is still host. |
| Coarsest solve | Gathered LU | Same idea as HYPRE’s sequential coarse solve; we gather (`_gather_sparse`), not a distributed coarse AMG. |

**Covered (short):** global V/W/F-cycle with tiled PMIS `P` and distributed RAP; Jacobi / ℓ1-Jacobi / Chebyshev / hybrid GS / ILU / RAS level smoothers; SA near-nullspace (`N` only) and `blocksize`; geometric RAP V-cycle; RAS as `PCASM`; per-tile AMG as Schwarz (do not call that BoomerAMG).

**Missing (short):** Falgout / CLJP / CGC / aggressive coarsening; extended / AIR / FF interpolation; additive cycles; FSAI; complex; GPU BoomerAMG; HYPRE `Pmax` / non-Galerkin knobs.

---

## Performance

Reusable harness: `benchmark/suites/linalg_integration.jl` (driver
`benchmark/suites/run_linalg_integration.sh`). Deep warmup, then **min** of
timed runs (AGENTS.md lesson 4). Dagger uses `BLAS.set_num_threads(1)` (task
parallelism); host dense uses OpenBLAS at `nthreads`. Iterative methods share
`atol=1e-10`, `rtol=1e-8`, `itmax=500`, GMRES `memory=50`; tables report
iterations and the un-preconditioned `‖Ax−b‖/‖b‖`. Speedup is
baseline/Dagger (`>1` means Dagger is faster). Empty cells are omitted, not
invented.

**Blocksize / assignment sweep (measured, SHA `6bf9aa2b`):**
`benchmark/suites/run_linalg_sweep.sh` / `LINALG_BENCH_SWEEP=1`. Existing
knobs only (`Blocks`, `distribute` assignment, `2d`/`1drow`/`1dcol`/`auto`,
`Dagger.scope` / `ProcessScope`). Keys: `dense_gemm`, `sparse_spmv`,
`krylov_cg`, `krylov_blockjacobi`. Warmup 5, samples 3, **min** of timed
runs. Same Krylov `atol=1e-10` / `rtol=1e-8`; rows record `‖Ax−b‖/‖b‖`.
Raw: `benchmark/results/linalg_integration_sweep_mt.json`.

**Sweep hardware:** AWS `c6i.4xlarge` (16 vCPU, 32 GiB, `us-east-1`),
Julia 1.12.7, 16 Julia threads, 2026-09-09 UTC. Dagger SHA
**`6bf9aa2b5080e7072cc299a2b5da0715a51ab7f3`** (clone of
`origin/Dagger-linalg-ultra` after BLAS-1 `d81d17ff` / BSR `d2784096` /
einsum `3bfcebf6`). Job **`f74175e59a65e3ec`** (`linalg-sweep-mt`, then
`done`). This is **not** the pre-fast-path SHA `292cd672` used by the
full-suite MT table below — Krylov/BLAS-1 walls moved.

**Best configuration (min Dagger time):**

| Kernel | Best (tile, assignment, layout, scope) | Dagger | Host | Speedup | Residual / iters |
|---|---|---|---|---|---|
| Dense GEMM `A*B` (n=4096) | `Blocks(1024,1024)`, `:blockrow`, `2d`, `default` | 268 ms | 240 ms (OpenBLAS 16-thread) | 0.89× | — |
| Sparse SpMV (1-D Laplacian n=160000) | `Blocks(2048,2048)`, `:cyclicrow`, `2d`, `default` | 517 ms | 702 µs (host CSC, 1 thread) | 0.0014× | — |
| Krylov CG, no PC (2-D Laplacian n=4096) | `Blocks(2048,2048)`, `:blockrow`, `2d`, `threads:8` | 322 ms | 4.63 ms (`Krylov.cg(::CSC)`) | 0.014× | 196/196; `‖r‖/‖b‖` 8.82e-9 / 8.82e-9 |
| Krylov CG + BlockJacobi | `Blocks(2048,2048)`, `:blockrow`, `2d`, `threads:8` | 81.5 ms | 137 ms (serial per-block LU) | **1.68×** | 31/31; `‖r‖/‖b‖` 6.41e-8 / 6.41e-8 |

**Overall:** there is no single winner. Krylov and SpMV want the coarsest
2-D tile (`2048`); GEMM wants `1024` (8×8). Assignment `:blockrow` is
best or tied on every kernel except SpMV, where `:cyclicrow` is 3% faster
than `:blockrow` (534 ms). `:arbitrary` is never the winner. `threads:8`
helps CG/BlockJacobi a few percent at tile 2048 and hurts GEMM/SpMV
versus the default 16-thread scope. Prefer **`Blocks(2048,2048)` +
`:blockrow`** for solves; **`Blocks(1024,1024)` + `:blockrow`** for GEMM.

Post-BLAS-1 CG at tile 2048 is 322 ms vs the published 3.37 s at tile
1024 on SHA `292cd672` (same 196 iters and `‖r‖/‖b‖`). Fine tiles are
still a cliff: 2-D tile 256 CG is 7.14–8.69 s at the same residual.

**Compact grid** (2-D, `scope=default`; Dagger min times). Assignment is
a small delta; tile size is not.

Dense GEMM n=4096:

| tile | `:arbitrary` | `:blockrow` | `:cyclicrow` |
|---|---|---|---|
| 256 | 532 ms | 521 ms | 507 ms |
| 512 | 327 ms | 308 ms | 322 ms |
| 1024 | 314 ms | **268 ms** | 278 ms |
| 2048 | 477 ms | 467 ms | 469 ms |

Sparse SpMV, 1-D Laplacian n=160000 (2-D `Blocks(t,t)` on a 160k×160k
operator — `gemv_dagger!` still spawns empty tile pairs):

| tile | `:arbitrary` | `:blockrow` | `:cyclicrow` |
|---|---|---|---|
| 256 | 42.6 s | 41.5 s | 44.7 s |
| 512 | 10.6 s | 9.35 s | 9.13 s |
| 1024 | 2.34 s | 2.15 s | 2.25 s |
| 2048 | 576 ms | 534 ms | **517 ms** |

Krylov CG, no PC, 2-D Laplacian n=4096 (always 196/196 iters,
`‖r‖/‖b‖` 8.82e-9 / 8.82e-9):

| tile | `:arbitrary` | `:blockrow` | `:cyclicrow` |
|---|---|---|---|
| 256 | 7.51 s | 7.14 s | 7.43 s |
| 512 | 2.53 s | 2.35 s | 2.36 s |
| 1024 | 864 ms | 818 ms | 807 ms |
| 2048 | 368 ms | **348 ms** | 351 ms |

Krylov CG + BlockJacobi (iters drop as tiles grow: 79 → 55 → 43 → 31;
`‖r‖/‖b‖` 1.57e-8 / 3.28e-8 / 4.65e-8 / 6.41e-8, matched on the host):

| tile | `:arbitrary` | `:blockrow` | `:cyclicrow` |
|---|---|---|---|
| 256 | 3.25 s | 3.16 s | 3.24 s |
| 512 | 818 ms | 800 ms | 795 ms |
| 1024 | 284 ms | 266 ms | 285 ms |
| 2048 | 95.9 ms | **88.2 ms** | 92.2 ms |

`threads:8` grid (same 24 cells) is in the JSON; at the winning tiles it
is within ~10–20% of `default` (CG/BJ slightly faster, GEMM/SpMV
slightly slower). `ProcessScope` was not swept (one worker: nearly a
no-op vs `DefaultScope`).

**Sweep hung / skipped (no invented numbers):**

- **1drow SpMV** (n=160000, `Blocks(256, 160000)`): OOM-killed Julia
  (RSS 31.6 GiB / 32 GiB) during the first 1drow cell. Remaining 1drow
  SpMV not retried — a full-width tile densify is 256×160000×8×(n/256)
  ≈ 200 GiB if `getindex` goes dense.
- **1drow GEMM / CG / BlockJacobi:** one probe completed
  (`tile=256`, `:arbitrary`, `default`): GEMM 926 ms; CG 180 s at the
  same 196 iters / 8.82e-9 residual (vs 7.5 s for 2-D tile 256). Rest of
  1drow aborted as not sweep-viable. 1drow BlockJacobi not timed.
- **`1dcol` / `auto`:** not launched after the 1drow OOM / 180 s CG.
- **MPI sweep:** not launched (known hangs: `mul!(C,A,A)`, dense Chol/QR,
  SpMV, sparse `cholesky`/`klu`/`splu`, assembly, `lu!(F,A)`, mixed
  SpMV).

The full-suite MT / MPI tables below are earlier SHAs and are **not**
replaced by this sweep.

**Hardware / software (multi-threaded):** AWS `c6i.4xlarge` (16 vCPU, 32 GiB,
`us-east-1`), Julia 1.12.7, 16 Julia threads, 2026-09-07 (PDT) /
2026-09-08 UTC. Dagger code SHA **`292cd672`** (`Dagger-linalg-ultra` as of
that clone). Dense GEMM / QR / SpMV / SpGEMM: warmup 8, samples 5. Remaining
rows: warmup 5, samples 3. Krylov used 4×4 tiles (`Blocks(1024,1024)` on
`n=4096`); a 16×16 (`tile=256`) CG probe was ~5× slower (15.8 s) at the same
iteration count — scheduling-bound, not a different residual.

**Hardware / software (MPI):** AWS 4× `m6i.xlarge` (4 vCPU / 16 GiB each,
`us-east-1`), one rank per VM (`mpiexec --map-by ppr:1:node --bind-to none`),
Julia 1.12.7, 4 Julia threads/rank, system OpenMPI 4.1.6,
2026-09-08 UTC. Dagger code SHA **`92c2aee4`**. Warmup 3, samples 3 (min).
Host baseline is the same problem on `Array`/`SparseMatrixCSC` on every rank
(4-thread OpenBLAS) — a **single-node** ecosystem number, not a distributed
PETSc/Trilinos baseline (none was practical to stand up on this AMI). Raw
JSON: `benchmark/results/linalg_integration_mpi.json`.

A first `mul!(C, A, A)` GEMM hung in MPI aliasing `bcast_yield` (same array
as both operands). `C = A * B` matches `contrib/mpi/run_matmul.jl` and
completed (that script: n=2000, tile=1000, 8.52 s including compile). Several
other ops then hit the 300 s hang detector on `recv`/`send` to rank 3; those
rows are omitted, not invented. `jps/datadeps-region-async` was not rebased.

### Multi-threaded

| Feature | Problem | Dagger | Baseline (name) | Time D / Time B | Speedup | Notes |
|---|---|---|---|---|---|---|
| Dense GEMM / `mul!` | n=4096, tile=512×512, Float64, `C←A*A` | 292 ms | 233 ms (`mul!(::Matrix)` OpenBLAS) | 292 ms / 233 ms | 0.80× | Dagger BLAS=1; host BLAS=16 |
| Dense LU + `\` | n=2048, tile=256×256, factor + `\` | 137 ms | 50.6 ms (`lu(::Matrix)` LAPACK getrf) | 137 ms / 50.6 ms | 0.37× | |
| Dense QR + `\` | n=2048, tile=256×256, factor + `\` | 546 ms | 111 ms (`qr(::Matrix)` LAPACK geqrf) | 546 ms / 111 ms | 0.20× | |
| Dense Cholesky + `\` | n=2048, tile=256×256, SPD `G*G'` | 201 ms | 54.9 ms (`cholesky(::Matrix)` LAPACK potrf) | 201 ms / 54.9 ms | 0.27× | |
| Dense SVD | n=256, tile=128×128, `svd` only | 406 ms | 13.7 ms (`svd(::Matrix)` LAPACK gesdd) | 406 ms / 13.7 ms | 0.034× | tiled Jacobi vs LAPACK; modest size |
| Sparse SpMV | 1-D Laplacian n=160000, nnz=479998, tile=20000 | 8.92 ms | 592 µs (`*(::CSC, ::Vector)`) | 8.92 ms / 592 µs | 0.066× | host CSC SpMV is single-threaded |
| Sparse SpGEMM | `sprand` n=2500, p=0.008, nnz=49889, tile=625×625 | 16.0 ms | 14.3 ms (`*(::CSC, ::CSC)`) | 16.0 ms / 14.3 ms | 0.90× | host CSC×CSC is single-threaded |
| Krylov CG (no PC) | 2-D Laplacian 64×64 (n=4096, nnz=20224), tile=1024×1024 | 3.37 s | 5.45 ms (`Krylov.cg(::CSC)`) | 3.37 s / 5.45 ms | 0.0016× | iters 196/196; `‖r‖/‖b‖` 8.82e-9 / 8.82e-9 |
| Krylov GMRES (no PC) | same 2-D Laplacian | 77.4 s | 45.4 ms (`Krylov.gmres(::CSC)`) | 77.4 s / 45.4 ms | 0.00059× | iters 192/192; `‖r‖/‖b‖` 9.6e-9 / 9.6e-9; memory=50 |
| Krylov CG + Jacobi | same 2-D Laplacian | 3.79 s | 65.0 ms (`Krylov.cg` + `Diagonal`) | 3.79 s / 65.0 ms | 0.017× | iters 196/196; PC setup D=733 ms, B=69 µs |
| Krylov CG + BlockJacobi | same 2-D Laplacian | 846 ms | 56.8 ms (hand-rolled per-block LU) | 846 ms / 56.8 ms | 0.067× | iters 43/43; `‖r‖/‖b‖` 4.65e-8 / 4.65e-8; no ecosystem BlockJacobi |
| Krylov GMRES + BlockILU | same 2-D Laplacian | 3.86 s | 1.87 ms (`IncompleteLU.ilu` of whole CSC) | 3.86 s / 1.87 ms | 0.00049× | iters 40/8; host ILU is global (stronger); `‖r‖/‖b‖` 2.73e-7 / 4.56e-9 |
| Krylov GMRES + per-tile AMG | same 2-D Laplacian | 3.76 s | 3.04 ms (AlgebraicMultigrid RS, global) | 3.76 s / 3.04 ms | 0.00081× | iters 39/5; Dagger is block-diagonal (lesson 19); `‖r‖/‖b‖` 2.87e-7 / 4.74e-7 |
| Additive Schwarz (RAS) | same 2-D Laplacian, overlap=1 | 3.80 s | 8.97 ms (serial pre-factored RAS) | 3.80 s / 8.97 ms | 0.0024× | iters 39/39; `‖r‖/‖b‖` 3.12e-7 / 3.12e-7; no distributed RAS in Julia |
| Sparse `cholesky` + `\` | 2-D Laplacian 80×80 (n=6400, nnz=31680), tile=1280×1280 | 2.42 s | 6.22 ms (CHOLMOD `cholesky(::CSC)`) | 2.42 s / 6.22 ms | 0.0026× | Dagger gathers then CHOLMOD; both fit in RAM |
| Incremental `sparse(I,J,V, Blocks)` | 2-D Laplacian COO 200×200 (n=40000, nnz=199200), tile=2500×2500 | 111 ms | 59.0 ms (`sparse` then `distribute`) | 111 ms / 59.0 ms | 0.53× | baseline includes host CSC + distribute |
| LinearSolve `KrylovJL_GMRES` | 2-D Laplacian 64×64, tile=1024×1024 | 78.0 s | 42.6 ms (`KrylovJL_GMRES(::CSC)`) | 78.0 s / 42.6 ms | 0.00055× | same LinearSolve algorithm on both sides |
| `Projected` `mul!` | 1-D Laplacian n=2048, tile=256, constant nullspace | 28.0 ms | 19.0 µs (serial `PAP`) | 28.0 ms / 19.0 µs | 0.00068× | correctness-adjacent; constructor orthonormalizes |
| `BlockOperator` `mul!` | 2-field nest n=2048 (2×1024), tile=256 | 44.6 ms | 242 µs (assembled `*(::Matrix)`) | 44.6 ms / 242 µs | 0.0054× | correctness-adjacent; `hvcat` would assemble |

### MPI

| Feature | Problem | Dagger | Baseline (name) | Time D / Time B | Speedup | Notes |
|---|---|---|---|---|---|---|
| Dense GEMM / `mul!` | n=2048, tile=1024×1024, Float64, `C←A*B` | 261 ms | 120 ms (`*(::Matrix, ::Matrix)` OpenBLAS, 4 threads) | 261 ms / 120 ms | 0.46× | 4 ranks; Dagger BLAS=1; host is single-node |
| Dense LU + `\` | n=1024, tile=512×512, factor + `\` | 2.00 s | 16.4 ms (`lu(::Matrix)` LAPACK getrf) | 2.00 s / 16.4 ms | 0.0082× | |
| Sparse SpGEMM | `sprand` n=1600, p=0.008, nnz=20561, tile=800×800 | 14.6 ms | 4.22 ms (`*(::CSC, ::CSC)`) | 14.6 ms / 4.22 ms | 0.29× | host CSC×CSC is single-threaded |
| `Projected` `mul!` | 1-D Laplacian n=1024, tile=512, constant nullspace | 15.9 ms | 15.3 µs (serial `PAP`) | 15.9 ms / 15.3 µs | 0.00096× | correctness-adjacent |
| `BlockOperator` `mul!` | 2-field nest n=1024 (2×512), tile=512 | 18.6 ms | 159 µs (assembled `*(::Matrix)`) | 18.6 ms / 159 µs | 0.0086× | correctness-adjacent |

### Skipped on this pass (no invented numbers)

- **MT GlobalAMG, `Dagger.klu` / `splu`, LinearSolve `PureUMFPACKFactorization`:** at the MT SHA `292cd672`, `DaggerSparseLU \ DVector` is ambiguous. Later commits (`775d4d4a`+) add `_solve_pinned_dvector`. Not re-run on the MT box.
- **MT P1** (numeric refactor, tiled-`P` GlobalAMG, RAS `:basic`, GMG, CSR SpMV, graph `partitioner=`, `eigen`, mixed-precision, multi-RHS): not on the MT clone. Harness now has rows; MPI re-run hung or was not launched for these.
- **MPI hangs** (300 s deadlock detector, rank 0 ↔ rank 3 `recv`/`send`/`bcast_meta`): dense Cholesky, dense QR (also hung at 900 s and was aborted), sparse SpMV, sparse `cholesky`/`klu`/`splu`, incremental `sparse(I,J,V, Blocks)`, numeric `lu!(F,A)`, mixed-eltype SpMV. No times published.
- **MPI not launched** (too likely to hang given the above, or no GPU): Krylov CG/GMRES and all PCs (Jacobi, BlockJacobi, BlockILU, per-tile AMG, GlobalAMG, RAS `:restrict`/`:basic`, GMG), LinearSolve, `eigen`, CSR SpMV, graph Metis, multi-RHS, GPU-PC. No distributed non-Dagger baseline.
- **`jps/datadeps-region-async`:** not rebased; no second column.

### Bottlenecks (MT profile, 2026-09-08)

Reusable profile harness: `benchmark/suites/linalg_profile.jl`, same driver
(`LINALG_BENCH_PROFILE=1|cpu|alloc|logs|all`). Deep warmup (10) + min of 5
`Base.gc_num` deltas (AGENTS.md lesson 4), then one `Profile.@profile` pass and
one `enable_logging!` / `fetch_logs!` pass. **Unlogged wall times are the
truth**; log category times are *sums* over overlapping events and include
logging overhead, so they can exceed wall. CPU-sample buckets count a frame if
it appears anywhere in the sample: with 16 processor-runner threads, `other`
is ~100% (`pthread_cond_wait` / `poptask`) and `scheduler` is ~48%
(`Sch.jl` `start_processor_runner!`). That is idle-thread evidence, not
“half the useful work is the scheduler.”

**Hardware / software:** AWS `c6i.4xlarge` (16 vCPU, 32 GiB, `us-east-1`),
Julia 1.12.7, 16 Julia threads, Dagger SHA **`862841e5`**, job
`5d72cfb887d5a4a8` (`linalg-profile-mt`, then `done`). Raw:
`benchmark/results/linalg_profile_mt.json`. Score is roughly
`frequency × remaining_gap × fixability` (each 1–10). Published full-solve
times below are from the MT table (SHA `292cd672`); profile applies are SHA
`862841e5` on the same instance class.

**Not profiled (no invented numbers):** MPI (same hang list as the table);
full 192-iter GMRES (used `itmax=8`); GlobalAMG / `klu` / `splu` / `eigen` /
CSR / mixed-eltype / multi-RHS / GPU; `jps/datadeps-region-async` second
column (shared branches were not rebased).

#### Ranked list

| Rank | Bucket | Item | Score | Frequency | Remaining gap | Fixability | Evidence | Suspected cause | If fixed | Next experiment |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | P0 | Per-op `spawn_datadeps` / scheduler tax on BLAS-1 (`dot` / `axpy!` / `axpby!` / `rmul!` / `norm` / `copyto!` / `fill!`) | ~800 | Every Krylov iter, every PC apply, every `mul!` support op (SciML `KrylovJL_GMRES` included) | 4-tile `n=4096`: `axpy!` 1.23 ms vs 0.7 µs; `dot` 477 µs vs 0.7 µs; `norm` 971 µs vs 1.2 µs. ~7 such ops/CG iter ≈ 7 ms before SpMV | High for fusion / local tile loops / region-async; Dagger will not beat host BLAS-1 on 4k vectors | Steady `axpby!` compute 0.20 ms vs wall 0.95 ms; 4 tasks; 6.3k allocs / 284 KiB (`ThreadProc`, `LockedObject{PriorityQueue}`, `ExactScope`). 16 tiles: `axpy!` 3.00 ms, 23k allocs | Each BLAS-1 is its own datadeps region + 4 (or 16) thunks. Useful work is sub-µs per tile | Cuts CG from ~12 ms/iter toward the SpMV term (~4 ms) and GMRES by more (Arnoldi is almost all BLAS-1) | Fuse one Krylov iteration into **one** `spawn_datadeps`; or skip datadeps when every chunk is local `ThreadProc`. Private rebase onto `jps/datadeps-region-async` and re-time `axpy!` / CG-one-iter |
| 2 | P0 | All-pairs tiled SpMV (empty tiles still spawned) | ~630 | Every Krylov / LinearSolve iter; `Projected` / `BlockOperator` | Krylov 4×4: 3.69 ms vs 17.4 µs (16 tasks, **10** nonempty). 16×16: 22.9 ms vs 17.7 µs (256 tasks, **46** nonempty). Published 1-D `n=160k`: 7.26 ms vs 539 µs (64 tasks, **22** nonempty) | High to skip empty tiles / one task per row panel. Does not make 4k-unknowns SpMV beat host CSC | Logs: 16 / 256 / 64 `matvecmul!` tasks. 16×16 vs 4×4 is 6.2× at the same `nnz` | `gemv_dagger!` loops every `(row,col)` tile. 2-D Laplacian on 4×4 is already 6 structural zeros; 16×16 is 210 | 196-iter CG would drop ~0.7 s from SpMV alone at 4×4; 16×16 CG would stop being 5× worse | Count `nnz` (or a cached pattern) before `@spawn`; time 4×4 vs 16×16 again |
| 3 | P0 | Tile-count multiplier (same tax × tiles) | ~510 | Anyone who picks small tiles “for more parallelism” | Published 16×16 CG ~5× slower than 4×4 at the same 196 iters. Profile: `axpy!` 1.23→3.00 ms; SpMV 3.69→22.9 ms | Docs + default `Blocks` policy are cheap; real fix is ranks 1–2 | 4 vs 16 tiles, same `n=4096`, same residual | More regions and more thunks, not more useful FLOPs (tiles are 256³ or smaller) | Stops users from making Krylov 5× worse by tiling finer | Publish a tile-size note; re-run CG at `Blocks(2048,2048)` (2×2) |
| 4 | P0 | Per-task scheduler allocations | ~360 | Same as ranks 1–2 (per-argument / per-task; lesson 3) | 4k–6k allocs per 4-tile BLAS-1; 23k / 1.0 MiB per 4×4 SpMV; 58k / 2.4 MiB per CG-shaped iter; 479k / 20 MiB for `Krylov.cg` `itmax=8` | Medium–high (pools already exist; more reuse) | `Profile.Allocs` top types: `Dagger.ThreadProc`, `LockedObject{PriorityQueue{TaskSpec}}`, `start_processor_runner!` closures, `ExactScope`. Not the `Float64` buffers | Planning / fire / steal allocate on every thunk | Lowers GC on GMRES (1.07M allocs / 44 MiB at `itmax=8`) and assembly (740k / 59 MiB) | `measure_steady_state_allocs` on `axpy!` after a region-async rebase (lesson 4) |
| 5 | P1 | `Krylov.cg` / SciML path (sum of P0) | — | Default iterative solve when the operator is SPD | Published 3.37 s vs 5.45 ms (196/196 iters, same `‖r‖/‖b‖`). Profile: **11.95 ms** for a CG-shaped iter (44 tasks: 16 `matvecmul!` + 12 `dot` + 8 `axpy!` + 4 `axpby!` + 4 `mapreduce`); `itmax=8` real `Krylov.cg` 101 ms (336 tasks, 12.7 ms/iter) | High *if* P0 lands; not a separate kernel bug | 196 × 11.95 ms ≈ 2.34 s, vs published 3.37 s (extra workspace `similar` / `copyto!` / stopping). Jacobi apply is another 1.80 ms (4 tasks) — published CG+Jacobi 3.79 s | Not “CG is slow”: 44 tiny tasks per iter on a 4k system | Same 196 iters at a few ms each would be competitive with host on this size only after P0; at large distributed `n` the tax is amortized | After P0, re-run the published CG row (do not treat `stats.solved` as `Ax≈b`, lesson 19) |
| 6 | P1 | `Krylov.gmres` / `LinearSolve.KrylovJL_GMRES` (Arnoldi × P0) | — | `defaultalg` for a general `DMatrix` / SciML | Published 77.4 s / 78.0 s vs 45.4 / 42.6 ms (192 iters). Profile `itmax=8` `memory=50`: 246 ms, **748 tasks** (128 `matvecmul!`, 176 `axpy!`, 144 `dot`, 224 `allocate_array` workspace) | High *if* P0 lands; GMRES will always do more BLAS-1 than CG | 748/8 ≈ 94 tasks/iter already at `itmax=8`; late Arnoldi steps approach `memory` dots+axpys. Workspace `similar(b)` is 50+ vectors × 4 tiles (one-time) | Arnoldi is rank-1 of the P0 tax. LinearSolve is the same algorithm | The 1800× table row moves with BLAS-1+SpMV, not with a LinearSolve wrapper | Time `itmax=20` and plot tasks/iter vs `j`; do not run a 77 s profile loop |
| 7 | P1 | Incremental `sparse(I,J,V, Blocks)` | — | Once per assembly / timestep | Profile 112 ms vs 31 ms host `sparse`+`distribute` (published 111 vs 59 ms). 512 tasks (256 `_assemble_coo_into_tile` + 256 `allocate_array`); 740k allocs / 59 MiB | Medium. MPI must keep a rank-uniform extract set (lesson 29) | `schedule`+`add_thunk` dominate the log sums; sparse compute is 3% of CPU samples | One spawn per (COO chunk, dest tile), including empties | Maybe ~2× vs host at this size; not the Krylov cliff | Profile with one COO chunk vs many; do not drop empty extracts under MPI |
| 8 | P1 | Dense GEMM `A*B` | — | Dense `mul!` users | Profile 332 vs 241 ms (0.73×; published 0.80×). 576 tasks (512 `matmatmul!` + 64 `allocate_array`). **Only kernel where BLAS is visible** (23% of samples) | Low at `n=4096`: 16-thread OpenBLAS is the right host. Maybe 30–50 ms of scheduler left | 440k allocs / 149 MiB ≈ one extra `n×n` (`C`) plus planning. Tile GEMM is real work (compute log-sum 4.43 s over 16 threads ≈ wall) | `BLAS.set_num_threads(1)` per tile vs host 16-thread GEMM; 8×8×8 reduction | Do not expect to beat host at this size. Larger `n` / multi-node is the actual target | Repeat at `n=8192` tile=1024; do not raise Dagger BLAS threads |
| 9 | P2 | Sparse `cholesky` gather-then-CHOLMOD | — | `A\b` direct; GlobalAMG / GMG coarse `\` | Setup 3.57 s; apply 1.29 ms vs host 405 µs. Combined factor+`\` 2.26 s (published 2.42 s vs 6.22 ms). **287M allocs / 9.8 GiB** on the combined path | Low at `n=6400`: both sides fit in RAM; CHOLMOD is already optimal. Distributed factor is a different feature | Apply is 6 tasks (`_direct_solve` + wraps). Combined log `compute` 26 s is a thread-sum; `map`×25 + `_assemble_and_factor_pinned` | Gather builds a process-local CSC (lesson 31). Alloc spike is the gather, not potrf | Apply is fine once factored. Setup cannot beat host CHOLMOD at this size | Split `_gather_sparse` vs `cholesky(::CSC)` timers; do not add `Dagger.spchol` |
| 10 | P2 | Dense tiled LU / QR / Chol vs LAPACK | — | Dense `A\b` | LU profile 167 vs 54 ms (0.32×; published 0.37×). 752 tasks (`swaprows_trail!` 288, `gemm!` 140, …). Published QR 0.20×, Chol 0.27× | Low at `n=2048`: tiled getrf with BLAS=1 vs 16-thread LAPACK | Same idle-runner profile as Krylov, but there *is* panel compute | Latency of many small panels | Do not chase this before P0. Bigger `n` only | One LU profile at `n=4096` tile=512 |
| 11 | P2 | `Projected` `mul!` | — | Nullspace / elasticity (not every solve) | 16.0 ms; 104 tasks (64 `matvecmul!` + 16 `dot` + 16 `axpy!` + 8 `map`) | Follows P0 (three SpMV-class applies + dots) | Published 28 ms vs 19 µs | `P A P` is three distributed applies | Moves with ranks 1–2 | None until P0 |
| 12 | P2 | Dense SVD / BlockOperator / specialty | — | Rare vs SciML `A\b` | SVD published 406 vs 13.7 ms (tiled Jacobi vs `gesdd`). BlockOperator published 44.6 ms vs 242 µs. Not re-profiled | Low / correctness-adjacent | Table only | Algorithm mismatch (SVD) or P0 × nest (BlockOperator) | Do not optimize SVD against LAPACK | — |

Block-PC apply is **not** a separate cliff: Jacobi 1.80 ms and BlockJacobi 1.92 ms (setup 563 ms) are one 4-task region, i.e. rank 1. BlockJacobi looks better in the published table (846 ms) because iters drop 196→43, not because apply is fast.

#### Suggested order (no implementation in this pass)

1. Fuse Krylov BLAS-1 (or a whole iteration) so 4-tile `axpy!` is not a 1 ms region.
2. Skip empty SpMV tile pairs (or spawn one task per row tile).
3. Re-profile CG / GMRES / LinearSolve; only then look at assembly allocs or GEMM.
4. Leave gather-then-CHOLMOD, tiled SVD, and `n=2048` dense LU as “Dagger will lose at this size.”

---

## Integration-branch git facts (coordinator)

- Workspace: `/home/jpsamaroo/.julia/dev/Dagger-linalg-ultra`
- Branch: `Dagger-linalg-ultra` tracks `origin/Dagger-linalg-ultra`
- Remotes: `origin` → `ssh://git@github.com/JuliaParallel/Dagger.jl.git`;
  `victor` → `victorcamaraa/Dagger.jl-with-Multistreams.git` (unrelated)
- Do not discard dirty work in this tree or in any listed `git worktree`.
- No Dagger tests or benchmarks were run on this host during the merge pass.
