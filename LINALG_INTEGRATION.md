# Dagger linear-algebra integration

Living status document for the multi-agent linalg program. **This file is the
source of truth.** Sibling agents implement on their own worktrees and feature
branches; they do **not** merge into this workspace branch, and they do **not**
edit other agents' rows here. Report status in your final message so the
coordinator can update the table.

Last coordinator pass: 2026-09-07 (seven workstreams merged onto `Dagger-linalg-ultra`).

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

| Workstream | Status | Branch | Owner notes | Last update |
|---|---|---|---|---|
| sparse-solve-dispatch | done | `linalg/sparse-solve-dispatch` @ `57c7414b` | Sparse `A\b` / `lu` / `factorize` route off dense LU via `is_sparse_backed`. AWS: array/linalg pass. Numeric refactor skipped — PureUMFPACK has no `splu!`. `inv` on sparse uses the sparse factor (`factorize` + `ldiv!` into `I`). | 2026-09-07 |
| linearsolve | done | `linalg/linearsolve` @ `55c38361` | `LinearSolve.solve` / `defaultalg` on `DArray` (direct `KrylovJL_GMRES` / `PureKLU` / `PureUMFPACK`, never `DefaultLinearSolver`). AWS: linearsolve 25/25, full array/linalg pass. AlgebraicMultigrid compat `"1, 2"`. | 2026-09-07 |
| assembly | done | `linalg/assembly` @ `6018c6f4` | `sparse` / `sparse!` + `Blocks` tile-routed COO; regular `@spawn`, not datadeps (`SparseCOOBucket` has no `move!`). AWS: assembly 64/64, array/linalg pass. | 2026-09-07 |
| overlapping-asm | done | `linalg/overlapping-asm` @ `5a2102f7` | `AdditiveSchwarzPreconditioner` (`PC_ASM_RESTRICT`). RAS is not SPD — use GMRES. Overlap benefit is problem-dependent (no `niter` drop on well-conditioned 1-D Laplacian with large tiles). AWS: iterativesolvers 346, array/linalg pass. | 2026-09-07 |
| sparse-chol-ic | done | `linalg/sparse-chol-ic` @ `5c1f9991` | Sparse `cholesky` via `_cholesky` / `cholesky!` (never `_chol!`); per-tile `BlockICPreconditioner` / `ichol`. AWS: sparse chol 55, array/linalg pass. | 2026-09-07 |
| global-amg | done | `linalg/global-amg` @ `c7779f81` | `GlobalAMG` / `SmoothedAggregationPreconditioner` / `RugeStubenPreconditioner`. First cut still gathers to build `P`; RAP and the V-cycle do not. Check `‖Ax−b‖`, not only `stats.solved`. AWS: global_amg 113, iterativesolvers 297. | 2026-09-07 |
| operator-types | done | `linalg/operator-types` @ `9158cb47` | `Projected` (nullspace; constructor orthonormalizes) + `BlockOperator` / `BlockDiagonalPC`. AWS: iterativesolvers 346, array/linalg pass. | 2026-09-07 |

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

## Remaining follow-ups

- **Numeric refactor of sparse LU** — skipped. Dispatch stays sparse; PureUMFPACK has no `splu!` in-place hook, so a later numeric rewrite of the actual factorization is still open.
- **GlobalAMG first cut still gathers to build `P`.** RAP and the V-cycle are distributed; setup of the prolongation is not. More coarse levels / smoothing variants are also open.
- **RAS overlap benefit is problem-dependent.** Overlap 1 does not reduce GMRES `niter` on a well-conditioned 1-D Laplacian with large tiles; the drop needs smaller subdomains or stronger convection. RAS is nonsymmetric — use GMRES, not CG.
- **`inv` on a sparse-backed `DMatrix`** uses the sparse factor (`factorize` + `ldiv!` into `I`) rather than a dedicated sparse inverse. The result is still a dense `I` solve.

`AGENTS.md` lessons 27–34 are the union of the per-workstream lesson 27s (LinearSolve `DefaultLinearSolver`; ASM Restricted / GMRES; GlobalAMG vs per-tile residual; qualify `cholesky!`/`mul!`; `SparseCOOBucket` not in datadeps; Projected orthonormalize / `Adjoint` `mul!`; `hvcat` is not `MatNest`). Lesson 20 remains unused (pre-existing gap).

---

## Integration-branch git facts (coordinator)

- Workspace: `/home/jpsamaroo/.julia/dev/Dagger-linalg-ultra`
- Branch: `Dagger-linalg-ultra` tracks `origin/Dagger-linalg-ultra`
- Remotes: `origin` → `ssh://git@github.com/JuliaParallel/Dagger.jl.git`;
  `victor` → `victorcamaraa/Dagger.jl-with-Multistreams.git` (unrelated)
- Do not discard dirty work in this tree or in any listed `git worktree`.
- No Dagger tests or benchmarks were run on this host during the merge pass.
