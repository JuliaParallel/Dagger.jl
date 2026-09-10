# Iterative Solvers

Dagger provides distributed, **matrix-free** Krylov solvers for linear systems
`A x = b`, built on top of [Krylov.jl](https://github.com/JuliaSmoothOptimizers/Krylov.jl).
They run entirely over `DArray`s: the operator `A` may be a dense or
[sparse](@ref "Sparse Distributed Arrays") `DMatrix`, or any object that
implements distributed `mul!(y, A, x)` over `DVector`s. This makes them a natural
fit for large, sparse systems arising from ODE/PDE discretizations.

The integration is a package extension, so loading `Krylov` is all that is
needed to enable it — after which **Krylov.jl's own entry points work on Dagger
arrays directly**:

```julia
using Distributed
addprocs(4)
using Dagger, SparseArrays, Krylov
```

(Dagger's `Dagger.cg` and friends are thin aliases over the same thing; see
[Dagger's own entry points](#Dagger's-own-entry-points). Calling one without
`Krylov` loaded gives a clear error telling you to `using Krylov`.)

## Solving a system

Call Krylov.jl exactly as you would for `Array`/`SparseMatrixCSC`, and pass
Dagger arrays in:

```julia
using SparseArrays, Krylov

# A symmetric positive-definite operator (1-D Laplacian) and a right-hand side
n = 1000
A = spdiagm(-1 => fill(-1.0, n-1), 0 => fill(2.0, n), 1 => fill(-1.0, n-1))
DA = distribute(A, Blocks(250, 250))   # square tiles are fastest (see below)
b  = distribute(rand(n), Blocks(250))

x, stats = Krylov.cg(DA, b)
@show stats.solved, stats.niter
```

That is the whole integration: an application already written against Krylov.jl
runs on Dagger just by handing it Dagger arrays. No Dagger-specific call site is
needed, and in particular no
`A isa DArray ? Dagger.cg(...) : Krylov.cg(...)` branch.

Each solver returns a tuple `(x, stats)`, where `x` is a `DVector` and `stats`
is Krylov's statistics object (`stats.solved`, `stats.niter`, residual history,
etc.). Keyword arguments such as `atol`, `rtol`, and `itmax` behave as they
always do.

Every Krylov entry-point shape works:

```julia
x, stats = Krylov.cg(DA, b)                      # out-of-place
x, stats = Krylov.krylov_solve(Val(:cg), DA, b)  # generic, by method symbol

workspace = Krylov.krylov_workspace(Val(:cg), DA, b)  # reuse across solves
Krylov.cg!(workspace, DA, b)
x = Krylov.solution(workspace)
```

as does every method Krylov exposes — `cg`, `cr`, `car`, `minres`, `minares`,
`minres_qlp`, `symmlq`, `cg_lanczos`, `gmres`, `fgmres`, `fom`, `diom`,
`dqgmres`, `bicgstab`, `cgs`, `bilq`, `qmr`, the rectangular least-squares
family (`lsqr`, `lsmr`, `lslq`, `cgls`, `crls`), and the **block** methods
`block_gmres` / `block_minres` for a `DMatrix` right-hand side. Preconditioners
are passed the usual way, as `M`:

```julia
x, stats = Krylov.cg(DA, b; M = Dagger.BlockJacobiPreconditioner(DA))
```

### How it works

Krylov's methods are already generic over the vector type — they need only
`mul!` plus the BLAS-1 kernels, all of which `DVector` implements distributed.
The single piece Dagger has to supply is workspace allocation: Krylov normally
allocates workspace vectors as `S(undef, n)` for `S = ktypeof(b)`, and a
`DVector`'s *type* records neither its block size nor its chunk layout. Dagger
therefore routes workspace construction through Krylov's
[`KrylovConstructor`](https://jso.dev/Krylov.jl/stable/workspaces/), which
allocates each vector with `similar(b)` instead — so every internal vector
inherits `b`'s element type, block size, and chunk placement.

!!! note "Rectangular matrix-free operators"
    The least-squares methods also need a workspace vector of length
    `size(A, 2)`, partitioned to match `A`'s columns. Dagger reads that
    partitioning off a `DMatrix` (or its adjoint/transpose); a matrix-free
    rectangular operator cannot expose it, so build the workspace yourself with
    an explicit prototype and call `Krylov.krylov_solve!`.

## Dagger's own entry points

Dagger also exports thin wrappers, which predate the direct support above and
are kept for callers that already use them. They are aliases, not a separate
implementation, so there is no reason to prefer them in new code.

| Function            | Operator class                | Notes                              |
|---------------------|-------------------------------|------------------------------------|
| [`Dagger.cg`](@ref)        | symmetric positive-definite | cheapest; the PDE workhorse        |
| [`Dagger.minres`](@ref)    | symmetric (indefinite ok)   | saddle-point / indefinite systems  |
| [`Dagger.gmres`](@ref)     | general nonsymmetric        | robust; `restart`/`memory` to bound memory |
| [`Dagger.bicgstab`](@ref)     | general nonsymmetric        | short recurrence, low memory       |
| [`Dagger.block_gmres`](@ref)  | general, many RHS (`DMatrix`) | one `mul!(W, A, P)` per iteration |
| [`Dagger.block_minres`](@ref) | Hermitian, many RHS         | same, for SPD / symmetric `A`      |

[`Dagger.krylov_solve`](@ref) is a generic entry point taking the method as a
symbol (`:cg`, `:minres`, `:gmres`, `:bicgstab`, `:block_gmres`, `:block_minres`):

```julia
x, stats = Dagger.krylov_solve(:gmres, DA, b; memory=50)
X, stats = Dagger.krylov_solve(:block_gmres, DA, DB)
```

## Multiple right-hand sides

`A \\ B` / `ldiv!(X, A, B)` with `B::DMatrix` follows Base: the dense LU/QR/Cholesky
paths already accept a matrix RHS. A sparse-backed `A` does **not** gather
columns and run `p` independent Krylov solves. When only `Krylov` is loaded,
`lu(A) \\ B` / `A \\ B` calls [`block_gmres`](@ref). `LinearSolve.solve` with a
`DMatrix` `b` is the SciML entry: `KrylovJL_GMRES` / `KrylovJL_MINRES` use
Krylov's block workspaces; a small sparse `A` still defaults to the pinned
direct factor, which already accepts `F \\ B`.

```julia
using SparseArrays, Krylov

A  = distribute(sprand(2000, 2000, 0.005) + 10I, Blocks(500, 500))
B  = distribute(rand(2000, 8), Blocks(500, 500))

X, stats = Krylov.block_gmres(A, B)   # or A \\ B when no sparse-direct backend
```

The tall n×p Krylov basis stays a `DMatrix` (same row blocking as `B`). The
p×p Hessenberg that block-GMRES Householder-QRs stays a host `Matrix` — `p` is
the number of right-hand sides, not the operator size. Check `‖A*X - B‖`, not
only `stats.solved` (the same residual caveat as the other Krylov paths).

## Matrix-free operators

The solvers never form `A⁻¹`, and they never require `A` to be a materialized
matrix. They only need `mul!(y, A, x)` (and `mul!(y, A', x)` for two-sided
methods like GMRES/BiCGStab) to work over `DVector`s. So you can pass any custom
operator type that implements distributed `mul!`:

```julia
struct MyStencilOperator
    # ...your distributed state...
end
function LinearAlgebra.mul!(y::Dagger.DVector, A::MyStencilOperator, x::Dagger.DVector)
    # fill y with A*x using Dagger tasks / datadeps
    return y
end

x, stats = Krylov.cg(MyStencilOperator(...), b)
```

Workspace vectors are allocated via `similar(b)`, so every internal vector
inherits `b`'s element type **and** partitioning. The distributed BLAS-1 building
blocks the solvers rely on — `dot`, `norm`, `axpy!`, `axpby!`, `rmul!`,
`copyto!`, `fill!`, broadcasting — are all implemented for `DVector` and align
mismatched partitionings automatically.

On a single process, those kernels skip `spawn_datadeps` when every tile is
already a local `ThreadProc` host chunk: the Datadeps region is ~1 ms while
the SIMD is sub-microsecond, so the scheduler would dominate. MPI, remote
workers, and GPU tiles stay on Datadeps on purpose — that is how Dagger moves
data across ranks and devices. There is no second MPI BLAS-1 path.

!!! note "Square tiles are fastest, but not required"
    When the operator is a `DMatrix`, **square tiles** (`Blocks(k, k)`) are the
    layout everything here is fastest on. The solver's workspace vectors are all
    allocated as `similar(b)` (one partitioning), and each must serve as both the
    length-`n` input and the length-`n` output of `mul!(y, A, x)`. The
    distributed SpMV wants the input to match `A`'s column blocks and the output
    to match `A`'s row blocks, which is only simultaneously true when those block
    sizes are equal. Any uniform square tile size qualifies (a ragged final block
    is fine).

    Nothing errors if your partitioning is something else, though. `mul!`
    repartitions its operands through a temporary buffer, and the
    preconditioners re-tile `A` to square tiles of size `min(mb, nb)` once at
    construction. Both are copies you would rather not pay for — the `mul!` one
    per product — but a `DArray` that arrived with an awkward partitioning from
    some earlier operation still solves correctly, with nothing to special-case
    at the call site.

    Unstructured meshes should not use raw geometric `Blocks` on the mesh
    numbering. `repartition(A, Blocks(k, k); partitioner=Metis)` (after
    `using Metis`) permutes by the adjacency graph so tiles cut fewer edges;
    apply the same `perm` to `b` (see [Sparse Arrays](@ref)). Per-tile
    Jacobi / ILU / AMG still see only the diagonal tiles — a better numbering
    is what makes those tiles a useful restriction, not a global coarse grid.

## Known nullspaces

Neumann Poisson, Stokes, and Maxwell operators have a small, known kernel
(the constant mode, or a handful of rigid-body modes) that must be kept out
of the Krylov residual. Julia's `LinearAlgebra.nullspace` *computes* a
basis; it does not attach one. Wrap the operator in
[`Dagger.Projected`](@ref) and pass that to Krylov — each `mul!` drops
`N(N'x)` for orthonormal columns of `N`, so there is no `project!` call at
the solve site:

```julia
# 1-D Neumann Laplacian: constant nullspace. `ones(n)` is fine; the
# constructor orthonormalizes (‖ones‖ = √n).
n, k = size(DA, 1), DA.partitioning.blocksize[1]
N = distribute(ones(n), Blocks(k))
b .-= sum(b) / n                          # compatible RHS (1ᵀb = 0)

x, stats = Krylov.minres(Dagger.Projected(DA, N), b)
```

`N` is a `DVector` (one column) or a `DMatrix` (columns of the basis),
partitioned like `b`. Left and right nullspaces can be passed separately as
`Projected(A, left, right)` when they differ; one `N` is the symmetric
case. The stored basis (`P.left` / `P.right`) is also what
[`SmoothedAggregationPreconditioner`](@ref) reads when the operator is a
`Projected` — or pass the same columns as `nullspace=` on the constructor
(PETSc `MatSetNearNullSpace`; not a setter).

## Nested / field-split operators

Multiphysics problems are block systems `A = [A₁₁ A₁₂; A₂₁ A₂₂]`.
`[A11 A12; A21 A22]` of `DMatrix`s already *concatenates* the tiles into
one bigger `DMatrix` (see `cat`), so a nested operator is
[`Dagger.BlockOperator`](@ref): `mul!` applies the blocks without
assembling them. Zero blocks are `nothing`; `I` / `λ*I` scales a field.

The matching preconditioner is [`Dagger.BlockDiagonalPC`](@ref) — PETSc
`PCFIELDSPLIT` with the additive / Jacobi composition (each field gets its
own PC). A Schur complement is a follow-up.

```julia
A = Dagger.BlockOperator(A11, A12, A21, A22)
P = Dagger.BlockDiagonalPC((
    Dagger.JacobiPreconditioner(A11),
    Dagger.JacobiPreconditioner(A22),
))
x, stats = Krylov.gmres(A, b; M = P)
```

This is *field*-split, not tile-split: [`Dagger.BlockJacobiPreconditioner`](@ref)
still means one operator per diagonal *tile* of a single `DMatrix`.

## Preconditioners

A preconditioner accelerates convergence by approximating `A⁻¹`. Dagger's
preconditioners follow Krylov's `ldiv=false` convention: the object **represents
the inverse operator `M⁻¹`** and is applied via `mul!(y, P, x)` (computing
`y = M⁻¹ x`). Pass one as the `M` keyword:

```julia
P = Dagger.JacobiPreconditioner(DA)
x, stats = Krylov.cg(DA, b; M = P)
```

The built-in preconditioners, from cheapest to strongest:

| Preconditioner                       | Needs                  | Idea                                       |
|--------------------------------------|------------------------|--------------------------------------------|
| [`Dagger.JacobiPreconditioner`](@ref)      | (core)            | scale by `1 ./ diag(A)`                    |
| [`Dagger.BlockJacobiPreconditioner`](@ref) | (core)            | exact `lu` solve per diagonal tile         |
| [`Dagger.AdditiveSchwarzPreconditioner`](@ref) | (core)        | overlapping ASM (`:restrict` default, or `:basic` for CG) |
| [`Dagger.BlockILUPreconditioner`](@ref)    | `IncompleteLU` or CUDA/ROCm sparse | incomplete-LU per tile (`τ` on host; vendor ILU0 on GPU) |
| [`Dagger.AMGPreconditioner`](@ref)         | `AlgebraicMultigrid` | AMG V-cycle **per diagonal tile** (additive Schwarz) |
| [`Dagger.GlobalAMG`](@ref)                 | `AlgebraicMultigrid` | true coarse grid over the whole `DMatrix` |
| [`Dagger.GeometricMultigrid`](@ref)        | `SparseArrays`    | geometric (PFMG-like) V-cycle on a regular grid |

```julia
using AlgebraicMultigrid, IncompleteLU

x, _ = Krylov.cg(DA, b; M = Dagger.BlockJacobiPreconditioner(DA))
x, _ = Krylov.gmres(DA, b; M = Dagger.AdditiveSchwarzPreconditioner(DA; overlap = 1))
x, _ = Krylov.cg(DA, b; M = Dagger.AdditiveSchwarzPreconditioner(DA; type = :basic))
x, _ = Krylov.cg(DA, b; M = Dagger.BlockILUPreconditioner(DA; τ = 0.01))
x, _ = Krylov.gmres(DA, b; M = Dagger.AMGPreconditioner(DA; method = :ruge_stuben))
x, _ = Krylov.gmres(DA, b; M = Dagger.SmoothedAggregationPreconditioner(DA))
x, _ = Krylov.gmres(DA, b; M = Dagger.GeometricMultigrid(DA))  # 1-D; pass grid=(nx,ny) for 2-D
```

`AdditiveSchwarzPreconditioner` is overlapping additive Schwarz (PETSc
`PCASM`). Each tile solves a halo-expanded diagonal block. The `type`
keyword chooses the interpolation:

- `:restrict` (default, `PC_ASM_RESTRICT`): write back only the owned
  interior. Typically the stronger GMRES / BiCGStab option. It is
  **nonsymmetric**, so `cg` can take *more* iterations with overlap than
  without (seen 15 → 171 on a 64×64 Laplacian). That pairing is a documented
  leftover, not a supported SPD path.
- `:basic` (`PC_ASM_BASIC`): scatter-add the whole subdomain solve onto the
  overlapping indices. Restriction and interpolation are adjoints, so `P`
  is SPD when `A` is — this is the option to pass as `M` to `cg` / `minres`.

`overlap = 0` is `BlockJacobiPreconditioner` for either type; the default
`overlap = 1` is PETSc's default. Overlap rows gather neighbor tiles onto
the tile's worker.

!!! warning "`stats.solved` is not `Ax ≈ b` for per-tile AMG"
    `AMGPreconditioner` is block-diagonal. Left-preconditioned Krylov can
    report `stats.solved` while `‖Ax−b‖` is still O(1)–O(100). Always check
    the un-preconditioned residual. [`GlobalAMG`](@ref) is the preconditioner
    that coarsens across tiles; use that when you need a real coarse grid.

### Bringing your own preconditioner

Krylov.jl itself ships no preconditioners — it only defines the *interface*
(`M`/`N`, applied with `mul!` under `ldiv=false` or `ldiv!` under `ldiv=true`),
which is exactly what Dagger's objects implement. So nothing above duplicates
Krylov.

Packages that *do* provide preconditioners — including
[KrylovPreconditioners.jl](https://github.com/JuliaSmoothOptimizers/KrylovPreconditioners.jl)
— build them from one concrete, node-local sparse matrix (its `ilu`, `kp_ilu0`,
`kp_ic0`, and `kp_block_jacobi` all analyze a `SparseMatrixCSC` or a device
sparse matrix and allocate alongside it). None is generic over a distributed
matrix, so none can be handed a `DMatrix` without first gathering the whole
operator onto one worker, which defeats the point.

What is worth reusing is the *numerics*, per tile — and that is what
[`Dagger.BlockPreconditioner`](@ref) is for. You supply a factory, Dagger
supplies only the distributed structure (splitting `A` into square diagonal
blocks, pinning each operator to its tile's worker, and moving vector chunks to
it):

```julia
using KrylovPreconditioners

# Reuse KrylovPreconditioners' ILU per diagonal tile.
P = Dagger.BlockPreconditioner(DA, tile -> KrylovPreconditioners.ilu(Dagger._tile_matrix(tile)))
x, stats = Krylov.gmres(DA, b; M = P)
```

The factory receives the raw tile; `Dagger._tile_matrix` unwraps it to the
backing `SparseMatrixCSC`. The returned operator is applied with `\` if it is a
`Factorization` and `ldiv!` otherwise, which covers essentially every
preconditioner package's convention, so **no Dagger-side code is needed per
package**. `BlockJacobiPreconditioner`, `BlockILUPreconditioner`, and
`AMGPreconditioner` are this same mechanism with a fixed factory.

For a preconditioner that is not block-diagonal at all, implement
`mul!(y::DVector, P::YourType, x::DVector)` and pass it as `M` — that is the
entire contract.

### How block preconditioners are distributed

`BlockJacobiPreconditioner`, `BlockILUPreconditioner`, `AMGPreconditioner`, and
`BlockPreconditioner` are all **block-diagonal** preconditioners: they build one
operator per diagonal tile of `A` and apply them independently per block. They
share a common mechanism ([`Dagger.AbstractBlockPreconditioner`](@ref)):

- The per-tile operator (an `lu`/ILU factorization, or an AMG hierarchy) is built
  **once**, at construction.
- A factorization/hierarchy generally cannot be moved between workers (sparse
  `lu` factors and AMG hierarchies hold process-bound resources). So each
  operator is **pinned** to the worker owning its tile, and every apply for that
  block is scheduled there — only the (small, movable) vector chunks are
  transferred.
- On a GPU tile the apply is pinned to that device (`ExactScope`), not just
  the worker (`ProcessScope`). A CPU-scoped apply would move GPU vector
  chunks to host and restamp the Krylov workspace as `Array`, killing the
  next GPU SpMV. Device-capable applies (Jacobi scale, dense vendor `lu` /
  getrf, cuSPARSE / rocSPARSE ILU0) stay on-device. Host-only factors
  (UMFPACK, CHOLMOD, AlgebraicMultigrid.jl, IncompleteLU.jl) still gather a
  temporary RHS *inside* the GPU-scoped task; the DArray chunk itself stays
  on the device. `BlockILUPreconditioner` does not require `IncompleteLU`
  when CUDA or ROCm sparse is loaded — those backends build vendor ILU(0)
  (`τ` is then ignored).

A useful consequence of the per-tile design: with a **single tile**
(`Blocks(n, n)`), any of these becomes a *global* preconditioner over the whole
matrix (e.g. a global AMG, or an exact direct solve for block-Jacobi). With many
tiles, it becomes a scalable block-Jacobi / additive-Schwarz variant that trades
some convergence for parallelism. If `A` does not have square tiles, it is
re-tiled to square ones of size `min(mb, nb)` at construction, so the block
structure follows the *finer* of the two block sizes.

### Global AMG (across tiles)

[`Dagger.GlobalAMG`](@ref) is a different object. It builds one hierarchy over
the whole operator: each coarse operator is the distributed Galerkin product
`Ac = P' * A * P`, and `mul!(y, M, x)` is a V-cycle (or W/F). Interpolation
`P` comes from HMIS-lite (local SA, then a parallel independent set on
unassigned interface nodes) over the tiled strength graph — leftover
interface nodes join a global C/F or aggregate assignment — then optional
Jacobi smoothing. `coarsen=:pmis` is a full MIS (opt-in; can lose to Jacobi
on 1-D Poisson n=128). Setup does not assemble a global CSC of `A`.
That is what PDE codes mean by AMG; `AMGPreconditioner` with many tiles is
not.

```julia
using AlgebraicMultigrid, Krylov

M = Dagger.SmoothedAggregationPreconditioner(DA)   # or RugeStubenPreconditioner(DA)
# also: M = aspreconditioner(smoothed_aggregation(DA))
# smoother=:l1jacobi|:chebyshev|:hybrid_gs|:ilu|:ras|:fsai
# cycle=:w|:f|:additive|:multadditive
# coarsen=:falgout|:cljp|:cgc|:aggressive  (default :hmis)
# interp=:extended|:exti|:ff|:air|:multipass
x, stats = Krylov.gmres(DA, b; M)
r = similar(b); mul!(r, DA, x); axpy!(-1, b, r)
@assert norm(r) / norm(b) < 1e-8   # do not stop at stats.solved
```

Default coarsening is HMIS-lite (`coarsen=:hmis`); `:pmis` is a full
independent set; `:standard` is the older per-tile + leftover-pair path;
`:falgout` / `:cljp` / `:cgc` / `:aggressive` are HYPRE-named opt-in
coarseners. Falgout / CLJP / aggressive / PMIS lose the 1-D n=128 Jacobi
gate and stay opt-in; they beat Jacobi on 2-D Poisson. CGC beats both.
`interp=:extended` / `:exti` / `:ff` / `:multipass` / `:air` are
distributed classical interpolants (AIR stores its own `R`). `pmax` /
`trunc_factor` / `coarse_drop` change `P` / `Ac` sparsity (zero = off).
`cycle=:additive` / `:multadditive` add a `1/n`-damped coarsest
correction (unscaled `R b` overshoots; a line search would make `mul!`
nonlinear). Prefer `:v` for Krylov. `smoother=:fsai` is a
block-diagonal sparse approximate inverse, not a new solver type.
`ComplexF64` operators work; the default path is still real.
Hierarchy recurses
with distributed RAP until `max_coarse` / `max_levels` (default 10).
Remaining gathers: coarsest LU, one row of tiles per coarsen (stays on the
worker), membership / header fetch, GPU tile host-stage. `nullspace=N`
gathers `N` only. Default setup still does not collect fine `A`.

On GPU tiles, construct `GlobalAMG` under the same device scope as the
operator (`Dagger.scope(rocm_gpu=1)` / `cuda_gpu=1`). Setup still host-stages
each *tile* inside that scope so AlgebraicMultigrid.jl can build `P`; the
V-cycle then applies with device `mul!` / Jacobi / ℓ1-Jacobi / Chebyshev.
Hybrid GS, per-tile [`AMGPreconditioner`](@ref), and the coarsest LU Adapt a
temporary *inside* the GPU-scoped task — the DArray chunk stays in VRAM
(lesson 35 / 52). Do not pin apply to `ProcessScope`. Check `‖Ax−b‖`.

Elasticity and other systems whose low-energy modes are not the scalar
constant need those modes as SA candidates — the PETSc
`MatSetNearNullSpace` / rigid-body set. Attach them on the constructor;
[`AMGPreconditioner`](@ref) stays per-tile and does not take this.
`nullspace=N` gathers `N` (not `A`) so coarse levels get the `R` from
`fit_candidates`. `blocksize` / `nvars` is HYPRE `NumFunctions` (nodal
coarsening of interleaved unknowns):

```julia
# N is n×k (e.g. 2 translations + 1 rotation). Default is ones (scalar SA).
M = Dagger.SmoothedAggregationPreconditioner(DA; nullspace=N)
# or read N off a Projected wrapper:
M = Dagger.SmoothedAggregationPreconditioner(Dagger.Projected(DA, N))
x, stats = Krylov.gmres(DA, b; M)
```

`B=` is the AlgebraicMultigrid.jl name for the same argument. Check
`‖Ax−b‖`, not only `stats.solved`.

### Geometric multigrid (regular grids)

[`Dagger.GeometricMultigrid`](@ref) is the structured-grid counterpart of
[`GlobalAMG`](@ref) — a first-cut HYPRE PFMG analog. Restriction is injection
or full-weighting, prolongation is linear (1-D) or bilinear (2-D), each coarse
operator is the distributed Galerkin product `Ac = R * A * P`, and
`mul!(y, M, x)` is a V-cycle with the same damped-Jacobi smoother as
`GlobalAMG`. `grid` is the lattice shape (`(n,)` by default; `(nx, ny)` for a
column-major 2-D grid). User-supplied `R` / `P` (`AbstractMatrix` or `DMatrix`)
replace the finest transfer.

This is **matrix-based**, not a stencil. `@stencil` (including
`origin/jps/sparse-stencil`) is a same-`idx`, same-size, same-chunk halo
sweep: neighborhood access at any other index is rejected, and operands must
share shape and layout. Geometric restriction / prolongation maps a fine grid
of size `n` onto a coarse grid of size `n/2` — a different index and a
different array size — so it cannot be written as `@stencil`. We did not grow
a competing stencil stack for that gap. Construction is
`_gmg_restriction_coo` / `_gmg_prolongation_coo` (`src/array/gmg.jl`) assembled
as sparse `DMatrix`s (`sparse(I, J, V, …, Blocks)` in `SparseArraysExt`),
then Galerkin `mul!` and a V-cycle that restricts / prolongs with `mul!`
(`_gmg_vcycle!`). The full contract, and what would have to change in
`@stencil` to make inter-grid transfers possible (we are not doing that), is
in [Why restriction / prolongation cannot be a `@stencil`](@ref stencil-no-gmg).

```julia
using SparseArrays, Krylov

M = Dagger.GeometricMultigrid(DA)                    # 1-D full-weighting
M = Dagger.GeometricMultigrid(DA; grid = (nx, ny))   # 2-D bilinear
x, stats = Krylov.gmres(DA, b; M)
r = similar(b); mul!(r, DA, x); axpy!(-1, b, r)
@assert norm(r) / norm(b) < 1e-8   # do not stop at stats.solved
```

`AMGPreconditioner` and `GlobalAMG` are unchanged: per-tile AMG is still
additive Schwarz; aggregation AMG is still `GlobalAMG`.

### AMG vs HYPRE BoomerAMG

This is not PETSc/HYPRE parity. Dagger’s AMG is a first cut on
`AlgebraicMultigrid.jl` plus distributed RAP / V-cycle. Judge quality by the
un-preconditioned residual `‖Ax−b‖`, not by Krylov `stats.solved` (per-tile
AMG in particular can report `solved` while `‖Ax−b‖` is O(1)–O(100)).

| Capability | Dagger | BoomerAMG |
|---|---|---|
| Across-tile coarse grid | [`GlobalAMG`](@ref) / SA / RS: tiled HMIS-lite `P`, Galerkin `P'AP`, V/W/F-cycle | Yes (the product) |
| Per-tile AMG | [`AMGPreconditioner`](@ref): block-diagonal Schwarz, **not** a coarse grid | Not the BoomerAMG model |
| Geometric MG | [`GeometricMultigrid`](@ref): injection / full-weighting `R`, linear / bilinear `P` | PFMG/SMG (separate) |
| Overlapping Schwarz | [`AdditiveSchwarzPreconditioner`](@ref) (`:restrict` / `:basic`); also `smoother=:ras` | Schwarz as a *smoother* option; also PETSc `PCASM` |
| Near-nullspace | SA constructor `nullspace=N` (`fit_candidates`); RS rejects it; `blocksize`/`nvars` | `InterpVectors` + variants; nodal / unknown-based systems |
| Coarsening | HMIS-lite (default); opt-in PMIS / Falgout / CLJP / CGC / aggressive; `:standard` leftover pairing | HMIS / PMIS / Falgout / CLJP / CGC / aggressive |
| Interpolation | Tentative SA + Jacobi smooth; RS `:direct`; opt-in `:extended` / `:exti` / `:ff` / `:multipass` / `:air` (distributed, no collect of `A`) | Classical / extended / ext+i / FF / AIR / multipass / … |
| Smoother | Jacobi (default), ℓ1-Jacobi, Chebyshev, hybrid GS, ILU, RAS, FSAI | Hybrid GS, Schwarz, Chebyshev, ILU, FSAI, ℓ1-Jacobi, … |
| Cycles | V (default), W, F, additive, mult-additive | V / W / F, additive and mult-additive AMG |
| Complex / nodal systems | Nodal via `blocksize`; `ComplexF64` `GlobalAMG` + GMRES | Yes |
| Native GPU AMG | Setup host-stages *tiles* (AlgebraicMultigrid.jl). V-cycle Jacobi / ℓ1-Jacobi / Chebyshev / SpMV keep vectors in VRAM; hybrid GS / per-tile AMG / coarse LU Adapt a temporary inside a GPU-scoped task | PMIS + ext+i (and more) on device |
| Strength / truncation / Pmax | AlgebraicMultigrid.jl `strength=` / `aggregate=` passthrough; `pmax` / `trunc_factor` / `coarse_drop` | First-class HYPRE knobs |
| Non-Galerkin coarse drop | `coarse_drop` on each Galerkin `Ac` | Yes |

Covered in spirit: a global V/W/F/additive/mult-additive cycle, HMIS-lite
coarsening (opt-in PMIS / Falgout / CLJP / CGC / aggressive), SA with optional
rigid-body candidates and `blocksize`, classical / extended / AIR interpolants,
Jacobi / ℓ1-Jacobi / Chebyshev / hybrid GS / ILU / RAS / FSAI level smoothers,
`pmax` / truncation / coarse drop, ComplexF64 AMG, geometric transfers on a
regular 1-D/2-D grid, and RAS as its own preconditioner. Still missing:
a fully device-side AMG hierarchy (setup is still host-staged per tile).
The longer table lives in `LINALG_INTEGRATION.md`.

### Choosing a preconditioner

- **SPD elliptic (Poisson-like) problems:** [`GlobalAMG`](@ref) /
  [`SmoothedAggregationPreconditioner`](@ref) is the algebraic coarse-grid
  choice; [`GeometricMultigrid`](@ref) is the one to use when the operator
  lives on a regular 1-D / 2-D grid. `AMGPreconditioner` is per-tile Schwarz
  and can look solved while `‖Ax−b‖` is huge. Prefer `gmres` if `cg` rejects
  the V-cycle as non-SPD.
- **Elasticity / systems with rigid-body modes:** same global SA, with
  `nullspace=N`. Scalar SA (`ones`) can stall on those modes; per-tile AMG
  is still not a coarse grid.
- **General sparse systems:** `BlockILUPreconditioner` is a solid, cheap-setup
  general-purpose option; pair with `gmres` or `bicgstab`.
- **Quick baseline / very well-conditioned systems:** `JacobiPreconditioner` (or
  none) may suffice.
- **Strong per-subdomain coupling:** `BlockJacobiPreconditioner` (exact tile
  solves) is stronger than diagonal Jacobi. For unstructured problems that
  need neighbor coupling, `AdditiveSchwarzPreconditioner` (overlap 1–2) is
  the PETSc-default next step. Use `type = :restrict` (default) with
  `gmres`; use `type = :basic` with `cg`.

## Eigenvalues (few pairs)

`LinearAlgebra.eigen` / `eigvals` on a `DMatrix` compute a **few** extreme
eigenpairs with LOBPCG, not a dense geev of the whole operator. The default is
the smallest algebraic pair (`nev=1`, `which=:SR`) — the 1-D Laplacian ground
state. A sparse-backed `DMatrix` is applied only through distributed `mul!`
and is never densified.

```julia
using SparseArrays, LinearAlgebra

n = 1000
A = spdiagm(-1 => fill(-1.0, n-1), 0 => fill(2.0, n), 1 => fill(-1.0, n-1))
DA = distribute(A, Blocks(250, 250))

F = eigen(DA)                    # one smallest pair
λ, x = F.values[1], collect(F.vectors)[:, 1]
# true residual — do not treat a Ritz residual as ‖Ax-λx‖
r = similar(distribute(x, Blocks(250)))
# or, after you have a DVector `dx`:
# mul!(r, DA, dx); axpy!(-λ, dx, r)

F3 = eigen(DA; nev=3, which=:SR) # three smallest
λmax = eigvals(DA; which=:LR)    # largest algebraic
```

`which` is `:SR` / `:SA` (smallest algebraic), `:LR` / `:LA` (largest), or
`:SM` / `:LM` (by magnitude). `P` (alias `M`) is an optional preconditioner
applied as `mul!(y, P, r)` (`y ← M⁻¹ r`), the same convention as Krylov
`ldiv=false` — `JacobiPreconditioner` is a cheap first try.

The same path works for matrix-free operators that already have `mul!` over
`DVector`s: [`Projected`](@ref) infers a trial-vector layout from its
nullspace basis; [`BlockOperator`](@ref) needs `v0::DVector`. This is not
ScaLAPACK dense geev and does not return the full spectrum.

`schur(::DMatrix)` is the dense-tile reduction: it `collect`s and runs
LAPACK `gees`. That is **not** a replacement for `eigen` and it **throws**
on a sparse-backed `DMatrix` (it would densify). There is no tiled
ScaLAPACK Schur.

## Sparse direct solvers

A sparse-backed `DMatrix` used as `A \ b` (or `ldiv!` / `lu` / `factorize`)
**never** takes the tiled dense LU path — that would silently densify the
operator and can OOM. Those LinearAlgebra entry points dispatch to a sparse
direct factor when `PureUMFPACK` or `PureKLU` is loaded, or to GMRES when only
`Krylov` is loaded:

```julia
using SparseArrays, PureUMFPACK   # or PureKLU, or just Krylov

A  = distribute(sprand(2000, 2000, 0.005) + 10I, Blocks(500, 500))
b  = distribute(rand(2000), Blocks(500))

x = A \ b             # DVector, partitioned like b
F = lu(A)             # DaggerSparseLU (direct) or SparseIterativeFactorization
x = F \ b
```

For systems that fit on a single worker, the explicit direct backends are
pure-Julia factorizations (unlike the C-bound `UmfpackLU`), so Dagger can move
and schedule them freely. Load `PureKLU` (KLU; good for unsymmetric/circuit
systems) or `PureUMFPACK` (UMFPACK-style multifrontal LU):

```julia
using SparseArrays, PureKLU, PureUMFPACK

A  = distribute(sprand(2000, 2000, 0.005) + 10I, Blocks(500, 500))
b  = distribute(rand(2000), Blocks(500))

F = Dagger.klu(A)     # or Dagger.splu(A)
x = F \ b             # returns a DVector partitioned like b
```

`Dagger.klu`/`Dagger.splu` gather the sparse `DMatrix` into one
`SparseMatrixCSC` (without densifying), factor it once, and return a
[`Dagger.DaggerSparseLU`](@ref) supporting `F \ b` and `ldiv!(x, F, b)`. Factor
once, solve many right-hand sides cheaply. `lu(A)` / `factorize(A)` prefer
`splu` when both packages are loaded.

Overdetermined / rank-revealing sparse systems use the same gather, via
`qr(A)` / `qr!(A)` (SuiteSparse SPQR; [`Dagger.DaggerSparseQR`](@ref)). That
path is **not** tiled dense Compact-WY QR — the same densify footgun as
sparse `lu` / `cholesky`. Solve with `F \\ b` (least squares; `x` has
length `size(A, 2)`). Least-squares Krylov (`lsqr` / `lsmr`) remains the
matrix-free alternative when the operator should stay distributed.

When the **values** change but the sparsity pattern does not (implicit time
stepping), refresh with `lu!(F, A)` — the LinearAlgebra factor-update API,
not a Dagger-specific `refactor`. `Dagger.klu` then `lu!(F, A)` reuses KLU's
symbolic analysis (`klu!`). PureUMFPACK has no `splu!`, so `lu!(F, A)` on an
`splu` / default-`lu` factor still works but rebuilds the numerics. Prefer
`klu` when you know the pattern is fixed. The same idea applies to sparse
Cholesky: `cholesky!(F, A)` reuses CHOLMOD's symbolic factor.

There are also **block direct preconditioners** that factor each diagonal tile
exactly (`Dagger.BlockKLUPreconditioner`, `Dagger.BlockUMFPACKPreconditioner`),
usable like the other block preconditioners. With a single tile they are exact
whole-matrix solves; with many tiles they are exact-block-Jacobi preconditioners
for the iterative solvers.

## Mixed precision

`mul!` and `*` on `DArray`s accept mixed element types the same way
LinearAlgebra does on host arrays. The usual PETSc/HYPRE pattern is an FP32
operator or preconditioner with FP64 Krylov vectors:

```julia
A32 = distribute(Float32.(A), Blocks(k, k))
A64 = distribute(Float64.(A), Blocks(k, k))
b64 = distribute(Float64.(b), Blocks(k))

y = A32 * b64   # DVector{Float64}; no convert-everything-first
P = Dagger.JacobiPreconditioner(A32)
x, stats = Krylov.gmres(A64, b64; M = P)
```

Same-type GEMM/GEMV still use BLAS on each tile. Mixed tiles call
LinearAlgebra's mixed `generic_matmatmul!` / `generic_matvecmul!`. A
preconditioner built from an FP32 `DMatrix` applies tile-locally in that
precision and writes back the Krylov vector's eltype.

Check the un-preconditioned residual (see the AMG warning above), not only
`stats.solved`.

## A worked example: implicit time stepping

Implicit ODE/PDE integrators repeatedly solve systems with the same operator
`(I - Δt·L)` and changing right-hand sides. Build the preconditioner once and
reuse it across steps:

```julia
using SparseArrays, Krylov, AlgebraicMultigrid

L  = distribute(laplacian, Blocks(k, k))     # discretized operator (square tiles)
A  = I - Δt * L                              # or a custom matrix-free operator
P  = Dagger.AMGPreconditioner(L)             # build hierarchy once

u = distribute(u0, Blocks(k))
for step in 1:nsteps
    rhs = ...                                # depends on current state
    u, stats = Krylov.cg(A, rhs; M = P, rtol = 1e-8)
end
```

If the implicit operator is small enough to gather and only its **values**
change each step, reuse a direct factor instead:

```julia
using SparseArrays, PureKLU

A = distribute(I - Δt * L_sparse, Blocks(k, k))   # same pattern every step
F = Dagger.klu(A)                                 # symbolic analysis once
for step in 1:nsteps
    # update the stored values of A (same I, J)
    lu!(F, A)                                     # KLU numeric refactor
    u = F \ rhs
end
```

## API

```@docs
Dagger.cg
Dagger.minres
Dagger.gmres
Dagger.bicgstab
Dagger.block_gmres
Dagger.block_minres
Dagger.krylov_solve
Dagger.Projected
Dagger.project!
Dagger.BlockOperator
Dagger.BlockDiagonalPC
Dagger.AbstractDaggerPreconditioner
Dagger.JacobiPreconditioner
Dagger.AbstractBlockPreconditioner
Dagger.BlockPreconditioner
Dagger.BlockJacobiPreconditioner
Dagger.AdditiveSchwarzPreconditioner
Dagger.BlockILUPreconditioner
Dagger.DeviceILU0
Dagger.AMGPreconditioner
Dagger.GlobalAMG
Dagger.GeometricMultigrid
Dagger.SmoothedAggregationPreconditioner
Dagger.RugeStubenPreconditioner
Dagger.BlockKLUPreconditioner
Dagger.BlockUMFPACKPreconditioner
Dagger.klu
Dagger.splu
Dagger.DaggerSparseQR
Dagger.DaggerSparseLU
Dagger.DaggerSparseCholesky
Dagger.SparseIterativeFactorization
Dagger.DistributedSparseLU
Dagger.DistributedSchurLU
```
