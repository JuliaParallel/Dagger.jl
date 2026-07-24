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
`dqgmres`, `bicgstab`, `cgs`, `bilq`, `qmr`, and the rectangular least-squares
family (`lsqr`, `lsmr`, `lslq`, `cgls`, `crls`). Preconditioners are passed the
usual way, as `M`:

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
| [`Dagger.bicgstab`](@ref)  | general nonsymmetric        | short recurrence, low memory       |

[`Dagger.krylov_solve`](@ref) is a generic entry point taking the method as a
symbol (`:cg`, `:minres`, `:gmres`, `:bicgstab`):

```julia
x, stats = Dagger.krylov_solve(:gmres, DA, b; memory=50)
```

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
| [`Dagger.BlockILUPreconditioner`](@ref)    | `IncompleteLU`    | incomplete-LU (drop tol `τ`) per tile      |
| [`Dagger.AMGPreconditioner`](@ref)         | `AlgebraicMultigrid` | AMG V-cycle per tile                    |

```julia
using AlgebraicMultigrid, IncompleteLU

x, _ = Krylov.cg(DA, b; M = Dagger.BlockJacobiPreconditioner(DA))
x, _ = Krylov.cg(DA, b; M = Dagger.BlockILUPreconditioner(DA; τ = 0.01))
x, _ = Krylov.gmres(DA, b; M = Dagger.AMGPreconditioner(DA; method = :ruge_stuben))
```

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

A useful consequence of the per-tile design: with a **single tile**
(`Blocks(n, n)`), any of these becomes a *global* preconditioner over the whole
matrix (e.g. a global AMG, or an exact direct solve for block-Jacobi). With many
tiles, it becomes a scalable block-Jacobi / additive-Schwarz variant that trades
some convergence for parallelism. If `A` does not have square tiles, it is
re-tiled to square ones of size `min(mb, nb)` at construction, so the block
structure follows the *finer* of the two block sizes.

### Choosing a preconditioner

- **SPD elliptic (Poisson-like) problems:** `AMGPreconditioner` gives near
  mesh-independent convergence and is usually the best choice; `cg` as the
  solver.
- **General sparse systems:** `BlockILUPreconditioner` is a solid, cheap-setup
  general-purpose option; pair with `gmres` or `bicgstab`.
- **Quick baseline / very well-conditioned systems:** `JacobiPreconditioner` (or
  none) may suffice.
- **Strong per-subdomain coupling:** `BlockJacobiPreconditioner` (exact tile
  solves) is stronger than diagonal Jacobi.

## Sparse direct solvers

For systems that fit on a single worker, Dagger also offers **direct** sparse
solves via pure-Julia factorization backends. Unlike the C-bound `UmfpackLU`,
these factorizations are plain Julia data, so Dagger can move and schedule them
freely.

Load `PureKLU` (KLU; good for unsymmetric/circuit systems) or `PureUMFPACK`
(UMFPACK-style multifrontal LU):

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
once, solve many right-hand sides cheaply.

There are also **block direct preconditioners** that factor each diagonal tile
exactly (`Dagger.BlockKLUPreconditioner`, `Dagger.BlockUMFPACKPreconditioner`),
usable like the other block preconditioners. With a single tile they are exact
whole-matrix solves; with many tiles they are exact-block-Jacobi preconditioners
for the iterative solvers.

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

## API

```@docs
Dagger.cg
Dagger.minres
Dagger.gmres
Dagger.bicgstab
Dagger.krylov_solve
Dagger.AbstractDaggerPreconditioner
Dagger.JacobiPreconditioner
Dagger.AbstractBlockPreconditioner
Dagger.BlockPreconditioner
Dagger.BlockJacobiPreconditioner
Dagger.BlockILUPreconditioner
Dagger.AMGPreconditioner
Dagger.BlockKLUPreconditioner
Dagger.BlockUMFPACKPreconditioner
Dagger.klu
Dagger.splu
Dagger.DaggerSparseLU
Dagger.DistributedSparseLU
Dagger.DistributedSchurLU
```
