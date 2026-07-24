# Iterative Solvers

Dagger provides distributed, **matrix-free** Krylov solvers for linear systems
`A x = b`, built on top of [Krylov.jl](https://github.com/JuliaSmoothOptimizers/Krylov.jl).
They run entirely over `DArray`s: the operator `A` may be a dense or
[sparse](@ref "Sparse Distributed Arrays") `DMatrix`, or any object that
implements distributed `mul!(y, A, x)` over `DVector`s. This makes them a natural
fit for large, sparse systems arising from ODE/PDE discretizations.

Solvers are provided through a package extension; load `Krylov` to enable them:

```julia
using Distributed
addprocs(4)
using Dagger, SparseArrays, Krylov
```

If you call a solver without `Krylov` loaded, you'll get a clear error telling
you to `using Krylov`.

## Solving a system

```julia
using SparseArrays, Krylov

# A symmetric positive-definite operator (1-D Laplacian) and a right-hand side
n = 1000
A = spdiagm(-1 => fill(-1.0, n-1), 0 => fill(2.0, n), 1 => fill(-1.0, n-1))
DA = distribute(A, Blocks(250, 250))   # square tiles (see below)
b  = distribute(rand(n), Blocks(250))

x, stats = Dagger.cg(DA, b)
@show stats.solved, stats.niter
```

Each solver returns a tuple `(x, stats)`, where `x` is a `DVector` and `stats`
is Krylov's statistics object (`stats.solved`, `stats.niter`, residual history,
etc.). Keyword arguments such as `atol`, `rtol`, and `itmax` are forwarded
to Krylov.

### Available methods

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

x, stats = Dagger.cg(MyStencilOperator(...), b)
```

Workspace vectors are allocated via `similar(b)`, so every internal vector
inherits `b`'s element type **and** partitioning. The distributed BLAS-1 building
blocks the solvers rely on — `dot`, `norm`, `axpy!`, `axpby!`, `rmul!`,
`copyto!`, `fill!`, broadcasting — are all implemented for `DVector` and align
mismatched partitionings automatically.

!!! note "Square tiles for `DMatrix` operators"
    When the operator is a `DMatrix`, use **square tiles** (`Blocks(k, k)`). The
    solver's workspace vectors are all allocated as `similar(b)` (one
    partitioning), and each must serve as both the length-`n` input and the
    length-`n` output of `mul!(y, A, x)`. The distributed SpMV requires the input
    to match `A`'s column blocks and the output to match `A`'s row blocks, which
    is only simultaneously possible when those block sizes are equal. Any uniform
    square tile size works (a ragged final block is fine). This constraint is on
    how *you* partition the operator — generic code calling `mul!`/`dot` never
    needs to know about it.

## Preconditioners

A preconditioner accelerates convergence by approximating `A⁻¹`. Dagger's
preconditioners follow Krylov's `ldiv=false` convention: the object **represents
the inverse operator `M⁻¹`** and is applied via `mul!(y, P, x)` (computing
`y = M⁻¹ x`). Pass one as the `M` keyword:

```julia
P = Dagger.JacobiPreconditioner(DA)
x, stats = Dagger.cg(DA, b; M = P)
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

x, _ = Dagger.cg(DA, b; M = Dagger.BlockJacobiPreconditioner(DA))
x, _ = Dagger.cg(DA, b; M = Dagger.BlockILUPreconditioner(DA; τ = 0.01))
x, _ = Dagger.gmres(DA, b; M = Dagger.AMGPreconditioner(DA; method = :ruge_stuben))
```

### How block preconditioners are distributed

`BlockJacobiPreconditioner`, `BlockILUPreconditioner`, and `AMGPreconditioner`
are all **block-diagonal** preconditioners: they build one operator per diagonal
tile of `A` and apply them independently per block. They share a common
mechanism ([`Dagger.AbstractBlockPreconditioner`](@ref)):

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
some convergence for parallelism. As with the operator, these require square
diagonal tiles.

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
    u, stats = Dagger.cg(A, rhs; M = P, rtol = 1e-8)
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
Dagger.BlockJacobiPreconditioner
Dagger.BlockILUPreconditioner
Dagger.AMGPreconditioner
Dagger.BlockKLUPreconditioner
Dagger.BlockUMFPACKPreconditioner
Dagger.klu
Dagger.splu
Dagger.DaggerSparseLU
```
