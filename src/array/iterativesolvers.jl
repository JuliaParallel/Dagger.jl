# Distributed iterative (Krylov) linear solvers.
#
# The user-facing entry points (`Dagger.cg`, `Dagger.minres`, `Dagger.gmres`,
# `Dagger.bicgstab`, and the generic `Dagger.krylov_solve`) live here so the
# solver backend can evolve without changing user code. The actual solves are
# implemented in `ext/KrylovExt.jl`, which is loaded when `Krylov` is available
# (`using Krylov`). The matrix-free building blocks they rely on -- distributed
# `mul!`/SpMV and the BLAS-1 vector ops -- live in `mul.jl`/`linalg.jl`.

"""
    cg(A, b::DVector; M=I, atol, rtol, itmax, ...) -> (x::DVector, stats)

Solve the symmetric positive-definite system `A x = b` with the conjugate
gradient method, distributed over `A`'s and `b`'s chunks. `A` may be a
`DMatrix` (dense or sparse-backed) or any object supporting `mul!(y, A, x)` over
`DVector`s (matrix-free). Requires `Krylov.jl` to be loaded.

See also [`minres`](@ref), [`gmres`](@ref), [`bicgstab`](@ref), and
[`krylov_solve`](@ref). Keyword arguments are forwarded to `Krylov.cg!`.
"""
function cg end

"""
    minres(A, b::DVector; M=I, atol, rtol, itmax, ...) -> (x::DVector, stats)

Solve the symmetric (possibly indefinite) system `A x = b` with MINRES.
Requires `Krylov.jl` to be loaded. See [`cg`](@ref).
"""
function minres end

"""
    gmres(A, b::DVector; M=I, N=I, restart=false, memory=20, ...) -> (x::DVector, stats)

Solve the general (nonsymmetric) system `A x = b` with restarted GMRES.
`M`/`N` are left/right preconditioners. Requires `Krylov.jl` to be loaded.
See [`cg`](@ref).
"""
function gmres end

"""
    bicgstab(A, b::DVector; M=I, N=I, ...) -> (x::DVector, stats)

Solve the general (nonsymmetric) system `A x = b` with BiCGStab (short
recurrence, low memory). Requires `Krylov.jl` to be loaded. See [`cg`](@ref).
"""
function bicgstab end

"""
    krylov_solve(method::Symbol, A, b::DVector; kwargs...) -> (x::DVector, stats)

Generic entry point dispatching to the iterative `method` (`:cg`, `:minres`,
`:gmres`, `:bicgstab`). Requires `Krylov.jl` to be loaded.
"""
function krylov_solve end

# Friendly fallbacks: these generic methods are shadowed by the more specific
# `(A, b::DVector)` methods added in `ext/KrylovExt.jl` once Krylov is loaded.
_krylov_required(name) = throw(ArgumentError(
    "Dagger.$name requires Krylov.jl. Run `using Krylov` (or `import Krylov`) \
    to enable distributed iterative solvers."))
cg(A, b; kwargs...) = _krylov_required(:cg)
minres(A, b; kwargs...) = _krylov_required(:minres)
gmres(A, b; kwargs...) = _krylov_required(:gmres)
bicgstab(A, b; kwargs...) = _krylov_required(:bicgstab)
krylov_solve(method::Symbol, A, b; kwargs...) = _krylov_required(:krylov_solve)

# --- Preconditioners ------------------------------------------------------
# A preconditioner object `P` represents the (approximate) *inverse* operator
# `M⁻¹`: applying it (`mul!(y, P, x)`) computes `y = M⁻¹ x`, which is exactly an
# ordinary matrix-vector product by the operator `P` stands for (no inversion
# happens at apply time -- any reciprocals/factorizations are precomputed once).
# This matches Krylov's `ldiv=false` convention, where the object passed as `M`
# is applied via `mul!(y, M, x)` to compute `y ← M⁻¹ x`, so these are passed
# straight through as `M=P`.
#
# These preconditioners are backend-agnostic: they operate on `DVector`s (and a
# `DMatrix`'s diagonal tiles) and need no solver backend, so they live in core.
#
# A note on tile shape: square tiles are the *fast* layout, not a requirement.
# The solvers allocate every workspace vector as `similar(b)` (one
# partitioning), and each must serve as both the length-`n` input and the
# length-`n` output of `mul!(y, A, x)`; `gemv_dagger!` wants the input to match
# `A`'s column blocks and the output to match `A`'s row blocks, which is only
# simultaneously true when those block sizes are equal. Otherwise `mul!`
# repartitions through `maybe_copy_buffered` -- correct, but a copy of the
# operand per product. The preconditioners take the analogous fallback once, at
# construction (see `_square_tiled_layout`). Any uniform square tile size is
# ideal, including a ragged final block.

"""
    AbstractDaggerPreconditioner

Supertype for Dagger's distributed preconditioners. A preconditioner `P`
represents an (approximate) inverse operator `M⁻¹` and applies it via
`mul!(y, P, x)` (`y = M⁻¹ x`) over `DVector`s, so it can be passed to the
solvers as `M=P` (with `ldiv=false`, the default).
"""
abstract type AbstractDaggerPreconditioner end

"""
    JacobiPreconditioner(A::DMatrix)

Diagonal (Jacobi) preconditioner. The object represents the inverse-diagonal
operator `M⁻¹ = inv(Diagonal(A))`; it precomputes and stores the reciprocal
diagonal `dinv = 1 ./ diag(A)`, so applying it (`mul!(y, P, x)`) is a single
elementwise multiply `y = dinv .* x` per chunk -- an ordinary product by the
stored operator, not an inversion. Cheap to build (one diagonal extraction per
diagonal tile) and to apply.

If `A` does not have square tiles, it is re-tiled to square ones at
construction (one extra copy of `A`); partition it with equal block sizes to
avoid that.
"""
struct JacobiPreconditioner{V<:DVector} <: AbstractDaggerPreconditioner
    dinv::V
end

JacobiPreconditioner(A::DMatrix) = JacobiPreconditioner(_jacobi_dinv(A))

# Per-tile kernel: write the reciprocal diagonal of `tile` into `out`.
_set_inv_diag!(out, tile) = (out .= inv.(LinearAlgebra.diag(tile)); return nothing)

# Resolve `A` to a square-tiled layout and return (n, Ac, mt, blocksize).
#
# A block-diagonal preconditioner is *defined* in terms of square diagonal
# blocks: each block is a square submatrix `A[r, r]` whose (approximate) inverse
# is applied to the matching slice of the vector, and the blocks must partition
# `1:n`. A non-square tiling has neither property, so `A` is re-tiled to square
# tiles of size `min(mb, nb)` rather than rejected -- the same fallback `lu`,
# `cholesky`, and `issymmetric` already take. It costs one tile-preserving copy
# of `A` at construction and nothing per apply.
#
# N.B. `repartition`, not `maybe_copy_buffered`: the latter frees its buffer when
# its body returns, but `_build_block_preconditioner` spawns the per-tile builds
# without awaiting them, and the operators it retains are built *from* these
# tiles.
function _square_tiled_layout(A::DMatrix)
    n = LinearAlgebra.checksquare(A)
    mb, nb = A.partitioning.blocksize
    Asq = mb == nb ? A : repartition(A, Blocks(min(mb, nb), min(mb, nb)))
    Ac = Asq.chunks
    mt, _ = size(Ac)
    return n, Ac, mt, Asq.partitioning.blocksize[1]
end

function _jacobi_dinv(A::DMatrix{T}) where T
    n, Ac, mt, mb = _square_tiled_layout(A)
    dinv = DVector{T}(undef, Blocks(mb), n)
    dc = dinv.chunks
    Dagger.spawn_datadeps() do
        for i in 1:mt
            Dagger.@spawn _set_inv_diag!(Out(dc[i]), In(Ac[i, i]))
        end
    end
    return dinv
end

function LinearAlgebra.mul!(y::DVector, P::JacobiPreconditioner, x::DVector)
    part = P.dinv.partitioning
    maybe_copy_buffered(P.dinv => part, x => part, y => part) do dinv, x, y
        dc, xc, yc = dinv.chunks, x.chunks, y.chunks
        Dagger.spawn_datadeps() do
            for i in eachindex(yc)
                Dagger.@spawn _jacobi_apply!(Out(yc[i]), In(dc[i]), In(xc[i]))
            end
        end
    end
    return y
end
_jacobi_apply!(y, dinv, x) = (y .= dinv .* x; return nothing)

# --- Block-diagonal preconditioners ---------------------------------------
# A family of preconditioners of the form `M⁻¹ = blockdiag(op₁, …, op_k)`, where
# each `opⱼ` approximates the inverse of `A`'s `j`-th diagonal tile `Aⱼⱼ`. They
# share all machinery and differ only in how a per-tile operator is *built*:
#
#   - BlockJacobiPreconditioner: exact tile solve via `lu` (dense or sparse).
#   - BlockILUPreconditioner:    incomplete LU per tile (needs IncompleteLU.jl).
#   - AMGPreconditioner:         an AMG hierarchy per tile (needs AlgebraicMultigrid.jl).
#
# Building per tile is embarrassingly parallel and a natural fit for the tiled
# layout. A single tile (`Blocks(n, n)`) makes any of these a *global*
# preconditioner over the whole matrix; many tiles make it a scalable
# block-Jacobi / additive-Schwarz variant that trades some convergence for
# parallelism. All re-tile a non-square `A` to square tiles (see
# `_square_tiled_layout`).
#
# A per-tile operator (`lu`/ILU factor, AMG hierarchy) generally cannot be moved
# between workers (dense `LU` has no `move!`; `UmfpackLU`/AMG hold process-bound
# resources). So each operator is built *once* and pinned (via
# `tochunk(..., ProcessScope)`) to the worker that owns its tile, and every apply
# for that block is scheduled there (`compute_scope`); datadeps then moves only
# the (movable) vector chunks to the operator, never the operator itself.

"""
    AbstractBlockPreconditioner <: AbstractDaggerPreconditioner

Block-diagonal preconditioner: holds one per-diagonal-tile operator `opⱼ`
(`y ← opⱼ⁻¹ x`), pinned to its tile's worker, and applies them independently per
block. Concrete subtypes (`BlockJacobiPreconditioner`, `BlockILUPreconditioner`,
`AMGPreconditioner`) share these fields: `ops`, `scopes`, `part`, `n`.
"""
abstract type AbstractBlockPreconditioner <: AbstractDaggerPreconditioner end

_tile_scope(c::Chunk) = ProcessScope(root_worker_id(c))
_tile_scope(t::DTask) = ProcessScope(root_worker_id(fetch(t; raw=true)))

"""
    PinnedTileOperator(op)

A per-diagonal-tile preconditioner operator, held opaque to Datadeps.

Block applies pass the operator as an untracked argument, but Datadeps still
computes aliasing for whatever it is handed, and recursing into a factorization
or multigrid hierarchy is both meaningless (it is never written) and frequently
impossible. Declaring the wrapper non-aliasing once covers every backend --
including operators from a user-supplied `build`, which is what makes
[`BlockPreconditioner`](@ref) work without any Dagger-side method per package.
"""
struct PinnedTileOperator{T}
    op::T
end
type_may_alias(::Type{<:PinnedTileOperator}) = false

# Build the per-tile operator on the current worker and pin the result there: the
# returned `Chunk` is process-scoped, so it can never be moved off this worker.
function _build_tile_pinned(build, tile)
    op = PinnedTileOperator(build(tile))
    proc = Dagger.task_processor()
    return tochunk(op, proc, ProcessScope(root_worker_id(proc)))
end

# Build a block preconditioner of type `Ctor` by applying `build` to each
# diagonal tile (pinned to the tile's worker). `build(tile) -> op` returns a
# per-tile operator supporting the block apply below.
function _build_block_preconditioner(Ctor, A::DMatrix, build)
    n, Ac, mt, mb = _square_tiled_layout(A)
    ops = Vector{Any}(undef, mt)
    scopes = Vector{ProcessScope}(undef, mt)
    for i in 1:mt
        tile = Ac[i, i]
        scope = _tile_scope(tile)
        scopes[i] = scope
        ops[i] = Dagger.@spawn compute_scope=scope _build_tile_pinned(build, tile)
    end
    return Ctor(ops, scopes, Blocks(mb), n)
end

function LinearAlgebra.mul!(y::DVector, P::AbstractBlockPreconditioner, x::DVector)
    part = P.part
    maybe_copy_buffered(x => part, y => part) do x, y
        xc, yc = x.chunks, y.chunks
        length(yc) == length(P.ops) || throw(DimensionMismatch(
            "$(nameof(typeof(P))) has $(length(P.ops)) blocks but the vector has \
            $(length(yc)) chunks"))
        Dagger.spawn_datadeps() do
            for i in eachindex(yc)
                # Pin the apply to the operator's worker; the operator is passed as
                # an untracked arg (read-only, already-pinned) so datadeps never
                # moves it -- only the vector chunks are moved to this worker.
                Dagger.@spawn compute_scope=P.scopes[i] _block_apply!(Out(yc[i]), P.ops[i], In(xc[i]))
            end
        end
    end
    return y
end

# Apply one block operator: `y = op⁻¹ x`.
#
# Host factorizations (UMFPACK/`lu` of a gathered CSC) need a host RHS, so a GPU
# vector chunk is gathered, solved, and written back; a CPU `Array`
# short-circuits.
_block_apply!(y, P::PinnedTileOperator, x) = _block_apply!(y, P.op, x)
function _block_apply!(y, op, x)
    if x isa Array
        _apply_inverse!(y, op, x)
    else
        xh = Adapt.adapt(Array, x)
        yh = similar(xh)
        _apply_inverse!(yh, op, xh)
        copyto!(y, yh)
    end
    return nothing
end

# `y = op⁻¹ x` for a host operator. Factorizations are applied with `\`;
# everything else (multigrid hierarchies, KrylovPreconditioners' operators, and
# whatever a user's `build` returns) with `ldiv!`, which is the interface those
# define. Backends may still override `_block_apply!` for a faster path.
_apply_inverse!(y, op, x) = LinearAlgebra.ldiv!(y, op, x)
_apply_inverse!(y, op::LinearAlgebra.Factorization, x) = copyto!(y, op \ x)

# Underlying matrix of a tile (overridden for `DSparseArray` in `sparse.jl`).
_tile_matrix(A) = A

"""
    BlockJacobiPreconditioner(A::DMatrix)

Block-Jacobi preconditioner: `M⁻¹ = blockdiag(A₁₁, …, A_kk)⁻¹`. Each diagonal
tile is factorized *once* with `lu` (sparse or dense) and the apply solves
`Aᵢᵢ yᵢ = xᵢ`. Stronger than `JacobiPreconditioner` (captures intra-block
coupling); a single tile recovers an exact solve. See
[`AbstractBlockPreconditioner`](@ref).
"""
struct BlockJacobiPreconditioner{F,S} <: AbstractBlockPreconditioner
    ops::F            # cached per-tile factorizations (pinned to their workers)
    scopes::S         # the `ProcessScope` each operator/apply is pinned to
    part::Blocks{1}   # partitioning of the vectors it applies to
    n::Int
end

# Factorize a diagonal tile. Backends override for their storage (e.g. sparse
# tiles factorize the inner `SparseMatrixCSC`); the default is a dense LU factor.
_factorize_tile(A) = LinearAlgebra.lu(A)

BlockJacobiPreconditioner(A::DMatrix) =
    _build_block_preconditioner(BlockJacobiPreconditioner, A, _factorize_tile)

"""
    BlockPreconditioner(A::DMatrix, build) -> BlockPreconditioner

Block-diagonal preconditioner from an arbitrary per-tile factory: `build(tile)`
is called once per diagonal tile of `A` and returns that block's operator, which
is then pinned to the tile's worker and applied via `ldiv!(y, op, x)` (or `\\`,
if it is a `Factorization`).

This is the extension point for third-party preconditioners, and it needs no
Dagger-side code per package. Dagger only supplies the distributed structure --
splitting `A` into square diagonal blocks, placing each operator with its tile,
and moving vector chunks to it; the numerics are entirely `build`'s.

```julia
using KrylovPreconditioners
P = Dagger.BlockPreconditioner(DA, tile -> KrylovPreconditioners.ilu(Dagger._tile_matrix(tile)))
x, stats = Krylov.gmres(DA, b; M = P)
```

`build` receives the raw tile, so unwrap it with `Dagger._tile_matrix` when the
factory wants the backing `SparseMatrixCSC` rather than Dagger's tile
container. The bundled
[`BlockJacobiPreconditioner`](@ref), [`BlockILUPreconditioner`](@ref), and
[`AMGPreconditioner`](@ref) are exactly this with a fixed `build`. See
[`AbstractBlockPreconditioner`](@ref).
"""
struct BlockPreconditioner{F,S} <: AbstractBlockPreconditioner
    ops::F
    scopes::S
    part::Blocks{1}
    n::Int
end

BlockPreconditioner(A::DMatrix, build) =
    _build_block_preconditioner(BlockPreconditioner, A, build)

"""
    BlockILUPreconditioner(A::DMatrix; τ=0.001, kwargs...)

Block incomplete-LU preconditioner: an ILU factorization (with drop tolerance
`τ`) of each diagonal tile, applied per block. Cheaper setup than a full block
solve, good as a general-purpose preconditioner. Requires `IncompleteLU.jl` to
be loaded and sparse-backed tiles. See [`AbstractBlockPreconditioner`](@ref).
"""
struct BlockILUPreconditioner{F,S} <: AbstractBlockPreconditioner
    ops::F
    scopes::S
    part::Blocks{1}
    n::Int
end
# Friendly fallback (shadowed by the `::DMatrix` method added in `ext/IncompleteLUExt.jl`).
BlockILUPreconditioner(A; kwargs...) = throw(ArgumentError(
    "Dagger.BlockILUPreconditioner requires IncompleteLU.jl. Run `using IncompleteLU` \
    to enable block incomplete-LU preconditioning."))

"""
    AMGPreconditioner(A::DMatrix; method=:ruge_stuben, kwargs...)

Algebraic-multigrid preconditioner: builds an AMG hierarchy (`method` is
`:ruge_stuben` or `:smoothed_aggregation`) for each diagonal tile and applies a
V-cycle per block. Near mesh-independent convergence for elliptic (Poisson-like)
operators. With one tile this is global AMG; with many tiles it is a scalable
block/additive-Schwarz AMG. Requires `AlgebraicMultigrid.jl` to be loaded and
sparse-backed tiles. See [`AbstractBlockPreconditioner`](@ref).
"""
struct AMGPreconditioner{F,S} <: AbstractBlockPreconditioner
    ops::F
    scopes::S
    part::Blocks{1}
    n::Int
end
# Friendly fallback (shadowed by the `::DMatrix` method added in `ext/AlgebraicMultigridExt.jl`).
AMGPreconditioner(A; kwargs...) = throw(ArgumentError(
    "Dagger.AMGPreconditioner requires AlgebraicMultigrid.jl. Run \
    `using AlgebraicMultigrid` to enable algebraic-multigrid preconditioning."))

"""
    BlockKLUPreconditioner(A::DMatrix; kwargs...)

Block direct preconditioner using KLU: an exact (pure-Julia) KLU factorization of
each diagonal tile, applied per block. With a single tile this is an exact direct
solve; with many tiles it is an exact-block-Jacobi preconditioner. Requires
`PureKLU.jl` to be loaded and sparse-backed tiles. See also
[`AbstractBlockPreconditioner`](@ref) and the whole-matrix solver
[`Dagger.klu`](@ref).
"""
struct BlockKLUPreconditioner{F,S} <: AbstractBlockPreconditioner
    ops::F
    scopes::S
    part::Blocks{1}
    n::Int
end
# Friendly fallback (shadowed by the `::DMatrix` method added in `ext/PureKLUExt.jl`).
BlockKLUPreconditioner(A; kwargs...) = throw(ArgumentError(
    "Dagger.BlockKLUPreconditioner requires PureKLU.jl. Run `using PureKLU` to \
    enable block KLU preconditioning."))

"""
    BlockUMFPACKPreconditioner(A::DMatrix; kwargs...)

Block direct preconditioner using a pure-Julia UMFPACK-style LU of each diagonal
tile, applied per block. With a single tile this is an exact direct solve; with
many tiles it is an exact-block-Jacobi preconditioner. Requires `PureUMFPACK.jl`
to be loaded and sparse-backed tiles. See also [`AbstractBlockPreconditioner`](@ref)
and the whole-matrix solver [`Dagger.splu`](@ref).
"""
struct BlockUMFPACKPreconditioner{F,S} <: AbstractBlockPreconditioner
    ops::F
    scopes::S
    part::Blocks{1}
    n::Int
end
# Friendly fallback (shadowed by the `::DMatrix` method added in `ext/PureUMFPACKExt.jl`).
BlockUMFPACKPreconditioner(A; kwargs...) = throw(ArgumentError(
    "Dagger.BlockUMFPACKPreconditioner requires PureUMFPACK.jl. Run \
    `using PureUMFPACK` to enable block UMFPACK preconditioning."))
