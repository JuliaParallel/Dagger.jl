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
# A note on the square-tile requirement below: the iterative solvers allocate
# *all* workspace vectors as `similar(b)` (one partitioning), and each must
# serve as both the length-`n` input and length-`n` output of `mul!(y, A, x)`.
# `gemv_dagger!` requires the input to match `A`'s column blocks and the output
# to match `A`'s row blocks, which is only simultaneously possible when those
# block sizes are equal. So the operator must use square tiles regardless of
# preconditioning; the preconditioners simply share that constraint. (Any
# uniform square tile size is fine, including a ragged final block.)

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

Requires square diagonal tiles (equal row/column block sizes); see the note
above on why the solver itself imposes this. Repartition `A` with equal block
sizes if needed.
"""
struct JacobiPreconditioner{V<:DVector} <: AbstractDaggerPreconditioner
    dinv::V
end

JacobiPreconditioner(A::DMatrix) = JacobiPreconditioner(_jacobi_dinv(A))

# Per-tile kernel: write the reciprocal diagonal of `tile` into `out`.
_set_inv_diag!(out, tile) = (out .= inv.(LinearAlgebra.diag(tile)); return nothing)

# Validate that `A` has square diagonal tiles and return (n, Ac, mt, blocksize).
function _check_square_tiles(A::DMatrix, who)
    n = LinearAlgebra.checksquare(A)
    Ac = A.chunks
    mt, nt = size(Ac)
    mb, nb = A.partitioning.blocksize
    # For a square matrix with square tiles, mt == nt follows automatically; we
    # only need to reject non-square tiles (mb != nb).
    mb == nb || throw(ArgumentError(
        "$who requires square diagonal tiles (got block size $(mb)x$(nb)); \
        repartition A with equal block sizes (this matches the iterative \
        solver's own requirement on the operator)"))
    return n, Ac, mt, mb
end

function _jacobi_dinv(A::DMatrix{T}) where T
    n, Ac, mt, mb = _check_square_tiles(A, "JacobiPreconditioner")
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

"""
    BlockJacobiPreconditioner(A::DMatrix)

Block-Jacobi preconditioner. The object represents the block-diagonal inverse
operator `M⁻¹ = blockdiag(A₁₁, …, A_kk)⁻¹`, where `Aᵢᵢ` are `A`'s diagonal tiles.
Applying it (`mul!(y, P, x)`) solves `Aᵢᵢ yᵢ = xᵢ` independently per block --
embarrassingly parallel and a natural fit for the tiled layout. Stronger than
`JacobiPreconditioner` (it captures intra-block coupling); a single tile recovers
an exact solve.

Requires square diagonal tiles (see the note above and `JacobiPreconditioner`).


Each diagonal tile is factorized *once* at construction. A factorization object
cannot be moved between workers (dense `LU` has no `move!`; sparse `UmfpackLU`
holds external/UMFPACK resources tied to its process), so each factor is pinned
(via `tochunk(..., ProcessScope)`) to the worker that owns its tile, and every
apply for that block is scheduled there (`compute_scope`). Datadeps then moves
only the (movable) vector chunks to the factor, never the factor itself.
"""
struct BlockJacobiPreconditioner{F,S} <: AbstractDaggerPreconditioner
    factors::F        # cached per-tile factorizations (pinned to their workers)
    scopes::S         # the `ProcessScope` each factor/apply is pinned to
    part::Blocks{1}   # partitioning of the vectors it applies to
    n::Int
end

# Factorize a diagonal tile. Backends override for their storage (e.g. sparse
# tiles factorize the inner `SparseMatrixCSC`); the default is a dense LU factor.
_factorize_tile(A) = LinearAlgebra.lu(A)

# Factorize on the current worker and pin the result there: the returned `Chunk`
# is process-scoped, so the factor can never be moved off this worker.
function _factorize_tile_pinned(Aii)
    F = _factorize_tile(Aii)
    proc = Dagger.task_processor()
    return tochunk(F, proc, ProcessScope(root_worker_id(proc)))
end

_tile_scope(c::Chunk) = ProcessScope(root_worker_id(c))
_tile_scope(t::DTask) = ProcessScope(root_worker_id(fetch(t; raw=true)))

function BlockJacobiPreconditioner(A::DMatrix)
    n, Ac, mt, mb = _check_square_tiles(A, "BlockJacobiPreconditioner")
    factors = Vector{Any}(undef, mt)
    scopes = Vector{ProcessScope}(undef, mt)
    for i in 1:mt
        tile = Ac[i, i]
        scope = _tile_scope(tile)
        scopes[i] = scope
        # Factor where the tile lives; the result is pinned to that worker.
        factors[i] = Dagger.@spawn compute_scope=scope _factorize_tile_pinned(tile)
    end
    return BlockJacobiPreconditioner(factors, scopes, Blocks(mb), n)
end

function LinearAlgebra.mul!(y::DVector, P::BlockJacobiPreconditioner, x::DVector)
    part = P.part
    maybe_copy_buffered(x => part, y => part) do x, y
        xc, yc = x.chunks, y.chunks
        length(yc) == length(P.factors) || throw(DimensionMismatch(
            "BlockJacobiPreconditioner has $(length(P.factors)) blocks but the \
            vector has $(length(yc)) chunks"))
        Dagger.spawn_datadeps() do
            for i in eachindex(yc)
                # Pin the solve to the factor's worker; the factor is passed as an
                # untracked arg (read-only, already-pinned) so datadeps never tries
                # to move it -- only the vector chunks are moved to this worker.
                Dagger.@spawn compute_scope=P.scopes[i] _blockjacobi_solve!(Out(yc[i]), P.factors[i], In(xc[i]))
            end
        end
    end
    return y
end
# Apply the cached block factorization: solve `Aᵢᵢ yᵢ = xᵢ`.
_blockjacobi_solve!(y, F, x) = (y .= F \ x; return nothing)
