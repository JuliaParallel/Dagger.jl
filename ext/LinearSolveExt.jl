module LinearSolveExt

# LinearSolve.jl on Dagger arrays.
#
# SciML / implicit ODE / NonlinearSolve pick linear solvers through LinearSolve.
# This extension is the dispatch hook: `solve(LinearProblem(A, b))` with a
# `DMatrix` / `DVector` chooses a LinearSolve algorithm (never a novel
# `Dagger.xyz` solver type), and the existing LinearSolve algorithms then run
# on the distributed arrays.
#
#   * Sparse-backed `DMatrix` that fits on one worker → `PureUMFPACKFactorization`
#     (if PureUMFPACK is loaded) or `PureKLUFactorization` (LinearSolve's hard
#     dep). Both route through `Dagger.splu` / `Dagger.klu` so the factor stays
#     sparse and pinned; we do *not* let LinearSolve's host `SparseMatrixCSC`
#     path `collect` the tiles.
#   * Everything else (dense `DMatrix`, matrix-free `mul!` operators, or a
#     sparse system too large to gather) → `KrylovJL_GMRES` (or LSMR / CRAIGMR
#     when not square). Krylov.jl already works on `DVector`s via `KrylovExt`;
#     workspace construction is routed through `KrylovConstructor` here so this
#     extension does not call into `KrylovExt` (load order is unspecified).
#
# `DefaultLinearSolver` is intentionally *not* returned. Its
# `needs_concrete_A = true` and its `init_cacheval` materializes every
# polyalgorithm slot (LU, QR, KLU, …), which is wrong for a distributed /
# matrix-free operator.

import Dagger
import Dagger: DMatrix, DVector, AbstractDaggerPreconditioner
import LinearAlgebra
import LinearSolve
import LinearSolve: OperatorAssumptions, LinearCache, LinearVerbosity,
    PureKLUFactorization, PureUMFPACKFactorization, KrylovJL

# SciMLBase / SciMLOperators / Krylov are LinearSolve hard deps, not triggers
# of this extension. Reach them through LinearSolve so the extension does not
# need extra weakdeps (and does not call into Dagger's other extensions).
const SciMLBase = LinearSolve.SciMLBase
const ReturnCode = SciMLBase.ReturnCode
const SciMLOperators = LinearSolve.SciMLOperators
const Krylov = LinearSolve.Krylov

# Gather-to-one-worker is the contract of `Dagger.klu` / `Dagger.splu`. Above
# this size the default is distributed Krylov; callers who still want a
# gathered factor pass `PureKLUFactorization()` / `PureUMFPACKFactorization()`
# explicitly.
const SPARSE_DIRECT_MAX_N = 10_000

# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------

function _sparse_backed(A::DMatrix)
    TT = Dagger.darray_tiletype(A)
    return TT <: Dagger.DSparseArray
end

_pureumfpack_loaded() = Base.get_extension(Dagger, :PureUMFPACKExt) !== nothing
_pureklu_loaded() = Base.get_extension(Dagger, :PureKLUExt) !== nothing

function _fits_sparse_direct(A::DMatrix)
    n = size(A, 1)
    return n == size(A, 2) && n <= SPARSE_DIRECT_MAX_N && _sparse_backed(A)
end

# ---------------------------------------------------------------------------
# Default algorithm
# ---------------------------------------------------------------------------

function _krylov_default(A, b, assump::OperatorAssumptions{Bool})
    if !assump.issq
        m, n = size(A)
        return m < n ? LinearSolve.KrylovJL_CRAIGMR() : LinearSolve.KrylovJL_LSMR()
    end
    return LinearSolve.KrylovJL_GMRES()
end

function _defaultalg_dmatrix(A::DMatrix, b, assump::OperatorAssumptions{Bool})
    if assump.issq && _fits_sparse_direct(A)
        # PDE-structured systems prefer UMFPACK when the optional backend is
        # present; KLU is always available once LinearSolve (hence PureKLU) is
        # loaded and Dagger's PureKLUExt has fired.
        _pureumfpack_loaded() && return PureUMFPACKFactorization()
        _pureklu_loaded() && return PureKLUFactorization()
    end
    return _krylov_default(A, b, assump)
end

function LinearSolve.defaultalg(
        A::DMatrix, b, assump::OperatorAssumptions{Bool}
    )
    return _defaultalg_dmatrix(A, b, assump)
end

# Intersection: `defaultalg(::DMatrix, ::Any)` and `defaultalg(::Any, ::DVector)`
# are otherwise unordered.
function LinearSolve.defaultalg(
        A::DMatrix, b::DVector, assump::OperatorAssumptions{Bool}
    )
    return _defaultalg_dmatrix(A, b, assump)
end

# Multi-RHS: same algorithm as a vector `b`. LinearSolve's `KrylovJL_GMRES` /
# `KrylovJL_MINRES` already construct `BlockGmresWorkspace` / `BlockMinresWorkspace`
# for an `AbstractMatrix` RHS; KrylovExt supplies the `DMatrix` constructors.
function LinearSolve.defaultalg(
        A::DMatrix, b::DMatrix, assump::OperatorAssumptions{Bool}
    )
    return _defaultalg_dmatrix(A, b, assump)
end

# Matrix-free (or any non-`DMatrix` operator) with a `DVector` RHS: only `mul!`
# is available, so Krylov. Returning `KrylovJL_*` directly — not
# `DefaultLinearSolver` — keeps `needs_concrete_A = false`.
function LinearSolve.defaultalg(
        A, b::DVector, assump::OperatorAssumptions{Bool}
    )
    return _krylov_default(A, b, assump)
end

# Disambiguate LinearSolve's `AbstractSciMLOperator` / `MatrixOperator` /
# `WOperator` methods against `defaultalg(::Any, ::DVector)`.
function LinearSolve.defaultalg(
        A::SciMLOperators.AbstractSciMLOperator, b::DVector,
        assump::OperatorAssumptions{Bool}
    )
    if LinearSolve.has_ldiv!(A)
        return LinearSolve.DirectLdiv!()
    end
    return _krylov_default(A, b, assump)
end

function LinearSolve.defaultalg(
        A::SciMLOperators.MatrixOperator, b::DVector,
        assump::OperatorAssumptions{Bool}
    )
    return LinearSolve.defaultalg(A.A, b, assump)
end

function LinearSolve.defaultalg(
        A::SciMLOperators.WOperator, b::DVector,
        assump::OperatorAssumptions{Bool}
    )
    # A Dagger Jacobian is not a host `DenseMatrix` / CSC, so LHL's reduction
    # does not apply; keep the operator on the Krylov path.
    return _krylov_default(A, b, assump)
end

function LinearSolve.defaultalg(
        A::LinearAlgebra.AdjOrTrans{<:Any, <:DMatrix}, b,
        assump::OperatorAssumptions{Bool}
    )
    return _krylov_default(A, b, assump)
end

function LinearSolve.defaultalg(
        A::LinearAlgebra.AdjOrTrans{<:Any, <:DMatrix}, b::DVector,
        assump::OperatorAssumptions{Bool}
    )
    return _krylov_default(A, b, assump)
end

# ---------------------------------------------------------------------------
# KrylovJL workspace: allocate every vector with `similar(b)`
# ---------------------------------------------------------------------------

# Same reason as LinearSolve's `ArrayPartition` method: `S(undef, n)` for
# `S = typeof(b)` cannot see a `DVector`'s block size or chunk layout.
# `KrylovConstructor` builds the workspace from `similar`, so every internal
# vector inherits `b`'s partitioning. `A` is unused — matrix-free operators
# only need `mul!` and `size`.
function LinearSolve.init_cacheval(
        alg::KrylovJL, A, b::DVector, u, Pl, Pr,
        maxiters::Int, abstol, reltol, verbose::Union{LinearVerbosity, Bool},
        assumptions::OperatorAssumptions; zeroinit = true
    )
    KS = LinearSolve.get_KrylovJL_solver(alg.KrylovAlg)
    kwargs_nt = NamedTuple(alg.kwargs)
    proto_b = similar(b)
    proto_u = u isa DVector ? similar(u) : similar(b)
    constructor = proto_u === proto_b || length(proto_u) == length(proto_b) ?
        Krylov.KrylovConstructor(proto_b) :
        Krylov.KrylovConstructor(proto_b, proto_u)

    solver = if (
            alg.KrylovAlg === Krylov.dqgmres! ||
                alg.KrylovAlg === Krylov.diom! ||
                alg.KrylovAlg === Krylov.gmres! ||
                alg.KrylovAlg === Krylov.fgmres! ||
                alg.KrylovAlg === Krylov.gpmr! ||
                alg.KrylovAlg === Krylov.fom!
        )
        memory = if zeroinit
            1
        elseif haskey(kwargs_nt, :memory)
            kwargs_nt[:memory]
        elseif alg.gmres_restart == 0
            min(20, size(A, 1))
        else
            alg.gmres_restart
        end
        KS(constructor; memory)
    elseif (
            alg.KrylovAlg === Krylov.minres! ||
                alg.KrylovAlg === Krylov.symmlq! ||
                alg.KrylovAlg === Krylov.lslq! ||
                alg.KrylovAlg === Krylov.lsqr! ||
                alg.KrylovAlg === Krylov.lsmr!
        )
        (!zeroinit && alg.window != 0) ? KS(constructor; window = alg.window) :
            KS(constructor)
    else
        KS(constructor)
    end

    solver.x = u
    return solver
end

# More specific than LinearSolve's `b::AbstractMatrix` arm so a `DMatrix` RHS
# hits KrylovExt's `BlockGmresWorkspace(A, B::DMatrix)` / `BlockMinresWorkspace`
# (tall blocks inherit `B`'s partitioning; the Hessenberg stays host).
function LinearSolve.init_cacheval(
        alg::KrylovJL, A, b::DMatrix, u, Pl, Pr,
        maxiters::Int, abstol, reltol, verbose::Union{LinearVerbosity, Bool},
        assumptions::OperatorAssumptions; zeroinit = true
    )
    if alg.KrylovAlg === Krylov.gmres!
        kwargs_nt = NamedTuple(alg.kwargs)
        memory = if haskey(kwargs_nt, :memory)
            kwargs_nt.memory
        elseif alg.gmres_restart == 0
            min(20, max(1, div(size(A, 1), max(size(b, 2), 1))))
        else
            alg.gmres_restart
        end
        return Krylov.BlockGmresWorkspace(A, b; memory)
    elseif alg.KrylovAlg === Krylov.minres!
        return Krylov.BlockMinresWorkspace(A, b)
    end
    return nothing
end

# LinearSolve's KrylovJL path applies `Pl`/`Pr` with `ldiv=true`. Dagger
# preconditioners represent `M⁻¹` and apply it with `mul!` (`ldiv=false` for
# Krylov.jl). These methods are the LinearSolve-facing adapter; they do not
# change the Krylov.jl `M=` convention used by `Dagger.cg` and friends.
function LinearAlgebra.ldiv!(
        y::DVector, P::AbstractDaggerPreconditioner, x::DVector
    )
    return LinearAlgebra.mul!(y, P, x)
end

function LinearAlgebra.ldiv!(P::AbstractDaggerPreconditioner, x::DVector)
    y = similar(x)
    LinearAlgebra.mul!(y, P, x)
    copyto!(x, y)
    return x
end

# ---------------------------------------------------------------------------
# Sparse direct: keep tiles sparse, pin the factor, solve over DVectors
# ---------------------------------------------------------------------------

# `LinearCache.cacheval` is typed from `init_cacheval`. A wrapper keeps that
# type stable across the empty-init / first-solve / `lu!(F, A)` refactor
# sequence (KLU reuses symbolic analysis; UMFPACK rebuilds).
mutable struct DaggerDirectCache
    F::Any
end

# Split per algorithm: a Union here is ambiguous with LinearSolve's
# `init_cacheval(::PureUMFPACKFactorization, ::AbstractArray, …)` (more specific
# on `alg`, less specific on `A`).
function LinearSolve.init_cacheval(
        ::PureKLUFactorization,
        A::DMatrix, b, u, Pl, Pr,
        maxiters::Int, abstol, reltol,
        verbose::Union{LinearVerbosity, Bool}, assumptions::OperatorAssumptions
    )
    return DaggerDirectCache(nothing)
end

function LinearSolve.init_cacheval(
        ::PureUMFPACKFactorization,
        A::DMatrix, b, u, Pl, Pr,
        maxiters::Int, abstol, reltol,
        verbose::Union{LinearVerbosity, Bool}, assumptions::OperatorAssumptions
    )
    return DaggerDirectCache(nothing)
end

function _require_darray_rhs(alg, b)
    (b isa DVector || b isa DMatrix) || throw(ArgumentError(
        "$(nameof(typeof(alg))) on a DMatrix requires a DVector or DMatrix \
        right-hand side (got $(typeof(b))). Distribute `b` with the same \
        blocking as `A`'s columns, e.g. \
        `distribute(b, Blocks(A.partitioning.blocksize[2]))`."))
    return b
end

function _store_solution!(u::Dagger.DArray, x::Dagger.DArray)
    u === x && return u
    copyto!(u, x)
    return u
end
function _store_solution!(u::AbstractVecOrMat, x::Dagger.DArray)
    copyto!(u, collect(x))
    return u
end
function _store_solution!(u, x)
    u === x && return u
    copyto!(u, x)
    return u
end

function _solve_dagger_direct!(cache::LinearCache, alg, factorize)
    b = _require_darray_rhs(alg, cache.b)
    F = cache.cacheval.F
    if F === nothing
        cache.cacheval.F = factorize(cache.A)
        cache.isfresh = false
    elseif cache.isfresh
        # Same sparsity, new values: `lu!(F, A)` reuses KLU's symbolic analysis.
        # PureUMFPACK has no `splu!`, so that path rebuilds inside the pinned box.
        try
            LinearAlgebra.lu!(F, cache.A)
        catch e
            e isa DimensionMismatch || rethrow()
            cache.cacheval.F = factorize(cache.A)
        end
        cache.isfresh = false
    end
    x = cache.cacheval.F \ b
    y = _store_solution!(cache.u, x)
    return SciMLBase.build_linear_solution(
        alg, y, nothing, nothing; retcode = ReturnCode.Success
    )
end

# More specific than LinearSolve's `LinearCache` + host-CSC `solve!`.
function SciMLBase.solve!(
        cache::LinearCache{<:DMatrix}, alg::PureKLUFactorization; kwargs...
    )
    return _solve_dagger_direct!(cache, alg, Dagger.klu)
end

function SciMLBase.solve!(
        cache::LinearCache{<:DMatrix}, alg::PureUMFPACKFactorization; kwargs...
    )
    return _solve_dagger_direct!(cache, alg, Dagger.splu)
end

end # module LinearSolveExt
