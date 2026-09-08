module AlgebraicMultigridExt

import AlgebraicMultigrid
import SparseArrays
import SparseArrays: SparseMatrixCSC
import Dagger
import Dagger: DMatrix, DVector, Blocks, GlobalAMG, GlobalAMGLevel
import LinearAlgebra

# ---------------------------------------------------------------------------
# Per-tile AMG (unchanged): one AlgebraicMultigrid hierarchy per diagonal tile.
# That is additive Schwarz. See `GlobalAMG` below for a real coarse grid.
# ---------------------------------------------------------------------------

_as_sparse(A::SparseMatrixCSC) = A
_as_sparse(A::AbstractMatrix) = SparseArrays.sparse(A)

function _amg_operator(tile, method::Symbol; kwargs...)
    S = _as_sparse(Dagger._tile_matrix(tile))
    ml = if method === :ruge_stuben
        AlgebraicMultigrid.ruge_stuben(S; kwargs...)
    elseif method === :smoothed_aggregation
        AlgebraicMultigrid.smoothed_aggregation(S; kwargs...)
    else
        throw(ArgumentError("AMGPreconditioner: unknown method $(method); use \
            :ruge_stuben or :smoothed_aggregation"))
    end
    return AlgebraicMultigrid.aspreconditioner(ml)
end

function Dagger.AMGPreconditioner(A::DMatrix; method::Symbol=:ruge_stuben, kwargs...)
    build = tile -> _amg_operator(tile, method; kwargs...)
    return Dagger._build_block_preconditioner(Dagger.AMGPreconditioner, A, build)
end

# ---------------------------------------------------------------------------
# Global AMG: coarsen across tiles, Galerkin `Ac = P' A P` (distributed),
# V-cycle apply in `src/array/amg.jl`.
# ---------------------------------------------------------------------------

const _HIERARCHY_KW = (
    :method, :smooth, :max_levels, :max_coarse, :relax, :presweeps, :postsweeps,
    :jacobi_ω,
)

function _passthrough_kwargs(; kwargs...)
    return (; (k => v for (k, v) in kwargs if k ∉ _HIERARCHY_KW)...)
end

function _square_amg_operator(A::DMatrix)
    n = LinearAlgebra.checksquare(A)
    mb, nb = A.partitioning.blocksize
    Asq = mb == nb ? A : Dagger.repartition(A, Blocks(min(mb, nb), min(mb, nb)))
    return n, Asq
end

# Strength + StandardAggregation (or classical interpolation) on a gathered
# CSC. The gather is setup-only and sees the *whole* graph, so aggregates
# cross tile boundaries. Do not confuse this with wrapping per-tile AMG.
function _sa_prolongation(A_csc::SparseMatrixCSC;
                          strength = AlgebraicMultigrid.SymmetricStrength(),
                          aggregate = AlgebraicMultigrid.StandardAggregation(),
                          smooth::Bool = true,
                          jacobi_ω = 4 / 3)
    S, _ = strength(A_csc)
    AggOp = aggregate(S)
    size(AggOp, 1) == 0 && return nothing
    n = size(A_csc, 1)
    T = eltype(A_csc)
    B = ones(T, n)
    Tent, _ = AlgebraicMultigrid.fit_candidates(AggOp, B)
    size(Tent, 2) == 0 && return nothing
    if !smooth
        return Tent
    end
    P = AlgebraicMultigrid.JacobiProlongation(T(jacobi_ω))(A_csc, Tent, S, B)
    return size(P, 2) == 0 ? nothing : P
end

function _rs_prolongation(A_csc::SparseMatrixCSC; kwargs...)
    # One interpolation only: we discard AMG.jl's local RAP and form `Ac`
    # as a distributed product instead.
    extra = _passthrough_kwargs(; kwargs...)
    n = size(A_csc, 1)
    ml = AlgebraicMultigrid.ruge_stuben(A_csc; max_levels=2, max_coarse=max(n - 1, 1), extra...)
    isempty(ml.levels) && return nothing
    P = SparseArrays.sparse(ml.levels[1].P)
    return size(P, 2) == 0 ? nothing : P
end

function _amg_prolongation(A::DMatrix{T}; method::Symbol, smooth::Bool,
                           jacobi_ω=4 / 3, kwargs...) where T
    A_csc = Dagger._collect_sparse_dmatrix(A)
    P_csc = if method === :smoothed_aggregation
        extra = _passthrough_kwargs(; kwargs...)
        _sa_prolongation(A_csc; smooth, jacobi_ω=jacobi_ω, extra...)
    elseif method === :ruge_stuben
        _rs_prolongation(A_csc; kwargs...)
    else
        throw(ArgumentError("GlobalAMG: unknown method $(method); use \
            :smoothed_aggregation or :ruge_stuben"))
    end
    P_csc === nothing && return nothing
    nc = size(P_csc, 2)
    (nc == 0 || nc >= size(A, 1)) && return nothing
    k = Int(A.partitioning.blocksize[1])
    # Same block size as `A` so `A * P` lines up; a ragged last block is fine
    # (including `nc < k`, which yields a single coarse column-tile).
    return Dagger.distribute(P_csc, Blocks(k, k))
end

# Distributed Galerkin product `Ac = P' A P`. Allocated through `allocate_tiled`
# so sparse tiles stay sparse (lesson 22 / 24).
function _amg_galerkin(A::DMatrix{T}, P::DMatrix{T}) where T
    n = size(A, 1)
    nc = size(P, 2)
    k = Int(A.partitioning.blocksize[1])
    kc = Int(P.partitioning.blocksize[2])
    TT = Dagger.darray_tiletype(A)
    AP = Dagger.allocate_tiled(TT, T, Blocks(k, kc), (n, nc))
    LinearAlgebra.mul!(AP, A, P)
    Ac = Dagger.allocate_tiled(TT, T, Blocks(kc, kc), (nc, nc))
    LinearAlgebra.mul!(Ac, P', AP)
    return Ac
end

function _amg_level(A::DMatrix{T}, P::DMatrix{T}) where T
    n = size(A, 1)
    nc = size(P, 2)
    k = Int(A.partitioning.blocksize[1])
    kc = Int(P.partitioning.blocksize[2])
    dinv = Dagger._jacobi_dinv(A)
    res = DVector{T}(undef, Blocks(k), n)
    coarse_x = DVector{T}(undef, Blocks(kc), nc)
    coarse_b = DVector{T}(undef, Blocks(kc), nc)
    return GlobalAMGLevel(A, P, dinv, res, coarse_x, coarse_b)
end

function Dagger.GlobalAMG(A::DMatrix;
                          method::Symbol=:smoothed_aggregation,
                          smooth::Bool=(method === :smoothed_aggregation),
                          max_levels::Integer=3,
                          max_coarse::Integer=32,
                          relax::Real=2 / 3,
                          presweeps::Integer=2,
                          postsweeps::Integer=2,
                          jacobi_ω::Real=4 / 3,
                          kwargs...)
    method === :smoothed_aggregation || method === :ruge_stuben || throw(ArgumentError(
        "GlobalAMG: unknown method $(method); use :smoothed_aggregation or :ruge_stuben"))
    max_levels >= 1 || throw(ArgumentError("max_levels must be ≥ 1"))
    max_coarse >= 1 || throw(ArgumentError("max_coarse must be ≥ 1"))
    presweeps >= 0 && postsweeps >= 0 || throw(ArgumentError(
        "presweeps and postsweeps must be ≥ 0"))

    n, A = _square_amg_operator(A)
    levels = GlobalAMGLevel[]
    while length(levels) + 1 < max_levels && size(A, 1) > max_coarse
        P = _amg_prolongation(A; method, smooth, jacobi_ω, kwargs...)
        P === nothing && break
        Ac = _amg_galerkin(A, P)
        push!(levels, _amg_level(A, P))
        A = Ac
    end
    coarse = Dagger._spawn_direct_factorization(A, LinearAlgebra.lu)
    part = Blocks(Int((isempty(levels) ? A : levels[1].A).partitioning.blocksize[1]))
    return GlobalAMG(levels, coarse, A, Float64(relax), Int(presweeps), Int(postsweeps),
                     n, part, method)
end

# AlgebraicMultigrid.jl-shaped entry points. `smoothed_aggregation(::DMatrix)`
# returns a Krylov-ready `GlobalAMG` (not a host `MultiLevel`);
# `aspreconditioner` is therefore the identity.
function AlgebraicMultigrid.smoothed_aggregation(A::DMatrix; kwargs...)
    return Dagger.GlobalAMG(A; method=:smoothed_aggregation, kwargs...)
end

function AlgebraicMultigrid.ruge_stuben(A::DMatrix; kwargs...)
    return Dagger.GlobalAMG(A; method=:ruge_stuben, kwargs...)
end

AlgebraicMultigrid.aspreconditioner(M::Dagger.GlobalAMG) = M

end # module
