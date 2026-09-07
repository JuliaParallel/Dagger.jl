# Global (distributed) algebraic multigrid.
#
# `AMGPreconditioner` is *per diagonal tile* — additive Schwarz, not AMG. A
# true coarse grid has to coarsen across tiles, form a Galerkin operator
# `Ac = R A P` as a distributed product, and apply a V-cycle over that
# hierarchy. That is what [`GlobalAMG`](@ref) does.
#
# First-cut limitations (honest): aggregation / classical interpolation still
# gather the current level to build `P` (those algorithms are sequential in
# AlgebraicMultigrid.jl). The Galerkin product and the V-cycle apply are
# distributed `DMatrix` / `DVector` operations. The coarsest solve is a
# gathered LU, same as `Dagger.klu` / `Dagger.splu`. Do not treat Krylov
# `stats.solved` as `Ax ≈ b`; check the un-preconditioned residual.

"""
    GlobalAMGLevel

One level of a [`GlobalAMG`](@ref) hierarchy: the level operator `A`, the
prolongation `P` (restriction is `P'`), a Jacobi `dinv`, and apply workspaces.
"""
struct GlobalAMGLevel{TA,TP,V}
    A::TA
    P::TP
    dinv::V
    res::V
    coarse_x::V
    coarse_b::V
end

"""
    GlobalAMG(A::DMatrix; method=:smoothed_aggregation, kwargs...)

A **global** algebraic-multigrid preconditioner over the whole sparse
`DMatrix` `A`. Unlike [`AMGPreconditioner`](@ref) (one hierarchy per diagonal
tile), this coarsens across tiles, forms each Galerkin coarse operator
`Ac = P' A P` by distributed sparse matmul, and applies a V-cycle via
`mul!(y, M, x)` (`y ← M⁻¹ x`, Krylov `ldiv=false`).

`method` is `:smoothed_aggregation` (default) or `:ruge_stuben`. Both reuse
AlgebraicMultigrid.jl for *setup* of `P` (strength + aggregation, or classical
interpolation). The expensive RAP and the apply are Dagger-distributed.

Keyword arguments:

- `max_levels=3`, `max_coarse=32` — stop after this many levels, or when the
  operator is this small (then a gathered LU is the coarse solver).
- `smooth=true` — Jacobi-smooth the tentative aggregation `P` (SA only).
- `relax=2/3`, `presweeps=1`, `postsweeps=1` — damped-Jacobi V-cycle sweeps.

Requires `AlgebraicMultigrid.jl`. A first cut: 1–2 coarse levels is enough
for a real coarse grid; this is not HYPRE BoomerAMG.

See also [`SmoothedAggregationPreconditioner`](@ref),
[`RugeStubenPreconditioner`](@ref).
"""
struct GlobalAMG{L,C,A} <: AbstractDaggerPreconditioner
    levels::L
    coarse::C             # pinned LU of the coarsest operator
    coarse_A::A           # coarsest `DMatrix` (the last Galerkin product, or `A`)
    relax::Float64
    presweeps::Int
    postsweeps::Int
    n::Int
    part::Blocks{1}
    method::Symbol
end

# Friendly fallback (shadowed by the `::DMatrix` method in AlgebraicMultigridExt).
GlobalAMG(A; kwargs...) = throw(ArgumentError(
    "Dagger.GlobalAMG requires AlgebraicMultigrid.jl. Run `using AlgebraicMultigrid` \
    to enable distributed (global) algebraic-multigrid preconditioning."))

"""
    SmoothedAggregationPreconditioner(A::DMatrix; kwargs...)

[`GlobalAMG`](@ref) with `method=:smoothed_aggregation`. Named so Krylov usage
reads like AlgebraicMultigrid.jl: `M = SmoothedAggregationPreconditioner(A)`.
"""
SmoothedAggregationPreconditioner(A; kwargs...) =
    GlobalAMG(A; method=:smoothed_aggregation, kwargs...)

"""
    RugeStubenPreconditioner(A::DMatrix; kwargs...)

[`GlobalAMG`](@ref) with `method=:ruge_stuben`. Named so Krylov usage reads
like AlgebraicMultigrid.jl: `M = RugeStubenPreconditioner(A)`.
"""
RugeStubenPreconditioner(A; kwargs...) =
    GlobalAMG(A; method=:ruge_stuben, kwargs...)

function Base.show(io::IO, M::GlobalAMG)
    print(io, "GlobalAMG(method=", M.method,
          ", levels=", length(M.levels) + 1,
          ", n=", M.n, ")")
end

# Per-tile kernels (named, so workers resolve them without closure capture).
_amg_residual!(r, b) = (r .= b .- r; nothing)

function _jacobi_smooth_chunk!(u, dinv, Au, b, ω)
    @. u += ω * dinv * (b - Au)
    return nothing
end

function _jacobi_smooth!(u::DVector, A::DMatrix, dinv::DVector, b::DVector,
                         r::DVector, ω, nsweeps)
    nsweeps <= 0 && return u
    ωT = eltype(u)(ω)
    part = u.partitioning
    for _ in 1:nsweeps
        mul!(r, A, u)
        maybe_copy_buffered(u => part, dinv => part, r => part, b => part) do u, dinv, r, b
            uc, dc, rc, bc = u.chunks, dinv.chunks, r.chunks, b.chunks
            Dagger.spawn_datadeps() do
                for i in eachindex(uc)
                    Dagger.@spawn _jacobi_smooth_chunk!(InOut(uc[i]), In(dc[i]),
                                                        In(rc[i]), In(bc[i]), ωT)
                end
            end
        end
    end
    return u
end

function _amg_restrict_residual!(res::DVector, A::DMatrix, u::DVector, b::DVector)
    mul!(res, A, u)
    part = res.partitioning
    maybe_copy_buffered(res => part, b => part) do res, b
        rc, bc = res.chunks, b.chunks
        Dagger.spawn_datadeps() do
            for i in eachindex(rc)
                Dagger.@spawn _amg_residual!(InOut(rc[i]), In(bc[i]))
            end
        end
    end
    return res
end

function _vcycle!(u::DVector, M::GlobalAMG, b::DVector, ℓ::Int)
    if ℓ > length(M.levels)
        copyto!(u, M.coarse \ b)
        return u
    end
    L = M.levels[ℓ]
    _jacobi_smooth!(u, L.A, L.dinv, b, L.res, M.relax, M.presweeps)
    _amg_restrict_residual!(L.res, L.A, u, b)
    mul!(L.coarse_b, L.P', L.res)
    fill!(L.coarse_x, zero(eltype(L.coarse_x)))
    _vcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1)
    mul!(L.res, L.P, L.coarse_x)
    axpy!(one(eltype(u)), L.res, u)
    _jacobi_smooth!(u, L.A, L.dinv, b, L.res, M.relax, M.postsweeps)
    return u
end

"""
    mul!(y, M::GlobalAMG, x)

One V-cycle: `y ← M⁻¹ x` with a zero initial guess. This is the Krylov
`ldiv=false` apply. Krylov `stats.solved` is the *preconditioned* residual;
check `‖A y − x‖` (un-preconditioned) when `x` is the right-hand side.
"""
function LinearAlgebra.mul!(y::DVector, M::GlobalAMG, x::DVector)
    length(x) == M.n || throw(DimensionMismatch(
        "GlobalAMG is $(M.n)×$(M.n) but x has length $(length(x))"))
    length(y) == M.n || throw(DimensionMismatch(
        "GlobalAMG is $(M.n)×$(M.n) but y has length $(length(y))"))
    part = M.part
    maybe_copy_buffered(x => part, y => part) do x, y
        fill!(y, zero(eltype(y)))
        _vcycle!(y, M, x, 1)
    end
    return y
end
