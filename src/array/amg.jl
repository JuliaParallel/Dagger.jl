# Global (distributed) algebraic multigrid.
#
# `AMGPreconditioner` is *per diagonal tile* — additive Schwarz, not AMG. A
# true coarse grid has to coarsen across tiles, form a Galerkin operator
# `Ac = R A P` as a distributed product, and apply a V-cycle over that
# hierarchy. That is what [`GlobalAMG`](@ref) does.
#
# `P` is built from tiled data: per-tile aggregation / classical interpolation
# plus leftover matching of unaggregated interface nodes. That does not
# assemble a global CSC of `A` on the default (scalar `ones`) path.
# `nullspace=N` still gathers `N` with `A` so `fit_candidates` can inject the
# rigid-body set and coarse levels get `R`, not the fine `N`. Merging
# already-assigned interface aggregates made the V-cycle worse than Jacobi
# on 1-D Poisson; do not reintroduce it without a residual check. The
# Galerkin product and the V-cycle apply are distributed. The coarsest
# solve is a gathered LU. Do not treat Krylov `stats.solved` as `Ax ≈ b`;
# check the un-preconditioned residual.

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
    GlobalAMG(A::DMatrix; method=:smoothed_aggregation, nullspace=N, kwargs...)

A **global** algebraic-multigrid preconditioner over the whole sparse
`DMatrix` `A`. Unlike [`AMGPreconditioner`](@ref) (one hierarchy per diagonal
tile), this coarsens across tiles, forms each Galerkin coarse operator
`Ac = P' A P` by distributed sparse matmul, and applies a V-cycle via
`mul!(y, M, x)` (`y ← M⁻¹ x`, Krylov `ldiv=false`).

`method` is `:smoothed_aggregation` (default) or `:ruge_stuben`. Setup of `P`
is tiled: AlgebraicMultigrid.jl runs per diagonal tile (strength +
aggregation, or classical interpolation). Unaggregated interface nodes
(off-tile entries in the same row) are paired; on 1-D Poisson local SA
assigns every node, so that matching is a no-op and `P` is block-diagonal
at the tentative level, then Jacobi-smoothed (`P ← T − ω D⁻¹ A T`) via
distributed SpGEMM. The expensive RAP and the apply are Dagger-distributed.

Keyword arguments:

- `nullspace` — near-nullspace / rigid-body modes (PETSc `MatSetNearNullSpace`).
  A `DMatrix` whose columns are the modes, a `DVector` (one mode), or a host
  `AbstractVecOrMat`. Smoothed aggregation injects them as `fit_candidates`
  (default is the scalar constant `ones`). `B` is an alias (AlgebraicMultigrid.jl
  name). Not a setter: there is no `Dagger.set_nearnullspace`. Ruge–Stüben
  rejects this keyword. [`AMGPreconditioner`](@ref) is unchanged (per-tile).
  `GlobalAMG(Projected(A, N))` reads `N` from the wrapper when `nullspace` is
  omitted. This path still gathers `N` with `A` to build `P`.
- `max_levels=3`, `max_coarse=32` — stop after this many levels, when the
  operator is this small, or when a later coarsening would see only a
  handful of tiles (then a gathered LU is the coarse solver).
- `smooth=true` — Jacobi-smooth the tentative aggregation `P` (SA only).
- `relax=2/3`, `presweeps=2`, `postsweeps=2` — damped-Jacobi V-cycle sweeps.
  One sweep each side is not enough for the coarse correction to beat
  Jacobi-only on 1-D Poisson; two is the smallest count that does.

Requires `AlgebraicMultigrid.jl`. A first cut: per-tile coarsening (not a
distributed MIS), typically one coarse grid (a later tiled coarsening on
≤3 tiles is skipped), gathered LU on the coarsest operator. Default setup
does not collect `A` to build `P`. `nullspace=N` still gathers. This is not
HYPRE BoomerAMG.

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
    nmodes::Int           # columns of `nullspace` (1 = default scalar ones)
end

# Friendly fallback (shadowed by the `::DMatrix` method in AlgebraicMultigridExt).
GlobalAMG(A; kwargs...) = throw(ArgumentError(
    "Dagger.GlobalAMG requires AlgebraicMultigrid.jl. Run `using AlgebraicMultigrid` \
    to enable distributed (global) algebraic-multigrid preconditioning."))

"""
    SmoothedAggregationPreconditioner(A::DMatrix; nullspace=N, kwargs...)

[`GlobalAMG`](@ref) with `method=:smoothed_aggregation`. Named so Krylov usage
reads like AlgebraicMultigrid.jl: `M = SmoothedAggregationPreconditioner(A)`.
Pass rigid-body / near-nullspace modes as `nullspace` (a `DMatrix` of columns);
see [`GlobalAMG`](@ref).
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
          ", n=", M.n,
          ", nullspace=", M.nmodes, ")")
end

# Per-tile interpolation payload (lives on the tile's worker). The caller
# fetches only `AMGTileHeader` (`nagg` + interface pairs), not `A`.
struct AMGTileInterp{T}
    I::Vector{Int}
    J::Vector{Int}
    V::Vector{T}
    nagg::Int
    iface_local::Vector{Int}
    iface_nbr::Vector{Int}
end

struct AMGTileHeader
    nagg::Int
    iface_local::Vector{Int}
    iface_nbr::Vector{Int}
end

_amg_interp_header(p::AMGTileInterp) =
    AMGTileHeader(p.nagg, p.iface_local, p.iface_nbr)

# Implemented in AlgebraicMultigridExt (named so workers resolve them).
function _amg_row_coarsen end
function _amg_fill_p_tile end
function _amg_row_abs_inv_chunk end
function _amg_smooth_p_tile end

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
        LinearAlgebra.mul!(r, A, u)
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
    LinearAlgebra.mul!(res, A, u)
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
    LinearAlgebra.mul!(L.coarse_b, L.P', L.res)
    fill!(L.coarse_x, zero(eltype(L.coarse_x)))
    _vcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1)
    LinearAlgebra.mul!(L.res, L.P, L.coarse_x)
    LinearAlgebra.axpy!(one(eltype(u)), L.res, u)
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
