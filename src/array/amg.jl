# Global (distributed) algebraic multigrid.
#
# `AMGPreconditioner` is *per diagonal tile* — additive Schwarz, not AMG. A
# true coarse grid has to coarsen across tiles, form a Galerkin operator
# `Ac = R A P` as a distributed product, and apply a V-cycle (or W/F) over
# that hierarchy. That is what [`GlobalAMG`](@ref) does.
#
# Default coarsening is HMIS-lite (local SA, then PMIS on leftovers) over
# the tiled strength graph: a coarsen task still sees one row of tiles, but
# interface nodes join a *global* C/F or aggregate assignment. Full PMIS
# (`coarsen=:pmis`) is the same MIS on every node; on 1-D Poisson n=128 that
# V-cycle loses to Jacobi (do not make it the default). That is not leftover
# pairing of unaggregated interface nodes, and it is not a merge of
# already-assigned aggregates (that scheme lost to Jacobi; do not bring it
# back without a residual check). The Galerkin product and the cycle apply
# are distributed. The coarsest solve is a gathered LU. Do not treat Krylov
# `stats.solved` as `Ax ≈ b`; check the un-preconditioned residual.

"""
    GlobalAMGLevel

One level of a [`GlobalAMG`](@ref) hierarchy: the level operator `A`, the
prolongation `P` (restriction is `P'`), a Jacobi-family `dinv`, apply
workspaces, and optional smoother state (`extra`: Chebyshev bounds or a
level ILU/RAS preconditioner).
"""
struct GlobalAMGLevel{TA,TP,V,S}
    A::TA
    P::TP
    dinv::V
    res::V
    coarse_x::V
    coarse_b::V
    work::V
    dir::V
    extra::S
end

"""
    GlobalAMG(A::DMatrix; method=:smoothed_aggregation, nullspace=N, kwargs...)

A **global** algebraic-multigrid preconditioner over the whole sparse
`DMatrix` `A`. Unlike [`AMGPreconditioner`](@ref) (one hierarchy per diagonal
tile), this coarsens across tiles, forms each Galerkin coarse operator
`Ac = P' A P` by distributed sparse matmul, and applies a V-cycle (or W/F)
via `mul!(y, M, x)` (`y ← M⁻¹ x`, Krylov `ldiv=false`).

`method` is `:smoothed_aggregation` (default) or `:ruge_stuben`. Default
coarsening is HMIS-lite (`coarsen=:hmis`): local SA/RS first, then a
tiled PMIS on unassigned interface nodes so they join a global C/F or
aggregate assignment. `coarsen=:pmis` is a full parallel independent set
(every node); on 1-D Poisson that V-cycle can lose to the same number of
Jacobi sweeps (n=128: residual ~1.5 vs ~0.89), so it is opt-in.
`coarsen=:standard` is the older per-tile + leftover-pair path. On 1-D
Poisson local SA assigns every node, so HMIS leftover matching is a no-op
and tentative `P` is block-diagonal, then Jacobi-smoothed
(`P ← T − ω D⁻¹ A T`) via distributed SpGEMM. The expensive RAP and the
apply are Dagger-distributed.

Keyword arguments:

- `nullspace` — near-nullspace / rigid-body modes (PETSc `MatSetNearNullSpace`).
  A `DMatrix` whose columns are the modes, a `DVector` (one mode), or a host
  `AbstractVecOrMat`. Smoothed aggregation injects them as `fit_candidates`
  (default is the scalar constant `ones`). `B` is an alias (AlgebraicMultigrid.jl
  name). Not a setter: there is no `Dagger.set_nearnullspace`. Ruge–Stüben
  rejects this keyword. [`AMGPreconditioner`](@ref) is unchanged (per-tile).
  `GlobalAMG(Projected(A, N))` reads `N` from the wrapper when `nullspace` is
  omitted. This path gathers `N` (not `A`) so coarse levels get `R` from
  `fit_candidates`.
- `blocksize` / `nvars` — HYPRE `NumFunctions`: coarsen the nodal graph of
  this many interleaved unknowns. Default `1`. When `> 1` and `nullspace` is
  omitted, candidates are the per-unknown constants. Ruge–Stüben ignores
  candidate injection; nodal coarsening still applies.
- `max_levels=10`, `max_coarse=32` — recurse with distributed RAP until the
  operator is this small, this many levels exist, or coarsening stalls.
  Gathered LU is only the true coarsest solve.
- `smooth=true` — Jacobi-smooth the tentative aggregation `P` (SA only).
- `smoother=:jacobi` — V-cycle pre/post: `:jacobi` (damped, `relax=2/3`),
  `:l1jacobi` (row-ℓ1 scaled, `relax=1` by default), `:chebyshev`
  (polynomial; `chebyshev_degree`, `chebyshev_ratio`), `:hybrid_gs`
  (processor-local GS on the diagonal tile, Jacobi off-tile), `:ilu`
  (existing [`BlockILUPreconditioner`](@ref) as a level smoother), `:ras`
  (existing [`AdditiveSchwarzPreconditioner`](@ref); default overlap 0).
- `relax=2/3`, `presweeps=2`, `postsweeps=2` — Jacobi-family damping and
  sweep counts. One Jacobi sweep each side is not enough for the coarse
  correction to beat Jacobi-only on 1-D Poisson; two is the smallest count
  that does.
- `cycle=:v` — `:v`, `:w`, or `:f`.
- `coarsen=:hmis` — `:hmis` (default), `:pmis`, or `:standard`.
- `interp=:sa` — SA tentative + smooth; RS uses `:direct` (classical
  distance-1). AlgebraicMultigrid.jl has no ext+i / AIR / FF hook.

Requires `AlgebraicMultigrid.jl`. Default setup does not collect `A` to
build `P`. `nullspace=N` gathers `N` only. This is not HYPRE BoomerAMG.

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
    smoother::Symbol
    cycle::Symbol
    coarsen::Symbol
    blocksize::Int
    chebyshev_degree::Int
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
          ", smoother=", M.smoother,
          ", cycle=", M.cycle,
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

# Strength graph of one row of tiles (lives on the tile's worker). Used by
# PMIS / HMIS / classical interpolation. The caller does not fetch `A`.
struct AMGTileGraph{T}
    row0::Int
    k::Int
    rowptr::Vector{Int}
    colval::Vector{Int}
    nzval::Vector{T}
    strong::BitVector
    diag::Vector{T}
end

# Implemented in AlgebraicMultigridExt (named so workers resolve them).
function _amg_row_coarsen end
function _amg_fill_p_tile end
function _amg_row_abs_inv_chunk end
function _amg_smooth_p_tile end
function _amg_row_graph end
function _amg_pmis_propose end
function _amg_pmis_mark_f end
function _amg_sa_membership end
function _amg_rs_interp_tile end
function _amg_local_sa_header end
function _hybrid_gs_tile! end

# Per-tile kernels (named, so workers resolve them without closure capture).
_amg_residual!(r, b) = (r .= b .- r; nothing)

function _jacobi_smooth_chunk!(u, dinv, Au, b, ω)
    @. u += ω * dinv * (b - Au)
    return nothing
end

function _amg_scale_chunk!(y, dinv, x)
    @. y = dinv * x
    return nothing
end

function _amg_scale_inplace_chunk!(y, dinv)
    y .*= dinv
    return nothing
end

function _cheby_d_update_chunk!(d, work, a, b)
    @. d = a * d + b * work
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

function _amg_scale_vec!(y::DVector, dinv::DVector, x::DVector)
    part = y.partitioning
    maybe_copy_buffered(y => part, dinv => part, x => part) do y, dinv, x
        yc, dc, xc = y.chunks, dinv.chunks, x.chunks
        Dagger.spawn_datadeps() do
            for i in eachindex(yc)
                Dagger.@spawn _amg_scale_chunk!(Out(yc[i]), In(dc[i]), In(xc[i]))
            end
        end
    end
    return y
end

function _amg_scale_inplace!(y::DVector, dinv::DVector)
    part = y.partitioning
    maybe_copy_buffered(y => part, dinv => part) do y, dinv
        yc, dc = y.chunks, dinv.chunks
        Dagger.spawn_datadeps() do
            for i in eachindex(yc)
                Dagger.@spawn _amg_scale_inplace_chunk!(InOut(yc[i]), In(dc[i]))
            end
        end
    end
    return y
end

function _chebyshev_smooth!(u::DVector, A::DMatrix, dinv::DVector, b::DVector,
                            r::DVector, work::DVector, dir::DVector, nsweeps,
                            λ_min::Float64, λ_max::Float64, degree::Int)
    nsweeps <= 0 && return u
    T = eltype(u)
    α = T(λ_min)
    β = T(λ_max)
    β > α || (β = α + one(T))
    c = (β + α) / 2
    dhalf = (β - α) / 2
    σ = c / dhalf
    part = u.partitioning
    deg = max(degree, 1)
    for _ in 1:nsweeps
        LinearAlgebra.mul!(r, A, u)
        maybe_copy_buffered(r => part, b => part) do r, b
            rc, bc = r.chunks, b.chunks
            Dagger.spawn_datadeps() do
                for i in eachindex(rc)
                    Dagger.@spawn _amg_residual!(InOut(rc[i]), In(bc[i]))
                end
            end
        end
        _amg_scale_vec!(dir, dinv, r)
        LinearAlgebra.axpy!(inv(c), dir, u)
        ρ = inv(σ)
        for _ in 2:deg
            LinearAlgebra.mul!(r, A, u)
            maybe_copy_buffered(r => part, b => part) do r, b
                rc, bc = r.chunks, b.chunks
                Dagger.spawn_datadeps() do
                    for i in eachindex(rc)
                        Dagger.@spawn _amg_residual!(InOut(rc[i]), In(bc[i]))
                    end
                end
            end
            _amg_scale_vec!(work, dinv, r)
            ρ_old = ρ
            ρ = inv(2 * σ - ρ)
            a = ρ * ρ_old
            bcoef = 2 * ρ / dhalf
            maybe_copy_buffered(dir => part, work => part) do d, w
                dc, wc = d.chunks, w.chunks
                Dagger.spawn_datadeps() do
                    for i in eachindex(dc)
                        Dagger.@spawn _cheby_d_update_chunk!(InOut(dc[i]), In(wc[i]),
                                                             a, bcoef)
                    end
                end
            end
            LinearAlgebra.axpy!(one(T), dir, u)
        end
    end
    return u
end

function _hybrid_gs_smooth!(u::DVector, A::DMatrix, b::DVector, r::DVector, nsweeps)
    nsweeps <= 0 && return u
    n, Ac, mt, _ = _square_tiled_layout(A)
    n == length(u) || throw(DimensionMismatch(
        "hybrid GS smoother expected length $n, got $(length(u))"))
    part = u.partitioning
    for _ in 1:nsweeps
        LinearAlgebra.mul!(r, A, u)
        maybe_copy_buffered(u => part, r => part, b => part) do u, r, b
            uc, rc, bc = u.chunks, r.chunks, b.chunks
            Dagger.spawn_datadeps() do
                for i in 1:mt
                    Dagger.@spawn _hybrid_gs_tile!(InOut(uc[i]), In(Ac[i, i]),
                                                   In(rc[i]), In(bc[i]))
                end
            end
        end
    end
    return u
end

function _pc_smooth!(u::DVector, A::DMatrix, Pc, b::DVector, r::DVector,
                     work::DVector, nsweeps)
    nsweeps <= 0 && return u
    for _ in 1:nsweeps
        LinearAlgebra.mul!(r, A, u)
        part = r.partitioning
        maybe_copy_buffered(r => part, b => part) do r, b
            rc, bc = r.chunks, b.chunks
            Dagger.spawn_datadeps() do
                for i in eachindex(rc)
                    Dagger.@spawn _amg_residual!(InOut(rc[i]), In(bc[i]))
                end
            end
        end
        LinearAlgebra.mul!(work, Pc, r)
        LinearAlgebra.axpy!(one(eltype(u)), work, u)
    end
    return u
end

function _level_smooth!(u::DVector, L::GlobalAMGLevel, M::GlobalAMG, b::DVector, nsweeps)
    s = M.smoother
    if s === :jacobi || s === :l1jacobi
        return _jacobi_smooth!(u, L.A, L.dinv, b, L.res, M.relax, nsweeps)
    elseif s === :chebyshev
        λ_min, λ_max = L.extra
        return _chebyshev_smooth!(u, L.A, L.dinv, b, L.res, L.work, L.dir, nsweeps,
                                  Float64(λ_min), Float64(λ_max), M.chebyshev_degree)
    elseif s === :hybrid_gs
        return _hybrid_gs_smooth!(u, L.A, b, L.res, nsweeps)
    elseif s === :ilu || s === :ras
        return _pc_smooth!(u, L.A, L.extra, b, L.res, L.work, nsweeps)
    else
        throw(ArgumentError("GlobalAMG: unknown smoother $(s); use :jacobi, \
            :l1jacobi, :chebyshev, :hybrid_gs, :ilu, or :ras"))
    end
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

function _mgcycle!(u::DVector, M::GlobalAMG, b::DVector, ℓ::Int, cycle::Symbol)
    if ℓ > length(M.levels)
        copyto!(u, M.coarse \ b)
        return u
    end
    L = M.levels[ℓ]
    _level_smooth!(u, L, M, b, M.presweeps)
    _amg_restrict_residual!(L.res, L.A, u, b)
    LinearAlgebra.mul!(L.coarse_b, L.P', L.res)
    fill!(L.coarse_x, zero(eltype(L.coarse_x)))
    nL = length(M.levels)
    if cycle === :w && ℓ < nL
        _mgcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1, :w)
        _mgcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1, :w)
    elseif cycle === :f && ℓ < nL
        _mgcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1, :f)
        _mgcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1, :v)
    else
        _mgcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1, :v)
    end
    LinearAlgebra.mul!(L.res, L.P, L.coarse_x)
    LinearAlgebra.axpy!(one(eltype(u)), L.res, u)
    _level_smooth!(u, L, M, b, M.postsweeps)
    return u
end

_vcycle!(u::DVector, M::GlobalAMG, b::DVector, ℓ::Int) =
    _mgcycle!(u, M, b, ℓ, M.cycle)

"""
    mul!(y, M::GlobalAMG, x)

One V-cycle (or W/F): `y ← M⁻¹ x` with a zero initial guess. This is the
Krylov `ldiv=false` apply. Krylov `stats.solved` is the *preconditioned*
residual; check `‖A y − x‖` (un-preconditioned) when `x` is the right-hand
side.
"""
function LinearAlgebra.mul!(y::DVector, M::GlobalAMG, x::DVector)
    length(x) == M.n || throw(DimensionMismatch(
        "GlobalAMG is $(M.n)×$(M.n) but x has length $(length(x))"))
    length(y) == M.n || throw(DimensionMismatch(
        "GlobalAMG is $(M.n)×$(M.n) but y has length $(length(y))"))
    part = M.part
    maybe_copy_buffered(x => part, y => part) do x, y
        fill!(y, zero(eltype(y)))
        _mgcycle!(y, M, x, 1, M.cycle)
    end
    return y
end
