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
# are distributed. The coarsest solve is a gathered LU. On GPU tiles, setup
# host-stages each tile inside a device `ExactScope`; V-cycle vectors stay
# in VRAM (lesson 52). Do not treat Krylov `stats.solved` as `Ax ≈ b`;
# check the un-preconditioned residual.

"""
    GlobalAMGLevel

One level of a [`GlobalAMG`](@ref) hierarchy: the level operator `A`, the
prolongation `P`, an optional restriction `R` (`nothing` means `P'`), a
Jacobi-family `dinv`, apply workspaces, and optional smoother state
(`extra`: Chebyshev bounds, a level ILU/RAS preconditioner, or an FSAI
`DMatrix`).
"""
struct GlobalAMGLevel{TA,TP,TR,V,S}
    A::TA
    P::TP
    R::TR
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
  (existing [`AdditiveSchwarzPreconditioner`](@ref); default overlap 0),
  `:fsai` (factorized sparse approximate inverse of each diagonal tile;
  apply is `G'G`, not a new solver type).
- `relax=2/3`, `presweeps=2`, `postsweeps=2` — Jacobi-family damping and
  sweep counts. One Jacobi sweep each side is not enough for the coarse
  correction to beat Jacobi-only on 1-D Poisson; two is the smallest count
  that does.
- `cycle=:v` — `:v`, `:w`, `:f`, `:additive` (fine smoother plus a
  `1/n`-damped coarsest solve of the *original* residual), or
  `:multadditive` (pre-smooth, damped coarsest of the *updated*
  residual, post-smooth). Unscaled `P (Ac \\ R b)` overshoots on 1-D
  Poisson (‖r‖/‖b‖ ~ 8 vs Jacobi ~ 0.98); a residual line search would
  make `mul!` nonlinear. Prefer `:v` for Krylov.
- `coarsen=:hmis` — `:hmis` (default), `:pmis`, `:standard`, `:falgout`
  (local RS, then CLJP on the interface), `:cljp` (measure-weighted
  independent set), `:cgc` (compatible-relaxation C-points), or
  `:aggressive` (PMIS on the distance-2 strength graph). Full PMIS / CLJP
  / aggressive can lose to Jacobi on 1-D Poisson n=128; they stay opt-in
  unless a residual gate says otherwise.
- `interp=:sa` — SA tentative + smooth; RS default is `:direct` (classical
  distance-1). Also `:extended`, `:exti` (`:extended_i` / `Symbol("ext+i")`),
  `:ff`, `:multipass`, `:air`. AIR stores a distributed restriction `R`
  (one-point approximate ideal restriction); it does not collect fine `A`.
  Classical distance-2 interpolants use a compact C-neighbor map, not a
  gather of `A`.
- `pmax=0`, `trunc_factor=0`, `coarse_drop=0` — first-class sparsity knobs.
  `pmax` keeps that many largest entries per row of `P`; `trunc_factor`
  drops `|p_ij| < θ max_k |p_ik|`; `coarse_drop` drops small off-diagonals
  of each Galerkin `Ac`. Zero means off (no silent HYPRE default).

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
    interp::Symbol
    pmax::Int
    trunc_factor::Float64
    coarse_drop::Float64
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
          ", coarsen=", M.coarsen,
          ", interp=", M.interp,
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
function _amg_cljp_measure end
function _amg_strong_nbrs end
function _amg_cneigh_frag end
function _amg_extended_interp_tile end
function _amg_injection_tile end
function _amg_air_r_tile end
function _amg_fill_r_tile end
function _amg_local_rs_header end
function _amg_zero_c_chunk! end
function _amg_fsai_tile end
function _amg_trunc_p_row end
function _amg_drop_ac_row end
function _amg_store_sparse_tile end

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
                    Dagger.@spawn compute_scope=_tile_scope(uc[i]) _jacobi_smooth_chunk!(
                        InOut(uc[i]), In(dc[i]), In(rc[i]), In(bc[i]), ωT)
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
                Dagger.@spawn compute_scope=_tile_scope(yc[i]) _amg_scale_chunk!(
                    Out(yc[i]), In(dc[i]), In(xc[i]))
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
                Dagger.@spawn compute_scope=_tile_scope(yc[i]) _amg_scale_inplace_chunk!(
                    InOut(yc[i]), In(dc[i]))
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
                    Dagger.@spawn compute_scope=_tile_scope(rc[i]) _amg_residual!(
                        InOut(rc[i]), In(bc[i]))
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
                        Dagger.@spawn compute_scope=_tile_scope(rc[i]) _amg_residual!(
                            InOut(rc[i]), In(bc[i]))
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
                        Dagger.@spawn compute_scope=_tile_scope(dc[i]) _cheby_d_update_chunk!(
                            InOut(dc[i]), In(wc[i]), a, bcoef)
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
                    Dagger.@spawn compute_scope=_tile_scope(uc[i]) _hybrid_gs_tile!(
                        InOut(uc[i]), In(Ac[i, i]), In(rc[i]), In(bc[i]))
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
                    Dagger.@spawn compute_scope=_tile_scope(rc[i]) _amg_residual!(
                        InOut(rc[i]), In(bc[i]))
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
    elseif s === :fsai
        return _fsai_smooth!(u, L.A, L.extra, b, L.res, L.work, L.dir, nsweeps)
    else
        throw(ArgumentError("GlobalAMG: unknown smoother $(s); use :jacobi, \
            :l1jacobi, :chebyshev, :hybrid_gs, :ilu, :ras, or :fsai"))
    end
end

function _fsai_smooth!(u::DVector, A::DMatrix, G, b::DVector, r::DVector,
                       work::DVector, dir::DVector, nsweeps)
    nsweeps <= 0 && return u
    for _ in 1:nsweeps
        _amg_restrict_residual!(r, A, u, b)
        LinearAlgebra.mul!(work, G, r)
        LinearAlgebra.mul!(dir, G', work)
        LinearAlgebra.axpy!(one(eltype(u)), dir, u)
    end
    return u
end

function _amg_restrict_apply!(dest::DVector, L::GlobalAMGLevel, src::DVector)
    if L.R === nothing
        LinearAlgebra.mul!(dest, L.P', src)
    else
        LinearAlgebra.mul!(dest, L.R, src)
    end
    return dest
end

function _amg_restrict_residual!(res::DVector, A::DMatrix, u::DVector, b::DVector)
    LinearAlgebra.mul!(res, A, u)
    part = res.partitioning
    maybe_copy_buffered(res => part, b => part) do res, b
        rc, bc = res.chunks, b.chunks
        Dagger.spawn_datadeps() do
            for i in eachindex(rc)
                Dagger.@spawn compute_scope=_tile_scope(rc[i]) _amg_residual!(
                    InOut(rc[i]), In(bc[i]))
            end
        end
    end
    return res
end

function _mgcycle!(u::DVector, M::GlobalAMG, b::DVector, ℓ::Int, cycle::Symbol)
    if cycle === :additive && ℓ == 1
        return _additive_cycle!(u, M, b)
    elseif cycle === :multadditive && ℓ == 1
        return _multadditive_cycle!(u, M, b)
    end
    if ℓ > length(M.levels)
        copyto!(u, M.coarse \ b)
        return u
    end
    L = M.levels[ℓ]
    _level_smooth!(u, L, M, b, M.presweeps)
    _amg_restrict_residual!(L.res, L.A, u, b)
    _amg_restrict_apply!(L.coarse_b, L, L.res)
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

# Nested restriction of `src` from level `ℓ0` downward into each `coarse_b`.
function _amg_restrict_nested!(M::GlobalAMG, src::DVector, ℓ0::Int)
    nL = length(M.levels)
    ℓ0 > nL && return
    _amg_restrict_apply!(M.levels[ℓ0].coarse_b, M.levels[ℓ0], src)
    for ℓ in (ℓ0 + 1):nL
        _amg_restrict_apply!(M.levels[ℓ].coarse_b, M.levels[ℓ],
                             M.levels[ℓ - 1].coarse_b)
    end
    return
end

# Prolong the coarsest correction (`levels[end].coarse_x`) to level `ℓ0`
# into `dest` (overwrite). Intermediate `coarse_x` / `res` are scratch.
function _amg_store_coarse_into!(dest::DVector, M::GlobalAMG, ℓ0::Int)
    nL = length(M.levels)
    ℓ0 > nL && return dest
    for ℓ in nL:-1:(ℓ0 + 1)
        LinearAlgebra.mul!(M.levels[ℓ].res, M.levels[ℓ].P, M.levels[ℓ].coarse_x)
        copyto!(M.levels[ℓ - 1].coarse_x, M.levels[ℓ].res)
    end
    LinearAlgebra.mul!(dest, M.levels[ℓ0].P, M.levels[ℓ0].coarse_x)
    return dest
end

# Fixed linear damping for the coarsest additive correction. Unscaled
# `P (Ac \ R b)` is O(10³) on 1-D Poisson (‖r‖/‖b‖ ~ 8). A residual
# line search would fix that *and* make `mul!` nonlinear (GMRES
# stagnates). `1/n` matches the 1-D n=64 residual-optimal scale (~0.013).
_amg_additive_ω(M::GlobalAMG, ::Type{T}) where T = inv(T(M.n))

# Additive AMG: independent fine smoother (original residual) plus a
# damped coarsest solve of `R b`. Linear in `b`. Not a V-cycle.
function _additive_cycle!(u::DVector, M::GlobalAMG, b::DVector)
    nL = length(M.levels)
    fill!(u, zero(eltype(u)))
    if nL == 0
        copyto!(u, M.coarse \ b)
        return u
    end
    L1 = M.levels[1]
    T = eltype(u)
    _level_smooth!(u, L1, M, b, M.presweeps + M.postsweeps)
    _amg_restrict_nested!(M, b, 1)
    copyto!(M.levels[end].coarse_x, M.coarse \ M.levels[end].coarse_b)
    _amg_store_coarse_into!(L1.dir, M, 1)
    LinearAlgebra.axpy!(_amg_additive_ω(M, T), L1.dir, u)
    return u
end

# Mult-additive: multiplicative pre-smooth, damped coarsest solve of the
# *updated* residual, then post-smooth. Intermediate levels stay additive
# (no residual update on the way down). Linear in `b`.
function _multadditive_cycle!(u::DVector, M::GlobalAMG, b::DVector)
    nL = length(M.levels)
    fill!(u, zero(eltype(u)))
    if nL == 0
        copyto!(u, M.coarse \ b)
        return u
    end
    L1 = M.levels[1]
    T = eltype(u)
    _level_smooth!(u, L1, M, b, M.presweeps)
    _amg_restrict_residual!(L1.res, L1.A, u, b)
    _amg_restrict_nested!(M, L1.res, 1)
    copyto!(M.levels[end].coarse_x, M.coarse \ M.levels[end].coarse_b)
    _amg_store_coarse_into!(L1.dir, M, 1)
    LinearAlgebra.axpy!(_amg_additive_ω(M, T), L1.dir, u)
    _level_smooth!(u, L1, M, b, M.postsweeps)
    return u
end

_vcycle!(u::DVector, M::GlobalAMG, b::DVector, ℓ::Int) =
    _mgcycle!(u, M, b, ℓ, M.cycle)

"""
    mul!(y, M::GlobalAMG, x)

One V-cycle (or W/F/additive/mult-additive): `y ← M⁻¹ x` with a zero
initial guess. This is the Krylov `ldiv=false` apply. Krylov
`stats.solved` is the *preconditioned* residual; check `‖A y − x‖`
(un-preconditioned) when `x` is the right-hand side.
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
