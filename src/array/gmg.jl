# Geometric multigrid (HYPRE PFMG/SMG analog, first cut).
#
# `jps/sparse-stencil` is a same-index, same-size, same-chunk `@stencil` with
# halo exchange. Restriction / prolongation map a fine grid of size `n` onto a
# coarse grid of size `n/2` — a different index space — so they cannot be
# written as `@stencil` (neighborhood access must use the same `idx`, and
# operands must share size and chunk layout). Do not grow a competing stencil
# stack for that gap. This file is matrix-based: injection / full-weighting
# `R` and linear / bilinear `P` are sparse `DMatrix`s, the coarse operator is
# the distributed Galerkin product `Ac = R A P`, and the V-cycle apply is
# `mul!(y, M, x)` (Krylov `ldiv=false`) with the same damped-Jacobi smoother
# as [`GlobalAMG`](@ref).
#
# `AMGPreconditioner` and `GlobalAMG` semantics are unchanged. This is the
# structured-grid counterpart of `GlobalAMG` (geometric `P`, not aggregation).
# Check the un-preconditioned residual; do not treat Krylov `stats.solved`
# as `Ax ≈ b` (lessons 19 / 32).

"""
    GeometricMGLevel

One level of a [`GeometricMultigrid`](@ref) hierarchy: the level operator `A`,
restriction `R`, prolongation `P`, a Jacobi `dinv`, and apply workspaces.
"""
struct GeometricMGLevel{TA,TR,TP,V}
    A::TA
    R::TR
    P::TP
    dinv::V
    res::V
    coarse_x::V
    coarse_b::V
end

"""
    GeometricMultigrid(A::DMatrix; grid=nothing, restriction=:full_weighting,
                       prolongation=nothing, kwargs...)
    GeometricMultigrid(A::DMatrix, R, P; kwargs...)

A **geometric** multigrid preconditioner for a regular-grid operator stored as
a `DMatrix`. Unlike [`GlobalAMG`](@ref) (aggregation / classical interpolation
from the matrix graph) this builds injection or full-weighting restriction and
linear / bilinear prolongation from the grid dimensions, forms each coarse
operator `Ac = R A P` by distributed sparse matmul, and applies a V-cycle via
`mul!(y, M, x)` (`y ← M⁻¹ x`, Krylov `ldiv=false`).

There is no Base / LinearAlgebra generic for geometric-MG setup, and
AlgebraicMultigrid.jl is algebraic-only, so this name is unavoidable — the
same class of constructor as [`GlobalAMG`](@ref).

`grid` is the structured-grid shape whose product is `size(A, 1)`. Omit it
(or pass `(n,)`) for a 1-D chain; pass `(nx, ny)` for a 2-D lattice stored
column-major (`i + (j-1)*nx`). Coarsening is standard even reduction
(`n → n÷2` per dimension).

`restriction` is `:full_weighting` (default; 1-D `(1,2,1)/4`, 2-D 9-point) or
`:injection`, or a user-supplied `AbstractMatrix` / `DMatrix` used on the
finest transfer only. `prolongation` defaults to `:linear` (1-D) or
`:bilinear` (2-D); `:injection` and a user-supplied matrix are also accepted.
Remaining levels after a user-supplied finest transfer fall back to the
matching geometric operators.

Keyword arguments:

- `max_levels=3`, `max_coarse=32` — stop after this many grids, or when the
  operator is this small (then a gathered LU is the coarse solver).
- `relax=2/3`, `presweeps=2`, `postsweeps=2` — damped-Jacobi V-cycle sweeps.
  One sweep each side can lose to Jacobi-only on 1-D Poisson; two is the
  smallest count that contributes a real coarse correction (lesson 32).

Requires `SparseArrays` (transfer assembly). The smoother is damped Jacobi
through existing `mul!`, not `@stencil`: the sparse-stencil branch cannot
address a coarse grid.

See also [`GlobalAMG`](@ref).
"""
struct GeometricMultigrid{L,C,A} <: AbstractDaggerPreconditioner
    levels::L
    coarse::C
    coarse_A::A
    relax::Float64
    presweeps::Int
    postsweeps::Int
    n::Int
    part::Blocks{1}
    grid::Dims
    restriction::Symbol
    prolongation::Symbol
end

GeometricMultigrid(A; kwargs...) = throw(ArgumentError(
    "Dagger.GeometricMultigrid requires SparseArrays.jl. Run `using SparseArrays` \
    to enable geometric-multigrid preconditioning."))

GeometricMultigrid(A, R, P; kwargs...) = GeometricMultigrid(A; restriction=R, prolongation=P, kwargs...)

function Base.show(io::IO, M::GeometricMultigrid)
    print(io, "GeometricMultigrid(grid=", M.grid,
          ", restriction=", M.restriction,
          ", prolongation=", M.prolongation,
          ", levels=", length(M.levels) + 1,
          ", n=", M.n, ")")
end

# ---------------------------------------------------------------------------
# Grid helpers and transfer COO (no SparseArrays — assembly lives in the ext).
# ---------------------------------------------------------------------------

function _gmg_normalize_grid(n::Integer, grid)
    if grid === nothing
        return (Int(n),)
    end
    g = ntuple(i -> Int(grid[i]), length(grid))
    length(g) <= 2 || throw(ArgumentError(
        "GeometricMultigrid supports 1-D and 2-D grids, got ndims=$(length(g))"))
    prod(g) == n || throw(DimensionMismatch(
        "grid $g has $(prod(g)) points but A is $(n)×$(n)"))
    return g
end

_gmg_coarse_grid(grid::Dims) = map(d -> d ÷ 2, grid)
_gmg_can_coarsen(grid::Dims, max_coarse::Integer) =
    prod(grid) > max_coarse && all(d -> d >= 2, grid)

_gmg_default_prolongation(grid::Dims) = length(grid) == 1 ? :linear : :bilinear

_gmg_kind_symbol(x::Symbol) = x
_gmg_kind_symbol(::Any) = :user

function _gmg_restriction_coo(::Type{T}, grid, kind::Symbol) where T
    kind === :full_weighting && return _gmg_restriction_coo(T, grid, Val(:full_weighting))
    kind === :injection && return _gmg_restriction_coo(T, grid, Val(:injection))
    throw(ArgumentError(
        "restriction must be :full_weighting or :injection, got $(repr(kind))"))
end

function _gmg_prolongation_coo(::Type{T}, grid, kind::Symbol) where T
    if kind === :linear || kind === :bilinear
        return length(grid) == 1 ?
            _gmg_prolongation_coo(T, grid, Val(:linear)) :
            _gmg_prolongation_coo(T, grid, Val(:bilinear))
    elseif kind === :injection
        return _gmg_prolongation_coo(T, grid, Val(:injection))
    end
    throw(ArgumentError(
        "prolongation must be :linear, :bilinear, or :injection, got $(repr(kind))"))
end

function _gmg_restriction_coo(::Type{T}, grid::Dims{1}, ::Val{:full_weighting}) where T
    n = grid[1]
    nc = n ÷ 2
    I = Int[]; J = Int[]; V = T[]
    sizehint!(I, 3nc); sizehint!(J, 3nc); sizehint!(V, 3nc)
    half = T(1) / T(2)
    quarter = T(1) / T(4)
    for i in 1:nc
        c = 2 * i
        if c - 1 >= 1
            push!(I, i); push!(J, c - 1); push!(V, quarter)
        end
        push!(I, i); push!(J, c); push!(V, half)
        if c + 1 <= n
            push!(I, i); push!(J, c + 1); push!(V, quarter)
        end
    end
    return I, J, V, nc, n
end

function _gmg_restriction_coo(::Type{T}, grid::Dims{1}, ::Val{:injection}) where T
    n = grid[1]
    nc = n ÷ 2
    I = collect(1:nc)
    J = 2 .* I
    V = ones(T, nc)
    return I, J, V, nc, n
end

function _gmg_restriction_coo(::Type{T}, grid::Dims{2}, ::Val{:full_weighting}) where T
    nx, ny = grid
    nxc, nyc = nx ÷ 2, ny ÷ 2
    n = nx * ny
    nc = nxc * nyc
    weights = (
        (-1, -1, T(1) / T(16)), (0, -1, T(1) / T(8)), (1, -1, T(1) / T(16)),
        (-1,  0, T(1) / T(8)),  (0,  0, T(1) / T(4)), (1,  0, T(1) / T(8)),
        (-1,  1, T(1) / T(16)), (0,  1, T(1) / T(8)), (1,  1, T(1) / T(16)),
    )
    I = Int[]; J = Int[]; V = T[]
    sizehint!(I, 9nc); sizehint!(J, 9nc); sizehint!(V, 9nc)
    for jc in 1:nyc, ic in 1:nxc
        i_c = ic + (jc - 1) * nxc
        fi, fj = 2 * ic, 2 * jc
        for (di, dj, w) in weights
            i = fi + di
            j = fj + dj
            if 1 <= i <= nx && 1 <= j <= ny
                push!(I, i_c)
                push!(J, i + (j - 1) * nx)
                push!(V, w)
            end
        end
    end
    return I, J, V, nc, n
end

function _gmg_restriction_coo(::Type{T}, grid::Dims{2}, ::Val{:injection}) where T
    nx, ny = grid
    nxc, nyc = nx ÷ 2, ny ÷ 2
    n = nx * ny
    nc = nxc * nyc
    I = Vector{Int}(undef, nc)
    J = Vector{Int}(undef, nc)
    V = ones(T, nc)
    p = 0
    for jc in 1:nyc, ic in 1:nxc
        p += 1
        I[p] = ic + (jc - 1) * nxc
        J[p] = (2 * ic) + (2 * jc - 1) * nx
    end
    return I, J, V, nc, n
end

function _gmg_prolongation_coo(::Type{T}, grid::Dims{1}, ::Val{:linear}) where T
    n = grid[1]
    nc = n ÷ 2
    I = Int[]; J = Int[]; V = T[]
    sizehint!(I, 3nc); sizehint!(J, 3nc); sizehint!(V, 3nc)
    half = T(1) / T(2)
    for i in 1:nc
        c = 2 * i
        if c - 1 >= 1
            push!(I, c - 1); push!(J, i); push!(V, half)
        end
        push!(I, c); push!(J, i); push!(V, one(T))
        if c + 1 <= n
            push!(I, c + 1); push!(J, i); push!(V, half)
        end
    end
    return I, J, V, n, nc
end

_gmg_prolongation_coo(::Type{T}, grid::Dims{1}, ::Val{:bilinear}) where T =
    _gmg_prolongation_coo(T, grid, Val(:linear))

function _gmg_prolongation_coo(::Type{T}, grid::Dims{1}, ::Val{:injection}) where T
    I, J, V, nc, n = _gmg_restriction_coo(T, grid, Val(:injection))
    return J, I, V, n, nc
end

function _gmg_prolongation_coo(::Type{T}, grid::Dims{2}, ::Val{:bilinear}) where T
    nx, ny = grid
    nxc, nyc = nx ÷ 2, ny ÷ 2
    n = nx * ny
    nc = nxc * nyc
    weights = (
        (-1, -1, T(1) / T(4)), (0, -1, T(1) / T(2)), (1, -1, T(1) / T(4)),
        (-1,  0, T(1) / T(2)), (0,  0, one(T)),      (1,  0, T(1) / T(2)),
        (-1,  1, T(1) / T(4)), (0,  1, T(1) / T(2)), (1,  1, T(1) / T(4)),
    )
    I = Int[]; J = Int[]; V = T[]
    sizehint!(I, 9nc); sizehint!(J, 9nc); sizehint!(V, 9nc)
    for jc in 1:nyc, ic in 1:nxc
        j_c = ic + (jc - 1) * nxc
        fi, fj = 2 * ic, 2 * jc
        for (di, dj, w) in weights
            i = fi + di
            j = fj + dj
            if 1 <= i <= nx && 1 <= j <= ny
                push!(I, i + (j - 1) * nx)
                push!(J, j_c)
                push!(V, w)
            end
        end
    end
    return I, J, V, n, nc
end

_gmg_prolongation_coo(::Type{T}, grid::Dims{2}, ::Val{:linear}) where T =
    _gmg_prolongation_coo(T, grid, Val(:bilinear))

function _gmg_prolongation_coo(::Type{T}, grid::Dims{2}, ::Val{:injection}) where T
    I, J, V, nc, n = _gmg_restriction_coo(T, grid, Val(:injection))
    return J, I, V, n, nc
end

# ---------------------------------------------------------------------------
# V-cycle. Jacobi / residual helpers are shared with GlobalAMG; restriction
# here is the stored `R` (not necessarily `P'`).
# ---------------------------------------------------------------------------

function _gmg_vcycle!(u::DVector, M::GeometricMultigrid, b::DVector, ℓ::Int)
    if ℓ > length(M.levels)
        copyto!(u, M.coarse \ b)
        return u
    end
    L = M.levels[ℓ]
    _jacobi_smooth!(u, L.A, L.dinv, b, L.res, M.relax, M.presweeps)
    _amg_restrict_residual!(L.res, L.A, u, b)
    LinearAlgebra.mul!(L.coarse_b, L.R, L.res)
    fill!(L.coarse_x, zero(eltype(L.coarse_x)))
    _gmg_vcycle!(L.coarse_x, M, L.coarse_b, ℓ + 1)
    LinearAlgebra.mul!(L.res, L.P, L.coarse_x)
    LinearAlgebra.axpy!(one(eltype(u)), L.res, u)
    _jacobi_smooth!(u, L.A, L.dinv, b, L.res, M.relax, M.postsweeps)
    return u
end

"""
    mul!(y, M::GeometricMultigrid, x)

One V-cycle: `y ← M⁻¹ x` with a zero initial guess. Krylov `ldiv=false` apply.
Check `‖A y − x‖` (un-preconditioned) when `x` is the right-hand side; do not
treat `stats.solved` as `Ax ≈ b`.
"""
function LinearAlgebra.mul!(y::DVector, M::GeometricMultigrid, x::DVector)
    length(x) == M.n || throw(DimensionMismatch(
        "GeometricMultigrid is $(M.n)×$(M.n) but x has length $(length(x))"))
    length(y) == M.n || throw(DimensionMismatch(
        "GeometricMultigrid is $(M.n)×$(M.n) but y has length $(length(y))"))
    part = M.part
    maybe_copy_buffered(x => part, y => part) do x, y
        fill!(y, zero(eltype(y)))
        _gmg_vcycle!(y, M, x, 1)
    end
    return y
end
