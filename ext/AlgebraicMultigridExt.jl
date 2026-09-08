module AlgebraicMultigridExt

import AlgebraicMultigrid
import SparseArrays
import SparseArrays: SparseMatrixCSC
import Dagger
import Dagger: DMatrix, DVector, Blocks, GlobalAMG, GlobalAMGLevel
import Dagger: AMGTileInterp, AMGTileHeader
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
# Global AMG: tiled interpolation `P`, Galerkin `Ac = P' A P` (distributed),
# V-cycle apply in `src/array/amg.jl`. Per-tile `AMGPreconditioner` above is
# unchanged.
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

# Tiled interpolation: one coarsen task per row-tile (diagonal tile + the
# rest of the row for interface edges). That is not a gather of `A` onto one
# worker. Do not confuse this with wrapping per-tile `AMGPreconditioner`.

function _amg_tile_csc(tile)
    return _as_sparse(Dagger._tile_matrix(tile))
end

function _amg_tile_starts(A::DMatrix)
    sd = A.subdomains
    mt, nt = size(A.chunks)
    row_starts = Vector{Int}(undef, mt)
    col_starts = Vector{Int}(undef, nt)
    for i in 1:mt
        row_starts[i] = first(sd[i, 1].indexes[1])
    end
    for j in 1:nt
        col_starts[j] = first(sd[1, j].indexes[2])
    end
    return row_starts, col_starts
end

function _amg_iface_from_row!(iface_local, iface_nbr, k, diag_j, col_starts, tiles...)
    for j in 1:length(tiles)
        j == diag_j && continue
        Aj = _amg_tile_csc(tiles[j])
        c0 = col_starts[j]
        for col in 1:size(Aj, 2)
            for p in SparseArrays.nzrange(Aj, col)
                r = Aj.rowval[p]
                (r < 1 || r > k) && continue
                Aj.nzval[p] == 0 && continue
                push!(iface_local, r)
                push!(iface_nbr, c0 + col - 1)
            end
        end
    end
    return nothing
end

function _amg_finish_interp(I, J, V, nagg, k, ::Type{T}, diag_j, col_starts, tiles...) where T
    iface_local = Int[]
    iface_nbr = Int[]
    _amg_iface_from_row!(iface_local, iface_nbr, k, diag_j, col_starts, tiles...)
    row_agg = zeros(Int, k)
    for p in eachindex(I)
        r = I[p]
        (1 <= r <= k) && (row_agg[r] = J[p])
    end
    iface_agg = Vector{Int}(undef, length(iface_local))
    for t in eachindex(iface_local)
        loc = iface_local[t]
        iface_agg[t] = (1 <= loc <= k) ? row_agg[loc] : 0
    end
    return AMGTileInterp{T}(I, J, V, nagg, iface_local, iface_nbr, iface_agg)
end

function _amg_row_coarsen_sa(diag_j::Int, col_starts::Vector{Int},
                             strength, aggregate, tiles...)
    Ad = _amg_tile_csc(tiles[diag_j])
    k = size(Ad, 1)
    T = eltype(Ad)
    S, _ = strength(Ad)
    AggOp = aggregate(S)
    nagg = size(AggOp, 1)
    I = Int[]
    J = Int[]
    V = T[]
    if nagg > 0
        Tent, _ = AlgebraicMultigrid.fit_candidates(AggOp, ones(T, k))
        if size(Tent, 2) > 0
            I, J, V = SparseArrays.findnz(Tent)
            nagg = size(Tent, 2)
        else
            nagg = 0
        end
    end
    return _amg_finish_interp(I, J, V, nagg, k, T, diag_j, col_starts, tiles...)
end

function _amg_row_coarsen_rs(diag_j::Int, col_starts::Vector{Int}, extra, tiles...)
    Ad = _amg_tile_csc(tiles[diag_j])
    k = size(Ad, 1)
    T = eltype(Ad)
    I = Int[]
    J = Int[]
    V = T[]
    nagg = 0
    if k <= 1
        nagg = 1
        I = Int[1]
        J = Int[1]
        V = T[one(T)]
    else
        ml = AlgebraicMultigrid.ruge_stuben(Ad; max_levels=2, max_coarse=max(k - 1, 1), extra...)
        if !isempty(ml.levels)
            P = SparseArrays.sparse(ml.levels[1].P)
            if size(P, 2) > 0
                I, J, V = SparseArrays.findnz(P)
                nagg = size(P, 2)
            end
        end
    end
    return _amg_finish_interp(I, J, V, nagg, k, T, diag_j, col_starts, tiles...)
end

# Named so workers resolve it without a closure. `tiles` is one row of `A`
# (rank-uniform: every column tile, including empties).
function Dagger._amg_row_coarsen(method::Symbol, diag_j::Int, col_starts::Vector{Int},
                                 strength, aggregate, extra, tiles...)
    if method === :smoothed_aggregation
        return _amg_row_coarsen_sa(diag_j, col_starts, strength, aggregate, tiles...)
    elseif method === :ruge_stuben
        return _amg_row_coarsen_rs(diag_j, col_starts, extra, tiles...)
    else
        throw(ArgumentError("GlobalAMG: unknown method $(method); use \
            :smoothed_aggregation or :ruge_stuben"))
    end
end

function _amg_find!(parent, x)
    while parent[x] != x
        parent[x] = parent[parent[x]]
        x = parent[x]
    end
    return x
end

function _amg_union!(parent, a, b)
    ra, rb = _amg_find!(parent, a), _amg_find!(parent, b)
    ra == rb && return
    if ra < rb
        parent[rb] = ra
    else
        parent[ra] = rb
    end
    return nothing
end

# Merge local aggregates that share an interface edge. Unassigned interface
# nodes (no local aggregate) are paired or kept as singletons, as before.
function _amg_match_interface(headers, row_starts, ::Type{T}) where T
    mt = length(headers)
    offsets = Vector{Int}(undef, mt)
    nagg_total = 0
    for i in 1:mt
        offsets[i] = nagg_total
        nagg_total += headers[i].nagg
    end

    node_info = Dict{Int,Tuple{Int,Int,Int}}()
    edges = Vector{Tuple{Int,Int}}()
    for i in 1:mt
        h = headers[i]
        r0 = row_starts[i]
        for t in eachindex(h.iface_local)
            loc = h.iface_local[t]
            nbr = Int(h.iface_nbr[t])
            agg = t <= length(h.iface_agg) ? Int(h.iface_agg[t]) : 0
            g = r0 + loc - 1
            node_info[g] = (i, loc, agg)
            lo, hi = minmax(g, nbr)
            push!(edges, (lo, hi))
        end
    end
    unique!(sort!(edges))

    unassigned = sort!(Int[g for (g, info) in node_info if info[3] == 0])
    extra_id = Dict{Int,Int}()
    nids = nagg_total
    for g in unassigned
        nids += 1
        extra_id[g] = nids
    end
    nids == 0 && return ([Int[] for _ in 1:mt],
                         [Vector{Tuple{Int,Int,T}}() for _ in 1:mt], 0)

    parent = collect(1:nids)
    matched = falses(nids)
    node_id = function (g)
        t, _, agg = node_info[g]
        return agg > 0 ? (offsets[t] + agg) : extra_id[g]
    end
    # Pairwise: each aggregate merges at most once. Unrestricted union-find
    # along a 1-D interface chain collapses into one domain-wide coarse
    # variable when a tile's two ends share an aggregate (seen as V-cycle
    # residuals of O(1)–O(5) vs Jacobi).
    for (a, b) in edges
        (haskey(node_info, a) && haskey(node_info, b)) || continue
        ia, ib = node_id(a), node_id(b)
        (matched[ia] || matched[ib] || ia == ib) && continue
        _amg_union!(parent, ia, ib)
        matched[ia] = matched[ib] = true
    end

    roots = Vector{Int}(undef, nids)
    for i in 1:nids
        roots[i] = _amg_find!(parent, i)
    end
    uroots = sort!(unique!(roots))
    col_of_root = Dict{Int,Int}(r => i for (i, r) in enumerate(uroots))
    nc = length(uroots)

    colmaps = [zeros(Int, headers[i].nagg) for i in 1:mt]
    for i in 1:mt
        for a in 1:headers[i].nagg
            colmaps[i][a] = col_of_root[_amg_find!(parent, offsets[i] + a)]
        end
    end

    matches = [Vector{Tuple{Int,Int,T}}() for _ in 1:mt]
    w2 = T(1 / sqrt(2))
    for g in unassigned
        t, loc, _ = node_info[g]
        root = _amg_find!(parent, extra_id[g])
        w = root <= nagg_total ? one(T) : w2
        push!(matches[t], (loc, col_of_root[root], w))
    end
    return colmaps, matches, nc
end

# Local-index CSC for one `P` tile. `match` is `(local_row, global_col, weight)`.
# `colmap[local_agg]` is the global coarse column after interface merges.
function Dagger._amg_fill_p_tile(interp::AMGTileInterp{T}, match, colmap,
                                 col0::Int, ncols::Int, tm::Int, tn::Int) where T
    I = Int[]
    J = Int[]
    V = T[]
    for p in eachindex(interp.I)
        gcol = colmap[interp.J[p]]
        if col0 <= gcol < col0 + ncols
            push!(I, interp.I[p])
            push!(J, gcol - col0 + 1)
            push!(V, interp.V[p])
        end
    end
    for (loc, gcol, w) in match
        if col0 <= gcol < col0 + ncols
            push!(I, loc)
            push!(J, gcol - col0 + 1)
            push!(V, T(w))
        end
    end
    S = SparseArrays.sparse(I, J, V, tm, tn)
    return Dagger._store_assembled_tile(S)
end

function _amg_replace_chunks(A::DMatrix{T}, new_chunks) where T
    return Dagger.DArray(T, A.domain, A.subdomains, new_chunks, A.partitioning, A.concat)
end

function _amg_assemble_p(A::DMatrix{T}, interp_tasks, colmaps, matches, nc) where T
    n = size(A, 1)
    k = Int(A.partitioning.blocksize[1])
    TT = Dagger.is_sparse_backed(A) ? Dagger.darray_tiletype(A) : Dagger.DSparseArray{T,2}
    P0 = Dagger.allocate_tiled(TT, T, Blocks(k, k), (n, nc))
    mt, nt = size(P0.chunks)
    col0s = Vector{Int}(undef, nt)
    colns = Vector{Int}(undef, nt)
    for j in 1:nt
        cr = P0.subdomains[1, j].indexes[2]
        col0s[j] = first(cr)
        colns[j] = length(cr)
    end
    new_chunks = Matrix{Dagger.DTask}(undef, mt, nt)
    for i in 1:mt, j in 1:nt
        tm = length(P0.subdomains[i, j].indexes[1])
        tn = length(P0.subdomains[i, j].indexes[2])
        new_chunks[i, j] = Dagger.@spawn return_type=Dagger.DSparseArray{T,2} Dagger._amg_fill_p_tile(
            interp_tasks[i], matches[i], colmaps[i], col0s[j], colns[j], tm, tn)
    end
    return _amg_replace_chunks(P0, new_chunks)
end

# LocalWeighting Jacobi prolongation: `P = T − ω D⁻¹ A T` with
# `Dᵢᵢ = ‖row i of A‖₁`, matching AlgebraicMultigrid.jl's `JacobiProlongation`.
function Dagger._amg_row_abs_inv_chunk(tiles...)
    S0 = _amg_tile_csc(tiles[1])
    k = size(S0, 1)
    T = eltype(S0)
    s = zeros(T, k)
    for tile in tiles
        S = _amg_tile_csc(tile)
        size(S, 1) == k || throw(DimensionMismatch(
            "row-tile heights differ ($(size(S, 1)) vs $k)"))
        for col in 1:size(S, 2)
            for p in SparseArrays.nzrange(S, col)
                r = S.rowval[p]
                (1 <= r <= k) && (s[r] += abs(S.nzval[p]))
            end
        end
    end
    out = similar(s)
    @inbounds for i in 1:k
        out[i] = s[i] == 0 ? zero(T) : inv(s[i])
    end
    return out
end

function _amg_row_abs_inv(A::DMatrix{T}) where T
    n = size(A, 1)
    k = Int(A.partitioning.blocksize[1])
    mt, nt = size(A.chunks)
    d0 = DVector{T}(undef, Blocks(k), n)
    new_chunks = Vector{Dagger.DTask}(undef, mt)
    for i in 1:mt
        new_chunks[i] = Dagger.@spawn Dagger._amg_row_abs_inv_chunk((A.chunks[i, j] for j in 1:nt)...)
    end
    return Dagger.DArray(T, d0.domain, d0.subdomains, new_chunks, d0.partitioning, d0.concat)
end

function Dagger._amg_smooth_p_tile(tent, ap, dscale, ω)
    Tm = _amg_tile_csc(tent)
    Am = copy(_amg_tile_csc(ap))
    ωT = eltype(Am)(ω)
    ds = Vector{eltype(Am)}(dscale)
    for col in 1:size(Am, 2)
        for p in SparseArrays.nzrange(Am, col)
            r = Am.rowval[p]
            Am.nzval[p] *= ωT * ds[r]
        end
    end
    return Dagger._store_assembled_tile(Tm - Am)
end

function _amg_smooth_prolongation(A::DMatrix{T}, Tent::DMatrix{T}, ω) where T
    n, nc = size(Tent)
    k = Int(A.partitioning.blocksize[1])
    kc = Int(Tent.partitioning.blocksize[2])
    TT = Dagger.darray_tiletype(Tent)
    AP = Dagger.allocate_tiled(TT, T, Blocks(k, kc), (n, nc))
    LinearAlgebra.mul!(AP, A, Tent)
    dscale = _amg_row_abs_inv(A)
    P0 = Dagger.allocate_tiled(TT, T, Blocks(k, kc), (n, nc))
    mt, nt = size(P0.chunks)
    new_chunks = Matrix{Dagger.DTask}(undef, mt, nt)
    for i in 1:mt, j in 1:nt
        new_chunks[i, j] = Dagger.@spawn return_type=Dagger.DSparseArray{T,2} Dagger._amg_smooth_p_tile(
            Tent.chunks[i, j], AP.chunks[i, j], dscale.chunks[i], ω)
    end
    return _amg_replace_chunks(P0, new_chunks)
end

function _amg_prolongation(A::DMatrix{T}; method::Symbol, smooth::Bool,
                           jacobi_ω=4 / 3, kwargs...) where T
    method === :smoothed_aggregation || method === :ruge_stuben || throw(ArgumentError(
        "GlobalAMG: unknown method $(method); use :smoothed_aggregation or :ruge_stuben"))
    extra = _passthrough_kwargs(; kwargs...)
    strength = get(extra, :strength, AlgebraicMultigrid.SymmetricStrength())
    aggregate = get(extra, :aggregate, AlgebraicMultigrid.StandardAggregation())
    n = size(A, 1)
    mt, nt = size(A.chunks)
    row_starts, col_starts = _amg_tile_starts(A)
    interp_tasks = Vector{Dagger.DTask}(undef, mt)
    for i in 1:mt
        interp_tasks[i] = Dagger.@spawn return_type=Dagger.AMGTileInterp{T} Dagger._amg_row_coarsen(
            method, i, col_starts, strength, aggregate, extra,
            (A.chunks[i, j] for j in 1:nt)...)
    end
    headers = Vector{AMGTileHeader}(undef, mt)
    for i in 1:mt
        headers[i] = fetch(Dagger.@spawn return_type=AMGTileHeader Dagger._amg_interp_header(interp_tasks[i]))
    end
    colmaps, matches, nc = _amg_match_interface(headers, row_starts, T)
    (nc == 0 || nc >= n) && return nothing
    Tent = _amg_assemble_p(A, interp_tasks, colmaps, matches, nc)
    if !smooth
        return Tent
    end
    return _amg_smooth_prolongation(A, Tent, jacobi_ω)
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
        # A later tiled coarsening on a handful of tiles sees only the
        # diagonal blocks of an already-denser RAP product; that P can make
        # the V-cycle worse than Jacobi (lesson 32 / 35). Stop and LU.
        if !isempty(levels) && size(A.chunks, 1) <= 3
            break
        end
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
