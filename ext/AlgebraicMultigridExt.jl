module AlgebraicMultigridExt

import AlgebraicMultigrid
import SparseArrays
import SparseArrays: SparseMatrixCSC
import Dagger
import Dagger: DMatrix, DVector, Blocks, GlobalAMG, GlobalAMGLevel, Projected
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
# unchanged. `nullspace=N` still gathers `N` with `A` (lesson 39).
# ---------------------------------------------------------------------------

const _HIERARCHY_KW = (
    :method, :smooth, :max_levels, :max_coarse, :relax, :presweeps, :postsweeps,
    :jacobi_ω, :nullspace, :B,
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

function _pick_nullspace(nullspace, B)
    if nullspace !== nothing && B !== nothing && nullspace !== B
        throw(ArgumentError("GlobalAMG: pass only one of nullspace= or B= \
            (B is the AlgebraicMultigrid.jl name for the same candidates)"))
    end
    return nullspace !== nothing ? nullspace : B
end

_as_candidates(N::DVector, T) = convert(Vector{T}, collect(N)), 1
function _as_candidates(N::DMatrix, T)
    M = convert(Matrix{T}, collect(N))
    return M, size(M, 2)
end
_as_candidates(N::AbstractVector, T) = convert(Vector{T}, collect(N)), 1
function _as_candidates(N::AbstractMatrix, T)
    M = convert(Matrix{T}, Matrix(N))
    return M, size(M, 2)
end

function _host_candidates(n::Int, T, N)
    if N === nothing
        return ones(T, n), 1, false
    end
    B, nmodes = _as_candidates(N, T)
    size(B, 1) == n || throw(DimensionMismatch(
        "nullspace has $(size(B, 1)) rows but the operator is $(n)×$(n)"))
    nmodes >= 1 || throw(ArgumentError("nullspace must have at least one column"))
    return B, nmodes, true
end

# Strength + StandardAggregation (or classical interpolation) on a gathered
# CSC. Used only when `nullspace=N` injects candidates (lesson 39): coarse
# levels need the `R` from `fit_candidates`, not the fine `N`. The default
# scalar-`ones` path is tiled and does not call this.
function _sa_prolongation(A_csc::SparseMatrixCSC, B;
                          inject_coarse::Bool,
                          strength = AlgebraicMultigrid.SymmetricStrength(),
                          aggregate = AlgebraicMultigrid.StandardAggregation(),
                          smooth::Bool = true,
                          jacobi_ω = 4 / 3)
    S, _ = strength(A_csc)
    AggOp = aggregate(S)
    size(AggOp, 1) == 0 && return nothing
    T = eltype(A_csc)
    Tent, B_c = AlgebraicMultigrid.fit_candidates(AggOp, B)
    size(Tent, 2) == 0 && return nothing
    P = if smooth
        AlgebraicMultigrid.JacobiProlongation(T(jacobi_ω))(A_csc, Tent, S, B_c)
    else
        Tent
    end
    size(P, 2) == 0 && return nothing
    B_next = inject_coarse ? B_c : ones(T, size(P, 2))
    return P, B_next
end

function _rs_prolongation(A_csc::SparseMatrixCSC; kwargs...)
    extra = _passthrough_kwargs(; kwargs...)
    n = size(A_csc, 1)
    ml = AlgebraicMultigrid.ruge_stuben(A_csc; max_levels=2, max_coarse=max(n - 1, 1), extra...)
    isempty(ml.levels) && return nothing
    P = SparseArrays.sparse(ml.levels[1].P)
    return size(P, 2) == 0 ? nothing : P
end

function _amg_prolongation_gathered(A::DMatrix{T}, B; method::Symbol, smooth::Bool,
                                    inject_coarse::Bool, jacobi_ω=4 / 3, kwargs...) where T
    A_csc = Dagger._collect_sparse_dmatrix(A)
    if method === :smoothed_aggregation
        extra = _passthrough_kwargs(; kwargs...)
        result = _sa_prolongation(A_csc, B; inject_coarse, smooth, jacobi_ω=jacobi_ω, extra...)
        result === nothing && return nothing
        P_csc, B_next = result
    elseif method === :ruge_stuben
        P_csc = _rs_prolongation(A_csc; kwargs...)
        P_csc === nothing && return nothing
        B_next = B
    else
        throw(ArgumentError("GlobalAMG: unknown method $(method); use \
            :smoothed_aggregation or :ruge_stuben"))
    end
    nc = size(P_csc, 2)
    (nc == 0 || nc >= size(A, 1)) && return nothing
    k = Int(A.partitioning.blocksize[1])
    return Dagger.distribute(P_csc, Blocks(k, k)), B_next
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

function _amg_iface_from_row!(iface_local, iface_nbr, assigned, diag_j, col_starts, tiles...)
    for j in 1:length(tiles)
        j == diag_j && continue
        Aj = _amg_tile_csc(tiles[j])
        c0 = col_starts[j]
        for col in 1:size(Aj, 2)
            for p in SparseArrays.nzrange(Aj, col)
                r = Aj.rowval[p]
                (r < 1 || r > length(assigned) || assigned[r]) && continue
                Aj.nzval[p] == 0 && continue
                push!(iface_local, r)
                push!(iface_nbr, c0 + col - 1)
            end
        end
    end
    return nothing
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
    assigned = falses(k)
    if nagg > 0
        Tent, _ = AlgebraicMultigrid.fit_candidates(AggOp, ones(T, k))
        if size(Tent, 2) > 0
            I, J, V = SparseArrays.findnz(Tent)
            nagg = size(Tent, 2)
            for r in I
                assigned[r] = true
            end
        else
            nagg = 0
        end
    end
    iface_local = Int[]
    iface_nbr = Int[]
    _amg_iface_from_row!(iface_local, iface_nbr, assigned, diag_j, col_starts, tiles...)
    return AMGTileInterp{T}(I, J, V, nagg, iface_local, iface_nbr)
end

function _amg_row_coarsen_rs(diag_j::Int, col_starts::Vector{Int}, extra, tiles...)
    Ad = _amg_tile_csc(tiles[diag_j])
    k = size(Ad, 1)
    T = eltype(Ad)
    I = Int[]
    J = Int[]
    V = T[]
    nagg = 0
    assigned = falses(k)
    if k <= 1
        nagg = 1
        I = Int[1]
        J = Int[1]
        V = T[one(T)]
        assigned[1] = true
    else
        ml = AlgebraicMultigrid.ruge_stuben(Ad; max_levels=2, max_coarse=max(k - 1, 1), extra...)
        if !isempty(ml.levels)
            P = SparseArrays.sparse(ml.levels[1].P)
            if size(P, 2) > 0
                I, J, V = SparseArrays.findnz(P)
                nagg = size(P, 2)
                for r in I
                    assigned[r] = true
                end
            end
        end
    end
    iface_local = Int[]
    iface_nbr = Int[]
    _amg_iface_from_row!(iface_local, iface_nbr, assigned, diag_j, col_starts, tiles...)
    return AMGTileInterp{T}(I, J, V, nagg, iface_local, iface_nbr)
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

function _amg_match_interface(headers, row_starts, ::Type{T}) where T
    mt = length(headers)
    edges = Vector{Tuple{Int,Int}}()
    node_tile = Dict{Int,Tuple{Int,Int}}()
    for i in 1:mt
        h = headers[i]
        r0 = row_starts[i]
        for (loc, nbr) in zip(h.iface_local, h.iface_nbr)
            g = r0 + loc - 1
            node_tile[g] = (i, loc)
            lo, hi = minmax(g, Int(nbr))
            push!(edges, (lo, hi))
        end
    end
    unique!(sort!(edges))
    unmatched = Set(keys(node_tile))
    pairs = Vector{Tuple{Int,Int}}()
    for (a, b) in edges
        (a in unmatched && b in unmatched) || continue
        (haskey(node_tile, a) && haskey(node_tile, b)) || continue
        delete!(unmatched, a)
        delete!(unmatched, b)
        push!(pairs, (a, b))
    end
    offsets = Vector{Int}(undef, mt)
    acc = 0
    for i in 1:mt
        offsets[i] = acc
        acc += headers[i].nagg
    end
    matches = [Vector{Tuple{Int,Int,T}}() for _ in 1:mt]
    extra = 0
    w2 = T(1 / sqrt(2))
    for (a, b) in pairs
        extra += 1
        gcol = acc + extra
        ta, la = node_tile[a]
        tb, lb = node_tile[b]
        push!(matches[ta], (la, gcol, w2))
        push!(matches[tb], (lb, gcol, w2))
    end
    for g in sort!(collect(unmatched))
        extra += 1
        gcol = acc + extra
        t, l = node_tile[g]
        push!(matches[t], (l, gcol, one(T)))
    end
    return offsets, matches, acc + extra
end

# Local-index CSC for one `P` tile. `match` is `(local_row, global_col, weight)`.
function Dagger._amg_fill_p_tile(interp::AMGTileInterp{T}, match, offset::Int,
                                 col0::Int, ncols::Int, tm::Int, tn::Int) where T
    I = Int[]
    J = Int[]
    V = T[]
    for p in eachindex(interp.I)
        gcol = offset + interp.J[p]
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

function _amg_assemble_p(A::DMatrix{T}, interp_tasks, offsets, matches, nc) where T
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
            interp_tasks[i], matches[i], offsets[i], col0s[j], colns[j], tm, tn)
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
    offsets, matches, nc = _amg_match_interface(headers, row_starts, T)
    (nc == 0 || nc >= n) && return nothing
    Tent = _amg_assemble_p(A, interp_tasks, offsets, matches, nc)
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
                          nullspace=nothing,
                          B=nothing,
                          kwargs...)
    method === :smoothed_aggregation || method === :ruge_stuben || throw(ArgumentError(
        "GlobalAMG: unknown method $(method); use :smoothed_aggregation or :ruge_stuben"))
    max_levels >= 1 || throw(ArgumentError("max_levels must be ≥ 1"))
    max_coarse >= 1 || throw(ArgumentError("max_coarse must be ≥ 1"))
    presweeps >= 0 && postsweeps >= 0 || throw(ArgumentError(
        "presweeps and postsweeps must be ≥ 0"))

    N = _pick_nullspace(nullspace, B)
    if method === :ruge_stuben && N !== nothing
        throw(ArgumentError("GlobalAMG: nullspace= is only used by smoothed \
            aggregation (PETSc MatSetNearNullSpace / GAMG). Ruge–Stüben has no \
            candidate injection."))
    end

    n, A = _square_amg_operator(A)
    T = eltype(A)
    B_lvl, nmodes, inject_coarse = _host_candidates(n, T, N)
    levels = GlobalAMGLevel[]
    while length(levels) + 1 < max_levels && size(A, 1) > max_coarse
        if inject_coarse
            # Near-nullspace: gather so coarse levels get `R` from
            # `fit_candidates` (lesson 39). Default scalar SA stays tiled.
            result = _amg_prolongation_gathered(A, B_lvl; method, smooth, inject_coarse,
                                                jacobi_ω, kwargs...)
            result === nothing && break
            P, B_lvl = result
        else
            # A later tiled coarsening on a handful of tiles sees only the
            # diagonal blocks of an already-denser RAP product; that P can make
            # the V-cycle worse than Jacobi (lesson 32 / 42). Stop and LU.
            if !isempty(levels) && size(A.chunks, 1) <= 3
                break
            end
            P = _amg_prolongation(A; method, smooth, jacobi_ω, kwargs...)
            P === nothing && break
        end
        Ac = _amg_galerkin(A, P)
        push!(levels, _amg_level(A, P))
        A = Ac
    end
    coarse = Dagger._spawn_direct_factorization(A, LinearAlgebra.lu)
    part = Blocks(Int((isempty(levels) ? A : levels[1].A).partitioning.blocksize[1]))
    return GlobalAMG(levels, coarse, A, Float64(relax), Int(presweeps), Int(postsweeps),
                     n, part, method, nmodes)
end

function Dagger.GlobalAMG(A::Projected; nullspace=nothing, B=nothing, kwargs...)
    inner = A.A
    inner isa DMatrix || throw(ArgumentError(
        "GlobalAMG(Projected(A, N)) requires A to be a DMatrix, got $(typeof(inner))"))
    N = _pick_nullspace(nullspace, B)
    N === nothing && (N = A.right)
    return Dagger.GlobalAMG(inner; nullspace=N, kwargs...)
end

# AlgebraicMultigrid.jl-shaped entry points. `smoothed_aggregation(::DMatrix)`
# returns a Krylov-ready `GlobalAMG` (not a host `MultiLevel`);
# `aspreconditioner` is therefore the identity.
function AlgebraicMultigrid.smoothed_aggregation(A::DMatrix; kwargs...)
    return Dagger.GlobalAMG(A; method=:smoothed_aggregation, kwargs...)
end

function AlgebraicMultigrid.smoothed_aggregation(A::Projected; kwargs...)
    return Dagger.GlobalAMG(A; method=:smoothed_aggregation, kwargs...)
end

function AlgebraicMultigrid.ruge_stuben(A::DMatrix; kwargs...)
    return Dagger.GlobalAMG(A; method=:ruge_stuben, kwargs...)
end

AlgebraicMultigrid.aspreconditioner(M::Dagger.GlobalAMG) = M

end # module
