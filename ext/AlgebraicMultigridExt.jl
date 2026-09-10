module AlgebraicMultigridExt

import AlgebraicMultigrid
import SparseArrays
import SparseArrays: SparseMatrixCSC
import Dagger
import Dagger: DMatrix, DVector, Blocks, GlobalAMG, GlobalAMGLevel, Projected
import Dagger: AMGTileInterp, AMGTileHeader, AMGTileGraph
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
# Global AMG: HMIS-lite (default) / PMIS / standard interpolation `P`, Galerkin
# `Ac = P' A P` (distributed), cycle apply in `src/array/amg.jl`.
# Per-tile `AMGPreconditioner` above is unchanged. `nullspace=N` gathers `N`
# only — not `A` — so coarse levels get `R` from `fit_candidates` (lesson 39).
# ---------------------------------------------------------------------------

const _HIERARCHY_KW = (
    :method, :smooth, :max_levels, :max_coarse, :relax, :presweeps, :postsweeps,
    :jacobi_ω, :nullspace, :B, :smoother, :cycle, :coarsen, :blocksize, :nvars,
    :chebyshev_degree, :chebyshev_ratio, :interp, :τ, :overlap, :type,
)

const _PMIS_U = UInt8(0)
const _PMIS_C = UInt8(1)
const _PMIS_F = UInt8(2)

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

function _unknown_modes(n::Int, bs::Int, ::Type{T}) where T
    n % bs == 0 || throw(ArgumentError(
        "GlobalAMG blocksize=$bs does not divide operator size $n"))
    nnode = n ÷ bs
    B = zeros(T, n, bs)
    for i in 1:nnode, c in 1:bs
        B[bs * (i - 1) + c, c] = one(T)
    end
    return B, bs
end

function _host_candidates(n::Int, T, N, blocksize::Int)
    if N === nothing && blocksize == 1
        return ones(T, n), 1, false
    elseif N === nothing
        B, nmodes = _unknown_modes(n, blocksize, T)
        return B, nmodes, true
    end
    B, nmodes = _as_candidates(N, T)
    size(B, 1) == n || throw(DimensionMismatch(
        "nullspace has $(size(B, 1)) rows but the operator is $(n)×$(n)"))
    nmodes >= 1 || throw(ArgumentError("nullspace must have at least one column"))
    return B, nmodes, true
end

# ---------------------------------------------------------------------------
# Tile strength graph (one row of tiles). Not a gather of `A`.
# ---------------------------------------------------------------------------

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

function _strength_kind(strength)
    if strength isa AlgebraicMultigrid.Classical
        return :classical, Float64(strength.θ)
    elseif strength isa AlgebraicMultigrid.SymmetricStrength
        return :symmetric, Float64(strength.θ)
    else
        return :symmetric, 0.0
    end
end

function _amg_host_diag(A::DMatrix{T}) where T
    dinv = Dagger._jacobi_dinv(A)
    id = collect(dinv)
    out = Vector{T}(undef, length(id))
    @inbounds for i in eachindex(id)
        out[i] = iszero(id[i]) ? zero(T) : inv(id[i])
    end
    return out
end

_pmis_hash(i::Int) = begin
    x = UInt64(i) * 0x9E3779B97F4A7C15
    x ⊻= x >> 30
    x *= 0xBF58476D1CE4E5B9
    x ⊻= x >> 27
    x *= 0x94D049BB133111EB
    return x ⊻ (x >> 31)
end

_pmis_beats(i::Int, j::Int) = let hi = _pmis_hash(i), hj = _pmis_hash(j)
    hi > hj || (hi == hj && i > j)
end

_amg_node(i::Int, bs::Int) = bs == 1 ? i : (i - 1) ÷ bs + 1

function Dagger._amg_row_graph(row0::Int, col_starts::Vector{Int}, n::Int,
                               θ::Float64, kind::Symbol, diags, tiles...)
    S0 = _amg_tile_csc(tiles[1])
    k = size(S0, 1)
    T = eltype(S0)
    I = Int[]
    J = Int[]
    V = T[]
    for (j, tile) in enumerate(tiles)
        S = _amg_tile_csc(tile)
        c0 = col_starts[j]
        for col in 1:size(S, 2)
            for p in SparseArrays.nzrange(S, col)
                r = S.rowval[p]
                (1 <= r <= k) || continue
                push!(I, r)
                push!(J, c0 + col - 1)
                push!(V, S.nzval[p])
            end
        end
    end
    Arow = SparseArrays.sparse(I, J, V, k, n)
    At = SparseArrays.sparse(LinearAlgebra.transpose(Arow))
    rowptr = At.colptr
    colval = At.rowval
    nzval = At.nzval
    diagv = Vector{T}(undef, k)
    strong = falses(length(nzval))
    for i in 1:k
        g = row0 + i - 1
        d = zero(T)
        maxoff = zero(real(T))
        for p in rowptr[i]:(rowptr[i + 1] - 1)
            j = colval[p]
            v = nzval[p]
            if j == g
                d += v
            else
                maxoff = max(maxoff, abs(v))
            end
        end
        diagv[i] = d
        di_g = diags === nothing ? abs(d) : abs(T(diags[g]))
        for p in rowptr[i]:(rowptr[i + 1] - 1)
            j = colval[p]
            j == g && continue
            v = nzval[p]
            is_strong = if kind === :classical
                maxoff == 0 ? false : (abs(v) >= θ * maxoff)
            elseif θ == 0
                v != 0
            else
                dj = diags === nothing ? di_g : abs(T(diags[j]))
                abs(v)^2 >= (θ * di_g) * (θ * dj)
            end
            strong[p] = is_strong
        end
    end
    return AMGTileGraph{T}(row0, k, rowptr, colval, nzval, strong, diagv)
end

function Dagger._amg_pmis_propose(g::AMGTileGraph, splitting::Vector{UInt8}, bs::Int)
    out = Int[]
    if bs == 1
        for i in 1:g.k
            gi = g.row0 + i - 1
            splitting[gi] == _PMIS_U || continue
            win = true
            for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
                g.strong[p] || continue
                j = g.colval[p]
                j == gi && continue
                splitting[j] == _PMIS_U || continue
                if !_pmis_beats(gi, j)
                    win = false
                    break
                end
            end
            win && push!(out, gi)
        end
        return out
    end
    first_node = _amg_node(g.row0, bs)
    last_node = _amg_node(g.row0 + g.k - 1, bs)
    for ν in first_node:last_node
        splitting[ν] == _PMIS_U || continue
        win = true
        for i in 1:g.k
            gi = g.row0 + i - 1
            _amg_node(gi, bs) == ν || continue
            for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
                g.strong[p] || continue
                jn = _amg_node(g.colval[p], bs)
                jn == ν && continue
                splitting[jn] == _PMIS_U || continue
                if !_pmis_beats(ν, jn)
                    win = false
                    break
                end
            end
            win || break
        end
        win && push!(out, ν)
    end
    return out
end

function Dagger._amg_pmis_mark_f(g::AMGTileGraph, splitting::Vector{UInt8},
                                 newC::Vector{Int}, bs::Int)
    Cset = Set(newC)
    out = Int[]
    if bs == 1
        for i in 1:g.k
            gi = g.row0 + i - 1
            splitting[gi] == _PMIS_U || continue
            gi in Cset && continue
            for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
                g.strong[p] || continue
                if g.colval[p] in Cset
                    push!(out, gi)
                    break
                end
            end
        end
        return out
    end
    first_node = _amg_node(g.row0, bs)
    last_node = _amg_node(g.row0 + g.k - 1, bs)
    for ν in first_node:last_node
        splitting[ν] == _PMIS_U || continue
        ν in Cset && continue
        hit = false
        for i in 1:g.k
            gi = g.row0 + i - 1
            _amg_node(gi, bs) == ν || continue
            for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
                g.strong[p] || continue
                if _amg_node(g.colval[p], bs) in Cset
                    hit = true
                    break
                end
            end
            hit && break
        end
        hit && push!(out, ν)
    end
    return out
end

function _amg_pmis_splitting(g_tasks, n_idx::Int, bs::Int)
    splitting = fill(_PMIS_U, n_idx)
    for _ in 1:n_idx
        props = Vector{Vector{Int}}(undef, length(g_tasks))
        for i in eachindex(g_tasks)
            props[i] = fetch(Dagger.@spawn Dagger._amg_pmis_propose(g_tasks[i], splitting, bs))
        end
        newC = Int[]
        for p in props
            append!(newC, p)
        end
        unique!(sort!(newC))
        filter!(i -> 1 <= i <= n_idx && splitting[i] == _PMIS_U, newC)
        isempty(newC) && break
        for i in newC
            splitting[i] = _PMIS_C
        end
        for i in eachindex(g_tasks)
            fs = fetch(Dagger.@spawn Dagger._amg_pmis_mark_f(g_tasks[i], splitting, newC, bs))
            for j in fs
                (1 <= j <= n_idx && splitting[j] == _PMIS_U) && (splitting[j] = _PMIS_F)
            end
        end
    end
    for i in 1:n_idx
        splitting[i] == _PMIS_U && (splitting[i] = _PMIS_C)
    end
    return splitting
end

function Dagger._amg_sa_membership(g::AMGTileGraph{T}, splitting::Vector{UInt8},
                                   c_to_agg::Vector{Int}, bs::Int) where T
    out = Vector{Tuple{Int,Int,T}}()
    if bs == 1
        for i in 1:g.k
            gi = g.row0 + i - 1
            if splitting[gi] == _PMIS_C
                push!(out, (gi, c_to_agg[gi], one(T)))
                continue
            end
            best_c = 0
            best_s = zero(T)
            for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
                g.strong[p] || continue
                j = g.colval[p]
                splitting[j] == _PMIS_C || continue
                s = abs(g.nzval[p])
                if s > best_s
                    best_s = s
                    best_c = j
                end
            end
            best_c == 0 && continue
            push!(out, (gi, c_to_agg[best_c], one(T)))
        end
        return out
    end
    first_node = _amg_node(g.row0, bs)
    last_node = _amg_node(g.row0 + g.k - 1, bs)
    node_agg = Dict{Int,Int}()
    for ν in first_node:last_node
        if splitting[ν] == _PMIS_C
            node_agg[ν] = c_to_agg[ν]
            continue
        end
        best_c = 0
        best_s = zero(T)
        for i in 1:g.k
            gi = g.row0 + i - 1
            _amg_node(gi, bs) == ν || continue
            for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
                g.strong[p] || continue
                jn = _amg_node(g.colval[p], bs)
                splitting[jn] == _PMIS_C || continue
                s = abs(g.nzval[p])
                if s > best_s
                    best_s = s
                    best_c = jn
                end
            end
        end
        best_c == 0 && continue
        node_agg[ν] = c_to_agg[best_c]
    end
    for i in 1:g.k
        gi = g.row0 + i - 1
        ν = _amg_node(gi, bs)
        haskey(node_agg, ν) || continue
        push!(out, (gi, node_agg[ν], one(T)))
    end
    return out
end

# Classical distance-1 interpolation (AlgebraicMultigrid.jl `direct_interpolation`)
# on the local rows, given a global C/F splitting. C-points of a nodal split
# expand to every DOF of that node.
function Dagger._amg_rs_interp_tile(g::AMGTileGraph{T}, splitting::Vector{UInt8},
                                    c_to_col::Vector{Int}, bs::Int) where T
    I = Int[]
    J = Int[]
    V = T[]
    function isC(idx)
        return splitting[_amg_node(idx, bs)] == _PMIS_C
    end
    for i in 1:g.k
        gi = g.row0 + i - 1
        if isC(gi)
            push!(I, i)
            push!(J, c_to_col[_amg_node(gi, bs)])
            push!(V, one(T))
            continue
        end
        sum_strong_pos = zero(T)
        sum_strong_neg = zero(T)
        for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
            g.strong[p] || continue
            j = g.colval[p]
            isC(j) || continue
            sval = g.nzval[p]
            if real(sval) < 0
                sum_strong_neg += sval
            else
                sum_strong_pos += sval
            end
        end
        sum_all_pos = zero(T)
        sum_all_neg = zero(T)
        diag = g.diag[i]
        for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
            j = g.colval[p]
            j == gi && continue
            aval = g.nzval[p]
            if real(aval) < 0
                sum_all_neg += aval
            else
                sum_all_pos += aval
            end
        end
        if sum_strong_pos == 0
            beta = zero(diag)
            real(diag) >= 0 && (diag += sum_all_pos)
        else
            beta = sum_all_pos / sum_strong_pos
        end
        if sum_strong_neg == 0
            alpha = zero(diag)
            real(diag) < 0 && (diag += sum_all_neg)
        else
            alpha = sum_all_neg / sum_strong_neg
        end
        if isapprox(real(diag), 0; atol=eps(real(T)))
            neg_coeff = zero(T)
            pos_coeff = zero(T)
        else
            neg_coeff = alpha / diag
            pos_coeff = beta / diag
        end
        for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
            g.strong[p] || continue
            j = g.colval[p]
            isC(j) || continue
            sval = g.nzval[p]
            w = real(sval) < 0 ? abs(neg_coeff * sval) : abs(pos_coeff * sval)
            push!(I, i)
            push!(J, c_to_col[_amg_node(j, bs)])
            push!(V, T(w))
        end
    end
    return AMGTileInterp{T}(I, J, V, 0, Int[], Int[])
end

# Hybrid GS: processor-local symmetric GS on the diagonal tile; off-tile
# connections stay at the Jacobi (old) values already baked into `Au`.
function _gs_pass!(A::SparseMatrixCSC, b, x, start, step, stop)
    z = zero(eltype(A))
    @inbounds for i in start:step:stop
        rsum = z
        d = z
        for j in SparseArrays.nzrange(A, i)
            row = A.rowval[j]
            val = A.nzval[j]
            d = ifelse(i == row, val, d)
            rsum += ifelse(i == row, z, val * x[row])
        end
        if d != z
            x[i] = (b[i] - rsum) / d
        end
    end
    return nothing
end

function Dagger._hybrid_gs_tile!(u, Adiag, Au, b)
    S = _amg_tile_csc(Adiag)
    n = length(u)
    aii_u = S * (u isa AbstractVector ? u : vec(u))
    rhs = similar(aii_u)
    @inbounds for i in 1:n
        rhs[i] = b[i] - Au[i] + aii_u[i]
    end
    _gs_pass!(S, rhs, u, 1, 1, n)
    _gs_pass!(S, rhs, u, n, -1, 1)
    return nothing
end

# ---------------------------------------------------------------------------
# Standard (legacy) per-tile SA/RS + leftover interface pairing.
# ---------------------------------------------------------------------------

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

function Dagger._amg_local_sa_header(g::AMGTileGraph{T}, strength, aggregate) where T
    # Restrict the row graph to local columns and run StandardAggregation so
    # HMIS can freeze interior aggregates before PMIS on the leftovers.
    I = Int[]
    J = Int[]
    V = T[]
    for i in 1:g.k
        gi = g.row0 + i - 1
        for p in g.rowptr[i]:(g.rowptr[i + 1] - 1)
            j = g.colval[p]
            (g.row0 <= j < g.row0 + g.k) || continue
            push!(I, i)
            push!(J, j - g.row0 + 1)
            push!(V, g.nzval[p])
        end
    end
    Aloc = SparseArrays.sparse(I, J, V, g.k, g.k)
    S, _ = strength(Aloc)
    AggOp = aggregate(S)
    nagg = size(AggOp, 1)
    assigned = Int[]
    agg_of = zeros(Int, g.k)
    if nagg > 0
        Tent, _ = AlgebraicMultigrid.fit_candidates(AggOp, ones(T, g.k))
        if size(Tent, 2) > 0
            Ii, Jj, _ = SparseArrays.findnz(Tent)
            nagg = size(Tent, 2)
            for p in eachindex(Ii)
                agg_of[Ii[p]] = Jj[p]
                push!(assigned, Ii[p])
            end
            unique!(sort!(assigned))
        else
            nagg = 0
        end
    end
    return (nagg, assigned, agg_of, g.row0)
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

function _amg_distribute_p(A::DMatrix{T}, P_csc::SparseMatrixCSC) where T
    n, nc = size(P_csc)
    k = Int(A.partitioning.blocksize[1])
    return Dagger.distribute(P_csc, Blocks(k, k))
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

function _amg_spawn_row_graphs(A::DMatrix{T}, strength) where T
    kind, θ = _strength_kind(strength)
    n = size(A, 1)
    mt, nt = size(A.chunks)
    row_starts, col_starts = _amg_tile_starts(A)
    diags = (kind === :symmetric && θ != 0) ? _amg_host_diag(A) : nothing
    g_tasks = Vector{Dagger.DTask}(undef, mt)
    for i in 1:mt
        g_tasks[i] = Dagger.@spawn return_type=AMGTileGraph{T} Dagger._amg_row_graph(
            row_starts[i], col_starts, n, θ, kind, diags,
            (A.chunks[i, j] for j in 1:nt)...)
    end
    return g_tasks, row_starts, col_starts
end

function _amg_c_maps(splitting::Vector{UInt8})
    n_idx = length(splitting)
    c_to_col = zeros(Int, n_idx)
    col = 0
    for i in 1:n_idx
        if splitting[i] == _PMIS_C
            col += 1
            c_to_col[i] = col
        end
    end
    return c_to_col, col
end

function _amg_cover_unassigned!(members::Vector{Tuple{Int,Int,T}}, n::Int, nagg::Int, ::Type{T}) where T
    seen = falses(n)
    for (gi, _, _) in members
        (1 <= gi <= n) && (seen[gi] = true)
    end
    extra = nagg
    for i in 1:n
        seen[i] && continue
        extra += 1
        push!(members, (i, extra, one(T)))
    end
    return extra
end

function _amg_membership_aggop(members, n::Int, nagg::Int, ::Type{T}) where T
    I = Vector{Int}(undef, length(members))
    J = Vector{Int}(undef, length(members))
    V = ones(T, length(members))
    for p in eachindex(members)
        I[p] = members[p][2]
        J[p] = members[p][1]
    end
    return SparseArrays.sparse(I, J, V, nagg, n)
end

function _amg_fit_and_distribute(A::DMatrix{T}, AggOp, B) where T
    Tent, B_next = AlgebraicMultigrid.fit_candidates(AggOp, B)
    size(Tent, 2) == 0 && return nothing
    P = _amg_distribute_p(A, SparseArrays.sparse(Tent))
    return P, B_next
end

# HMIS-lite: keep local SA aggregates, then PMIS on unassigned interface nodes.
# Leftover F may *join* a neighboring local aggregate (StandardAggregation pass 2).
# Do *not* merge two already-assigned aggregates (that lost to Jacobi).
function _amg_hmis_sa_members(g_tasks, n::Int, bs::Int, strength, aggregate, ::Type{T}) where T
    mt = length(g_tasks)
    headers = Vector{Any}(undef, mt)
    for i in 1:mt
        headers[i] = fetch(Dagger.@spawn Dagger._amg_local_sa_header(g_tasks[i], strength, aggregate))
    end
    n_idx = n ÷ bs
    offsets = Vector{Int}(undef, mt)
    acc = 0
    for i in 1:mt
        offsets[i] = acc
        acc += headers[i][1]
    end
    members = Vector{Tuple{Int,Int,T}}()
    assigned_nodes = Set{Int}()
    c_to_agg = zeros(Int, n_idx)
    splitting = fill(_PMIS_U, n_idx)
    for i in 1:mt
        nagg, assigned, agg_of, row0 = headers[i]
        for loc in assigned
            gi = row0 + loc - 1
            agg = offsets[i] + agg_of[loc]
            push!(members, (gi, agg, one(T)))
            ν = _amg_node(gi, bs)
            push!(assigned_nodes, ν)
            c_to_agg[ν] = agg
            splitting[ν] = _PMIS_C
        end
    end
    splitting = _amg_pmis_splitting_from(g_tasks, splitting, n_idx, bs)
    leftover_C = [ν for ν in 1:n_idx if splitting[ν] == _PMIS_C && ν ∉ assigned_nodes]
    extra = 0
    for ν in leftover_C
        extra += 1
        c_to_agg[ν] = acc + extra
    end
    nagg = acc + extra
    for i in eachindex(g_tasks)
        part = fetch(Dagger.@spawn Dagger._amg_sa_membership(g_tasks[i], splitting, c_to_agg, bs))
        for (gi, agg, w) in part
            _amg_node(gi, bs) in assigned_nodes && continue
            push!(members, (gi, agg, w))
        end
    end
    return members, nagg
end

function _amg_pmis_splitting_from(g_tasks, splitting::Vector{UInt8}, n_idx::Int, bs::Int)
    for _ in 1:n_idx
        props = Vector{Vector{Int}}(undef, length(g_tasks))
        for i in eachindex(g_tasks)
            props[i] = fetch(Dagger.@spawn Dagger._amg_pmis_propose(g_tasks[i], splitting, bs))
        end
        newC = Int[]
        for p in props
            append!(newC, p)
        end
        unique!(sort!(newC))
        filter!(i -> 1 <= i <= n_idx && splitting[i] == _PMIS_U, newC)
        isempty(newC) && break
        for i in newC
            splitting[i] = _PMIS_C
        end
        for i in eachindex(g_tasks)
            fs = fetch(Dagger.@spawn Dagger._amg_pmis_mark_f(g_tasks[i], splitting, newC, bs))
            for j in fs
                (1 <= j <= n_idx && splitting[j] == _PMIS_U) && (splitting[j] = _PMIS_F)
            end
        end
    end
    for i in 1:n_idx
        splitting[i] == _PMIS_U && (splitting[i] = _PMIS_C)
    end
    return splitting
end

function _amg_prolongation_standard(A::DMatrix{T}; method::Symbol, smooth::Bool,
                                    jacobi_ω, extra, strength, aggregate) where T
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
    P = smooth ? _amg_smooth_prolongation(A, Tent, jacobi_ω) : Tent
    return P, nothing
end

function _amg_prolongation(A::DMatrix{T}, B; method::Symbol, smooth::Bool,
                           jacobi_ω=4 / 3, coarsen::Symbol=:hmis,
                           interp::Symbol=:sa, blocksize::Int=1, kwargs...) where T
    method === :smoothed_aggregation || method === :ruge_stuben || throw(ArgumentError(
        "GlobalAMG: unknown method $(method); use :smoothed_aggregation or :ruge_stuben"))
    extra = _passthrough_kwargs(; kwargs...)
    strength = get(extra, :strength, method === :ruge_stuben ?
                   AlgebraicMultigrid.Classical() : AlgebraicMultigrid.SymmetricStrength())
    aggregate = get(extra, :aggregate, AlgebraicMultigrid.StandardAggregation())
    if interp === :extended || interp === :exti || interp === :air ||
            interp === :ff || interp === :multipass
        throw(ArgumentError("GlobalAMG: interp=$(repr(interp)) is not implemented \
            (AlgebraicMultigrid.jl has no ext+i / AIR / FF / multipass hook, and a \
            tiled distance-2 interpolant would need a 2-hop gather of A). Use \
            interp=:sa or interp=:direct."))
    end
    n = size(A, 1)
    bs = Int(blocksize)
    n % bs == 0 || throw(ArgumentError(
        "GlobalAMG blocksize=$bs does not divide operator size $n"))
    n_idx = n ÷ bs

    if coarsen === :standard
        method === :ruge_stuben && interp === :sa && (interp = :direct)
        return _amg_prolongation_standard(A; method, smooth, jacobi_ω, extra, strength, aggregate)
    end
    coarsen === :pmis || coarsen === :hmis || throw(ArgumentError(
        "GlobalAMG: coarsen must be :pmis, :hmis, or :standard, got $(repr(coarsen))"))

    g_tasks, _, _ = _amg_spawn_row_graphs(A, strength)
    use_sa = method === :smoothed_aggregation && interp !== :direct
    if coarsen === :hmis && use_sa
        members, nagg = _amg_hmis_sa_members(g_tasks, n, bs, strength, aggregate, T)
        isempty(members) && return nothing
        _amg_cover_unassigned!(members, n, nagg, T)
        nagg = maximum(m -> m[2], members)
        (nagg == 0 || nagg >= n) && return nothing
        AggOp = _amg_membership_aggop(members, n, nagg, T)
        result = _amg_fit_and_distribute(A, AggOp, B)
        result === nothing && return nothing
        Tent, B_next = result
        P = smooth ? _amg_smooth_prolongation(A, Tent, jacobi_ω) : Tent
        return P, B_next
    end

    splitting = _amg_pmis_splitting(g_tasks, n_idx, bs)
    c_to_col, nC = _amg_c_maps(splitting)
    (nC == 0 || nC >= n_idx) && return nothing

    if use_sa
        members = Vector{Tuple{Int,Int,T}}()
        for i in eachindex(g_tasks)
            part = fetch(Dagger.@spawn Dagger._amg_sa_membership(g_tasks[i], splitting, c_to_col, bs))
            append!(members, part)
        end
        isempty(members) && return nothing
        nagg = nC
        _amg_cover_unassigned!(members, n, nagg, T)
        nagg = maximum(m -> m[2], members)
        AggOp = _amg_membership_aggop(members, n, nagg, T)
        result = _amg_fit_and_distribute(A, AggOp, B)
        result === nothing && return nothing
        Tent, B_next = result
        P = smooth ? _amg_smooth_prolongation(A, Tent, jacobi_ω) : Tent
        return P, B_next
    end

    # Classical RS interpolation from the tiled graph (no gather of A).
    interp_tasks = Vector{Dagger.DTask}(undef, length(g_tasks))
    for i in eachindex(g_tasks)
        interp_tasks[i] = Dagger.@spawn return_type=AMGTileInterp{T} Dagger._amg_rs_interp_tile(
            g_tasks[i], splitting, c_to_col, bs)
    end
    mt = length(g_tasks)
    offsets = zeros(Int, mt)
    matches = [Vector{Tuple{Int,Int,T}}() for _ in 1:mt]
    P = _amg_assemble_p(A, interp_tasks, offsets, matches, nC)
    return P, B
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

function _amg_estimate_lmax(A::DMatrix{T}, dinv::DVector{T}; iters::Int=12) where T
    n = size(A, 1)
    k = Int(A.partitioning.blocksize[1])
    v = DVector{T}(undef, Blocks(k), n)
    w = DVector{T}(undef, Blocks(k), n)
    fill!(v, one(T))
    nv = LinearAlgebra.norm2(v)
    LinearAlgebra.rmul!(v, inv(nv))
    λ = one(real(T))
    ε = eps(real(T))
    for _ in 1:iters
        LinearAlgebra.mul!(w, A, v)
        Dagger._amg_scale_inplace!(w, dinv)
        nw = LinearAlgebra.norm2(w)
        λ = real(T)(nw)
        copyto!(v, w)
        LinearAlgebra.rmul!(v, inv(nw + ε))
    end
    return 1.1 * Float64(λ)
end

function _amg_level(A::DMatrix{T}, P::DMatrix{T}, smoother::Symbol;
                    chebyshev_degree::Int=2, chebyshev_ratio::Float64=0.3,
                    ilu_kw=(;), ras_kw=(;)) where T
    n = size(A, 1)
    nc = size(P, 2)
    k = Int(A.partitioning.blocksize[1])
    kc = Int(P.partitioning.blocksize[2])
    dinv = smoother === :l1jacobi ? _amg_row_abs_inv(A) : Dagger._jacobi_dinv(A)
    res = DVector{T}(undef, Blocks(k), n)
    work = DVector{T}(undef, Blocks(k), n)
    dir = DVector{T}(undef, Blocks(k), n)
    coarse_x = DVector{T}(undef, Blocks(kc), nc)
    coarse_b = DVector{T}(undef, Blocks(kc), nc)
    extra = if smoother === :chebyshev
        λ_max = _amg_estimate_lmax(A, dinv)
        (chebyshev_ratio * λ_max, λ_max)
    elseif smoother === :ilu
        Dagger.BlockILUPreconditioner(A; ilu_kw...)
    elseif smoother === :ras
        Dagger.AdditiveSchwarzPreconditioner(A; ras_kw...)
    else
        nothing
    end
    return GlobalAMGLevel(A, P, dinv, res, coarse_x, coarse_b, work, dir, extra)
end

function _default_relax(smoother::Symbol, relax)
    relax !== nothing && return Float64(relax)
    return smoother === :jacobi ? (2 / 3) : 1.0
end

function Dagger.GlobalAMG(A::DMatrix;
                          method::Symbol=:smoothed_aggregation,
                          smooth::Bool=(method === :smoothed_aggregation),
                          max_levels::Integer=10,
                          max_coarse::Integer=32,
                          relax::Union{Real,Nothing}=nothing,
                          presweeps::Integer=2,
                          postsweeps::Integer=2,
                          jacobi_ω::Real=4 / 3,
                          nullspace=nothing,
                          B=nothing,
                          smoother::Symbol=:jacobi,
                          cycle::Symbol=:v,
                          coarsen::Symbol=:hmis,
                          blocksize::Integer=1,
                          nvars::Union{Integer,Nothing}=nothing,
                          chebyshev_degree::Integer=2,
                          chebyshev_ratio::Real=0.3,
                          interp::Symbol=(method === :ruge_stuben ? :direct : :sa),
                          kwargs...)
    method === :smoothed_aggregation || method === :ruge_stuben || throw(ArgumentError(
        "GlobalAMG: unknown method $(method); use :smoothed_aggregation or :ruge_stuben"))
    max_levels >= 1 || throw(ArgumentError("max_levels must be ≥ 1"))
    max_coarse >= 1 || throw(ArgumentError("max_coarse must be ≥ 1"))
    presweeps >= 0 && postsweeps >= 0 || throw(ArgumentError(
        "presweeps and postsweeps must be ≥ 0"))
    cycle === :v || cycle === :w || cycle === :f || throw(ArgumentError(
        "GlobalAMG: cycle must be :v, :w, or :f, got $(repr(cycle))"))
    smoother ∈ (:jacobi, :l1jacobi, :chebyshev, :hybrid_gs, :ilu, :ras) || throw(ArgumentError(
        "GlobalAMG: unknown smoother $(repr(smoother))"))
    coarsen === :pmis || coarsen === :hmis || coarsen === :standard || throw(ArgumentError(
        "GlobalAMG: coarsen must be :pmis, :hmis, or :standard, got $(repr(coarsen))"))
    if interp === :extended || interp === :exti || interp === :air ||
            interp === :ff || interp === :multipass
        throw(ArgumentError("GlobalAMG: interp=$(repr(interp)) is not implemented \
            (AlgebraicMultigrid.jl has no ext+i / AIR / FF / multipass hook, and a \
            tiled distance-2 interpolant would need a 2-hop gather of A). Use \
            interp=:sa or interp=:direct."))
    end
    interp === :sa || interp === :direct || throw(ArgumentError(
        "GlobalAMG: interp must be :sa or :direct, got $(repr(interp))"))

    bs = nvars === nothing ? Int(blocksize) : Int(nvars)
    bs >= 1 || throw(ArgumentError("blocksize / nvars must be ≥ 1"))
    if nvars !== nothing && Int(blocksize) != 1 && Int(blocksize) != Int(nvars)
        throw(ArgumentError("GlobalAMG: pass only one of blocksize= or nvars="))
    end

    N = _pick_nullspace(nullspace, B)
    if method === :ruge_stuben && N !== nothing
        throw(ArgumentError("GlobalAMG: nullspace= is only used by smoothed \
            aggregation (PETSc MatSetNearNullSpace / GAMG). Ruge–Stüben has no \
            candidate injection."))
    end

    n, A = _square_amg_operator(A)
    T = eltype(A)
    B_lvl, nmodes, _ = _host_candidates(n, T, N, bs)
    ω = _default_relax(smoother, relax)
    ilu_kw = (; (k => v for (k, v) in kwargs if k === :τ)...)
    ras_kw = (; (k => v for (k, v) in kwargs if k === :overlap || k === :type)...)
    if smoother === :ras && !haskey(ras_kw, :overlap)
        ras_kw = (; ras_kw..., overlap=0)
    end

    levels = GlobalAMGLevel[]
    while length(levels) + 1 < max_levels && size(A, 1) > max_coarse
        result = _amg_prolongation(A, B_lvl; method, smooth, jacobi_ω, coarsen, interp,
                                   blocksize=bs, kwargs...)
        result === nothing && break
        P, B_next = result
        nc = size(P, 2)
        (nc == 0 || nc >= size(A, 1) || nc > 0.85 * size(A, 1)) && break
        Ac = _amg_galerkin(A, P)
        push!(levels, _amg_level(A, P, smoother; chebyshev_degree=Int(chebyshev_degree),
                                 chebyshev_ratio=Float64(chebyshev_ratio),
                                 ilu_kw, ras_kw))
        A = Ac
        B_next === nothing || (B_lvl = B_next)
    end
    coarse = Dagger._spawn_direct_factorization(A, LinearAlgebra.lu)
    part = Blocks(Int((isempty(levels) ? A : levels[1].A).partitioning.blocksize[1]))
    return GlobalAMG(levels, coarse, A, ω, Int(presweeps), Int(postsweeps),
                     n, part, method, nmodes, smoother, cycle, coarsen, bs,
                     Int(chebyshev_degree))
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
