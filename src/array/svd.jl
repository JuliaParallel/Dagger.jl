# Tiled singular value decomposition for `DMatrix`, built on the Datadeps API.
#
# Algorithm: block one-sided Jacobi.  The columns of `A` are partitioned into
# block-columns (following `A`'s existing column tiling).  For every pair of
# block-columns `(i, j)` we form the small Hermitian Gram matrix
#
#       G = [ Aᵢ'Aᵢ  Aᵢ'Aⱼ ;  Aⱼ'Aᵢ  Aⱼ'Aⱼ ]
#
# accumulated over the row tiles, diagonalize it (`G = W Λ W'`), and apply the
# orthogonal rotation `W` to the two block-columns of `A` (and to the
# accumulator `V`).  A full pass over all pairs is one "sweep"; sweeps repeat
# until the largest relative off-diagonal coupling drops below `tol`.  At
# convergence the columns of `A` are mutually orthogonal: their norms are the
# singular values, and the normalized columns are the left singular vectors.
#
# The design mirrors the other tiled factorizations in this directory — each
# `spawn_datadeps` region emits tasks whose `In`/`Out`/`InOut` annotations let
# the scheduler recover parallelism — with two additions that one-sided Jacobi
# needs to run well across processes:
#
#  1. Row-band pinning.  A rotation only ever mixes *columns*, so a row tile is
#     self-contained under it: if row-tile `k` of every block-column lives on
#     one worker, that worker can compute its share of any pair's Gram matrix
#     and apply any rotation without reading another worker's memory.  `A` and
#     `V` are laid out `:cyclicrow` and the sweep tasks carry a `compute_scope`
#     pinning them to the band they touch, so tile data never moves during the
#     sweeps.  All that crosses the wire is the `(wᵢ+wⱼ)²` Gram/rotation pair —
#     a cost independent of how many rows `A` has.
#
#  2. Tournament pair ordering.  The natural cyclic order `(1,2), (1,3), …`
#     shares a block-column between consecutive pairs, so the aliasing analysis
#     serializes almost the whole sweep.  `_svd_tournament_rounds` reorders a
#     sweep into rounds of mutually disjoint pairs, which datadeps then runs
#     concurrently.
#
# Without the pinning, Dagger's default round-robin datadeps scheduler places
# successive tasks on unrelated workers and each pair re-gathers both of its
# block-columns from wherever the last task left them.
#
# Only the thin SVD is produced (U is m×min(m,n), Vᵀ is min(m,n)×n).  This
# variant forms Gram matrices of pairs of block-columns, so its accuracy is that
# of standard block Jacobi — excellent for the well- to moderately-conditioned
# case; it is not the bidiagonalization route used for extreme conditioning.

# ────────────────────────────────────────────────────────────────────────────
# Tile / host-array kernels (run on chunk data inside Datadeps tasks)
# ────────────────────────────────────────────────────────────────────────────

# Accumulate the four sub-blocks of the pair Gram matrix from one row tile.
# `Ai`, `Aj` are the (rows × wᵢ), (rows × wⱼ) tiles of block-columns i and j.
function _svd_gram_acc!(G::AbstractMatrix{T}, Ai::AbstractMatrix{T}, Aj::AbstractMatrix{T}) where {T}
    wi = size(Ai, 2)
    wj = size(Aj, 2)
    n = wi + wj
    o = one(T)
    @views LinearAlgebra.mul!(G[1:wi, 1:wi],       Ai', Ai, o, o)
    @views LinearAlgebra.mul!(G[1:wi, wi+1:n],     Ai', Aj, o, o)
    @views LinearAlgebra.mul!(G[wi+1:n, 1:wi],     Aj', Ai, o, o)
    @views LinearAlgebra.mul!(G[wi+1:n, wi+1:n],   Aj', Aj, o, o)
    return G
end

# The relative off-diagonal coupling ‖G₁₂‖ / √(‖G₁₁‖‖G₂₂‖) of one pair.  The
# largest value over a sweep drives the convergence test.
function _svd_offdiag(G::AbstractMatrix{T}, wi::Int) where {T}
    R = real(float(T))
    n = size(G, 1)
    @views num = LinearAlgebra.norm(G[1:wi, wi+1:n])
    @views den = sqrt(LinearAlgebra.norm(G[1:wi, 1:wi]) * LinearAlgebra.norm(G[wi+1:n, wi+1:n]))
    return den == 0 ? zero(R) : R(num / den)
end

# Classical cyclic (two-sided) Jacobi eigenvalue algorithm for a small dense
# Hermitian matrix.
#
# The pair Gram matrices `G` that this file diagonalizes are graded: once a
# block-column pair has nearly converged, `G`'s diagonal spans the squared
# column norms of *all* of `A`'s columns, which can differ by many orders of
# magnitude (e.g. the ratio of the largest to smallest singular value of the
# input). A general-purpose dense eigensolver (`LinearAlgebra.eigen`, backed
# by Hessenberg/QR-type LAPACK routines) is only backward-stable with respect
# to the matrix's *overall* norm: it recovers eigenvectors of `G + E` with
# `‖E‖ = O(eps)‖G‖`, which can dwarf the small eigenvalues themselves and
# destroy the corresponding singular vectors' accuracy after normalization.
# Classical Jacobi rotations, by contrast, are computed from *ratios* of the
# 2×2 sub-block entries and are therefore accurate independent of the overall
# matrix scale (Demmel & Veselic, "Jacobi's method is more accurate than QR",
# 1992) — exactly the property this block algorithm needs to preserve the
# high relative accuracy that one-sided Jacobi SVD is prized for.
function _svd_hermitian_jacobi_eigen(H::LinearAlgebra.Hermitian{T}) where {T<:Number}
    n = size(H, 1)
    R = real(T)
    A = Matrix{T}(H)
    V = Matrix{T}(LinearAlgebra.I, n, n)
    n <= 1 && return real.(LinearAlgebra.diag(A)), V
    maxsweeps = 100
    @inbounds for _s in 1:maxsweeps
        rotated = false
        for p in 1:n-1, q in p+1:n
            apq = A[p, q]
            r = abs(apq)
            r == 0 && continue
            App = real(A[p, p]); Aqq = real(A[q, q])
            # Skip pairs that are already negligible *relative to their own*
            # diagonal scale.  Using a single tolerance based on the norm of
            # the whole matrix (as opposed to this pair's own scale) is
            # exactly the kind of absolute test that loses relative accuracy
            # for small eigenvalues in a graded matrix — the same failure
            # mode this Jacobi solver exists to avoid.
            r*r <= eps(R)^2 * App * Aqq && continue
            rotated = true
            # Rotate out the phase of `apq` so the classical real 2×2 Jacobi
            # formulas (scale-invariant in App, Aqq) apply directly; then fold
            # the phase back into the accumulated rotation (see e.g. Golub &
            # Van Loan, "Matrix Computations", §8.5, for the real case).
            phase = apq / r
            theta = (Aqq - App) / (2r)
            t = theta == 0 ? one(R) : sign(theta) / (abs(theta) + sqrt(1 + theta^2))
            cR = 1 / sqrt(1 + t^2)
            sR = t * cR
            cphase = conj(phase)
            for i in 1:n
                Aip = A[i, p]; Aiq = A[i, q]
                A[i, p] = cR*Aip - cphase*sR*Aiq
                A[i, q] = sR*Aip + cphase*cR*Aiq
            end
            for j in 1:n
                Apj = A[p, j]; Aqj = A[q, j]
                A[p, j] = cR*Apj - phase*sR*Aqj
                A[q, j] = sR*Apj + phase*cR*Aqj
            end
            for i in 1:n
                Vip = V[i, p]; Viq = V[i, q]
                V[i, p] = cR*Vip - cphase*sR*Viq
                V[i, q] = sR*Vip + cphase*cR*Viq
            end
        end
        rotated || break
    end
    return real.(LinearAlgebra.diag(A)), V
end

# Diagonalize the (symmetrized) Hermitian Gram matrix; store eigenvectors in `W`.
function _svd_rot!(W::AbstractMatrix{T}, G::AbstractMatrix{T}) where {T}
    H = LinearAlgebra.Hermitian((G .+ G') ./ 2)
    _, vecs = _svd_hermitian_jacobi_eigen(H)
    copyto!(W, vecs)
    return W
end

# Apply the block rotation W to a row-tile pair in place: [Cᵢ Cⱼ] ← [Cᵢ Cⱼ] W.
function _svd_apply_rot!(Ci::AbstractMatrix{T}, Cj::AbstractMatrix{T}, W::AbstractMatrix{T}, wi::Int) where {T}
    n = size(W, 1)
    o = one(T)
    @views W11 = W[1:wi, 1:wi]
    @views W21 = W[wi+1:n, 1:wi]
    @views W12 = W[1:wi, wi+1:n]
    @views W22 = W[wi+1:n, wi+1:n]
    Ti = Ci * W11
    LinearAlgebra.mul!(Ti, Cj, W21, o, o)
    Tj = Ci * W12
    LinearAlgebra.mul!(Tj, Cj, W22, o, o)
    copyto!(Ci, Ti)
    copyto!(Cj, Tj)
    return nothing
end

# Accumulate squared column norms of one row tile into the block-column's buffer.
function _svd_colnorm_acc!(s::AbstractVector{R}, A::AbstractMatrix{T}) where {R,T}
    @inbounds for c in axes(A, 2)
        acc = zero(R)
        @views for r in axes(A, 1)
            acc += abs2(A[r, c])
        end
        s[c] += acc
    end
    return s
end

# Scale each column of a tile by 1/σ (or zero it when σ = 0), forming U in place.
function _svd_scale_cols!(A::AbstractMatrix{T}, s::AbstractVector{R}) where {R,T}
    @inbounds for c in axes(A, 2)
        sc = s[c]
        if sc > 0
            invs = inv(sc)
            @views A[:, c] .*= invs
        else
            @views A[:, c] .= zero(T)
        end
    end
    return A
end

# Single-block-column kernels: accumulate A'A and apply A ← A W (no partner).
_svd_gram_self_acc!(G::AbstractMatrix{T}, A::AbstractMatrix{T}) where {T} =
    (LinearAlgebra.mul!(G, A', A, one(T), one(T)); G)
_svd_apply_self!(A::AbstractMatrix{T}, W::AbstractMatrix{T}) where {T} =
    (Tmp = A * W; copyto!(A, Tmp); nothing)

# ────────────────────────────────────────────────────────────────────────────
# Worker-granular sweep kernels
# ────────────────────────────────────────────────────────────────────────────
#
# Each of these handles *all* the row tiles one worker owns, rather than one
# tile per task.  Two reasons, both measured:
#
#  - Task count.  A tile-granular sweep emits `O(sweeps · nt² · mt)` tasks; at
#    the tilings the testsuite uses that is tens of thousands of tasks, and
#    Dagger's per-task planning cost (chiefly compiling a fresh specialization
#    of the datadeps planner per argument-tuple shape) dominated everything
#    else.  Folding a worker's tiles into one task divides that by `mt/P`.
#  - Data movement.  A per-worker task reads only tiles that worker already
#    owns, so with the pinning below nothing has to be copied at all.
#
# These take their tiles as a vararg and are deliberately *not* marked
# `@nospecialize`.  The obvious worry is that a fresh specialization per tile
# count would trade runtime for compile time, but the arity only ever varies
# with `mt/P` — a handful of values across a whole session — so the trade never
# materializes: measured against a `@nospecialize`d version, specializing was
# both ~10% faster at runtime *and* slightly cheaper to compile, because
# `@nospecialize` forces a dynamic dispatch per tile without removing any
# specialization of the concretely-typed inner kernels it calls.

# This worker's contribution to the pair Gram matrix.  `tiles` is the row tiles
# of block-column `i` this worker owns, followed by the matching tiles of `j`.
function _svd_gram_partial!(G::AbstractMatrix, nij::Int, tiles::Vararg{Any})
    nk = length(tiles) >> 1
    Gv = @view G[1:nij, 1:nij]
    fill!(Gv, zero(eltype(G)))
    @inbounds for k in 1:nk
        _svd_gram_acc!(Gv, tiles[k], tiles[nk+k])
    end
    return nothing
end

# Sum the per-worker Gram contributions, record the pair's off-diagonal
# coupling, and diagonalize.  Runs on one worker over `O(nb²)` data, so this is
# the only part of a sweep that communicates — and its cost is independent of
# the number of rows in `A`.
function _svd_reduce_rot!(W::AbstractMatrix, off::AbstractVector, nij::Int, wi::Int,
                          Gs::Vararg{Any})
    G = Matrix(@view (Gs[1]::AbstractMatrix)[1:nij, 1:nij])
    @inbounds for idx in 2:length(Gs)
        G .+= @view (Gs[idx]::AbstractMatrix)[1:nij, 1:nij]
    end
    off[1] = max(off[1], _svd_offdiag(G, wi))
    _svd_rot!(@view(W[1:nij, 1:nij]), G)
    return nothing
end

# Apply the block rotation to every row tile of the pair this worker owns.
function _svd_apply_rot_block!(W::AbstractMatrix, nij::Int, wi::Int, tiles::Vararg{Any})
    nk = length(tiles) >> 1
    Wv = @view W[1:nij, 1:nij]
    @inbounds for k in 1:nk
        _svd_apply_rot!(tiles[k], tiles[nk+k], Wv, wi)
    end
    return nothing
end

# Squared column norms of a whole block-column, one task per worker.
function _svd_colnorms_block!(s::AbstractVector, tiles::Vararg{Any})
    @inbounds for A in tiles
        _svd_colnorm_acc!(s, A)
    end
    return nothing
end

# Accumulate A'A over a whole block-column, one task per worker.
function _svd_gram_self_block!(G::AbstractMatrix, tiles::Vararg{Any})
    @inbounds for A in tiles
        _svd_gram_self_acc!(G, A)
    end
    return nothing
end

_svd_adjoint_tile!(dst, src) = (copyto!(dst, adjoint(src)); dst)
_svd_set_identity_diag!(B) = (@inbounds B[LinearAlgebra.diagind(B)] .= one(eltype(B)); B)

# ────────────────────────────────────────────────────────────────────────────
# Small distributed helpers
# ────────────────────────────────────────────────────────────────────────────

# ────────────────────────────────────────────────────────────────────────────
# Sweep ordering and data placement
# ────────────────────────────────────────────────────────────────────────────

# Split one sweep into rounds of mutually disjoint block-column pairs.
#
# A sweep must visit every unordered pair exactly once, but the order is free —
# and the obvious cyclic order `(1,2), (1,3), …, (1,nt), (2,3), …` is the worst
# available one, because consecutive pairs share a block-column and the
# aliasing analysis therefore serializes nearly the whole sweep.
#
# The classical round-robin *tournament* (or "circle") ordering instead groups
# the pairs into `nt-1` rounds of `nt/2` pairs each, exactly as one schedules a
# round-robin sports tournament: column 1 stays put while the rest rotate
# around it.  No two pairs in a round share a block-column, so datadeps sees no
# dependency between them and runs the entire round concurrently.
#
# An odd `nt` gets a phantom column ("bye"); pairs drawn against it are
# dropped, which is what leaves one column idle per round.
function _svd_tournament_rounds(nt::Int)
    npad = isodd(nt) ? nt + 1 : nt
    nrounds = npad - 1
    half = npad ÷ 2
    rounds = Vector{Vector{Tuple{Int,Int}}}(undef, nrounds)
    ring = collect(2:npad)          # every column but the pinned first one
    for r in 1:nrounds
        order = vcat(1, ring)
        pairs = Tuple{Int,Int}[]
        for k in 1:half
            i, j = minmax(order[k], order[npad+1-k])
            j <= nt && push!(pairs, (i, j))  # drop the bye
        end
        rounds[r] = pairs
        pushfirst!(ring, pop!(ring))         # rotate for the next round
    end
    return rounds
end

# Group a factor's row-tile indices by the worker that owns them.
#
# One-sided Jacobi only ever mixes *columns*, so a row tile is complete with
# respect to every rotation: if row-tile `k` of every block-column sits on the
# same worker, that worker can compute its share of any pair's Gram matrix and
# apply any rotation without ever touching another worker's memory.  Pinning
# the sweep tasks to these groups (via `compute_scope`) is what makes the
# sweeps movement-free; see `_svd_rowcyclic` for how that layout is arranged.
#
# The grouping is derived from the same `:cyclicrow` processor grid the array
# was allocated with, so it agrees with the real placement by construction —
# `DArray.chunks` holds `DTask`s, whose location can't be read back directly.
# Grouping by *worker* rather than by processor is deliberate: several threads
# of one worker share an address space, so any of them can use the band
# without a copy.
#
# Returns `worker_scope => row_tile_indices` pairs, in worker order.
function _svd_row_groups(sizeA::Dims{2}, blocksize::Dims{2}, mt::Int)
    pg = build_procgrid(:cyclicrow, sizeA, blocksize, current_acceleration())
    byworker = Dict{Int,Vector{Int}}()
    for k in 1:mt
        w = root_worker_id(procgrid_processor(pg, CartesianIndex(k, 1)))
        push!(get!(Vector{Int}, byworker, w), k)
    end
    return [ProcessScope(w) => byworker[w] for w in sort!(collect(keys(byworker)))]
end
_svd_row_groups(A::DMatrix) =
    _svd_row_groups(size(A), A.partitioning.blocksize, size(A.chunks, 1))

# Restage `A` so that each row-tile band lives entirely on one worker.
#
# Dagger's default (`:arbitrary`) chunk assignment spreads tiles over workers
# by linear index, so row-tile `k` of block-column 1 and of block-column 2
# generally land on different workers — and then no pinning can avoid moving
# tiles during a sweep.  `:cyclicrow` maps tile `(k, i)` to worker `mod1(k, P)`
# for every `i`, which is exactly the layout the sweeps want.
#
# The restage costs one copy of `A`.  The sweeps it enables would otherwise
# copy all of `A` in and back out *per sweep*, because a datadeps region always
# returns written data to the space it came from — so this trades `O(sweeps)`
# copies of the matrix for one.  With a single worker there is nothing to
# arrange and `A` is used as-is.
function _svd_rowcyclic(A::DMatrix{T}) where {T}
    length(_svd_row_groups(A)) > 1 || return A, false
    mb, nb = A.partitioning.blocksize
    B = DArray{T,2}(undef, Blocks(mb, nb), size(A); assignment=:cyclicrow)
    copyto!(B, A)
    return B, true
end

# Widths of each block-column from a cumulative-length vector.
function _svd_tilewidths(cum::AbstractVector{<:Integer})
    w = Vector{Int}(undef, length(cum))
    prev = 0
    @inbounds for i in eachindex(cum)
        w[i] = cum[i] - prev
        prev = cum[i]
    end
    return w
end

# Locate the block-column and in-tile offset of global column `c`.
@inline function _svd_locate_col(cum::AbstractVector{<:Integer}, c::Int)
    k = searchsortedfirst(cum, c)
    prev = k == 1 ? 0 : cum[k-1]
    return k, c - prev
end

# Conjugate transpose of a `DMatrix`, tile by tile.
function _svd_adjoint(A::DMatrix{T}) where {T}
    m, n = size(A)
    mb, nb = A.partitioning.blocksize
    At = DArray{T,2}(undef, Blocks(nb, mb), (n, m))
    Ac = A.chunks
    Atc = At.chunks
    at_mt, at_nt = size(Atc)
    Dagger.spawn_datadeps() do
        for i in 1:at_mt, j in 1:at_nt
            Dagger.@spawn _svd_adjoint_tile!(Out(Atc[i, j]), In(Ac[j, i]))
        end
    end
    return At
end

# n×n distributed identity with uniform `nb` column/row tiling.
#
# `:cyclicrow` matches the placement the sweeps pin to (see `_svd_row_groups`),
# so `V`'s tiles never move once created.
function _svd_identity(::Type{T}, n::Int, nb::Int) where {T}
    V = zeros(Blocks(nb, nb), T, n, n; assignment=:cyclicrow)
    Vc = V.chunks
    nt = size(Vc, 1)
    Dagger.spawn_datadeps() do
        for k in 1:nt
            Dagger.@spawn _svd_set_identity_diag!(InOut(Vc[k, k]))
        end
    end
    return V
end

# Fill `dst`'s columns from `srcs` (the distinct source tiles this destination
# tile draws from) according to `colmap[ld] = (i, lc)`: column `ld` of `dst`
# comes from column `lc` of `srcs[i]`.
function _svd_permute_tile!(dst::AbstractMatrix{T}, colmap::Vector{Tuple{Int,Int}},
                             srcs::Vararg{AbstractMatrix{T}}) where {T}
    for (ld, (i, lc)) in enumerate(colmap)
        @views dst[:, ld] .= srcs[i][:, lc]
    end
    return dst
end

# New `DMatrix` whose column `c` is column `p[c]` of `D` (same partitioning).
#
# Each destination tile is written by exactly one task per row-tile, which
# gathers all the source columns it needs and writes the whole tile at once.
# Spawning one task *per column* (each an `InOut` of a view into a shared
# destination tile) would let the scheduler treat those column writes as
# independent and run them concurrently; that's safe under shared memory
# (`-p 0`) but not once tiles are pulled to separate worker processes, where
# each concurrent task can end up mutating its own copy of the *whole* tile
# and only the last one to write back survives — silently dropping every
# other column's update.  Writing the full tile from a single task avoids the
# hazard entirely.
function _svd_permute_columns(D::DMatrix{T}, p::AbstractVector{<:Integer}) where {T}
    m, n = size(D)
    cum = D.subdomains.cumlength[2]
    R = similar(D)
    Dc = D.chunks
    Rc = R.chunks
    rt = size(Dc, 1)
    nt = size(Dc, 2)
    Dagger.spawn_datadeps() do
        for kd in 1:nt
            lo = kd == 1 ? 1 : cum[kd-1] + 1
            hi = cum[kd]
            srcs_needed = Int[]
            colmap = Vector{Tuple{Int,Int}}(undef, hi - lo + 1)
            for c in lo:hi
                s = Int(p[c])
                ks, ls = _svd_locate_col(cum, s)
                i = findfirst(==(ks), srcs_needed)
                if i === nothing
                    push!(srcs_needed, ks)
                    i = length(srcs_needed)
                end
                colmap[c - lo + 1] = (i, ls)
            end
            for k in 1:rt
                srcs = ntuple(i -> Dc[k, srcs_needed[i]], length(srcs_needed))
                Dagger.@spawn _svd_permute_tile!(Out(Rc[k, kd]), colmap, srcs...)
            end
        end
    end
    return R
end

# A `sqrt(eps)` threshold (a common rule of thumb, relying on Jacobi's
# asymptotic quadratic convergence to reach full precision one sweep after
# crossing it) is too loose here: the measured relative off-diagonal coupling
# does not always cross `sqrt(eps)` and then immediately collapse to `eps` on
# the very next sweep — for some inputs it lands on a plateau anywhere between
# the two. Stopping as soon as `sqrt(eps)` is reached can therefore leave the
# block-columns short of full convergence, which — since the left singular
# vectors are formed by dividing columns by their own (possibly tiny) norm —
# gets magnified into large errors in `U`. Requiring a full `eps`-level
# relative coupling keeps sweeping until that plateau is actually reached.
_svd_default_tol(::Type{T}) where {T} = eps(real(float(T)))

# ────────────────────────────────────────────────────────────────────────────
# Core block one-sided Jacobi (requires m ≥ n)
# ────────────────────────────────────────────────────────────────────────────

# Returns `(U, S, V)` where `U` is m×n (or `nothing` when `vectors=false`),
# `S` is the descending singular values, and `V` is n×n (or `nothing`).
function _svd_jacobi!(A::DMatrix{T}; tol::Real=_svd_default_tol(T),
                      maxsweeps::Integer=30, vectors::Bool=true) where {T<:Number}
    m, n = size(A)
    m >= n || throw(ArgumentError("_svd_jacobi! requires m ≥ n (got $m×$n)"))
    R = real(float(T))
    # Put each row-tile band on a single worker so the sweeps can be pinned to
    # the data instead of chasing it (see `_svd_rowcyclic`).  `A` is scratch
    # here — the caller's factors are built fresh by `_svd_permute_columns` —
    # so restaging it is safe.
    A, restaged = _svd_rowcyclic(A)
    Ac = A.chunks
    mt, nt = size(Ac)
    cum = A.subdomains.cumlength[2]
    widths = _svd_tilewidths(cum)
    nb = A.partitioning.blocksize[2]
    a_groups = _svd_row_groups(A)
    local V::DMatrix{T}

    if nt == 1
        # Single block-column: the eigenvectors of A'A orthogonalize the columns
        # exactly in one shot, so no sweeps are needed.
        G = zeros(T, n, n)
        Dagger.spawn_datadeps() do
            for (scope, ks) in a_groups
                args = Any[_svd_gram_self_block!, Options(; compute_scope=scope), InOut(G)]
                for k in ks
                    push!(args, In(Ac[k, 1]))
                end
                Dagger.spawn(args...)
            end
        end
        vals, vecs = _svd_hermitian_jacobi_eigen(LinearAlgebra.Hermitian((G .+ G') ./ 2))
        if !vectors
            return nothing, sort(sqrt.(max.(vals, zero(R))); rev=true), nothing
        end
        W = Matrix{T}(vecs)
        V = _svd_identity(T, n, nb)
        Vc = V.chunks
        Dagger.spawn_datadeps() do
            for (scope, ks) in a_groups, k in ks
                Dagger.@spawn compute_scope=scope _svd_apply_self!(InOut(Ac[k, 1]), In(W))
            end
            for (scope, ks) in _svd_row_groups(V), k in ks
                Dagger.@spawn compute_scope=scope _svd_apply_self!(InOut(Vc[k, 1]), In(W))
            end
        end
    else
        if vectors
            V = _svd_identity(T, n, nb)
            Vc = V.chunks
            vt = size(Vc, 1)
            v_groups = _svd_row_groups(V)
        end
        rounds = _svd_tournament_rounds(nt)
        nslots = maximum(length, rounds)
        np = length(a_groups)

        # Scratch for the sweeps, allocated once and reused by every round of
        # every sweep.  There is one set per *pair slot* — the slots within a
        # round run concurrently, so they need separate buffers, but slot `s`
        # of consecutive rounds is already ordered by the block-column
        # dependencies between them and can share.
        #
        # Everything is sized for the widest possible pair (`2nb`) and used
        # through a `1:nij` view, so a short trailing block-column doesn't
        # force a second set of buffers (or a second compiled specialization).
        maxn = 2 * nb
        slot_G = [[zeros(T, maxn, maxn) for _ in 1:np] for _ in 1:nslots]
        slot_W = [Matrix{T}(undef, maxn, maxn) for _ in 1:nslots]
        slot_off = [R[zero(R)] for _ in 1:nslots]

        # Cyclic Jacobi's off-diagonal coupling converges quadratically at
        # first, but — once it nears the floor imposed by the working
        # precision — can plateau at a value that never quite dips below a
        # fixed `tol` (see `_svd_default_tol`).  Without a plateau check,
        # every sweep after that point is wasted work, running all the way to
        # `maxsweeps` for no further accuracy gain.  A single sweep with a
        # disappointing improvement ratio isn't a reliable plateau signal on
        # its own — with many block-columns some sweeps in the middle of
        # otherwise-healthy quadratic convergence can still only manage,
        # say, a 2x reduction — so only stop early once two *consecutive*
        # sweeps each fail to shrink the coupling by much (a true plateau,
        # as opposed to a merely slow sweep, keeps failing to improve).
        prev_offv = R(Inf)
        stall_count = 0
        for _ in 1:maxsweeps
            foreach(o -> o[1] = zero(R), slot_off)
            Dagger.spawn_datadeps() do
                for round in rounds, (s, (i, j)) in enumerate(round)
                    wi = widths[i]
                    nij = wi + widths[j]
                    Gs = slot_G[s]
                    W = slot_W[s]
                    # Gram: each worker reduces over the row tiles it owns.
                    for (g, (scope, ks)) in enumerate(a_groups)
                        args = Any[_svd_gram_partial!, Options(; compute_scope=scope),
                                   Out(Gs[g]), nij]
                        for k in ks; push!(args, In(Ac[k, i])); end
                        for k in ks; push!(args, In(Ac[k, j])); end
                        Dagger.spawn(args...)
                    end
                    # Combine and diagonalize on one worker.  This is the only
                    # communication in a sweep, and it moves `np+1` matrices of
                    # `nij²` elements — a cost independent of `m`.
                    rot_scope = first(a_groups[mod1(s, np)])
                    rot_args = Any[_svd_reduce_rot!, Options(; compute_scope=rot_scope),
                                   Out(W), InOut(slot_off[s]), nij, wi]
                    for G in Gs; push!(rot_args, In(G)); end
                    Dagger.spawn(rot_args...)
                    # Apply: again, each worker touches only its own tiles.
                    for (scope, ks) in a_groups
                        args = Any[_svd_apply_rot_block!, Options(; compute_scope=scope),
                                   In(W), nij, wi]
                        for k in ks; push!(args, InOut(Ac[k, i])); end
                        for k in ks; push!(args, InOut(Ac[k, j])); end
                        Dagger.spawn(args...)
                    end
                    if vectors
                        for (scope, ks) in v_groups
                            args = Any[_svd_apply_rot_block!, Options(; compute_scope=scope),
                                       In(W), nij, wi]
                            for k in ks; push!(args, InOut(Vc[k, i])); end
                            for k in ks; push!(args, InOut(Vc[k, j])); end
                            Dagger.spawn(args...)
                        end
                    end
                end
            end
            offv = maximum(o -> o[1], slot_off)
            offv <= tol && break
            stall_count = offv > prev_offv * R(0.9) ? stall_count + 1 : 0
            prev_offv = offv
            stall_count >= 2 && break
        end
    end

    # Singular values from converged column norms.
    svbuf = [zeros(R, widths[i]) for i in 1:nt]
    Dagger.spawn_datadeps() do
        for i in 1:nt, (scope, ks) in a_groups
            args = Any[_svd_colnorms_block!, Options(; compute_scope=scope), InOut(svbuf[i])]
            for k in ks
                push!(args, In(Ac[k, i]))
            end
            Dagger.spawn(args...)
        end
    end
    for i in 1:nt
        svbuf[i] .= sqrt.(svbuf[i])
    end
    σ = reduce(vcat, svbuf)

    if !vectors
        restaged && unsafe_free!(A)
        return nothing, sort(σ; rev=true), nothing
    end

    # Normalize columns of A in place → left singular vectors, then sort.
    Dagger.spawn_datadeps() do
        for i in 1:nt, (scope, ks) in a_groups
            for k in ks
                Dagger.@spawn compute_scope=scope _svd_scale_cols!(InOut(Ac[k, i]), In(svbuf[i]))
            end
        end
    end
    p = sortperm(σ; rev=true)
    U = _svd_permute_columns(A, p)
    Vsorted = _svd_permute_columns(V, p)
    unsafe_free!(V)
    restaged && unsafe_free!(A)
    return U, σ[p], Vsorted
end

# ────────────────────────────────────────────────────────────────────────────
# Public LinearAlgebra interface
# ────────────────────────────────────────────────────────────────────────────

"""
    svd!(A::DMatrix; tol, maxsweeps, full=false) -> SVD

In-place thin singular value decomposition of a distributed matrix using a
tiled block one-sided Jacobi algorithm (see `src/array/svd.jl`).  `A` is used as
scratch and its contents afterwards are unspecified — with more than one worker
the sweeps run on an internally restaged copy, so `A` may be left untouched
instead of overwritten.  Returns a `LinearAlgebra.SVD` holding distributed `U`
and `Vt` factors and a host `Vector` of singular values in descending order.

`tol` is the relative off-diagonal threshold for sweep convergence and
`maxsweeps` bounds the number of Jacobi sweeps.  Only the thin factorization is
supported; `full=true` raises an error.
"""
function LinearAlgebra.svd!(A::DMatrix{T}; tol::Real=_svd_default_tol(T),
                            maxsweeps::Integer=30, full::Bool=false) where {T<:Number}
    full && throw(ArgumentError("full=true SVD is not supported for DMatrix; only the thin SVD is available"))
    m, n = size(A)
    if m >= n
        U, S, V = _svd_jacobi!(A; tol=tol, maxsweeps=maxsweeps)
        Vt = _svd_adjoint(V)
        unsafe_free!(V)
        return LinearAlgebra.SVD(U, S, Vt)
    else
        # Wide matrix: factor the (tall) adjoint, then swap the roles of U and V.
        # A = (Aᴴ)ᴴ = (U₁ Σ V₁ᴴ)ᴴ = V₁ Σ U₁ᴴ  ⇒  U = V₁, Vᵀ = U₁ᴴ.
        At = _svd_adjoint(A)
        U1, S, V1 = _svd_jacobi!(At; tol=tol, maxsweeps=maxsweeps)
        unsafe_free!(At)
        Vt = _svd_adjoint(U1)
        unsafe_free!(U1)
        return LinearAlgebra.SVD(V1, S, Vt)
    end
end

"""
    svd(A::DMatrix; kwargs...) -> SVD

Thin singular value decomposition of a distributed matrix.  `A` is not modified;
see [`svd!`](@ref) for the in-place variant and the list of keyword arguments.
"""
LinearAlgebra.svd(A::DMatrix; kwargs...) = LinearAlgebra.svd!(copy(A); kwargs...)

"""
    svdvals!(A::DMatrix; tol, maxsweeps) -> Vector

In-place computation of the singular values of `A` (descending).  Overwrites
`A` and skips forming the singular vectors.
"""
function LinearAlgebra.svdvals!(A::DMatrix{T}; tol::Real=_svd_default_tol(T),
                                maxsweeps::Integer=30) where {T<:Number}
    m, n = size(A)
    if m >= n
        _, S, _ = _svd_jacobi!(A; tol=tol, maxsweeps=maxsweeps, vectors=false)
        return S
    else
        At = _svd_adjoint(A)
        _, S, _ = _svd_jacobi!(At; tol=tol, maxsweeps=maxsweeps, vectors=false)
        unsafe_free!(At)
        return S
    end
end

"""
    svdvals(A::DMatrix; kwargs...) -> Vector

Singular values of `A` in descending order.  `A` is not modified.
"""
LinearAlgebra.svdvals(A::DMatrix; kwargs...) = LinearAlgebra.svdvals!(copy(A); kwargs...)

# ────────────────────────────────────────────────────────────────────────────
# Solve via SVD: x = V Σ⁺ Uᴴ b  (least-squares / min-norm)
# ────────────────────────────────────────────────────────────────────────────

# Scale each row of a tile by the reciprocal of the corresponding singular
# value (or zero it when σ is treated as numerically zero).  `s` is the full
# descending singular-value vector; `row_offset` is the 0-based global start
# of this tile's rows.
function _svd_scale_rows!(Y::AbstractMatrix{T}, s::AbstractVector{R},
                          row_offset::Int, k::Int) where {T,R}
    @inbounds for i in axes(Y, 1)
        gi = row_offset + i
        if gi <= k
            sc = s[gi]
            invs = iszero(sc) ? zero(T) : T(inv(sc))
            @views Y[i, :] .*= invs
        else
            @views Y[i, :] .= zero(T)
        end
    end
    return Y
end
function _svd_scale_rows!(y::AbstractVector{T}, s::AbstractVector{R},
                          row_offset::Int, k::Int) where {T,R}
    @inbounds for i in eachindex(y)
        gi = row_offset + i
        if gi <= k
            sc = s[gi]
            y[i] = iszero(sc) ? zero(T) : T(y[i] / sc)
        else
            y[i] = zero(T)
        end
    end
    return y
end

# Apply Σ⁺ in place to a distributed vector/matrix whose leading dimension
# matches the singular-value length (or is longer — trailing rows are zeroed).
function _svd_apply_pinv_S!(Y::DVecOrMat{T}, S::AbstractVector{R}, k::Int) where {T,R}
    Yc = Y.chunks
    if Y isa DVector
        cum = Y.subdomains.cumlength[1]
        Dagger.spawn_datadeps() do
            for i in eachindex(Yc)
                off = i == 1 ? 0 : cum[i - 1]
                Dagger.@spawn _svd_scale_rows!(InOut(Yc[i]), In(S), off, k)
            end
        end
    else
        cum = Y.subdomains.cumlength[1]
        mt, nt = size(Yc)
        Dagger.spawn_datadeps() do
            for i in 1:mt, j in 1:nt
                off = i == 1 ? 0 : cum[i - 1]
                Dagger.@spawn _svd_scale_rows!(InOut(Yc[i, j]), In(S), off, k)
            end
        end
    end
    return Y
end

"""
    ldiv!(F::SVD{<:Any,<:Any,<:DMatrix}, B::DVecOrMat) -> B

Solve `F.U * Diagonal(F.S) * F.Vt * X ≈ B` in place using the distributed thin
SVD factors.  Overwrites `B` with the least-squares / minimum-norm solution
(same contract as `LinearAlgebra.ldiv!(::SVD, ::AbstractVecOrMat)`): for an
`m×n` factorization, `B` must have `max(m, n)` rows and the solution occupies
the leading `n` rows.

Singular values below `eps(real(T)) * S[1]` are treated as zero (truncated
pseudoinverse), matching the dense `LinearAlgebra` SVD solve.
"""
function LinearAlgebra.ldiv!(F::LinearAlgebra.SVD{T,<:Any,<:DMatrix{T}},
                             B::DVecOrMat) where {T}
    m, n = size(F)
    size(B, 1) == max(m, n) || throw(DimensionMismatch(
        "B has $(size(B, 1)) rows but SVD is $m×$n (need max(m,n)=$(max(m, n)) rows)"))
    k = searchsortedlast(F.S, eps(real(T)) * F.S[1]; rev=true)
    # y ← Uᴴ b  (length m for thin U; pad/truncate into B's leading m rows)
    # For the thin SVD, U is m×min(m,n).  When B is taller than m (wide A),
    # only the leading m rows of B hold the RHS; when B is exactly m (tall or
    # square), the whole vector is the RHS.
    if B isa DVector
        b_rhs = size(B, 1) > m ? B[1:m] : B
        y = F.U' * b_rhs
        _svd_apply_pinv_S!(y, F.S, k)
        # x ← V y = Vt' y  (length n).  Write into leading n entries of B.
        x = F.Vt' * y
        if size(B, 1) == n
            copyto!(B, x)
        else
            # Tall: B is length m > n; solution occupies 1:n, rest unused.
            fill!(B, zero(eltype(B)))
            B[1:n] = x
        end
        unsafe_free!(y)
        unsafe_free!(x)
        b_rhs !== B && unsafe_free!(b_rhs)
    else
        B_rhs = size(B, 1) > m ? B[1:m, :] : B
        Y = F.U' * B_rhs
        _svd_apply_pinv_S!(Y, F.S, k)
        X = F.Vt' * Y
        if size(B, 1) == n
            copyto!(B, X)
        else
            fill!(B, zero(eltype(B)))
            B[1:n, :] = X
        end
        unsafe_free!(Y)
        unsafe_free!(X)
        B_rhs !== B && unsafe_free!(B_rhs)
    end
    return B
end

function LinearAlgebra.ldiv!(X::DVecOrMat,
                             F::LinearAlgebra.SVD{T,<:Any,<:DMatrix{T}},
                             B::DVecOrMat) where {T}
    m, n = size(F)
    size(B, 1) == m || throw(DimensionMismatch(
        "B has $(size(B, 1)) rows but SVD has $m rows"))
    size(X, 1) == n || throw(DimensionMismatch(
        "X has $(size(X, 1)) rows but SVD has $n columns (solution length)"))
    if ndims(X) == 2
        size(X, 2) == size(B, 2) || throw(DimensionMismatch(
            "X and B must have the same number of columns"))
    end
    # Workspace for `ldiv!(F, ·)` must have max(m, n) rows.
    if m >= n
        Bc = m == n ? copyto!(X, B) : copy(B)
        LinearAlgebra.ldiv!(F, Bc)
        if m > n
            if X isa DVector
                copyto!(X, Bc[1:n])
            else
                copyto!(X, Bc[1:n, :])
            end
            unsafe_free!(Bc)
        end
    else
        # Underdetermined: pad B into an n-row workspace, solve, result is X.
        fill!(X, zero(eltype(X)))
        if X isa DVector
            X[1:m] = B
        else
            X[1:m, :] = B
        end
        LinearAlgebra.ldiv!(F, X)
    end
    return X
end

# Base's generic `ldiv(F::Factorization, B)` — which `\` calls — stages the RHS
# into a plain `zeros(...)` host `Vector`/`Matrix`, discarding `B`'s block
# structure.  `ldiv!(F::SVD{<:DMatrix}, ·)` above then never sees a `DVecOrMat`,
# so the solve falls through to dense `LinearAlgebra` operating on a mix of
# `DMatrix` factors and a host array.  That path resolves to element-at-a-time
# access across process boundaries: measured on two workers, a single
# `F \ (64×3 DMatrix)` took over six minutes, essentially none of it
# compilation.  Mirror the LU override in `linalg.jl` and stage the RHS as a
# `DMatrix` with U's row blocking so the distributed `ldiv!` is used.
function LinearAlgebra.ldiv(F::LinearAlgebra.SVD{T,<:Any,<:DMatrix{T}},
                            B::AbstractVecOrMat) where {T}
    LinearAlgebra.require_one_based_indexing(B)
    m, n = size(F)
    size(B, 1) == m || throw(DimensionMismatch(
        "B has $(size(B, 1)) rows but SVD has $m rows"))
    TFB = typeof(oneunit(eltype(B)) / oneunit(T))
    mb = F.U.partitioning.blocksize[1]
    X = if B isa AbstractVector
        zeros(Blocks(mb), TFB, n)
    else
        # Keep a `DMatrix` RHS's own column tiling; for a host RHS fall back to
        # U's row blocking rather than one tile spanning every column, so a
        # wide RHS still gets partitioned.
        col_bs = B isa DMatrix ? B.partitioning.blocksize[2] : min(size(B, 2), mb)
        zeros(Blocks(mb, col_bs), TFB, n, size(B, 2))
    end
    # A distributed RHS whose row blocking disagrees with U's would reintroduce
    # the misalignment this override exists to avoid, so restage it too.
    B_dist = if B isa DVecOrMat && B.partitioning.blocksize[1] == mb
        B
    elseif B isa AbstractVector
        distribute(B isa DVector ? collect(B) : B, Blocks(mb))
    else
        distribute(B isa DMatrix ? collect(B) : B,
                   Blocks(mb, B isa DMatrix ? B.partitioning.blocksize[2] : min(size(B, 2), mb)))
    end
    LinearAlgebra.ldiv!(X, F, B_dist)
    B_dist !== B && unsafe_free!(B_dist)
    return X
end

"""
    inv(F::SVD{<:Any,<:Any,<:DMatrix}) -> DMatrix

Pseudoinverse `V Σ⁺ Uᴴ` of a square distributed SVD factorization.  Singular
values below `eps(real(T)) * S[1]` are treated as zero.
"""
function LinearAlgebra.inv(F::LinearAlgebra.SVD{T,<:Any,<:DMatrix{T}}) where {T}
    LinearAlgebra.checksquare(F)
    @inbounds for i in eachindex(F.S)
        iszero(F.S[i]) && throw(LinearAlgebra.SingularException(i))
    end
    k = searchsortedlast(F.S, eps(real(T)) * F.S[1]; rev=true)
    # V Σ⁺ Uᴴ = Vt' * Diagonal(Σ⁺) * U'
    Ut = _svd_adjoint(F.U)
    _svd_apply_pinv_S!(Ut, F.S, k)
    P = F.Vt' * Ut
    unsafe_free!(Ut)
    return P
end
