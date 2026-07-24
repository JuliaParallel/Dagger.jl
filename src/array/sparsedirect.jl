# Sparse direct solvers.
#
# Pure-Julia sparse LU backends (KLU via `PureKLU.jl`, UMFPACK-style via
# `PureUMFPACK.jl`) provide *direct* solves for sparse systems. Unlike the
# C-bound `SparseArrays.UmfpackLU` (whose factorization holds process-local
# UMFPACK resources and can't be moved between workers), these factorizations are
# plain Julia data: serializable and movable, so Dagger can schedule them freely.
#
# Two entry points are exposed (implemented in `ext/PureKLUExt.jl` /
# `ext/PureUMFPACKExt.jl`):
#
#   - A whole-matrix direct solve: `Dagger.klu(A)` / `Dagger.splu(A)` gather the
#     (sparse) `DMatrix` onto a single worker (the one owning the most tiles),
#     assemble one `SparseMatrixCSC`, factor it once, and return a
#     `DaggerSparseLU` whose factor is pinned there. Solves move only the RHS /
#     solution vectors. This is a correctness-first, single-worker factorization
#     for systems that fit on one worker (e.g. coarse-grid solves).
#   - An opt-in distributed solve path: `Dagger.splu(A; distributed=true)`
#     (PureUMFPACK only) reuses that factorization, extracts the triangular
#     factors into sparse `DMatrix`es, and solves with a tiled datadeps
#     forward/backward substitution (`DistributedSparseLU`, `method=:trsv`).
#   - A Schur-complement domain-decomposition path:
#     `Dagger.splu(A; distributed=true, method=:schur)` factors interior blocks
#     in parallel across workers and solves via a separator Schur system
#     (`DistributedSchurLU`; requires Metis.jl).
#   - Per-tile block direct solves (`BlockKLUPreconditioner` /
#     `BlockUMFPACKPreconditioner`, in `iterativesolvers.jl`), which reuse the
#     block-preconditioner machinery.

"""
    klu(A::DMatrix; kwargs...) -> DaggerSparseLU

Direct sparse LU factorization of a sparse `DMatrix` using KLU. Tiles are
gathered onto one worker, assembled into a single `SparseMatrixCSC`, and factored
once; the factor stays pinned there. Solve with `F \\ b` or `ldiv!(x, F, b)` over
`DVector`s. Requires `PureKLU.jl` to be loaded.

KLU is well suited to unsymmetric systems with near-triangular structure (e.g.
circuit simulation). See also [`splu`](@ref).
"""
function klu end

"""
    splu(A::DMatrix; distributed=false, method=:trsv, blocksize=nothing, nparts=nothing, kwargs...)

Direct sparse LU factorization of a sparse `DMatrix` using a pure-Julia
UMFPACK-style multifrontal solver.

- `distributed=false` → [`DaggerSparseLU`](@ref): gather+factor on one worker.
- `distributed=true, method=:trsv` (default) → [`DistributedSparseLU`](@ref):
  Stage-4a factor plus tiled datadeps triangular solves (`blocksize` controls
  the solve tiling).
- `distributed=true, method=:schur` → [`DistributedSchurLU`](@ref): single-level
  Schur-complement domain decomposition (METIS separator; `nparts` defaults to
  `max(2, nworkers())`). Requires `Metis.jl`.

Requires `PureUMFPACK.jl`. See also [`klu`](@ref).
"""
function splu end

_klu_required() = throw(ArgumentError(
    "Dagger.klu requires PureKLU.jl. Run `using PureKLU` to enable sparse direct \
    (KLU) solves."))
_splu_required() = throw(ArgumentError(
    "Dagger.splu requires PureUMFPACK.jl. Run `using PureUMFPACK` to enable sparse \
    direct (UMFPACK) solves."))
klu(A; kwargs...) = _klu_required()
splu(A; kwargs...) = _splu_required()

"""
    DaggerSparseLU

A direct sparse factorization of a sparse `DMatrix`, produced by
[`Dagger.klu`](@ref) or [`Dagger.splu`](@ref). The underlying factorization is
pure-Julia and pinned to a single worker (`fact` is a `Chunk`/`DTask` with
`scope`). Solve `A x = b` with `F \\ b` (returns a `DVector` partitioned like
`b`) or `ldiv!(x, F, b)`.
"""
struct DaggerSparseLU{F,S,P}
    fact::F          # pinned factorization (`Chunk` / `DTask`)
    scope::S         # `ProcessScope` of the factor's worker
    n::Int           # system size (square)
    part::P          # default vector partitioning (column blocks of `A`)
end

Base.size(F::DaggerSparseLU) = (F.n, F.n)
Base.size(F::DaggerSparseLU, i::Integer) = i <= 2 ? F.n : 1

"""
    DistributedSparseLU

A sparse LU factorization with triangular factors stored as sparse `DMatrix`es,
produced by `Dagger.splu(A; distributed=true)`. Solves use a tiled datadeps
forward/backward substitution over `L` and `U` (PureUMFPACK only).
"""
struct DistributedSparseLU{L,U,P,Q,R,Part}
    L::L             # unit-lower triangular factor (`DMatrix` of sparse tiles)
    U::U             # upper triangular factor (`DMatrix` of sparse tiles)
    p::P             # row permutation
    q::Q             # column permutation
    Rs::R            # row scaling
    n::Int           # system size (square)
    part::Part       # vector partitioning matching `L`/`U` tile rows
end

Base.size(F::DistributedSparseLU) = (F.n, F.n)
Base.size(F::DistributedSparseLU, i::Integer) = i <= 2 ? F.n : 1

# Assemble local tiles into one `SparseMatrixCSC` (no densification). The real
# method is added by `SparseArraysExt`; without `SparseArrays` there are no
# sparse tiles to gather.
function _gather_sparse end
_gather_sparse(args...) = throw(ArgumentError(
    "gathering a sparse DMatrix requires SparseArrays to be loaded"))

# Backend-specific extraction of `(L, U, p, q, Rs)` from a factored object.
# Implemented for `PureUMFPACK.PureLU` in `PureUMFPACKExt`.
function _extract_lu_factors end
_extract_lu_factors(F) = throw(ArgumentError(
    "distributed sparse LU requires a PureUMFPACK.PureLU factor (got $(typeof(F))). \
    Use `Dagger.splu(A; distributed=true)` with PureUMFPACK.jl loaded."))

# Choose the worker that already owns the most tiles of `A` (minimizes gather
# traffic). Ties break toward the lowest worker id.
function _select_factor_scope(A::DMatrix)
    counts = Dict{Int,Int}()
    for c in A.chunks
        wid = _tile_scope(c).wid
        counts[wid] = get(counts, wid, 0) + 1
    end
    best_wid, best_count = 0, -1
    for (wid, cnt) in counts
        if cnt > best_count || (cnt == best_count && wid < best_wid)
            best_wid, best_count = wid, cnt
        end
    end
    return ProcessScope(best_wid)
end

# Runs on the chosen worker: assemble from tiles (already moved here by the
# scheduler), factorize, and pin the factor to this process.
function _assemble_and_factor_pinned(factorize, ::Type{T}, row_offsets, col_offsets,
                                    m, n, tiles...) where T
    S = _gather_sparse(T, tiles, row_offsets, col_offsets, m, n)
    F = factorize(S)
    proc = task_processor()
    return tochunk(F, proc, ProcessScope(root_worker_id(proc)))
end

# Shared path for `klu` / `splu`: pick a worker, spawn assemble+factor there.
function _spawn_direct_factorization(A::DMatrix{T}, factorize) where T
    n = LinearAlgebra.checksquare(A)
    scope = _select_factor_scope(A)
    Ac = A.chunks
    mt, nt = size(Ac)
    ntiles = mt * nt
    row_offsets = Vector{Int}(undef, ntiles)
    col_offsets = Vector{Int}(undef, ntiles)
    tiles = Vector{Any}(undef, ntiles)
    idx = 1
    for i in 1:mt, j in 1:nt
        dom = A.subdomains[i, j]
        row_offsets[idx] = first(dom.indexes[1]) - 1
        col_offsets[idx] = first(dom.indexes[2]) - 1
        tiles[idx] = Ac[i, j]
        idx += 1
    end
    mA, nA = size(A)
    part = Blocks(A.partitioning.blocksize[2])
    fact = spawn(_assemble_and_factor_pinned, Options(; compute_scope=scope),
                 factorize, T, row_offsets, col_offsets, mA, nA, tiles...)
    return DaggerSparseLU(fact, scope, n, part)
end

# Gather RHS chunks, solve against the pinned factor, return the dense solution.
function _direct_solve(fact, bparts...)
    b = length(bparts) == 1 ? bparts[1] : reduce(vcat, bparts)
    return fact \ b
end

function Base.:\(F::DaggerSparseLU, b::DVector)
    length(b) == F.n || throw(DimensionMismatch(
        "factorization is $(F.n)×$(F.n) but b has length $(length(b))"))
    # Solve on the factor's worker (factor stays pinned); only the O(n) solution
    # returns here to be redistributed like `b`.
    x = fetch(spawn(_direct_solve, Options(; compute_scope=F.scope),
                    F.fact, b.chunks...))
    return distribute(x, b.partitioning)
end

function LinearAlgebra.ldiv!(x::DVector, F::DaggerSparseLU, b::DVector)
    return copyto!(x, F \ b)
end

# ---- Stage 4b: distributed triangular solves --------------------------------

_fetch_lu_factors(fact) = _extract_lu_factors(fact)

function _resolve_solve_blocksize(A::DMatrix, blocksize)
    if blocksize === nothing
        return Int(min(A.partitioning.blocksize...))
    end
    bs = Int(blocksize)
    bs > 0 || throw(ArgumentError("blocksize must be positive, got $bs"))
    return bs
end

# Re-tile PureLU factors into sparse `DMatrix`es after a Stage-4a factorization.
function _make_distributed_sparse_lu(F::DaggerSparseLU, A::DMatrix; blocksize=nothing)
    bs = _resolve_solve_blocksize(A, blocksize)
    L, U, p, q, Rs = fetch(spawn(_fetch_lu_factors, Options(; compute_scope=F.scope), F.fact))
    part2 = Blocks(bs, bs)
    part1 = Blocks(bs)
    return DistributedSparseLU(distribute(L, part2), distribute(U, part2),
                               p, q, Rs, F.n, part1)
end

# Tile kernels: unwrap `DSparseArray` → `SparseMatrixCSC`, never densify.
function _sparse_unit_ltrsv!(Ltile, x::AbstractVector)
    LinearAlgebra.ldiv!(UnitLowerTriangular(_tile_matrix(Ltile)), x)
    return
end

function _sparse_utrsv!(Utile, x::AbstractVector)
    LinearAlgebra.ldiv!(UpperTriangular(_tile_matrix(Utile)), x)
    return
end

function _sparse_gemv_sub!(Atile, x::AbstractVector, y::AbstractVector)
    matvecmul!(y, 'N', Atile, x, -one(eltype(y)), one(eltype(y)))
    return
end

# Blocked forward substitution for unit-lower `L` (mirrors `trsv!` lower/'N').
function _sparse_trsv_forward!(L::DMatrix, c::DVector)
    Lc, Cc = L.chunks, c.chunks
    nt = length(Cc)
    size(Lc, 1) == nt && size(Lc, 2) == nt || throw(DimensionMismatch(
        "L tile grid $(size(Lc)) incompatible with vector tiles $nt"))
    Dagger.spawn_datadeps() do
        for k in 1:nt
            Dagger.@spawn _sparse_unit_ltrsv!(In(Lc[k, k]), InOut(Cc[k]))
            for i in (k + 1):nt
                Dagger.@spawn _sparse_gemv_sub!(In(Lc[i, k]), In(Cc[k]), InOut(Cc[i]))
            end
        end
    end
    return c
end

# Blocked backward substitution for upper `U` (mirrors `trsv!` upper/'N').
function _sparse_trsv_backward!(U::DMatrix, c::DVector)
    Uc, Cc = U.chunks, c.chunks
    nt = length(Cc)
    size(Uc, 1) == nt && size(Uc, 2) == nt || throw(DimensionMismatch(
        "U tile grid $(size(Uc)) incompatible with vector tiles $nt"))
    Dagger.spawn_datadeps() do
        for k in reverse(1:nt)
            Dagger.@spawn _sparse_utrsv!(In(Uc[k, k]), InOut(Cc[k]))
            for i in 1:(k - 1)
                Dagger.@spawn _sparse_gemv_sub!(In(Uc[i, k]), In(Cc[k]), InOut(Cc[i]))
            end
        end
    end
    return c
end

function Base.:\(F::DistributedSparseLU, b::DVector)
    length(b) == F.n || throw(DimensionMismatch(
        "factorization is $(F.n)×$(F.n) but b has length $(length(b))"))
    # Permute + row-scale (driver O(n)); matches PureUMFPACK `_solve_factor`.
    b_local = collect(b)
    T = promote_type(eltype(b_local), eltype(F.Rs), eltype(F.U))
    c_local = Vector{T}(undef, F.n)
    p, q, Rs = F.p, F.q, F.Rs
    @inbounds for k in eachindex(p)
        c_local[k] = Rs[p[k]] * b_local[p[k]]
    end
    c = distribute(c_local, F.part)
    _sparse_trsv_forward!(F.L, c)
    _sparse_trsv_backward!(F.U, c)
    c_local = collect(c)
    x_local = similar(c_local)
    @inbounds for k in eachindex(q)
        x_local[q[k]] = c_local[k]
    end
    return distribute(x_local, b.partitioning)
end

function LinearAlgebra.ldiv!(x::DVector, F::DistributedSparseLU, b::DVector)
    return copyto!(x, F \ b)
end

# ---- Stage 4c: Schur-complement domain decomposition -------------------------
#
# Single-level (non-recursive) k-way Schur factorization. Recursive / nested
# dissection of the separator is future work.

"""
    DistributedSchurLU

Sparse LU via single-level Schur-complement domain decomposition, produced by
`Dagger.splu(A; distributed=true, method=:schur)`. Interior blocks are factored
in parallel (pinned round-robin across workers); the separator Schur system is
factored on one worker. Solve with `F \\ b` / `ldiv!(x, F, b)`.
"""
struct DistributedSchurLU{F,W,G,SF,S,P}
    interior_facts::Vector{F}   # pinned `Aᵢᵢ` factors
    W::Vector{W}                # pinned `Wᵢ = Aᵢᵢ⁻¹ AᵢΓ` (dense columns)
    AΓi::Vector{G}              # pinned `A_Γi` blocks
    interior_scopes::Vector{S}
    Sfact::SF                   # pinned Schur factor (`nothing` if `|Γ|==0`)
    Sscope::S
    interiors::Vector{Vector{Int}}
    separator::Vector{Int}
    perm::Vector{Int}
    n::Int
    part::P
end

Base.size(F::DistributedSchurLU) = (F.n, F.n)
Base.size(F::DistributedSchurLU, i::Integer) = i <= 2 ? F.n : 1

# METIS partition + separator (implemented in `MetisExt`).
function _nested_dissection_partition end
_nested_dissection_partition(A, nparts) = throw(ArgumentError(
    "Dagger.splu(...; method=:schur) requires Metis.jl. Run `using Metis` to \
    enable Schur-complement distributed sparse LU."))

# Gather a sparse `DMatrix` into one `SparseMatrixCSC` on the caller (pattern
# assembly for the partitioner; no densification).
function _collect_sparse_dmatrix(A::DMatrix{T}) where T
    Ac = A.chunks
    mt, nt = size(Ac)
    ntiles = mt * nt
    row_offsets = Vector{Int}(undef, ntiles)
    col_offsets = Vector{Int}(undef, ntiles)
    tiles = Vector{Any}(undef, ntiles)
    idx = 1
    for i in 1:mt, j in 1:nt
        dom = A.subdomains[i, j]
        row_offsets[idx] = first(dom.indexes[1]) - 1
        col_offsets[idx] = first(dom.indexes[2]) - 1
        tiles[idx] = fetch(Ac[i, j])
        idx += 1
    end
    mA, nA = size(A)
    return _gather_sparse(T, tiles, row_offsets, col_offsets, mA, nA)
end

_schur_worker_ids() = (w = workers(); isempty(w) ? Int[myid()] : Int[w...])

# Factor `Aᵢᵢ`, form dense `Wᵢ = Aᵢᵢ⁻¹ AᵢΓ` (column solves; densifying the thin
# RHS is simpler than sparse multi-RHS), and local Schur contribution `A_Γi Wᵢ`.
# Pins `(fact, W, A_Γi)` to the current worker.
function _schur_factor_interior_pinned(factorize, Aii, AiΓ, AΓi)
    F = factorize(Aii)
    ni, ng = size(AiΓ)
    T = eltype(AiΓ)
    W = zeros(T, ni, ng)
    for j in 1:ng
        W[:, j] = F \ Vector{T}(AiΓ[:, j])
    end
    Si = AΓi * W
    proc = task_processor()
    scope = ProcessScope(root_worker_id(proc))
    return (tochunk(F, proc, scope), tochunk(W, proc, scope),
            tochunk(AΓi, proc, scope), Si)
end

function _schur_factor_interface_pinned(factorize, S)
    F = factorize(S)
    proc = task_processor()
    scope = ProcessScope(root_worker_id(proc))
    return tochunk(F, proc, scope)
end

# Forward: `y = Aᵢᵢ⁻¹ bᵢ`, `contrib = A_Γi y`. Pin `y` for the back-substitution.
function _schur_forward_pinned(fact, AΓi, bI)
    y = fact \ bI
    contrib = AΓi * y
    proc = task_processor()
    scope = ProcessScope(root_worker_id(proc))
    return tochunk(y, proc, scope), contrib
end

_schur_back_pinned(y, W, xΓ) = y - W * xΓ

_schur_interface_solve(Sfact, g) = Sfact \ g

function _make_schur_sparse_lu(A::DMatrix, factorize; nparts=nothing)
    n = LinearAlgebra.checksquare(A)
    np = nparts === nothing ? max(2, nworkers()) : Int(nparts)
    np >= 1 || throw(ArgumentError("nparts must be ≥ 1, got $np"))

    Asp = _collect_sparse_dmatrix(A)
    nd = _nested_dissection_partition(Asp, np)
    interiors = nd.interiors
    separator = nd.separator
    perm = nd.perm

    # Trivial partition → Stage-4a serial factorization.
    if length(interiors) <= 1 && isempty(separator)
        return _spawn_direct_factorization(A, factorize)
    end

    wids = _schur_worker_ids()
    k = length(interiors)
    interior_facts = Vector{Any}(undef, k)
    Ws = Vector{Any}(undef, k)
    AΓis = Vector{Any}(undef, k)
    scopes = Vector{ProcessScope}(undef, k)
    Si_tasks = Vector{Any}(undef, k)

    Γ = separator
    ng = length(Γ)
    for i in 1:k
        Ii = interiors[i]
        scope = ProcessScope(wids[mod1(i, length(wids))])
        scopes[i] = scope
        Aii = Asp[Ii, Ii]
        AiΓ = Asp[Ii, Γ]
        AΓi = Asp[Γ, Ii]
        task = spawn(_schur_factor_interior_pinned, Options(; compute_scope=scope),
                     factorize, Aii, AiΓ, AΓi)
        Si_tasks[i] = task
    end

    # Fetch pinned chunks + local Schur contributions; reduce S on the driver.
    T = eltype(Asp)
    S = isempty(Γ) ? zeros(T, 0, 0) : Matrix{T}(Asp[Γ, Γ])
    for i in 1:k
        Fi, Wi, AΓi_c, Si = fetch(Si_tasks[i])
        interior_facts[i] = Fi
        Ws[i] = Wi
        AΓis[i] = AΓi_c
        isempty(Γ) || (S .-= Si)
    end

    if isempty(Γ)
        Sfact = nothing
        Sscope = scopes[1]
    else
        Sscope = ProcessScope(wids[1])
        # Schur fill-in is expected; hand a sparse view to the backend factorizer.
        Ssp = _sparse_copy_of(S)
        Sfact = fetch(spawn(_schur_factor_interface_pinned,
                            Options(; compute_scope=Sscope), factorize, Ssp))
    end

    part = Blocks(A.partitioning.blocksize[2])
    return DistributedSchurLU(interior_facts, Ws, AΓis, scopes, Sfact, Sscope,
                              interiors, separator, perm, n, part)
end

# Convert a dense Schur matrix to the sparse type the backend expects. Defined
# as a hook so the core package need not depend on SparseArrays; the real method
# lives in `SparseArraysExt`.
function _sparse_copy_of end
_sparse_copy_of(S) = throw(ArgumentError(
    "Schur-complement sparse LU requires SparseArrays to be loaded"))

function Base.:\(F::DistributedSchurLU, b::DVector)
    length(b) == F.n || throw(DimensionMismatch(
        "factorization is $(F.n)×$(F.n) but b has length $(length(b))"))
    b_local = collect(b)
    bp = b_local[F.perm]
    k = length(F.interiors)
    ng = length(F.separator)

    # Split permuted RHS into interior segments + separator.
    ys = Vector{Any}(undef, k)
    contrib_tasks = Vector{Any}(undef, k)
    off = 0
    for i in 1:k
        ni = length(F.interiors[i])
        bI = bp[off+1:off+ni]
        off += ni
        contrib_tasks[i] = spawn(_schur_forward_pinned,
                                 Options(; compute_scope=F.interior_scopes[i]),
                                 F.interior_facts[i], F.AΓi[i], bI)
    end

    g = ng == 0 ? similar(bp, 0) : copy(bp[off+1:end])
    for i in 1:k
        yi, contrib = fetch(contrib_tasks[i])
        ys[i] = yi
        ng == 0 || (g .-= contrib)
    end

    xΓ = if ng == 0
        similar(g)
    else
        fetch(spawn(_schur_interface_solve, Options(; compute_scope=F.Sscope),
                    F.Sfact, g))
    end

    x_parts = Vector{Any}(undef, k)
    back_tasks = Vector{Any}(undef, k)
    for i in 1:k
        back_tasks[i] = spawn(_schur_back_pinned,
                              Options(; compute_scope=F.interior_scopes[i]),
                              ys[i], F.W[i], xΓ)
    end
    for i in 1:k
        x_parts[i] = fetch(back_tasks[i])
    end

    xp = vcat(x_parts..., xΓ)
    x_local = similar(b_local)
    x_local[F.perm] = xp
    return distribute(x_local, b.partitioning)
end

function LinearAlgebra.ldiv!(x::DVector, F::DistributedSchurLU, b::DVector)
    return copyto!(x, F \ b)
end

