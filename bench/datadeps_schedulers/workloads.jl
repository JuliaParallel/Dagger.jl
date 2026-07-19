using LinearAlgebra
using Dagger
using Dagger: In, Out, InOut
using Distributed: remotecall_fetch

# Signatures relaxed from `Matrix{<:Matrix}` to `AbstractMatrix` so tile
# elements may be plain `Matrix` (single-process test paths) OR
# `Dagger.Chunk` values that live on a specific worker/processor
# (multi-process benchmark paths — see `make_spd_tiles` /
# `make_matmul_tiles` below). `Dagger.@spawn f(In(M[k,k]))` and
# `Dagger.@spawn f(InOut(M[k,k]))` accept either transparently: for a
# `Chunk`, datadeps reads the chunk's `scope` / memory space directly and
# constrains task placement accordingly; for a `Matrix`, datadeps
# observes the tile as master-resident data as before.

function tiled_cholesky!(M::AbstractMatrix)
    mt = size(M, 1)
    for k in 1:mt
        Dagger.@spawn LinearAlgebra.LAPACK.potrf!('L', InOut(M[k, k]))
        for m in (k+1):mt
            Dagger.@spawn LinearAlgebra.BLAS.trsm!('R', 'L', 'T', 'N',
                                                   1.0, In(M[k, k]),
                                                   InOut(M[m, k]))
        end
        for n in (k+1):mt
            Dagger.@spawn LinearAlgebra.BLAS.syrk!('L', 'N', -1.0,
                                                   In(M[n, k]), 1.0,
                                                   InOut(M[n, n]))
            for m in (n+1):mt
                Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'T', -1.0,
                                                       In(M[m, k]),
                                                       In(M[n, k]), 1.0,
                                                       InOut(M[m, n]))
            end
        end
    end
    return M
end

function tiled_matmul!(C::AbstractMatrix, A::AbstractMatrix, B::AbstractMatrix)
    nt = size(C, 1)
    @assert size(A) == size(B) == size(C) == (nt, nt)
    for i in 1:nt, j in 1:nt
        for k in 1:nt
            Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                   In(A[i, k]),
                                                   In(B[k, j]), 1.0,
                                                   InOut(C[i, j]))
        end
    end
    return C
end

"""
    _placement_procs() -> Vector{Dagger.Processor}

Interleaved list of processors across which tiles are distributed by
`make_spd_tiles` / `make_matmul_tiles`. Uses `Dagger.all_processors()`
so multi-worker sessions (e.g. `julia -p 7 -t 12`) distribute tiles
across every worker's `ThreadProc`s, while single-process sessions
distribute across master's `ThreadProc`s only.

Consecutive indices in the returned vector round-robin across worker
pids: `[pid₁_t₁, pid₂_t₁, …, pidₙ_t₁, pid₁_t₂, pid₂_t₂, …]`. That
guarantees small tile counts (e.g. `nt=2` → 4 tiles) are spread across
multiple workers rather than concentrated on the first-listed pid.
A naive `sort!(procs; by = pid)` would place all of master's
ThreadProcs first and small `nt` would land only on master, silently
underdistributing at small `nt` and confounding the tile-count axis
with a hidden "how many workers happened to be hit" axis.

Within each pid, the per-thread ordering is stable-sorted by
`string(proc)` so tile→proc mapping is reproducible run-to-run for
identical sessions.
"""
function _placement_procs()
    procs = collect(Dagger.all_processors())
    # Group by owning worker pid so we can interleave across pids.
    by_pid = Dict{Int, Vector{eltype(procs)}}()
    for p in procs
        pid = Dagger.root_worker_id(p)
        push!(get!(by_pid, pid, eltype(procs)[]), p)
    end
    # Deterministic within-pid ordering for reproducibility.
    for slice in values(by_pid)
        sort!(slice, by = string)
    end
    # Interleave: take the k-th processor from each pid in pid order,
    # then move to k+1. Yields [pid₁_t₁, pid₂_t₁, …, pid₁_t₂, …].
    pids = sort!(collect(keys(by_pid)))
    max_slice = maximum(length(v) for v in values(by_pid))
    interleaved = eltype(procs)[]
    sizehint!(interleaved, sum(length(v) for v in values(by_pid)))
    for tid_idx in 1:max_slice
        for pid in pids
            slice = by_pid[pid]
            if tid_idx <= length(slice)
                push!(interleaved, slice[tid_idx])
            end
        end
    end
    return interleaved
end

"""
    _distribute_tile(tile::AbstractMatrix, target_proc::Dagger.Processor)
        -> Dagger.Chunk

Ships `tile` (currently master-resident) to `target_proc`'s worker via
`remotecall_fetch`, then wraps it as a `Chunk` pinned to that processor
via `ExactScope(target_proc)`. `MemPool.poolset` runs on the target
worker inside the closure, so the resulting DRef lives in that worker's
memory space — subsequent datadeps tasks that touch only this tile
incur zero cross-worker transfer.

The scope-and-proc metadata on the returned `Chunk` is what
`spawn_datadeps` inspects when computing per-task placement. Without
this distribution step every tile would be master-resident, and
`spawn_datadeps` would (correctly) place every task on master to avoid
shipping 8-MB tiles out and back — which is the exact master-only
concentration Hudson reproduced.
"""
function _distribute_tile(tile::AbstractMatrix, target_proc::Dagger.Processor)
    target_pid = Dagger.root_worker_id(target_proc)
    return remotecall_fetch(target_pid) do
        Dagger.tochunk(tile, target_proc, Dagger.ExactScope(target_proc))
    end
end

# Round-robin tile-index → processor mapping (row-major linearisation).
# Kept as a separate helper so make_spd_tiles / make_matmul_tiles use
# identical placement conventions and every tile at logical position
# `(i, j)` lands on the same processor across independent workloads
# (matmul's `A[i, k]`, `B[k, j]` co-locate with `C[i, j]` for at least
# one `k`, reducing transfers in the common case).
@inline function _tile_proc(procs, nt::Int, i::Int, j::Int)
    return procs[mod1((i - 1) * nt + j, length(procs))]
end

function make_spd_tiles(sz::Int, nb::Int)
    @assert sz % nb == 0
    nt = sz ÷ nb
    A = rand(sz, sz)
    A = A * A'
    A[diagind(A)] .+= sz
    procs = _placement_procs()
    # Copy each tile before shipping so the closure carries an
    # independent Matrix (not a view into `A`), avoiding accidental
    # aliasing with `A`'s buffer that would confuse datadeps' aliasing
    # analysis.
    return [_distribute_tile(copy(A[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb]),
                             _tile_proc(procs, nt, i, j))
            for i in 1:nt, j in 1:nt]
end

function make_matmul_tiles(sz::Int, nb::Int)
    @assert sz % nb == 0
    nt = sz ÷ nb
    procs = _placement_procs()
    A = [_distribute_tile(rand(nb, nb), _tile_proc(procs, nt, i, j))
         for i in 1:nt, j in 1:nt]
    B = [_distribute_tile(rand(nb, nb), _tile_proc(procs, nt, i, j))
         for i in 1:nt, j in 1:nt]
    C = [_distribute_tile(zeros(nb, nb), _tile_proc(procs, nt, i, j))
         for i in 1:nt, j in 1:nt]
    return A, B, C
end
