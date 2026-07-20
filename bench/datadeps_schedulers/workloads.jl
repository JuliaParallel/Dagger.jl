using LinearAlgebra
using Random
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
`remotecall_fetch`, then wraps it as a `Chunk` that LIVES on that
processor without pinning task placement to it. `MemPool.poolset` runs
on the target worker inside the closure, so the resulting DRef lives in
that worker's memory space — subsequent datadeps tasks that touch only
this tile can either (a) run on `target_proc` for zero-cost local access,
or (b) run elsewhere and incur the measured transfer cost γ. That
trade-off is exactly what the schedulers are meant to weigh.

Historical note (this is why NO ExactScope): the earlier revision of
this function pinned tiles via `ExactScope(target_proc)`. That worked to
distribute tiles across workers, but Dagger's ExactScope is a HARD
constraint on task placement, not just a data-location hint. Under it,
every scheduler is forced onto the tile's owning processor — RR, Greedy,
IG, SA, JuMP, OptimizingScheduler all converged to byte-identical
n_copies counts because none of them had any placement choice to make.
Hudson's matmul sweep at bs=1024 confirmed the collapse: n_copies
identical to the single-integer across all six schedulers at every tile
count, and per-trial CV up to 34% because the residual variation was
scheduler-independent system noise. Removing ExactScope restores the
paper's core mechanism: schedulers see distributed data (γ > 0) and
DECIDE placement rather than having it forced on them.

The scope-and-proc metadata on the returned `Chunk` is what
`spawn_datadeps` inspects when computing per-task placement. Without
the initial distribution step every tile would be master-resident, and
`spawn_datadeps` would (correctly) place every task on master to avoid
shipping 8-MB tiles out and back — which was the pre-distribution
master-only concentration Hudson previously reproduced.
"""
function _distribute_tile(tile::AbstractMatrix, target_proc::Dagger.Processor)
    target_pid = Dagger.root_worker_id(target_proc)
    return remotecall_fetch(target_pid) do
        # No explicit scope: chunk lives on `target_proc` but scheduler
        # is free to route tasks touching this tile anywhere, weighing
        # γ transfer cost against compute-vs-parallelism trade-offs.
        #
        # `_localize_to_target` converts the host-serialized `Matrix` into
        # whatever array type `target_proc` expects: `Matrix` for
        # CPU procs (identity), `CuArray` on `CuArrayDeviceProc`, `ROCArray`
        # on `ROCArrayDeviceProc`, etc. Vendor conversion is dispatched
        # through `Dagger.move` so the vendor extensions (CUDAExt, ROCExt,
        # MetalExt, oneAPIExt, OpenCLExt) handle it — this file stays
        # vendor-agnostic and works unchanged across every accelerator
        # Dagger supports.
        localized = _localize_to_target(tile, target_proc)
        Dagger.tochunk(localized, target_proc)
    end
end

# Host-resident data stays as-is on CPU procs. Split into concrete-type
# methods rather than a `Union` fallback so dispatch is unambiguous with
# the vendor GPU-proc fallback below.
_localize_to_target(tile::AbstractMatrix, ::Dagger.ThreadProc) = tile
_localize_to_target(tile::AbstractMatrix, ::Dagger.OSProc) = tile

# GPU / accelerator procs: hop through `Dagger.move(OSProc, target_proc, x)`
# which the vendor extensions override to construct the correct array type
# on the target device (e.g. `Dagger.move(::CPUProc, ::CuArrayDeviceProc, x)`
# in `ext/CUDAExt.jl` does `adapt(CuArray, x)` under `with_context(to_proc)`).
# `from_proc = OSProc()` is correct because the closure body runs on the
# target worker and `tile` was just serialized here as host memory.
_localize_to_target(tile::AbstractMatrix, target_proc::Dagger.Processor) =
    Dagger.move(Dagger.OSProc(), target_proc, tile)

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

# ─── Sinnen-Sousa Layer-by-Layer random DAG ───────────────────────────
#
# Random DAGs are the standard evaluation regime for HEFT-family
# schedulers because structured BLAS DAGs (matmul, cholesky) leave
# little room between EFT list scheduling and any smarter method — the
# dependency skeleton is regular enough that Greedy sits at a local
# optimum (Topcuoglu 2002 §5, Sinnen-Sousa 2004 §5, Ruiz-Stützle 2007
# §6, Orsila 2008 §5). The Layer-by-Layer construction below matches
# Sinnen-Sousa 2004's method: pin every task to a discrete DAG level,
# then sample edges only between strictly-lower-level and
# strictly-higher-level tasks. Enforces acyclicity by construction
# without post-hoc cycle removal.
#
# Task in-degree is capped at 2 so each task maps to a single
# tile-level `BLAS.gemm!` (matches the tile-BLAS style of matmul /
# cholesky and keeps `sched_phase_ms` comparable to the other
# workloads — one Dagger.@spawn per DAG node, not one-per-parent).
# Parent count of 0/1/2 is a well-known simplification in HEFT
# evaluation harnesses (Topcuoglu §5.1); it preserves the essential
# scheduling difficulty (topology irregularity, critical-path
# variability) without inflating the DAG with accumulate-fan-in
# artifacts.

"""
    make_random_dag_tiles(n_tasks, n_levels, edge_probability, nb; seed=42)

Constructs a Sinnen-Sousa 2004 Layer-by-Layer random DAG plus the
distributed tile set the workload operates on. Deterministic in `seed`
so the same DAG topology is reproduced across scheduler cells and
trials (essential for making scheduler-A vs scheduler-B comparisons
apples-to-apples on the same graph).

Layer assignment: task `t` is placed at level
`min(n_levels, ((t-1) * n_levels) ÷ n_tasks + 1)`, so tasks 1..n_tasks
partition evenly across `1..n_levels`.

Edge sampling: for every pair (parent, child) with
`level(parent) < level(child)`, an edge is created with probability
`edge_probability`, stopping when the child accumulates 2 parents
(cap enforces single-gemm-per-task).

Placement: output tiles are round-robin distributed across
`_placement_procs()` — same interleaved-across-workers discipline used
by matmul/cholesky, so tiles-across-workers-and-devices behaviour is
consistent across all three workload classes.

Returns a NamedTuple with fields:
- `tiles`: `Vector{Chunk}` of `n_tasks` output tiles
- `parents`: `Vector{Vector{Int}}` giving parent-task indices per task
- `initial_A`, `initial_B`: distributed operand tiles for
   0-in-degree (root) tasks
- `level_of`: layer assignment per task (for reporting / analysis)
"""
function make_random_dag_tiles(n_tasks::Int, n_levels::Int, edge_probability::Real, nb::Int;
                               seed::Int=42)
    n_tasks >= 1        || throw(ArgumentError("n_tasks must be ≥ 1"))
    n_levels >= 1       || throw(ArgumentError("n_levels must be ≥ 1"))
    n_levels <= n_tasks || throw(ArgumentError("n_levels ($n_levels) must be ≤ n_tasks ($n_tasks)"))
    (0 <= edge_probability <= 1) ||
        throw(ArgumentError("edge_probability must be in [0, 1]"))

    rng = Random.MersenneTwister(seed)

    # Sinnen-Sousa Layer-by-Layer: partition tasks across `n_levels`
    # layers in order. Assignment is deterministic (no rng draw), so
    # topology depends only on the edge Bernoulli draws below — cleaner
    # for reproducibility and for stripping seed sensitivity away from
    # layer structure.
    level_of = [min(n_levels, ((t - 1) * n_levels) ÷ n_tasks + 1) for t in 1:n_tasks]

    # Edge sampling with in-degree cap of 2. Iterate candidates in a
    # shuffled order so the two selected parents aren't systematically
    # the lowest-index tasks at earlier levels — mirrors Sinnen-Sousa's
    # unbiased edge sampling.
    parents = [Int[] for _ in 1:n_tasks]
    for t in 2:n_tasks
        my_lvl = level_of[t]
        candidates = [i for i in 1:(t - 1) if level_of[i] < my_lvl]
        Random.shuffle!(rng, candidates)
        for c in candidates
            length(parents[t]) >= 2 && break
            rand(rng) < edge_probability && push!(parents[t], c)
        end
    end

    procs = _placement_procs()
    tiles = [_distribute_tile(zeros(nb, nb), procs[mod1(t, length(procs))])
             for t in 1:n_tasks]

    # Distinct operand tiles for root tasks. Placed on two distinct
    # procs (when available) so their transfer costs to child tasks
    # are not artificially zero across the board.
    initial_A = _distribute_tile(rand(nb, nb), procs[1])
    initial_B = _distribute_tile(rand(nb, nb), procs[min(2, length(procs))])

    return (; tiles, parents, initial_A, initial_B, level_of)
end

"""
    tiled_random_dag!(tiles, parents, initial_A, initial_B)

Executes the Sinnen-Sousa random DAG defined by `parents`. Each task
is a single `BLAS.gemm!` on tile-sized operands, chosen by parent
count so that every DAG node maps to exactly one `Dagger.@spawn`:

- 0 parents (root):      `tiles[t] ← initial_A * initial_B'`
- 1 parent:              `tiles[t] ← tiles[p] * initial_B'`
- 2 parents:             `tiles[t] ← tiles[p₁] * tiles[p₂]'`

Uses `LinearAlgebra.BLAS.gemm!` directly — CUDAExt/ROCExt's auto-
generated `Dagger.move(::CPUProc, ::GPUProc, ::typeof(BLAS.gemm!))`
overloads substitute `CUBLAS.gemm!` / `rocBLAS.gemm!` at task
execution time when the task lands on a GPU proc, so this workload
runs unmodified on CPU + NVIDIA + AMD.

The `β=0` GEMM (rather than `β=1`) makes each task idempotent — the
task's output depends only on its inputs, not on the current value
of `tiles[t]`. This matches Sinnen-Sousa's dataflow semantics and
makes correctness verification (finiteness) trivial: no accumulator
drift across trials.
"""
function tiled_random_dag!(tiles::AbstractVector, parents::Vector{Vector{Int}},
                            initial_A, initial_B)
    @assert length(tiles) == length(parents)
    for t in eachindex(tiles, parents)
        p = parents[t]
        if isempty(p)
            Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'T', 1.0,
                                                    In(initial_A),
                                                    In(initial_B),
                                                    0.0,
                                                    InOut(tiles[t]))
        elseif length(p) == 1
            Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'T', 1.0,
                                                    In(tiles[p[1]]),
                                                    In(initial_B),
                                                    0.0,
                                                    InOut(tiles[t]))
        else
            Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'T', 1.0,
                                                    In(tiles[p[1]]),
                                                    In(tiles[p[2]]),
                                                    0.0,
                                                    InOut(tiles[t]))
        end
    end
    return tiles
end
