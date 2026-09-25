import Statistics

# The facts a task's metrics record live on its `DTaskTLS` (set once by
# `Sch.do_task`), not in `ScopedValue`s. Entering a scope for five of them cost
# ~939 allocations / 32 KB per task on a `fetch(@spawn 1+1)` round-trip --
# more than the entire rest of the scheduler path -- and `set_tls!` already
# runs once per thunk, so reading from it is free.
_metrics_tls() = DTASK_TLS[]

# The facts that identify a task's measurements: its signature, the processor
# it ran on and that processor's worker. These are not measured on the worker
# (see `TaskMetrics`); the scheduler writes them when it folds a task's
# `TaskMetrics` into the cache, from the `Thunk` and processor it holds.
struct SignatureMetric <: MT.AbstractMetric end
MT.metric_applies(::SignatureMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{SignatureMetric}) = Union{Vector{Any}, Nothing}

struct ProcessorMetric <: MT.AbstractMetric end
MT.metric_applies(::ProcessorMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{ProcessorMetric}) = Union{Processor, Nothing}

struct WorkerMetric <: MT.AbstractMetric end
MT.metric_applies(::WorkerMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{WorkerMetric}) = Union{Int, Nothing}

struct TransferSizeMetric <: MT.AbstractMetric end
MT.metric_applies(::TransferSizeMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{TransferSizeMetric}) = Union{UInt64, Nothing}
MT.start_metric(::TransferSizeMetric) = nothing
MT.stop_metric(::TransferSizeMetric, _) = (tls = _metrics_tls(); tls === nothing || tls.metrics_transfer_size == 0 ? nothing : tls.metrics_transfer_size)

struct TransferTimeMetric <: MT.AbstractMetric end
MT.metric_applies(::TransferTimeMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{TransferTimeMetric}) = Union{UInt64, Nothing}
MT.start_metric(::TransferTimeMetric) = nothing
MT.stop_metric(::TransferTimeMetric, _) = (tls = _metrics_tls(); tls === nothing || tls.metrics_transfer_time == 0 ? nothing : tls.metrics_transfer_time)

struct TransferRateMetric <: MT.AbstractMetric end
MT.metric_applies(::TransferRateMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{TransferRateMetric}) = Union{UInt64, Nothing}
MT.start_metric(::TransferRateMetric) = nothing
function MT.stop_metric(::TransferRateMetric, _)
    tls = _metrics_tls()
    tls === nothing && return nothing
    size = tls.metrics_transfer_size
    elapsed = tls.metrics_transfer_time
    if elapsed == 0 || size == 0
        return nothing
    end
    return round(UInt64, Float64(size) / (Float64(elapsed) / 1e9))
end

struct FromSpaceMetric <: MT.AbstractMetric end
MT.metric_applies(::FromSpaceMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{FromSpaceMetric}) = Union{MemorySpace, Nothing}

struct ToSpaceMetric <: MT.AbstractMetric end
MT.metric_applies(::ToSpaceMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{ToSpaceMetric}) = Union{MemorySpace, Nothing}

struct MoveSizeMetric <: MT.AbstractMetric end
MT.metric_applies(::MoveSizeMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{MoveSizeMetric}) = Union{UInt64, Nothing}

"""
    KernelTimeMetric

A task's cost, for the cost model: the CPU time of the thread that ran the
task's function.

`ThreadProc.execute!` clocks the call itself (`DTaskTLS.metrics_kernel_time`),
because the call does not always run on the scheduler task's thread. An
`MPIProcessor`'s runner is not pinned (MPI's waits spin on `yield`, which a
pinned task must not do), so its kernel runs on a sub-task pinned to the
processor's thread while the runner waits -- and may resume elsewhere. Clocking
the runner's thread, as `MT.ThreadTimeMetric` does, then measures whatever else
that thread did meanwhile: a 256x256 matmul read 0.23 ms against 0.9 ms of wall
time, and a runner that migrated subtracted one thread's clock from another's,
wrapping to ~1.8e19 ns. Rank 0 plans every rank's work from these numbers.

Processors whose `execute!` reports no time fall back to the runner thread's
clock, but only if the runner stayed on one thread; otherwise the task records
no runtime rather than a meaningless one.
"""
struct KernelTimeMetric <: MT.AbstractMetric end
MT.metric_applies(::KernelTimeMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{KernelTimeMetric}) = Union{UInt64, Nothing}
MT.start_metric(::KernelTimeMetric) = (Threads.threadid(), cputhreadtime())
function MT.stop_metric(::KernelTimeMetric, start::Tuple{Int, UInt64})
    tls = _metrics_tls()
    if tls !== nothing && tls.metrics_kernel_time != 0
        return tls.metrics_kernel_time
    end
    tid, t0 = start
    Threads.threadid() == tid || return nothing
    return cputhreadtime() - t0
end

# What `do_task` measures around a task's `execute!`. Only measurements: the
# task's signature, processor and worker are attached where its result is
# handled (see `apply_task_metrics!`).
const EXECUTE_METRICS_SPEC = MT.MetricsSpec(
    MT.TimeMetric(),
    KernelTimeMetric(),
    MT.AllocMetric(),
    TransferSizeMetric(),
    TransferTimeMetric(),
    TransferRateMetric(),
)

execute_metrics_spec() = EXECUTE_METRICS_SPEC

function _record_move_metrics!(cache::MT.MetricsCache, thunk_id::Int,
                                source_space::MemorySpace, dest_space::MemorySpace,
                                size::Union{UInt64, Nothing})
    MT.bulk_update!(cache) do c
        ctx = MT.pending_context!(c, Dagger, :execute!, Int)
        from_storage = MT.get_or_create_storage!(ctx, FromSpaceMetric())
        to_storage = MT.get_or_create_storage!(ctx, ToSpaceMetric())
        MT.set_metric_value!(from_storage, thunk_id, source_space)
        MT.set_metric_value!(to_storage, thunk_id, dest_space)
        if size !== nothing
            size_storage = MT.get_or_create_storage!(ctx, MoveSizeMetric())
            MT.set_metric_value!(size_storage, thunk_id, size)
        end
    end
    return
end

_move_source_size(source::Chunk) =
    source.handle.size === nothing ? nothing : UInt64(source.handle.size)
# Not every moved handle carries a size (e.g. `ChunkView` under MPI); those
# moves still record their spaces, just without a size to derive a rate from.
_move_source_size(@nospecialize(source)) = nothing

"""
    move_toplevel!(dep_mod, dest_space, source_space, dest, source)

The entry point of a Datadeps copy task: runs `move!`, then records the copy's
source/destination spaces and size in the metrics cache. Datadeps spawns its
copy tasks through this (not `move!` directly, whose methods recurse into one
another), which is what gives `metrics_lookup_move_rate` per-space-pair transfer
rates to cost data movement with.

The metrics are written directly into the executing thunk's cache (reached via
the TLS), *not* through a `ScopedValue` or `TaskLocalValue`: `ThreadProc.execute!`
runs the thunk on a sub-task, so a scope entered here has already exited — and a
task-local set here is already gone — by the time `with_metrics` commits.
"""
function move_toplevel!(dep_mod, dest_space::MemorySpace, source_space::MemorySpace,
                        dest, source)
    result = move!(dep_mod, dest_space, source_space, dest, source)
    tls = DTASK_TLS[]
    if tls !== nothing && tls.metrics_cache !== nothing
        thunk_id = tls.sch_handle.thunk_id.id
        _record_move_metrics!(tls.metrics_cache, thunk_id, source_space, dest_space,
                              _move_source_size(source))
    end
    return result
end

"""
    is_move_task(f) -> Bool

Whether `f` is the function of a Datadeps copy task: `move!`, or the
`move_toplevel!` that Datadeps actually spawns.

Execution backends that treat copy tasks specially must ask this instead of
comparing `f` against `move!`. MPI is the sharp case: every rank has to take
part in a copy, since the source rank does the send, so a copy that is not
recognized as one runs on its destination rank alone and blocks forever in a
receive nobody answers.
"""
is_move_task(f) = f === move! || f === move_toplevel!

"""
    records_metrics(proc::Processor, f, args) -> Bool

Whether this process's measurements of task `f` (placed on `proc`, called with
`args`) describe that task, and so belong in its metrics cache.

Ordinarily a process runs exactly the tasks placed on it, so they always do.
Under uniform execution (MPI) every rank runs every task, but only the rank
owning `proc` computes it; the others return almost at once. What they measure
is not the task's cost, and recording it tells the cost model that every other
rank's processors are nearly free: rank 0, which plans for all ranks, then
piles a region's tasks onto whichever remote rank looks cheapest.
"""
records_metrics(::Processor, f, args) = true

function _reduce_uint64(reducer::Function, vals::Vector{UInt64})
    isempty(vals) && return nothing
    raw = reducer(vals)
    return raw isa UInt64 ? raw : round(UInt64, raw)
end

function _runtime_lookup_chain(sig::Vector, proc::Processor, worker_id::Int)
    return (
        (MT.LookupExact(SignatureMetric(), sig),
         MT.LookupExact(ProcessorMetric(), proc)),
        (MT.LookupExact(SignatureMetric(), sig),
         MT.LookupSubtype(ProcessorMetric(), typeof(proc)),
         MT.LookupCustom(WorkerMetric(), w -> w == worker_id)),
        (MT.LookupExact(SignatureMetric(), sig),
         MT.LookupSubtype(ProcessorMetric(), typeof(proc))),
        (MT.LookupExact(SignatureMetric(), sig),),
    )
end

function metrics_lookup_runtime(snap::MT.MetricsSnapshot, sig::Vector,
                                proc::Processor, worker_id::Int;
                                reducer::Function=first)
    target = KernelTimeMetric()
    for lookups in _runtime_lookup_chain(sig, proc, worker_id)
        matched = MT.find_keys(snap, Dagger, :execute!, lookups)
        isempty(matched) && continue
        vals = UInt64[]
        sizehint!(vals, length(matched))
        for k in matched
            v = MT.lookup_value(snap, Dagger, :execute!, target, k)
            v !== nothing && push!(vals, v)
        end
        result = _reduce_uint64(reducer, vals)
        result !== nothing && return result
    end
    return nothing
end

"""
    SignatureRuntimeIndex

Precomputed per-signature runtime index built by
`build_signature_runtime_index`. Groups measured `KernelTimeMetric`
runtimes for a fixed signature `sig` by `(processor, worker_id)`,
`(processor_type, worker_id)`, `(processor_type)`, and all-matching so
that per-processor lookups via
`metrics_lookup_runtime_from_index` can traverse the fallback chain in
O(1) hash lookups instead of re-scanning the snapshot per call.

The scheduler's per-task cost estimator (`estimate_task_costs!`)
evaluates every candidate processor with the same signature, so building
this index once amortises what would otherwise be `O(W × N)` snapshot
scans (where `W` is candidate-processor count and `N` is total metric
keys) into a single `O(N)` scan plus `O(W)` amortized dict lookups.
"""
struct SignatureRuntimeIndex
    by_proc_worker::Dict{Tuple{Processor,Int},Vector{UInt64}}
    by_type_worker::Dict{Tuple{DataType,Int},Vector{UInt64}}
    by_type::Dict{DataType,Vector{UInt64}}
    any_matching::Vector{UInt64}
end

"""
    build_signature_runtime_index(snap::MT.MetricsSnapshot, sig::Vector)
        -> SignatureRuntimeIndex

Single-pass build: scan the SignatureMetric storage once, keep only keys
matching `sig`, then for each such key materialise its
(processor, worker_id, KernelTimeMetric) tuple and index into the four
buckets used by the fallback chain in `metrics_lookup_runtime`. The
result is safe to reuse across many `metrics_lookup_runtime_from_index`
calls for different processors as long as `sig` and `snap` are
unchanged.
"""
function build_signature_runtime_index(snap::MT.MetricsSnapshot, sig::Vector)
    ctx = get(snap.contexts, (Dagger, :execute!), nothing)
    if ctx === nothing
        return SignatureRuntimeIndex(
            Dict{Tuple{Processor,Int},Vector{UInt64}}(),
            Dict{Tuple{DataType,Int},Vector{UInt64}}(),
            Dict{DataType,Vector{UInt64}}(),
            UInt64[],
        )
    end

    sig_storage = get(ctx.storages, SignatureMetric(), nothing)
    proc_storage = get(ctx.storages, ProcessorMetric(), nothing)
    worker_storage = get(ctx.storages, WorkerMetric(), nothing)
    time_storage = get(ctx.storages, KernelTimeMetric(), nothing)

    by_proc_worker = Dict{Tuple{Processor,Int},Vector{UInt64}}()
    by_type_worker = Dict{Tuple{DataType,Int},Vector{UInt64}}()
    by_type        = Dict{DataType,Vector{UInt64}}()
    any_matching   = UInt64[]

    # Missing any of the storages means no runtime data has been recorded
    # yet — return the empty index (all buckets empty), which mirrors what
    # the original `metrics_lookup_runtime` would produce (returns `nothing`
    # via `_reduce_uint64` on empty vectors).
    if sig_storage === nothing || proc_storage === nothing ||
       worker_storage === nothing || time_storage === nothing
        return SignatureRuntimeIndex(by_proc_worker, by_type_worker, by_type, any_matching)
    end

    _index_signature_runtimes!(by_proc_worker, by_type_worker, by_type, any_matching,
                               sig, sig_storage, time_storage, proc_storage, worker_storage)
    return SignatureRuntimeIndex(by_proc_worker, by_type_worker, by_type, any_matching)
end

# N.B. A function barrier: the storages come out of the context as an abstract
# type, and scanning one inline dispatched dynamically on every entry.
function _index_signature_runtimes!(by_proc_worker, by_type_worker, by_type, any_matching,
                                    sig, sig_storage, time_storage, proc_storage, worker_storage)
    # One O(N) scan across sig-matching keys; each bucket insertion is O(1)
    # amortised. `sig` is a `Vector{Any}` and the stored value is the same
    # type, so `==` compares element-wise — matching the semantics of
    # `LookupExact(SignatureMetric(), sig)` in `_runtime_lookup_chain`.
    for (k, s) in sig_storage.data
        s == sig || continue
        v = get(time_storage.data, k, nothing)
        v === nothing && continue
        rt = v::UInt64
        p = get(proc_storage.data, k, nothing)
        w = get(worker_storage.data, k, nothing)
        push!(any_matching, rt)
        if p !== nothing
            proc_v = p::Processor
            proc_type = typeof(proc_v)
            push!(get!(() -> UInt64[], by_type, proc_type), rt)
            if w !== nothing
                worker_v = w::Int
                push!(get!(() -> UInt64[], by_proc_worker,
                          (proc_v, worker_v)), rt)
                push!(get!(() -> UInt64[], by_type_worker,
                          (proc_type, worker_v)), rt)
            end
        end
    end
    return
end

"""
    metrics_lookup_runtime_from_index(idx::SignatureRuntimeIndex,
                                      proc::Processor, worker_id::Int;
                                      reducer=first) -> Union{UInt64,Nothing}

Fast per-processor runtime lookup using the precomputed
`SignatureRuntimeIndex`. Traverses the same fallback chain as
`metrics_lookup_runtime`:

  1. Exact `(proc, worker_id)`
  2. `(typeof(proc), worker_id)`
  3. `typeof(proc)` on any worker
  4. Any measurement for the signature

Returns the reduced runtime for the first non-empty bucket, or `nothing`
if the signature has no measurements at all.
"""
function metrics_lookup_runtime_from_index(idx::SignatureRuntimeIndex,
                                            proc::Processor, worker_id::Int;
                                            reducer::Function=first)
    proc_type = typeof(proc)
    vals = get(idx.by_proc_worker, (proc, worker_id), nothing)
    if vals === nothing || isempty(vals)
        vals = get(idx.by_type_worker, (proc_type, worker_id), nothing)
    end
    if vals === nothing || isempty(vals)
        vals = get(idx.by_type, proc_type, nothing)
    end
    if vals === nothing || isempty(vals)
        vals = idx.any_matching
    end
    (vals === nothing || isempty(vals)) && return nothing
    return _reduce_uint64(reducer, vals)
end

"""
    SignatureRuntimeIndexCache

Memo of `build_signature_runtime_index` results (and alloc/transfer-rate lookups), keyed by signature
hash and attached to the one snapshot object it describes.

Building an index is a full scan of the snapshot's signature storage — measured
at 770 allocations / 30 KB — and the scheduler builds one per scheduling pass.
But the index is a pure function of `(snapshot, signature)`, and in steady state
both repeat: signatures recur across tasks, and the cost model reuses one
snapshot for as long as it is allowed to go stale. Memoizing therefore collapses
the per-task build to a dictionary lookup.

The memo lives on the snapshot (`MT.snapshot_memo!`), so a rebuild starts
with an empty one and every task holding a snapshot shares its results. It
used to be task-local, to avoid locking between concurrent scheduling tasks;
but scheduling runs on a pool of tasks, so each one redid every scan after
each snapshot rebuild, and that multiplied the cost of the metrics bound by the
number of scheduling tasks -- at a 5000-task bound, MPI matmul on the default
path ran 10x slower than at 100.
"""
mutable struct SignatureRuntimeIndexCache
    lock::ReentrantLock
    entries::Dict{UInt, SignatureRuntimeIndex}
    # `metrics_lookup_alloc` and `metrics_lookup_transfer_rate` both resolve
    # through `MT.find_keys`, which scans every storage and every key of the
    # snapshot while building `Set{Any}`s -- and they run once per candidate
    # processor per task. Unlike the runtime lookup they have no index, so
    # their (small) results are memoized directly.
    # N.B. A miss is a meaningful result and is cached as such: the scan costs
    # the same whether or not it finds anything, so `nothing` must be memoized
    # too or the common no-samples case keeps paying full price.
    alloc::Dict{Tuple{UInt, UInt}, Union{UInt64, Nothing}}
    rate::Dict{Tuple{UInt, Int}, Union{UInt64, Nothing}}
end
SignatureRuntimeIndexCache() =
    SignatureRuntimeIndexCache(ReentrantLock(), Dict{UInt, SignatureRuntimeIndex}(),
                               Dict{Tuple{UInt, UInt}, Union{UInt64, Nothing}}(),
                               Dict{Tuple{UInt, Int}, Union{UInt64, Nothing}}())

_cost_memo(snap::MT.MetricsSnapshot) =
    MT.snapshot_memo!(SignatureRuntimeIndexCache, snap)::SignatureRuntimeIndexCache

# Look `key` up in one of `memo`'s tables, computing it with `f()` on a miss.
# `f` runs outside the lock (it scans the snapshot); two tasks racing on one
# key both compute it, and the first stored result is kept.
function _memoized(f, memo::SignatureRuntimeIndexCache, table::Dict{K,V}, key::K) where {K,V}
    existing = @lock memo.lock get(table, key, missing)
    existing === missing || return existing::V
    val = f()::V
    return @lock memo.lock get!(table, key, val)
end

"""
    cached_signature_runtime_index(snap, sig, sig_hash) -> SignatureRuntimeIndex

`build_signature_runtime_index`, memoized per `(snapshot, sig_hash)`. See
[`SignatureRuntimeIndexCache`](@ref).
"""
function cached_signature_runtime_index(snap::MT.MetricsSnapshot, sig::Vector, sig_hash::UInt)
    memo = _cost_memo(snap)
    return _memoized(() -> build_signature_runtime_index(snap, sig), memo, memo.entries, sig_hash)
end

"""
    cached_metrics_lookup_alloc(snap, sig, sig_hash, proc)

`metrics_lookup_alloc`, memoized per `(snapshot, sig_hash, proc)`. See
[`SignatureRuntimeIndexCache`](@ref) for why.
"""
function cached_metrics_lookup_alloc(snap::MT.MetricsSnapshot, sig::Vector,
                                     sig_hash::UInt, proc::Processor)
    memo = _cost_memo(snap)
    return _memoized(() -> metrics_lookup_alloc(snap, sig, proc), memo, memo.alloc,
                     (sig_hash, hash(proc)))
end

"""
    cached_metrics_lookup_transfer_rate(snap, proc, worker_id)

`metrics_lookup_transfer_rate`, memoized per `(snapshot, proc, worker_id)`. See
[`SignatureRuntimeIndexCache`](@ref) for why.
"""
function cached_metrics_lookup_transfer_rate(snap::MT.MetricsSnapshot,
                                             proc::Processor, worker_id::Int)
    memo = _cost_memo(snap)
    return _memoized(() -> metrics_lookup_transfer_rate(snap, proc, worker_id), memo, memo.rate,
                     (hash(proc), worker_id))
end

metrics_lookup_runtime_mean(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=Statistics.mean)
metrics_lookup_runtime_median(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=Statistics.median)
metrics_lookup_runtime_min(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=minimum)
metrics_lookup_runtime_max(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=maximum)

function _alloc_lookup_chain(sig::Vector, proc::Processor)
    return (
        (MT.LookupExact(SignatureMetric(), sig),
         MT.LookupExact(ProcessorMetric(), proc)),
        (MT.LookupExact(SignatureMetric(), sig),),
    )
end

function metrics_lookup_alloc(snap::MT.MetricsSnapshot, sig::Vector,
                              proc::Processor;
                              reducer::Function=first)
    target = MT.AllocMetric()
    for lookups in _alloc_lookup_chain(sig, proc)
        matched = MT.find_keys(snap, Dagger, :execute!, lookups)
        isempty(matched) && continue
        vals = UInt64[]
        sizehint!(vals, length(matched))
        for k in matched
            diff = MT.lookup_value(snap, Dagger, :execute!, target, k)
            if diff !== nothing
                gc_diff = diff::Base.GC_Diff
                push!(vals, UInt64(max(gc_diff.allocd, 0)))
            end
        end
        result = _reduce_uint64(reducer, vals)
        result !== nothing && return result
    end
    return nothing
end

metrics_lookup_alloc_mean(snap, sig, proc) =
    metrics_lookup_alloc(snap, sig, proc; reducer=Statistics.mean)
metrics_lookup_alloc_median(snap, sig, proc) =
    metrics_lookup_alloc(snap, sig, proc; reducer=Statistics.median)
metrics_lookup_alloc_min(snap, sig, proc) =
    metrics_lookup_alloc(snap, sig, proc; reducer=minimum)
metrics_lookup_alloc_max(snap, sig, proc) =
    metrics_lookup_alloc(snap, sig, proc; reducer=maximum)

"""
    TaskMetrics

What a worker reports about one finished task, for the scheduler to fold into
its metrics cache with [`apply_task_metrics!`](@ref). Measurements only: the
task's signature, processor and worker are known where the result arrives
(`handle_result!` holds the `Thunk` and the processor it fired the task on),
so they are not sent. They used to be: every result carried its signature, a
vector of types, and serializing that (plus a `Processor` and a vector of
boxed metric pairs) cost more than the rest of the scheduler path -- with
three workers, an eager task went from 0.029 ms on master to 0.071 ms, and a
Datadeps task from 0.175 ms to 0.280 ms; with nothing sent, 0.035 and 0.183.

A zero means "not measured" for `kernel_time` and the transfer fields, and
`move_from === nothing` means the task was not a copy.
"""
struct TaskMetrics
    time::UInt64
    kernel_time::UInt64
    alloc::Base.GC_Diff
    transfer_size::UInt64
    transfer_time::UInt64
    transfer_rate::UInt64
    move_from::Union{MemorySpace, Nothing}
    move_to::Union{MemorySpace, Nothing}
    move_size::UInt64
end

# The storage for `m` in `ctx`, with its concrete type (the context holds
# storages behind an abstract type; asserting it here keeps the reads below
# statically dispatched), or `nothing` if `m` was never recorded.
function _typed_storage(ctx::MT.ContextStorage{K}, m::M) where {K, M<:MT.AbstractMetric}
    s = get(ctx.storages, m, nothing)
    s === nothing && return nothing
    return s::MT.MetricStorage{M, K, MT.metric_type(M)}
end
function _stored(ctx::MT.ContextStorage{K}, m::MT.AbstractMetric, key::K, default) where K
    s = _typed_storage(ctx, m)
    s === nothing && return default
    v = get(s.data, key, nothing)
    return v === nothing ? default : v
end

"""
    collect_task_metrics(local_cache::MT.MetricsCache, key::Int) -> Union{TaskMetrics, Nothing}

Read the metrics `with_metrics` (and `move_toplevel!`) recorded for `key`
out of `do_task`'s task-local cache.
"""
function collect_task_metrics(local_cache::MT.MetricsCache, key::Int)
    # `local_cache` is task-local and already fully written by `with_metrics`
    # (on this same task) before we drain it, so read its pending storages
    # directly instead of taking a defensive deep-copy snapshot.
    ctx = MT.pending_context(local_cache, Dagger, :execute!)
    ctx === nothing && return nothing
    ctx = ctx::MT.ContextStorage{Int}
    time = _stored(ctx, MT.TimeMetric(), key, nothing)
    time === nothing && return nothing
    return TaskMetrics(time::UInt64,
                       _stored(ctx, KernelTimeMetric(), key, UInt64(0))::UInt64,
                       _stored(ctx, MT.AllocMetric(), key, Base.GC_Diff(Base.gc_num(), Base.gc_num()))::Base.GC_Diff,
                       _stored(ctx, TransferSizeMetric(), key, UInt64(0))::UInt64,
                       _stored(ctx, TransferTimeMetric(), key, UInt64(0))::UInt64,
                       _stored(ctx, TransferRateMetric(), key, UInt64(0))::UInt64,
                       _stored(ctx, FromSpaceMetric(), key, nothing),
                       _stored(ctx, ToSpaceMetric(), key, nothing),
                       _stored(ctx, MoveSizeMetric(), key, UInt64(0))::UInt64)
end

# Bound the global metrics cache to the most-recent this-many tasks (distinct
# thunk_id keys) per `(mod, context)`, plus a trim slack (see
# `apply_task_metrics!`). Without a bound the cache grows one entry per
# metric per task forever, which dominates scheduler allocations (Dict rehash
# churn) on long-running workloads. The cost model only needs recent samples,
# so we keep a rolling window.
const METRICS_CACHE_MAX_TASKS = Ref(1000)

"""
    metrics_cache_max_tasks!(n::Integer)

Set the rolling-window bound described above, returning the previous value.

The default is 1000. It was 100, which held fewer tasks than one region's
kernels plus its copies: a 16x16-tile Cholesky runs ~800 kernels, so by the
time a cost-model planner looked, most kernel signatures had been evicted and
fell back to the 1 s placeholder runtime. (Likewise a GPU warmup's ~60 entries
were evicted by the CPU trials after it.) Measured demand for a full
warm+trials cycle is ~235 entries at cholesky nt=4 and ~835 at nt=8. A larger
bound is not free: RoundRobin on the default path, 4 nodes, 256^2 tiles, ran
MPI matmul in 4.15 s at 100, 3.75 s at 1000 and 5.61 s at 5000.

The scheduler's own cost model no longer depends on this bound: it reads
per-signature runtimes from the [`CostSummary`](@ref), which is not trimmed.

Note this is process-local; multi-worker runs must set it on each worker.
"""
function metrics_cache_max_tasks!(n::Integer)
    n > 0 || throw(ArgumentError("metrics cache bound must be positive, got $n"))
    old = METRICS_CACHE_MAX_TASKS[]
    METRICS_CACHE_MAX_TASKS[] = Int(n)
    return old
end

"""
    CostSummary

The scheduler's cost model needs one number per question -- how long does a
task with this signature take on this processor? how much does it allocate?
how fast do inputs reach that processor? -- not the samples behind it. This
keeps those numbers: a running estimate per key, blended from each finished
task's metrics as they arrive (`summarize_task_metrics!`), and read back in
O(1) (`runtime_estimate`, `alloc_estimate`, `transfer_rate_estimate`), so the
scheduler never takes or scans a snapshot of the per-task cache.

It exists because the per-task metrics cache is the wrong place to answer that
question from. That cache is bounded to the most recent `METRICS_CACHE_MAX_TASKS`
tasks, so any region with more tasks than the bound evicts every sample of
the signatures that ran before it. The scheduler then falls back to its 1 s
placeholder for a task it has run thousands of times, and that placeholder
decides placement: with pressure counted in whole seconds per reserved task,
the 0.5 s transfer penalty that keeps a task next to its data is outweighed
as soon as its owner has one more task queued than another worker. On four
nodes, every `copy(A)` that Cholesky at 256² tiles makes ran after a region
of ~2,000 tasks had flushed the cache, so a quarter of its tiles were copied
to other workers, and the factorization moved 2.8x the data and took twice as
long as on master (whose per-signature table was never bounded).

Blending is `(old + new) ÷ 2`, as the scheduler's original table did: a
first-call sample that includes compilation is halved out within a few
tasks. The runtime fallback chain matches `metrics_lookup_runtime`: exact
processor, then processor type on the same worker, then processor type
anywhere, then any processor.

Keys are the signature's process-local hash (`signature_hash`), which is also
what `Signature` equality compares.
"""
struct CostSummary
    lock::ReentrantLock
    runtime_by_proc::Dict{Tuple{UInt,Processor},UInt64}
    runtime_by_type_worker::Dict{Tuple{UInt,DataType,Int},UInt64}
    runtime_by_type::Dict{Tuple{UInt,DataType},UInt64}
    runtime_any::Dict{UInt,UInt64}
    # Bytes allocated by a task, by (signature, processor) and by signature.
    alloc_by_proc::Dict{Tuple{UInt,Processor},UInt64}
    alloc_any::Dict{UInt,UInt64}
    # Bytes per second moving a task's inputs to a processor, by processor,
    # by (processor type, worker) and by processor type.
    rate_by_proc::Dict{Processor,UInt64}
    rate_by_type_worker::Dict{Tuple{DataType,Int},UInt64}
    rate_by_type::Dict{DataType,UInt64}
    # Keys of each table holding exactly one sample (see `_blend!`).
    once_runtime_by_proc::Set{Tuple{UInt,Processor}}
    once_runtime_by_type_worker::Set{Tuple{UInt,DataType,Int}}
    once_runtime_by_type::Set{Tuple{UInt,DataType}}
    once_runtime_any::Set{UInt}
    once_alloc_by_proc::Set{Tuple{UInt,Processor}}
    once_alloc_any::Set{UInt}
    once_rate_by_proc::Set{Processor}
    once_rate_by_type_worker::Set{Tuple{DataType,Int}}
    once_rate_by_type::Set{DataType}
end
CostSummary() = CostSummary(ReentrantLock(),
                            Dict{Tuple{UInt,Processor},UInt64}(),
                            Dict{Tuple{UInt,DataType,Int},UInt64}(),
                            Dict{Tuple{UInt,DataType},UInt64}(),
                            Dict{UInt,UInt64}(),
                            Dict{Tuple{UInt,Processor},UInt64}(),
                            Dict{UInt,UInt64}(),
                            Dict{Processor,UInt64}(),
                            Dict{Tuple{DataType,Int},UInt64}(),
                            Dict{DataType,UInt64}(),
                            Set{Tuple{UInt,Processor}}(),
                            Set{Tuple{UInt,DataType,Int}}(),
                            Set{Tuple{UInt,DataType}}(),
                            Set{UInt}(),
                            Set{Tuple{UInt,Processor}}(),
                            Set{UInt}(),
                            Set{Processor}(),
                            Set{Tuple{DataType,Int}}(),
                            Set{DataType}())

const GLOBAL_COST_SUMMARY = CostSummary()
global_cost_summary() = GLOBAL_COST_SUMMARY

# Blend a new sample into `table[key]`. The first sample of a key is taken as
# is and then *replaced* by the second, not blended with it: the first task
# of a signature on a process compiles it, and that sample can be 1000x the
# real cost. Blending would decay it, but only as more samples reach the same
# key -- and a key a processor sees once (a driver thread that ran one of a
# region's allocation tasks while the rest went elsewhere) keeps its outlier
# indefinitely. Measured: two driver threads at 140 ms and 281 ms for a task
# every other thread put at 10-130 us, which sent every later allocation off
# the driver. Keys not seen a second time are marked in `once`.
function _blend!(table::Dict{K,UInt64}, once::Set{K}, key::K, value::UInt64) where K
    old = get(table, key, nothing)
    if old === nothing
        table[key] = value
        push!(once, key)
    elseif key in once
        table[key] = value
        delete!(once, key)
    else
        table[key] = (old + value) ÷ UInt64(2)
    end
    return
end

"""
    summarize_task_metrics!(summary::CostSummary, sig_hash::UInt, proc::Processor,
                            worker_id::Int; kernel_ns=0, alloc_bytes=nothing, rate=0)

Blend one finished task's measurements into `summary`: its kernel time
(`kernel_ns`, 0 if not measured), bytes allocated (`alloc_bytes`, `nothing`
if not measured) and the rate its inputs moved at (`rate`, 0 if it moved
none).
"""
function summarize_task_metrics!(summary::CostSummary, sig_hash::UInt, proc::Processor,
                                 worker_id::Int; kernel_ns::UInt64=UInt64(0),
                                 alloc_bytes::Union{UInt64,Nothing}=nothing,
                                 rate::UInt64=UInt64(0))
    T = typeof(proc)
    @lock summary.lock begin
        if kernel_ns != 0
            _blend!(summary.runtime_any, summary.once_runtime_any, sig_hash, kernel_ns)
            _blend!(summary.runtime_by_type, summary.once_runtime_by_type, (sig_hash, T), kernel_ns)
            _blend!(summary.runtime_by_proc, summary.once_runtime_by_proc, (sig_hash, proc), kernel_ns)
            _blend!(summary.runtime_by_type_worker, summary.once_runtime_by_type_worker, (sig_hash, T, worker_id), kernel_ns)
        end
        if alloc_bytes !== nothing
            _blend!(summary.alloc_any, summary.once_alloc_any, sig_hash, alloc_bytes)
            _blend!(summary.alloc_by_proc, summary.once_alloc_by_proc, (sig_hash, proc), alloc_bytes)
        end
        if rate != 0
            _blend!(summary.rate_by_proc, summary.once_rate_by_proc, proc, rate)
            _blend!(summary.rate_by_type_worker, summary.once_rate_by_type_worker, (T, worker_id), rate)
            _blend!(summary.rate_by_type, summary.once_rate_by_type, T, rate)
        end
    end
    return
end

"""
    runtime_estimate(summary::CostSummary, sig_hash::UInt, proc::Processor, worker_id::Int)
        -> Union{UInt64, Nothing}

The estimated kernel time, in nanoseconds, of a task with signature hash
`sig_hash` on `proc` (owned by `worker_id`), or `nothing` if no task with
that signature has finished yet. See [`CostSummary`](@ref) for the fallback
chain.
"""
function runtime_estimate(summary::CostSummary, sig_hash::UInt, proc::Processor, worker_id::Int)
    T = typeof(proc)
    @lock summary.lock begin
        r = get(summary.runtime_by_proc, (sig_hash, proc), nothing)
        r === nothing || return r
        r = get(summary.runtime_by_type_worker, (sig_hash, T, worker_id), nothing)
        r === nothing || return r
        r = get(summary.runtime_by_type, (sig_hash, T), nothing)
        r === nothing || return r
        return get(summary.runtime_any, sig_hash, nothing)
    end
end

"""
    runtime_estimate(summary::CostSummary, sig_hash::UInt, T::Type{<:Processor})
        -> Union{UInt64, Nothing}

The estimate for a task with signature hash `sig_hash` on any processor of
type `T`, falling back to any processor. This is what the scheduler ranks
candidates by: identical processors then cost the same and the tie is broken
fairly (a shuffle), where per-processor estimates differ by measurement noise
and by which thread happened to compile the task, and a sort then sends every
such task to whichever processor once measured lowest (or, with a compiled
first sample on the driver's threads, away from the driver: a matmul's output
tiles scattered over remote workers, 1587 MB moved per call against master's
239 MB). What per-processor estimates exist for -- a processor *type* that is
genuinely faster for the signature -- this keeps.
"""
function runtime_estimate(summary::CostSummary, sig_hash::UInt, T::Type{<:Processor})
    @lock summary.lock begin
        r = get(summary.runtime_by_type, (sig_hash, T), nothing)
        r === nothing || return r
        return get(summary.runtime_any, sig_hash, nothing)
    end
end

"""
    runtime_estimate(summary::CostSummary, sig_hash::UInt) -> Union{UInt64, Nothing}

The last tier of the chain alone: the estimate for a task with signature
hash `sig_hash` on any processor, or `nothing` if none has finished.
"""
runtime_estimate(summary::CostSummary, sig_hash::UInt) =
    @lock summary.lock get(summary.runtime_any, sig_hash, nothing)

"""
    alloc_estimate(summary::CostSummary, sig_hash::UInt, proc::Processor)
        -> Union{UInt64, Nothing}

The estimated bytes a task with signature hash `sig_hash` allocates on
`proc`, falling back to any processor, or `nothing` if none has finished.
"""
function alloc_estimate(summary::CostSummary, sig_hash::UInt, proc::Processor)
    @lock summary.lock begin
        r = get(summary.alloc_by_proc, (sig_hash, proc), nothing)
        r === nothing || return r
        return get(summary.alloc_any, sig_hash, nothing)
    end
end

"""
    transfer_rate_estimate(summary::CostSummary, proc::Processor, worker_id::Int)
        -> Union{UInt64, Nothing}

The estimated rate, in bytes per second, at which a task's inputs reach
`proc` (owned by `worker_id`): by that processor, then its type on that
worker, then its type anywhere; `nothing` if no task there has moved inputs.
"""
function transfer_rate_estimate(summary::CostSummary, proc::Processor, worker_id::Int)
    T = typeof(proc)
    @lock summary.lock begin
        r = get(summary.rate_by_proc, proc, nothing)
        r === nothing || return r
        r = get(summary.rate_by_type_worker, (T, worker_id), nothing)
        r === nothing || return r
        return get(summary.rate_by_type, T, nothing)
    end
end

function Base.empty!(summary::CostSummary)
    @lock summary.lock begin
        empty!(summary.runtime_by_proc)
        empty!(summary.runtime_by_type_worker)
        empty!(summary.runtime_by_type)
        empty!(summary.runtime_any)
        empty!(summary.alloc_by_proc)
        empty!(summary.alloc_any)
        empty!(summary.rate_by_proc)
        empty!(summary.rate_by_type_worker)
        empty!(summary.rate_by_type)
        empty!(summary.once_runtime_by_proc)
        empty!(summary.once_runtime_by_type_worker)
        empty!(summary.once_runtime_by_type)
        empty!(summary.once_runtime_any)
        empty!(summary.once_alloc_by_proc)
        empty!(summary.once_alloc_any)
        empty!(summary.once_rate_by_proc)
        empty!(summary.once_rate_by_type_worker)
        empty!(summary.once_rate_by_type)
    end
    return summary
end

"""
    apply_task_metrics!(cache::MT.MetricsCache, key::Int, m::TaskMetrics,
                        sig::Signature, proc::Processor, worker_id::Int)

Fold a finished task's [`TaskMetrics`](@ref) into `cache` under `key`, tagged
with the signature, processor and worker the scheduler knows the task by, and
blend its measurements into the [`CostSummary`](@ref).
"""
function apply_task_metrics!(cache::MT.MetricsCache, key::Int, m::TaskMetrics,
                             sig::Signature, proc::Processor, worker_id::Int)
    summarize_task_metrics!(global_cost_summary(), sig.hash, proc, worker_id;
                            kernel_ns=m.kernel_time,
                            alloc_bytes=UInt64(max(m.alloc.allocd, 0)),
                            rate=m.transfer_rate)
    MT.bulk_update!(cache) do c
        ctx = MT.pending_context!(c, Dagger, :execute!, Int)
        MT.set_metric_value!(MT.get_or_create_storage!(ctx, MT.TimeMetric()), key, m.time)
        if m.kernel_time != 0
            MT.set_metric_value!(MT.get_or_create_storage!(ctx, KernelTimeMetric()), key, m.kernel_time)
        end
        MT.set_metric_value!(MT.get_or_create_storage!(ctx, MT.AllocMetric()), key, m.alloc)
        MT.set_metric_value!(MT.get_or_create_storage!(ctx, SignatureMetric()), key, sig.sig)
        MT.set_metric_value!(MT.get_or_create_storage!(ctx, ProcessorMetric()), key, proc)
        MT.set_metric_value!(MT.get_or_create_storage!(ctx, WorkerMetric()), key, worker_id)
        if m.transfer_size != 0
            MT.set_metric_value!(MT.get_or_create_storage!(ctx, TransferSizeMetric()), key, m.transfer_size)
        end
        if m.transfer_time != 0
            MT.set_metric_value!(MT.get_or_create_storage!(ctx, TransferTimeMetric()), key, m.transfer_time)
        end
        if m.transfer_rate != 0
            MT.set_metric_value!(MT.get_or_create_storage!(ctx, TransferRateMetric()), key, m.transfer_rate)
        end
        if m.move_from !== nothing && m.move_to !== nothing
            MT.set_metric_value!(MT.get_or_create_storage!(ctx, FromSpaceMetric()), key, m.move_from)
            MT.set_metric_value!(MT.get_or_create_storage!(ctx, ToSpaceMetric()), key, m.move_to)
            if m.move_size != 0
                MT.set_metric_value!(MT.get_or_create_storage!(ctx, MoveSizeMetric()), key, m.move_size)
            end
        end
        # Trim in batches: let the context overshoot its bound by a slack
        # before cutting it back. `trim_context!` is O(keys) per call, so
        # trimming on every insert made each task completion cost O(bound) on
        # the scheduler, which is what kept the bound too small to hold even one
        # region's worth of samples.
        keep = METRICS_CACHE_MAX_TASKS[]
        if MT.context_key_count(ctx) > keep + metrics_cache_trim_slack(keep)
            MT.trim_context!(ctx, keep)
        end
    end
    return
end

# How far past its bound the cache may grow before a trim cuts it back.
metrics_cache_trim_slack(keep::Integer) = max(keep >> 3, 1)

function _move_matching_keys(snap::MT.MetricsSnapshot,
                             from_space::MemorySpace, to_space::MemorySpace)
    matched = MT.find_keys(snap, Dagger, :execute!,
                            (MT.LookupExact(FromSpaceMetric(), from_space),
                             MT.LookupExact(ToSpaceMetric(), to_space)))
    if isempty(matched)
        matched = MT.find_keys(snap, Dagger, :execute!,
                                (MT.LookupSubtype(FromSpaceMetric(), typeof(from_space)),
                                 MT.LookupSubtype(ToSpaceMetric(), typeof(to_space))))
    end
    return matched
end

function metrics_lookup_move_time(snap::MT.MetricsSnapshot,
                                   from_space::MemorySpace, to_space::MemorySpace;
                                   reducer::Function=Statistics.mean)
    matched = _move_matching_keys(snap, from_space, to_space)
    isempty(matched) && return nothing
    vals = UInt64[]
    sizehint!(vals, length(matched))
    for k in matched
        t = MT.lookup_value(snap, Dagger, :execute!, MT.TimeMetric(), k)
        if t !== nothing && t > 0
            push!(vals, t)
        end
    end
    return _reduce_uint64(reducer, vals)
end

metrics_lookup_move_time_median(snap, from_space, to_space) =
    metrics_lookup_move_time(snap, from_space, to_space; reducer=Statistics.median)
metrics_lookup_move_time_min(snap, from_space, to_space) =
    metrics_lookup_move_time(snap, from_space, to_space; reducer=minimum)
metrics_lookup_move_time_max(snap, from_space, to_space) =
    metrics_lookup_move_time(snap, from_space, to_space; reducer=maximum)

# Transfers below this are dominated by fixed per-task overhead rather than
# bandwidth, so their implied rate is numeric noise. Excluded from the sample.
const MOVE_RATE_MIN_SIZE_BYTES = UInt64(4096)

"""
    metrics_lookup_move_rate(snap, from_space, to_space; reducer=Statistics.median)

Estimated bytes/second between two memory spaces, or `nothing` if no usable
samples exist.

Reduces over *per-move* rates rather than `sum(size) / sum(time)`. The sum form
is not robust: a `move!` sample records end-to-end task time, so the first few
moves carry compilation and device-context setup. Measured on cholesky nt=4
bs=1024 CPU+GPU, 5 cold-start samples out of 36 (405-1072ms, against a 0.57ms
steady-state minimum) contributed ~96% of total elapsed time and dragged the
aggregate to ~90MB/s, roughly 100x below the 14.8GB/s observed at steady state.
That understates PCIe so severely that no compute advantage can offset it and
GPU placement is effectively banned.

Taking a median over per-move rates matches how the compute side already
reduces its samples (`metrics_lookup_runtime_median`), so both halves of the
cost model use the same estimator.

Samples of this exact pair of spaces are preferred, then those of any pair of
the same space types -- falling back when the exact samples yield no *rate*,
not only when there are none. Under MPI only a copy's source rank knows its
size (the destination holds a size-0 placeholder), so rank 0 records every
copy *into* it without a size. Falling back only on a lack of keys priced all
of those at the 1 MB/s default while every other pair got the ~1.5 GB/s it had
measured, and rank 0, which plans for every rank, kept work off itself.
"""
function metrics_lookup_move_rate(snap::MT.MetricsSnapshot,
                                   from_space::MemorySpace, to_space::MemorySpace;
                                   reducer::Function=Statistics.median)
    exact = MT.find_keys(snap, Dagger, :execute!,
                         (MT.LookupExact(FromSpaceMetric(), from_space),
                          MT.LookupExact(ToSpaceMetric(), to_space)))
    rate = _move_rate_from(snap, exact, reducer)
    rate === nothing || return rate
    similar_pairs = MT.find_keys(snap, Dagger, :execute!,
                                 (MT.LookupSubtype(FromSpaceMetric(), typeof(from_space)),
                                  MT.LookupSubtype(ToSpaceMetric(), typeof(to_space))))
    return _move_rate_from(snap, similar_pairs, reducer)
end

function _move_rate_from(snap::MT.MetricsSnapshot, matched, reducer::Function)
    isempty(matched) && return nothing
    rates = Float64[]
    sizehint!(rates, length(matched))
    for k in matched
        t = MT.lookup_value(snap, Dagger, :execute!, MT.TimeMetric(), k)
        s = MT.lookup_value(snap, Dagger, :execute!, MoveSizeMetric(), k)
        (t === nothing || s === nothing) && continue
        (t == 0 || s < MOVE_RATE_MIN_SIZE_BYTES) && continue
        push!(rates, Float64(s) / (Float64(t) / 1e9))
    end
    isempty(rates) && return nothing
    rate = reducer(rates)
    (isfinite(rate) && rate > 0) || return nothing
    return round(UInt64, rate)
end

function metrics_lookup_transfer_rate(snap::MT.MetricsSnapshot, proc::Processor, worker_id::Int)
    target = TransferRateMetric()
    rate = MT.cache_lookup(snap, Dagger, :execute!, target,
                           MT.LookupExact(ProcessorMetric(), proc))
    if rate !== nothing
        return rate::UInt64
    end
    rate = MT.cache_lookup(snap, Dagger, :execute!, target,
                           (MT.LookupSubtype(ProcessorMetric(), typeof(proc)),
                            MT.LookupCustom(WorkerMetric(), w -> w == worker_id)))
    if rate !== nothing
        return rate::UInt64
    end
    rate = MT.cache_lookup(snap, Dagger, :execute!, target,
                           MT.LookupSubtype(ProcessorMetric(), typeof(proc)))
    if rate !== nothing
        return rate::UInt64
    end
    return nothing
end
