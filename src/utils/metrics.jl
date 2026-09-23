import Statistics

# The facts a task's metrics record live on its `DTaskTLS` (set once by
# `Sch.do_task`), not in `ScopedValue`s. Entering a scope for five of them cost
# ~939 allocations / 32 KB per task on a `fetch(@spawn 1+1)` round-trip --
# more than the entire rest of the scheduler path -- and `set_tls!` already
# runs once per thunk, so reading from it is free.
_metrics_tls() = DTASK_TLS[]

struct SignatureMetric <: MT.AbstractMetric end
MT.metric_applies(::SignatureMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{SignatureMetric}) = Union{Vector{Any}, Nothing}
MT.start_metric(::SignatureMetric) = nothing
MT.stop_metric(::SignatureMetric, _) = (tls = _metrics_tls(); tls === nothing ? nothing : tls.metrics_sig)

struct ProcessorMetric <: MT.AbstractMetric end
MT.metric_applies(::ProcessorMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{ProcessorMetric}) = Union{Processor, Nothing}
MT.start_metric(::ProcessorMetric) = nothing
MT.stop_metric(::ProcessorMetric, _) = (tls = _metrics_tls(); tls === nothing ? nothing : tls.processor)

struct WorkerMetric <: MT.AbstractMetric end
MT.metric_applies(::WorkerMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{WorkerMetric}) = Union{Int, Nothing}
MT.start_metric(::WorkerMetric) = nothing
MT.stop_metric(::WorkerMetric, _) = (_metrics_tls() === nothing ? nothing : myid())

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

const EXECUTE_METRICS_SPEC = MT.MetricsSpec(
    MT.TimeMetric(),
    MT.ThreadTimeMetric(),
    MT.AllocMetric(),
    SignatureMetric(),
    ProcessorMetric(),
    WorkerMetric(),
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
    target = MT.ThreadTimeMetric()
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
`build_signature_runtime_index`. Groups measured `ThreadTimeMetric`
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
(processor, worker_id, ThreadTimeMetric) tuple and index into the four
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
    time_storage = get(ctx.storages, MT.ThreadTimeMetric(), nothing)

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

    return SignatureRuntimeIndex(by_proc_worker, by_type_worker, by_type, any_matching)
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

Per-task memo of `build_signature_runtime_index` results, keyed by signature
hash and valid only for one snapshot object.

Building an index is a full scan of the snapshot's signature storage — measured
at 770 allocations / 30 KB — and the scheduler builds one per scheduling pass.
But the index is a pure function of `(snapshot, signature)`, and in steady state
both repeat: signatures recur across tasks, and the cost model reuses one
snapshot for as long as it is allowed to go stale. Memoizing therefore collapses
the per-task build to a dictionary lookup.

Keyed on the snapshot's `objectid`, so a rebuild invalidates every entry at
once. Task-local, so no locking and no sharing between the concurrent
scheduling tasks the hierarchical path spawns.
"""
mutable struct SignatureRuntimeIndexCache
    snap_id::UInt
    entries::Dict{UInt, SignatureRuntimeIndex}
    # `metrics_lookup_alloc` and `metrics_lookup_transfer_rate` both resolve
    # through `MT.find_keys`, which scans every storage and every key of the
    # snapshot while building `Set{Any}`s -- and they run once per candidate
    # processor per task. Unlike the runtime lookup they have no index, so
    # their (small) results are memoized directly. This is the single largest
    # remaining cost on the scheduling path; profiling attributes it to
    # `MetricsTracker/lookup.jl`'s scan loop.
    # N.B. A miss is a meaningful result and is cached as such: the scan costs
    # the same whether or not it finds anything, so `nothing` must be memoized
    # too or the common no-samples case keeps paying full price.
    alloc::Dict{Tuple{UInt, UInt}, Union{UInt64, Nothing}}
    rate::Dict{Tuple{UInt, Int}, Union{UInt64, Nothing}}
end
SignatureRuntimeIndexCache() =
    SignatureRuntimeIndexCache(UInt(0), Dict{UInt, SignatureRuntimeIndex}(),
                               Dict{Tuple{UInt, UInt}, Union{UInt64, Nothing}}(),
                               Dict{Tuple{UInt, Int}, Union{UInt64, Nothing}}())

const SIGNATURE_RUNTIME_INDEX_CACHE =
    TaskLocalValue{SignatureRuntimeIndexCache}(() -> SignatureRuntimeIndexCache())

"""
    cached_signature_runtime_index(snap, sig, sig_hash) -> SignatureRuntimeIndex

`build_signature_runtime_index`, memoized per `(snapshot, sig_hash)`. See
[`SignatureRuntimeIndexCache`](@ref).
"""
function cached_signature_runtime_index(snap::MT.MetricsSnapshot, sig::Vector, sig_hash::UInt)
    cache = SIGNATURE_RUNTIME_INDEX_CACHE[]
    snap_id = objectid(snap)
    _reset_cost_cache_if_stale!(cache, snap_id)
    existing = get(cache.entries, sig_hash, nothing)
    existing === nothing || return existing
    idx = build_signature_runtime_index(snap, sig)
    cache.entries[sig_hash] = idx
    return idx
end

function _reset_cost_cache_if_stale!(cache::SignatureRuntimeIndexCache, snap_id::UInt)
    if cache.snap_id != snap_id
        # New snapshot: every memoized result describes the old one.
        empty!(cache.entries)
        empty!(cache.alloc)
        empty!(cache.rate)
        cache.snap_id = snap_id
    end
    return
end

"""
    cached_metrics_lookup_alloc(snap, sig, sig_hash, proc)

`metrics_lookup_alloc`, memoized per `(snapshot, sig_hash, proc)`. See
[`SignatureRuntimeIndexCache`](@ref) for why.
"""
function cached_metrics_lookup_alloc(snap::MT.MetricsSnapshot, sig::Vector,
                                     sig_hash::UInt, proc::Processor)
    cache = SIGNATURE_RUNTIME_INDEX_CACHE[]
    _reset_cost_cache_if_stale!(cache, objectid(snap))
    key = (sig_hash, hash(proc))
    haskey(cache.alloc, key) && return cache.alloc[key]
    val = metrics_lookup_alloc(snap, sig, proc)
    cache.alloc[key] = val
    return val
end

"""
    cached_metrics_lookup_transfer_rate(snap, proc, worker_id)

`metrics_lookup_transfer_rate`, memoized per `(snapshot, proc, worker_id)`. See
[`SignatureRuntimeIndexCache`](@ref) for why.
"""
function cached_metrics_lookup_transfer_rate(snap::MT.MetricsSnapshot,
                                             proc::Processor, worker_id::Int)
    cache = SIGNATURE_RUNTIME_INDEX_CACHE[]
    _reset_cost_cache_if_stale!(cache, objectid(snap))
    key = (hash(proc), worker_id)
    haskey(cache.rate, key) && return cache.rate[key]
    val = metrics_lookup_transfer_rate(snap, proc, worker_id)
    cache.rate[key] = val
    return val
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

function extract_collected_metrics(local_cache::MT.MetricsCache, key)
    # `local_cache` is task-local and already fully written by `with_metrics`
    # (on this same task) before we drain it, so read its pending storages
    # directly instead of taking a defensive deep-copy snapshot.
    ctx = MT.pending_context(local_cache, Dagger, :execute!)
    ctx === nothing && return nothing
    pairs = Tuple{MT.AbstractMetric, Any}[]
    for (metric, storage) in ctx.storages
        if haskey(storage.data, key)
            push!(pairs, (metric, storage.data[key]))
        end
    end
    isempty(pairs) && return nothing
    return pairs
end

# Bound the global metrics cache to the most-recent this-many tasks (distinct
# thunk_id keys) per `(mod, context)`, plus a trim slack (see
# `apply_collected_metrics!`). Without a bound the cache grows one entry per
# metric per task forever, which dominates scheduler allocations (Dict rehash
# churn) on long-running workloads. The cost model only needs recent samples,
# so we keep a rolling window.
const METRICS_CACHE_MAX_TASKS = Ref(100)

"""
    metrics_cache_max_tasks!(n::Integer)

Set the rolling-window bound described above, returning the previous value.

The default of 100 is tuned for steady-state scheduling, where only recent
samples matter. It is too small for benchmark harnesses that warm the cost
model deliberately: a GPU warmup writes ~60 entries, after which CPU warmup and
measured trials push past 100 and evict the GPU samples, so heterogeneous cost
lookups silently fall back to CPU-derived estimates. Measured demand for a full
warm+trials cycle is ~235 entries at cholesky nt=4 and ~835 at nt=8.

Note this is process-local; multi-worker runs must set it on each worker.
"""
function metrics_cache_max_tasks!(n::Integer)
    n > 0 || throw(ArgumentError("metrics cache bound must be positive, got $n"))
    old = METRICS_CACHE_MAX_TASKS[]
    METRICS_CACHE_MAX_TASKS[] = Int(n)
    return old
end

function apply_collected_metrics!(cache::MT.MetricsCache, key::K, pairs) where K
    pairs === nothing && return
    isempty(pairs) && return
    MT.bulk_update!(cache) do c
        ctx = MT.pending_context!(c, Dagger, :execute!, K)
        for (metric, value) in pairs
            value === nothing && continue
            storage = MT.get_or_create_storage!(ctx, metric)
            MT.set_metric_value!(storage, key, value)
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
"""
function metrics_lookup_move_rate(snap::MT.MetricsSnapshot,
                                   from_space::MemorySpace, to_space::MemorySpace;
                                   reducer::Function=Statistics.median)
    matched = _move_matching_keys(snap, from_space, to_space)
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
