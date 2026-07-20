import Statistics

const TASK_SIGNATURE = ScopedValue{Union{Vector{Any}, Nothing}}(nothing)
const TASK_PROCESSOR = ScopedValue{Union{Processor, Nothing}}(nothing)
const TASK_WORKER = ScopedValue{Union{Int, Nothing}}(nothing)
const TASK_TRANSFER_SIZE = ScopedValue{Union{UInt64, Nothing}}(nothing)
const TASK_TRANSFER_TIME = ScopedValue{Union{UInt64, Nothing}}(nothing)

struct SignatureMetric <: MT.AbstractMetric end
MT.metric_applies(::SignatureMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{SignatureMetric}) = Union{Vector{Any}, Nothing}
MT.start_metric(::SignatureMetric) = nothing
MT.stop_metric(::SignatureMetric, _) = TASK_SIGNATURE[]

struct ProcessorMetric <: MT.AbstractMetric end
MT.metric_applies(::ProcessorMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{ProcessorMetric}) = Union{Processor, Nothing}
MT.start_metric(::ProcessorMetric) = nothing
MT.stop_metric(::ProcessorMetric, _) = TASK_PROCESSOR[]

struct WorkerMetric <: MT.AbstractMetric end
MT.metric_applies(::WorkerMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{WorkerMetric}) = Union{Int, Nothing}
MT.start_metric(::WorkerMetric) = nothing
MT.stop_metric(::WorkerMetric, _) = TASK_WORKER[]

struct TransferSizeMetric <: MT.AbstractMetric end
MT.metric_applies(::TransferSizeMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{TransferSizeMetric}) = Union{UInt64, Nothing}
MT.start_metric(::TransferSizeMetric) = nothing
MT.stop_metric(::TransferSizeMetric, _) = TASK_TRANSFER_SIZE[]

struct TransferTimeMetric <: MT.AbstractMetric end
MT.metric_applies(::TransferTimeMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{TransferTimeMetric}) = Union{UInt64, Nothing}
MT.start_metric(::TransferTimeMetric) = nothing
MT.stop_metric(::TransferTimeMetric, _) = TASK_TRANSFER_TIME[]

struct TransferRateMetric <: MT.AbstractMetric end
MT.metric_applies(::TransferRateMetric, ::Val{:execute!}) = true
MT.metric_type(::Type{TransferRateMetric}) = Union{UInt64, Nothing}
MT.start_metric(::TransferRateMetric) = nothing
function MT.stop_metric(::TransferRateMetric, _)
    size = TASK_TRANSFER_SIZE[]
    elapsed = TASK_TRANSFER_TIME[]
    if size === nothing || elapsed === nothing || elapsed == 0 || size == 0
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

function instrumented_move!(dep_mod, dest_space::MemorySpace, source_space::MemorySpace,
                            dest::Chunk, source::Chunk)
    result = move!(dep_mod, dest_space, source_space, dest, source)
    tls = DTASK_TLS[]
    if tls !== nothing && tls.metrics_cache !== nothing
        thunk_id = tls.sch_handle.thunk_id.id
        raw_size = source.handle.size
        size = raw_size === nothing ? nothing : UInt64(raw_size)
        _record_move_metrics!(tls.metrics_cache, thunk_id, source_space, dest_space, size)
    end
    return result
end

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

metrics_lookup_runtime_mean(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=Statistics.mean)
metrics_lookup_runtime_median(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=Statistics.median)
metrics_lookup_runtime_min(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=minimum)
metrics_lookup_runtime_max(snap, sig, proc, worker_id) =
    metrics_lookup_runtime(snap, sig, proc, worker_id; reducer=maximum)

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
# thunk_id keys) per `(mod, context)`. Without a bound the cache grows one entry
# per metric per task forever, which dominates scheduler allocations (Dict
# rehash churn) on long-running workloads. The cost model only needs recent
# samples, so we keep a rolling window.
const METRICS_CACHE_MAX_TASKS = 100

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
        MT.trim_context!(ctx, METRICS_CACHE_MAX_TASKS)
    end
    return
end

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

function metrics_lookup_move_rate(snap::MT.MetricsSnapshot,
                                   from_space::MemorySpace, to_space::MemorySpace)
    matched = _move_matching_keys(snap, from_space, to_space)
    isempty(matched) && return nothing

    total_time = UInt64(0)
    total_size = UInt64(0)
    for k in matched
        t = MT.lookup_value(snap, Dagger, :execute!, MT.TimeMetric(), k)
        s = MT.lookup_value(snap, Dagger, :execute!, MoveSizeMetric(), k)
        if t !== nothing && s !== nothing && t > 0 && s > 0
            total_time += t
            total_size += s
        end
    end
    (total_time == 0 || total_size == 0) && return nothing
    return round(UInt64, Float64(total_size) / (Float64(total_time) / 1e9))
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
