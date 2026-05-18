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

function metrics_lookup_runtime(snap::MT.MetricsSnapshot, sig::Vector,
                                proc::Processor, worker_id::Int)
    target = MT.ThreadTimeMetric()
    result = MT.cache_lookup(snap, Dagger, :execute!, target,
                             (MT.LookupExact(SignatureMetric(), sig),
                              MT.LookupExact(ProcessorMetric(), proc)))
    if result !== nothing
        return result::UInt64
    end

    result = MT.cache_lookup(snap, Dagger, :execute!, target,
                             (MT.LookupExact(SignatureMetric(), sig),
                              MT.LookupSubtype(ProcessorMetric(), typeof(proc)),
                              MT.LookupCustom(WorkerMetric(), w -> w == worker_id)))
    if result !== nothing
        return result::UInt64
    end

    result = MT.cache_lookup(snap, Dagger, :execute!, target,
                             (MT.LookupExact(SignatureMetric(), sig),
                              MT.LookupSubtype(ProcessorMetric(), typeof(proc))))
    if result !== nothing
        return result::UInt64
    end

    result = MT.cache_lookup(snap, Dagger, :execute!, target,
                             MT.LookupExact(SignatureMetric(), sig))
    if result !== nothing
        return result::UInt64
    end

    return nothing
end

function metrics_lookup_alloc(snap::MT.MetricsSnapshot, sig::Vector,
                              proc::Processor)
    target = MT.AllocMetric()
    diff = MT.cache_lookup(snap, Dagger, :execute!, target,
                           (MT.LookupExact(SignatureMetric(), sig),
                            MT.LookupExact(ProcessorMetric(), proc)))
    if diff !== nothing
        gc_diff = diff::Base.GC_Diff
        return UInt64(max(gc_diff.allocd, 0))
    end
    diff = MT.cache_lookup(snap, Dagger, :execute!, target,
                           MT.LookupExact(SignatureMetric(), sig))
    if diff !== nothing
        gc_diff = diff::Base.GC_Diff
        return UInt64(max(gc_diff.allocd, 0))
    end
    return nothing
end

function extract_collected_metrics(local_cache::MT.MetricsCache, key)
    snap = MT.snapshot(local_cache)
    ctx = get(snap.contexts, (Dagger, :execute!), nothing)
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

function apply_collected_metrics!(cache::MT.MetricsCache, key, pairs)
    pairs === nothing && return
    isempty(pairs) && return
    MT.bulk_update!(cache) do c
        ctx = MT.pending_context!(c, Dagger, :execute!)
        for (metric, value) in pairs
            value === nothing && continue
            storage = MT.get_or_create_storage!(ctx, metric)
            storage.data[key] = value
        end
    end
    return
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
