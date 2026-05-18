mutable struct MetricsSnapshot
    const contexts::Dict{ContextKey, ContextStorage}
    const generation::UInt64
    @atomic key_indexes::Union{Dict{Tuple{ContextKey, AbstractMetric, Any}, Any}, Nothing}
end

MetricsSnapshot(contexts::Dict{ContextKey, ContextStorage}, generation::UInt64) =
    MetricsSnapshot(contexts, generation, nothing)

Base.length(s::MetricsSnapshot) = length(s.contexts)
Base.isempty(s::MetricsSnapshot) = isempty(s.contexts)
Base.keys(s::MetricsSnapshot) = keys(s.contexts)
Base.iterate(s::MetricsSnapshot, args...) = iterate(s.contexts, args...)
Base.haskey(s::MetricsSnapshot, ctx::ContextKey) = haskey(s.contexts, ctx)
Base.getindex(s::MetricsSnapshot, ctx::ContextKey) = s.contexts[ctx]

function context_storage(s::MetricsSnapshot, mod::Module, context::Symbol)
    return get(s.contexts, (mod, context), nothing)
end

function metric_storage(s::MetricsSnapshot, mod::Module, context::Symbol, m::M) where {M<:AbstractMetric}
    ctx = get(s.contexts, (mod, context), nothing)
    ctx === nothing && return nothing
    storage = get(ctx.storages, m, nothing)
    storage === nothing && return nothing
    return storage::MetricStorage{M, metric_type(M)}
end

@inline function lookup_value(s::MetricsSnapshot, mod::Module, context::Symbol, m::M, key) where {M<:AbstractMetric}
    T = metric_type(M)
    ctx = get(s.contexts, (mod, context), nothing)
    ctx === nothing && return nothing
    storage = get(ctx.storages, m, nothing)
    storage === nothing && return nothing
    typed_storage = storage::MetricStorage{M, T}
    return get(typed_storage.data, key, nothing)::Union{T, Nothing}
end

mutable struct MetricsCache
    @atomic generation::UInt64
    @atomic active_snapshot::MetricsSnapshot
    write_lock::ReentrantLock
    pending::Dict{ContextKey, ContextStorage}

    function MetricsCache()
        empty_contexts = Dict{ContextKey, ContextStorage}()
        initial_snapshot = MetricsSnapshot(empty_contexts, UInt64(0))
        return new(UInt64(0), initial_snapshot, ReentrantLock(), Dict{ContextKey, ContextStorage}())
    end
end

function with_write_lock(f, cache::MetricsCache)
    return lock(f, cache.write_lock)
end

function pending_context!(cache::MetricsCache, mod::Module, context::Symbol)
    ctx_key = (mod, context)
    ctx = get(cache.pending, ctx_key, nothing)
    if ctx === nothing
        new_ctx = ContextStorage()
        cache.pending[ctx_key] = new_ctx
        return new_ctx
    end
    return ctx
end

function write_metric_value!(cache::MetricsCache, mod::Module, context::Symbol,
                             key, m::M, value) where {M<:AbstractMetric}
    with_write_lock(cache) do
        ctx = pending_context!(cache, mod, context)
        storage = get_or_create_storage!(ctx, m)
        storage.data[key] = value
        @atomic cache.generation += UInt64(1)
        return
    end
    return
end

function bulk_update!(f::Function, cache::MetricsCache)
    with_write_lock(cache) do
        f(cache)
        @atomic cache.generation += UInt64(1)
        return
    end
    return
end

function snapshot(cache::MetricsCache)
    active = @atomic cache.active_snapshot
    current_gen = @atomic cache.generation
    if active.generation == current_gen
        return active
    end
    return rebuild_snapshot!(cache)
end

function rebuild_snapshot!(cache::MetricsCache)
    return with_write_lock(cache) do
        active = @atomic cache.active_snapshot
        current_gen = @atomic cache.generation
        if active.generation == current_gen
            return active
        end
        new_contexts = Dict{ContextKey, ContextStorage}()
        for (ctx_key, ctx) in cache.pending
            new_contexts[ctx_key] = copy_context(ctx)
        end
        new_snapshot = MetricsSnapshot(new_contexts, current_gen)
        @atomic cache.active_snapshot = new_snapshot
        return new_snapshot
    end
end

function snapshot_view(cache::MetricsCache)
    return snapshot(cache)
end

const GLOBAL_METRICS_CACHE = MetricsCache()
global_metrics_cache() = GLOBAL_METRICS_CACHE

const LOCAL_METRICS_CACHE = TaskLocalValue{MetricsCache}(() -> MetricsCache())
local_metrics_cache() = LOCAL_METRICS_CACHE[]

function reset_global_cache!()
    cache = GLOBAL_METRICS_CACHE
    with_write_lock(cache) do
        empty!(cache.pending)
        @atomic cache.generation = UInt64(0)
        @atomic cache.active_snapshot = MetricsSnapshot(Dict{ContextKey, ContextStorage}(), UInt64(0))
        return
    end
    return
end

function merge_into!(dest::MetricsCache, src::MetricsCache)
    src_snap = snapshot(src)
    bulk_update!(dest) do c
        for (ctx_key, ctx) in src_snap.contexts
            dest_ctx = pending_context!(c, ctx_key[1], ctx_key[2])
            for (metric, storage) in ctx.storages
                dest_storage = get_or_create_storage!(dest_ctx, metric)
                for (k, v) in storage.data
                    dest_storage.data[k] = v
                end
            end
        end
    end
    return dest
end
