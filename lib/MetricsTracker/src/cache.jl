mutable struct MetricsSnapshot
    const contexts::Dict{ContextKey, AbstractContextStorage}
    const generation::UInt64
    @atomic key_indexes::Union{Dict{Tuple{ContextKey, AbstractMetric, Any}, Any}, Nothing}
end

MetricsSnapshot(contexts::Dict{ContextKey, AbstractContextStorage}, generation::UInt64) =
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
    return storage
end

@inline function lookup_value(s::MetricsSnapshot, mod::Module, context::Symbol, m::M, key) where {M<:AbstractMetric}
    T = metric_type(M)
    ctx = get(s.contexts, (mod, context), nothing)
    ctx === nothing && return nothing
    storage = get(ctx.storages, m, nothing)
    storage === nothing && return nothing
    return get(storage.data, key, nothing)::Union{T, Nothing}
end

mutable struct MetricsCache
    @atomic generation::UInt64
    @atomic active_snapshot::MetricsSnapshot
    write_lock::ReentrantLock
    pending::Dict{ContextKey, AbstractContextStorage}

    function MetricsCache()
        empty_contexts = Dict{ContextKey, AbstractContextStorage}()
        initial_snapshot = MetricsSnapshot(empty_contexts, UInt64(0))
        return new(UInt64(0), initial_snapshot, ReentrantLock(), Dict{ContextKey, AbstractContextStorage}())
    end
end

function with_write_lock(f, cache::MetricsCache)
    return lock(f, cache.write_lock)
end

function pending_context!(cache::MetricsCache, mod::Module, context::Symbol, ::Type{K}=Any) where K
    ctx_key = (mod, context)
    ctx = get(cache.pending, ctx_key, nothing)
    if ctx === nothing
        new_ctx = ContextStorage(K)
        cache.pending[ctx_key] = new_ctx
        return new_ctx
    end
    if ctx isa ContextStorage{K}
        return ctx
    end
    existing_K = key_type(ctx::ContextStorage)
    throw(ArgumentError(
        "Context ($mod, $context) was initialized with key type $existing_K; cannot use key type $K"
    ))
end

function write_metric_value!(cache::MetricsCache, mod::Module, context::Symbol,
                             key::K, m::M, value) where {M<:AbstractMetric, K}
    with_write_lock(cache) do
        ctx = pending_context!(cache, mod, context, K)
        storage = get_or_create_storage!(ctx, m)
        set_metric_value!(storage, key, value)
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
        new_contexts = Dict{ContextKey, AbstractContextStorage}()
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

"""
    pending_context(cache::MetricsCache, mod::Module, context::Symbol)

Return the live (pending) `AbstractContextStorage` for `(mod, context)`, or
`nothing` if none exists, *without* building a snapshot. Reads the cache's
mutable state directly, so it is only safe when there are no concurrent writers
(e.g. a task-local cache being drained by its own task). For that case it avoids
the full deep-copy a `snapshot` would perform purely to read values back out.
"""
pending_context(cache::MetricsCache, mod::Module, context::Symbol) =
    get(cache.pending, (mod, context), nothing)

const GLOBAL_METRICS_CACHE = MetricsCache()
global_metrics_cache() = GLOBAL_METRICS_CACHE

const LOCAL_METRICS_CACHE = TaskLocalValue{MetricsCache}(() -> MetricsCache())
local_metrics_cache() = LOCAL_METRICS_CACHE[]

function reset_global_cache!()
    cache = GLOBAL_METRICS_CACHE
    with_write_lock(cache) do
        empty!(cache.pending)
        @atomic cache.generation = UInt64(0)
        @atomic cache.active_snapshot = MetricsSnapshot(Dict{ContextKey, AbstractContextStorage}(), UInt64(0))
        return
    end
    return
end

_reset_storage!(s::MetricStorage) = (empty!(s.data); empty!(s.insertion_order); nothing)

"""
    reset_pending!(cache::MetricsCache) -> cache

Clear a cache's pending values for reuse while *retaining* its allocated storage
objects: each per-metric `Dict` and insertion-order `Vector` is `empty!`'d but
keeps its capacity. This makes a `MetricsCache` cheaply reusable across many uses
(e.g. a task-local scratch cache drained after every task) without reallocating
its per-metric storage each time. Only safe when there are no concurrent
readers/writers of `cache` (as with a task-local cache owned by one task).
"""
function reset_pending!(cache::MetricsCache)
    with_write_lock(cache) do
        for (_, ctx) in cache.pending
            for (_, s) in ctx.storages
                _reset_storage!(s)
            end
        end
        @atomic cache.generation = UInt64(0)
        return
    end
    return cache
end

function copy_context(c::AbstractContextStorage)
    K = key_type(c)
    new_storage = ContextStorage(K)
    for (m, s) in c.storages
        new_storage.storages[m] = copy_storage(s)
    end
    return new_storage
end

function merge_into!(dest::MetricsCache, src::MetricsCache)
    src_snap = snapshot(src)
    bulk_update!(dest) do c
        for (ctx_key, ctx) in src_snap.contexts
            K = key_type(ctx)
            dest_ctx = pending_context!(c, ctx_key[1], ctx_key[2], K)
            for (metric, storage) in ctx.storages
                dest_storage = get_or_create_storage!(dest_ctx, metric)
                for k in storage.insertion_order
                    set_metric_value!(dest_storage, k, storage.data[k])
                end
            end
        end
    end
    return dest
end

function trim!(cache::MetricsCache; keep_per_metric::Integer)
    keep = Int(keep_per_metric)
    @assert keep >= 0 "keep_per_metric must be non-negative"
    bulk_update!(cache) do c
        for (_, ctx) in c.pending
            for (_, storage) in ctx.storages
                _trim_storage!(storage, keep)
            end
        end
    end
    return cache
end

function _trim_storage!(s::MetricStorage{M, K, T}, keep::Int) where {M, K, T}
    sync_insertion_order!(s)
    n = length(s.data)
    n <= keep && return
    drop = n - keep
    for i in 1:drop
        k = s.insertion_order[i]
        delete!(s.data, k)
    end
    deleteat!(s.insertion_order, 1:drop)
    return
end

"""
    trim_context!(ctx::AbstractContextStorage, max_keys::Integer)

Bound the number of distinct keys retained in `ctx` to the most-recent
`max_keys`, evicting the oldest keys (by first-seen insertion order) from *every*
storage in the context together.

Unlike per-storage trimming, this keeps a key present-or-absent consistently
across all of a context's metrics, which is required by multi-metric lookups
(e.g. matching a `SignatureMetric` *and* a `ProcessorMetric` on the same key).
The longest per-metric insertion order is used as the canonical key ordering: a
metric that is recorded for every key (e.g. an always-present timing metric) is a
superset of the others and captures the true global oldest->newest order.
"""
function trim_context!(ctx::AbstractContextStorage, max_keys::Integer)
    max_keys < 0 && return ctx
    keep = Int(max_keys)
    canonical = nothing
    canonical_len = -1
    for (_, s) in ctx.storages
        sync_insertion_order!(s)
        len = length(s.insertion_order)
        if len > canonical_len
            canonical_len = len
            canonical = s.insertion_order
        end
    end
    (canonical === nothing || canonical_len <= keep) && return ctx
    drop = canonical_len - keep
    # Snapshot the keys to evict before mutating any insertion order.
    evict = canonical[1:drop]
    for k in evict
        for (_, s) in ctx.storages
            delete_metric_value!(s, k)
        end
    end
    return ctx
end
