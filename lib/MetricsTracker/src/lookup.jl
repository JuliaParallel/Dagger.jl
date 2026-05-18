abstract type AbstractLookup end

function lookup_match_metric end
function lookup_match_value end

struct LookupExact{M<:AbstractMetric, T} <: AbstractLookup
    metric::M
    target::T
end

lookup_match_metric(l::LookupExact{M}, metric::AbstractMetric) where {M} = isa(metric, M) && metric == l.metric

@inline function lookup_match_value(l::LookupExact{M, T}, value) where {M, T}
    if value isa T
        return l.target == value
    end
    return false
end

struct LookupSubtype{M<:AbstractMetric, T} <: AbstractLookup
    metric::M
    supertype::Type{T}
end

lookup_match_metric(l::LookupSubtype{M}, metric::AbstractMetric) where {M} = isa(metric, M) && metric == l.metric

@inline function lookup_match_value(::LookupSubtype{M, T}, value) where {M, T}
    return value isa T
end

struct LookupCustom{M<:AbstractMetric, F} <: AbstractLookup
    metric::M
    func::F
end

lookup_match_metric(l::LookupCustom{M}, metric::AbstractMetric) where {M} = isa(metric, M) && metric == l.metric
lookup_match_value(l::LookupCustom{M, F}, value) where {M, F} = l.func(value)

@inline function cache_lookup(cache::MetricsCache, mod::Module, context::Symbol, key, m::M) where {M<:AbstractMetric}
    return cache_lookup(snapshot(cache), mod, context, key, m)
end

@inline function cache_lookup(snap::MetricsSnapshot, mod::Module, context::Symbol, key, m::M) where {M<:AbstractMetric}
    return lookup_value(snap, mod, context, m, key)
end

function cache_lookup(cache::MetricsCache, mod::Module, context::Symbol,
                      target_metric::M, lookup) where {M<:AbstractMetric}
    return cache_lookup(snapshot(cache), mod, context, target_metric, lookup)
end

function cache_lookup(snap::MetricsSnapshot, mod::Module, context::Symbol,
                      target_metric::M, lookup::AbstractLookup) where {M<:AbstractMetric}
    T = metric_type(M)
    ctx = get(snap.contexts, (mod, context), nothing)
    ctx === nothing && return nothing
    target_storage = get(ctx.storages, target_metric, nothing)
    target_storage === nothing && return nothing
    for key in keys(target_storage.data)
        if matches_all(ctx, key, (lookup,))
            return target_storage.data[key]::T
        end
    end
    return nothing
end

function cache_lookup(snap::MetricsSnapshot, mod::Module, context::Symbol,
                      target_metric::M, lookups::Union{Vector, Tuple}) where {M<:AbstractMetric}
    T = metric_type(M)
    ctx = get(snap.contexts, (mod, context), nothing)
    ctx === nothing && return nothing
    target_storage = get(ctx.storages, target_metric, nothing)
    target_storage === nothing && return nothing
    for key in keys(target_storage.data)
        if matches_all(ctx, key, lookups)
            return target_storage.data[key]::T
        end
    end
    return nothing
end

function matches_all(ctx::AbstractContextStorage, key, lookups)
    for lookup in lookups
        if !matches_lookup(ctx, key, lookup)
            return false
        end
    end
    return true
end

function matches_lookup(ctx::AbstractContextStorage, key, lookup::AbstractLookup)
    for (metric, storage) in ctx.storages
        if lookup_match_metric(lookup, metric)
            if haskey(storage.data, key)
                value = storage.data[key]
                if lookup_match_value(lookup, value)
                    return true
                end
            end
        end
    end
    return false
end

function find_keys(snap::MetricsSnapshot, mod::Module, context::Symbol, lookup::AbstractLookup)
    result = Any[]
    ctx = get(snap.contexts, (mod, context), nothing)
    ctx === nothing && return result
    seen = Set{Any}()
    for (metric, storage) in ctx.storages
        if lookup_match_metric(lookup, metric)
            for (key, value) in storage.data
                if !(key in seen) && lookup_match_value(lookup, value)
                    push!(seen, key)
                    push!(result, key)
                end
            end
        end
    end
    return result
end

function find_keys(snap::MetricsSnapshot, mod::Module, context::Symbol,
                   lookups::Union{Vector, Tuple})
    result = Any[]
    ctx = get(snap.contexts, (mod, context), nothing)
    ctx === nothing && return result
    candidate_keys = Set{Any}()
    first_pass = true
    for lookup in lookups
        matched = Set{Any}()
        for (metric, storage) in ctx.storages
            if lookup_match_metric(lookup, metric)
                for (key, value) in storage.data
                    if lookup_match_value(lookup, value)
                        push!(matched, key)
                    end
                end
            end
        end
        if first_pass
            candidate_keys = matched
            first_pass = false
        else
            intersect!(candidate_keys, matched)
        end
        isempty(candidate_keys) && return result
    end
    append!(result, candidate_keys)
    return result
end

function ensure_key_indexes!(snap::MetricsSnapshot)
    indexes = @atomic snap.key_indexes
    if indexes === nothing
        new_indexes = Dict{Tuple{ContextKey, AbstractMetric, Any}, Any}()
        replace_result = @atomicreplace snap.key_indexes nothing => new_indexes
        if replace_result.success
            indexes = new_indexes
        else
            indexes = replace_result.old
        end
    end
    return indexes::Dict{Tuple{ContextKey, AbstractMetric, Any}, Any}
end

function index_keys_by_value(snap::MetricsSnapshot, mod::Module, context::Symbol,
                             m::M, target_value) where {M<:AbstractMetric}
    indexes = ensure_key_indexes!(snap)
    cache_key = ((mod, context), m, target_value)
    cached = get(indexes, cache_key, nothing)
    if cached !== nothing
        return cached::Vector{Any}
    end
    result = find_keys(snap, mod, context, LookupExact(m, target_value))
    indexes[cache_key] = result
    return result
end

function values_for_metric(snap::MetricsSnapshot, mod::Module, context::Symbol,
                           m::M) where {M<:AbstractMetric}
    T = metric_type(M)
    result = Dict{Any, T}()
    ctx = get(snap.contexts, (mod, context), nothing)
    ctx === nothing && return result
    storage = get(ctx.storages, m, nothing)
    storage === nothing && return result
    for (k, v) in storage.data
        result[k] = v
    end
    return result
end

function values_for_metric(snap::MetricsSnapshot, mod::Module, context::Symbol,
                           m::M, ::Type{K}) where {M<:AbstractMetric, K}
    T = metric_type(M)
    ctx = get(snap.contexts, (mod, context), nothing)
    ctx === nothing && return Dict{K, T}()
    storage = get(ctx.storages, m, nothing)
    storage === nothing && return Dict{K, T}()
    typed_storage = storage::MetricStorage{M, K, T}
    return typed_storage.data::Dict{K, T}
end
