abstract type AbstractLookup end

function lookup_match_metric end
function lookup_match_value end

function cache_lookup(cache::MetricsCache, mod::Module, context::Symbol, key, target_metric::AbstractMetric)
    # Check if the cache has the results for this module and context
    if !haskey(cache.results, (mod, context))
        return nothing
    end
    inner_cache = cache.results[(mod, context)]

    # Check if the cache has the results for this key
    if !haskey(inner_cache, key)
        return nothing
    end
    keyed_metrics = inner_cache[key]

    # Check if the target metric exists
    if !haskey(keyed_metrics, target_metric)
        return nothing
    end
    target_metric_type = metric_type(target_metric)
    return keyed_metrics[target_metric]::target_metric_type
end
function cache_lookup(cache::MetricsCache, mod::Module, context::Symbol, target_metric::AbstractMetric, lookup::AbstractLookup)
    # Check if the cache has the results for this module and context
    if !haskey(cache.results, (mod, context))
        return nothing
    end
    inner_cache = cache.results[(mod, context)]

    target_metric_type = metric_type(target_metric)
    for (key, keyed_metrics) in inner_cache
        for (metric, value) in keyed_metrics
            # Check if lookup matches for this key
            if lookup_match_metric(lookup, metric) && lookup_match_value(lookup, value)
                # Lookup matched, return the target metric if it exists
                if !haskey(keyed_metrics, target_metric)
                    return nothing
                end
                return keyed_metrics[target_metric]::target_metric_type
            end
        end
    end
    return nothing
end
function cache_lookup(cache::MetricsCache, mod::Module, context::Symbol, target_metric::AbstractMetric, lookups::Union{Vector, Tuple})
    # Check if the cache has the results for this module and context
    if !haskey(cache.results, (mod, context))
        return nothing
    end
    inner_cache = cache.results[(mod, context)]

    target_metric_type = metric_type(target_metric)
    for (key, keyed_metrics) in inner_cache
        # Check if all lookups match for this key
        all_lookups_matched = true
        for lookup in lookups
            lookup_matched = false
            for (metric, value) in keyed_metrics
                if lookup_match_metric(lookup, metric) && lookup_match_value(lookup, value)
                    lookup_matched = true
                    break
                end
            end
            if !lookup_matched
                all_lookups_matched = false
                break
            end
        end

        if all_lookups_matched
            # All lookups matched, return the target metric if it exists
            if !haskey(keyed_metrics, target_metric)
                return nothing
            end
            return keyed_metrics[target_metric]::target_metric_type
        end
    end
    return nothing
end

struct LookupExact{M<:AbstractMetric,T} <: AbstractLookup
    metric::M
    target::T
end
lookup_match_metric(l::LookupExact, metric::AbstractMetric) = l.metric == metric
lookup_match_value(l::LookupExact{M,T}, value::T) where {M,T} = l.target == value

struct LookupSubtype{M<:AbstractMetric,T} <: AbstractLookup
    metric::M
    supertype::Type{T}
end
lookup_match_metric(l::LookupSubtype, metric::AbstractMetric) = l.metric == metric
lookup_match_value(l::LookupSubtype{M,T}, ::T) where {M,T} = true
lookup_match_value(l::LookupSubtype{M,T1}, ::T2) where {M,T1,T2} = false

struct LookupCustom{M<:AbstractMetric,F} <: AbstractLookup
    metric::M
    func::F
end
lookup_match_metric(l::LookupCustom, metric::AbstractMetric) = l.metric == metric
lookup_match_value(l::LookupCustom{M,F}, value) where {M,F} = l.func(value)
