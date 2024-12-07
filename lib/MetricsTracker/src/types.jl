abstract type AbstractMetric end

struct MetricsSpec{M<:Tuple}
    metrics::M

    function MetricsSpec(m::Vararg{AbstractMetric})
        return new{typeof(m)}(m)
    end
end

# TODO: Should we flip the order so that each metric has its own type-stable cache?
struct MetricsCache <: AbstractDict{Tuple{Module, Symbol}, Dict{Any, Dict{AbstractMetric, Any}}}
    results::Dict{Tuple{Module, Symbol}, Dict{Any, Dict{AbstractMetric, Any}}}

    MetricsCache() =
        new(Dict{Tuple{Module, Symbol}, Dict{Any, Dict{AbstractMetric, Any}}}())
end

abstract type SyncLocation end
struct SyncTask <: SyncLocation end
struct SyncGlobal <: SyncLocation end
struct SyncInto <: SyncLocation
    cache::MetricsCache
end

struct WithMetrics{MS<:MetricsSpec, C, K, S<:SyncLocation}
    spec::MS
    context::C
    key::K
    sync_loc::S
end

const COLLECTING_METRICS = ScopedValue{Bool}(false)
