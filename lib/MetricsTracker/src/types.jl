abstract type AbstractMetric end

metric_type(::M) where {M<:AbstractMetric} = metric_type(M)
metric_type(::Type{<:AbstractMetric}) = Any

metric_applies(::AbstractMetric, _) = false
is_result_metric(::AbstractMetric) = false

start_metric(::AbstractMetric) = nothing
stop_metric(::AbstractMetric, _) = nothing
result_metric(::AbstractMetric, _) = nothing

struct MetricsSpec{M<:Tuple}
    metrics::M

    function MetricsSpec(m::Vararg{AbstractMetric})
        return new{typeof(m)}(m)
    end
end

const ContextKey = Tuple{Module, Symbol}

abstract type AbstractMetricStorage end

struct MetricStorage{M<:AbstractMetric, T} <: AbstractMetricStorage
    metric::M
    data::Dict{Any, T}
    MetricStorage(metric::M) where {M<:AbstractMetric} =
        new{M, metric_type(M)}(metric, Dict{Any, metric_type(M)}())
    MetricStorage{M, T}(metric::M, data::Dict{Any, T}) where {M<:AbstractMetric, T} =
        new{M, T}(metric, data)
end

Base.length(s::MetricStorage) = length(s.data)
Base.isempty(s::MetricStorage) = isempty(s.data)
Base.keys(s::MetricStorage) = keys(s.data)
Base.values(s::MetricStorage) = values(s.data)
Base.iterate(s::MetricStorage, args...) = iterate(s.data, args...)
Base.haskey(s::MetricStorage, key) = haskey(s.data, key)
Base.getindex(s::MetricStorage{M, T}, key) where {M, T} = s.data[key]::T
Base.get(s::MetricStorage{M, T}, key, default) where {M, T} = get(s.data, key, default)::Union{T, typeof(default)}

function copy_storage(s::MetricStorage{M, T}) where {M, T}
    return MetricStorage{M, T}(s.metric, copy(s.data))
end

struct ContextStorage
    storages::IdDict{AbstractMetric, AbstractMetricStorage}
    ContextStorage() = new(IdDict{AbstractMetric, AbstractMetricStorage}())
end

Base.isempty(c::ContextStorage) = isempty(c.storages)
Base.length(c::ContextStorage) = length(c.storages)
Base.keys(c::ContextStorage) = keys(c.storages)
Base.values(c::ContextStorage) = values(c.storages)
Base.iterate(c::ContextStorage, args...) = iterate(c.storages, args...)
Base.haskey(c::ContextStorage, m::AbstractMetric) = haskey(c.storages, m)
Base.getindex(c::ContextStorage, m::AbstractMetric) = c.storages[m]

function copy_context(c::ContextStorage)
    new_storage = ContextStorage()
    for (m, s) in c.storages
        new_storage.storages[m] = copy_storage(s)
    end
    return new_storage
end

function get_or_create_storage!(c::ContextStorage, m::M) where {M<:AbstractMetric}
    storage = get(c.storages, m, nothing)
    if storage === nothing
        new_storage = MetricStorage(m)
        c.storages[m] = new_storage
        return new_storage
    end
    return storage::MetricStorage{M, metric_type(M)}
end

abstract type SyncLocation end
struct SyncTask <: SyncLocation end
struct SyncGlobal <: SyncLocation end
struct SyncInto{C} <: SyncLocation
    cache::C
end

const COLLECTING_METRICS = ScopedValue{Bool}(false)

const METRIC_REGION = ScopedValue{Union{ContextKey, Nothing}}(nothing)
metric_region() = METRIC_REGION[]

const METRIC_KEY = ScopedValue{Union{Some{Any}, Nothing}}(nothing)
metric_key() = something(METRIC_KEY[])
