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

struct MetricStorage{M<:AbstractMetric, K, T} <: AbstractMetricStorage
    metric::M
    data::Dict{K, T}
    insertion_order::Vector{K}
    MetricStorage(metric::M, ::Type{K}=Any) where {M<:AbstractMetric, K} =
        new{M, K, metric_type(M)}(metric, Dict{K, metric_type(M)}(), Vector{K}())
    MetricStorage{M, K, T}(metric::M, data::Dict{K, T}, order::Vector{K}=collect(keys(data))) where {M<:AbstractMetric, K, T} =
        new{M, K, T}(metric, data, order)
end

function set_metric_value!(s::MetricStorage{M, K, T}, key::K, value::T) where {M, K, T}
    if !haskey(s.data, key)
        push!(s.insertion_order, key)
    end
    s.data[key] = value
    return value
end

function delete_metric_value!(s::MetricStorage{M, K, T}, key::K) where {M, K, T}
    if haskey(s.data, key)
        delete!(s.data, key)
        idx = findfirst(==(key), s.insertion_order)
        idx !== nothing && deleteat!(s.insertion_order, idx)
    end
    return
end

function sync_insertion_order!(s::MetricStorage{M, K, T}) where {M, K, T}
    if length(s.insertion_order) == length(s.data)
        all_present = true
        for k in s.insertion_order
            if !haskey(s.data, k)
                all_present = false
                break
            end
        end
        if all_present
            return s
        end
    end
    seen = Set{K}()
    new_order = Vector{K}()
    sizehint!(new_order, length(s.data))
    for k in s.insertion_order
        if haskey(s.data, k) && !(k in seen)
            push!(new_order, k)
            push!(seen, k)
        end
    end
    for k in keys(s.data)
        if !(k in seen)
            push!(new_order, k)
            push!(seen, k)
        end
    end
    empty!(s.insertion_order)
    append!(s.insertion_order, new_order)
    return s
end

key_type(::MetricStorage{M, K, T}) where {M, K, T} = K
value_type(::MetricStorage{M, K, T}) where {M, K, T} = T

Base.length(s::MetricStorage) = length(s.data)
Base.isempty(s::MetricStorage) = isempty(s.data)
Base.keys(s::MetricStorage) = keys(s.data)
Base.values(s::MetricStorage) = values(s.data)
Base.iterate(s::MetricStorage, args...) = iterate(s.data, args...)
Base.haskey(s::MetricStorage, key) = haskey(s.data, key)
Base.getindex(s::MetricStorage{M, K, T}, key) where {M, K, T} = s.data[key]::T
Base.get(s::MetricStorage{M, K, T}, key, default) where {M, K, T} =
    get(s.data, key, default)::Union{T, typeof(default)}

function copy_storage(s::MetricStorage{M, K, T}) where {M, K, T}
    return MetricStorage{M, K, T}(s.metric, copy(s.data), copy(s.insertion_order))
end

abstract type AbstractContextStorage end

struct ContextStorage{K} <: AbstractContextStorage
    storages::IdDict{AbstractMetric, AbstractMetricStorage}
    ContextStorage(::Type{K}=Any) where K = new{K}(IdDict{AbstractMetric, AbstractMetricStorage}())
end

key_type(::ContextStorage{K}) where K = K

Base.isempty(c::AbstractContextStorage) = isempty(c.storages)
Base.length(c::AbstractContextStorage) = length(c.storages)
Base.keys(c::AbstractContextStorage) = keys(c.storages)
Base.values(c::AbstractContextStorage) = values(c.storages)
Base.iterate(c::AbstractContextStorage, args...) = iterate(c.storages, args...)
Base.haskey(c::AbstractContextStorage, m::AbstractMetric) = haskey(c.storages, m)
Base.getindex(c::AbstractContextStorage, m::AbstractMetric) = c.storages[m]

function copy_context(c::ContextStorage{K}) where K
    new_storage = ContextStorage(K)
    for (m, s) in c.storages
        new_storage.storages[m] = copy_storage(s)
    end
    return new_storage
end

function get_or_create_storage!(c::ContextStorage{K}, m::M) where {M<:AbstractMetric, K}
    storage = get(c.storages, m, nothing)
    if storage === nothing
        new_storage = MetricStorage(m, K)
        c.storages[m] = new_storage
        return new_storage
    end
    return storage::MetricStorage{M, K, metric_type(M)}
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
