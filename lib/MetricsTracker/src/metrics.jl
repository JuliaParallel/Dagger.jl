export @with_metrics, with_metrics

const METRIC_REGION = ScopedValue{Union{Tuple{Symbol,Symbol},Nothing}}(nothing)
metric_region() = METRIC_REGION[]
const METRIC_KEY = ScopedValue{Union{Some{Any},Nothing}}(nothing)
metric_key() = something(METRIC_KEY[])

metric_applies(::AbstractMetric, _) = false
is_result_metric(::AbstractMetric) = false

function with_metrics(f, ms::MetricsSpec, mod::Module, context::Symbol, key, sync_loc::SyncLocation)
    @assert !COLLECTING_METRICS[] "Nested metrics collection not yet supported"
    # TODO: Early filter out non-applicable metrics
    cross_metric_values = ntuple(length(ms.metrics)) do i
        m = ms.metrics[i]
        if metric_applies(m, Val{context}()) && !is_result_metric(m)
            return start_metric(m)
        else
            return nothing
        end
    end

    @debug "Starting metrics collection for ($mod, $context) [$key]"
    result = nothing
    try
        return @with COLLECTING_METRICS=>true f()
    finally
        @debug "Finished metrics collection for ($mod, $context) [$key]"

        final_metric_values = reverse(ntuple(length(ms.metrics)) do i
            m = ms.metrics[length(ms.metrics) - i + 1]
            if metric_applies(m, Val{context}())
                if is_result_metric(m) && result !== nothing
                    return result_metric(m, something(result))
                else
                    return stop_metric(m, cross_metric_values[length(ms.metrics) - i + 1])
                end
            else
                return nothing
            end
        end)
        set_metric_values!(ms, mod, context, key, sync_loc, final_metric_values)
    end
end
function with_metrics(f, ms::Tuple, mod::Module, context::Symbol, sync_loc::SyncLocation, key)
    with_metrics(f, MetricsSpec(ms...), mod, context, key)
end
macro with_metrics(ms, context, key, sync_loc, body)
    esc(quote
        $with_metrics(() -> $body, $ms, $__module__, $context, $key, $sync_loc)
    end)
end
macro with_metrics(ms, mod, context, key, sync_loc, body)
    esc(quote
        $with_metrics(() -> $body, $ms, $mod, $context, $key, $sync_loc)
    end)
end

function (wm::WithMetrics)(f, args...; kwargs...)
    return @with_metrics wm.spec wm.context wm.key wm.sync_loc f(args...; kwargs...)
end

Base.getindex(cache::MetricsCache, mod_context::Tuple{Module, Symbol}) =
    getindex(cache.results, mod_context)
Base.setindex!(cache::MetricsCache, value, mod_context::Tuple{Module, Symbol}) =
    setindex!(cache.results, value, mod_context)
Base.get(cache::MetricsCache, mod_context::Tuple{Module, Symbol}, default) =
    get(cache.results, mod_context, default)
Base.iterate(cache::MetricsCache) = iterate(cache.results)
Base.iterate(cache::MetricsCache, state) = iterate(cache.results, state)
Base.length(cache::MetricsCache) = length(cache.results)
function Base.show(io::IO, ::MIME"text/plain", cache::MetricsCache)
    println("MetricsCache:")
    for ((mod, context), metrics) in cache.results
        println(io, "  Metrics for ($mod, $context):")
        for (key, values) in metrics
            println(io, "    Key: $key")
            for (metric, value) in values
                println(io, "      $metric: $value")
            end
        end
    end
end

# TODO: Add recursive tracking?
const LOCAL_METRICS_CACHE = TaskLocalValue{MetricsCache}(()->MetricsCache())
local_metrics_cache() = LOCAL_METRICS_CACHE[]
local_metrics_cache(mod::Module, context::Symbol) =
    get!(local_metrics_cache(), (mod, context)) do
         Dict{Any, Dict{AbstractMetric, Any}}()
    end
local_metrics_cache(mod::Module, context::Symbol, key) =
    get!(local_metrics_cache(mod, context), key) do
        Dict{AbstractMetric, Any}()
    end

const GLOBAL_METRICS_CACHE = Base.Lockable(MetricsCache())
global_metrics_cache(f) = lock(f, GLOBAL_METRICS_CACHE)
global_metrics_cache(f, mod::Module, context::Symbol, key) = global_metrics_cache() do cache
    inner_cache = get!(get!(cache, (mod, context)) do
        Dict{Any, Dict{AbstractMetric, Any}}()
    end, key) do
        Dict{AbstractMetric, Any}()
    end
    return f(inner_cache)
end

function set_metric_values!(ms::MetricsSpec,
                            mod::Module, context::Symbol,
                            key,
                            ::SyncTask,
                            values::Tuple)
    cache = local_metrics_cache(mod, context, key)
    sync_results_into!(cache, ms, values)
    return
end
function set_metric_values!(ms::MetricsSpec,
                            mod::Module, context::Symbol,
                            key,
                            ::SyncGlobal,
                            values::Tuple)
    global_metrics_cache(mod, context, key) do cache
        sync_results_into!(cache, ms, values)
    end
    return
end
function set_metric_values!(ms::MetricsSpec,
                            mod::Module, context::Symbol,
                            key,
                            sync_loc::SyncInto,
                            values::Tuple)
    sync_results_into!(sync_loc.cache, ms, mod, context, key, values)
    return
end

function sync_results_into!(cache::MetricsCache,
                            ms::MetricsSpec,
                            mod::Module,
                            context::Symbol,
                            key,
                            values::Tuple)
    inner_cache = get!(cache.results, (mod, context)) do
        Dict{Any, Dict{AbstractMetric, Any}}()
    end
    keyed_cache = get!(inner_cache, key) do
        Dict{AbstractMetric, Any}()
    end
    sync_results_into!(keyed_cache, ms, values)
    return
end
function sync_results_into!(cache::Dict{AbstractMetric, Any},
                            ms::MetricsSpec,
                            values::Tuple)
    ntuple(length(ms.metrics)) do i
        m = ms.metrics[i]
        cache[m] = values[i]
        return
    end
    return
end
function sync_results_into!(dest_cache::MetricsCache, src_cache::MetricsCache)
    for ((mod, context), metrics) in src_cache.results
        dest_inner_cache = get!(dest_cache.results, (mod, context)) do
            Dict{Any, Dict{AbstractMetric, Any}}()
        end
        for (key, keyed_metrics) in metrics
            dest_keyed_metrics = get!(dest_inner_cache, key) do
                Dict{AbstractMetric, Any}()
            end
            for (metric, value) in keyed_metrics
                dest_keyed_metrics[metric] = value
            end
        end
    end
    return
end
