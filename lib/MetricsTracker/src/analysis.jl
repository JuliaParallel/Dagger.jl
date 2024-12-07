const RequiredMetrics = Dict{Tuple{Module,Symbol},Vector{AnalysisOrMetric}}
const RequiredMetricsAny = Vector{AnalysisOrMetric}
const NO_REQUIRED_METRICS = RequiredMetrics()
required_metrics(::AnalysisOrMetric, _, _) = NO_REQUIRED_METRICS

function fetch_metric(m::AnalysisOrMetric, mod::Module, context::Symbol, key, extra; cached=false)
    @assert !COLLECTING_METRICS[] "Nesting analysis and metrics collection not yet supported"
    # Check if this is already cached
    cache = local_metrics_cache(mod, context, key)
    if cached
        return cache[m]
    end
    # FIXME: Proper invalidation support
    if m isa AbstractMetric
        if haskey(cache, m)
            value = cache[m]
            @debug "-- HIT for ($mod, $context) $m [$key] = $value"
            return value
        else
            # The metric isn't available yet
            @debug "-- MISS for ($mod, $context) $m [$key]"
            return nothing
        end
    elseif m isa AbstractAnalysis
        # Run the analysis
        @debug "Running ($mod, $context) $m [$key]"
        value = run_analysis(m, Val{nameof(mod)}(), Val{context}(), key, extra)
        # TODO: Allocate the correct Dict type
        get!(Dict, cache, m)[key] = value
        @debug "Finished ($mod, $context) $m [$key] = $value"
        return value
    end
end

#### Built-in Analyses ####

struct RuntimeWithoutCompilation <: AbstractAnalysis end
required_metrics(::RuntimeWithoutCompilation) =
    RequiredMetricsAny([TimeMetric(),
                        CompileTimeMetric()])
metric_type(::RuntimeWithoutCompilation) = UInt64
function run_analysis(::RuntimeWithoutCompilation, mod, context, key, extra)
    time = fetch_metric(TimeMetric(), mod, context, key, extra)
    ctime = fetch_metric(CompileTimeMetric(), mod, context, key, extra)
    return time - ctime[1]
end
