abstract type AbstractAggregator <: AbstractAnalysis end

#### Built-in Aggregators ####

struct SimpleAverageAggregator{T} <: AbstractAggregator
    inner::T
end
required_metrics(agg::SimpleAverageAggregator, ::Val{context}, ::Val{op}) where {context,op} =
    RequiredMetrics((context, op) => [agg.inner])
function run_analysis(agg::SimpleAverageAggregator, ::Val{context}, ::Val{op}, @nospecialize(args...)) where {context,op}
    prev = fetch_metric_cached(agg, context, op, args...)
    next = fetch_metric(agg.inner, context, op, args...)
    if prev === nothing || next === nothing
        return next
    end
    return (prev + next) ÷ 2
end
