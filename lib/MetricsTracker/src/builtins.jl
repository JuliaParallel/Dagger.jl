#### Built-in Metrics ####

struct TimeMetric <: AbstractMetric end
metric_applies(::TimeMetric, _) = true
metric_type(::TimeMetric) = UInt64
start_metric(::TimeMetric) = time_ns()
stop_metric(::TimeMetric, last::UInt64) = time_ns() - last

struct ThreadTimeMetric <: AbstractMetric end
metric_applies(::ThreadTimeMetric, _) = true
metric_type(::ThreadTimeMetric) = UInt64
start_metric(::ThreadTimeMetric) = cputhreadtime()
stop_metric(::ThreadTimeMetric, last::UInt64) = cputhreadtime() - last

struct CompileTimeMetric <: AbstractMetric end
metric_applies(::CompileTimeMetric, _) = true
metric_type(::CompileTimeMetric) = Tuple{UInt64, UInt64}
function start_metric(::CompileTimeMetric)
    Base.cumulative_compile_timing(true)
    return Base.cumulative_compile_time_ns()
end
function stop_metric(::CompileTimeMetric, last::Tuple{UInt64, UInt64})
    Base.cumulative_compile_timing(false)
    return Base.cumulative_compile_time_ns() .- last
end

struct AllocMetric <: AbstractMetric end
metric_applies(::AllocMetric, _) = true
metric_type(::AllocMetric) = Base.GC_Diff
start_metric(::AllocMetric) = Base.gc_num()
stop_metric(::AllocMetric, last::Base.GC_Num) = Base.GC_Diff(Base.gc_num(), last)

struct ResultShapeMetric <: AbstractMetric end
metric_applies(::ResultShapeMetric, _) = true
metric_type(::ResultShapeMetric) = Union{Dims, Nothing}
is_result_metric(::ResultShapeMetric) = true
result_metric(m::ResultShapeMetric, result) =
    result isa AbstractArray ? size(result) : nothing

struct LoadAverageMetric <: AbstractMetric end
metric_applies(::LoadAverageMetric, _) = true
metric_type(::LoadAverageMetric) = Tuple{Float64, Float64, Float64}
start_metric(::LoadAverageMetric) = nothing
stop_metric(::LoadAverageMetric, _) = (Sys.loadavg()...,) ./ Sys.CPU_THREADS

# TODO: Useful metrics to add
# perf performance counters
# BPF probe-collected metrics
