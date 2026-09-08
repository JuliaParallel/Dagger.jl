struct TimeMetric <: AbstractMetric end
metric_applies(::TimeMetric, _) = true
metric_type(::Type{TimeMetric}) = UInt64
start_metric(::TimeMetric) = time_ns()
stop_metric(::TimeMetric, last::UInt64) = time_ns() - last

struct TimeSpec
    tv_sec::UInt64
    tv_nsec::UInt64
end

maketime(ts::TimeSpec) = ts.tv_sec * UInt(1_000_000_000) + ts.tv_nsec

@static if Sys.islinux()
    const CLOCK_THREAD_CPUTIME_ID = Cint(3)
    @inline function _clock_gettime(cid)
        ts = Ref{TimeSpec}()
        ccall(:clock_gettime, Cint, (Cint, Ref{TimeSpec}), cid, ts)
        return ts[]
    end
    @inline cputhreadtime() = maketime(_clock_gettime(CLOCK_THREAD_CPUTIME_ID))
else
    @inline cputhreadtime() = time_ns()
end

struct ThreadTimeMetric <: AbstractMetric end
metric_applies(::ThreadTimeMetric, _) = true
metric_type(::Type{ThreadTimeMetric}) = UInt64
start_metric(::ThreadTimeMetric) = cputhreadtime()
stop_metric(::ThreadTimeMetric, last::UInt64) = cputhreadtime() - last

struct CompileTimeMetric <: AbstractMetric end
metric_applies(::CompileTimeMetric, _) = true
metric_type(::Type{CompileTimeMetric}) = Tuple{UInt64, UInt64}
function start_metric(::CompileTimeMetric)
    Base.cumulative_compile_timing(true)
    return Base.cumulative_compile_time_ns()
end
function stop_metric(::CompileTimeMetric, last::Tuple{UInt64, UInt64})
    Base.cumulative_compile_timing(false)
    now = Base.cumulative_compile_time_ns()
    return (now[1] - last[1], now[2] - last[2])
end

struct AllocMetric <: AbstractMetric end
metric_applies(::AllocMetric, _) = true
metric_type(::Type{AllocMetric}) = Base.GC_Diff
start_metric(::AllocMetric) = Base.gc_num()
stop_metric(::AllocMetric, last::Base.GC_Num) = Base.GC_Diff(Base.gc_num(), last)

struct ResultShapeMetric <: AbstractMetric end
metric_applies(::ResultShapeMetric, _) = true
metric_type(::Type{ResultShapeMetric}) = Union{Dims, Nothing}
is_result_metric(::ResultShapeMetric) = true
result_metric(::ResultShapeMetric, result) =
    result isa AbstractArray ? size(result) : nothing

struct LoadAverageMetric <: AbstractMetric end
metric_applies(::LoadAverageMetric, _) = true
metric_type(::Type{LoadAverageMetric}) = NTuple{3, Float64}
start_metric(::LoadAverageMetric) = nothing
stop_metric(::LoadAverageMetric, _) = (Sys.loadavg()...,) ./ Sys.CPU_THREADS

struct TransferTimeMetric <: AbstractMetric end
metric_applies(::TransferTimeMetric, _) = true
metric_type(::Type{TransferTimeMetric}) = UInt64

struct TransferSizeMetric <: AbstractMetric end
metric_applies(::TransferSizeMetric, _) = true
metric_type(::Type{TransferSizeMetric}) = UInt64

struct TransferRateMetric <: AbstractMetric end
metric_applies(::TransferRateMetric, _) = true
metric_type(::Type{TransferRateMetric}) = UInt64
