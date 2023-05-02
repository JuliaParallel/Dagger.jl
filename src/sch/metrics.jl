abstract type AbstractMetric end

const METRIC_REGION = ScopedValue{Union{Tuple{Symbol,Symbol},Nothing}}(nothing)
metric_region() = METRIC_REGION[]
const METRIC_KEY = ScopedValue{Union{Some{Any},Nothing}}(nothing)
metric_key() = something(METRIC_KEY[])

metric_applies(@nospecialize(::AbstractMetric), _, _) = false

function with_metric(@nospecialize(f), m::AbstractMetric, context::Symbol, op::Symbol)
    init = try
        Some(start_metric(m))
    catch err
        @error "Error while starting metric $m" exception=(err,catch_backtrace())
        nothing
    end
    try
        f()
    finally
        if init !== nothing
            final = try
                Some(stop_metric(m, something(init)))
            catch err
                @error "Error while stopping metric $m" exception=(err,catch_backtrace())
                nothing
            end
            if final !== nothing
                set_metric_value!(m, context, op, something(final))
            end
        end
    end
end
function with_metrics(@nospecialize(f), ms::Vector, context::Symbol, op::Symbol, @nospecialize(key))
    if EAGER_STATE[] === nothing
        # Skip metrics collection during precompile
        return f()
    end

    inner_f = f
    for m in reverse(ms)
        if metric_applies(m, Val{context}(), Val{op}())
            inner_f = with_metric_callable(inner_f, m, context, op)
        end
    end

    @dagdebug nothing :metrics "Starting metrics collection for ($context, $op) [$key]"
    try
        @with METRIC_REGION=>(context, op) METRIC_KEY=>Some{Any}(key) begin
            return inner_f()
        end
    finally
        @dagdebug nothing :metrics "Finished metrics collection for ($context, $op) [$key]"

        # Merge local metrics into global scheduler metrics
        merge_local_metrics!(context, op, key)
    end
end
function with_metric_callable(@nospecialize(f), m::AbstractMetric, context::Symbol, op::Symbol)
    return ()->with_metric(f, m, context, op)
end

#### Metric Contexts ####

const METRIC_SUPPLEMENT = TaskLocalValue{NamedTuple}(()->NamedTuple())
function setup_metric_supplement!(supp::NamedTuple)
    METRIC_SUPPLEMENT[] = supp
end
function clear_metric_supplement!()
    METRIC_SUPPLEMENT[] = NamedTuple()
end
metric_supplement() = METRIC_SUPPLEMENT[]

#### Built-in Metrics ####

struct TimeMetric <: AbstractMetric end
metric_applies(::TimeMetric, _, _) = true
start_metric(::TimeMetric) = time_ns()
stop_metric(::TimeMetric, last::UInt64) = time_ns() - last

struct ThreadTimeMetric <: AbstractMetric end
metric_applies(::ThreadTimeMetric, _, _) = true
start_metric(::ThreadTimeMetric) = cputhreadtime()
stop_metric(::ThreadTimeMetric, last::UInt64) = cputhreadtime() - last

struct CompileTimeMetric <: AbstractMetric end
metric_applies(::CompileTimeMetric, _, _) = true
function start_metric(::CompileTimeMetric)
    Base.cumulative_compile_timing(true)
    return Base.cumulative_compile_time_ns()
end
function stop_metric(::CompileTimeMetric, last::Tuple{UInt64, UInt64})
    Base.cumulative_compile_timing(false)
    return Base.cumulative_compile_time_ns() .- last
end

struct AllocMetric <: AbstractMetric end
metric_applies(::AllocMetric, _, _) = true
start_metric(::AllocMetric) = Base.gc_num()
stop_metric(::AllocMetric, last::Base.GC_Num) = Base.GC_Diff(Base.gc_num(), last)

struct ProcessorTimePressureMetric <: AbstractMetric end
metric_applies(::ProcessorTimePressureMetric, ::Val{:processor}, ::Val{:run}) = true
start_metric(::ProcessorTimePressureMetric) = nothing
stop_metric(::ProcessorTimePressureMetric, _) = metric_supplement().time_pressure

struct ProcessorOccupancyMetric <: AbstractMetric end
metric_applies(::ProcessorOccupancyMetric, ::Val{:processor}, ::Val{:run}) = true
start_metric(::ProcessorOccupancyMetric) = nothing
stop_metric(::ProcessorOccupancyMetric, _) = metric_supplement().occupancy

struct ResultSizeMetric <: AbstractMetric end
metric_applies(::ResultSizeMetric, _, _) = true
function with_metric(@nospecialize(f), m::ResultSizeMetric, context::Symbol, op::Symbol)
    result = f()
    size = result isa Chunk ? UInt64(result.handle.size) : UInt64(MemPool.approx_size(result))
    set_metric_value!(m, context, op, size)
    return result
end

struct ResultShapeMetric <: AbstractMetric end
metric_applies(::ResultShapeMetric, _, _) = true
function with_metric(@nospecialize(f), m::ResultShapeMetric, context::Symbol, op::Symbol)
    result = f()
    size = result isa AbstractArray ? size(result) : nothing
    set_metric_value!(m, context, op, size)
    return result
end

struct LoadAverageMetric <: AbstractMetric end
metric_applies(::LoadAverageMetric, _, _) = true
start_metric(::LoadAverageMetric) = nothing
stop_metric(::LoadAverageMetric, _) = (Sys.loadavg()...,) ./ Sys.CPU_THREADS

# TODO: Useful metrics to add
# perf performance counters
# BPF probe-collected metrics
