abstract type AbstractMetric end

function metric_region()
    tls = task_local_storage()
    if haskey(tls, :_dagger_local_metric_region)
        return tls[:_dagger_local_metric_region]::Tuple{Symbol,Symbol}
    end
    return nothing
end
set_metric_region!(context::Symbol, op::Symbol) =
    task_local_storage(:_dagger_local_metric_region, (context, op))
set_metric_region!(region::Tuple{Symbol,Symbol}) =
    set_metric_region!(region[1], region[2])

function metric_key()
    tls = task_local_storage()
    if haskey(tls, :_dagger_local_metric_key)
        return tls[:_dagger_local_metric_key]
    end
    return nothing
end
set_metric_key!(@nospecialize(key)) =
    task_local_storage(:_dagger_local_metric_key, key)

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
    inner_f = f
    for m in reverse(ms)
        if metric_applies(m, Val{context}(), Val{op}())
            inner_f = with_metric_callable(inner_f, m, context, op)
        end
    end

    old_region = metric_region()
    set_metric_region!(context, op)
    old_key = metric_key()
    set_metric_key!(key)
    @dagdebug nothing :metrics "Starting metrics collection for ($context, $op) [$key]"
    try
        return inner_f()
    finally
        @dagdebug nothing :metrics "Finished metrics collection for ($context, $op) [$key]"
        if old_region !== nothing || old_key !== nothing
            # Re-enable previous metric region and key
            @dagdebug nothing :metrics "Re-entering outer metrics region ($(old_region[1]), $(old_region[2])) [$old_key]"
            set_metric_region!(old_region)
            set_metric_key!(old_key)
        end
    end
end
function with_metric_callable(@nospecialize(f), m::AbstractMetric, context::Symbol, op::Symbol)
    return ()->with_metric(f, m, context, op)
end

#### Metric Contexts ####

function setup_metric_supplement!(supp::NamedTuple)
    task_local_storage(:_dagger_metric_supplement, supp)
end
clear_metric_supplement!() =
    delete!(task_local_storage(), :_dagger_metric_supplement)
metric_supplement() =
    task_local_storage(:_dagger_metric_supplement)::NamedTuple

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
