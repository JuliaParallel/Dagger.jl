abstract type AbstractAnalysis end

const AnalysisOrMetric = Union{AbstractAnalysis, AbstractMetric}

const RequiredMetrics = Dict{Tuple{Symbol,Symbol},Vector{AnalysisOrMetric}}
const NO_REQUIRED_METRICS = RequiredMetrics()
required_metrics(::AnalysisOrMetric, _, _) = NO_REQUIRED_METRICS

const MetricsCache = Dict{Tuple{Symbol,Symbol},Dict{AnalysisOrMetric,Any}}
const MetricsCacheLocked = LockedObject{MetricsCache}
function create_global_metrics_cache()
    metrics = MetricsCache()
    for op in (:run,)
        metrics[(:processor, op)] = Dict{AnalysisOrMetric,Dict{Processor,Any}}()
    end
    for op in (:execute, :schedule)
        metrics[(:signature, op)] = Dict{AnalysisOrMetric,Dict{Signature,Any}}()
    end
    for op in (:move,)
        metrics[(:chunk, op)] = Dict{AnalysisOrMetric,WeakKeyDict{Chunk,Any}}()
    end
    return LockedObject(metrics)
end
const EMPTY_METRICS = create_global_metrics_cache()
function global_metrics_cache()
    state = EAGER_STATE[]
    if state === nothing
        return EMPTY_METRICS
    end
    return state.metrics::MetricsCacheLocked
end

const LocalMetricsCache = Dict{AnalysisOrMetric,Any}
const LOCAL_METRICS_CACHE = TaskLocalValue{LocalMetricsCache}(()->LocalMetricsCache())
local_metrics_cache() = LOCAL_METRICS_CACHE[]
local_metrics_cache(context::Symbol, op::Symbol) =
    local_metrics_cache()[(context, op)]
function fetch_local_metrics_cache!()
    metrics = local_metrics_cache()
    metrics_copy = deepcopy(metrics)
    empty!(metrics)
    return metrics_copy
end
function set_metric_value!(@nospecialize(m::AbstractMetric),
                           context::Symbol, op::Symbol,
                           @nospecialize(value))
    local_metrics_cache()[m] = value
end

function merge_local_metrics!(context::Symbol, op::Symbol, key)
    global_metrics_locked = global_metrics_cache()
    local_metrics = fetch_local_metrics_cache!()
    @lock1 global_metrics_locked global_metrics begin
        for m in keys(local_metrics)
            global_metrics_selected = global_metrics[(context, op)]
            if context == :worker
                get!(Dict{Int,Any}, global_metrics_selected, m)[key] = local_metrics[m]
            elseif context == :processor
                get!(Dict{Processor,Any}, global_metrics_selected, m)[key] = local_metrics[m]
            elseif context == :signature
                get!(Dict{Signature,Any}, global_metrics_selected, m)[key] = local_metrics[m]
            elseif context == :chunk
                get!(WeakKeyDict{Chunk,Any}, global_metrics_selected, m)[key] = local_metrics[m]
            end
        end
    end
end
function merge_remote_metrics!(global_metrics_locked::MetricsCacheLocked, remote_metrics::MetricsCache)
    @lock1 global_metrics_locked global_metrics begin
        for region in keys(remote_metrics)
            context, op = region
            for m in keys(remote_metrics[region])
                global_metrics_selected = global_metrics[region]
                remote_metrics_selected = remote_metrics[region][m]
                for key in keys(remote_metrics[region][m])
                    if context == :worker
                        get!(Dict{Int,Any}, global_metrics_selected, m)[key] = remote_metrics_selected[key]
                    elseif context == :processor
                        get!(Dict{Processor,Any}, global_metrics_selected, m)[key] = remote_metrics_selected[key]
                    elseif context == :signature
                        get!(Dict{Signature,Any}, global_metrics_selected, m)[key] = remote_metrics_selected[key]
                    elseif context == :chunk
                        get!(WeakKeyDict{Chunk,Any}, global_metrics_selected, m)[key] = remote_metrics_selected[key]
                    end
                end
            end
        end
    end
end
function delete_metrics_for!(global_metrics_locked::MetricsCacheLocked, proc::OSProc)
    child_procs = Dagger.children(proc)
    @lock1 global_metrics_locked global_metrics begin
        for region in keys(global_metrics)
            context, op = region
            for m in keys(global_metrics[region])
                if context == :worker
                    @dagdebug nothing :metrics "-- DELETE for ($context, $op) $m [$(proc.pid)]"
                    delete!(global_metrics[region][m], proc.pid)
                elseif context == :processor
                    for child_proc in child_procs
                        @dagdebug nothing :metrics "-- DELETE for ($context, $op) $m [$child_proc]"
                        delete!(global_metrics[region][m], child_proc)
                    end
                end
            end
        end
    end
end

function fetch_metric(@nospecialize(m::AnalysisOrMetric), context::Symbol, op::Symbol, @nospecialize(args...))
    global_metrics_locked = global_metrics_cache()
    @lock1 global_metrics_locked global_metrics begin
        # Check if this is already cached
        cache = global_metrics[(context, op)]
        key = first(args)
        # FIXME: Proper invalidation support
        if m isa AbstractMetric
            if haskey(cache, m) && haskey(cache[m], key)
                value = cache[m][key]
                @dagdebug nothing :metrics "-- HIT for ($context, $op) $m [$key] = $value"
                return value
            else
                # The metric isn't available yet
                @dagdebug nothing :metrics "-- MISS for ($context, $op) $m [$key]"
                return nothing
            end
        elseif m isa AbstractAnalysis
            # Run the analysis
            @dagdebug nothing :metrics "Running ($context, $op) $m [$key]"
            value = run_analysis(m, Val{context}(), Val{op}(), args...)
            # TODO: Allocate the correct Dict type
            get!(Dict, cache, m)[key] = value
            @dagdebug nothing :metrics "Finished ($context, $op) $m [$key] = $value"
            return value
        end
    end
end
function fetch_metric_cached(@nospecialize(m::AnalysisOrMetric), context::Symbol, op::Symbol, @nospecialize(args...))
    global_metrics_locked = global_metrics_cache()
    @lock1 global_metrics_locked global_metrics begin
        cache = global_metrics[(context, op)]
        key = first(args)
        if haskey(cache, m) && haskey(cache[m], key)
            value = cache[m][key]
            @dagdebug nothing :metrics "-- HIT (stale) for ($context, $op) $m [$key] = $value"
            return value
        end
        # The metric isn't available yet
        @dagdebug nothing :metrics "-- MISS (stale) for ($context, $op) $m [$key]"
        return nothing
    end
end

#### Built-in Analyses ####

"Estimates network transfer rate based on combined transfer times and sizes."
struct TransferRateAnalysis <: AbstractAnalysis end
required_metrics(::TransferRateAnalysis, ::Val{:signature}, ::Val{:schedule}) =
    RequiredMetrics((:chunk, :move) => [ThreadTimeMetric()])
function run_analysis(::TransferRateAnalysis, ::Val{:signature}, ::Val{:schedule}, signature::Signature, inputs::Vector{Pair{Union{Symbol,Nothing},Any}}, proc::Processor)
    transfer_size = UInt64(0)
    transfer_time = UInt64(0)
    for (_,input) in inputs
        if input isa Chunk
            transfer_size += affinity(input)[2]::UInt64
            transfer_time += something(fetch_metric(ThreadTimeMetric(), :chunk, :move, input), UInt64(0))::UInt64
        else
            size = UInt64(MemPool.approx_size(input))
            transfer_size += size
            # FIXME: Transfer time based on global average
            transfer_time += size / 1_000_000
        end
    end
    if transfer_size > 0 && transfer_time > 0
        return round(UInt64, Float64(transfer_size) / (Float64(transfer_time) / 10^9))
    else
        return nothing
    end
end

"Estimates network transfer costs based on data size."
struct NetworkTransferAnalysis <: AbstractAnalysis end
required_metrics(::NetworkTransferAnalysis, ::Val{:signature}, ::Val{:schedule}) =
    RequiredMetrics((:signature, :schedule) => [TransferRateAnalysis()])
function run_analysis(::NetworkTransferAnalysis, ::Val{:signature}, ::Val{:schedule}, signature::Signature, inputs::Vector{Pair{Union{Symbol,Nothing},Any}}, proc::Processor)
    # N.B. `affinity(x)` really means "data size of `x`"
    # N.B. We treat same-worker transfers as having zero transfer cost
    # TODO: For non-Chunk, model cost from scheduler to worker
    # TODO: Measure and model processor move overhead
    tx_rate = fetch_metric(TransferRateAnalysis(), :signature, :schedule, signature, inputs, proc)
    if tx_rate === nothing || tx_rate == 0
        tx_rate = UInt64(1_000_000)
    end
    tx_rate::UInt64
    tx_costs = UInt64(0)
    for (_,input) in inputs
        if input isa Chunk
            if get_parent(processor(input)) != get_parent(proc)
                tx_costs += affinity(input)[2]::UInt64
            end
        else
            # FIXME: Fetch cost for this input
        end
    end
    return round(UInt64, impute_sum(tx_costs) / tx_rate)
end
