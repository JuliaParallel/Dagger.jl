abstract type AbstractDecision end

function required_metrics_to_collect(@nospecialize(m::AbstractDecision), context::Symbol, op::Symbol)
    metrics = Dict{Tuple{Symbol,Symbol},Vector{AnalysisOrMetric}}()
    to_expand = Tuple{Symbol,Symbol,AnalysisOrMetric}[]
    for ((dep_context, dep_op), metrics) in required_metrics_for_all_decisions(m)
        append!(to_expand, map(metric->(dep_context, dep_op, metric), metrics))
    end

    while !isempty(to_expand)
        local_context, local_op, metric = pop!(to_expand)
        metrics_vec = get!(()->AnalysisOrMetric[], metrics, (local_context, local_op))
        if !(metric in metrics_vec)
            push!(metrics_vec, metric)
        end
        for ((dep_context, dep_op), dep_metrics) in required_metrics(metric, Val{local_context}(), Val{local_op}())
            append!(to_expand, map(metric->(dep_context, dep_op, metric), dep_metrics))
        end
    end

    if !haskey(metrics, (context, op))
        return AbstractMetric[]
    end
    return filter(dep_m->dep_m isa AbstractMetric, metrics[(context, op)])
end

const DECISION_POINTS = [(:signature, :schedule)]
function required_metrics_for_all_decisions(@nospecialize(m::AbstractDecision))
    metrics = Dict{Tuple{Symbol,Symbol},Vector{AnalysisOrMetric}}()
    for (context, op) in DECISION_POINTS
        decision_metrics = required_metrics(m, Val{context}(), Val{op}())
        for ((dec_context, dec_op), dec_metrics) in decision_metrics
            append!(get!(Vector{AnalysisOrMetric}, metrics, (dec_context, dec_op)),
                    dec_metrics)
        end
    end
    return metrics
end

#### Contexts and Operations ####
# Every metric, analysis, and decision is associated with a combination of a
# "context" and an "operation", where the context is the kind of object being
# operated on, and that object is the key that will be used to later lookup
# that metric's value.

#### Available Contexts and Operations ####
# chunk - Scoped to a Chunk object, cached in core scheduler only
#   move - When move() is called on a Chunk
# signature - Scoped to any task with a matching function signature, cached in core scheduler only
#   execute - When execute!() is called on a matching task
#   schedule - When a matching task is being scheduled
# processor - Scoped to a Processor object, cached in core and worker schedulers
#   run - When do_task() is called to run a task
# worker - Scoped to a worker, cached in core and worker schedulers
#
# Currently, all worker-cached metric values are returned to the core when each task completes.

#### Built-in Scheduling Decisions ####

"""
    SchDefaultModel <: AbstractDecision

The scheduler's default decision model. Estimates the cost of scheduling `task`
on each processor in `procs`. Considers current estimated per-processor compute
pressure, and transfer costs for each `Chunk` argument to `task`. Returns
`(procs, costs)`, with `procs` sorted in order of ascending cost.
"""
struct SchDefaultModel <: AbstractDecision end

required_metrics(::SchDefaultModel, ::Val{:signature}, ::Val{:schedule}) =
    RequiredMetrics((:signature, :schedule) => [NetworkTransferAnalysis()],
                    (:processor, :run) => [ProcessorTimePressureMetric()],
                    (:signature, :execute) => [SimpleAverageAggregator(ThreadTimeMetric()),
                                               SimpleAverageAggregator(ResultSizeMetric())])

function make_decision(::SchDefaultModel, ::Val{:signature}, ::Val{:schedule}, signature::Signature, inputs::Vector{Pair{Union{Symbol,Nothing},Any}}, all_procs::Vector{<:Processor})
    # Estimate total cost for each processor
    costs = Dict{Processor,UInt64}()
    for proc in all_procs
        wait_cost = something(fetch_metric(ProcessorTimePressureMetric(), :processor, :run, proc)::Union{UInt64,Nothing}, UInt64(0))
        transfer_cost = something(fetch_metric(NetworkTransferAnalysis(), :signature, :schedule, signature, inputs, proc)::Union{UInt64,Nothing}, UInt64(0))
        costs[proc] = wait_cost + transfer_cost
    end

    # Shuffle procs around, so equally-costly procs are equally considered
    P = randperm(length(all_procs))
    sorted_procs = getindex.(Ref(all_procs), P)

    # Sort by lowest cost first
    sort!(sorted_procs, by=p->costs[p])

    # Move our corresponding ThreadProc to be the last considered
    if length(sorted_procs) > 1
        sch_threadproc = Dagger.ThreadProc(myid(), Threads.threadid())
        sch_thread_idx = findfirst(==(sch_threadproc), sorted_procs)
        if sch_thread_idx !== nothing
            deleteat!(sorted_procs, sch_thread_idx)
            push!(sorted_procs, sch_threadproc)
        end
    end

    return sorted_procs
end

"The scheduler's basic decision model."
struct SchBasicModel <: AbstractDecision end

mutable struct ProcessorCacheEntry
    gproc::OSProc
    proc::Processor
    next::ProcessorCacheEntry

    ProcessorCacheEntry(gproc::OSProc, proc::Processor) = new(gproc, proc)
end
Base.isequal(p1::ProcessorCacheEntry, p2::ProcessorCacheEntry) =
    p1.proc === p2.proc
function Base.show(io::IO, entry::ProcessorCacheEntry)
    entries = 1
    next = entry.next
    while next !== entry
        entries += 1
        next = next.next
    end
    print(io, "ProcessorCacheEntry(pid $(entry.gproc.pid), $(entry.proc), $entries entries)")
end

function make_decision(::SchBasicModel, ::Val{:signature}, ::Val{:schedule}, signature, inputs, all_procs)
    # Populate the cache if empty
    # FIXME: Implement cache through SchBasicModel?
    procs_cache_list::Base.RefValue{Union{ProcessorCacheEntry,Nothing}}()
    if state.procs_cache_list[] === nothing
        current = nothing
        for p in map(x->x.pid, procs)
            for proc in get_processors(OSProc(p))
                next = ProcessorCacheEntry(OSProc(p), proc)
                if current === nothing
                    current = next
                    current.next = current
                    state.procs_cache_list[] = current
                else
                    current.next = next
                    current = next
                    current.next = state.procs_cache_list[]
                end
            end
        end
    end

    # Fast fallback algorithm, useful when the smarter cost model algorithm
    # would be too expensive
    selected_entry = nothing
    entry = state.procs_cache_list[]
    cap, extra_util = nothing, nothing
    procs_found = false
    # N.B. if we only have one processor, we need to select it now
    can_use, scope = can_use_proc(task, entry.gproc, entry.proc, opts, scope)
    if can_use
        has_cap, est_time_util, est_alloc_util, est_occupancy =
            has_capacity(state, entry.proc, entry.gproc.pid, opts.time_util, opts.alloc_util, opts.occupancy, sig)
        if has_cap
            selected_entry = entry
        else
            procs_found = true
            entry = entry.next
        end
    else
        entry = entry.next
    end
    while selected_entry === nothing
        if entry === state.procs_cache_list[]
            # Exhausted all procs
            if procs_found
                push!(failed_scheduling, task)
            else
                state.cache[task] = SchedulingException("No processors available, try widening scope")
                state.errored[task] = true
                set_failed!(state, task)
            end
            return Processor[], Dict()
        end

        can_use, scope = can_use_proc(task, entry.gproc, entry.proc, opts, scope)
        if can_use
            has_cap, est_time_util, est_alloc_util, est_occupancy =
                has_capacity(state, entry.proc, entry.gproc.pid, opts.time_util, opts.alloc_util, opts.occupancy, sig)
            if has_cap
                # Select this processor
                selected_entry = entry
            else
                # We could have selected it otherwise
                procs_found = true
                entry = entry.next
            end
        else
            # Try next processor
            entry = entry.next
        end
    end
    @assert selected_entry !== nothing
    state.procs_cache_list[] = state.procs_cache_list[].next

    return Processor[proc], Dict(proc=>UInt64(0))
end
