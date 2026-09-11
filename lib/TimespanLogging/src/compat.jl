struct FilterLog
    f::Function
    inner_chan::Any
end

function write_event(c::FilterLog, event)
    if c.f(event)
        write_event(c.inner_chan, event)
    end
end

get_logs!(f::FilterLog; kwargs...) = get_logs!(f.inner_chan; kwargs...)

function write_event(io::IO, event::Event)
    serialize(io, event)
end

function write_event(chan::Union{RemoteChannel, Channel}, event::Event)
    put!(chan, event)
end

function write_event(arr::AbstractArray, event::Event)
    push!(arr, event)
end

const event_log_lock = Threads.ReentrantLock()

"""
    LocalEventLog

Compatibility sink. Events are recorded into per-thread buffers; `get_logs!`
steals them and optionally pairs start/finish into `Timespan`s.
"""
struct LocalEventLog end

function write_event(::LocalEventLog, event::Event)
    # Direct write_event is rare; route through the TLS legacy list so we
    # do not reintroduce a process-wide lock on the common path.
    phase = event isa Event{:start} ? 0x00 : 0x01
    _emit_legacy(phase, event.category, event.id, event.timeline)
    return nothing
end

function get_logs!(::LocalEventLog; raw=false, only_local=false)
    wkrs = only_local ? Int[myid()] : procs()
    fetched = _map_workers(wkrs) do
        TimespanLogging.steal_all_old_events()
    end
    logs = Dict{Int,Vector{Event}}(p => v for (p, v) in fetched)
    if raw
        return logs
    else
        spans = build_timespans(vcat(values(logs)...)).completed
        return convert(Vector{Timespan}, spans)
    end
end
get_logs!(l::LocalEventLog, raw::Bool; kwargs...) = get_logs!(l; raw=raw, kwargs...)

mutable struct MultiEventLogState
    consumers::Dict{Symbol,Any}
    consumer_logs::Dict{Symbol,Vector}
    aggregators::Dict{Symbol,Any}
end
MultiEventLogState() = MultiEventLogState(Dict{Symbol,Any}(),
                                          Dict{Symbol,Vector}(),
                                          Dict{Symbol,Any}())

const MultiEventLogState_PLS = Dict{UInt64,MultiEventLogState}()

"""
    MultiEventLog

Compatibility sink. Recording is per-thread; consumers and aggregators run
once at `get_logs!` on the stolen batch (not on every emit).
"""
struct MultiEventLog
    uid::UInt64
    consumers::Dict{Symbol,Any}
    aggregators::Dict{Symbol,Any}
end
MultiEventLog() = MultiEventLog(rand(UInt64), Dict{Symbol,Any}(), Dict{Symbol,Any}())

function Base.setindex!(ml::MultiEventLog, c, name::Symbol)
    ml.consumers[name] = c
end

function get_state(ml::MultiEventLog)
    @lock event_log_lock begin
        mls = get!(() -> MultiEventLogState(), MultiEventLogState_PLS, ml.uid)
        for name in keys(ml.consumers)
            if !haskey(mls.consumers, name)
                mls.consumers[name] = init_similar(ml.consumers[name])
                mls.consumer_logs[name] = Any[]
            end
        end
        for name in keys(ml.aggregators)
            if !haskey(mls.aggregators, name)
                mls.aggregators[name] = init_similar(ml.aggregators[name])
            end
        end
        mls
    end
end

"Creates a copy of `x` with the same configuration, but fresh/empty data."
init_similar(x) = x

function write_event(::MultiEventLog, event::Event)
    phase = event isa Event{:start} ? 0x00 : 0x01
    _emit_legacy(phase, event.category, event.id, event.timeline)
    return nothing
end

function get_logs!(ml::MultiEventLog; only_local=false)
    wkrs = only_local ? Int[myid()] : procs()
    fetched = _map_workers(wkrs) do
        mls = get_state(ml)
        events = TimespanLogging.steal_all_old_events()
        TimespanLogging.consume_events(events, mls.consumers, mls.aggregators)
    end
    return Dict{Int,Dict{Symbol,Vector}}(p => v for (p, v) in fetched)
end

# Profile-enabled finish (rare). Still records a legacy event; attaches
# profiler samples via a second legacy event on the timeline if needed.
const prof_refcount = Ref{Threads.Atomic{Int}}(Threads.Atomic{Int}(0))
const prof_lock = Threads.ReentrantLock()
const prof_tasks = IdDict{Any, Vector{Task}}()

function prof_task_put!(id, task::Task=Base.current_task())
    @lock prof_lock push!(get!(()->Task[], prof_tasks, id), task)
end
function prof_tasks_take!(id)
    @lock prof_lock begin
        if haskey(prof_tasks, id)
            pop!(prof_tasks, id)
        else
            Task[]
        end
    end
end

function _timespan_finish_profile(sink, category, @nospecialize(id), @nospecialize(tl), tasks)
    time = time_ns()
    gcn = gc_num()
    prof = UInt[]
    lidict = Dict{UInt64, Vector{Base.StackTraces.StackFrame}}()
    tasks === nothing && (tasks = prof_tasks_take!(id))
    GC.@preserve tasks begin
        @lock prof_lock begin
            prof_done = Threads.atomic_sub!(prof_refcount[], 1) == 1
            if prof_done
                Profile.stop_timer()
            end
            prof = @static if VERSION >= v"1.8-"
                Profile.fetch(;include_meta=true)
            else
                Profile.fetch()
            end
            prof = tasks !== nothing ? filter_profile_data(prof, tasks) : prof
            lidict = Profile.getdict(prof)
            if prof_done
                Profile.clear()
            end
        end
        ev = Event(:finish, category, id, tl, time, gcn, ProfilerResult(prof, lidict, tasks))
        write_event(sink, ev)
    end
    return nothing
end

function timespan_start(ctx, category::Symbol, @nospecialize(id), @nospecialize(tl), ::Val{:profile})
    sink = log_sink(ctx)
    isa(sink, NoOpLog) && return
    if profile(ctx, category, id, tl) && Threads.atomic_add!(prof_refcount[], 1) == 0
        @lock prof_lock Profile.start_timer()
    end
    _emit_legacy(0x00, category, id, tl)
    return nothing
end

@static if VERSION >= v"1.8-"
    function filter_profile_data(prof, tasks::Vector{UInt})
        newprof = UInt[]
        startidx = 1
        for i in 1:length(prof)
            if prof[i] == 0
                if (i > 2 && prof[i-2] == 0) ||
                   (i > 3 && prof[i-3] == 0) ||
                   (i > 4 && prof[i-4] == 0)
                    continue
                end
                task = prof[i - 3]
                if task in tasks
                    append!(newprof, prof[startidx:i])
                end
                startidx = i+1
            end
        end
        newprof
    end
    filter_profile_data(prof, tasks::Vector{Task}) =
        filter_profile_data(prof, map(x->UInt(Base.pointer_from_objref(x)), tasks))
else
    filter_profile_data(prof, tasks) = prof
end

# Start/finish matching used by LocalEventLog and visualization helpers.

mutable struct State
    start_events::Dict
    finish_events::Dict
    completed::Vector
    start_time::Timestamp
    finish_time::Timestamp
end
State() = State(Dict(), Dict(), Any[], 0, 0)

function add_span(state, tl, category, span)
    push!(state.completed, span)
    if state.start_time == 0
        state.start_time = span.start
    else
        state.start_time = min(span.start, state.start_time)
    end
    if state.finish_time == 0
        state.finish_time = span.finish
    else
        state.finish_time = max(span.finish, state.finish_time)
    end
    state
end

function next_state(state::State, event::Event{:start})
    key = (event.category, event.id)
    if haskey(state.finish_events, key)
        span = make_timespan(event, pop!(state.finish_events, key))
        add_span(state, event.timeline, event.category, span)
    else
        state.start_events[key] = event
    end
    state
end

function next_state(state::State, event::Event{:finish})
    key = (event.category, event.id)
    if haskey(state.start_events, key)
        span = make_timespan(pop!(state.start_events, key), event)
        add_span(state, event.timeline, event.category, span)
    else
        state.finish_events[key] = event
    end
    state
end
next_state(state::State, events::AbstractArray) =
    foldl(next_state, events, init=state)

function mix_samples(a, b)
    ProfilerResult(vcat(a.samples, b.samples),
                   merge(a.lineinfo, b.lineinfo),
                   unique(vcat(a.tasks, b.tasks)))
end

function build_timespans(events)
    next_state(State(), events)
end

function add_gc_diff(x, y)
    Base.GC_Diff(
        x.allocd     + y.allocd,
        x.malloc     + y.malloc,
        x.realloc    + y.realloc,
        x.poolalloc  + y.poolalloc,
        x.bigalloc   + y.bigalloc,
        x.freecall   + y.freecall,
        x.total_time + y.total_time,
        x.pause      + y.pause,
        x.full_sweep + y.full_sweep
    )
end

function aggregate_events(xs)
    gc_diff = reduce(add_gc_diff, map(x -> x.gc_diff, xs))
    time_spent = sum(map(x -> x.finish - x.start, xs))
    profiler_samples = treereduce(mix_samples, map(x->x.profiler_samples, xs))
    time_spent, gc_diff, profiler_samples
end

function summarize_events(time_spent, gc_diff, profiler_samples)
    Base.time_print(time_spent, gc_diff.allocd, gc_diff.total_time, Base.gc_alloc_count(gc_diff))
    if !isempty(profiler_samples.samples)
        Profile.print(profiler_samples.samples, profiler_samples.lineinfo)
    end
end

summarize_events(xs) = summarize_events(aggregate_events(xs)...)

# `timespan_start` used to start the profile timer. Keep that when profile()
# is true by wrapping the exported method.
function _maybe_start_profile(ctx, category, id, tl)
    if profile(ctx, category, id, tl) && Threads.atomic_add!(prof_refcount[], 1) == 0
        @lock prof_lock Profile.start_timer()
    end
    return nothing
end
