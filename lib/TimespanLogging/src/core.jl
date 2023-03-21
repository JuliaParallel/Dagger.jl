using Distributed
import Profile
import Base.gc_num

export timespan_start, timespan_finish

const Timestamp = UInt64

struct ProfilerResult
    samples::Vector{UInt}
    lineinfo::AbstractDict
    tasks::Vector{UInt}
end
ProfilerResult(samples, lineinfo, tasks::Vector{Task}) =
    ProfilerResult(samples, lineinfo, map(Base.pointer_from_objref, tasks))
ProfilerResult(samples, lineinfo, tasks::Nothing) =
    ProfilerResult(samples, lineinfo, map(Base.pointer_from_objref, UInt[]))

"""
    Timespan

Identifies space (category, id) and time (timeline, start, finish). It also
tracks GC allocations and profiling samples.
"""
struct Timespan
    category::Symbol
    id::Any
    timeline::Any
    start::Timestamp
    finish::Timestamp
    gc_diff::Base.GC_Diff
    profiler_samples::ProfilerResult
end

struct Event{phase}
    category::Symbol
    id::Any
    timeline::Any
    timestamp::Timestamp
    gc_num::Base.GC_Num
    profiler_samples::ProfilerResult
end

@inline Event(phase::Symbol, category::Symbol,
              @nospecialize(id), @nospecialize(tl),
              time, gc_num, prof) =
    Event{phase}(category, id, tl, time, gc_num, prof)

"""
    make_timespan(start::Event, finish::Event) -> Timespan

Creates a `Timespan` given the start and finish `Event`s.
"""
function make_timespan(start::Event, finish::Event)
    @assert start.category == finish.category
    @assert start.id == finish.id

    Timespan(start.category,
             start.id,
             finish.timeline,
             start.timestamp,
             finish.timestamp,
             Base.GC_Diff(finish.gc_num,start.gc_num),
             mix_samples(start.profiler_samples, finish.profiler_samples))
end

get_logs!(ctx; kwargs...) = get_logs!(log_sink(ctx); kwargs...)

"""
    NoOpLog

Disables event logging entirely.
"""
struct NoOpLog end

function write_event(::NoOpLog, event::Event)
end

get_logs!(::NoOpLog) = nothing

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

Stores events in a process-local array. Accessing the logs is all-or-nothing;
if multiple consumers call `get_logs!`, they will get different sets of logs.
"""
struct LocalEventLog end

const _local_event_log = Any[]

function write_event(::LocalEventLog, event::Event)
    lock(event_log_lock) do
        write_event(_local_event_log, event)
    end
end

"""
    get_logs!(::LocalEventLog, raw=false; only_local=false) -> Union{Vector{Timespan},Vector{Event}}

Get the logs from each process' local event log, clearing it in the process.
Set `raw` to `true` to get potentially unmatched `Event`s; the default is to
return only matched events as `Timespan`s. If `only_local` is set `true`, only
process-local logs will be fetched; the default is to fetch logs from all
processes.
"""
function get_logs!(::LocalEventLog; raw=false, only_local=false)
    logs = Dict()
    wkrs = only_local ? myid() : procs()
    # FIXME: Log this logic
    @sync for p in wkrs
        @async logs[p] = remotecall_fetch(p) do
            lock(event_log_lock) do
                log = copy(_local_event_log)
                empty!(_local_event_log)
                log
            end
        end
    end
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

Processes events immediately, generating multiple log streams. Multiple
consumers may register themselves in the `MultiEventLog`, and when accessed,
log events will be provided to all consumers. A consumer is simply a function
or callable struct which will be called with an event when it's generated. The
return value of the consumer will be pushed into a log stream dedicated to that
consumer. Errors thrown by consumers will be caught and rendered, but will not
otherwise interrupt consumption by other consumers, or future consumption
cycles. An error will result in `nothing` being appended to that consumer's
log.
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
    lock(event_log_lock) do
        mls = get!(()->MultiEventLogState(), MultiEventLogState_PLS, ml.uid)
        max_length = reduce(max, map(length, values(mls.consumer_logs)); init=0)
        for name in keys(ml.consumers)
            if !haskey(mls.consumers, name)
                mls.consumers[name] = init_similar(ml.consumers[name])
                mls.consumer_logs[name] = Vector{Any}(fill(nothing, max_length))
            end
        end
        for name in keys(ml.aggregators)
            if !haskey(mls.aggregators, name)
                mls.aggregators[name] = init_similar(ml.aggregators[name])
            end
        end
        # FIXME: Remove deleted consumers and aggregators
        mls
    end
end

"Creates a copy of `x` with the same configuration, but fresh/empty data."
init_similar(x) = x

function write_event(ml::MultiEventLog, event::Event)
    mls = get_state(ml)
    lock(event_log_lock) do
        for name in keys(mls.consumers)
            cevent = try
                mls.consumers[name](event)
            catch err
                @error "Error during event consumption:" exception=(err,catch_backtrace())
                nothing
            end
            push!(mls.consumer_logs[name], cevent)
        end
        for name in keys(mls.aggregators)
            try
                mls.aggregators[name](mls.consumer_logs)
            catch err
                @error "Error during log aggregation:" exception=(err,catch_backtrace())
                nothing
            end
        end
    end
end

function get_logs!(ml::MultiEventLog; only_local=true)
    if only_local
        # Only return logs from current process
        mls = get_state(ml)
        return mls.consumer_logs
    else
        # Return logs from all processes
        logs = Dict{Int,Dict{Symbol,Vector}}()
        wkrs = procs()
        @sync for p in wkrs 
            @async begin
                logs[p] = remotecall_fetch(p, ml) do ml
                    mls = get_state(ml)
                    lock(event_log_lock) do
                        sublogs = Dict{Symbol,Vector}()
                        for name in keys(mls.consumers)
                            sublogs[name] = mls.consumer_logs[name]
                            mls.consumer_logs[name] = []
                        end
                        sublogs
                    end
                end
            end
        end
        return logs
    end
end


# Core logging operations

empty_prof() = ProfilerResult(UInt[], Dict{UInt64, Vector{Base.StackTraces.StackFrame}}(), UInt[])

const prof_refcount = Ref{Threads.Atomic{Int}}(Threads.Atomic{Int}(0))
const prof_lock = Threads.ReentrantLock()
const prof_tasks = IdDict{Any, Vector{Task}}()

function prof_task_put!(id, task::Task=Base.current_task())
    lock(prof_lock) do
        push!(get!(()->Task[], prof_tasks, id), task)
    end
end
function prof_tasks_take!(id)
    lock(prof_lock) do
        if haskey(prof_tasks, id)
            pop!(prof_tasks, id)
        else
            Task[]
        end
    end
end

log_sink(ctx) = NoOpLog()
profile(ctx, category, id, tl) = false

function timespan_start(ctx, category, @nospecialize(id), @nospecialize(tl))
    sink = log_sink(ctx)
    isa(sink, NoOpLog) && return
    do_profile = profile(ctx, category, id, tl)
    if do_profile && Threads.atomic_add!(prof_refcount[], 1) == 0
        lock(prof_lock) do
            Profile.start_timer()
        end
    end
    ev = Event(:start, category, id, tl, time_ns(), gc_num(), empty_prof())
    write_event(sink, ev)
    nothing
end

function timespan_finish(ctx, category, @nospecialize(id), @nospecialize(tl); tasks=prof_tasks_take!(id))
    sink = log_sink(ctx)
    isa(sink, NoOpLog) && return
    do_profile = profile(ctx, category, id, tl)
    time = time_ns()
    gcn = gc_num()
    prof = UInt[]
    lidict = Dict{UInt64, Vector{Base.StackTraces.StackFrame}}()
    GC.@preserve tasks begin
        if do_profile
            lock(prof_lock) do
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
        end
        ev = Event(:finish, category, id, tl, time, gcn, ProfilerResult(prof, lidict, tasks))
        write_event(sink, ev)
    end
    nothing
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
                    # XXX: Somehow we can get truncated frames?
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

"""
Overall state used during visualization
"""
mutable struct State
    start_events::Dict # (category, id) => Event
    finish_events::Dict  # (category, id) => Event
    #completed::Dict      # timeline => category => Array
    completed::Vector
    start_time::Timestamp
    finish_time::Timestamp
end
State() = State(Dict(), Dict(), Any[], 0, 0)

"""
Add a Timespan to a given State under `tl` (timeline)
and `category`.
"""
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

"""When building state for real-time visualization,
   use next_state to progress gantt state."""
function next_state(state::State, event::Event{:start})
    key = (event.category, event.id)
    if haskey(state.finish_events, key) # finish event reached before start
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

# util

function pushkey(dict, key, thing)
    if haskey(dict, key)
        push!(dict[key],thing)
    else
        dict[key] = Any[thing]
    end
end

function pushkey(dict, key1, args...)
    if haskey(dict, key1)
        pushkey(dict[key1], args...)
    else
        dict[key1] = Dict()
        pushkey(dict[key1], args...)
    end
end

function mix_samples(a,b)
    ProfilerResult(vcat(a.samples, b.samples),
                   merge(a.lineinfo, b.lineinfo),
                   unique(vcat(a.tasks, b.tasks)))
end

function build_timespans(events)
    next_state(State(), events)
end

function add_gc_diff(x,y)
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

