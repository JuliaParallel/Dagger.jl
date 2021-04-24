using Profile
import Base.gc_num
export summarize_events

const Timestamp = UInt64

struct ProfilerResult
    samples::Vector{UInt64}
    lineinfo::AbstractDict
end

"""
identifies

space (category, id)
time (timeline, start, finish)

also tracks gc_num during this and profiling samples.
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

Event(phase::Symbol, category::Symbol,
      id, tl, time, gc_num, prof) =
    Event{phase}(category, id, tl, time, gc_num, prof)

"""
create a timespan given the strt and finish events
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


"""
Various means of writing an event to something.
"""
struct NoOpLog end

function write_event(::NoOpLog, event::Event)
end

struct FilterLog
    f::Function
    inner_chan::Any
end

function write_event(c::FilterLog, event)
    if c.f(event)
        write_event(c.inner_chan, event)
    end
end

function write_event(io::IO, event::Event)
    serialize(io, event)
end

function write_event(chan::Union{RemoteChannel, Channel}, event::Event)
    put!(chan, event)
end

function write_event(arr::AbstractArray, event::Event)
    push!(arr, event)
end

#function write_event(sig::Signal, event::Event)
#    push!(sig, event)
#end
"""
represents a process local events array.

A context with log_sink set to LocalEventLog() will
cause events to be recorded into the local event log.
"""
struct LocalEventLog end

const _local_event_log = Any[]
const _local_event_log_lock = ReentrantLock()
function write_event(::LocalEventLog, event::Event)
    lock(_local_event_log_lock) do
        write_event(Dagger._local_event_log, event)
    end
end

function raise_event(ctx, phase, category, id,tl, t, gc_num, prof, async)
    ev = Event(phase, category, id, tl, t, gc_num, prof)
    if async
        @async write_event(ctx, ev)
    else
        write_event(ctx, ev)
    end
end

empty_prof() = ProfilerResult(UInt[], Profile.getdict(UInt[]))

function timespan_start(ctx, category, id, tl, async=isasync(ctx.log_sink))
    isa(ctx.log_sink, NoOpLog) && return # don't go till raise
    if ctx.profile && category == :compute
        Profile.start_timer()
    end
    raise_event(ctx, :start, category, id, tl, time_ns(), gc_num(), empty_prof(), async)
    nothing
end

function timespan_end(ctx, category, id, tl, async=isasync(ctx.log_sink))
    isa(ctx.log_sink, NoOpLog) && return
    prof = UInt[]
    if ctx.profile && category == :compute
        Profile.stop_timer()
        prof = Profile.fetch()
        Profile.clear()
    end
    raise_event(ctx, :finish, category, id, tl,time_ns(), gc_num(), ProfilerResult(prof, Profile.getdict(prof)), async)
    nothing
end

isasync(x) = false
isasync(x::Union{Channel, RemoteChannel, IO}) = true
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
                   merge(a.lineinfo, b.lineinfo))
end

function build_timespans(events)
    next_state(State(), events)
end

macro logmsg(ex)
    #:(println($(esc(ex))))
end


"""
Get the logs from each process, clear it too
"""
function get_logs!(::LocalEventLog, raw=false)
    logs = Dict()
    @sync for p in workers()
        @async logs[p] = remotecall_fetch(p) do
            log = lock(_local_event_log_lock) do
                log = copy(Dagger._local_event_log)
                empty!(_local_event_log)
                log
            end
            log
        end
    end
    if raw
        return logs
    else
        spans = build_timespans(vcat(values(logs)...)).completed
        return convert(Vector{Timespan}, spans)
    end
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
