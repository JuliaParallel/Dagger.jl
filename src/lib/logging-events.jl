module Events

import ..Dagger
import ..Dagger: Context, Chunk

using ..TimespanLogging
import .TimespanLogging: Event, init_similar
import .TimespanLogging.Events: EventSaturation

TimespanLogging.log_sink(ctx::Context) = ctx.log_sink
TimespanLogging.profile(ctx::Context, category, id, tl) =
    ctx.profile && category == :compute

"""
    BytesAllocd

Tracks memory allocated for `Chunk`s.
"""
mutable struct BytesAllocd
    allocd::Int
end
BytesAllocd() = BytesAllocd(0)
init_similar(::BytesAllocd) = BytesAllocd()

function (ba::BytesAllocd)(ev::Event{:start})
    if ev.category in (:move, :evict) && ev.timeline.data isa Chunk
        sz = Int(ev.timeline.data.handle.size)
        if ev.category == :move && !haskey(Dagger.Sch.CHUNK_CACHE, ev.timeline.data)
            ba.allocd += sz
        elseif ev.category == :evict && haskey(Dagger.Sch.CHUNK_CACHE, ev.timeline.data)
            ba.allocd -= sz
        end
    end
    ba.allocd
end
(ba::BytesAllocd)(ev::Event{:finish}) = ba.allocd

"""
    ProcessorSaturation

Tracks the compute saturation (running tasks) per-processor.
"""
mutable struct ProcessorSaturation
    saturation::Dict{Dagger.Processor,Int}
end
ProcessorSaturation() = ProcessorSaturation(Dict{Dagger.Processor,Int}())
init_similar(::ProcessorSaturation) = ProcessorSaturation()

function (ps::ProcessorSaturation)(ev::Event{:start})
    if ev.category == :compute
        proc = ev.timeline.to_proc
        old = get(ps.saturation, proc, 0)
        ps.saturation[proc] = old + 1
    end
    @debug begin
        sat_string = "$(join(["$cat => $(ps.saturation[cat])" for cat in keys(ps.saturation) if ps.saturation[cat] > 0 ], ", "))"
        "Event: $(ev.category) start, procs: ($sat_string)"
    end _line=nothing _file=nothing _module=TimespanLogging
    # FIXME: JSON doesn't support complex arguments as object keys, so use a vector of tuples instead
    #filter(x->x[2]>0, ps.saturation)
    return map(x->(x[1], x[2]), filter(x->x[2]>0, collect(ps.saturation)))
end
function (ps::ProcessorSaturation)(ev::Event{:finish})
    if ev.category == :compute
        proc = ev.timeline.to_proc
        old = get(ps.saturation, proc, 0)
        ps.saturation[proc] = old - 1
        if old == 1
            delete!(ps.saturation, proc)
        end
    end
    @debug begin
        sat_string = "$(join(["$cat => $(ps.saturation[cat])" for cat in keys(ps.saturation) if ps.saturation[cat] > 0 ], ", "))"
        "Event: $(ev.category) finish, procs: ($sat_string)"
    end _line=nothing _file=nothing _module=TimespanLogging
    #filter(x->x[2]>0, ps.saturation)
    return map(x->(x[1], x[2]), filter(x->x[2]>0, collect(ps.saturation)))
end

"""
    WorkerSaturation

Tracks the compute saturation (running tasks).
"""
mutable struct WorkerSaturation
    saturation::Int
end
WorkerSaturation() = WorkerSaturation(0)
init_similar(::WorkerSaturation) = WorkerSaturation()

function (ws::WorkerSaturation)(ev::Event{:start})
    if ev.category == :compute
        ws.saturation += 1
    end
    ws.saturation
end
function (ws::WorkerSaturation)(ev::Event{:finish})
    if ev.category == :compute
        ws.saturation -= 1
    end
    ws.saturation
end

end # module Events
