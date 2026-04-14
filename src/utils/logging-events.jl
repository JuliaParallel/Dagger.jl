struct LoggedMutableObject
    objid::UInt
    kind::Symbol
end

module Events

import ..Dagger
import ..Dagger: Context, Chunk

import ..TimespanLogging
import .TimespanLogging: Event, init_similar

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
        proc = ev.id.processor
        old = get(ps.saturation, proc, 0)
        ps.saturation[proc] = old + 1
    end
    # FIXME: JSON doesn't support complex arguments as object keys, so use a vector of tuples instead
    #filter(x->x[2]>0, ps.saturation)
    return map(x->(x[1], x[2]), filter(x->x[2]>0, collect(ps.saturation)))
end
function (ps::ProcessorSaturation)(ev::Event{:finish})
    if ev.category == :compute
        proc = ev.id.processor
        old = get(ps.saturation, proc, 0)
        ps.saturation[proc] = old - 1
        if old == 1
            delete!(ps.saturation, proc)
        end
    end
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

"""
    TaskNames

Creates a unique name for each task.
"""
struct TaskNames end
function (::TaskNames)(ev::Event{:start})
    if ev.category == :add_thunk
        id = ev.id.thunk_id
        f = Dagger.chunktype(ev.timeline.f)
        if hasproperty(f, :instance) && isdefined(f, :instance)
            f = f.instance
        end
        return "$(func_name(f)) [$id]"
    end
    return
end
(td::TaskNames)(ev::Event{:finish}) = nothing
func_name(f::Function) = nameof(f)
func_name(x) = repr(x)
func_name(::Dagger.ExpandedBroadcast{F}) where F = Symbol('.', nameof(F))

"""
    TaskFunctionNames

Records the function name of each task.
"""
struct TaskFunctionNames end
function (::TaskFunctionNames)(ev::Event{:start})
    if ev.category == :add_thunk
        f = Dagger.chunktype(ev.timeline.f)
        if hasproperty(f, :instance) && isdefined(f, :instance)
            f = f.instance
        end
        return String(func_name(f))
    end
    return
end
(td::TaskFunctionNames)(ev::Event{:finish}) = nothing

"""
    TaskArguments

Records the raw (mutable) arguments of each submitted task.
"""
struct TaskArguments end
(::TaskArguments)(ev::Event{:start}) = nothing
function (ta::TaskArguments)(ev::Event{:finish})
    if ev.category == :move
        args = Pair{Union{Symbol,Int},Dagger.LoggedMutableObject}[]
        thunk_id = ev.id.thunk_id::Int
        pos = Dagger.raw_position(ev.id.position::Dagger.ArgPosition)::Union{Symbol,Int}
        arg = ev.timeline.data
        if ismutable(arg)
            push!(args, pos => Dagger.objectid_or_chunkid(arg))
        end
        return thunk_id => args
    end
    return
end

"""
    TaskArgumentMoves

Records any `move`-derived copies of arguments of each task.
"""
struct TaskArgumentMoves
    pre_move_args::Dict{Int,Dict{Union{Int,Symbol},Dagger.LoggedMutableObject}}
end
TaskArgumentMoves() =
    TaskArgumentMoves(Dict{Int,Dict{Union{Int,Symbol},Dagger.LoggedMutableObject}}())
init_similar(::TaskArgumentMoves) = TaskArgumentMoves()
function (ta::TaskArgumentMoves)(ev::Event{:start})
    if ev.category == :move
        data = ev.timeline.data
        thunk_id = ev.id.thunk_id::Int
        if ismutable(data) && thunk_id != 0 # Ignore Datadeps moves, because we don't have TIDs for them
            position = Dagger.raw_position(ev.id.position::Dagger.ArgPosition)::Union{Symbol,Int}
            d = get!(Dict{Union{Int,Symbol},Dagger.LoggedMutableObject}, ta.pre_move_args, thunk_id)
            d[position] = Dagger.objectid_or_chunkid(data)
        end
    end
    return
end
function (ta::TaskArgumentMoves)(ev::Event{:finish})
    if ev.category == :move
        post_data = ev.timeline.data
        if ismutable(post_data)
            thunk_id = ev.id.thunk_id::Int
            position = Dagger.raw_position(ev.id.position::Dagger.ArgPosition)::Union{Symbol,Int}
            if haskey(ta.pre_move_args, thunk_id)
                d = ta.pre_move_args[thunk_id]
                if haskey(d, position)
                    pre_data = d[position]
                    return thunk_id, position, pre_data, Dagger.objectid_or_chunkid(post_data)
                else
                    @warn "No TID $(thunk_id), Position $(position)"
                end
            elseif thunk_id != 0
                @warn "No TID $(thunk_id)"
            end
        end
    end
    return
end

"""
    TaskResult

Records the raw (mutable) return value of each submitted task.
"""
struct TaskResult end
(::TaskResult)(ev::Event{:start}) = nothing
function (ta::TaskResult)(ev::Event{:finish})
    if ev.category == :compute
        thunk_id = ev.id.thunk_id::Int
        result = ev.timeline.result
        if ismutable(result)
            return thunk_id => Dagger.objectid_or_chunkid(result)
        end
    end
    return
end

"""
    TaskDependencies

Records the dependencies of each submitted task.
"""
struct TaskDependencies end
(td::TaskDependencies)(ev::Event{:start}) = nothing
function (::TaskDependencies)(ev::Event{:finish})
    local deps_tids::Vector{Int}
    function get_deps!(deps)
        for dep in deps
            @assert dep isa Dagger.ThunkSyncdep && dep.thunk isa Dagger.WeakThunk
            dep = Dagger.unwrap_weak_checked(dep)
            @assert dep isa Dagger.Thunk
            push!(deps_tids, dep.id)
        end
    end
    if ev.category == :add_thunk
        deps_tids = Int[]
        get_deps!(@something(ev.timeline.options.syncdeps, Set{Dagger.ThunkSyncdep}()))
        return ev.id.thunk_id => deps_tids
    end
    return
end

"""
    TaskUIDtoTID

Maps DTask UIDs to Thunk TIDs.
"""
struct TaskUIDtoTID end
function (tut::TaskUIDtoTID)(ev::Event{:start})
    if ev.category == :add_thunk
        thunk_id = ev.id.thunk_id::Int
        uid = ev.timeline.uid::UInt
        return uid => thunk_id
    end
    return
end
(tut::TaskUIDtoTID)(ev::Event{:finish}) = nothing

struct TaskToChunk end
(td::TaskToChunk)(ev::Event{:start}) = nothing
function (::TaskToChunk)(ev::Event{:finish})
    if ev.category == :finish
        thunk_id = ev.id.thunk_id::Int
        result = ev.timeline.result
        if ismutable(result)
            chunk_id = Dagger.objectid_or_chunkid(result)
            return thunk_id => chunk_id
        end
    end
    return
end

"""
    GCStats

Tracks GC allocations (`Base.GC_Diff.allocd`) between the start and finish of
each event, using the `gc_num` field already captured in each `Event`.
"""
mutable struct GCStats
    active_starts::Dict{Any, Base.GC_Num}
end
GCStats() = GCStats(Dict{Any, Base.GC_Num}())
init_similar(::GCStats) = GCStats()

function (gs::GCStats)(ev::Event{:start})
    gs.active_starts[(ev.category, ev.id)] = ev.gc_num
    nothing
end
function (gs::GCStats)(ev::Event{:finish})
    start_gc = pop!(gs.active_starts, (ev.category, ev.id), nothing)
    start_gc === nothing && return nothing
    return Base.GC_Diff(ev.gc_num, start_gc).allocd
end

"""
    LockContentionMetrics

Tracks the number of lock contention events between the start and finish of
each event. Uses a global atomic refcount to enable/disable
`Base.Threads.lock_profiling` only while at least one event is in-flight.
"""
const _lock_contention_refcount = Threads.Atomic{Int}(0)

mutable struct LockContentionMetrics
    active_starts::Dict{Any, Int64}
end
LockContentionMetrics() = LockContentionMetrics(Dict{Any, Int64}())
init_similar(::LockContentionMetrics) = LockContentionMetrics()

function (lc::LockContentionMetrics)(ev::Event{:start})
    old = Threads.atomic_add!(_lock_contention_refcount, 1)
    if old == 0
        Base.Threads.lock_profiling(true)
    end
    lc.active_starts[(ev.category, ev.id)] = Int64(Base.Threads.LOCK_CONFLICT_COUNT[])
    nothing
end
function (lc::LockContentionMetrics)(ev::Event{:finish})
    baseline = pop!(lc.active_starts, (ev.category, ev.id), nothing)
    baseline === nothing && return nothing
    current = Int64(Base.Threads.LOCK_CONFLICT_COUNT[])
    old = Threads.atomic_sub!(_lock_contention_refcount, 1)
    if old == 1
        Base.Threads.lock_profiling(false)
    end
    return current - baseline
end

"""
    CompileTimeMetrics

Tracks cumulative Julia compile time (via `Base.cumulative_compile_time_ns`)
between the start and finish of each event. Uses a global atomic refcount to
enable/disable timing only while at least one event is in-flight.
"""
const _compile_timing_refcount = Threads.Atomic{Int}(0)

mutable struct CompileTimeMetrics
    active_starts::Dict{Any, Int64}
end
CompileTimeMetrics() = CompileTimeMetrics(Dict{Any, Int64}())
init_similar(::CompileTimeMetrics) = CompileTimeMetrics()

function (ct::CompileTimeMetrics)(ev::Event{:start})
    old = Threads.atomic_add!(_compile_timing_refcount, 1)
    if old == 0
        Base.cumulative_compile_timing(true)
    end
    ct.active_starts[(ev.category, ev.id)] = Int64(Base.cumulative_compile_time_ns()[1])
    nothing
end
function (ct::CompileTimeMetrics)(ev::Event{:finish})
    baseline = pop!(ct.active_starts, (ev.category, ev.id), nothing)
    baseline === nothing && return nothing
    current = Int64(Base.cumulative_compile_time_ns()[1])
    old = Threads.atomic_sub!(_compile_timing_refcount, 1)
    if old == 1
        Base.cumulative_compile_timing(false)
    end
    return current - baseline
end

end # module Events
