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
        return "$(nameof(f)) [$id]"
    end
    return
end
(td::TaskNames)(ev::Event{:finish}) = nothing

"""
    TaskArguments

Records the raw (mutable) arguments of each submitted task.
"""
struct TaskArguments end
(::TaskArguments)(ev::Event{:start}) = nothing
function (ta::TaskArguments)(ev::Event{:finish})
    if ev.category == :move
        args = Pair{Union{Symbol,Int},UInt}[]
        thunk_id = ev.id.thunk_id::Int
        pos = ev.id.position::Union{Symbol,Int}
        arg = ev.timeline.data
        if ismutable(arg)
            push!(args, pos => objectid(arg))
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
    pre_move_args::Dict{Int,Dict{Union{Int,Symbol},UInt}}
end
TaskArgumentMoves() =
    TaskArgumentMoves(Dict{Int,Dict{Union{Int,Symbol},UInt}}())
init_similar(::TaskArgumentMoves) = TaskArgumentMoves()
function (ta::TaskArgumentMoves)(ev::Event{:start})
    if ev.category == :move
        data = ev.timeline.data
        if ismutable(data)
            thunk_id = ev.id.thunk_id::Int
            position = ev.id.position::Union{Symbol,Int}
            d = get!(Dict{Union{Int,Symbol},UInt}, ta.pre_move_args, thunk_id)
            d[position] = objectid(data)
        end
    end
    return
end
function (ta::TaskArgumentMoves)(ev::Event{:finish})
    if ev.category == :move
        post_data = ev.timeline.data
        if ismutable(post_data)
            thunk_id = ev.id.thunk_id::Int
            position = ev.id.position::Union{Symbol,Int}
            if haskey(ta.pre_move_args, thunk_id)
                d = ta.pre_move_args[thunk_id]
                if haskey(d, position)
                    pre_data = d[position]
                    return thunk_id, position, pre_data, objectid(post_data)
                else
                    @warn "No TID $(thunk_id), Position $(position)"
                end
            else
                @warn "No TID $(thunk_id)"
            end
        end
    end
    return
end

"""
    TaskDependencies

Records the dependencies of each submitted task.
"""
struct TaskDependencies end
function (::TaskDependencies)(ev::Event{:start})
    local deps_tids::Vector{Int}
    function get_deps!(deps)
        for dep in deps
            dep = Dagger.unwrap_weak_checked(dep)
            if dep isa Dagger.Thunk || dep isa Dagger.Sch.ThunkID
                push!(deps_tids, dep.id)
            elseif dep isa Dagger.DTask && myid() == 1
                tid = lock(Dagger.Sch.EAGER_ID_MAP) do id_map
                    id_map[dep.uid]
                end
                push!(deps_tids, tid)
            else
                @warn "Unexpected dependency type: $dep"
            end
        end
    end
    if ev.category == :add_thunk
        deps_tids = Int[]
        get_deps!(Iterators.filter(Dagger.istask, Iterators.map(last, ev.timeline.args)))
        get_deps!(get(Set, ev.timeline.options, :syncdeps))
        return ev.id.thunk_id => deps_tids
    end
    return
end
(td::TaskDependencies)(ev::Event{:finish}) = nothing

end # module Events
