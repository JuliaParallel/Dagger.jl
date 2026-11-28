# A `spawn_datadeps` region executed on the owner-computes path (src/owner_computes.jl).  Off by default; `Dagger.owner_computes_regions!(true)` turns it on.
#
# The region is recorded as usual (every `@spawn` becomes a `DTaskPair` in `queue.seen_tasks`); before the Datadeps planner starts, `owner_computes_region!`
# asks whether the WHOLE region can run on the owner-computes path.  It can when every task has default options and only these arguments:
#   * a dense `Chunk` (an isbits `Array`, e.g. a tile of a DArray) as `In`/`Out`/`InOut` or bare, i.e. a whole-tile dependency,
#   * a plain `Array` of an isbits type (every rank has its own copy: a small result such as the `info` of a factorization) as a dependency,
#   * constants (isbits values, symbols, strings, types, functions).
# Anything else (views and sub-ranges, scopes and other options, results of tasks, a task without a tile) sends the region to the unchanged planner.
# The decision reads replicated metadata only, so every rank takes the same path.
#
# What changes for the user of a lowered region: nothing but speed. Its tasks have no Thunk (`DTask.stub` is a `TileStub`), so `fetch` is the collective it
# is under MPI anyway - the rank that ran the task broadcasts the value - and a task used as an argument of a later task is replaced by that value.
# A failing task raises `DTaskFailedException` from the region on every rank.

const OWNER_COMPUTES_REGIONS = Ref(false)
"How many regions ran on the owner-computes layer in this process (tests assert on it: the fallback gives the same numbers)."
const OWNER_COMPUTES_REGION_RUNS = Threads.Atomic{Int}(0)
"`owner_computes_regions!(on::Bool)`: let `spawn_datadeps` regions that qualify run on the owner-computes layer. Call it on every rank with the same value (under MPI it is a collective that throws on every rank when the values differ)."
function owner_computes_regions!(on::Bool)
    owner_computes_agree_switch(:owner_computes_regions!, on)      # collective under MPI: a mismatch throws on every rank instead of hanging later
    return OWNER_COMPUTES_REGIONS[] = on
end

const _REGION_OK_OPTIONS = (:name, :acceleration, :return_type)
# Which option fields must be unset for a task to qualify: all but a few, generated from `Options` itself so a new field is covered automatically.
let checks = Expr[]
    for f in fieldnames(Options)
        (f in _REGION_OK_OPTIONS || f in (:scope, :compute_scope, :exec_scope, :propagates)) && continue
        push!(checks, :(getfield(opts, $(QuoteNode(f))) === nothing || return $(QuoteNode(f))))
    end
    @eval function _region_unset_options(opts::Options)
        $(checks...)
        return nothing
    end
end
# Why a region was not lowered is visible with `JULIA_DEBUG=Dagger`.
_region_no(why) = (@debug "Region not lowered onto the owner-computes path: $why"; nothing)

struct RegionSlot
    k::Int                                  # which of the tile arrays handed to the kernel
end

struct RegionCall
    f::Any
    template::Vector{Any}                   # positional arguments: constants and RegionSlots
    kwargs::Vector{Pair{Symbol,Any}}
    task::DTask
    spec::Any                               # for the failure summary
    accel::Any                              # the acceleration the region was recorded under
end
function region_failure(c::RegionCall, e)
    s = ThunkSummary(Int(c.task.uid), Argument[Argument(ArgPosition(a.pos), a.value) for a in c.spec.fargs])
    return DTaskFailedException{typeof(e)}(s, s, e)
end
function (c::RegionCall)(arrs...)
    args = Any[a isa RegionSlot ? arrs[a.k] : a for a in c.template]
    # Library kernels ask for `task_processor()` & co.: give the light task the task-local state a Dagger task has (as streams do)
    set_tls!(ThreadProc(myid(), Threads.threadid()), typemax(UInt64), nothing, [], CancelToken(), false, c.accel)
    try
        v = isempty(c.kwargs) ? c.f(args...) : c.f(args...; c.kwargs...)
        put!(c.task.future, v)
        return nothing
    catch e
        isready(c.task.future) || put!(c.task.future, region_failure(c, e); error=true)
        rethrow()
    end
end

mutable struct RegionTile
    kind::Symbol                            # :chunk | :array
    obj::Any                                # the Chunk or the Array
    T::DataType
    dims::Union{Nothing,Tuple}              # nothing: not known on this rank (a chunk of another rank is a placeholder); filled in by the exchange
    owner::Int
end

_region_const(v) = isbitstype(typeof(v)) || v isa Union{Symbol,AbstractString,Type,Function,Nothing}

# Classify one argument: (:const, v) | (:tile, key, RegionTile, write::Bool) | nothing (unsupported)
function _region_classify(v, tr, np::Int, memo::IdDict{DTask,Any})
    v0, deps = unwrap_inout(v)
    wrapped = v isa Union{In,Out,InOut,Deps}
    if v0 isa DTask
        # N.B. The decision must not depend on timing (whether the task happens to be finished), or ranks could disagree and hang: a task that is
        # not launched yet was spawned inside this very region (program order, the same on every rank) and rules the region out; a launched one is
        # waited for (a DArray's chunks are created asynchronously).
        (istaskstarted(v0) && v0.stub === nothing) || return _region_no("an argument is a task of the region itself (a result used inside the region)")
        r = get(memo, v0, nothing)
        if r === nothing        # a DArray's chunk task is fetched once per region, not once per use
            r = try fetch(v0; raw=true, move_value=false, unwrap=false) catch; return _region_no("fetching an argument task failed") end
            memo[v0] = r
        end
        r isa Chunk || return _region_no("an argument task's result is a $(typeof(r)), not a Chunk")
        v0 = r
    end
    if v0 isa Chunk
        (length(deps) == 1 && deps[1][1] === identity) || return _region_no("a chunk argument is not a whole-tile dependency")
        ct = chunktype(v0)
        (ct <: Array && isbitstype(eltype(ct)) && isconcretetype(ct)) || return _region_no("a chunk holds a $ct, not a dense isbits Array")
        o = tile_chunk_owner(v0)
        (0 <= o < np) || return _region_no("a chunk's owner $o is not a rank of the transport")
        tile_chunk_transport_ok(tr, v0) || return _region_no("a chunk belongs to another communicator")
        # the chunk of another rank is a placeholder without a size: the owner tells everybody (see `_exchange_dims!`)
        dims = o == tile_rank(tr) ? (try Tuple(size(v0.domain)) catch; () end) : nothing
        return (:tile, _identity_hash(v0), RegionTile(:chunk, v0, eltype(ct), dims, o), deps[1][3])
    elseif typeof(v0) <: Array && isbitstype(eltype(v0))
        (length(deps) == 1 && deps[1][1] === identity) || return _region_no("an array argument is not a whole dependency")
        return (:tile, (:array, objectid(v0)), RegionTile(:array, v0, eltype(v0), size(v0), 0), deps[1][3])
    elseif !wrapped && _region_const(v0)
        return (:const, v0)
    end
    return _region_no("unsupported argument of type $(typeof(v0))")
end

# The wrapper of a dependency, cheaply: (value, writes). `Deps` and anything else unusual is left to the full classification (it rejects what it cannot do).
@inline function _region_split(v)
    v isa In && return (v.x, false)
    v isa Out && return (v.x, true)
    v isa InOut && return (v.x, true)
    return (v, false)
end

# Only in-place functions (`gemm!`, `potrf_checked!`, `copyto!`, ...) are lowered: what they do is the effect on their arguments, and their result is read, if at
# all, with `fetch`. Any other task may produce a block or a value that the rest of Dagger passes around as an ordinary `Chunk` with a location (the copies
# a DArray operation makes, `adjoint(chunk)`, ...), which a lowered task does not give: it would hand `TileRemoteResult` to the next task.
const REGION_INPLACE = Dict{DataType,Bool}()
const REGION_INPLACE_LOCK = ReentrantLock()
function _region_inplace(fn)::Bool
    fn isa Type && return false
    T = typeof(fn)
    return @lock REGION_INPLACE_LOCK get!(REGION_INPLACE, T) do
        try
            endswith(string(nameof(fn)), "!")
        catch
            false
        end
    end
end

function _region_plan(queue, tr, np::Int, accel)
    pairs = queue.seen_tasks
    memo = IdDict{DTask,Any}()
    isempty(pairs) && return nothing
    tiles = RegionTile[]; index = Dict{Any,Int}(); seen = IdDict{Any,Int}()
    calls = Any[]
    for pair in pairs
        spec = pair.spec; task = pair.task
        opts = getfield(spec, :options)
        bad = _region_unset_options(opts)
        bad === nothing || return _region_no("task option $bad is set")
        scope_rank = nothing                  # the rank a scope pins the task to (adopted as the task's rank)
        for f in (:scope, :compute_scope, :exec_scope)
            v = getfield(opts, f)
            v === nothing && continue
            r = tile_scope_rank(v)
            r === nothing && return _region_no("task option $f is set to a scope the owner-computes path does not understand")
            (scope_rank === nothing || scope_rank == r) || return _region_no("task scopes name different ranks")
            scope_rank = r
        end
        # `propagates` lists the names of scoped options in effect when the task was spawned; `task_queue` is the region's own queue
        pr = getfield(opts, :propagates)
        (pr === nothing || (pr isa Vector && all(==(:task_queue), pr))) || return _region_no("task option propagates is set ($pr)")
        fa = spec.fargs
        isempty(fa) && return _region_no("a task without arguments")
        a1 = first(fa)
        (ispositional(a1) && a1.pos.idx == 0) || return _region_no("unexpected function position")
        fn = value(a1)
        _region_inplace(fn) || return _region_no("task function $(fn) is not an in-place (!) function: its result may be used as a block or a value")
        template = Any[]; kwargs = Pair{Symbol,Any}[]; slots = Tuple{Int,Bool}[]
        last = 0                              # positions need only ascend (a typed spec starts at 2, an untyped one at 1)
        hasdep = false                        # a task declares a dependency (In/Out/InOut/Deps) on a tile, or it is not about data flow at all
        for a in fa
            (ispositional(a) && a.pos.idx != 0) || continue
            a.pos.idx > last || return _region_no("positional arguments out of order")
            last = a.pos.idx
            v = value(a)
            # Fast path: the same Chunk (or array) object is used by many tasks - look it up instead of classifying it again
            v0, wr = _region_split(v)
            c0 = v0 isa DTask ? get(memo, v0, nothing) : v0
            fid = (c0 isa Chunk || c0 isa Array) ? get(seen, c0, 0) : 0
            if fid != 0
                hasdep |= v0 !== v
                push!(slots, (fid, wr)); push!(template, RegionSlot(length(slots)))
                continue
            end
            c = _region_classify(v, tr, np, memo)
            c === nothing && return nothing
            if c[1] === :const
                push!(template, c[2])
            else
                _, key, tile, write = c
                hasdep |= v isa Union{In,Out,InOut,Deps}
                id = get(index, key, 0)
                if id == 0
                    push!(tiles, tile); id = length(tiles); index[key] = id
                elseif tile.kind === :chunk && (tiles[id].obj::Chunk).handle !== (tile.obj::Chunk).handle
                    return _region_no("two chunk objects for one datum")
                end
                seen[tile.obj] = id                    # (an array tile is keyed by its object, a chunk tile by the Chunk object we were handed)
                push!(slots, (id, write)); push!(template, RegionSlot(length(slots)))
            end
        end
        for a in fa
            ispositional(a) && continue
            c = _region_classify(value(a), tr, np, memo)
            (c === nothing || c[1] !== :const) && return _region_no("a keyword argument is not a constant")
            push!(kwargs, pos_kw(a) => c[2])
        end
        isempty(slots) && return _region_no("a task without a tile argument")
        # A task whose tile arguments are all bare produces a block or a value for others to use (the tasks that make the blocks of a DArray are of this
        # kind): its result must stay an ordinary `Chunk` with a location, which a lowered task does not give. Only data flow is lowered.
        hasdep || return _region_no("a task without a declared dependency (In/Out/InOut) on a tile")
        w = findfirst(s -> s[2], slots)
        rank = scope_rank !== nothing ? scope_rank : tiles[(w === nothing ? slots[1] : slots[w])[1]].owner
        (0 <= rank < np) || return _region_no("a scope names rank $rank, outside the transport")
        push!(calls, (; call=RegionCall(fn, template, kwargs, task, spec, accel), slots, rank))
    end
    return (; tiles, calls)
end

# Every rank needs the dimensions of every tile (to allocate receive buffers), but a chunk of another rank is only a placeholder. Each rank sends the
# dimensions of the tiles it owns to all others (at most 3 dimensions); a tile whose owner could not tell makes every rank fall back. Returns false then.
function _exchange_dims!(tr, tiles::Vector{RegionTile})
    tile_nranks(tr) == 1 && return all(t -> t.dims !== nothing && !isempty(t.dims), tiles)
    me = tile_rank(tr)
    v = zeros(Int, 4 * length(tiles))
    for (i, t) in enumerate(tiles)
        t.owner == me || continue
        d = t.dims
        (d !== nothing && 1 <= length(d) <= 3) || (v[4i-3] = -1; continue)
        v[4i-3] = length(d); for (j, x) in enumerate(d); v[4i-3+j] = x; end
    end
    all_v = tile_allgather_ints(tr, v)
    for (i, t) in enumerate(tiles)
        w = all_v[t.owner + 1]
        w[4i-3] < 1 && return false
        t.dims = Tuple(w[4i-3+j] for j in 1:w[4i-3])
    end
    return true
end

"""
    owner_computes_region!(queue) -> Bool

Run the recorded region on the owner-computes layer when it qualifies (see the file header) and return `true`; return `false` and touch nothing otherwise.
"""
function owner_computes_region!(queue)::Bool
    OWNER_COMPUTES_REGIONS[] || return false
    accel = current_acceleration()
    tr = tile_transport(accel)
    np = tile_nranks(tr)
    np <= 64 || return false
    plan = _region_plan(queue, tr, np, accel)
    plan === nothing && return false
    tiles = plan.tiles; me = tile_rank(tr)
    _exchange_dims!(tr, tiles) || return false
    # A plain array belongs to rank 0 of the region's communicator, as in `spawn_datadeps`: the others read it from there, only the owner gets the
    # result written back (they keep their own copy; a group's region must not change the arrays of the members that are not its owner).
    mask(id) = UInt64(1) << tiles[id].owner
    sp = TileSpace(length(tiles), id -> tiles[id].owner, id -> tiles[id].dims,
                   id -> (t = tiles[id]; t.kind === :chunk ? tile_chunk_array(t.obj) : t.obj);
                   types = id -> tiles[id].T, initial = mask, homes = mask, transport = tr)
    Threads.atomic_add!(OWNER_COMPUTES_REGION_RUNS, 1)
    for c in plan.calls
        c.call.task.stub = TileStub(c.rank)
        tile_spawn!(sp, c.call, [w ? TileRW(id) : TileIn(id) for (id, w) in c.slots]...; at=c.rank)
    end
    err, anyfail = tile_restore!(sp; throw_errors=false)
    # every future gets filled: the value (already done by the kernel on its rank), a marker where the task ran elsewhere, a failure where it did not run
    failure = err === nothing ? ErrorException("a task of the region failed on another rank") : err
    for c in plan.calls
        t = c.call.task
        isready(t.future) && continue
        if anyfail != 0 || err !== nothing
            fe = failure isa DTaskFailedException ? failure : region_failure(c.call, failure)
            put!(t.future, fe; error=true)
        else
            put!(t.future, TileRemoteResult(c.rank))
        end
    end
    # The tasks were never enqueued into the surrounding `wait_all`, so nobody else reports their failures: do what it does, in task order
    for c in plan.calls
        fetch(c.call.task; move_value=false, unwrap=false)
    end
    return true
end

"""
    owner_computes_agree_info(info)

A factorization that ran as a lowered region (`owner_computes_regions!(true)`) leaves the status array that a failing kernel wrote into on the rank that owns
it. The ranks agree on the largest value with one small collective. Without the switch (the default), and in one process, this returns `info` unchanged.
"""
function owner_computes_agree_info(info::Integer)
    OWNER_COMPUTES_REGIONS[] || return info
    tr = tile_transport(current_acceleration())
    tile_nranks(tr) == 1 && return info
    return convert(typeof(info), tile_allreduce_max(tr, Int(info)))
end
