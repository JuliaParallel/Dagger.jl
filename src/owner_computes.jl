export TileSpace, TileIn, TileRW, tile_spawn!, tile_sync!, tile_data, tile_stats

# Owner-computes execution of task graphs over whole tiles.  Opt-in and additive; the Datadeps path is untouched.
#
# Every rank walks the same program (the loop nest of the algorithm) and calls `tile_spawn!` for EVERY task, but only does real work for the tasks it
# owns.  Per task each rank updates one tiny replicated table: per tile a version and a bitmask of the ranks that hold the current version.  From
# it every rank derives, without a single message, which transfers happen (sender = lowest holder, receiver = the task's rank, tag = per-pair
# sequence number) and whether it takes part.  Own tasks run as `@light` tasks, so there is no Thunk, Chunk, MemPool, scope or processor per task.
#
# A task is `tile_spawn!(space, f, TileIn(id), TileRW(id)...)`.  It runs on the rank of its first `TileRW` tile (or `at=`); `f` receives the tile
# arrays in argument order and modifies the `TileRW` ones in place.  Tiles are whole objects: no views or sub-ranges (use Datadeps for those), no
# tasks created inside tasks.  Tiles are created by many threads (first touch of the memory is spread over the machine) and received into pooled buffers.
#
# Transports (`TileTransport`) carry the arrays: `LocalTileTransport` (one process, nothing to send) is the default; MPIExt provides one for MPI.

abstract type TileTransport end
"One process, one rank: every tile is local and nothing is ever sent."
struct LocalTileTransport <: TileTransport end
"`tile_transport(accel)`: the transport for an acceleration (MPIExt adds the MPI one)."
tile_transport(accel) = LocalTileTransport()
tile_rank(::LocalTileTransport) = 0
tile_nranks(::LocalTileTransport) = 1
"`tile_post_send(tr, arr, dest, tag)` / `tile_post_recv(tr, arr, source, tag)`: start a non-blocking transfer, return a `Base.Event` that is notified when it is done."
function tile_post_send end
function tile_post_recv end
"`tile_barrier(tr)`: a barrier that does not block the OS thread (cooperative)."
tile_barrier(::LocalTileTransport) = nothing

struct TileIn; id::Int; end
struct TileRW; id::Int; end

mutable struct LocalTile                # the local copy of the CURRENT version of a tile
    arr::Any
    replica::Bool                       # a received copy: its buffer goes back to the pool once nobody reads it any more
    ready::Any                          # nothing | LightTask that produces/fills it
    readers::Vector{Any}                # LightTasks that read this version (a later writer must wait for them)
end

mutable struct TileSpace
    tr::TileTransport
    me::Int
    np::Int
    holders::Vector{UInt64}             # replicated: bit r set = rank r holds the current version
    version::Vector{Int32}              # replicated
    tiles::Vector{Union{Nothing,LocalTile}}
    own::Function                       # id -> rank that owns the data initially (and runs the tasks that write it first)
    tsize::Function                     # id -> dims (a tuple) of the tile: needed to allocate receive buffers
    types::Function                     # id -> element type of the tile (an isbits type, so that tiles are plain buffers)
    homes::Vector{UInt64}               # ranks whose ORIGINAL array must hold the final version (`tile_restore!`); bit(own) for an ordinary tile
    origs::Vector{Any}                  # the array each rank started with (nothing where it has none)
    pairseq::Vector{Int}                # np*np sequence numbers of transfers per (sender, dest)
    ntasks::Int; nown::Int; nxfer::Int; nsent::Int; nrecv::Int
    all_tasks::Vector{Any}
    pool::Dict{Tuple{DataType,Tuple},Vector{Any}}
    pool_lock::ReentrantLock
end

"""
    TileSpace(ntiles, own, tsize, init; T=Float64, types=nothing, initial=nothing, homes=nothing, transport=...)

A set of `ntiles` tiles identified by `1:ntiles`.  `own(id)` is the rank that initially holds tile `id`, `tsize(id)` its dimensions as a tuple (any rank),
`init(id)` creates it (called on the owner, from many threads: first touch of the memory should be spread over the machine).  The element type is `T`,
or `types(id)` per tile.  A tile can start on several ranks (`initial(id)`: a bitmask, for small arrays every rank has its own copy of) and
`homes(id)` says on which ranks the final version must end up in the array they started with (`tile_restore!`); both default to the owner.
Every rank must build the same space and make the same `tile_spawn!` calls in the same order.
"""
function TileSpace(ntiles::Int, own::Function, tsize::Function, init::Function; T::DataType=Float64, types=nothing, initial=nothing, homes=nothing,
                   transport::TileTransport=tile_transport(current_acceleration()))
    me = tile_rank(transport); np = tile_nranks(transport)
    np <= 64 || throw(ArgumentError("TileSpace supports at most 64 ranks (holder bitmask)"))
    tfun = types === nothing ? (id -> T) : types
    isbitstype(T) || throw(ArgumentError("TileSpace needs an isbits element type, got $T"))
    sp = TileSpace(transport, me, np, fill(UInt64(0), ntiles), zeros(Int32, ntiles), Vector{Union{Nothing,LocalTile}}(nothing, ntiles),
                   own, tsize, tfun, fill(UInt64(0), ntiles), Vector{Any}(nothing, ntiles), zeros(Int, np * np), 0, 0, 0, 0, 0, Any[],
                   Dict{Tuple{DataType,Tuple},Vector{Any}}(), ReentrantLock())
    mine = Int[]
    for id in 1:ntiles
        o = own(id); (0 <= o < np) || throw(ArgumentError("own($id) = $o is not a rank of 0:$(np-1)"))
        m0 = initial === nothing ? UInt64(1) << o : UInt64(initial(id))
        sp.holders[id] = m0
        sp.homes[id] = homes === nothing ? UInt64(1) << o : UInt64(homes(id))
        (m0 >> me) & 1 == 1 && push!(mine, id)
    end
    # first touch by many threads: on a NUMA machine the creating thread would otherwise put every tile in one domain
    tl = Vector{Any}(undef, length(mine))
    Threads.@threads :static for i in eachindex(mine)
        tl[i] = init(mine[i])
    end
    for (i, id) in enumerate(mine); sp.tiles[id] = LocalTile(tl[i], false, nothing, Any[]); sp.origs[id] = tl[i]; end
    return sp
end

function _pool_take(sp::TileSpace, ::Type{T}, dims::Tuple) where T
    lock(sp.pool_lock) do
        v = get(sp.pool, (T, dims), nothing)
        (v !== nothing && !isempty(v)) ? pop!(v) : Array{T}(undef, dims...)
    end
end
function _pool_give(sp::TileSpace, a::AbstractArray)
    lock(sp.pool_lock) do
        push!(get!(Vector{Any}, sp.pool, (eltype(a), size(a))), a)
    end
    return nothing
end

struct _TileRelease; sp::TileSpace; arr::Any; end
(r::_TileRelease)(deps...) = _pool_give(r.sp, r.arr)
struct _TileRunner; f::Any; arrs::Vector{Any}; end
(g::_TileRunner)(deps...) = (g.f(g.arrs...); nothing)
# A send goes out even when the task that produced the tile failed (`LightAfter`): the receiving rank is already waiting for it, and a rank that
# never gets its tile would hang the whole run instead of failing it. The error is agreed on in `tile_sync!`.
_tile_send(tr, arr, dest, tag, ready...) = (wait(tile_post_send(tr, arr, dest, tag)); nothing)
_tile_wait(ev) = (wait(ev); nothing)

"Thread pool for the communication tasks (sends, waits for receives, the MPI progress task): `:interactive` keeps them off the worker threads, whose kernels never yield."
const TILE_COMM_POOL = :interactive
const TAG_MOD = 4_000_000     # tags stay below Open MPI's tag_ub (8388607); matching is by (pair, tag) order, tags are unique per pair in flight

# Make rank `r` hold the current version of tile `id`: decided identically on every rank (sender = lowest holder, tag = per-pair sequence number),
# only the sender and the receiver do anything.
function _tile_ensure!(sp::TileSpace, id::Int, r::Int)
    rbit = UInt64(1) << r
    h = sp.holders[id]
    h & rbit != 0 && return
    me = sp.me
    sender = trailing_zeros(h)
    k = sender * sp.np + r + 1
    sp.pairseq[k] += 1; tag = sp.pairseq[k] % TAG_MOD
    sp.nxfer += 1
    if me == sender
        t = sp.tiles[id]::LocalTile
        st = light_spawn_on(TILE_COMM_POOL, _tile_send, sp.tr, t.arr, r, tag, t.ready === nothing ? nothing : LightAfter(t.ready))
        push!(t.readers, st); push!(sp.all_tasks, st); sp.nsent += 1
    elseif me == r
        arr = _pool_take(sp, sp.types(id), sp.tsize(id))
        ev = tile_post_recv(sp.tr, arr, sender, tag)          # posted now, in program order: rendezvous sends find their receive at once
        rt = light_spawn_on(TILE_COMM_POOL, _tile_wait, ev)
        sp.tiles[id] = LocalTile(arr, true, rt, Any[]); sp.nrecv += 1
    end
    sp.holders[id] = h | rbit
    return
end

"""
    tile_spawn!(space, f, args...; at=nothing)

Run `f(arrays...)` on the owner of the first `TileRW` argument (or on rank `at`).  Call it on every rank, identically.  Returns `nothing`.
"""
function tile_spawn!(sp::TileSpace, f, args::Union{TileIn,TileRW}...; at::Union{Nothing,Int}=nothing)
    isempty(args) && throw(ArgumentError("tile_spawn! needs at least one tile"))
    sp.ntasks += 1
    r = at
    if r === nothing
        for a in args
            a isa TileRW && (r = sp.own(a.id); break)
        end
        r === nothing && (r = sp.own(first(args).id))
    end
    # the task's rank for a tile that moved: a tile always lives where it was last written, not where it started
    me = sp.me; rbit = UInt64(1) << r
    for a in args; _tile_ensure!(sp, a.id, r); end
    if me == r
        sp.nown += 1
        deps = Any[]; arrs = Any[]
        for a in args
            t = sp.tiles[a.id]::LocalTile
            push!(arrs, t.arr); t.ready === nothing || push!(deps, t.ready)
            a isa TileRW && append!(deps, t.readers)             # write-after-read
        end
        task = light_spawn(_TileRunner(f, arrs), deps...)
        push!(sp.all_tasks, task)
        for a in args
            t = sp.tiles[a.id]::LocalTile
            if a isa TileRW
                t.ready = task; empty!(t.readers)
            else
                push!(t.readers, task)
            end
        end
    end
    for a in args
        if a isa TileRW
            sp.version[a.id] += 1; sp.holders[a.id] = rbit
            t = sp.tiles[a.id]
            if t !== nothing && t.replica
                if me == r
                    t.replica = false                            # the owner now writes into it: a live tile, not a pooled copy
                else
                    deps = Any[]; t.ready === nothing || push!(deps, t.ready); append!(deps, t.readers)
                    push!(sp.all_tasks, light_spawn(_TileRelease(sp, t.arr), deps...))
                    sp.tiles[a.id] = nothing
                end
            end
        end
    end
    return nothing
end

"""
    tile_sync!(space)

Wait for everything this rank started, then agree with the other ranks on whether any kernel failed anywhere: the barrier is reached on every rank
even when a kernel failed here, the first local error is rethrown, and a rank that saw no error of its own still throws when another rank did.
"""
function tile_sync!(sp::TileSpace)
    err, anyfail = tile_wait_all!(sp)
    err === nothing || throw(err)
    anyfail == 0 || error("Owner-computes: a task failed on another rank")
    return nothing
end

"""
    tile_wait_all!(space) -> (err, anyfail)

Like [`tile_sync!`](@ref) but returns instead of throwing: the first error of a task of this rank (or `nothing`) and the maximum failure flag over all
ranks (0: nobody failed). Every rank passes the barrier and the agreement either way. Used where something (the restore of the results) must still
happen after a failure.
"""
function tile_wait_all!(sp::TileSpace)
    err = nothing
    for t in sp.all_tasks
        try
            fetch(t)
        catch e
            err === nothing && (err = e)
        end
    end
    empty!(sp.all_tasks)
    tile_barrier(sp.tr)
    anyfail = tile_allreduce_max(sp.tr, err === nothing ? 0 : 1)
    return err, anyfail
end

"""
    owner_computes_agree_switch(name, on::Bool)

The decision to lower work onto this layer must be the same on every rank, or the ranks would enter different collectives and wait for each other for
ever. The switches are therefore set collectively: every rank that sets one exchanges the value, and all of them throw an `ArgumentError` (the switch
keeps its old value) when the values differ. In one process this does nothing. Like every collective it must be called by all ranks of the communicator.
"""
function owner_computes_agree_switch(name::Symbol, on::Bool)
    tr = tile_transport(current_acceleration())
    tile_nranks(tr) == 1 && return nothing
    vals = tile_allgather_ints(tr, [Int(on)])
    all(v -> v[1] == vals[1][1], vals) ||
        throw(ArgumentError("$name was not set to the same value on every rank ($(Bool[v[1] != 0 for v in vals])); it must be set identically everywhere"))
    return nothing
end

"`tile_scope_rank(scope) -> Union{Int,Nothing}`: the rank a scope pins a task to, or `nothing` when it is something the owner-computes path does not understand (MPIExt adds MPI scopes)."
tile_scope_rank(scope) = nothing

"""
    tile_allgather_ints(tr, v::Vector{Int}) -> Vector{Vector{Int}}

Every rank contributes `v` (same length everywhere) and gets all contributions, indexed by rank + 1. Built on the send/receive primitives, cooperative.
"""
function tile_allgather_ints(tr::TileTransport, v::Vector{Int})
    np = tile_nranks(tr); me = tile_rank(tr)
    np == 1 && return [v]
    tag = TAG_MOD + 2000
    out = [i == me ? v : zeros(Int, length(v)) for i in 0:np-1]
    evs = Base.Event[]
    for r in 0:np-1; r == me || push!(evs, tile_post_recv(tr, out[r+1], r, tag)); end
    for r in 0:np-1; r == me || push!(evs, tile_post_send(tr, v, r, tag)); end
    for ev in evs; wait(ev); end
    return out
end

const TILE_COLL_SEQ = Threads.Atomic{Int}(0)     # numbers the collectives below; every rank calls them in the same order

"""
    tile_bcast_value(tr, root, value) -> value

Collective: rank `root` sends `value` (any serializable object) to all ranks, which all return it. Built on the send/receive primitives. This is what
makes `fetch` of a task that ran on one rank give the same value everywhere.
"""
function tile_bcast_value(tr::TileTransport, root::Int, value)
    np = tile_nranks(tr)
    np == 1 && return value
    me = tile_rank(tr)
    tag = TAG_MOD + 3000 + 2 * (Threads.atomic_add!(TILE_COLL_SEQ, 1) % 1000)
    if me == root
        io = IOBuffer(); Serialization.serialize(io, value); bytes = take!(io)
        len = Int[length(bytes)]
        evs = Base.Event[]
        for r in 0:np-1
            r == root && continue
            push!(evs, tile_post_send(tr, len, r, tag)); push!(evs, tile_post_send(tr, bytes, r, tag + 1))
        end
        for ev in evs; wait(ev); end
        return value
    else
        len = zeros(Int, 1)
        wait(tile_post_recv(tr, len, root, tag))
        bytes = Vector{UInt8}(undef, len[1])
        wait(tile_post_recv(tr, bytes, root, tag + 1))
        return Serialization.deserialize(IOBuffer(bytes))
    end
end

"`tile_allreduce_max(tr, x::Int)`: the maximum of `x` over all ranks (cooperative; every rank must call it)."
tile_allreduce_max(::LocalTileTransport, x::Int) = x
"""
Generic allreduce on the tile send/receive primitives: everybody sends its value to rank 0, rank 0 sends the maximum back (tags above `TAG_MOD`,
so they never meet a tile transfer).
"""
function tile_allreduce_max(tr::TileTransport, x::Int)
    me = tile_rank(tr); np = tile_nranks(tr)
    np == 1 && return x
    tag = TAG_MOD + 1000
    buf = [Float64(x)]
    if me == 0
        m = x
        bufs = [zeros(1) for _ in 1:np-1]
        evs = [tile_post_recv(tr, bufs[r], r, tag) for r in 1:np-1]
        for ev in evs; wait(ev); end
        for b in bufs; m = max(m, Int(b[1])); end
        out = [Float64(m)]
        sevs = [tile_post_send(tr, out, r, tag + 1) for r in 1:np-1]
        for ev in sevs; wait(ev); end
        return m
    else
        sev = tile_post_send(tr, buf, 0, tag)
        res = zeros(1)
        rev = tile_post_recv(tr, res, 0, tag + 1)
        wait(sev); wait(rev)
        return Int(res[1])
    end
end

"The local array of a tile (only valid on a rank that holds the current version, after `tile_sync!`)."
tile_data(sp::TileSpace, id::Int) = (t = sp.tiles[id]; t === nothing ? nothing : t.arr)
tile_stats(sp::TileSpace) = (tasks=sp.ntasks, own=sp.nown, transfers=sp.nxfer, sent=sp.nsent, received=sp.nrecv)


# --- Chunks as tiles: which rank holds a `Chunk`, and its array on that rank ---------------------------------------------------------------
"`tile_chunk_owner(chunk) -> Int`: the rank of the transport that holds the chunk's array, or -1 when this layer cannot use it (MPIExt adds MPI chunks)."
tile_chunk_owner(c::Chunk) = c.handle isa MemPool.DRef && c.handle.owner == myid() && myid() == 1 ? 0 : -1
"`tile_chunk_array(chunk)`: the array itself (no copy) on the holding rank, `nothing` elsewhere."
tile_chunk_array(c::Chunk) = c.handle isa MemPool.DRef ? MemPool.poolget(c.handle) : nothing
"`tile_chunk_transport_ok(tr, chunk)`: whether the chunk belongs to the communicator of `tr`."
tile_chunk_transport_ok(tr::TileTransport, c::Chunk) = tr isa LocalTileTransport

"""
    tile_restore!(space; throw_errors=true) -> (err, anyfail)

Finish a computation: wait for everything, then make every tile's final version end up in the array each rank started with (`homes`): a tile that was
last written on another rank is sent back, a tile that lives in a received buffer is copied into the original array. A tile nobody wrote is untouched.
This is what makes the result visible through the `DArray` (or the small array) the tiles came from. Call it on every rank. The restore also runs when
a task failed (the transfers it waits for still happen), and then the agreed failure is thrown, or returned with `throw_errors=false`.
"""
function tile_restore!(sp::TileSpace; throw_errors::Bool=true)
    err, anyfail = tile_wait_all!(sp)                  # nothing may still read or write an original array
    n = length(sp.holders)
    for id in 1:n
        hm = sp.homes[id]
        hm == 0 && continue
        for r in 0:sp.np-1
            (hm >> r) & 1 == 1 && _tile_ensure!(sp, id, r)
        end
    end
    err2, anyfail2 = tile_wait_all!(sp)                # the transfers are done
    err === nothing && (err = err2)
    anyfail = max(anyfail, anyfail2)
    ids = [id for id in 1:n if (sp.homes[id] >> sp.me) & 1 == 1 && sp.origs[id] !== nothing && sp.tiles[id] !== nothing &&
                               (sp.tiles[id]::LocalTile).arr !== sp.origs[id]]
    Threads.@threads :static for i in eachindex(ids)
        id = ids[i]
        copyto!(sp.origs[id], (sp.tiles[id]::LocalTile).arr)
    end
    if throw_errors
        err === nothing || throw(err)
        anyfail == 0 || error("Owner-computes: a task failed on another rank")
    end
    return err, anyfail
end
