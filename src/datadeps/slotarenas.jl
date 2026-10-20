### Dense CPU slot arenas, owned by a DataDepsContext epoch ###

const DATADEPS_SLOT_ARENAS = Ref(true)
"Maximum arena capacity per context epoch, across every worker; excess slots use DRefs."
const DATADEPS_SLOT_ARENA_LIMIT = Ref(256 * 2^20)
const SLOT_ARENA_POISON = Ref(false) # Debug: poison before free, on the worker.
const SLOT_ARENA_ALIGN = 64
const SLOT_ARENA_MIN_BYTES = 64 * 2^10
const SLOT_ARENA_BYTES = 64 * 2^20

_arena_slot_type(::Type{<:Array{E}}, ::CPURAMMemorySpace) where {E} =
    isbitstype(E) && Base.datatype_alignment(E) <= SLOT_ARENA_ALIGN
_arena_slot_type(::Type, ::MemorySpace) = false
_arena_aligned(n::Int) = Base.Checked.checked_mul(cld(n, SLOT_ARENA_ALIGN), SLOT_ARENA_ALIGN)
function _arena_slot_bytes(::Type{<:Array{E}}, header) where {E}
    bytes = sizeof(E)
    for n in header
        bytes = Base.Checked.checked_mul(bytes, n)
    end
    return bytes
end

# Eligible cold hierarchical slots must also use DRefs: that lets the normal
# slot cache retain them and makes the *next* region a cache hit. Flat regions
# do not populate this eligibility set and can use the arena fast path.
function arena_slot_cacheable(data, from_space, to_space)
    region = SLOT_REUSE_REGION[]
    return region !== nothing && slot_reusable(region, data, from_space, to_space)
end

function _arena_slot!(pool::SlotPool, to_proc, space::CPURAMMemorySpace,
                      ::Type{T}, header::Dims{N}) where {T<:Array,N}
    nbytes = _arena_slot_bytes(T, header)
    arena = _arena_with_room!(pool, space, nbytes)
    arena === nothing && return nothing # Epoch budget exhausted: use a DRef.
    offset = arena.used
    arena.used += _arena_aligned(nbytes)
    ref = ArenaRef{eltype(T),N}(root_worker_id(space), arena.id, arena.base + offset, header)
    slot = Chunk{T,typeof(ref),typeof(to_proc),AnyScope,typeof(space)}(
        T, ArrayDomain([1:l for l in header]), ref, to_proc, AnyScope(), space)
    ainfo = ContiguousAliasing(MemorySpan{CPURAMMemorySpace}(
        RemotePtr{Cvoid}(ref.addr, space), nbytes))
    memoize_ainfo!(ainfo_memo_key(slot, identity), ainfo)
    return slot
end

function _arena_with_room!(pool::SlotPool, space::CPURAMMemorySpace, nbytes::Int)
    pool.epoch.freed && return nothing
    needed = _arena_aligned(nbytes)
    arenas = get!(Vector{SlotArena}, pool.arenas, space)
    prev = 0
    if !isempty(arenas)
        arena = last(arenas)
        arena.capacity - arena.used >= needed && return arena
        prev = arena.capacity
    end
    remaining = @lock pool.epoch.lock DATADEPS_SLOT_ARENA_LIMIT[] - pool.epoch.reserved_bytes
    needed <= remaining || return nothing
    want = max(needed, clamp(max(2prev, min(8needed, SLOT_ARENA_BYTES)),
                             SLOT_ARENA_MIN_BYTES, SLOT_ARENA_BYTES))
    want = min(want, remaining)
    before = length(arenas)
    reserve_slot_arenas!(pool, Dict(space => want))
    return length(arenas) == before ? nothing : last(arenas)
end

"Reserve at most the epoch's remaining budget, registering each successful RPC."
function reserve_slot_arenas!(pool::SlotPool, bytes::Dict{CPURAMMemorySpace,Int})
    epoch = pool.epoch
    @sync for (space, want) in bytes
        # Claim the budget before starting an RPC: parallel partitions share it.
        claimed = @lock epoch.lock begin
            if epoch.freed || want > DATADEPS_SLOT_ARENA_LIMIT[] - epoch.reserved_bytes
                false
            else
                epoch.reserved_bytes += want
                true
            end
        end
        claimed || continue
        Threads.@spawn begin
            wid = root_worker_id(space)
            id, base = try
                wid == myid() ? _reserve_slot_arena(want) :
                    remotecall_fetch(_reserve_slot_arena, wid, want)
            catch
                @lock epoch.lock epoch.reserved_bytes -= want
                rethrow()
            end
            # Register before anything else can fail, including pool insertion.
            @lock epoch.lock begin
                push!(epoch.arenas, (wid, id))
                push!(get!(Vector{SlotArena}, pool.arenas, space), SlotArena(space, id, base, want, 0))
            end
        end
    end
    return nothing
end

# Worker registry owns the original malloc pointer, aligned base and capacity.
const SLOT_ARENAS = Dict{UInt,Tuple{Ptr{Cvoid},UInt,Int}}()
const SLOT_ARENAS_LOCK = ReentrantLock()
const SLOT_ARENA_NEXT_ID = Threads.Atomic{UInt}(1)
function _reserve_slot_arena(nbytes::Int)
    nbytes >= 0 || throw(ArgumentError("negative slot arena size"))
    p = Libc.malloc(Base.Checked.checked_add(nbytes, SLOT_ARENA_ALIGN - 1))
    p == C_NULL && throw(OutOfMemoryError())
    base = (UInt(p) + UInt(SLOT_ARENA_ALIGN - 1)) & ~UInt(SLOT_ARENA_ALIGN - 1)
    id = Threads.atomic_add!(SLOT_ARENA_NEXT_ID, UInt(1))
    try
        @lock SLOT_ARENAS_LOCK SLOT_ARENAS[id] = (p, base, nbytes)
    catch
        Libc.free(p)
        rethrow()
    end
    return id, base
end
function _free_slot_arenas!(ids::Vector{UInt})
    for id in ids
        p, base, nbytes = @lock SLOT_ARENAS_LOCK pop!(SLOT_ARENAS, id)
        if SLOT_ARENA_POISON[]
            ccall(:memset, Ptr{Cvoid}, (Ptr{Cvoid}, Cint, Csize_t), Ptr{Cvoid}(base), 0xa5, nbytes)
        end
        Libc.free(p)
    end
    return nothing
end

root_worker_id(h::ArenaRef) = h.owner
datasize(h::ArenaRef{T}) where {T} = sizeof(T) * prod(h.dims)
function unwrap(h::ArenaRef{T,N}) where {T,N}
    h.owner == myid() || throw(ArgumentError("ArenaRef belongs to worker $(h.owner), not $(myid())"))
    @lock SLOT_ARENAS_LOCK begin
        arena = get(SLOT_ARENAS, h.arena, nothing)
        arena === nothing && throw(ArgumentError("slot arena $(h.arena) has already been drained"))
        _, base, bytes = arena
        h.addr >= base && h.addr - base <= UInt(bytes) &&
            UInt(datasize(h)) <= UInt(bytes) - (h.addr - base) ||
            throw(ArgumentError("slot lies outside its arena"))
    end
    # The context's planning gate and inflight tasks hold the lifetime. A slot
    # does not independently own these bytes, so unsafe_free!(slot) is a no-op.
    return unsafe_wrap(Array{T,N}, Ptr{T}(h.addr), h.dims; own=false)
end
fetch_handle(h::ArenaRef; uniform::Bool=false) =
    h.owner == myid() ? unwrap(h) : remotecall_fetch(unwrap, h.owner, h)
MemPool.poolget(h::ArenaRef) = fetch_handle(h)
unwrappable(::ArenaRef) = true
move(from_proc::Processor, to_proc::Processor, x::ArenaRef) = move(from_proc, to_proc, fetch_handle(x))
move(to_proc::Processor, d::ArenaRef) = move(OSProc(d.owner), to_proc, d)
freed_with_its_arena(slot) = slot isa Chunk && slot.handle isa ArenaRef

# A timeline may inspect a recorded slot's *aliasing* after the epoch ended.
# Identity aliasing needs only the descriptor, never a live Array or an RPC.
# Materializing the expired value itself still raises in unwrap above.
function aliasing(slot::Chunk{T,H}, ::typeof(identity)) where {T,H<:ArenaRef}
    ref = slot.handle
    space = memory_space(slot)
    return ContiguousAliasing(MemorySpan{CPURAMMemorySpace}(
        RemotePtr{Cvoid}(ref.addr, space), datasize(ref)))
end

"Enable a pool explicitly owned by ddctx; prefill only slots absent from the epoch."
function prefill_slot_pool!(ddctx::DataDepsContext, state::DataDepsState,
                            pairs::Vector{DTaskPair}, schedule::Dict{DTask,Processor})
    if !DATADEPS_SLOT_ARENAS[]
        state.slot_pool = nothing
        return
    end
    accel = current_acceleration()
    accel isa DistributedAcceleration || return
    length(procs()) > 1 || return
    # Timeline logs may materialize a recorded slot after its epoch ends.
    if !(Sch.eager_context().log_sink isa TimespanLogging.NoOpLog)
        state.slot_pool = nothing
        return
    end
    epoch = @lock ddctx.lock begin
        if ddctx.arenas === nothing
            ddctx.arenas = EpochSlotArenas()
        end
        ddctx.arenas::EpochSlotArenas
    end
    pool = state.slot_pool
    if pool === nothing || pool.epoch !== epoch
        pool = SlotPool(epoch)
        state.slot_pool = pool
    end
    isempty(schedule) && return
    bytes = Dict{CPURAMMemorySpace,Int}()
    seen = Set{Tuple{UInt,MemorySpace}}()
    for pair in pairs
        proc = get(schedule, pair.task, nothing)
        proc === nothing && continue
        _count_spec_slots!(bytes, seen, accel, state, pair.spec, only(memory_spaces(proc)))
    end
    isempty(bytes) || reserve_slot_arenas!(pool, bytes)
    return nothing
end
_count_spec_slots!(bytes, seen, accel, state, spec::DTaskSpec, space) =
    _foreach_arg(arg -> _count_arg_slot!(bytes, seen, accel, state, value(arg), space), spec.fargs)
function _count_arg_slot!(bytes, seen, accel, state, v, space)
    arg, _ = unwrap_inout(v)
    if arg isa DTask
        isready(arg) || return
        return _count_arg_slot!(bytes, seen, accel, state, fetch(arg; raw=true), space)
    end
    arg isa Chunk || return
    existing = get(state.remote_args, space, nothing)
    existing !== nothing && haskey(existing, arg) && return
    origin = memory_space(arg)
    arena_slot_cacheable(arg, origin, space) && return
    direct = direct_uninit_slot(accel, arg, first(processors(origin)), first(processors(space)), origin, space)
    direct === nothing && return
    T, header = direct
    _arena_slot_type(T, space) || return
    key = (_identity_hash(arg), space)
    key in seen && return
    push!(seen, key)
    bytes[space] = Base.Checked.checked_add(get(bytes, space, 0), _arena_aligned(_arena_slot_bytes(T, header)))
    return nothing
end

"Free after *all* inflight work, including failed siblings and write-back, has retired."
function free_slot_arenas!(ddctx::DataDepsContext)
    epoch = ddctx.arenas
    # A pristine state can still have a prefilled pool after a planning error.
    isdefined(ddctx, :state) && (ddctx.state.slot_pool = nothing)
    epoch === nothing && return
    empty_epoch = @lock epoch.lock begin
        epoch.freed = true
        isempty(epoch.arenas)
    end
    if empty_epoch
        ddctx.arenas = nothing
        return nothing
    end
    by_owner = Dict{Int,Vector{UInt}}()
    @lock epoch.lock begin
        for (wid, id) in epoch.arenas
            push!(get!(Vector{UInt}, by_owner, wid), id)
        end
    end
    @sync for (wid, ids) in by_owner
        Threads.@spawn try
            wid == myid() ? _free_slot_arenas!(ids) : remotecall_wait(_free_slot_arenas!, wid, ids)
        catch err
            @warn "Could not free Datadeps slot arenas on worker $wid" exception=(err, catch_backtrace())
        end
    end
    ddctx.arenas = nothing
    return nothing
end
