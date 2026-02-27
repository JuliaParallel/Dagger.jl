using MPI

const CHECK_UNIFORMITY = Ref{Bool}(false)
function check_uniformity!(check::Bool=true)
    CHECK_UNIFORMITY[] = check
end
function check_uniform(value::Integer, original=value)
    CHECK_UNIFORMITY[] || return true
    comm = MPI.COMM_WORLD
    rank = MPI.Comm_rank(comm)
    matched = compare_all(value, comm)
    if !matched
        if rank == 0
            Core.print("[$rank] Found non-uniform value!\n")
        end
        Core.print("[$rank] value=$value, original=$original")
        throw(ArgumentError("Non-uniform value"))
    end
    MPI.Barrier(comm)
    return matched
end
function check_uniform(value, original=value)
    CHECK_UNIFORMITY[] || return true
    return check_uniform(hash(value), original)
end

function compare_all(value, comm)
    rank = MPI.Comm_rank(comm)
    size = MPI.Comm_size(comm)
    for i in 0:(size-1)
        if i != rank
            send_yield(value, comm, i, UInt32(0); check_seen=false)
        end
    end
    match = true
    for i in 0:(size-1)
        if i != rank
            other_value = recv_yield(comm, i, UInt32(0))
            if value != other_value
                match = false
            end
        end
    end
    return match
end

struct MPIAcceleration <: Acceleration
    comm::MPI.Comm
end
MPIAcceleration() = MPIAcceleration(MPI.COMM_WORLD)

const ALIASING_REQUEST_TAG = UInt32(0xFF00)

# Deterministic tag for data movement (remotecall_endpoint). Uses a separate tag space
# so it cannot collide with the global to_tag() counter used by execute! / aliasing.
# Prevents symmetric deadlock when one rank blocks in remotecall_endpoint recv while
# another rank consumes the same counter value in execute! recv.
const REMOTECALL_TAG_BASE = 100_000
const REMOTECALL_TAG_RANGE = 424_287  # so base+range-1 <= typical tag_ub
function remotecall_tag(comm::MPI.Comm, uid, from_rank::Int, to_rank::Int, ref_id)
    tag_ub = Int(MPI.tag_ub())
    range = min(REMOTECALL_TAG_RANGE, max(1, tag_ub - REMOTECALL_TAG_BASE + 1))
    h = hash((uid, from_rank, to_rank, ref_id))
    tag = REMOTECALL_TAG_BASE + Int(rem(h, UInt(range)))
    return UInt32(tag)
end

function aliasing(accel::MPIAcceleration, x::Chunk, T)
    handle = x.handle::MPIRef
    @assert accel.comm == handle.comm "MPIAcceleration comm mismatch"
    rank = MPI.Comm_rank(accel.comm)
    if handle.rank == rank
        ainfo = aliasing(x, T)
        check_uniform(ainfo)
        return ainfo
    end
    response_tag = to_tag()
    check_uniform(response_tag)
    request_payload = (handle, T, response_tag)
    _send_yield_raw(request_payload, accel.comm, handle.rank, Int(ALIASING_REQUEST_TAG))
    ainfo = recv_yield(accel.comm, handle.rank, response_tag)
    if ainfo isa Exception
        throw(ainfo)
    end
    check_uniform(ainfo)
    return ainfo
end
default_processor(accel::MPIAcceleration) = MPIOSProc(accel.comm, 0)
default_processor(accel::MPIAcceleration, x) = MPIOSProc(accel.comm, 0)
default_processor(accel::MPIAcceleration, x::Chunk) = MPIOSProc(x.handle.comm, x.handle.rank)
default_processor(accel::MPIAcceleration, x::Function) = MPIOSProc(accel.comm, MPI.Comm_rank(accel.comm))
default_processor(accel::MPIAcceleration, T::Type) = MPIOSProc(accel.comm, MPI.Comm_rank(accel.comm))

#TODO: Add a lock
const MPIClusterProcChildren = Dict{MPI.Comm, Set{Processor}}()

struct MPIClusterProc <: Processor
    comm::MPI.Comm
    function MPIClusterProc(comm::MPI.Comm)
        populate_children(comm)
        return new(comm)
    end
end

Sch.init_proc(state, proc::MPIClusterProc, log_sink) = Sch.init_proc(state, MPIOSProc(proc.comm), log_sink)

MPIClusterProc() = MPIClusterProc(MPI.COMM_WORLD)

function populate_children(comm::MPI.Comm)
    children = get_processors(OSProc())
    MPIClusterProcChildren[comm] = children
end

struct MPIOSProc <: Processor
    comm::MPI.Comm
    rank::Int
end

function MPIOSProc(comm::MPI.Comm)
    rank = MPI.Comm_rank(comm)
    return MPIOSProc(comm, rank)
end

function MPIOSProc()
    return MPIOSProc(MPI.COMM_WORLD)
end

ProcessScope(p::MPIOSProc) = ProcessScope(myid())

function check_uniform(proc::MPIOSProc, original=proc)
    return check_uniform(hash(MPIOSProc), original) &&
           check_uniform(proc.rank, original)
end

function memory_spaces(proc::MPIOSProc)
    children = get_processors(proc)
    spaces = Set{MemorySpace}()
    for proc in children
        for space in memory_spaces(proc)
            push!(spaces, space)
        end
    end
    return spaces
end

struct MPIProcessScope <: AbstractScope
    comm::MPI.Comm
    rank::Int
end

Base.isless(::MPIProcessScope, ::MPIProcessScope) = false
Base.isless(::MPIProcessScope, ::NodeScope) = true
Base.isless(::MPIProcessScope, ::UnionScope) = true
Base.isless(::MPIProcessScope, ::TaintScope) = true
Base.isless(::MPIProcessScope, ::AnyScope) = true
constrain(x::MPIProcessScope, y::MPIProcessScope) =
    x == y ? y : InvalidScope(x, y)
constrain(x::NodeScope, y::MPIProcessScope) =
    x == y.parent ? y : InvalidScope(x, y)

Base.isless(::ExactScope, ::MPIProcessScope) = true
constrain(x::MPIProcessScope, y::ExactScope) =
    x == y.parent ? y : InvalidScope(x, y)

function enclosing_scope(proc::MPIOSProc)
    return MPIProcessScope(proc.comm, proc.rank)
end

function Dagger.to_scope(::Val{:mpi_rank}, sc::NamedTuple)
    if sc.mpi_rank == Colon()
        return Dagger.to_scope(Val{:mpi_ranks}(), merge(sc, (;mpi_ranks=Colon())))
    else
        @assert sc.mpi_rank isa Integer "Expected a single GPU device ID for :mpi_rank, got $(sc.mpi_rank)\nConsider using :mpi_ranks instead."
        return Dagger.to_scope(Val{:mpi_ranks}(), merge(sc, (;mpi_ranks=[sc.mpi_rank])))
    end
end
Dagger.scope_key_precedence(::Val{:mpi_rank}) = 2
function Dagger.to_scope(::Val{:mpi_ranks}, sc::NamedTuple)
    comm = get(sc, :mpi_comm, MPI.COMM_WORLD)
    if sc.ranks != Colon()
        ranks = sc.ranks
    else
        ranks = MPI.Comm_size(comm)
    end
    inner_sc = NamedTuple(filter(kv->kv[1] != :mpi_ranks, Base.pairs(sc))...)
    # FIXME: What to do here?
    inner_scope = Dagger.to_scope(inner_sc)
    scopes = Dagger.ExactScope[]
    for rank in ranks
        procs = Dagger.get_processors(Dagger.MPIOSProc(comm, rank))
        rank_scope = MPIProcessScope(comm, rank)
        for proc in procs
            proc_scope = Dagger.ExactScope(proc)
            constrain(proc_scope, rank_scope) isa Dagger.InvalidScope && continue
            push!(scopes, proc_scope)
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:mpi_ranks}) = 2

struct MPIProcessor{P<:Processor} <: Processor
    innerProc::P
    comm::MPI.Comm
    rank::Int
end
proc_in_scope(proc::Processor, scope::MPIProcessScope) = false
proc_in_scope(proc::MPIProcessor, scope::MPIProcessScope) =
    proc.comm == scope.comm && proc.rank == scope.rank

function check_uniform(proc::MPIProcessor, original=proc)
    return check_uniform(hash(MPIProcessor), original) &&
           check_uniform(proc.rank, original) &&
           # TODO: Not always valid (if pointer is embedded, say for GPUs)
           check_uniform(hash(proc.innerProc), original)
end

Dagger.iscompatible_func(::MPIProcessor, opts, ::Any) = true
Dagger.iscompatible_arg(::MPIProcessor, opts, ::Any) = true

default_enabled(proc::MPIProcessor) = default_enabled(proc.innerProc)

root_worker_id(proc::MPIProcessor) = myid()
root_worker_id(proc::MPIOSProc) = myid()
root_worker_id(proc::MPIClusterProc) = myid()

get_parent(proc::MPIClusterProc) = proc
get_parent(proc::MPIOSProc) = MPIClusterProc(proc.comm)
get_parent(proc::MPIProcessor) = MPIOSProc(proc.comm, proc.rank)

short_name(proc::MPIProcessor) = "(MPI: $(proc.rank), $(short_name(proc.innerProc)))"

function get_processors(mosProc::MPIOSProc)
    populate_children(mosProc.comm)
    children = MPIClusterProcChildren[mosProc.comm]
    mpiProcs = Set{Processor}()
    for proc in children
        push!(mpiProcs, MPIProcessor(proc, mosProc.comm, mosProc.rank))
    end
    return mpiProcs
end

#TODO: non-uniform ranking through MPI groups
#TODO: use a lazy iterator
function get_processors(proc::MPIClusterProc)
    children = Set{Processor}()
    for i in 0:(MPI.Comm_size(proc.comm)-1)
        for innerProc in MPIClusterProcChildren[proc.comm]
            push!(children, MPIProcessor(innerProc, proc.comm, i))
        end
    end
    return children
end

struct MPIMemorySpace{S<:MemorySpace} <: MemorySpace
    innerSpace::S
    comm::MPI.Comm
    rank::Int
end

function check_uniform(space::MPIMemorySpace, original=space)
    return check_uniform(space.rank, original) &&
           # TODO: Not always valid (if pointer is embedded, say for GPUs)
           check_uniform(hash(space.innerSpace), original)
end

default_processor(space::MPIMemorySpace) = MPIOSProc(space.comm, space.rank)
default_memory_space(accel::MPIAcceleration) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, 0)

default_memory_space(accel::MPIAcceleration, x) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, 0)
default_memory_space(accel::MPIAcceleration, x::Array) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, MPI.Comm_rank(accel.comm))
default_memory_space(accel::MPIAcceleration, x::AliasedObjectCacheStore) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, MPI.Comm_rank(accel.comm))
default_memory_space(accel::MPIAcceleration, x::Chunk) = MPIMemorySpace(CPURAMMemorySpace(myid()), x.handle.comm, x.handle.rank)
default_memory_space(accel::MPIAcceleration, x::Function) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, MPI.Comm_rank(accel.comm))
default_memory_space(accel::MPIAcceleration, T::Type) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, MPI.Comm_rank(accel.comm))

function memory_spaces(proc::MPIClusterProc)
    rawMemSpace = Set{MemorySpace}()
    for rnk in 0:(MPI.Comm_size(proc.comm) - 1)
        for innerSpace in memory_spaces(OSProc())
            push!(rawMemSpace, MPIMemorySpace(innerSpace, proc.comm, rnk))
        end
    end
    return rawMemSpace
end

function memory_spaces(proc::MPIProcessor)
    rawMemSpace = Set{MemorySpace}()
    for innerSpace in memory_spaces(proc.innerProc)
        push!(rawMemSpace, MPIMemorySpace(innerSpace, proc.comm, proc.rank))
    end
    return rawMemSpace
end

root_worker_id(mem_space::MPIMemorySpace) = myid()

function processors(memSpace::MPIMemorySpace)
    rawProc = Set{Processor}()
    for innerProc in processors(memSpace.innerSpace)
        push!(rawProc, MPIProcessor(innerProc, memSpace.comm, memSpace.rank))
    end
    return rawProc
end

struct MPIRefID
    tid::Int
    uid::UInt
    id::Int
    function MPIRefID(tid, uid, id)
        @assert tid > 0 || uid > 0 "Invalid MPIRefID: tid=$tid, uid=$uid, id=$id"
        return new(tid, uid, id)
    end
end
Base.hash(id::MPIRefID, h::UInt=UInt(0)) =
    hash(id.tid, hash(id.uid, hash(id.id, hash(MPIRefID, h))))

function check_uniform(ref::MPIRefID, original=ref)
    return check_uniform(ref.tid, original) &&
           check_uniform(ref.uid, original) &&
           check_uniform(ref.id, original)
end

const MPIREF_TID = Dict{Int, Threads.Atomic{Int}}()
const MPIREF_UID = Dict{Int, Threads.Atomic{Int}}()

mutable struct MPIRef
    comm::MPI.Comm
    rank::Int
    size::Int
    innerRef::Union{DRef, Nothing}
    id::MPIRefID
end
Base.hash(ref::MPIRef, h::UInt=UInt(0)) = hash(ref.id, hash(MPIRef, h))
root_worker_id(ref::MPIRef) = myid()
@warn "Move this definition somewhere else" maxlog=1
root_worker_id(ref::DRef) = ref.owner

function check_uniform(ref::MPIRef, original=ref)
    return check_uniform(ref.rank, original) &&
           check_uniform(ref.id, original)
end

move(from_proc::Processor, to_proc::Processor, x::MPIRef) =
    move(from_proc, to_proc, poolget(x; uniform=FETCH_UNIFORM[]))

function affinity(x::MPIRef)
    if x.innerRef === nothing
        return MPIOSProc(x.comm, x.rank)=>0
    else
        return MPIOSProc(x.comm, x.rank)=>x.innerRef.size
    end
end

const MPIREF_ORPHAN = Threads.Atomic{Int}(1)

function take_ref_id!()
    tid = 0
    uid = 0
    id = 0
    _branch = ""
    if Dagger.in_task()
        tid = sch_handle().thunk_id.id
        uid = 0
        counter = get!(MPIREF_TID, tid, Threads.Atomic{Int}(1))
        id = Threads.atomic_add!(counter, 1)
        _branch = "in_task"
    elseif MPI_TID[] != 0
        tid = MPI_TID[]
        uid = 0
        counter = get!(MPIREF_TID, tid, Threads.Atomic{Int}(1))
        id = Threads.atomic_add!(counter, 1)
        _branch = "MPI_TID"
    elseif MPI_UID[] != 0
        tid = 0
        uid = MPI_UID[]
        counter = get!(MPIREF_UID, uid, Threads.Atomic{Int}(1))
        id = Threads.atomic_add!(counter, 1)
        _branch = "MPI_UID"
    else
        tid = 0
        uid = Int(Threads.atomic_add!(MPIREF_ORPHAN, 1))
        counter = get!(MPIREF_UID, uid, Threads.Atomic{Int}(1))
        id = Threads.atomic_add!(counter, 1)
        _branch = "orphan"
    end
    return MPIRefID(tid, uid, id)
end

const MPIREF_REGISTRY = Base.Lockable(Dict{MPIRefID, DRef}())

const ALIASING_PENDING = Vector{Tuple{MPIRef, Any, UInt32, Int}}()

"""
Service any pending aliasing requests where we are the owner.
Called from recv_yield loops to avoid deadlock when a requester is blocking
waiting for aliasing from us while we're blocked waiting for someone else.
"""
# #region agent log
const _SAR_ACTIVE = Threads.Atomic{Int}(0)
# #endregion
const _SAR_OUTBOX = Vector{Tuple{Any, MPI.Comm, Int, UInt32}}()

const _SAR_STATE = Ref{Symbol}(:idle)
const _SAR_LEN_BUF = Int64[0]
const _SAR_REQ = Ref{Union{Nothing, MPI.Request}}(nothing)
const _SAR_DATA_BUF = Ref{Vector{UInt8}}(UInt8[])
const _SAR_SRC = Ref{Int}(0)

function service_aliasing_requests(comm::MPI.Comm)
    _prev = Threads.atomic_add!(_SAR_ACTIVE, 1)
    if _prev > 0
        Threads.atomic_sub!(_SAR_ACTIVE, 1)
        return
    end

    rank = MPI.Comm_rank(comm)

    if !isempty(ALIASING_PENDING)
        still_pending = Tuple{MPIRef, Any, UInt32, Int}[]
        for (handle, dep_mod, response_tag, src) in ALIASING_PENDING
            inner_ref = lock(MPIREF_REGISTRY) do reg
                get(reg, handle.id, nothing)
            end
            if inner_ref !== nothing
                value = poolget(inner_ref)
                ainfo = aliasing(value, dep_mod)
                push!(_SAR_OUTBOX, (ainfo, comm, src, response_tag))
            else
                push!(still_pending, (handle, dep_mod, response_tag, src))
            end
        end
        empty!(ALIASING_PENDING)
        append!(ALIASING_PENDING, still_pending)
    end

    while true
        if _SAR_STATE[] == :idle
            _SAR_LEN_BUF[1] = 0
            _SAR_REQ[] = MPI.Irecv!(MPI.Buffer(_SAR_LEN_BUF), comm;
                source=Int(MPI.API.MPI_ANY_SOURCE[]), tag=Int(ALIASING_REQUEST_TAG))
            _SAR_STATE[] = :wait_len
        end

        if _SAR_STATE[] == :wait_len
            done, status = MPI.Test(_SAR_REQ[], MPI.Status)
            if !done
                break
            end
            _SAR_SRC[] = MPI.Get_source(status)
            nbytes = _SAR_LEN_BUF[1]
            _SAR_DATA_BUF[] = Array{UInt8}(undef, nbytes)
            _SAR_REQ[] = MPI.Irecv!(MPI.Buffer(_SAR_DATA_BUF[]), comm;
                source=_SAR_SRC[], tag=Int(ALIASING_REQUEST_TAG))
            _SAR_STATE[] = :wait_data
        end

        if _SAR_STATE[] == :wait_data
            done, status = MPI.Test(_SAR_REQ[], MPI.Status)
            if !done
                break
            end
            payload = MPI.deserialize(_SAR_DATA_BUF[])
            # (SAR recv log removed to reduce noise)
            (handle::MPIRef, dep_mod, response_tag::UInt32) = payload
            if handle.rank == rank
                inner_ref = handle.innerRef
                if inner_ref === nothing
                    inner_ref = lock(MPIREF_REGISTRY) do reg
                        get(reg, handle.id, nothing)
                    end
                end
                if inner_ref !== nothing
                    value = poolget(inner_ref)
                    ainfo = aliasing(value, dep_mod)
                    push!(_SAR_OUTBOX, (ainfo, comm, _SAR_SRC[], response_tag))
                else
                    push!(ALIASING_PENDING, (handle, dep_mod, response_tag, _SAR_SRC[]))
                end
            end
            _SAR_STATE[] = :idle
            continue
        end
    end

    while !isempty(_SAR_OUTBOX)
        (ainfo, _comm, dest, rtag) = popfirst!(_SAR_OUTBOX)
        _send_outbox_response(ainfo, _comm, dest, Int(rtag))
    end

    Threads.atomic_sub!(_SAR_ACTIVE, 1)
end

function _send_outbox_response(value, comm, dest, tag)
    buf = MPI.serialize(value)
    len_buf = Int64[length(buf)]
    lock(SEND_SERIALIZE_LOCK) do
        GC.@preserve buf len_buf begin
            req_len = MPI.Isend(len_buf, comm; dest, tag)
            while true
                finish, _ = MPI.Test(req_len, MPI.Status)
                finish && break
                yield()
            end
            req_data = MPI.Isend(buf, comm; dest, tag)
            while true
                finish, _ = MPI.Test(req_data, MPI.Status)
                finish && break
                yield()
            end
        end
    end
end

#TODO: partitioned scheduling with comm bifurcation
function tochunk_pset(x, space::MPIMemorySpace; device=nothing, kwargs...)
    @assert space.comm == MPI.COMM_WORLD "$(space.comm) != $(MPI.COMM_WORLD)"
    local_rank = MPI.Comm_rank(space.comm)
    Mid = take_ref_id!()
    if local_rank != space.rank
        return MPIRef(space.comm, space.rank, 0, nothing, Mid)
    else
        innerRef = poolset(x; device, kwargs...)
        lock(MPIREF_REGISTRY) do reg
            reg[Mid] = innerRef
        end
        return MPIRef(space.comm, space.rank, sizeof(x), innerRef, Mid)
    end
end

const DEADLOCK_DETECT = TaskLocalValue{Bool}(()->true)
const DEADLOCK_WARN_PERIOD = TaskLocalValue{Float64}(()->10.0)
const DEADLOCK_TIMEOUT_PERIOD = TaskLocalValue{Float64}(()->60.0)
# When true, __wait_for_request spins without yield so remotecall sender completes before other tasks run.
const REMOTECALL_SENDER_NO_YIELD = TaskLocalValue{Bool}(()->false)
const RECV_WAITING = Base.Lockable(Dict{Tuple{MPI.Comm, Int, Int}, Base.Event}())

# MPI_ANY_TAG + queue: pool of Irecv(ANY_SOURCE, ANY_TAG) and completion queue keyed by (comm, source, tag).
const RECV_POOL_SIZE = 64
const _RECV_POOL = Dict{MPI.Comm, Any}()  # comm -> RecvPoolState
const _RECV_POOL_LOCK = ReentrantLock()

# Completion queue: (comm, source, tag) -> list of received values (one waiter per key at a time).
const _RECV_COMPLETION_QUEUE = Base.Lockable(Dict{Tuple{MPI.Comm, Int, Int}, Vector{Any}}())

struct InplaceInfo
    type::DataType
    shape::Tuple
end
struct InplaceSparseInfo
    type::DataType
    m::Int
    n::Int
    colptr::Int
    rowval::Int
    nzval::Int
end

# MPI.Buffer uses Int32 for count; reject corrupt or oversized length to avoid InexactError.
const MAX_SERIALIZED_RECV_LENGTH = Int64(typemax(Int32))

# Per-slot state for the recv pool. Phase: :waiting_length | :waiting_data | :waiting_inplace_* | :idle (slot free).
mutable struct RecvPoolSlot
    phase::Symbol
    comm::MPI.Comm
    source::Int
    tag::Int
    len_buf::Vector{Int64}
    req::Union{MPI.Request, Nothing}
    data_buf::Vector{UInt8}
    # Inplace: for InplaceInfo we store the array buffer; for InplaceSparseInfo we store (colptr, rowval, nzval) as we go.
    inplace_info::Union{InplaceInfo, InplaceSparseInfo, Nothing}
    inplace_bufs::Vector{Any}  # accumulated inplace arrays
end

mutable struct RecvPoolState
    slots::Vector{RecvPoolSlot}
    initialized::Bool
end

function _recv_pool_for_comm(comm::MPI.Comm)
    lock(_RECV_POOL_LOCK) do
        if !haskey(_RECV_POOL, comm)
            _RECV_POOL[comm] = RecvPoolState(RecvPoolSlot[], false)
        end
        return _RECV_POOL[comm]
    end
end

const _MPI_ANY_SOURCE = Int(MPI.API.MPI_ANY_SOURCE[])
const _MPI_ANY_TAG = Int(MPI.API.MPI_ANY_TAG[])

function _recv_pool_init!(pool::RecvPoolState, comm::MPI.Comm)
    pool.initialized && return
    rank = MPI.Comm_rank(comm)
    for i in 1:RECV_POOL_SIZE
        len_buf = Int64[0]
        req = MPI.Irecv!(MPI.Buffer(len_buf), comm; source=_MPI_ANY_SOURCE, tag=_MPI_ANY_TAG)
        slot = RecvPoolSlot(:waiting_length, comm, -1, -1, len_buf, req, UInt8[], nothing, Any[])
        push!(pool.slots, slot)
    end
    pool.initialized = true
end

function supports_inplace_mpi(value)
    if value isa DenseArray && isbitstype(eltype(value))
        return true
    else
        return false
    end
end
function recv_yield!(buffer, comm, src, tag)
    rank = MPI.Comm_rank(comm)
    if !supports_inplace_mpi(buffer)
        return recv_yield(comm, src, tag), false
    end
    # Inplace: sender uses InplaceInfo+array (length-prefix), pool assembles and queues the array; copy to user buffer.
    value = recv_yield(comm, src, tag)
    copy!(buffer, value)
    return buffer, true
end

function recv_yield(comm, src, tag)
    rank = MPI.Comm_rank(comm)
    key = (comm, src, tag)

    # Check completion queue first (message may already have been received by the pool).
    value = lock(_RECV_COMPLETION_QUEUE) do q
        if haskey(q, key) && !isempty(q[key])
            ref = popfirst!(q[key])
            isempty(q[key]) && delete!(q, key)
            return poolget(ref)
        end
        return nothing
    end
    if value !== nothing
        return value
    end

    # Ensure no other receiver is waiting for this (comm, src, tag).
    our_event = Base.Event()
    @label retry
    other_event = lock(RECV_WAITING) do waiting
        if haskey(waiting, key)
            waiting[key]
        else
            waiting[key] = our_event
            nothing
        end
    end
    if other_event !== nothing
        wait(other_event)
        @goto retry
    end

    # Loop: drain pool, check queue, service aliasing, deadlock detect, yield.
    time_start = time_ns()
    detect = DEADLOCK_DETECT[]
    warn_period = round(UInt64, DEADLOCK_WARN_PERIOD[] * 1e9)
    timeout_period = round(UInt64, DEADLOCK_TIMEOUT_PERIOD[] * 1e9)
    @label wait_loop
    service_recv_pool(comm)
    value = lock(_RECV_COMPLETION_QUEUE) do q
        if haskey(q, key) && !isempty(q[key])
            ref = popfirst!(q[key])
            isempty(q[key]) && delete!(q, key)
            return poolget(ref)
        end
        return nothing
    end
    if value !== nothing
        lock(RECV_WAITING) do waiting
            delete!(waiting, key)
            notify(our_event)
        end
        return value
    end
    service_aliasing_requests(comm)
    warn_period = mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, rank, tag, "recv", src)
    yield()
    @goto wait_loop
end

function recv_yield_inplace!(array, comm, my_rank, their_rank, tag)
    time_start = time_ns()
    detect = DEADLOCK_DETECT[]
    warn_period = round(UInt64, DEADLOCK_WARN_PERIOD[] * 1e9)
    timeout_period = round(UInt64, DEADLOCK_TIMEOUT_PERIOD[] * 1e9)

    req = MPI.Irecv!(MPI.Buffer(array), comm; source=their_rank, tag=tag)
    while true
        finish, status = MPI.Test(req, MPI.Status)
        if finish
            if MPI.Get_error(status) != MPI.SUCCESS
                error("recv_yield failed with error $(MPI.Get_error(status))")
            end
            return array
        end
        service_recv_pool(comm)
        service_aliasing_requests(comm)
        warn_period = mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, my_rank, tag, "recv", their_rank)
        yield()
    end
end

function recv_yield_inplace(_value::InplaceInfo, comm, my_rank, their_rank, tag)
    T = _value.type
    @assert T <: Array && isbitstype(eltype(T)) "recv_yield_inplace only supports inplace MPI transfers of bitstype dense arrays"
    array = Array{eltype(T)}(undef, _value.shape)
    return recv_yield_inplace!(array, comm, my_rank, their_rank, tag)
end

function recv_yield_inplace(_value::InplaceSparseInfo, comm, my_rank, their_rank, tag)
    T = _value.type
    @assert T <: SparseMatrixCSC "recv_yield_inplace only supports inplace MPI transfers of SparseMatrixCSC"

    colptr = recv_yield_inplace!(Vector{Int64}(undef, _value.colptr), comm, my_rank, their_rank, tag)
    rowval = recv_yield_inplace!(Vector{Int64}(undef, _value.rowval), comm, my_rank, their_rank, tag)
    nzval = recv_yield_inplace!(Vector{eltype(T)}(undef, _value.nzval), comm, my_rank, their_rank, tag)

    return SparseMatrixCSC{eltype(T), Int64}(_value.m, _value.n, colptr, rowval, nzval)
end

"""
Drain the recv pool: Test each slot's request; when complete, advance the state machine
(length -> data -> optional inplace -> ready). Push completed messages to the completion
queue and notify waiters. Replenish slots with new Irecv(ANY_SOURCE, ANY_TAG) for length.
Called from recv_yield's wait loop and from __wait_for_request (and recv_yield_inplace!).
"""
function service_recv_pool(comm::MPI.Comm)
    pool = _recv_pool_for_comm(comm)
    _recv_pool_init!(pool, comm)
    rank = MPI.Comm_rank(comm)

    for slot in pool.slots
        slot.req === nothing && continue
        done, status = MPI.Test(slot.req, MPI.Status)
        if !done
            continue
        end
        if MPI.Get_error(status) != MPI.SUCCESS
            error("recv pool slot failed with error $(MPI.Get_error(status))")
        end

        if slot.phase == :waiting_length
            slot.source = MPI.Get_source(status)
            slot.tag = MPI.Get_tag(status)
            count = slot.len_buf[1]
            if count < 0 || count > MAX_SERIALIZED_RECV_LENGTH
                error("recv pool: invalid or corrupt length $count (max $(MAX_SERIALIZED_RECV_LENGTH)); source=$(slot.source), tag=$(slot.tag)")
            end
            slot.data_buf = Array{UInt8}(undef, count)
            slot.req = MPI.Irecv!(MPI.Buffer(slot.data_buf), comm; source=slot.source, tag=slot.tag)
            slot.phase = :waiting_data
            continue
        end

        if slot.phase == :waiting_data
            value = MPI.deserialize(slot.data_buf)
            if slot.tag == Int(ALIASING_REQUEST_TAG)
                # Hand off to aliasing path (same as service_aliasing_requests).
                (handle::MPIRef, dep_mod, response_tag::UInt32) = value
                if handle.rank == rank
                    inner_ref = handle.innerRef
                    if inner_ref === nothing
                        inner_ref = lock(MPIREF_REGISTRY) do reg
                            get(reg, handle.id, nothing)
                        end
                    end
                    if inner_ref !== nothing
                        v = poolget(inner_ref)
                        ainfo = aliasing(v, dep_mod)
                        push!(_SAR_OUTBOX, (ainfo, comm, slot.source, response_tag))
                    else
                        push!(ALIASING_PENDING, (handle, dep_mod, response_tag, slot.source))
                    end
                end
                _recv_pool_slot_reset!(slot, comm)
                continue
            end

            if value isa InplaceInfo
                T = value.type
                @assert T <: Array && isbitstype(eltype(T))
                arr = Array{eltype(T)}(undef, value.shape)
                slot.inplace_info = value
                slot.inplace_bufs = [arr]
                slot.req = MPI.Irecv!(MPI.Buffer(arr), comm; source=slot.source, tag=slot.tag)
                slot.phase = :waiting_inplace_dense
                continue
            end

            if value isa InplaceSparseInfo
                slot.inplace_info = value
                colptr_buf = Vector{Int64}(undef, value.colptr)
                slot.inplace_bufs = [colptr_buf]
                slot.req = MPI.Irecv!(MPI.Buffer(colptr_buf), comm; source=slot.source, tag=slot.tag)
                slot.phase = :waiting_inplace_colptr
                continue
            end

            # Serialized value complete.
            _recv_pool_push_and_reset!(slot, comm, value)
            continue
        end

        if slot.phase == :waiting_inplace_dense
            arr = slot.inplace_bufs[1]
            _recv_pool_push_and_reset!(slot, comm, arr)
            continue
        end

        if slot.phase == :waiting_inplace_colptr
            sp = slot.inplace_info::InplaceSparseInfo
            rowval_buf = Vector{Int64}(undef, sp.rowval)
            push!(slot.inplace_bufs, rowval_buf)
            slot.req = MPI.Irecv!(MPI.Buffer(rowval_buf), comm; source=slot.source, tag=slot.tag)
            slot.phase = :waiting_inplace_rowval
            continue
        end

        if slot.phase == :waiting_inplace_rowval
            sp = slot.inplace_info::InplaceSparseInfo
            nzval_buf = Vector{eltype(sp.type)}(undef, sp.nzval)
            push!(slot.inplace_bufs, nzval_buf)
            slot.req = MPI.Irecv!(MPI.Buffer(nzval_buf), comm; source=slot.source, tag=slot.tag)
            slot.phase = :waiting_inplace_nzval
            continue
        end

        if slot.phase == :waiting_inplace_nzval
            sp = slot.inplace_info::InplaceSparseInfo
            colptr = slot.inplace_bufs[1]::Vector{Int64}
            rowval = slot.inplace_bufs[2]::Vector{Int64}
            nzval = slot.inplace_bufs[3]
            mat = SparseMatrixCSC{eltype(sp.type), Int64}(sp.m, sp.n, colptr, rowval, nzval)
            _recv_pool_push_and_reset!(slot, comm, mat)
            continue
        end
    end
end

function _recv_pool_slot_reset!(slot::RecvPoolSlot, comm::MPI.Comm)
    slot.phase = :waiting_length
    slot.source = -1
    slot.tag = -1
    slot.len_buf[1] = 0
    slot.req = MPI.Irecv!(MPI.Buffer(slot.len_buf), comm; source=_MPI_ANY_SOURCE, tag=_MPI_ANY_TAG)
    slot.data_buf = UInt8[]
    slot.inplace_info = nothing
    slot.inplace_bufs = Any[]
end

function _recv_pool_push_and_reset!(slot::RecvPoolSlot, comm::MPI.Comm, value::Any)
    key = (comm, slot.source, slot.tag)
    ref = poolset(value)
    lock(_RECV_COMPLETION_QUEUE) do q
        if !haskey(q, key)
            q[key] = Any[]
        end
        push!(q[key], ref)
    end
    lock(RECV_WAITING) do waiting
        if haskey(waiting, key)
            notify(waiting[key])
        end
    end
    _recv_pool_slot_reset!(slot, comm)
end

function recv_yield_serialized(comm, my_rank, their_rank, tag)
    time_start = time_ns()
    detect = DEADLOCK_DETECT[]
    warn_period = round(UInt64, DEADLOCK_WARN_PERIOD[] * 1e9)
    timeout_period = round(UInt64, DEADLOCK_TIMEOUT_PERIOD[] * 1e9)
    len_buf = Int64[0]
    local req_len
    try
        req_len = MPI.Irecv!(MPI.Buffer(len_buf), comm; source=their_rank, tag=tag)
    catch e
        # #region agent log
        try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H15_mpi_err\",\"location\":\"mpi.jl:recv_ser:Irecv_len\",\"message\":\"Irecv! len threw\",\"data\":{\"rank\":$my_rank,\"tag\":$tag,\"src\":$their_rank,\"error\":\"$(sprint(showerror, e))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
        # #endregion
        rethrow()
    end
    while true
        local finish, status
        try
            finish, status = MPI.Test(req_len, MPI.Status)
        catch e
            # #region agent log
            try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H15_mpi_err\",\"location\":\"mpi.jl:recv_ser:Test_len\",\"message\":\"Test len threw\",\"data\":{\"rank\":$my_rank,\"tag\":$tag,\"src\":$their_rank,\"error\":\"$(sprint(showerror, e))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
            # #endregion
            rethrow()
        end
        if finish
            if MPI.Get_error(status) != MPI.SUCCESS
                error("recv_yield_serialized len failed with error $(MPI.Get_error(status))")
            end
            break
        end
        service_aliasing_requests(comm)
        warn_period = mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, my_rank, tag, "recv", their_rank)
        yield()
    end

    count = len_buf[1]
    if count < 0 || count > MAX_SERIALIZED_RECV_LENGTH
        error("recv_yield_serialized: invalid or corrupt length $count (max $(MAX_SERIALIZED_RECV_LENGTH)); src=$their_rank, tag=$tag")
    end
    buf = Array{UInt8}(undef, count)
    local req_data
    try
        req_data = MPI.Irecv!(MPI.Buffer(buf), comm; source=their_rank, tag=tag)
    catch e
        # #region agent log
        try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H15_mpi_err\",\"location\":\"mpi.jl:recv_ser:Irecv_data\",\"message\":\"Irecv! data threw\",\"data\":{\"rank\":$my_rank,\"tag\":$tag,\"src\":$their_rank,\"count\":$count,\"error\":\"$(sprint(showerror, e))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
        # #endregion
        rethrow()
    end
    while true
        local finish, status
        try
            finish, status = MPI.Test(req_data, MPI.Status)
        catch e
            # #region agent log
            try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H15_mpi_err\",\"location\":\"mpi.jl:recv_ser:Test_data\",\"message\":\"Test data threw\",\"data\":{\"rank\":$my_rank,\"tag\":$tag,\"src\":$their_rank,\"error\":\"$(sprint(showerror, e))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
            # #endregion
            rethrow()
        end
        if finish
            if MPI.Get_error(status) != MPI.SUCCESS
                error("recv_yield_serialized data failed with error $(MPI.Get_error(status))")
            end
            return MPI.deserialize(buf)
        end
        service_aliasing_requests(comm)
        warn_period = mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, my_rank, tag, "recv", their_rank)
        yield()
    end
end

const SEEN_TAGS = Dict{Int32, Type}()
# Serialize nonblocking sends so only one Isend+wait is in flight at a time; avoids MPICH internal_Isend segfault with many concurrent requests.
const SEND_SERIALIZE_LOCK = ReentrantLock()
send_yield!(value, comm, dest, tag; check_seen::Bool=true) =
    _send_yield(value, comm, dest, tag; check_seen, inplace=true)
send_yield(value, comm, dest, tag; check_seen::Bool=true) =
    _send_yield(value, comm, dest, tag; check_seen, inplace=false)
function _send_yield(value, comm, dest, tag; check_seen::Bool=true, inplace::Bool)
    rank = MPI.Comm_rank(comm)

    if check_seen && haskey(SEEN_TAGS, tag) && SEEN_TAGS[tag] !== typeof(value)
        @error "[rank $(MPI.Comm_rank(comm))][tag $tag] Already seen tag (previous type: $(SEEN_TAGS[tag]), new type: $(typeof(value)))" exception=(InterruptException(),backtrace())
    end
    if check_seen
        SEEN_TAGS[tag] = typeof(value)
    end
    # Inplace sends use InplaceInfo+array so the recv pool (ANY_TAG) can receive them; never send raw array only.
    if inplace && supports_inplace_mpi(value)
        send_yield_serialized(InplaceInfo(typeof(value), size(value)), comm, rank, dest, tag)
        send_yield_inplace(value, comm, rank, dest, tag)
    else
        send_yield_serialized(value, comm, rank, dest, tag)
    end
end

function send_yield_inplace(value, comm, my_rank, their_rank, tag)
    @opcounter :send_yield_inplace
    lock(SEND_SERIALIZE_LOCK) do
        GC.@preserve value begin
            local req
            try
                req = MPI.Isend(value, comm; dest=their_rank, tag)
            catch e
                # #region agent log
                try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H15_mpi_err\",\"location\":\"mpi.jl:send_inplace:Isend\",\"message\":\"Isend inplace threw\",\"data\":{\"rank\":$my_rank,\"tag\":$tag,\"dest\":$their_rank,\"error\":\"$(sprint(showerror, e))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
                # #endregion
                rethrow()
            end
            __wait_for_request(req, comm, my_rank, their_rank, tag, "send_yield", "send")
        end
    end
end

function send_yield_serialized(value, comm, my_rank, their_rank, tag)
    @opcounter :send_yield_serialized
    if value isa Array && isbitstype(eltype(value))
        send_yield_serialized(InplaceInfo(typeof(value), size(value)), comm, my_rank, their_rank, tag)
        send_yield_inplace(value, comm, my_rank, their_rank, tag)
    elseif value isa SparseMatrixCSC && isbitstype(eltype(value))
        send_yield_serialized(InplaceSparseInfo(typeof(value), value.m, value.n, length(value.colptr), length(value.rowval), length(value.nzval)), comm, my_rank, their_rank, tag)
        send_yield_inplace(value.colptr, comm, my_rank, their_rank, tag)
        send_yield_inplace(value.rowval, comm, my_rank, their_rank, tag)
        send_yield_inplace(value.nzval,  comm, my_rank, their_rank, tag)
    else
        buf = MPI.serialize(value)
        n = length(buf)
        lock(SEND_SERIALIZE_LOCK) do
            # Non-GC buffers so MPICH gets a stable pointer. Still Isend + yielding wait (no blocking).
            ptr_len = Base.Libc.malloc(8)
            ptr_len === C_NULL && throw(OutOfMemoryError())
            try
                arr_len = Base.unsafe_wrap(Array, Ptr{Int64}(ptr_len), (1,); own=false)
                arr_len[1] = n
                req_len = MPI.Isend(arr_len, comm; dest=their_rank, tag)
                __wait_for_request(req_len, comm, my_rank, their_rank, tag, "send_yield", "send")
            finally
                Base.Libc.free(ptr_len)
            end
            ptr = Base.Libc.malloc(n)
            ptr === C_NULL && throw(OutOfMemoryError())
            try
                arr = Base.unsafe_wrap(Array, Ptr{UInt8}(ptr), (n,); own=false)
                copyto!(arr, buf)
                req_data = MPI.Isend(arr, comm; dest=their_rank, tag)
                __wait_for_request(req_data, comm, my_rank, their_rank, tag, "send_yield", "send")
            finally
                Base.Libc.free(ptr)
            end
        end
    end
end

function _send_yield_raw(value, comm, dest, tag)
    rank = MPI.Comm_rank(comm)
    buf = MPI.serialize(value)
    n = length(buf)
    lock(SEND_SERIALIZE_LOCK) do
        ptr_len = Base.Libc.malloc(8)
        ptr_len === C_NULL && throw(OutOfMemoryError())
        try
            arr_len = Base.unsafe_wrap(Array, Ptr{Int64}(ptr_len), (1,); own=false)
            arr_len[1] = n
            req_len = MPI.Isend(arr_len, comm; dest, tag)
            __wait_for_request(req_len, comm, rank, dest, tag, "send_yield_raw_len", "send")
        finally
            Base.Libc.free(ptr_len)
        end
        ptr = Base.Libc.malloc(n)
        ptr === C_NULL && throw(OutOfMemoryError())
        try
            arr = Base.unsafe_wrap(Array, Ptr{UInt8}(ptr), (n,); own=false)
            copyto!(arr, buf)
            req_data = MPI.Isend(arr, comm; dest, tag)
            __wait_for_request(req_data, comm, rank, dest, tag, "send_yield_raw_data", "send")
        finally
            Base.Libc.free(ptr)
        end
    end
end

function __wait_for_request(req, comm, my_rank, their_rank, tag, fn::String, kind::String)
    time_start = time_ns()
    detect = DEADLOCK_DETECT[]
    warn_period = round(UInt64, DEADLOCK_WARN_PERIOD[] * 1e9)
    timeout_period = round(UInt64, DEADLOCK_TIMEOUT_PERIOD[] * 1e9)
    while true
        local finish, status
        try
            finish, status = MPI.Test(req, MPI.Status)
        catch e
            # #region agent log
            try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H15_mpi_err\",\"location\":\"mpi.jl:__wait_for_request:Test\",\"message\":\"MPI.Test threw\",\"data\":{\"rank\":$my_rank,\"tag\":$tag,\"fn\":\"$fn\",\"kind\":\"$kind\",\"dest\":$their_rank,\"error\":\"$(sprint(showerror, e))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
            # #endregion
            rethrow()
        end
        if finish
            if MPI.Get_error(status) != MPI.SUCCESS
                error("$fn failed with error $(MPI.Get_error(status))")
            end
            return
        end
        if REMOTECALL_SENDER_NO_YIELD[]
            # Sender in remotecall_endpoint: spin until send completes so we don't yield to other tasks.
            if detect && (time_ns() - time_start) > timeout_period
                error("[rank $my_rank][tag $tag] Hit hang on $kind (dest: $their_rank) [remotecall sender spin]")
            end
            continue
        end
        service_recv_pool(comm)
        service_aliasing_requests(comm)
        warn_period = mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, my_rank, tag, kind, their_rank)
        yield()
    end
end

function bcast_send_yield(value, comm, root, tag)
    @opcounter :bcast_send_yield
    sz = MPI.Comm_size(comm)
    rank = MPI.Comm_rank(comm)
    for other_rank in 0:(sz-1)
        rank == other_rank && continue
        send_yield(value, comm, other_rank, tag)
    end
end

#= Maybe can be worth it to implement this
function bcast_send_yield!(value, comm, root, tag)
    sz = MPI.Comm_size(comm)
    rank = MPI.Comm_rank(comm)

    for other_rank in 0:(sz-1)
        rank == other_rank && continue
        #println("[rank $rank] Sending to rank $other_rank")
        send_yield!(value, comm, other_rank, tag)
    end
end

function bcast_recv_yield!(value, comm, root, tag)
    sz = MPI.Comm_size(comm)
    rank = MPI.Comm_rank(comm)
    #println("[rank $rank] receive from rank $root")
    recv_yield!(value, comm, root, tag)
end
=#
function mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, rank, tag, kind, srcdest)
    time_elapsed = (time_ns() - time_start)
    if detect && time_elapsed > warn_period
        @warn "[rank $rank][tag $tag] Hit probable hang on $kind (dest: $srcdest) [$(round(time_elapsed/1e9, digits=1))s]"
        return typemax(UInt64)
    end
    if detect && time_elapsed > timeout_period
        # #region agent log
        try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H11_timeout\",\"location\":\"mpi.jl:deadlock_detect:TIMEOUT\",\"message\":\"deadlock TIMEOUT - will throw\",\"data\":{\"rank\":$rank,\"tag\":$tag,\"kind\":\"$kind\",\"srcdest\":$srcdest,\"elapsed_s\":$(time_elapsed/1e9)},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
        # #endregion
        error("[rank $rank][tag $tag] Hit hang on $kind (dest: $srcdest)")
    end
    return warn_period
end

#discuss this with julian
WeakChunk(c::Chunk{T,H}) where {T,H<:MPIRef} = WeakChunk(c.handle.rank, c.handle.id.id, WeakRef(c))

function MemPool.poolget(ref::MPIRef; uniform::Bool=false)
    if !uniform && ref.rank != MPI.Comm_rank(ref.comm)
        # #region agent log
        _r = MPI.Comm_rank(ref.comm); try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H12_poolget\",\"location\":\"mpi.jl:poolget\",\"message\":\"MPIRef rank mismatch about to assert\",\"data\":{\"local_rank\":$_r,\"ref_rank\":$(ref.rank),\"ref_id\":\"$(ref.id)\",\"uniform\":$uniform,\"backtrace\":\"$(replace(sprint(Base.show_backtrace, backtrace()), '\"'=>'\'', '\n'=>' '))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end
        # #endregion
    end
    @assert uniform || ref.rank == MPI.Comm_rank(ref.comm) "MPIRef rank mismatch: $(ref.rank) != $(MPI.Comm_rank(ref.comm))"
    if uniform
        tag = to_tag()
        if ref.rank == MPI.Comm_rank(ref.comm)
            value = poolget(ref.innerRef)
            @opcounter :poolget_bcast_send_yield
            bcast_send_yield(value, ref.comm, ref.rank, tag)
            return value
        else
            return recv_yield(ref.comm, ref.rank, tag)
        end
    else
        return poolget(ref.innerRef)
    end
end
fetch_handle(ref::MPIRef; uniform::Bool=false) = poolget(ref; uniform)

function move!(dep_mod, to_space::MPIMemorySpace, from_space::MPIMemorySpace, to::Chunk, from::Chunk)
    @assert to.handle isa MPIRef && from.handle isa MPIRef "MPIRef expected"
    @assert to.handle.comm == from.handle.comm "MPIRef comm mismatch"
    @assert to.handle.rank == to_space.rank && from.handle.rank == from_space.rank "MPIRef rank mismatch"
    local_rank = MPI.Comm_rank(from.handle.comm)
    if to_space.rank == from_space.rank == local_rank
        move!(dep_mod, to_space.innerSpace, from_space.innerSpace, to, from)
    else
        @dagdebug nothing :mpi "[$local_rank][$tag] Moving from  $(from_space.rank)  to  $(to_space.rank)\n"
        tag = to_tag()
        if local_rank == from_space.rank
            send_yield!(poolget(from.handle; uniform=false), to_space.comm, to_space.rank, tag)
        elseif local_rank == to_space.rank
            #@dagdebug nothing :mpi "[$local_rank][$tag] Receiving from rank $(from_space.rank) with tag $tag, type of buffer: $(typeof(poolget(to.handle; uniform=false)))"
            to_val = poolget(to.handle; uniform=false)
            val, inplace = recv_yield!(to_val, from_space.comm, from_space.rank, tag)
            if !inplace
                move!(dep_mod, to_space.innerSpace, from_space.innerSpace, to_val, val)
            end
        end
    end
    @dagdebug nothing :mpi "[$local_rank][$tag] Finished moving from  $(from_space.rank)  to  $(to_space.rank) successfuly\n"
end
function move!(dep_mod::RemainderAliasing{<:MPIMemorySpace}, to_space::MPIMemorySpace, from_space::MPIMemorySpace, to::Chunk, from::Chunk)
    @assert to.handle isa MPIRef && from.handle isa MPIRef "MPIRef expected"
    @assert to.handle.comm == from.handle.comm "MPIRef comm mismatch"
    @assert to.handle.rank == to_space.rank && from.handle.rank == from_space.rank "MPIRef rank mismatch"
    local_rank = MPI.Comm_rank(from.handle.comm)
    if to_space.rank == from_space.rank == local_rank
        move!(dep_mod, to_space.innerSpace, from_space.innerSpace, to, from)
    else
        tag = to_tag()
        @dagdebug nothing :mpi "[$local_rank][$tag] Moving from  $(from_space.rank)  to  $(to_space.rank)\n"
        if local_rank == from_space.rank
            # Get the source data for each span
            len = sum(span_tuple->span_len(span_tuple[1]), dep_mod.spans)
            copies = Vector{UInt8}(undef, len)
            offset = 1
            for (from_span, _) in dep_mod.spans
                #GC.@preserve copy begin
                    from_ptr = Ptr{UInt8}(from_span.ptr)
                    to_ptr = Ptr{UInt8}(pointer(copies, offset))
                    unsafe_copyto!(to_ptr, from_ptr, from_span.len)
                    offset += from_span.len
                #end
            end

            # Send the spans
            #send_yield(len, to_space.comm, to_space.rank, tag)
            send_yield!(copies, to_space.comm, to_space.rank, tag; check_seen=false)
            #send_yield(copies, to_space.comm, to_space.rank, tag)
        elseif local_rank == to_space.rank
            # Receive the spans
            len = sum(span_tuple->span_len(span_tuple[1]), dep_mod.spans)
            copies = Vector{UInt8}(undef, len)
            recv_yield!(copies, from_space.comm, from_space.rank, tag)
            #copies = recv_yield(from_space.comm, from_space.rank, tag)

            # Copy the data into the destination object
            #for (copy, (_, to_span)) in zip(copies, dep_mod.spans)
            offset = 1
            for (_, to_span) in dep_mod.spans
                #GC.@preserve copy begin
                    from_ptr = Ptr{UInt8}(pointer(copies, offset))
                    to_ptr = Ptr{UInt8}(to_span.ptr)
                    unsafe_copyto!(to_ptr, from_ptr, to_span.len)
                    offset += to_span.len
                #end
            end

            # Ensure that the data is visible
            Core.Intrinsics.atomic_fence(:release)
        end
    end

    return
end


move(::MPIOSProc, ::MPIProcessor, x::Union{Function,Type}) = x
move(::MPIOSProc, ::MPIProcessor, x::Chunk{<:Union{Function,Type}}) = poolget(x.handle)

#TODO: out of place MPI move
function move(src::MPIOSProc, dst::MPIProcessor, x::Chunk)
    @assert src.comm == dst.comm "Multi comm move not supported"
    if Sch.SCHED_MOVE[]
        if dst.rank == MPI.Comm_rank(dst.comm)
            return poolget(x.handle)
        end
    else
        @assert src.rank == MPI.Comm_rank(src.comm) "Unwrapping not permited"
        @assert src.rank == x.handle.rank == dst.rank
        return poolget(x.handle)
    end
end

const MPI_UNIFORM = ScopedValue{Bool}(false)

@warn "bcast T if return type is not concrete" maxlog=1
function remotecall_endpoint(f, accel::Dagger.MPIAcceleration, from_proc, to_proc, from_space, to_space, data)
    loc_rank = MPI.Comm_rank(accel.comm)
    task = DATADEPS_CURRENT_TASK[]
    return with(MPI_UID=>task.uid, MPI_UNIFORM=>true) do
        @assert data isa Chunk "Expected Chunk, got $(typeof(data))"
        space = memory_space(data)
        tag = remotecall_tag(accel.comm, task.uid, from_proc.rank, to_proc.rank, data.handle.id)
        # #region agent log
        if loc_rank <= 1 && tag >= 598 && tag <= 612; try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H16_tag_op\",\"location\":\"mpi.jl:remotecall_endpoint\",\"message\":\"remotecall tag assigned\",\"data\":{\"rank\":$loc_rank,\"tag\":$tag,\"from_rank\":$(from_proc.rank),\"to_rank\":$(to_proc.rank),\"space_rank\":$(space.rank),\"task_uid\":$(task.uid),\"task_id\":$(task.id)},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end; end
        # #endregion
        if space.rank != from_proc.rank
            # If the data is already where it needs to be
            @assert space.rank == to_proc.rank
            if space.rank == loc_rank
                value = poolget(data.handle)
                data_converted = f(move(from_proc.innerProc, to_proc.innerProc, value))
                return tochunk(data_converted, to_proc, to_space)
            else
                T = move_type(from_proc.innerProc, to_proc.innerProc, chunktype(data))
                T_new = f !== identity ? Base._return_type(f, Tuple{T}) : T
                @assert isconcretetype(T_new) "Return type inference failed, expected concrete type, got $T -> $T_new"
                return tochunk(nothing, to_proc, to_space; type=T_new)
            end
        end

        # The data is on the source rank
        @assert space.rank == from_proc.rank
        if loc_rank == from_proc.rank == to_proc.rank
            value = poolget(data.handle)
            data_converted = f(move(from_proc.innerProc, to_proc.innerProc, value))
            return tochunk(data_converted, to_proc, to_space)
        else
            if loc_rank == from_proc.rank
                value = poolget(data.handle)
                data_moved = move(from_proc.innerProc, to_proc.innerProc, value)
                try
                    REMOTECALL_SENDER_NO_YIELD[] = true
                    Dagger.send_yield(data_moved, accel.comm, to_proc.rank, tag)
                finally
                    REMOTECALL_SENDER_NO_YIELD[] = false
                end
                # FIXME: This is wrong to take typeof(data_moved), because the type may change
                return tochunk(nothing, to_proc, to_space; type=typeof(data_moved))
            elseif loc_rank == to_proc.rank
                data_moved = Dagger.recv_yield(accel.comm, from_space.rank, tag)
                data_converted = f(move(from_proc.innerProc, to_proc.innerProc, data_moved))
                return tochunk(data_converted, to_proc, to_space)
            else
                T = move_type(from_proc.innerProc, to_proc.innerProc, chunktype(data))
                T_new = f !== identity ? Base._return_type(f, Tuple{T}) : T
                @assert isconcretetype(T_new) "Return type inference failed, expected concrete type, got $T -> $T_new"
                return tochunk(nothing, to_proc, to_space; type=T_new)
            end
        end
    end
end

move(src::Processor, dst::MPIProcessor, x::Chunk) = error("MPI move not supported")
move(to_proc::MPIProcessor, chunk::Chunk) =
    move(chunk.processor, to_proc, chunk)
move(to_proc::Processor, d::MPIRef) =
    move(MPIOSProc(d.rank), to_proc, d)
move(to_proc::MPIProcessor, x) =
    move(MPIOSProc(), to_proc, x)

move(::MPIProcessor, ::MPIProcessor, x::Union{Function,Type}) = x
move(::MPIProcessor, ::MPIProcessor, x::Chunk{<:Union{Function,Type}}) = poolget(x.handle)

@warn "Is this uniform logic valuable to have?" maxlog=1
function move(src::MPIProcessor, dst::MPIProcessor, x::Chunk)
    uniform = false #uniform = MPI_UNIFORM[]
    @assert uniform || src.rank == dst.rank "Unwrapping not permitted"
    if Sch.SCHED_MOVE[]
        # We can either unwrap locally, or return nothing
        if dst.rank == MPI.Comm_rank(dst.comm)
            return poolget(x.handle)
        end
    else
        # Either we're uniform (so everyone cooperates), or we're unwrapping locally
        if !uniform
            @assert src.rank == MPI.Comm_rank(src.comm) "Unwrapping not permitted"
            @assert src.rank == x.handle.rank == dst.rank
        end
        return poolget(x.handle; uniform)
    end
end

_precise_typeof(x) = typeof(x)
_precise_typeof(::Type{T}) where {T} = Type{T}

function execute!(proc::MPIProcessor, f, args...; kwargs...)
    local_rank = MPI.Comm_rank(proc.comm)
    islocal = local_rank == proc.rank
    inplace_move = f === move!
    result = nothing

    if islocal || inplace_move
        result = execute!(proc.innerProc, f, args...; kwargs...)
    end

    if inplace_move
        space = memory_space(nothing, proc)::MPIMemorySpace
        return tochunk(nothing, proc, space)
    end

    tag = to_tag()
    # #region agent log
    if local_rank <= 1 && tag >= 598 && tag <= 612; try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H16_tag_op\",\"location\":\"mpi.jl:execute!\",\"message\":\"execute! tag assigned\",\"data\":{\"rank\":$local_rank,\"tag\":$tag,\"proc_rank\":$(proc.rank),\"islocal\":$islocal,\"f\":\"$(nameof(f))\"},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end; end
    # #endregion
    if islocal
        T = typeof(result)
        space = memory_space(result, proc)::MPIMemorySpace
        T_space = (T, space.innerSpace)
        @opcounter :execute_bcast_send_yield
        bcast_send_yield(T_space, proc.comm, proc.rank, tag)
        return tochunk(result, proc, space)
    else
        T, innerSpace = recv_yield(proc.comm, proc.rank, tag)
        space = MPIMemorySpace(innerSpace, proc.comm, proc.rank)
        return tochunk(nothing, proc, space; type=T)
    end
end

accelerate!(::Val{:mpi}) = accelerate!(MPIAcceleration())

function initialize_acceleration!(a::MPIAcceleration)
    if !MPI.Initialized()
        MPI.Init(;threadlevel=:multiple)
    end
    # #region agent log
    _r = MPI.Comm_rank(a.comm); _tl = MPI.Query_thread(); if _r <= 1; try; open("/flare/dagger/fdadagger/.cursor/debug-852f70.log", "a") do io; println(io, "{\"sessionId\":\"852f70\",\"hypothesisId\":\"H2_init\",\"location\":\"mpi.jl:initialize_acceleration!\",\"message\":\"MPI init\",\"data\":{\"rank\":$_r,\"nthreads\":$(Threads.nthreads()),\"mpi_thread_level\":$_tl,\"tag_ub\":$(MPI.tag_ub())},\"timestamp\":$(round(Int,time()*1000))}"); end; catch; end; end
    # #endregion
    ctx = Dagger.Sch.eager_context()
    sz = MPI.Comm_size(a.comm)
    for i in 0:(sz-1)
        push!(ctx.procs, MPIOSProc(a.comm, i))
    end
    unique!(ctx.procs)
end

accel_matches_proc(accel::MPIAcceleration, proc::MPIOSProc) = true
accel_matches_proc(accel::MPIAcceleration, proc::MPIClusterProc) = true
accel_matches_proc(accel::MPIAcceleration, proc::MPIProcessor) = true
accel_matches_proc(accel::MPIAcceleration, proc) = false

function distribute(accel::MPIAcceleration, A::AbstractArray{T,N}, dist::Blocks{N}) where {T,N}
    comm = accel.comm
    rank = MPI.Comm_rank(comm)

    DA = view(A, dist)
    DB = DArray{T,N}(undef, dist, size(A))
    copyto!(DB, DA)

    return DB
end
