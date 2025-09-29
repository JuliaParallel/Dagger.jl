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

function aliasing(accel::MPIAcceleration, x::Chunk, T)
    handle = x.handle::MPIRef
    @assert accel.comm == handle.comm "MPIAcceleration comm mismatch"
    tag = to_tag(hash(handle.id, hash(:aliasing)))
    check_uniform(tag)
    rank = MPI.Comm_rank(accel.comm)

    if handle.rank == rank
        ainfo = aliasing(x, T)
        #Core.print("[$rank] aliasing: $ainfo, sending\n")
        bcast_send_yield(ainfo, accel.comm, handle.rank, tag)
    else
        #Core.print("[$rank] aliasing: receiving from $(handle.rank)\n")
        ainfo = recv_yield(accel.comm, handle.rank, tag)
        #Core.print("[$rank] aliasing: received $ainfo\n")
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

function take_ref_id!()
    tid = 0
    uid = 0
    id = 0
    if Dagger.in_task()
        tid = sch_handle().thunk_id.id
        uid = 0
        counter = get!(MPIREF_TID, tid, Threads.Atomic{Int}(1))
        id = Threads.atomic_add!(counter, 1)
    elseif MPI_TID[] != 0
        tid = MPI_TID[]
        uid = 0
        counter = get!(MPIREF_TID, tid, Threads.Atomic{Int}(1))
        id = Threads.atomic_add!(counter, 1)
    elseif MPI_UID[] != 0
        tid = 0
        uid = MPI_UID[]
        counter = get!(MPIREF_UID, uid, Threads.Atomic{Int}(1))
        id = Threads.atomic_add!(counter, 1)
    end
    return MPIRefID(tid, uid, id)
end

function to_tag(h::UInt)
    # FIXME: Use some kind of bounded re-hashing
    # FIXME: Re-hash with upper and lower
    bound = MPI.tag_ub()
    tag = abs(Base.unsafe_trunc(Int32, h))
    while tag > bound
        tag = tag - bound
    end
    return tag
end

#TODO: partitioned scheduling with comm bifurcation
function tochunk_pset(x, space::MPIMemorySpace; device=nothing, kwargs...)
    @assert space.comm == MPI.COMM_WORLD "$(space.comm) != $(MPI.COMM_WORLD)"
    local_rank = MPI.Comm_rank(space.comm)
    Mid = take_ref_id!()
    if local_rank != space.rank
        return MPIRef(space.comm, space.rank, 0, nothing, Mid)
    else
        return MPIRef(space.comm, space.rank, sizeof(x), poolset(x; device, kwargs...), Mid)
    end
end

const DEADLOCK_DETECT = TaskLocalValue{Bool}(()->true)
const DEADLOCK_WARN_PERIOD = TaskLocalValue{Float64}(()->10.0)
const DEADLOCK_TIMEOUT_PERIOD = TaskLocalValue{Float64}(()->60.0)
const RECV_WAITING = Base.Lockable(Dict{Tuple{MPI.Comm, Int, Int}, Base.Event}())

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

function supports_inplace_mpi(value)
    if value isa DenseArray && isbitstype(eltype(value))
        return true
    else
        return false
    end
end
function recv_yield!(buffer, comm, src, tag)
    rank = MPI.Comm_rank(comm)
    #Core.println("buffer recv: $buffer, type of buffer: $(typeof(buffer)), is in place? $(supports_inplace_mpi(buffer))")
    if !supports_inplace_mpi(buffer)
        return recv_yield(comm, src, tag), false
    end
    #Core.println("[rank $(MPI.Comm_rank(comm))][tag $tag] Starting recv! from [$src]")

    # Ensure no other receiver is waiting
    our_event = Base.Event()
    @label retry
    other_event = lock(RECV_WAITING) do waiting
        if haskey(waiting, (comm, src, tag))
            waiting[(comm, src, tag)]
        else
            waiting[(comm, src, tag)] = our_event
            nothing
        end
    end
    if other_event !== nothing
        #Core.println("[rank $(MPI.Comm_rank(comm))][tag $tag] Waiting for other receiver...")
        wait(other_event)
        @goto retry
    end

    buffer = recv_yield_inplace!(buffer, comm, rank, src, tag)

    lock(RECV_WAITING) do waiting
        delete!(waiting, (comm, src, tag))
        notify(our_event)
    end

    return buffer, true

end

function recv_yield(comm, src, tag)
    rank = MPI.Comm_rank(comm)
    #Core.println("[rank $(MPI.Comm_rank(comm))][tag $tag] Starting recv from [$src]")

    # Ensure no other receiver is waiting
    our_event = Base.Event()
    @label retry
    other_event = lock(RECV_WAITING) do waiting
        if haskey(waiting, (comm, src, tag))
            waiting[(comm, src, tag)]
        else
            waiting[(comm, src, tag)] = our_event
            nothing
        end
    end
    if other_event !== nothing
        #Core.println("[rank $(MPI.Comm_rank(comm))][tag $tag] Waiting for other receiver...")
        wait(other_event)
        @goto retry
    end
    #Core.println("[rank $(MPI.Comm_rank(comm))][tag $tag] Receiving...")

    type = nothing
    @label receive
    value = recv_yield_serialized(comm, rank, src, tag)
    if value isa InplaceInfo || value isa InplaceSparseInfo
        value = recv_yield_inplace(value, comm, rank, src, tag)
    end

    lock(RECV_WAITING) do waiting
        delete!(waiting, (comm, src, tag))
        notify(our_event)
    end
    return value
end

function recv_yield_inplace!(array, comm, my_rank, their_rank, tag)
    time_start = time_ns()
    detect = DEADLOCK_DETECT[]
    warn_period = round(UInt64, DEADLOCK_WARN_PERIOD[] * 1e9)
    timeout_period = round(UInt64, DEADLOCK_TIMEOUT_PERIOD[] * 1e9)

    while true
        (got, msg, stat) = MPI.Improbe(their_rank, tag, comm, MPI.Status)
        if got
            if MPI.Get_error(stat) != MPI.SUCCESS
                error("recv_yield failed with error $(MPI.Get_error(stat))")
            end
            count = MPI.Get_count(stat, UInt8)
            @assert count == sizeof(array) "recv_yield_inplace: expected $(sizeof(array)) bytes, got $count"
            buf = MPI.Buffer(array)
            req = MPI.Imrecv!(buf, msg)
            __wait_for_request(req, comm, my_rank, their_rank, tag, "recv_yield", "recv")
            return array
        end
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

function recv_yield_serialized(comm, my_rank, their_rank, tag)
    time_start = time_ns()
    detect = DEADLOCK_DETECT[]
    warn_period = round(UInt64, DEADLOCK_WARN_PERIOD[] * 1e9)
    timeout_period = round(UInt64, DEADLOCK_TIMEOUT_PERIOD[] * 1e9)

    while true
        (got, msg, stat) = MPI.Improbe(their_rank, tag, comm, MPI.Status)
        if got
            if MPI.Get_error(stat) != MPI.SUCCESS
                error("recv_yield failed with error $(MPI.Get_error(stat))")
            end
            count = MPI.Get_count(stat, UInt8)
            buf = Array{UInt8}(undef, count)
            req = MPI.Imrecv!(MPI.Buffer(buf), msg)
            __wait_for_request(req, comm, my_rank, their_rank, tag, "recv_yield", "recv")
            return MPI.deserialize(buf)
        end
        warn_period = mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, my_rank, tag, "recv", their_rank)
        yield()
    end
end

const SEEN_TAGS = Dict{Int32, Type}()
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
    #Core.println("[rank $(MPI.Comm_rank(comm))][tag $tag] Starting send to [$dest]: $(typeof(value)), is support inplace? $(supports_inplace_mpi(value))")
    if inplace && supports_inplace_mpi(value)
        send_yield_inplace(value, comm, rank, dest, tag)
    else
        send_yield_serialized(value, comm, rank, dest, tag)
    end
end

function send_yield_inplace(value, comm, my_rank, their_rank, tag)
    req = MPI.Isend(value, comm; dest=their_rank, tag)
    __wait_for_request(req, comm, my_rank, their_rank, tag, "send_yield", "send")
end

function send_yield_serialized(value, comm, my_rank, their_rank, tag)
    if value isa Array && isbitstype(eltype(value))
        send_yield_serialized(InplaceInfo(typeof(value), size(value)), comm, my_rank, their_rank, tag)
        send_yield_inplace(value, comm, my_rank, their_rank, tag)
    elseif value isa SparseMatrixCSC && isbitstype(eltype(value))
        send_yield_serialized(InplaceSparseInfo(typeof(value), value.m, value.n, length(value.colptr), length(value.rowval), length(value.nzval)), comm, my_rank, their_rank, tag)
        send_yield_inplace(value.colptr, comm, my_rank, their_rank, tag)
        send_yield_inplace(value.rowval, comm, my_rank, their_rank, tag)
        send_yield_inplace(value.nzval,  comm, my_rank, their_rank, tag)
    else
        req = MPI.isend(value, comm; dest=their_rank, tag)
        __wait_for_request(req, comm, my_rank, their_rank, tag, "send_yield", "send")
    end
end

function __wait_for_request(req, comm, my_rank, their_rank, tag, fn::String, kind::String)
    time_start = time_ns()
    detect = DEADLOCK_DETECT[]
    warn_period = round(UInt64, DEADLOCK_WARN_PERIOD[] * 1e9)
    timeout_period = round(UInt64, DEADLOCK_TIMEOUT_PERIOD[] * 1e9)
    while true
        finish, status = MPI.Test(req, MPI.Status)
        if finish
            if MPI.Get_error(status) != MPI.SUCCESS
                error("$fn failed with error $(MPI.Get_error(status))")
            end
            return
        end
        warn_period = mpi_deadlock_detect(detect, time_start, warn_period, timeout_period, my_rank, tag, kind, their_rank)
        yield()
    end
end

function bcast_send_yield(value, comm, root, tag)
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
        @warn "[rank $rank][tag $tag] Hit probable hang on $kind (dest: $srcdest)"
        return typemax(UInt64)
    end
    if detect && time_elapsed > timeout_period
        error("[rank $rank][tag $tag] Hit hang on $kind (dest: $srcdest)")
    end
    return warn_period
end

#discuss this with julian
WeakChunk(c::Chunk{T,H}) where {T,H<:MPIRef} = WeakChunk(c.handle.rank, c.handle.id.id, WeakRef(c))

function MemPool.poolget(ref::MPIRef; uniform::Bool=false)
    @assert uniform || ref.rank == MPI.Comm_rank(ref.comm) "MPIRef rank mismatch: $(ref.rank) != $(MPI.Comm_rank(ref.comm))"
    if uniform
        tag = to_tag(hash(ref.id, hash(:poolget)))
        if ref.rank == MPI.Comm_rank(ref.comm)
            value = poolget(ref.innerRef)
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
        tag = to_tag(hash(dep_mod, hash(to.handle.id, hash(from.handle.id, hash(:move!)))))
        @dagdebug nothing :mpi "[$local_rank][$tag] Moving from  $(from_space.rank)  to  $(to_space.rank)\n"
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
        tag = to_tag(hash(dep_mod, hash(to.handle.id, hash(from.handle.id, hash(:move!)))))
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
            send_yield(len, to_space.comm, to_space.rank, tag)
            send_yield!(copies, to_space.comm, to_space.rank, tag; check_seen=false)
            #send_yield(copies, to_space.comm, to_space.rank, tag)
        elseif local_rank == to_space.rank
            # Receive the spans
            len = recv_yield(from_space.comm, from_space.rank, tag)
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
        if data isa Chunk
            tag = to_tag(hash(data.handle.id))
            space = memory_space(data)
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
            elseif loc_rank == from_proc.rank
                value = poolget(data.handle)
                data_moved = move(from_proc.innerProc, to_proc.innerProc, value)
                Dagger.send_yield(data_moved, accel.comm, to_proc.rank, tag)
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
        else
            error("We shouldn't call f here, if we're not the destination rank")
            data_converted = f(move(from_proc, to_proc, data))
            return tochunk(data_converted, to_proc, to_space)
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

#FIXME:try to think of a better move! scheme
function execute!(proc::MPIProcessor, world::UInt64, f, args...; kwargs...)
    local_rank = MPI.Comm_rank(proc.comm)
    tag = to_tag(hash(sch_handle().thunk_id.id, hash(:execute!, UInt(0))))
    islocal = local_rank == proc.rank
    inplace_move = f === move!
    result = nothing
    if islocal || inplace_move
        result = execute!(proc.innerProc, world, f, args...; kwargs...)
    end
    if inplace_move
        # move! already handles communication
        space = memory_space(nothing, proc)::MPIMemorySpace
        return tochunk(nothing, proc, space)
    else
        # Handle communication ourselves
        if islocal
            T = typeof(result)
            space = memory_space(result, proc)::MPIMemorySpace
            T_space = (T, space.innerSpace)
            bcast_send_yield(T_space, proc.comm, proc.rank, tag)
            return tochunk(result, proc, space)
        else
            T, innerSpace = recv_yield(proc.comm, proc.rank, tag)
            space = MPIMemorySpace(innerSpace, proc.comm, proc.rank)
            return tochunk(nothing, proc, space; type=T)
        end
    end
end

accelerate!(::Val{:mpi}) = accelerate!(MPIAcceleration())

function initialize_acceleration!(a::MPIAcceleration)
    if !MPI.Initialized()
        MPI.Init(;threadlevel=:multiple)
    end
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
