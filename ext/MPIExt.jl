module MPIExt

import Dagger
import Dagger: Thunk, Chunk, Processor, MemorySpace
import Dagger: @dagdebug, @opcounter

# Types and generic functions from Dagger that this extension either
# extends with new methods (for MPI-specific types) or calls directly.
import Dagger:
    AbstractAliasing, accelerate!, accel_matches_proc, aliased_object!,
    AliasedObjectCache, AliasedObjectCacheStore, aliasing, aliasing_unwrapped,
    bind_moved_argument,
    chunktype, ChunkView, check_uniform, check_uniformity!, CHECK_UNIFORMITY,
    cleanup_tasks_accel!, constrain, CPURAMMemorySpace,
    current_acceleration, CyclicProcGrid, datasize, default_enabled,
    default_memory_space, default_processor, default_procgrid,
    finalize_acceleration!, execute!, fetch_handle, fire_order_key, get_parent,
    get_processors, gpu_kernel_backend, gpu_memory_kind,
    initialize_acceleration!, InvalidScope, ipc_copyto!, ipc_eligible,
    ipc_export, ipc_materialize, IPC_MIN_BYTES, ipc_release!, is_local,
    istask, LockedObject, memory_space, memory_spaces, MemorySpan,
    memory_spans, move, move!, move_rewrap, move_rewrap_build, DATADEPS_THUNK_ID,
    mpi_device_direct, mpi_device_sync, mpi_library_gpu_aware, mpi_remap_space,
    myid,
    move_rewrap_child_types, move_rewrap_header_mode, move_rewrap_parts,
    move_rewrap_result_type, move_type, multi_span_gather!,
    multi_span_scatter!, next_id, NoAliasing, Options, OSProc,
    post_stage_array_chunks!, processors, ProcessScope, proc_in_scope,
    remotecall_endpoint_toplevel, RemotePtr, root_worker_id, same_node,
    schedule_argument_move, sch_handle, select_processors_uniform!, set_key_stored!,
    set_stored!, short_name, span_len, stage_acquire!, stage_release!,
    stage_to_host!, system_uuid, task_processor_preference, ThreadProc, to_tag,
    tochunk, tochunk_pset, uniform_execution, UnknownAliasing, unwrap,
    unwrap_weak_checked, value_memory_space, WeakChunk, with_context!,
    _with_default_acceleration

import MemPool
import MemPool: DRef, poolget, poolset

# `Base.ScopedValues` only exists on Julia 1.11+; on 1.10 (LTS) fall back to the
# `ScopedValues` package (a Dagger dependency), matching `src/Dagger.jl`.
if !isdefined(Base, :ScopedValues)
    import ScopedValues: ScopedValue, @with, with
else
    import Base.ScopedValues: ScopedValue, @with, with
end

import TaskLocalValues: TaskLocalValue

import SparseArrays: SparseMatrixCSC

import Random
import Serialization

using MPI

function check_uniform(value::Integer, original=value)
    CHECK_UNIFORMITY[] && uniform_execution() || return true
    comm = MPI.COMM_WORLD
    rank = MPI.Comm_rank(comm)
    matched = compare_all(value, comm)
    if !matched
        if rank == 0
            Core.print("[$rank] Found non-uniform value!\n")
        end
        Core.print("[$rank] value=$value, original=$original\n")
        throw(ArgumentError("Non-uniform value"))
    end
    MPI.Barrier(comm)
    return matched
end

# MPI tag reserved for `compare_all` / `check_uniform` P2P only. It must not
# collide with ordinary Dagger message tags on the same `comm` (which come from
# `to_tag` = thunk/ref IDs, and which `remotecall_endpoint_toplevel` broadcast
# metadata uses `0` for), or ranks steal each other's messages and hang until
# `mpi_deadlock_detect` fires.
#
# Use the top of the MPI tag space (`MPI.tag_ub()`): `take_ref_id!` asserts every
# ordinary tag is strictly `< MPI.tag_ub()`, so this value provably cannot
# collide with one. `tag_ub()` is a valid tag (the MPI standard allows tags in
# `0:MPI_TAG_UB`) and is a library constant identical on every rank, which the
# cross-rank uniformity sends/receives require. (Computed lazily rather than as a
# load-time `const`, since MPI is not yet initialized at module load.)
compare_all_mpi_tag() = UInt32(MPI.tag_ub())

function compare_all(value, comm)
    rank = MPI.Comm_rank(comm)
    size = MPI.Comm_size(comm)
    tag = compare_all_mpi_tag()
    for i in 0:(size-1)
        if i != rank
            send_yield(value, comm, i, tag)
        end
    end
    match = true
    for i in 0:(size-1)
        if i != rank
            other_value = recv_yield(comm, i, tag)
            if value != other_value
                match = false
            end
        end
    end
    return match
end

"""
    sync_mpi_rng!(comm)

Broadcast a seed from rank 0 and `Random.seed!` it on every rank so default
RNGs are coherent under SPMD MPI acceleration.
"""
function sync_mpi_rng!(comm::MPI.Comm)
    seed = Ref{UInt64}(0)
    if MPI.Comm_rank(comm) == 0
        seed[] = rand(Random.RandomDevice(), UInt64)
    end
    MPI.Bcast!(seed, 0, comm)
    Random.seed!(seed[])
    return seed[]
end

"""
    check_mpi_rng_coherent!(comm)

Probe `rand(UInt64)` on every rank and throw if any rank differs. Always runs
(independent of `CHECK_UNIFORMITY[]`) because coherent RNGs are required for
SPMD correctness after MPI acceleration init.
"""
function check_mpi_rng_coherent!(comm::MPI.Comm)
    probe = rand(UInt64)
    if !compare_all(probe, comm)
        throw(ArgumentError("Non-uniform RNG under MPI acceleration"))
    end
    return true
end

struct MPIAcceleration <: Dagger.Acceleration
    comm::MPI.Comm
end
MPIAcceleration() = MPIAcceleration(MPI.COMM_WORLD)

function aliasing(accel::MPIAcceleration, x::Chunk, T)
    handle = x.handle::MPIRef
    @assert accel.comm == handle.comm "MPIAcceleration comm mismatch"
    tag = to_tag()
    check_uniform(tag)
    rank = MPI.Comm_rank(accel.comm)
    if handle.rank == rank
        ainfo = _with_default_acceleration() do
            aliasing(x, T)
        end
        ainfo = mpi_remap_ainfo(ainfo, handle.rank)
        @opcounter :aliasing_bcast_send_yield
        ainfo = bcast_yield(accel.comm, handle.rank, tag, ainfo)
    else
        ainfo = bcast_yield(accel.comm, handle.rank, tag)
    end
    check_uniform(ainfo)
    return ainfo
end

default_processor(accel::MPIAcceleration) = MPIOSProc(accel.comm, 0)
default_processor(accel::MPIAcceleration, x) = MPIOSProc(accel.comm, 0)
default_processor(accel::MPIAcceleration, x::Chunk) = MPIOSProc(x.handle.comm, x.handle.rank)
default_processor(accel::MPIAcceleration, x::Function) = MPIOSProc(accel.comm, MPI.Comm_rank(accel.comm))
default_processor(accel::MPIAcceleration, T::Type) = MPIOSProc(accel.comm, MPI.Comm_rank(accel.comm))
uniform_execution(accel::MPIAcceleration) = true

# Children / uniform-processor caches are shared across reusable scheduler
# tasks. Protect both with one lock; never mutate them unlocked.
const MPI_PROC_CACHE_LOCK = Threads.ReentrantLock()
const MPIClusterProcChildren = Dict{MPI.Comm, Set{Processor}}()
const MPIUniformProcsCache = Dict{MPI.Comm, Vector{Processor}}()

struct MPIClusterProc <: Processor
    comm::MPI.Comm
    function MPIClusterProc(comm::MPI.Comm)
        ensure_children!(comm)
        return new(comm)
    end
end

Dagger.Sch.init_proc(state, proc::MPIClusterProc, log_sink) = Dagger.Sch.init_proc(state, MPIOSProc(proc.comm), log_sink)

MPIClusterProc() = MPIClusterProc(MPI.COMM_WORLD)

# Populate `MPIClusterProcChildren[comm]` once. Safe on the scheduler hot path;
# does not clear the uniform-processor cache on every lookup.
function ensure_children!(comm::MPI.Comm)
    # `@lock` (not `lock(...) do`) so the early `return` on a cache hit actually
    # returns from `ensure_children!`; inside a `do` closure it would only
    # return from the closure and we'd needlessly re-fetch the children below.
    @lock MPI_PROC_CACHE_LOCK begin
        haskey(MPIClusterProcChildren, comm) && return MPIClusterProcChildren[comm]
    end
    # Fetch outside the lock: `get_processors(OSProc())` may remotecall / take
    # OSPROC_PROCESSOR_CACHE and must not run under MPI_PROC_CACHE_LOCK.
    children = get_processors(OSProc())
    @lock MPI_PROC_CACHE_LOCK begin
        if !haskey(MPIClusterProcChildren, comm)
            MPIClusterProcChildren[comm] = children
            # Invalidate only this communicator's uniform listing
            delete!(MPIUniformProcsCache, comm)
        end
        return MPIClusterProcChildren[comm]
    end
end

# Force-refresh children for `comm` (e.g. after processor callbacks change).
function populate_children!(comm::MPI.Comm)
    children = get_processors(OSProc())
    lock(MPI_PROC_CACHE_LOCK) do
        MPIClusterProcChildren[comm] = children
        delete!(MPIUniformProcsCache, comm)
    end
    return children
end

function uniform_mpi_processors(accel::MPIAcceleration)
    # Ensure children first so the `get!` callback never mutates this Dict
    # mid-insertion (via MPIClusterProc / ensure_children!).
    ensure_children!(accel.comm)
    lock(MPI_PROC_CACHE_LOCK) do
        get!(MPIUniformProcsCache, accel.comm) do
            children = MPIClusterProcChildren[accel.comm]
            procs = Processor[]
            for i in 0:(MPI.Comm_size(accel.comm)-1)
                for innerProc in children
                    push!(procs, MPIProcessor(innerProc, accel.comm, i))
                end
            end
            # Default placement only targets default-enabled (CPU) processors; GPU
            # processors are opt-in via explicit scopes
            filter!(default_enabled, procs)
            select_processors_uniform!(procs, accel)
            procs
        end
    end
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

struct MPIProcessScope <: Dagger.AbstractScope
    comm::MPI.Comm
    rank::Int
end

Base.isless(::MPIProcessScope, ::MPIProcessScope) = false
Base.isless(::MPIProcessScope, ::Dagger.ProcessScope) = true
Base.isless(::MPIProcessScope, ::Dagger.NodeScope) = true
Base.isless(::MPIProcessScope, ::Dagger.UnionScope) = true
Base.isless(::MPIProcessScope, ::Dagger.TaintScope) = true
Base.isless(::MPIProcessScope, ::Dagger.AnyScope) = true
Base.isless(::Dagger.ProcessScope, ::MPIProcessScope) = false
Base.isless(::Dagger.NodeScope, ::MPIProcessScope) = false
constrain(x::MPIProcessScope, y::MPIProcessScope) =
    x == y ? y : InvalidScope(x, y)
# Under MPI, every processor reports root_worker_id == myid(), so a local
# ProcessScope intersects any MPIProcessScope as the more specific rank scope
constrain(x::Dagger.ProcessScope, y::MPIProcessScope) =
    x.wid == myid() ? y : InvalidScope(x, y)
constrain(x::Dagger.NodeScope, y::MPIProcessScope) =
    x == Dagger.NodeScope() ? y : Dagger.InvalidScope(x, y)

Base.isless(::Dagger.ExactScope, ::MPIProcessScope) = true
constrain(x::MPIProcessScope, y::Dagger.ExactScope) =
    (y.processor isa MPIProcessor &&
     y.processor.comm == x.comm &&
     y.processor.rank == x.rank) ? y : Dagger.InvalidScope(x, y)

function enclosing_scope(proc::MPIOSProc)
    return MPIProcessScope(proc.comm, proc.rank)
end

function Dagger.to_scope(::Val{:mpi_rank}, sc::NamedTuple)
    if sc.mpi_rank == Colon()
        return Dagger.to_scope(Val{:mpi_ranks}(), merge(sc, (;mpi_ranks=Colon())))
    else
        @assert sc.mpi_rank isa Integer "Expected a single MPI rank for :mpi_rank, got $(sc.mpi_rank)\nConsider using :mpi_ranks instead."
        return Dagger.to_scope(Val{:mpi_ranks}(), merge(sc, (;mpi_ranks=[sc.mpi_rank])))
    end
end
Dagger.scope_key_precedence(::Val{:mpi_rank}) = 2
function Dagger.to_scope(::Val{:mpi_ranks}, sc::NamedTuple)
    comm = get(sc, :mpi_comm, MPI.COMM_WORLD)
    # `:mpi_ranks` is `Colon()` to select every rank, otherwise an iterable of
    # rank indices. (`mpi_rank=r` funnels through here as `mpi_ranks=[r]`.)
    if sc.mpi_ranks == Colon()
        ranks = 0:(MPI.Comm_size(comm)-1)
    else
        ranks = sc.mpi_ranks
    end
    scopes = Dagger.ExactScope[]
    for rank in ranks
        procs = Dagger.get_processors(MPIOSProc(comm, rank))
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
           # Compare the logical identity (kind + ordinal), not the raw struct:
           # device handles/UUIDs of GPU processors are rank-local
           check_uniform(hash(short_name(proc.innerProc)), original)
end

Dagger.iscompatible_func(::MPIProcessor, opts, ::Any) = true
Dagger.iscompatible_arg(::MPIProcessor, opts, ::Any) = true

# Under uniform (SPMD) execution every rank must run each task on the rank it
# was deterministically scheduled to; local work stealing would diverge
Dagger.Sch.stealing_permitted(::MPIProcessor) = false

default_enabled(proc::MPIProcessor) = default_enabled(proc.innerProc)

root_worker_id(proc::MPIProcessor) = myid()
root_worker_id(proc::MPIOSProc) = myid()
root_worker_id(proc::MPIClusterProc) = myid()

get_parent(proc::MPIClusterProc) = proc
get_parent(proc::MPIOSProc) = MPIClusterProc(proc.comm)
get_parent(proc::MPIProcessor) = MPIOSProc(proc.comm, proc.rank)

# Rank-deterministic keys: default fire_order_key uses root_worker_id, which
# is always myid() for MPI processors and is not comparable across ranks.
function fire_order_key(proc::MPIClusterProc)
    wid = root_worker_id(proc)
    return (wid, wid)
end
fire_order_key(proc::MPIOSProc) = (proc.rank, 0)
fire_order_key(proc::MPIProcessor) = (get_parent(proc).rank, proc.rank)

function task_processor_preference(accel::MPIAcceleration, task::Thunk, state)
    # N.B. inputs[1] is the function, whose chunk (when wrapped) is deliberately
    # owned by the local rank on every rank — only data arguments carry
    # rank-uniform ownership
    for arg in task.inputs[2:end]
        arg_value = unwrap_weak_checked(Dagger.value(arg))
        arg_chunk = if istask(arg_value)
            Dagger.Sch.load_result(state, arg_value)
        elseif arg_value isa Chunk
            arg_value
        else
            nothing
        end
        if arg_chunk isa Chunk && arg_chunk.handle isa MPIRef
            return arg_chunk.handle.rank
        end
    end
    return nothing
end

short_name(proc::MPIProcessor) = "(MPI: $(proc.rank), $(short_name(proc.innerProc)))"

function get_processors(mosProc::MPIOSProc)
    children = ensure_children!(mosProc.comm)
    mpiProcs = Set{Processor}()
    for proc in children
        push!(mpiProcs, MPIProcessor(proc, mosProc.comm, mosProc.rank))
    end
    return mpiProcs
end

#TODO: non-uniform ranking through MPI groups
#TODO: use a lazy iterator
function get_processors(proc::MPIClusterProc)
    children = ensure_children!(proc.comm)
    result = Set{Processor}()
    for i in 0:(MPI.Comm_size(proc.comm)-1)
        for innerProc in children
            push!(result, MPIProcessor(innerProc, proc.comm, i))
        end
    end
    return result
end

struct MPIMemorySpace{S<:MemorySpace} <: MemorySpace
    innerSpace::S
    comm::MPI.Comm
    rank::Int
end

function Base.:(==)(a::MPIMemorySpace, b::MPIMemorySpace)
    return a.innerSpace == b.innerSpace &&
           a.comm == b.comm &&
           a.rank == b.rank
end
Base.hash(space::MPIMemorySpace, h::UInt=UInt(0)) =
    hash(space.innerSpace, hash(space.comm.val, hash(space.rank, hash(MPIMemorySpace, h))))

function check_uniform(space::MPIMemorySpace, original=space)
    return check_uniform(space.rank, original) &&
           # Compare the logical identity (kind + ordinal), not the raw struct:
           # device handles/UUIDs of GPU memory spaces are rank-local
           check_uniform(hash(short_name(space.innerSpace)), original)
end

short_name(space::MPIMemorySpace) = "(MPI: $(space.rank), $(short_name(space.innerSpace)))"

default_processor(space::MPIMemorySpace) = MPIOSProc(space.comm, space.rank)
default_memory_space(accel::MPIAcceleration) = MPIMemorySpace(CPURAMMemorySpace(myid()), accel.comm, 0)

default_memory_space(accel::MPIAcceleration, x) = MPIMemorySpace(value_memory_space(x), accel.comm, 0)
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
    rawProc = Processor[]
    for innerProc in processors(memSpace.innerSpace)
        push!(rawProc, MPIProcessor(innerProc, memSpace.comm, memSpace.rank))
    end
    # Return a deterministically-ordered vector rather than a `Set`: callers pick
    # the owner processor with `first(processors(space))`, and that choice must be
    # identical on every rank for SPMD uniformity. `Set` iterates in hash order,
    # which is not guaranteed to agree across separately-launched ranks (an
    # `MPIProcessor` embeds an identity-hashed `MPI.Comm`, so its hash — and thus
    # the set's iteration order — can differ per process). Sort by the same
    # rank-uniform identity (`short_name` of the inner processor) used by
    # `check_uniform`.
    sort!(rawProc; by=p->short_name(p.innerProc))
    return rawProc
end

struct MPIRefID
    tid::UInt32
    generic::Bool
    id::UInt32
    function MPIRefID(tid, generic, id)
        @assert tid > 0 || generic "Invalid MPIRefID: tid=$tid, generic=$generic, id=$id"
        return new(tid, generic, id)
    end
end
Base.hash(id::MPIRefID, h::UInt=UInt(0)) =
    hash(id.tid, hash(id.generic, hash(id.id, hash(MPIRefID, h))))

function check_uniform(ref::MPIRefID, original=ref)
    return check_uniform(ref.tid, original) &&
           check_uniform(ref.generic, original) &&
           check_uniform(ref.id, original)
end

function to_tag()
    if Dagger.in_task()
        # Tag is already assigned
        opts = Dagger.get_tls().task_spec.options
        tag = opts.tag
        if tag !== nothing
            return tag
        end
        # Tasks spawned outside the datadeps queue may not have a tag option;
        # derive a uniform tag from the thunk ID instead
        return to_tag(take_ref_id!())
    end

    # Generate a tag based on the TID
    @assert !Dagger.Sch.SCHED_MOVE[] "We should not create a tag during Sch move"
    return to_tag(take_ref_id!())
end
to_tag(id::MPIRefID) = id.generic ? id.id : id.tid

# DATADEPS_THUNK_ID (current task uid for deterministic MPIRefID generation) is owned by
# core Dagger (src/datadeps/queue.jl) and imported below; core datadeps sets it
# and this extension reads it.
# Private internal value for tracking TID-based ID generations
# N.B. Sub-counters are keyed by TID: every rank allocates IDs for a given TID
# in the same deterministic order (sequential SPMD planning, or the fixed
# tochunk sequence within a thunk), so the resulting MPIRefIDs are uniform.
const _MPIREF_TID = LockedObject(Dict{Int,Int}())

mutable struct MPIRef
    comm::MPI.Comm
    rank::Int
    size::Int
    innerRef::Union{DRef, Nothing}
    id::MPIRefID
end
Base.hash(ref::MPIRef, h::UInt=UInt(0)) = hash(ref.rank, hash(ref.id, hash(MPIRef, h)))
root_worker_id(ref::MPIRef) = myid()

function check_uniform(ref::MPIRef, original=ref)
    return check_uniform(ref.rank, original) &&
           check_uniform(ref.id, original)
end

function unwrap(handle::MPIRef)
    @assert handle.rank == MPI.Comm_rank(handle.comm) "MPIRef $handle is not owned by this rank: $(handle.rank) != $(MPI.Comm_rank(handle.comm))"
    return unwrap(handle.innerRef)
end

# An MPIRef's data is only inspectable on the rank that owns it; other ranks
# must not attempt to `unwrap`/`aliasing` it during (rank-uniform) planning.
Dagger.aliasing_available(x::Chunk{<:Any,<:MPIRef}) =
    x.handle.rank == MPI.Comm_rank(x.handle.comm)

to_tag(ref::MPIRef) = to_tag(ref.id)

move(from_proc::Processor, to_proc::Processor, x::MPIRef) =
    move(from_proc, to_proc, poolget(x; uniform=uniform_execution()))

# Read the size carried on the MPIRef itself: `innerRef` only exists on the
# owning rank, so querying it there would report 0 everywhere else. Non-owning
# ranks hold a placeholder (size 0) since they never see the value at
# construction; this residual per-rank difference is harmless because `datasize`
# only feeds cost-based placement, which uniform (MPI) execution overrides via
# `select_processors_uniform!` (see estimate_task_costs! / schedule!).
datasize(x::MPIRef) = x.size

next_ref_sub_id!(tid) =
    lock(_MPIREF_TID) do counters
        counters[tid] = get(counters, tid, 0) + 1
    end

# Forget completed tasks' ref-ID sub-counters: a finished task generates no
# further IDs, and without cleanup the table grows with every task for the
# whole session. Reclaim at wait_all (datadeps region end) after execution —
# planning (DATADEPS_THUNK_ID=>uid) and in-task tochunk share the same TID key (eager
# uid == thunk id), so reclaiming between planning and execution would collide
# on sub-ids. Rank-local and timing-independent: only handed-out values matter
# for uniformity, and TIDs never recur (monotonic shared counter). Deferral
# does not exhaust MPI tags (tags track monotonic TIDs / hashed (tid,sub)).
mpi_cleanup_tid(tid::Integer) =
    lock(_MPIREF_TID) do counters
        delete!(counters, Int(tid))
    end

function mpi_cleanup_tids!(tids)
    lock(_MPIREF_TID) do counters
        for tid in tids
            delete!(counters, Int(tid))
        end
    end
end

cleanup_tasks_accel!(::MPIAcceleration, tasks) =
    mpi_cleanup_tids!(Iterators.map(t -> Int(t.uid), tasks))

function schedule_argument_move(::MPIAcceleration, thunk_id::Integer, f::Function)
    with(DATADEPS_THUNK_ID => Int64(thunk_id)) do
        f()
    end
    return nothing
end

function take_ref_id!()
    tid = 0
    generic = 0
    id = 0
    if Dagger.in_task()
        tid = sch_handle().thunk_id.id
        id = next_ref_sub_id!(tid)
    elseif DATADEPS_THUNK_ID[] != 0
        tid = DATADEPS_THUNK_ID[]
        id = next_ref_sub_id!(tid)
    else
        if current_task() !== Base.roottask
            throw(ConcurrencyViolationError("Attempted to generate generic MPIRefID in a multi-threaded context"))
        end
        generic = true
        id = next_id() # Abuse the TID counter for generic IDs
        check_uniform(id)
    end
    # Only the tag-relevant component must fit in an MPI tag
    # N.B. Skip when MPI is uninitialized (e.g. pure-Distributed callers)
    @assert !MPI.Initialized() || (generic == true ? id : tid) < MPI.tag_ub()
    return MPIRefID(tid, generic, id)
end

#TODO: partitioned scheduling with comm bifurcation
function tochunk_pset(x, space::MPIMemorySpace; device=nothing, force_nonlocal=false, kwargs...)
    @assert space.comm == MPI.COMM_WORLD "$(space.comm) != $(MPI.COMM_WORLD)"
    local_rank = MPI.Comm_rank(space.comm)
    Mid = take_ref_id!()
    if local_rank != space.rank || force_nonlocal
        return MPIRef(space.comm, space.rank, 0, nothing, Mid)
    else
        # type= is for Chunk metadata only; MemPool.poolset does not accept it
        pset_kw = (; (k => v for (k, v) in pairs(kwargs) if k !== :type)...)
        return MPIRef(space.comm, space.rank, sizeof(x), poolset(x; device, pset_kw...), Mid)
    end
end

const DEADLOCK_DETECT = TaskLocalValue{Bool}(()->true)
const DEADLOCK_WARN_PERIOD = TaskLocalValue{Float64}(()->10.0)
const DEADLOCK_TIMEOUT_PERIOD = TaskLocalValue{Float64}(()->120.0)
const RECV_WAITING = LockedObject(Dict{Tuple{MPI.Comm, Int, Int}, Base.Event}())

# Envelope for the out-of-place raw-bytes MPI path: serialize a small
# descriptor, then send contiguous bitstype buffers. Types register via
# inplace_mpi_parts / inplace_mpi_alloc / inplace_mpi_build (same style as
# move_rewrap_parts / move_rewrap_build).
struct InplaceInfo
    type::DataType
    header  # Any MPI-serializable reconstruction metadata
end

# Extensibility defaults: unsupported → full Julia serialize
inplace_mpi_parts(x) = nothing
# -> (parts::Tuple, header) where each part is a DenseArray with bitstype eltype
inplace_mpi_alloc(::Type{T}, header) where T =
    error("inplace_mpi_alloc not defined for $T")
inplace_mpi_build(::Type{T}, parts, header) where T =
    error("inplace_mpi_build not defined for $T")

# DenseArray: single contiguous buffer + shape header
function inplace_mpi_parts(A::DenseArray)
    isbitstype(eltype(A)) || return nothing
    return ((A,), size(A))
end
function inplace_mpi_alloc(::Type{T}, shape::Tuple) where {T<:DenseArray}
    # Always materialize on the host: the sender host-stages device arrays,
    # and callers convert to the destination space's native storage
    return (Array{eltype(T)}(undef, shape),)
end
inplace_mpi_build(::Type{<:DenseArray}, (A,), ::Tuple) = A

# SparseMatrixCSC: three vectors + (m, n, lengths) header; preserve Ti
function inplace_mpi_parts(S::SparseMatrixCSC)
    isbitstype(eltype(S)) || return nothing
    return ((S.colptr, S.rowval, S.nzval),
            (S.m, S.n, length(S.colptr), length(S.rowval), length(S.nzval)))
end
function inplace_mpi_alloc(::Type{T}, header::Tuple) where {T<:SparseMatrixCSC}
    Tv, Ti = eltype(T), T.parameters[2]
    _, _, ncolptr, nrowval, nnzval = header
    return (Vector{Ti}(undef, ncolptr),
            Vector{Ti}(undef, nrowval),
            Vector{Tv}(undef, nnzval))
end
function inplace_mpi_build(::Type{T}, (colptr, rowval, nzval), header::Tuple) where {T<:SparseMatrixCSC}
    m, n, _, _, _ = header
    Tv, Ti = eltype(T), T.parameters[2]
    return SparseMatrixCSC{Tv,Ti}(m, n, colptr, rowval, nzval)
end

function supports_inplace_mpi(value)
    if value isa DenseArray && isbitstype(eltype(value))
        return true
    else
        return false
    end
end

# Whether the loaded MPI library is known to support device buffers of `kind`
# (GPU extensions cannot depend on MPI, so they query through this).
# `mpi_device_direct`/`mpi_device_sync`/`mpi_remap_space` defaults live in core
# Dagger (src/gpu.jl); this method refines `mpi_library_gpu_aware`.
function mpi_library_gpu_aware(kind::Symbol)
    MPI.Initialized() || return false
    kind === :CUDA && return MPI.has_cuda()
    kind === :ROC && return MPI.has_rocm()
    # MPI.jl has no has_ze/has_level_zero; Intel GPU-aware MPI is opt-in via
    # Intel MPI + I_MPI_OFFLOAD, or JULIA_MPI_HAS_ONEAPI / DAGGER_MPI_GPU_DIRECT
    if kind === :oneAPI
        env = get(ENV, "JULIA_MPI_HAS_ONEAPI", "")
        !isempty(env) && return something(tryparse(Bool, env), false)
        offload = get(ENV, "I_MPI_OFFLOAD", "")
        isempty(offload) && return false
        something(tryparse(Int, offload), 0) <= 0 && return false
        impl = try
            MPI.MPI_LIBRARY
        catch
            ""
        end
        return occursin(r"(?i)intel|impi", String(impl))
    end
    return false
end

### Same-node device IPC fast path (MPI handle transport)
# Eligibility and export/import live in gpu.jl / GPU extensions. Here we only
# exchange the small handle over MPI P2P and wait for an ack so the sender can
# release its staging allocation. Host staging for the non-IPC path uses
# STAGE_POOLS / stage_* from gpu.jl.

# The sender keeps its staging alive until the receiver acknowledges the
# device-to-device copy; the ack wait polls via Improbe+yield (recv_yield)
function mpi_ipc_send(value, comm, dest, tag)
    info, token = ipc_export(value)
    try
        send_yield(info, comm, dest, tag)
        ack = recv_yield(comm, dest, tag)
        @assert ack === true "Unexpected IPC ack: $(repr(ack))"
    finally
        ipc_release!(token)
    end
    return
end
function mpi_ipc_recv!(dest, comm, src, tag)
    info = recv_yield(comm, src, tag)
    ipc_copyto!(dest, info)
    send_yield(true, comm, src, tag)
    return dest
end
function mpi_ipc_recv_materialize(comm, src, tag)
    info = recv_yield(comm, src, tag)
    value = ipc_materialize(info)
    send_yield(true, comm, src, tag)
    return value
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
    if value isa InplaceInfo
        value = recv_yield_inplace(value, comm, rank, src, tag)
    end

    lock(RECV_WAITING) do waiting
        delete!(waiting, (comm, src, tag))
        notify(our_event)
    end
    return value
end

# Device-resident dense array: receive directly when the MPI library is
# GPU-aware, otherwise through a host staging buffer
function recv_yield_inplace!(array::DenseArray, comm, my_rank, their_rank, tag)
    if mpi_device_direct(array)
        mpi_device_sync(array)
        return _recv_yield_inplace_raw!(array, comm, my_rank, their_rank, tag)
    end
    kind = gpu_memory_kind(array)
    buf = stage_acquire!(kind, sizeof(array))
    GC.@preserve buf begin
        host = unsafe_wrap(Array, Ptr{eltype(array)}(pointer(buf)), size(array))
        _recv_yield_inplace_raw!(host, comm, my_rank, their_rank, tag)
        with_context!(memory_space(array))
        copyto!(array, host)
    end
    # The upload runs on this task's stream; consumers run on other
    # tasks/streams, so it must be visible before we hand the buffer over
    mpi_device_sync(array)
    stage_release!(kind, buf)
    return array
end
recv_yield_inplace!(array::Array, comm, my_rank, their_rank, tag) =
    _recv_yield_inplace_raw!(array, comm, my_rank, their_rank, tag)
function _recv_yield_inplace_raw!(array, comm, my_rank, their_rank, tag)
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
    parts = inplace_mpi_alloc(T, _value.header)
    parts = map(p -> recv_yield_inplace!(p, comm, my_rank, their_rank, tag), parts)
    return inplace_mpi_build(T, parts, _value.header)
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

send_yield!(value, comm, dest, tag) =
    _send_yield(value, comm, dest, tag; inplace=true)
send_yield(value, comm, dest, tag) =
    _send_yield(value, comm, dest, tag; inplace=false)
function _send_yield(value, comm, dest, tag; inplace::Bool)
    rank = MPI.Comm_rank(comm)

    #Core.println("[rank $(MPI.Comm_rank(comm))][tag $tag] Starting send to [$dest]: $(typeof(value)), is support inplace? $(supports_inplace_mpi(value))")
    if inplace && supports_inplace_mpi(value)
        if value isa Array
            send_yield_inplace(value, comm, rank, dest, tag)
        elseif mpi_device_direct(value)
            # GPU-aware MPI: hand the device buffer to MPI directly
            mpi_device_sync(value)
            send_yield_inplace(value, comm, rank, dest, tag)
        else
            # Device-resident dense array: stage through a pooled (pinned)
            # host buffer so the raw-bytes protocol works without GPU-aware
            # MPI. Producers may have enqueued work on other streams, so sync
            # before reading.
            mpi_device_sync(value)
            host, buf, kind = stage_to_host!(value)
            GC.@preserve buf begin
                send_yield_inplace(host, comm, rank, dest, tag)
            end
            stage_release!(kind, buf)
        end
    else
        send_yield_serialized(value, comm, rank, dest, tag)
    end
end

function send_yield_inplace(value, comm, my_rank, their_rank, tag)
    @opcounter :send_yield_inplace
    req = MPI.Isend(value, comm; dest=their_rank, tag)
    __wait_for_request(req, comm, my_rank, their_rank, tag, "send_yield", "send")
end

# Send one contiguous part, staging device arrays through host when needed
function send_yield_inplace_part(part, comm, my_rank, their_rank, tag)
    if part isa Array
        send_yield_inplace(part, comm, my_rank, their_rank, tag)
    elseif part isa DenseArray
        # Device-resident dense array: send bytes directly under GPU-aware
        # MPI, or host-staged otherwise (receiver materializes on the host)
        mpi_device_sync(part)
        if mpi_device_direct(part)
            send_yield_inplace(part, comm, my_rank, their_rank, tag)
        else
            host, buf, kind = stage_to_host!(part)
            GC.@preserve buf begin
                send_yield_inplace(host, comm, my_rank, their_rank, tag)
            end
            stage_release!(kind, buf)
        end
    else
        send_yield_inplace(part, comm, my_rank, their_rank, tag)
    end
end

function send_yield_serialized(value, comm, my_rank, their_rank, tag)
    @opcounter :send_yield_serialized
    parts_hdr = inplace_mpi_parts(value)
    if parts_hdr !== nothing
        parts, header = parts_hdr
        send_yield_serialized(InplaceInfo(typeof(value), header), comm, my_rank, their_rank, tag)
        for part in parts
            send_yield_inplace_part(part, comm, my_rank, their_rank, tag)
        end
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
    @opcounter :bcast_send_yield
    sz = MPI.Comm_size(comm)
    rank = MPI.Comm_rank(comm)
    for other_rank in 0:(sz-1)
        rank == other_rank && continue
        send_yield(value, comm, other_rank, tag)
    end
end

# Binomial-tree broadcast built from the tagged nonblocking P2P primitives
# (never an MPI collective). Every rank calls this at the same logical point:
# the root passes the value, the others receive from their tree parent,
# forward to their tree children, and return the received value. The tree is
# a pure function of (rank, root, comm size), hence rank-uniform; O(log P)
# latency instead of the root serially sending to P-1 peers.
function bcast_yield(comm, root::Integer, tag, value=nothing)
    @opcounter :bcast_yield
    sz = MPI.Comm_size(comm)
    sz == 1 && return value
    rank = MPI.Comm_rank(comm)
    vrank = mod(rank - root, sz)
    mask = 1
    while mask < sz
        if vrank & mask != 0
            value = recv_yield(comm, mod(rank - mask, sz), tag)
            break
        end
        mask <<= 1
    end
    mask >>= 1
    while mask > 0
        if vrank + mask < sz
            send_yield(value, comm, mod(rank + mask, sz), tag)
        end
        mask >>= 1
    end
    return value
end

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

# ---------------------------------------------------------------------------
# Dispatch-decoupled binomial-tree broadcast for per-task metadata
#
# `bcast_yield` (above) is a correct O(log P) tree, but when it runs INSIDE a
# task's syncdep-gated `execute!` coroutine an intermediate rank cannot forward
# a broadcast until the scheduler dispatches that same task on it. With many
# per-task metadata broadcasts overlapping (4 ranks × many chunks) that
# "forward only once I'm dispatched" gate closes a cross-rank wait cycle — the
# hang seen on Julia 1.10/1.11.
#
# A non-owner needs none of the task's data to relay/consume the tiny status/
# type payload, so drive the tree from an always-running per-rank relay that
# forwards ON MESSAGE ARRIVAL (independent of task dispatch) over a dedicated
# duplicated `bcast_comm`, delivering payloads to per-tag slots. The root just
# fires to its children and returns; non-roots yield-wait their slot. The wait
# graph then reduces to the acyclic datadeps DAG (receiver → owner compute →
# owner syncdeps), so it cannot deadlock, while keeping the O(log P) tree.
# Only the fixed-size `execute!` metadata uses this path; large value
# broadcasts (poolget/collect/fetch) keep the in-task tree at their sequential,
# non-overlapping points.

# Binomial-tree children of `rank` in the tree rooted at `root` over `sz` ranks
# (identical virtual-rank math to `bcast_yield`, so relay and root agree on the
# topology).
function bcast_tree_children(rank::Int, root::Int, sz::Int)
    vrank = mod(rank - root, sz)
    mask = 1
    while mask < sz
        (vrank & mask != 0) && break
        mask <<= 1
    end
    mask >>= 1
    children = Int[]
    while mask > 0
        if vrank + mask < sz
            push!(children, mod(rank + mask, sz))
        end
        mask >>= 1
    end
    return children
end

# One-shot promise for a delivered broadcast payload (one per tag).
mutable struct BcastSlot
    ev::Base.Event
    value::Any
    done::Bool
    BcastSlot() = new(Base.Event(), nothing, false)
end

mutable struct BcastState
    bcast_comm::MPI.Comm
    slots::Dict{UInt32,BcastSlot}
    lock::Threads.ReentrantLock
    running::Threads.Atomic{Bool}
    relay::Union{Task,Nothing}
end

# Keyed by the acceleration comm (what `execute!`/`bcast_meta_yield` see). The
# relay owns the duplicated `bcast_comm`, so it can `Improbe(ANY_SOURCE,
# ANY_TAG)` without colliding with main-comm P2P transfers or the in-task
# `bcast_yield` tree.
const BCAST_STATES = LockedObject(Dict{MPI.Comm,BcastState}())

bcast_state_for(comm::MPI.Comm) =
    lock(BCAST_STATES) do states
        states[comm]
    end

bcast_serialize(x) = (io = IOBuffer(); Serialization.serialize(io, x); take!(io))

function bcast_deliver!(state::BcastState, tag::UInt32, value)
    slot = lock(state.lock) do
        get!(BcastSlot, state.slots, tag)
    end
    slot.value = value
    slot.done = true
    notify(slot.ev)
    return
end

function bcast_slot_wait(state::BcastState, tag::UInt32)
    slot = lock(state.lock) do
        get!(BcastSlot, state.slots, tag)
    end
    # `Base.Event` latches: a `notify` that already fired makes `wait` return
    # immediately, so relay-before-consumer and consumer-before-relay both work.
    wait(slot.ev)
    value = slot.value
    lock(state.lock) do
        delete!(state.slots, tag)
    end
    return value
end

# Always-running per-rank progress engine: receive a broadcast on `bcast_comm`,
# forward the raw bytes to this rank's tree children, then deliver the payload.
# Forwarding is NOT gated by datadeps task dispatch — that is what breaks the
# forwarder wait-cycle.
function bcast_relay_loop(state::BcastState)
    comm = state.bcast_comm
    rank = MPI.Comm_rank(comm)
    sz = MPI.Comm_size(comm)
    while state.running[]
        try
            MPI.Finalized() && break
            got, msg, stat = MPI.Improbe(MPI.ANY_SOURCE, MPI.ANY_TAG, comm, MPI.Status)
            if !got
                yield()
                continue
            end
            src = MPI.Get_source(stat)
            tag = UInt32(MPI.Get_tag(stat))
            count = MPI.Get_count(stat, UInt8)
            buf = Array{UInt8}(undef, count)
            req = MPI.Imrecv!(MPI.Buffer(buf), msg)
            __wait_for_request(req, comm, rank, src, tag, "bcast_relay", "recv")
            root, value = Serialization.deserialize(IOBuffer(buf))
            for child in bcast_tree_children(rank, Int(root), sz)
                sreq = MPI.Isend(buf, comm; dest=child, tag=tag)
                __wait_for_request(sreq, comm, rank, child, tag, "bcast_relay", "send")
            end
            bcast_deliver!(state, tag, value)
        catch err
            # Comm torn down (disable/finalize) races the loop: stop quietly.
            # A genuine mid-session fault surfaces via errormonitor.
            (state.running[] && !MPI.Finalized()) || break
            rethrow(err)
        end
    end
    return
end

function start_bcast_relay!(accel_comm::MPI.Comm)
    bcast_comm = MPI.Comm_dup(accel_comm)
    state = BcastState(bcast_comm, Dict{UInt32,BcastSlot}(),
                       Threads.ReentrantLock(), Threads.Atomic{Bool}(true), nothing)
    lock(BCAST_STATES) do states
        states[accel_comm] = state
    end
    t = Task(() -> bcast_relay_loop(state))
    t.sticky = false
    schedule(t)
    Dagger.Sch.errormonitor_tracked("mpi_bcast_relay", t)
    state.relay = t
    # `start_bcast_relay!` runs strictly after `MPI.Init`, so this hook is
    # registered after MPI.jl's Finalize hook and (atexit is LIFO) runs before
    # it — the relay is stopped and joined before MPI shuts down, otherwise a
    # relay mid-`Improbe` on another thread races Finalize and MPICH aborts.
    atexit(() -> stop_bcast_relay!(accel_comm))
    return state
end

function stop_bcast_relay!(accel_comm::MPI.Comm)
    state = lock(BCAST_STATES) do states
        s = get(states, accel_comm, nothing)
        s === nothing || delete!(states, accel_comm)
        s
    end
    state === nothing && return nothing
    state.running[] = false
    # Join so no in-flight probe/recv can outlive the comm; a relay that died
    # to a teardown race already reported via errormonitor, so swallow it here.
    relay = state.relay
    if relay !== nothing && relay !== current_task()
        try
            wait(relay)
        catch
        end
    end
    return nothing
end

# Dispatch-decoupled broadcast used by `execute!` for per-task metadata. The
# root fires `(root, value)` to its tree children on `bcast_comm` and returns;
# every other rank waits for the relay to deliver its copy.
function bcast_meta_yield(comm::MPI.Comm, root::Integer, tag, value=nothing)
    sz = MPI.Comm_size(comm)
    sz == 1 && return value
    rank = MPI.Comm_rank(comm)
    state = bcast_state_for(comm)
    utag = UInt32(tag)
    if rank == root
        buf = bcast_serialize((Int(root), value))
        bcomm = state.bcast_comm
        for child in bcast_tree_children(rank, Int(root), sz)
            req = MPI.Isend(buf, bcomm; dest=child, tag=utag)
            __wait_for_request(req, bcomm, rank, child, utag, "bcast_meta", "send")
        end
        return value
    else
        return bcast_slot_wait(state, utag)
    end
end

WeakChunk(c::Chunk{T,H}) where {T,H<:MPIRef} = WeakChunk(c.handle.rank, c.handle.id.id, WeakRef(c), T)

function MemPool.poolget(ref::MPIRef; uniform::Bool=uniform_execution())
    @assert uniform || ref.rank == MPI.Comm_rank(ref.comm) "MPIRef rank mismatch: $(ref.rank) != $(MPI.Comm_rank(ref.comm))"
    if uniform
        tag = to_tag()
        if ref.rank == MPI.Comm_rank(ref.comm)
            return bcast_yield(ref.comm, ref.rank, tag, poolget(ref.innerRef))
        else
            return bcast_yield(ref.comm, ref.rank, tag)
        end
    else
        return poolget(ref.innerRef)
    end
end
fetch_handle(ref::MPIRef; uniform::Bool=uniform_execution()) = poolget(ref; uniform)

function move!(dep_mod, to_space::MPIMemorySpace, from_space::MPIMemorySpace, to::Chunk, from::Chunk)
    @assert to.handle isa MPIRef && from.handle isa MPIRef "MPIRef expected"
    @assert to.handle.comm == from.handle.comm "MPIRef comm mismatch"
    @assert to.handle.rank == to_space.rank && from.handle.rank == from_space.rank "MPIRef rank mismatch"
    local_rank = MPI.Comm_rank(from.handle.comm)
    if to_space.rank == from_space.rank == local_rank
        move!(dep_mod, to_space.innerSpace, from_space.innerSpace, to, from)
    else
        # Prefer the copy task's own (uniform, unique) tag; fall back to the
        # source handle's tag outside of task context
        tag = Dagger.in_task() ? to_tag() : to_tag(from.handle)
        @dagdebug nothing :mpi "[$local_rank][$tag] Moving from  $(from_space.rank)  to  $(to_space.rank)\n"
        if dep_mod === identity
            # Below the crossover, handle+ack overhead makes host staging
            # competitive; each side checks its own (equal-sized) buffer
            local_nbytes = local_rank == from_space.rank ? from.handle.size :
                           local_rank == to_space.rank   ? to.handle.size : 0
            if ipc_eligible(from_space.innerSpace, to_space.innerSpace) &&
               local_nbytes >= IPC_MIN_BYTES[] &&
               same_node(current_acceleration(), from_space.rank, to_space.rank)
                # Same-node device-to-device: ship only an IPC handle
                if local_rank == from_space.rank
                    mpi_ipc_send(poolget(from.handle; uniform=false), to_space.comm, to_space.rank, tag)
                elseif local_rank == to_space.rank
                    mpi_ipc_recv!(poolget(to.handle; uniform=false), from_space.comm, from_space.rank, tag)
                end
            # Full copy: receive directly into the destination buffer when possible
            elseif local_rank == from_space.rank
                send_yield!(poolget(from.handle; uniform=false), to_space.comm, to_space.rank, tag)
            elseif local_rank == to_space.rank
                to_val = poolget(to.handle; uniform=false)
                val, inplace = recv_yield!(to_val, from_space.comm, from_space.rank, tag)
                if !inplace
                    # Dense payloads may arrive host-staged; materialize in
                    # dest storage so inner move! sees matching spaces
                    val = mpi_localize(to_space.innerSpace, val)
                    move!(dep_mod, to_space.innerSpace, from_space.innerSpace, to_val, val)
                end
            end
        else
            # Partial copy (e.g. UpperTriangular): the destination must only
            # update the dep_mod-selected region. Both sides compute the
            # modifier's memory spans from their own replicas (counts and
            # lengths are rank-uniform; addresses are local) and only the
            # selected region travels, packed through the staging pool.
            # Device arrays gather/scatter via multi_span_* (one KA launch).
            pack = true
            if local_rank == from_space.rank
                from_val = poolget(from.handle; uniform=false)
                if pack
                    ainfo = _with_default_acceleration() do
                        aliasing(from_val, dep_mod)
                    end
                    # Both sides compute this from identically-typed replicas,
                    # so the protocol choice stays rank-uniform
                    pack = !(ainfo isa NoAliasing || ainfo isa UnknownAliasing)
                end
                if pack
                    spans = memory_spans(ainfo)
                    len = sum(span_len, spans; init=UInt64(0))
                    kind = gpu_memory_kind(from_space.innerSpace)
                    stage_buf = stage_acquire!(kind, len)
                    copies = unsafe_wrap(Array, pointer(stage_buf), Int(len))
                    with_context!(from_space.innerSpace)
                    mpi_device_sync(from_space.innerSpace)
                    GC.@preserve stage_buf begin
                        multi_span_gather!(copies, from_val, spans)
                        send_yield!(copies, to_space.comm, to_space.rank, tag)
                    end
                    stage_release!(kind, stage_buf)
                else
                    send_yield(from_val, to_space.comm, to_space.rank, tag)
                end
            elseif local_rank == to_space.rank
                to_val = poolget(to.handle; uniform=false)
                if pack
                    ainfo = _with_default_acceleration() do
                        aliasing(to_val, dep_mod)
                    end
                    pack = !(ainfo isa NoAliasing || ainfo isa UnknownAliasing)
                end
                if pack
                    spans = memory_spans(ainfo)
                    len = sum(span_len, spans; init=UInt64(0))
                    kind = gpu_memory_kind(to_space.innerSpace)
                    stage_buf = stage_acquire!(kind, len)
                    copies = unsafe_wrap(Array, pointer(stage_buf), Int(len))
                    GC.@preserve stage_buf begin
                        recv_yield!(copies, from_space.comm, from_space.rank, tag)
                        with_context!(to_space.innerSpace)
                        multi_span_scatter!(to_val, copies, spans)
                    end
                    mpi_device_sync(to_space.innerSpace)
                    stage_release!(kind, stage_buf)
                else
                    val = recv_yield(from_space.comm, from_space.rank, tag)
                    # Dense payloads arrive host-staged; materialize in this
                    # space's storage so dep_mod views copy device-to-device
                    val = mpi_localize(to_space.innerSpace, val)
                    move!(dep_mod, to_space.innerSpace, from_space.innerSpace, to_val, val)
                end
            end
        end
    end
    @dagdebug nothing :mpi "[$local_rank] Finished moving from  $(from_space.rank)  to  $(to_space.rank) successfuly\n"
end
function move!(dep_mod::Dagger.RemainderAliasing{<:MPIMemorySpace}, to_space::MPIMemorySpace, from_space::MPIMemorySpace, to::Chunk, from::Chunk)
    @assert to.handle isa MPIRef && from.handle isa MPIRef "MPIRef expected"
    @assert to.handle.comm == from.handle.comm "MPIRef comm mismatch"
    @assert to.handle.rank == to_space.rank && from.handle.rank == from_space.rank "MPIRef rank mismatch"
    local_rank = MPI.Comm_rank(from.handle.comm)
    if to_space.rank == from_space.rank == local_rank
        move!(dep_mod, to_space.innerSpace, from_space.innerSpace, to, from)
    else
        tag = Dagger.in_task() ? to_tag() : to_tag(from.handle)
        @dagdebug nothing :mpi "[$local_rank][$tag] Moving from  $(from_space.rank)  to  $(to_space.rank)\n"
        if local_rank == from_space.rank
            # Gather the source spans into a host buffer (device arrays pack
            # via multi_span_gather! / one KA launch, then a single DtoH)
            len = sum(span_tuple->span_len(span_tuple[1]), dep_mod.spans)
            kind = gpu_memory_kind(from_space.innerSpace)
            stage_buf = stage_acquire!(kind, len)
            copies = unsafe_wrap(Array, pointer(stage_buf), Int(len))
            from_raw = poolget(from.handle; uniform=false)
            with_context!(from_space.innerSpace)
            # Producers may have enqueued work on other tasks' streams
            mpi_device_sync(from_space.innerSpace)
            GC.@preserve stage_buf begin
                multi_span_gather!(copies, from_raw, first.(dep_mod.spans))
                send_yield!(copies, to_space.comm, to_space.rank, tag)
            end
            stage_release!(kind, stage_buf)
        elseif local_rank == to_space.rank
            # Receive the spans
            len = sum(span_tuple->span_len(span_tuple[1]), dep_mod.spans)
            kind = gpu_memory_kind(to_space.innerSpace)
            stage_buf = stage_acquire!(kind, len)
            copies = unsafe_wrap(Array, pointer(stage_buf), Int(len))
            GC.@preserve stage_buf begin
                recv_yield!(copies, from_space.comm, from_space.rank, tag)

                # Scatter the received bytes into the destination spans
                to_raw = poolget(to.handle; uniform=false)
                with_context!(to_space.innerSpace)
                multi_span_scatter!(to_raw, copies, last.(dep_mod.spans))
            end
            # Consumers run on other tasks/streams
            mpi_device_sync(to_space.innerSpace)
            stage_release!(kind, stage_buf)

            # Ensure that the data is visible
            Core.Intrinsics.atomic_fence(:release)
        end
    end

    return
end

# Functions route through the inner processor, so GPU backends can swap
# kernels for device equivalents (e.g. LAPACK.potrf! -> CUSOLVER.potrf!)
move(::MPIOSProc, dst::MPIProcessor, x::Union{Function,Type}) = move(OSProc(), dst.innerProc, x)
move(::MPIOSProc, ::MPIProcessor, x::Chunk{<:Union{Function,Type}}) = poolget(x.handle)

# Out-of-place MPI move for OSProc-labeled Chunks: delegate to the
# MPIProcessor Chunk path so egress runs on the owner and ingress on dest.
function move(src::MPIOSProc, dst::MPIProcessor, x::Chunk)
    @assert src.comm == dst.comm "Multi comm move not supported"
    from = x.processor
    if !(from isa MPIProcessor) || from.rank != x.handle.rank || from.comm != src.comm
        from = MPIProcessor(OSProc(), src.comm, x.handle.rank)
    end
    return move(from, dst, x)
end

function is_local(accel::MPIAcceleration, target)
    return target.rank == MPI.Comm_rank(accel.comm)
end

### SPMD-symmetric datadeps endpoints
#
# Under MPI, datadeps planning (`distribute_tasks!`) runs identically on every
# rank (SPMD). All ranks therefore enter `generate_slot!` /
# `remotecall_endpoint_toplevel` collectively and walk the exact same
# structure-driven path: the aliased-object cache is a per-rank replicated
# store updated symmetrically at the same logical points (no RPC, no
# side-channel registries), aliasing metadata is broadcast from the owner at
# these aligned points, and bulk data is routed owner->destination via P2P
# inside the endpoint.

# A value participating in an SPMD endpoint: the owner rank holds the actual
# value, all other ranks hold only the type and the origin (owner) space.
struct MPIWireValue{T}
    value::Union{Some{T},Nothing}
    space::MPIMemorySpace
end
wire_type(::MPIWireValue{T}) where T = T
has_value(w::MPIWireValue) = w.value !== nothing
wire_value(w::MPIWireValue) = something(w.value)

memory_space(w::MPIWireValue) = w.space
default_memory_space(accel::MPIAcceleration, w::MPIWireValue) = w.space

function check_uniform(w::MPIWireValue{T}, original=w) where T
    # Compare logical metadata only: the value itself is rank-local
    return check_uniform(hash(T), original) &&
           check_uniform(w.space, original)
end

function tochunk(w::MPIWireValue{T}, proc::P, scope::S=Dagger.AnyScope(); kwargs...) where {T,P<:Processor,S}
    if has_value(w)
        return tochunk(wire_value(w), proc, w.space, scope; kwargs...)
    else
        return tochunk(nothing, proc, w.space, scope; type=T, kwargs...)
    end
end

# Remap rank-local memory space stamps (CPURAMMemorySpace(myid()), where
# myid() == 1 on every rank) in aliasing info to the owning rank, so that
# spans from different ranks never falsely alias (SPMD heaps have very
# similar address layouts). `mpi_remap_space` defaults live in core Dagger.
mpi_remap_ptr(ptr::Dagger.RemotePtr{T}, owner::Int) where T =
    RemotePtr{T}(ptr.addr, mpi_remap_space(ptr.space, owner))
mpi_remap_span(span::MemorySpan, owner::Int) =
    MemorySpan(mpi_remap_ptr(span.ptr, owner), span.len)

mpi_remap_ainfo(a::Dagger.NoAliasing, owner::Int) = a
mpi_remap_ainfo(a::Dagger.UnknownAliasing, owner::Int) = a
mpi_remap_ainfo(a::Dagger.ContiguousAliasing, owner::Int) =
    Dagger.ContiguousAliasing(mpi_remap_span(a.span, owner))
mpi_remap_ainfo(a::Dagger.ObjectAliasing, owner::Int) =
    Dagger.ObjectAliasing(mpi_remap_ptr(a.ptr, owner), a.sz)
function mpi_remap_ainfo(a::Dagger.StridedAliasing{T,N,S}, owner::Int) where {T,N,S}
    base_ptr = mpi_remap_ptr(a.base_ptr, owner)
    return Dagger.StridedAliasing{T,N,typeof(base_ptr.space)}(base_ptr,
                                                       mpi_remap_ptr(a.ptr, owner),
                                                       a.base_inds, a.lengths, a.strides)
end
function mpi_remap_ainfo(a::Dagger.TriangularAliasing{T,S}, owner::Int) where {T,S}
    ptr = mpi_remap_ptr(a.ptr, owner)
    return Dagger.TriangularAliasing{T,typeof(ptr.space)}(ptr, a.stride, a.isupper, a.diagonal)
end
function mpi_remap_ainfo(a::Dagger.DiagonalAliasing{T,S}, owner::Int) where {T,S}
    ptr = mpi_remap_ptr(a.ptr, owner)
    return Dagger.DiagonalAliasing{T,typeof(ptr.space)}(ptr, a.stride)
end
mpi_remap_ainfo(a::Dagger.CombinedAliasing, owner::Int) =
    Dagger.CombinedAliasing(Dagger.AbstractAliasing[mpi_remap_ainfo(sub, owner) for sub in a.sub_ainfos])
mpi_remap_ainfo(a::Dagger.AliasingWrapper, owner::Int) =
    Dagger.AliasingWrapper(mpi_remap_ainfo(a.inner, owner))
mpi_remap_ainfo(a::Dagger.AbstractAliasing, owner::Int) = a

# Owner computes the aliasing info for its local value and broadcasts it;
# all ranks must call this collectively at the same logical point.
function Dagger.aliasing(accel::MPIAcceleration, w::MPIWireValue, dep_mod)
    tag = to_tag()
    check_uniform(tag)
    rank = MPI.Comm_rank(accel.comm)
    if w.space.rank == rank
        ainfo = Dagger._with_default_acceleration() do
            Dagger.aliasing(wire_value(w), dep_mod)
        end
        ainfo = mpi_remap_ainfo(ainfo, w.space.rank)
        @opcounter :aliasing_bcast_send_yield
        ainfo = bcast_yield(accel.comm, w.space.rank, tag, ainfo)
    else
        ainfo = bcast_yield(accel.comm, w.space.rank, tag)
    end
    check_uniform(ainfo)
    return ainfo
end

# All ranks enter collectively; the owner unwraps its local value, all other
# ranks construct a wire proxy carrying only type and origin space. The
# `move_rewrap` recursion below then walks the same path on every rank.
function remotecall_endpoint_toplevel(f, accel::MPIAcceleration, cache::AliasedObjectCache, from_proc, to_proc, from_space, to_space, data::Chunk)
    local_rank = MPI.Comm_rank(accel.comm)
    T = chunktype(data)
    w = if local_rank == from_space.rank
        MPIWireValue{T}(Some{T}(unwrap(data)), from_space)
    else
        MPIWireValue{T}(nothing, from_space)
    end
    return f(accel, cache, from_proc, to_proc, from_space, to_space, w)::Chunk
end

# Materialize a host-staged received value in the native storage of `space`
# (e.g. upload a host Array into device memory for a GPU destination)
function mpi_localize(space::MemorySpace, value)
    space isa CPURAMMemorySpace && return value
    value isa Array || return value
    return move(OSProc(), first(processors(space)), value)
end

# Inner processor of a possibly-MPI-wrapped processor
mpi_inner_proc(proc::MPIProcessor) = proc.innerProc
mpi_inner_proc(proc::Processor) = proc

# Ensure a processor carries MPI rank metadata for dual-sided OOP move
function as_mpi_proc(proc::MPIProcessor, space::MPIMemorySpace)
    if proc.rank == space.rank && proc.comm == space.comm
        return proc
    end
    return MPIProcessor(proc.innerProc, space.comm, space.rank)
end
as_mpi_proc(proc::Processor, space::MPIMemorySpace) =
    MPIProcessor(proc, space.comm, space.rank)

# Source-half OOP conversion: adapt off the owner's device toward a
# transportable form. Dense device buffers that send_yield can ship
# GPU-direct (or stage itself) are left as-is after a device sync.
function mpi_move_egress(src::MPIProcessor, x)
    if x isa DenseArray && isbitstype(eltype(x)) && !(x isa Array)
        mpi_device_sync(x)
        return x
    end
    return move(src.innerProc, OSProc(), x)
end

# Dest-half OOP conversion: adapt a received (often host-staged) value
# into the destination inner processor's native storage (e.g. HtoD).
mpi_move_ingress(dst::MPIProcessor, x) = move(OSProc(), dst.innerProc, x)

# Rank-aware out-of-place move: egress on the source rank, ingress on the
# destination rank. Distributed can stay dest-pull; MPI needs both sides.
function move(src::MPIProcessor, dst::MPIProcessor, x)
    @assert src.comm == dst.comm "Multi comm move not supported"
    local_rank = MPI.Comm_rank(src.comm)
    if src.rank == dst.rank
        if local_rank == src.rank
            return move(src.innerProc, dst.innerProc, x)
        end
        return nothing
    elseif local_rank == src.rank
        return mpi_move_egress(src, x)
    elseif local_rank == dst.rank
        return mpi_move_ingress(dst, x)
    end
    return nothing
end

# Transfer the wire value from its owner to the destination rank, returning a
# uniform Chunk (real payload on the destination, placeholder elsewhere).
# Both ranks run move: owner does egress before send, dest does ingress after
# recv. Chunk type is computed uniformly on every rank via move_type.
function mpi_endpoint_transfer(accel::MPIAcceleration, from_proc, to_proc, from_space, to_space, w::MPIWireValue{T}) where T
    local_rank = MPI.Comm_rank(accel.comm)
    from_mpi = as_mpi_proc(from_proc, from_space)
    to_mpi = as_mpi_proc(to_proc, to_space)
    from_inner = mpi_inner_proc(from_mpi)
    to_inner = mpi_inner_proc(to_mpi)
    T_new = move_type(from_inner, to_inner, T)
    if from_space.rank == to_space.rank
        # No cross-rank movement; the owner (re)wraps its local value,
        # converting between inner spaces (e.g. CPU -> GPU) when they differ
        if local_rank == from_space.rank
            value = move(from_mpi, to_mpi, wire_value(w))
            return tochunk(value, to_proc, to_space; type=T_new)
        else
            return tochunk(nothing, to_proc, to_space; type=T_new)
        end
    end
    tag = to_tag()
    if T <: DenseArray && isbitstype(eltype(T)) &&
       ipc_eligible(from_space.innerSpace, to_space.innerSpace) &&
       same_node(accel, from_space.rank, to_space.rank)
        # Same-node device-to-device slot creation: ship only an IPC handle
        if local_rank == from_space.rank
            mpi_ipc_send(wire_value(w), accel.comm, to_space.rank, tag)
            return tochunk(nothing, to_proc, to_space; type=T_new)
        elseif local_rank == to_space.rank
            value = mpi_ipc_recv_materialize(accel.comm, from_space.rank, tag)
            return tochunk(value, to_proc, to_space; type=T_new)
        else
            return tochunk(nothing, to_proc, to_space; type=T_new)
        end
    end
    if local_rank == from_space.rank
        # Owner: egress move, then send the (possibly host-staged) value
        value = move(from_mpi, to_mpi, wire_value(w))
        send_yield(value, accel.comm, to_space.rank, tag)
        return tochunk(nothing, to_proc, to_space; type=T_new)
    elseif local_rank == to_space.rank
        value = recv_yield(accel.comm, from_space.rank, tag)
        # Dest: ingress move into this space's native storage
        value = move(from_mpi, to_mpi, value)
        return tochunk(value, to_proc, to_space; type=T_new)
    else
        return tochunk(nothing, to_proc, to_space; type=T_new)
    end
end

# Generic / wrapper MPIWireValue: leaf transfer, or header+children rebuild
function move_rewrap(accel::MPIAcceleration, cache::AliasedObjectCache, from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, w::MPIWireValue{T}) where T
    child_types = move_rewrap_child_types(T)
    if child_types === nothing
        # Leaf: transfer to the destination, sharing via the aliased-object cache
        return aliased_object!(cache, w) do w
            return mpi_endpoint_transfer(accel, from_proc, to_proc, from_space, to_space, w)
        end
    end

    local_rank = MPI.Comm_rank(accel.comm)
    child_wires = if has_value(w)
        children, _ = move_rewrap_parts(wire_value(w))
        ntuple(length(child_types)) do i
            CT = child_types[i]
            MPIWireValue{CT}(Some{CT}(children[i]), w.space)
        end
    else
        ntuple(length(child_types)) do i
            CT = child_types[i]
            MPIWireValue{CT}(nothing, w.space)
        end
    end

    child_chunks = map(cw -> move_rewrap(accel, cache, from_proc, to_proc, from_space, to_space, cw), child_wires)
    for cc in child_chunks
        check_uniform(cc.handle)
    end

    mode = move_rewrap_header_mode(T)
    header = if mode === :broadcast
        tag = to_tag()
        if local_rank == w.space.rank
            _, hdr = move_rewrap_parts(wire_value(w))
            bcast_yield(accel.comm, w.space.rank, tag, hdr)
        else
            bcast_yield(accel.comm, w.space.rank, tag)
        end
    elseif mode === :none
        nothing
    else
        error("MPIWireValue move_rewrap does not support header mode $mode for $T")
    end

    child_cts = map(chunktype, child_chunks)
    T_new = move_rewrap_result_type(T, child_cts, header)
    if local_rank == to_space.rank
        children_local = map(unwrap, child_chunks)
        v_new = move_rewrap_build(T, children_local, header)
        return tochunk(v_new, to_proc, to_space; type=T_new)
    else
        return tochunk(nothing, to_proc, to_space; type=T_new)
    end
end

# ChunkView: replicated SPMD metadata (inner chunk record + slices exist on
# every rank), so all ranks dispatch symmetrically without a wire proxy
function remotecall_endpoint_toplevel(f, accel::MPIAcceleration, cache::AliasedObjectCache, from_proc, to_proc, from_space, to_space, data::ChunkView)
    return f(accel, cache, from_proc, to_proc, from_space, to_space, data)::Chunk
end

# Share/transfer the backing chunk, then re-create the view on the destination
# (header is replicated SPMD metadata — no broadcast)
function move_rewrap(accel::MPIAcceleration, cache::AliasedObjectCache, from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, slice::ChunkView)
    local_rank = MPI.Comm_rank(accel.comm)
    children, header = move_rewrap_parts(slice)
    p_chunk = move_rewrap(accel, cache, from_proc, to_proc, from_space, to_space, children[1])
    check_uniform(p_chunk.handle)
    T_view = move_rewrap_result_type(typeof(slice), (chunktype(p_chunk),), header)
    if local_rank == to_space.rank
        v_new = move_rewrap_build(typeof(slice), (unwrap(p_chunk),), header)
        return tochunk(v_new, to_proc, to_space; type=T_view)
    else
        return tochunk(nothing, to_proc, to_space; type=T_view)
    end
end

# A Chunk-wrapped ChunkView cannot cross ranks: the inner chunk record is only
# resolvable on the wrapping rank. `datadeps_arg_wrap` keeps ChunkViews raw
# under MPI, so reaching this indicates a manual `tochunk(::ChunkView)`.
function move_rewrap(accel::MPIAcceleration, cache::AliasedObjectCache, from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, w::MPIWireValue{T}) where {T<:ChunkView}
    throw(ArgumentError("ChunkView must not be wrapped in a Chunk under MPI; pass the ChunkView directly"))
end

# Owner computes the view's aliasing info locally and broadcasts it
function aliasing(accel::MPIAcceleration, x::ChunkView, dep_mod)
    @assert dep_mod === identity "Dependency modifiers not yet supported for ChunkView: $dep_mod"
    handle = x.chunk.handle::MPIRef
    tag = to_tag()
    check_uniform(tag)
    rank = MPI.Comm_rank(accel.comm)
    if handle.rank == rank
        ainfo = _with_default_acceleration() do
            v = view(unwrap(x.chunk), x.slices...)
            # Resolve whole-object containers (e.g. `DSparseArray`) where `v`
            # lives; see `aliasing_unwrapped`.
            aliasing_unwrapped(v, dep_mod)
        end
        ainfo = mpi_remap_ainfo(ainfo, handle.rank)
        @opcounter :aliasing_bcast_send_yield
        ainfo = bcast_yield(accel.comm, handle.rank, tag, ainfo)
    else
        ainfo = bcast_yield(accel.comm, handle.rank, tag)
    end
    check_uniform(ainfo)
    return ainfo
end

# The aliased-object cache is a per-rank replicated store; every rank updates
# its own copy at the same logical point during SPMD planning, so no
# cross-rank communication is required here.
function set_stored!(accel::MPIAcceleration, cache::AliasedObjectCache, value::Chunk, ainfo::AbstractAliasing)
    set_stored!(unwrap(cache.chunk)::AliasedObjectCacheStore, cache.space, value, ainfo)
    return
end
function set_key_stored!(accel::MPIAcceleration, cache::AliasedObjectCache, space::MemorySpace, ainfo::AbstractAliasing, value::Chunk)
    set_key_stored!(unwrap(cache.chunk)::AliasedObjectCacheStore, space, ainfo, value)
    return
end

# Chunk may be MPI-backed (MPIRef) but labeled with OSProc; treat source as the owning rank
function move(src::OSProc, dst::MPIProcessor, x::Chunk)
    if x.handle isa MPIRef
        return move(MPIOSProc(x.handle.comm, x.handle.rank), dst, x)
    end
    error("MPI move not supported")
end

move(src::Processor, dst::MPIProcessor, x::Chunk) = error("MPI move not supported")
move(to_proc::MPIProcessor, chunk::Chunk) =
    move(chunk.processor, to_proc, chunk)
move(to_proc::Processor, d::MPIRef) =
    move(MPIOSProc(d.rank), to_proc, d)
move(to_proc::MPIProcessor, x) =
    move(MPIOSProc(), to_proc, x)

# Non-Chunk task arguments: dual-sided OOP move via MPIProcessor ranks
move(src::MPIOSProc, dst::MPIProcessor, x) =
    move(MPIProcessor(OSProc(), src.comm, src.rank), dst, x)

# Functions route through the inner processor, so GPU backends can swap
# kernels for device equivalents (e.g. BLAS.gemm! -> CUBLAS.gemm!)
move(src::MPIProcessor, dst::MPIProcessor, x::Union{Function,Type}) =
    move(OSProc(), dst.innerProc, x)
move(::MPIProcessor, ::MPIProcessor, x::Chunk{<:Union{Function,Type}}) = poolget(x.handle)

function move(src::MPIProcessor, dst::MPIProcessor, x::Chunk)
    if Dagger.Sch.SCHED_MOVE[]
        # SPMD argument move at task launch: every rank enters; owner runs
        # egress move before send, destination runs ingress after recv.
        # When the chunk is not resident on the destination rank, the owner
        # sends it P2P on the thunk's uniform tag (argument moves run
        # sequentially in argument order, so FIFO matching is safe).
        local_rank = MPI.Comm_rank(dst.comm)
        if x.handle.rank == dst.rank
            if local_rank == dst.rank
                value = poolget(x.handle; uniform=false)
                return move(src, dst, value)
            end
            return nothing
        else
            @assert DATADEPS_THUNK_ID[] != 0 "Cross-rank argument transfer requires the thunk's TID in scope (chunk owner $(x.handle.rank), destination rank $(dst.rank))"
            tag = UInt32(DATADEPS_THUNK_ID[])
            # Source processor for egress uses the chunk's owning rank
            from = src.rank == x.handle.rank ? src :
                   MPIProcessor(src.innerProc, src.comm, x.handle.rank)
            if local_rank == x.handle.rank
                value = move(from, dst, poolget(x.handle; uniform=false))
                send_yield(value, dst.comm, dst.rank, tag)
                return nothing
            elseif local_rank == dst.rank
                value = recv_yield(dst.comm, x.handle.rank, tag)
                return move(from, dst, value)
            end
            return nothing
        end
    else
        uniform = uniform_execution()
        # Either we're uniform (so everyone cooperates), or we're unwrapping locally
        if !uniform
            @assert src.rank == MPI.Comm_rank(src.comm) "Unwrapping not permitted"
            @assert src.rank == x.handle.rank == dst.rank
        end
        return poolget(x.handle; uniform)
    end
end

gpu_kernel_backend(proc::MPIProcessor) = gpu_kernel_backend(proc.innerProc)

# Owner-local payload that preserves the Chunk's SPMD-uniform chunktype after
# Sch unwraps a Chunk to a device value (e.g. Matrix chunktype + CuArray value).
# Without this, promote_op/chunktype diverge across ranks (CuArray vs Matrix).
struct MPILocalArg{T}
    value
end
chunktype(::MPILocalArg{T}) where T = T
mpi_unwrap_arg(a::MPILocalArg) = a.value
mpi_unwrap_arg(a) = a

function bind_moved_argument(::MPIAcceleration, original, moved)
    if moved === nothing && (original isa Chunk || original isa WeakChunk)
        return original
    elseif (original isa Chunk || original isa WeakChunk) &&
           moved !== nothing && !(moved isa Chunk)
        return MPILocalArg{chunktype(original)}(moved)
    end
    return moved
end

# Exceptions may capture rank-local, non-serializable state; fall back to a
# string summary when they can't go over the wire
function mpi_wire_exception(err)
    try
        Serialization.serialize(IOBuffer(), err)
        return err
    catch
        return ErrorException(sprint(showerror, err))
    end
end

# Decide how much result metadata execute! must broadcast.
# - need_type_bcast: return type/space unknown → full (:ok,T,space)/(:error,ex)
# - !need_type_bcast && nothrow: concrete + proven non-throwing → zero MPI
# - !need_type_bcast && !nothrow: concrete but may throw → status-only (:ok / :error)
#
# Device processors (CUDA/ROCm/…) adapt values (e.g. Matrix→CuArray), so
# promote_op on chunktypes is not a safe SPMD stamp; keep full broadcast there.
function mpi_execute_bcast_plan(f, args, proc::MPIProcessor)
    if !(proc.innerProc isa ThreadProc)
        inferred = Base.promote_op(f, map(chunktype, args)...)
        return (; need_type_bcast=true, nothrow=false, inferred)
    end
    arg_types = map(chunktype, args)
    inferred = Base.promote_op(f, arg_types...)
    # `Nothing` is a concrete type and is deliberately NOT forced onto the
    # broadcast path: a `nothing` return (the common in-place / mutating task)
    # is fully known on every rank (all ranks stamp `Chunk{Nothing}` and
    # `memory_space(nothing, proc)`), so it can take the concrete fast path and
    # skip metadata traffic. `Union{}` (function always throws) and `Any`
    # (inference gave up) are not concrete and still need the full broadcast.
    need_type_bcast = !isconcretetype(inferred) || inferred === Union{} ||
                      inferred === Any
    if need_type_bcast
        return (; need_type_bcast=true, nothrow=false, inferred)
    end
    effects = Base.infer_effects(f, Tuple{arg_types...})
    nothrow = Core.Compiler.is_nothrow(effects)
    return (; need_type_bcast=false, nothrow, inferred)
end

function execute!(proc::MPIProcessor, f, args...; kwargs...)
    local_rank = MPI.Comm_rank(proc.comm)
    islocal = local_rank == proc.rank
    inplace_move = f === move!
    tag = to_tag()
    # Plan from SPMD-uniform chunktypes (Chunk / MPILocalArg); run on unwrapped values
    raw_args = map(mpi_unwrap_arg, args)

    if inplace_move
        # Copy tasks are dest-scoped, but every rank runs move!. Bind the
        # local endpoint's device (source vs dest), not always dest's
        # mirrored innerProc — otherwise multi-GPU ranks activate the wrong
        # context before DtoH/gather. Spectators use ThreadProc so no
        # unrelated GPU context is set.
        to_space = raw_args[2]::MPIMemorySpace
        from_space = raw_args[3]::MPIMemorySpace
        local_proc = if local_rank == to_space.rank
            first(processors(to_space.innerSpace))
        elseif local_rank == from_space.rank
            first(processors(from_space.innerSpace))
        else
            ThreadProc(myid(), 1)
        end
        execute!(local_proc, f, raw_args...; kwargs...)
        space = memory_space(nothing, proc)::MPIMemorySpace
        dest_type = chunktype(args[4])
        return tochunk(nothing, proc, space; type=dest_type)
    end

    plan = mpi_execute_bcast_plan(f, args, proc)

    # Concrete + nothrow: type/space known locally and the call cannot throw,
    # so non-owners need no status or metadata traffic. Stamp the inferred
    # type on every rank so chunktype stays SPMD-uniform.
    if !plan.need_type_bcast && plan.nothrow
        if islocal
            result = execute!(proc.innerProc, f, raw_args...; kwargs...)
            space = memory_space(result, proc)::MPIMemorySpace
            return tochunk(result, proc, space; type=plan.inferred)
        else
            space = memory_space(nothing, proc)::MPIMemorySpace
            return tochunk(nothing, proc, space; type=plan.inferred)
        end
    end

    # Concrete but may throw: broadcast only status (no T/space on the wire).
    # Type inference still skips metadata; poison keeps SPMD aligned on error.
    if !plan.need_type_bcast
        if islocal
            result = try
                execute!(proc.innerProc, f, raw_args...; kwargs...)
            catch err
                @opcounter :execute_bcast_send_yield
                bcast_meta_yield(proc.comm, proc.rank, tag, (:error, mpi_wire_exception(err)))
                rethrow()
            end
            @opcounter :execute_bcast_send_yield
            bcast_meta_yield(proc.comm, proc.rank, tag, :ok)
            space = memory_space(result, proc)::MPIMemorySpace
            return tochunk(result, proc, space; type=plan.inferred)
        else
            msg = bcast_meta_yield(proc.comm, proc.rank, tag)
            if msg isa Tuple && first(msg) === :error
                throw(msg[2])
            end
            space = memory_space(nothing, proc)::MPIMemorySpace
            return tochunk(nothing, proc, space; type=plan.inferred)
        end
    end

    # Non-concrete return type: full status + type/space (or error) broadcast
    if islocal
        result = try
            execute!(proc.innerProc, f, raw_args...; kwargs...)
        catch err
            @opcounter :execute_bcast_send_yield
            bcast_meta_yield(proc.comm, proc.rank, tag, (:error, mpi_wire_exception(err)))
            rethrow()
        end
        T = typeof(result)
        space = memory_space(result, proc)::MPIMemorySpace
        @opcounter :execute_bcast_send_yield
        bcast_meta_yield(proc.comm, proc.rank, tag, (:ok, T, space.innerSpace))
        return tochunk(result, proc, space; type=T)
    else
        msg = bcast_meta_yield(proc.comm, proc.rank, tag)
        if msg[1] === :error
            throw(msg[2])
        end
        _, T, innerSpace = msg
        space = MPIMemorySpace(innerSpace, proc.comm, proc.rank)
        return tochunk(nothing, proc, space; type=T)
    end
end

accelerate!(::Val{:mpi}) = accelerate!(MPIAcceleration())

# Comms with an active MPI acceleration. Guards `initialize_acceleration!`
# idempotency.
const MPI_INITIALIZED_COMMS = LockedObject(Set{MPI.Comm}())

# rank -> system_uuid (node identity), populated once at initialization via
# Allgather — same node definition as Distributed CUDAExt IPC.
const MPI_NODE_UUIDS = LockedObject(Dict{MPI.Comm, Vector{UInt128}}())

# Whether two ranks share a node (false when the map is unavailable)
function same_node(accel::MPIAcceleration, r1::Integer, r2::Integer)
    uuids = lock(MPI_NODE_UUIDS) do node_maps
        get(node_maps, accel.comm, nothing)
    end
    uuids === nothing && return false
    return uuids[r1+1] == uuids[r2+1]
end
# Compat for benches that still have a raw comm
function mpi_same_node(comm::MPI.Comm, r1::Integer, r2::Integer)
    uuids = lock(MPI_NODE_UUIDS) do node_maps
        get(node_maps, comm, nothing)
    end
    uuids === nothing && return false
    return uuids[r1+1] == uuids[r2+1]
end

function initialize_acceleration!(a::MPIAcceleration)
    # Idempotent: user `accelerate!` may be called more than once, but
    # initialization involves collectives (`MPI.Comm_dup`) which must only
    # ever run once per installed session. `accelerate!` skips this when
    # `same_acceleration` holds; after `finalize_acceleration!` the backend
    # entry is gone and re-init is allowed. Per-task execution uses
    # `set_task_acceleration!` in `do_task` (TLS only, no re-init).
    already = lock(MPI_INITIALIZED_COMMS) do comms
        a.comm in comms
    end
    already && return
    if !MPI.Initialized()
        MPI.Init(;threadlevel=:multiple)
    end
    lock(MPI_INITIALIZED_COMMS) do comms
        push!(comms, a.comm)
    end
    # Node-locality map for shared-memory/IPC fast paths. Init-time
    # collectives are sanctioned only here (alongside the RPC backend setup);
    # the map is cached for the whole session. Uses system_uuid (same as
    # Distributed CUDAExt) rather than COMM_TYPE_SHARED.
    local_uuid = system_uuid().value
    node_uuids = MPI.Allgather(local_uuid, a.comm)
    lock(MPI_NODE_UUIDS) do node_maps
        node_maps[a.comm] = node_uuids
    end
    # Dedicated broadcast communicator + per-rank relay for per-task metadata
    # broadcasts (see `bcast_meta_yield`). `Comm_dup` is collective and, like
    # `Allgather` above, is sanctioned only at init.
    start_bcast_relay!(a.comm)
    ctx = Dagger.Sch.eager_context()
    sz = MPI.Comm_size(a.comm)
    lock(ctx) do
        for i in 0:(sz-1)
            push!(ctx.procs, MPIOSProc(a.comm, i))
        end
        unique!(ctx.procs)
    end
    # SPMD planning can call `rand`/`randperm`; keep default RNGs aligned.
    sync_mpi_rng!(a.comm)
    check_mpi_rng_coherent!(a.comm)
end

function finalize_acceleration!(a::MPIAcceleration)
    stop_bcast_relay!(a.comm)
    lock(MPI_INITIALIZED_COMMS) do comms
        delete!(comms, a.comm)
    end
    lock(MPI_NODE_UUIDS) do node_maps
        delete!(node_maps, a.comm)
    end
    ctx = Dagger.Sch.eager_context()
    lock(ctx) do
        filter!(p -> !(p isa MPIOSProc && p.comm == a.comm), ctx.procs)
    end
    return nothing
end

"""
    mpi_propagate_chunk_types!(tasks, accel::MPIAcceleration, expected_type)

Ensure all ranks use the same concrete type for the given tasks by setting
each task's options.return_type to expected_type when it is concrete.
This allows chunktype(task) to return the concrete type on every rank
without an MPI allgather of actual result types.
"""
function mpi_propagate_chunk_types!(tasks, accel::MPIAcceleration, expected_type)
    isconcretetype(expected_type) || return
    for t in tasks
        if t isa Thunk
            if t.options !== nothing
                t.options.return_type = expected_type
            else
                t.options = Options(return_type=expected_type)
            end
        end
    end
    return
end

# Deterministic round-robin assignment of array blocks to MPI processors:
# every rank computes the same grid, so block ownership is uniform by
# construction (the scheduler's measured costs are rank-local and cannot be
# used for placement decisions under SPMD).
function default_procgrid(accel::MPIAcceleration, nblocks::NTuple{N,Int}) where N
    return CyclicProcGrid(uniform_mpi_processors(accel), nblocks)
end

function post_stage_array_chunks!(accel::MPIAcceleration, tasks, eltype::Type{T}, nd::Int) where {T}
    mpi_propagate_chunk_types!(tasks, accel, Array{eltype, nd})
    return tasks
end

accel_matches_proc(accel::MPIAcceleration, proc::MPIOSProc) = true
accel_matches_proc(accel::MPIAcceleration, proc::MPIClusterProc) = true
accel_matches_proc(accel::MPIAcceleration, proc::MPIProcessor) = true
accel_matches_proc(accel::MPIAcceleration, proc) = false

end # module MPIExt