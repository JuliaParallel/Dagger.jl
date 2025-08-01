import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv

export In, Out, InOut, Deps, spawn_datadeps

#=
==============================================================================
                    DATADEPS ALIASING AND DATA MOVEMENT SYSTEM
==============================================================================

This file implements the data dependencies system for Dagger tasks, which allows
tasks to write to their arguments in a controlled manner. The system maintains
data coherency across distributed workers by tracking aliasing relationships
and orchestrating data movement operations.

OVERVIEW:
---------
The datadeps system enables parallel execution of tasks that modify shared data
by analyzing memory aliasing relationships and scheduling appropriate data 
transfers. The core challenge is maintaining coherency when aliased data (e.g., 
an array and its views) needs to be accessed by tasks running on different workers.

KEY CONCEPTS:
-------------

1. ALIASING ANALYSIS:
   - Every mutable argument is analyzed for its memory access pattern
   - Memory spans are computed to determine which bytes in memory are accessed
   - Objects that access overlapping memory spans are considered "aliasing"
   - Examples: An array A and view(A, 2:3, 2:3) alias each other

2. DATA LOCALITY TRACKING:
   - The system tracks where the "source of truth" for each piece of data lives
   - As tasks execute and modify data, the source of truth may move between workers
   - Each aliasing region can have its own independent source of truth location

3. ALIASED OBJECT MANAGEMENT:
   - When copying arguments between workers, the system tracks "aliased objects"
   - This ensures that if both an array and its view need to be copied to a worker,
     only one copy of the underlying array is made, with the view pointing to it
   - The aliased_object!() functions manage this sharing

THE DISTRIBUTED ALIASING PROBLEM:
---------------------------------

In a multithreaded environment, aliasing "just works" because all tasks operate
on the same memory. However, in a distributed environment, arguments must be
copied between workers, which breaks aliasing relationships.

Consider this scenario:
```julia
A = rand(4, 4)
vA = view(A, 2:3, 2:3)

Dagger.spawn_datadeps() do
    Dagger.@spawn inc!(InOut(A), 1)    # Task 1: increment all of A
    Dagger.@spawn inc!(InOut(vA), 2)   # Task 2: increment view of A
end
```

MULTITHREADED BEHAVIOR (WORKS):
- Both tasks run on the same worker
- They operate on the same memory, with proper dependency tracking
- Task dependencies ensure correct ordering (e.g., Task 1 then Task 2)

DISTRIBUTED BEHAVIOR (THE PROBLEM):
- Tasks may be scheduled on different workers
- Each argument must be copied to the destination worker
- Without special handling, we would copy A to worker1 and vA to worker2
- This creates two separate arrays, breaking the aliasing relationship
- Updates to the view on worker2 don't affect the array on worker1

THE SOLUTION - PARTIAL DATA MOVEMENT:
-------------------------------------

The datadeps system solves this by:

1. UNIFIED ALLOCATION:
   - When copying aliased objects, ensure only one underlying array exists per worker
   - Use aliased_object!() to detect and reuse existing allocations
   - Views on the destination worker point to the shared underlying array

2. PARTIAL DATA TRANSFER:
   - Instead of copying entire objects, only transfer the "dirty" regions
   - This minimizes network traffic and maximizes parallelism
   - Uses the move!(dep_mod, ...) function with dependency modifiers

3. REMAINDER TRACKING:
   - When a partial region is updated, track what parts still need updating
   - Before a task needs the full object, copy the remaining "clean" regions
   - This preserves all updates while avoiding overwrites

EXAMPLE EXECUTION FLOW:
-----------------------

Given: A = 4x4 array, vA = view(A, 2:3, 2:3)
Tasks: T1 modifies InOut(A), T2 modifies InOut(vA)

1. INITIAL STATE:
   - A and vA both exist on worker0 (main worker)
   - A's data_locality = worker0, vA's data_locality = worker0

2. T1 SCHEDULED ON WORKER1:
   - Copy A from worker0 to worker1
   - T1 executes, modifying all of A on worker1
   - Update: A's data_locality = worker1, A is now "dirty" on worker1

3. T2 SCHEDULED ON WORKER2:
   - T2 needs vA, but vA aliases with A (which was modified by T1)
   - Copy vA-region of A from worker1 to worker2
   - This is a PARTIAL copy - only the 2:3, 2:3 region
   - Create vA on worker2 pointing to the appropriate region
   - T2 executes, modifying vA region on worker2
   - Update: vA's data_locality = worker2

4. FINAL SYNCHRONIZATION:
   - Some future task needs the complete A
   - A needs to be assembled from: worker1 (non-vA regions) + worker2 (vA region)
   - REMAINDER COPY: Copy non-vA regions from worker1 to worker2
   - OR INVERSE: Copy vA-region from worker2 to worker1, then copy full A

MEMORY SPAN COMPUTATION:
------------------------

The system uses memory spans to determine aliasing and compute remainders:

- ContiguousAliasing: Single contiguous memory region (e.g., full array)
- StridedAliasing: Multiple non-contiguous regions (e.g., SubArray)
- DiagonalAliasing: Diagonal elements only (e.g., Diagonal(A))
- TriangularAliasing: Triangular regions (e.g., UpperTriangular(A))

Remainder computation involves:
1. Computing memory spans for all overlapping aliasing objects
2. Finding the set difference: full_object_spans - updated_spans
3. Creating a "remainder aliasing" object representing the not-yet-updated regions
4. Performing move! with this remainder object to copy only needed data

DATA MOVEMENT FUNCTIONS:
------------------------

move!(dep_mod, to_space, from_space, to, from):
- The core in-place data movement function
- dep_mod specifies which part of the data to copy (identity, UpperTriangular, etc.)
- Supports partial copies via dependency modifiers

move_rewrap():
- Handles copying of wrapped objects (SubArrays, ChunkViews)
- Ensures aliased objects are reused on destination worker

enqueue_copy_to!():
- Schedules data movement tasks before user tasks
- Ensures data is up-to-date on the worker where a task will run

CURRENT LIMITATIONS AND TODOS:
-------------------------------

1. REMAINDER COMPUTATION: 
   - The system currently handles simple overlaps but needs sophisticated 
     remainder calculation for complex aliasing patterns
   - Need functions to compute span set differences

2. ORDERING DEPENDENCIES:
   - Need to ensure remainder copies happen in correct order
   - Must not overwrite more recent updates with stale data

3. COMPLEX ALIASING PATTERNS:
   - Multiple overlapping views of the same array
   - Nested aliasing structures (views of views)
   - Mixed aliasing types (diagonal + triangular regions)

4. PERFORMANCE OPTIMIZATION:
   - Minimize number of copy operations
   - Batch compatible transfers
   - Optimize for common access patterns
=#

#= FIXME: Integrate with above
Problem statement:

Remainder copy calculation needs to ensure that, for a given argument and
dependency modifier, and for a given target memory space, any data not yet
updated (whether through this arg or through another that aliases) is added to
the remainder, while any data that has been updated is not in the remainder.
Remainder copies may be multi-part, as data may be spread across multiple other
memory spaces.

Ainfo is not alone sufficient to identify the combination of argument and
dependency modifier, as ainfo is specific to an allocation in a given memory
space. Thus, this combination needs to be tracked together, and separately from
memory space. However, information may span multiple memory spaces (and thus
multiple ainfos), so we should try to make queries of cross-memory space
information fast, as they will need to be performed for every task, for every
combination.

Game Plan:

- Use ArgumentWrapper to track this combination throughout the codebase, ideally generated just once
- Maintain the keying of remote_args only on argument, as the dependency modifier doesn’t affect the argument being passed into the task, so it should not factor into generating and tracking remote argument copies
- Add a structure to track the mapping from ArgumentWrapper to memory space to ainfo, as a quick way to lookup all ainfos needing to be considered
- When considering a remainder copy, only look at a single memory space’s ainfos at a time, as the ainfos should overlap exactly the same way on any memory space, and this allows us to use ainfo_overlaps to track overlaps
- Remainder copies will need to separately consider the source memory space, and the destination memory space when acquiring spans to copy to/from
- Memory spans for ainfos generated from the same ArgumentWrapper should be assumed to be paired in the same order, regardless of memory space, to ensure we can perform the translation from source to destination span address
    - Alternatively, we might provide an API to take source and destination ainfos, and desired remainder memory spans, which then performs the copy for us
- When a task or copy writes to arguments, we should record this happening for all overlapping ainfos, in a manner that will be efficient to query from another memory space. We can probably walk backwards and attach this to a structure keyed on ArgumentWrapper, as that will be very efficient for later queries (because the history will now be linearized in one vector).
- Remainder copies will need to know, for all overlapping ainfos of the ArgumentWrapper ainfo at the target memory space, how recently that ainfo was updated relative to other ainfos, and relative to how recently the target ainfo was written.
    - The last time the target ainfo was written is the furthest back we need to consider, as the target data must have been fully up-to-date when that write completed.
    - Consideration of updates should start at most recent first, walking backwards in time, as the most recent updates contain the up-to-date data.
        - For each span under consideration, we should subtract from it the current remainder set, to ensure we only copy up-to-date data.
        - We must add that span portion to the remainder set no matter what, but if it was updated on the target memory space, we don’t need to schedule a copy for it, since it’s already where it needs to be.
    - Even before the last target write is seen, we are allowed to stop searching if we find that our target ainfo is fully covered (because this implies that the target ainfo is fully out-of-date).
=#

"Specifies a read-only dependency."
struct In{T}
    x::T
end
"Specifies a write-only dependency."
struct Out{T}
    x::T
end
"Specifies a read-write dependency."
struct InOut{T}
    x::T
end
"Specifies one or more dependencies."
struct Deps{T,DT<:Tuple}
    x::T
    deps::DT
end
Deps(x, deps...) = Deps(x, deps)

function unwrap_inout(arg)
    readdep = false
    writedep = false
    if arg isa In
        readdep = true
        arg = arg.x
    elseif arg isa Out
        writedep = true
        arg = arg.x
    elseif arg isa InOut
        readdep = true
        writedep = true
        arg = arg.x
    elseif arg isa Deps
        alldeps = Tuple[]
        for dep in arg.deps
            dep_mod, inner_deps = unwrap_inout(dep)
            for (_, readdep, writedep) in inner_deps
                push!(alldeps, (dep_mod, readdep, writedep))
            end
        end
        arg = arg.x
        return arg, alldeps
    else
        readdep = true
    end
    return arg, Tuple[(identity, readdep, writedep)]
end

_identity_hash(arg, h::UInt=UInt(0)) = ismutable(arg) ? objectid(arg) : hash(arg, h)
_identity_hash(arg::SubArray, h::UInt=UInt(0)) = hash(arg.indices, hash(arg.offset1, hash(arg.stride1, _identity_hash(arg.parent, h))))
_identity_hash(arg::CartesianIndices, h::UInt=UInt(0)) = hash(arg.indices, hash(typeof(arg), h))

struct ArgumentWrapper
    arg
    dep_mod
    hash::UInt

    function ArgumentWrapper(arg, dep_mod)
        h = hash(dep_mod)
        h = _identity_hash(arg, h)
        return new(arg, dep_mod, h)
    end
end
Base.hash(aw::ArgumentWrapper) = hash(ArgumentWrapper, aw.hash)
Base.:(==)(aw1::ArgumentWrapper, aw2::ArgumentWrapper) =
    aw1.hash == aw2.hash

struct DataDepsState
    # The ordered list of tasks and their read/write dependencies
    # FIXME: Remove me
    #dependencies::Vector{Pair{DTask,Vector{Tuple{Bool,Bool,AliasingWrapper,<:Any,<:Any}}}}

    # The origin memory space of each argument
    # Used to track the original location of an argument, for final copy-from
    arg_origin::IdDict{Any,MemorySpace}

    # The mapping of memory space to argument to remote argument copies
    # Used to replace an argument with its remote copy
    remote_args::Dict{MemorySpace,IdDict{Any,Chunk}}

    # The mapping of argument and dep_mod to memory space to ainfo
    # Used to lookup which ainfos represent a given argument and dep_mod
    arg_ainfos::IdDict{ArgumentWrapper,Dict{MemorySpace,AliasingWrapper}}

    # The mapping of ainfo to argument and dep_mod
    # Used to lookup which argument and dep_mod a given ainfo is generated from
    ainfos_arg::Dict{AliasingWrapper,ArgumentWrapper}

    # The history of writes (direct or indirect) to each argument and dep_mod, in terms of ainfos directly written to
    # Updated when a new write happens on an overlapping ainfo
    # Used by remainder copies to track which portions of an argument and dep_mod are up-to-date
    arg_history::Dict{ArgunmentWrapper,Vector{AliasingWrapper}}

    # The mapping of memory space and argument to the memory space of the last direct write
    # Used by remainder copies to lookup the "backstop" if any portion of the target ainfo is not updated by the remainder
    arg_owner::IdDict{ArgumentWrapper,MemorySpace}

    # The mapping of, for a given memory space, the backing Chunks that an ainfo references
    # Used by slot generation to replace the backing Chunks during move
    ainfo_backing_chunk::Dict{MemorySpace,Dict{AbstractAliasing,Chunk}}

    # The history of aliasing objects for each argument
    # FIXME: Remove me
    #args_history::Dict{AliasingWrapper,Vector{AliasingWrapper}}

    # Cache of argument's supports_inplace_move query result
    supports_inplace_cache::IdDict{Any,Bool}

    # Cache of argument and dep_mod to ainfo
    ainfo_cache::Dict{ArgumentWrapper,AliasingWrapper}

    # The overlapping ainfos for each ainfo
    # Incrementally updated as new ainfos are created
    # Used for fast will_alias lookups
    ainfos_overlaps::Dict{AliasingWrapper,Set{AliasingWrapper}}

    # Track writers ("owners") and readers
    # FIXME: Remove me?
    ainfos_owner::Dict{AliasingWrapper,Union{Pair{DTask,Int},Nothing}}
    ainfos_readers::Dict{AliasingWrapper,Vector{Pair{DTask,Int}}}

    # Remainder tracking: for each base aliasing object, track which sub-regions have been updated
    # This maps base_ainfo => Set{updated_sub_ainfos}
    # FIXME: Remove me
    #updated_regions::Dict{AliasingWrapper,Set{AliasingWrapper}}

    # Track the base aliasing object for each sub-region
    # This maps sub_ainfo => base_ainfo
    # FIXME: Remove me
    #region_to_base::Dict{AliasingWrapper,AliasingWrapper}

    function DataDepsState(aliasing::Bool)
        if !aliasing
            @warn "aliasing=false is no longer supported, aliasing is now always enabled" maxlog=1
        end

        arg_origin = IdDict{Any,MemorySpace}()
        remote_args = Dict{MemorySpace,IdDict{Any,Any}}()
        arg_ainfos = IdDict{ArgumentWrapper,Dict{MemorySpace,AliasingWrapper}}()
        ainfos_arg = Dict{AliasingWrapper,ArgumentWrapper}()
        arg_owner = IdDict{Any,MemorySpace}()
        ainfo_backing_chunk = Dict{MemorySpace,Dict{AbstractAliasing,Chunk}}()
        arg_history = Dict{AliasingWrapper,Vector{AliasingWrapper}}()

        supports_inplace_cache = IdDict{Any,Bool}()
        ainfo_cache = Dict{ArgumentWrapper,AliasingWrapper}()

        ainfos_overlaps = Dict{AliasingWrapper,Set{AliasingWrapper}}()

        ainfos_owner = Dict{AliasingWrapper,Union{Pair{DTask,Int},Nothing}}()
        ainfos_readers = Dict{AliasingWrapper,Vector{Pair{DTask,Int}}}()

        #updated_regions = Dict{AliasingWrapper,Set{AliasingWrapper}}()
        #region_to_base = Dict{AliasingWrapper,AliasingWrapper}()

        return new(arg_origin, remote_args, arg_ainfos, ainfos_arg, arg_owner, ainfo_backing_chunk, arg_history,
                   supports_inplace_cache, ainfo_cache, ainfos_overlaps, ainfos_owner, ainfos_readers)
    end
end

function aliasing(state::DataDepsState, aw::ArgumentWrapper)
    arg = aw.arg
    dep_mod = aw.dep_mod
    return get!(state.ainfo_cache, aw) do
        return AliasingWrapper(aliasing(arg, dep_mod))
    end
end
aliasing(state::DataDepsState, arg, dep_mod) = aliasing(state, ArgumentWrapper(arg, dep_mod))

function supports_inplace_move(state::DataDepsState, arg)
    return get!(state.supports_inplace_cache, arg) do
        return supports_inplace_move(arg)
    end
end

# Determine which arguments could be written to, and thus need tracking
"Whether `arg` is written to by `task`."
function is_writedep(arg, deps, task::DTask)
    return any(dep->dep[3], deps)
end

# Aliasing state setup
function populate_task_info!(state::DataDepsState, spec::DTaskSpec, task::DTask)
    # Populate task dependencies
    #dependencies_to_add = Vector{Tuple{Bool,Bool,AliasingWrapper,<:Any,<:Any}}()

    # Track the task's arguments and access patterns
    for (idx, (pos, arg)) in enumerate(spec.args)
        # Unwrap In/InOut/Out wrappers and record dependencies
        arg, deps = unwrap_inout(arg)

        # Unwrap the Chunk underlying any DTask arguments
        arg = arg isa DTask ? fetch(arg; raw=true) : arg

        # Skip non-aliasing arguments
        type_may_alias(typeof(arg)) || continue

        # Populate argument info and generate slots
        origin_space = memory_space(arg)
        ainfo = aliasing(state, arg, identity)
        populate_argument_info!(state, ainfo, origin_space)
        get_or_generate_slot!(state, origin_space, arg, deps)

        # Add all aliasing dependencies
        for (dep_mod, readdep, writedep) in deps
            ainfo = aliasing(state, arg, dep_mod)
            #push!(dependencies_to_add, (readdep, writedep, ainfo, dep_mod, arg))
            state.args_history[ainfo] = Vector{AliasingWrapper}()
        end
    end

    # Track the task result too
    # N.B. We state no readdep/writedep because, while we can't model the aliasing info for the task result yet, we don't want to synchronize because of this
    #push!(dependencies_to_add, (false, false, AliasingWrapper(UnknownAliasing()), identity, task))

    # Record argument/result dependencies
    #push!(state.dependencies, task => dependencies_to_add)
end
function record_argument_dep!(state::DataDepsState, task::DTask, arg, deps, dest_space::MemorySpace)
    deps_idx = findfirst(t->t[1] === task, state.dependencies)
    deps_record = state.dependencies[deps_idx][2]
    for (dep_mod, readdep, writedep) in deps
        ainfo = aliasing(state, arg, dep_mod)
        push!(deps_record, (readdep, writedep, ainfo, dep_mod, arg))
    end
end
function populate_argument_info!(state::DataDepsState, ainfo::AbstractAliasing, origin_space::MemorySpace)
    # Initialize owner and readers
    if !haskey(state.ainfos_owner, ainfo)
        overlaps = Set{AliasingWrapper}()
        push!(overlaps, ainfo)
        for other_ainfo in keys(state.ainfos_owner)
            ainfo == other_ainfo && continue
            if will_alias(ainfo, other_ainfo)
                push!(overlaps, other_ainfo)
                push!(state.ainfos_overlaps[other_ainfo], ainfo)
            end
        end
        state.ainfos_overlaps[ainfo] = overlaps
        state.ainfos_owner[ainfo] = nothing
        state.ainfos_readers[ainfo] = Pair{DTask,Int}[]
        state.ainfos_history[ainfo] = AliasingWrapper[]
    end

    # Assign data owner and locality
    if !haskey(state.data_locality, ainfo)
        @info "[$origin_space] Populating argument info for $ainfo ($(length(overlaps)) overlaps)"
        state.data_locality[ainfo] = origin_space
        state.data_origin[ainfo] = origin_space
    end
end
function populate_return_info!(state::DataDepsState, task, space)
    @assert !haskey(state.data_locality, task)
    # FIXME: We don't yet know about ainfos for this task
end

"""
    supports_inplace_move(x) -> Bool

Returns `false` if `x` doesn't support being copied into from another object
like `x`, via `move!`. This is used in `spawn_datadeps` to prevent attempting
to copy between values which don't support mutation or otherwise don't have an
implemented `move!` and want to skip in-place copies. When this returns
`false`, datadeps will instead perform out-of-place copies for each non-local
use of `x`, and the data in `x` will not be updated when the `spawn_datadeps`
region returns.
"""
supports_inplace_move(x) = true
supports_inplace_move(t::DTask) = supports_inplace_move(fetch(t; raw=true))
function supports_inplace_move(c::Chunk)
    # FIXME: Use MemPool.access_ref
    pid = root_worker_id(c.processor)
    if pid == myid()
        return supports_inplace_move(poolget(c.handle))
    else
        return remotecall_fetch(supports_inplace_move, pid, c)
    end
end
supports_inplace_move(::Function) = false

# Read/write dependency management
function get_write_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo_or_arg, task, write_num, syncdeps)
    _get_write_deps!(state, dest_space, ainfo_or_arg, task, write_num, syncdeps)
    _get_read_deps!(state, dest_space, ainfo_or_arg, task, write_num, syncdeps)
end
function get_read_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo_or_arg, task, write_num, syncdeps)
    _get_write_deps!(state, dest_space, ainfo_or_arg, task, write_num, syncdeps)
end

function _get_write_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, task, write_num, syncdeps)
    ainfo.inner isa NoAliasing && return
    for other_ainfo in state.ainfos_overlaps[ainfo]
        other_task_write_num = state.ainfos_owner[other_ainfo]
        @dagdebug nothing :spawn_datadeps_sync "Considering sync with writer via $ainfo -> $other_ainfo"
        other_task_write_num === nothing && continue
        other_task, other_write_num = other_task_write_num
        write_num == other_write_num && continue
        @dagdebug nothing :spawn_datadeps_sync "Sync with writer via $ainfo -> $other_ainfo"
        push!(syncdeps, other_task)
        #@info "[$dest_space] Adding $ainfo to history of $other_ainfo"
        #push!(state.ainfos_history[other_ainfo], ainfo)
    end
end
function _get_read_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, task, write_num, syncdeps)
    ainfo.inner isa NoAliasing && return
    for other_ainfo in state.ainfos_overlaps[ainfo]
        @dagdebug nothing :spawn_datadeps_sync "Considering sync with reader via $ainfo -> $other_ainfo"
        other_tasks = state.ainfos_readers[other_ainfo]
        for (other_task, other_write_num) in other_tasks
            write_num == other_write_num && continue
            @dagdebug nothing :spawn_datadeps_sync "Sync with reader via $ainfo -> $other_ainfo"
            push!(syncdeps, other_task)
        end
    end
end
function add_writer!(state::DataDepsState, ainfo::AbstractAliasing, task, write_num)
    state.ainfos_owner[ainfo] = task=>write_num
    empty!(state.ainfos_readers[ainfo])
    # Not necessary to assert a read, but conceptually it's true
    add_reader!(state, ainfo, task, write_num)
end
function add_reader!(state::DataDepsState, ainfo::AbstractAliasing, task, write_num)
    push!(state.ainfos_readers[ainfo], task=>write_num)
end

# Make a copy of each piece of data on each worker
# memory_space => {arg => copy_of_arg}
isremotehandle(x) = false
isremotehandle(x::DTask) = true
isremotehandle(x::Chunk) = true
@warn "Use state for aliasing checks" maxlog=1
@warn "Re-enable assertions" maxlog=1
@warn "DON'T PULL FROM ORIGIN BLINDLY, IT'S NOT ALWAYS UP TO DATE" maxlog=1
function generate_slot!(state::DataDepsState, dest_space, data, deps)
    if data isa DTask
        data = fetch(data; raw=true)
    end
    # FIXME: Do proper sync with current owner of data, and then grab it
    orig_space = memory_space(data)
    to_proc = first(processors(dest_space))
    from_proc = first(processors(orig_space))
    dest_space_args = get!(IdDict{Any,Any}, state.remote_args, dest_space)
    ALIASED_OBJECT_CACHE[] = get!(Dict{AbstractAliasing,Chunk}, state.ainfo_backing_chunk, dest_space)
    ALIASED_OBJECTS[] = Vector{Any}()
    STATE[] = state
    if orig_space == dest_space && (data isa Chunk || !isremotehandle(data))
        # Fast path for local data or data already in a Chunk
        data_chunk = tochunk(data, from_proc)
        @assert processor(data_chunk) in processors(dest_space) || data isa Chunk && processor(data) isa Dagger.OSProc
        @assert memory_space(data_chunk) == orig_space
    else
        ctx = Sch.eager_context()
        id = rand(Int)
        timespan_start(ctx, :move, (;thunk_id=0, id, position=0, processor=to_proc), (;f=nothing, data))
        data_chunk = move_rewrap(from_proc, to_proc, data)
        timespan_finish(ctx, :move, (;thunk_id=0, id, position=0, processor=to_proc), (;f=nothing, data=data_chunk))
        @assert processor(data_chunk) in processors(dest_space)
        #@assert memory_space(data_converted) == memory_space(data_chunk) "space mismatch! $(memory_space(data_converted)) != $(memory_space(data_chunk)) ($(typeof(data_converted)) vs. $(typeof(data_chunk))), spaces ($orig_space -> $dest_space)"
        if orig_space != dest_space
            @assert orig_space != memory_space(data_chunk) "space preserved! $orig_space != $(memory_space(data_chunk)) ($(typeof(data)) vs. $(typeof(data_chunk))), spaces ($orig_space -> $dest_space)"
        end
    end
    # ainfo = aliasing(state, data, identity)
    # ALIASED_OBJECT_CACHE[][ainfo] = data_chunk
    dest_space_args[data] = data_chunk
    state.arg_owner[data] = orig_space

    # Join aliased objects to the base object
    for obj in ALIASED_OBJECTS[]
        base_ainfo = aliasing(state, obj, identity)
        populate_argument_info!(state, base_ainfo, dest_space)
        for (dep_mod, _, _) in deps
            aliased_ainfo = aliasing(state, data_chunk, dep_mod)
            state.region_to_base[aliased_ainfo] = base_ainfo
            populate_argument_info!(state, aliased_ainfo, dest_space)
        end
    end

    ALIASED_OBJECTS[] = nothing
    ALIASED_OBJECT_CACHE[] = nothing
    STATE[] = nothing

    return dest_space_args[data]
end
function get_or_generate_slot!(state, dest_space, data, deps)
    if !haskey(state.remote_args, dest_space)
        state.remote_args[dest_space] = IdDict{Any,Any}()
    end
    if !haskey(state.remote_args[dest_space], data)
        return generate_slot!(state, dest_space, data, deps)
    end
    return state.remote_args[dest_space][data]
end
function move_rewrap(from_proc::Processor, to_proc::Processor, data)
    return aliased_object!(data) do data
        to_w = root_worker_id(to_proc)
        return remotecall_fetch(to_w, from_proc, to_proc, data) do from_proc, to_proc, data
            data_converted = move(from_proc, to_proc, data)
            return tochunk(data_converted, to_proc)
        end
    end
end
const STATE = TaskLocalValue{Union{DataDepsState,Nothing}}(()->nothing)
const ALIASED_OBJECT_CACHE = TaskLocalValue{Union{Dict{AbstractAliasing,Chunk}, Nothing}}(()->nothing)
const ALIASED_OBJECTS = TaskLocalValue{Union{Vector{Any}, Nothing}}(()->nothing)
@warn "Document these public methods" maxlog=1
function declare_aliased_object!(x)
    push!(ALIASED_OBJECTS[], x)
end
function aliased_object!(x; ainfo=aliasing(STATE[], x, identity))
    cache = ALIASED_OBJECT_CACHE[]
    if haskey(cache, ainfo)
        y = cache[ainfo]
    else
        @assert x isa Chunk "x must be a Chunk\nUse functor form of aliased_object!"
        cache[ainfo] = x
        y = x
    end
    push!(ALIASED_OBJECTS[], y)
    return y
end
function aliased_object!(f, x; ainfo=aliasing(STATE[], x, identity))
    cache = ALIASED_OBJECT_CACHE[]
    if haskey(cache, ainfo)
        y = cache[ainfo]
    else
        y = f(x)
        @assert y isa Chunk "Didn't get a Chunk from functor"
        cache[ainfo] = y
    end
    push!(ALIASED_OBJECTS[], y)
    return y
end
function aliased_object_unwrap!(x::Chunk)
    y = unwrap(x)
    ainfo = aliasing(STATE[], y, identity)
    return unwrap(aliased_object!(x; ainfo))
end

struct DataDepsSchedulerState
    task_to_spec::Dict{DTask,DTaskSpec}
    assignments::Dict{DTask,MemorySpace}
    dependencies::Dict{DTask,Set{DTask}}
    task_completions::Dict{DTask,UInt64}
    space_completions::Dict{MemorySpace,UInt64}
    capacities::Dict{MemorySpace,Int}

    function DataDepsSchedulerState()
        return new(Dict{DTask,DTaskSpec}(),
                   Dict{DTask,MemorySpace}(),
                   Dict{DTask,Set{DTask}}(),
                   Dict{DTask,UInt64}(),
                   Dict{MemorySpace,UInt64}(),
                   Dict{MemorySpace,Int}())
    end
end