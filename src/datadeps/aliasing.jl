import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv

export In, Out, InOut, Deps, spawn_datadeps

#=
==============================================================================
                    DATADEPS ALIASING AND DATA MOVEMENT SYSTEM
==============================================================================

This file implements the data dependencies system for Dagger tasks, which allows
tasks to access their arguments in a controlled manner. The system maintains
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

ALIASING INFO:
--------------

The system uses different types of aliasing info to represent different types of
aliasing relationships:

- ContiguousAliasing: Single contiguous memory region (e.g., full array)
- StridedAliasing: Multiple non-contiguous regions (e.g., SubArray)
- DiagonalAliasing: Diagonal elements only (e.g., Diagonal(A))
- TriangularAliasing: Triangular regions (e.g., UpperTriangular(A))

Any two aliasing objects can be compared using the will_alias function to
determine if they overlap. Additionally, any aliasing object can be converted to
a vector of memory spans, which represents the contiguous regions of memory that
the aliasing object covers.

DATA MOVEMENT FUNCTIONS:
------------------------

move!(dep_mod, to_space, from_space, to, from):
- The core in-place data movement function
- dep_mod specifies which part of the data to copy (identity, UpperTriangular, etc.)
- Supports partial copies via RemainderAliasing dependency modifiers

move_rewrap(...):
- Handles copying of wrapped objects (SubArrays, ChunkViews)
- Ensures aliased objects are reused on destination worker

read/write_remainder!(...):
- Read/write a span of memory from an object to/from a buffer
- Used by move! to copy the remainder of an aliased object

THE DISTRIBUTED ALIASING PROBLEM:
---------------------------------

In a multithreaded environment, aliasing "just works" because all tasks operate
on the user-provided memory. However, in a distributed environment, arguments
must be copied between workers, which breaks aliasing relationships if care is
not taken.

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
   - This prevents overwrites of data that has already been updated by another task
   - This also minimizes network traffic and overall copy time
   - Uses the move!(dep_mod, ...) function with RemainderAliasing dependency modifiers

3. REMAINDER TRACKING:
   - When a task needs the full object, copy partial regions as needed
   - When a partial region is updated, track what parts still need updating
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
   - Create vA on worker2 pointing to the appropriate region of A
   - T2 executes, modifying vA region on worker2
   - Update: vA's data_locality = worker2

4. FINAL SYNCHRONIZATION:
   - Need to copy-back A and vA to worker0
   - A needs to be assembled from: worker1 (non-vA regions of A) + worker2 (vA region of A)
   - REMAINDER COPY: Copy non-vA regions from worker1 to worker0
   - REMAINDER COPY: Copy vA region from worker2 to worker0

REMAINDER COMPUTATION:
----------------------

Remainder computation involves:
1. Computing memory spans for all overlapping aliasing objects
2. Finding the set difference: full_object_spans - updated_spans
3. Creating a RemainderAliasing object representing the difference between spans
4. Performing one or more move! calls with this RemainderAliasing object to copy only needed data
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

chunktype(::In{T}) where T = T
chunktype(::Out{T}) where T = T
chunktype(::InOut{T}) where T = T
chunktype(::Deps{T,DT}) where {T,DT} = T

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
_identity_hash(arg::Chunk, h::UInt=UInt(0)) = hash(arg.handle, hash(Chunk, h))
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
Base.isequal(aw1::ArgumentWrapper, aw2::ArgumentWrapper) =
    aw1.hash == aw2.hash

struct HistoryEntry
    ainfo::AliasingWrapper
    space::MemorySpace
    write_num::Int
end

struct DataDepsState
    # The mapping of original raw argument to its Chunk
    raw_arg_to_chunk::IdDict{Any,Chunk}

    # The origin memory space of each argument
    # Used to track the original location of an argument, for final copy-from
    arg_origin::IdDict{Any,MemorySpace}

    # The mapping of memory space to argument to remote argument copies
    # Used to replace an argument with its remote copy
    remote_args::Dict{MemorySpace,IdDict{Any,Chunk}}

    # The mapping of remote argument to original argument
    remote_arg_to_original::IdDict{Any,Any}

    # The mapping of original argument wrapper to remote argument wrapper
    remote_arg_w::Dict{ArgumentWrapper,Dict{MemorySpace,ArgumentWrapper}}

    # The mapping of ainfo to argument and dep_mod
    # Used to lookup which argument and dep_mod a given ainfo is generated from
    # N.B. This is a mapping for remote argument copies
    ainfo_arg::Dict{AliasingWrapper,ArgumentWrapper}

    # The history of writes (direct or indirect) to each argument and dep_mod, in terms of ainfos directly written to, and the memory space they were written to
    # Updated when a new write happens on an overlapping ainfo
    # Used by remainder copies to track which portions of an argument and dep_mod were written to elsewhere, through another argument
    arg_history::Dict{ArgumentWrapper,Vector{HistoryEntry}}

    # The mapping of memory space and argument to the memory space of the last direct write
    # Used by remainder copies to lookup the "backstop" if any portion of the target ainfo is not updated by the remainder
    arg_owner::Dict{ArgumentWrapper,MemorySpace}

    # The overlap of each argument with every other argument, based on the ainfo overlaps
    # Incrementally updated as new ainfos are created
    # Used for fast history updates
    arg_overlaps::Dict{ArgumentWrapper,Set{ArgumentWrapper}}

    # The mapping of, for a given memory space, the backing Chunks that an ainfo references
    # Used by slot generation to replace the backing Chunks during move
    ainfo_backing_chunk::Dict{MemorySpace,Dict{AbstractAliasing,Chunk}}

    # Cache of argument's supports_inplace_move query result
    supports_inplace_cache::IdDict{Any,Bool}

    # Cache of argument and dep_mod to ainfo
    # N.B. This is a mapping for remote argument copies
    ainfo_cache::Dict{ArgumentWrapper,AliasingWrapper}

    # The oracle for aliasing lookups
    # Used to populate ainfos_overlaps efficiently
    ainfos_lookup::AliasingLookup

    # The overlapping ainfos for each ainfo
    # Incrementally updated as new ainfos are created
    # Used for fast will_alias lookups
    ainfos_overlaps::Dict{AliasingWrapper,Set{AliasingWrapper}}

    # Track writers ("owners") and readers
    # Updated as new writer and reader tasks are launched
    # Used by task dependency tracking to calculate syncdeps and ensure correct launch ordering
    ainfos_owner::Dict{AliasingWrapper,Union{Pair{DTask,Int},Nothing}}
    ainfos_readers::Dict{AliasingWrapper,Vector{Pair{DTask,Int}}}

    function DataDepsState(aliasing::Bool)
        if !aliasing
            @warn "aliasing=false is no longer supported, aliasing is now always enabled" maxlog=1
        end

        arg_to_chunk = IdDict{Any,Chunk}()
        arg_origin = IdDict{Any,MemorySpace}()
        remote_args = Dict{MemorySpace,IdDict{Any,Any}}()
        remote_arg_to_original = IdDict{Any,Any}()
        remote_arg_w = Dict{ArgumentWrapper,Dict{MemorySpace,ArgumentWrapper}}()
        ainfo_arg = Dict{AliasingWrapper,ArgumentWrapper}()
        arg_owner = Dict{ArgumentWrapper,MemorySpace}()
        arg_overlaps = Dict{ArgumentWrapper,Set{ArgumentWrapper}}()
        ainfo_backing_chunk = Dict{MemorySpace,Dict{AbstractAliasing,Chunk}}()
        arg_history = Dict{ArgumentWrapper,Vector{HistoryEntry}}()

        supports_inplace_cache = IdDict{Any,Bool}()
        ainfo_cache = Dict{ArgumentWrapper,AliasingWrapper}()

        ainfos_lookup = AliasingLookup()
        ainfos_overlaps = Dict{AliasingWrapper,Set{AliasingWrapper}}()

        ainfos_owner = Dict{AliasingWrapper,Union{Pair{DTask,Int},Nothing}}()
        ainfos_readers = Dict{AliasingWrapper,Vector{Pair{DTask,Int}}}()

        return new(arg_to_chunk, arg_origin, remote_args, remote_arg_to_original, remote_arg_w, ainfo_arg, arg_owner, arg_overlaps, ainfo_backing_chunk, arg_history,
                   supports_inplace_cache, ainfo_cache, ainfos_lookup, ainfos_overlaps, ainfos_owner, ainfos_readers)
    end
end

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
function populate_task_info!(state::DataDepsState, task_args, spec::DTaskSpec, task::DTask)
    # Track the task's arguments and access patterns
    return map_or_ntuple(task_args) do idx
        _arg = task_args[idx]

        # Unwrap the argument
        _arg_with_deps = value(_arg)
        pos = _arg.pos

        # Unwrap In/InOut/Out wrappers and record dependencies
        arg_pre_unwrap, deps = unwrap_inout(_arg_with_deps)

        # Unwrap the Chunk underlying any DTask arguments
        arg = arg_pre_unwrap isa DTask ? fetch(arg_pre_unwrap; raw=true) : arg_pre_unwrap

        # Skip non-aliasing arguments or arguments that don't support in-place move
        may_alias = type_may_alias(typeof(arg))
        inplace_move = may_alias && supports_inplace_move(state, arg)
        if !may_alias || !inplace_move
            arg_w = ArgumentWrapper(arg, identity)
            if is_typed(spec)
                return TypedDataDepsTaskArgument(arg, pos, may_alias, inplace_move, (DataDepsTaskDependency(arg_w, false, false),))
            else
                return DataDepsTaskArgument(arg, pos, may_alias, inplace_move, [DataDepsTaskDependency(arg_w, false, false)])
            end
        end

        # Generate a Chunk for the argument if necessary
        if haskey(state.raw_arg_to_chunk, arg)
            arg_chunk = state.raw_arg_to_chunk[arg]
        else
            if !(arg isa Chunk)
                arg_chunk = tochunk(arg)
                state.raw_arg_to_chunk[arg] = arg_chunk
            else
                state.raw_arg_to_chunk[arg] = arg
                arg_chunk = arg
            end
        end

        # Track the origin space of the argument
        origin_space = memory_space(arg_chunk)
        state.arg_origin[arg_chunk] = origin_space
        state.remote_arg_to_original[arg_chunk] = arg_chunk

        # Populate argument info for all aliasing dependencies
        # And return the argument, dependencies, and ArgumentWrappers
        if is_typed(spec)
            deps = Tuple(DataDepsTaskDependency(arg_chunk, dep) for dep in deps)
            map_or_ntuple(deps) do dep_idx
                dep = deps[dep_idx]
                # Populate argument info
                populate_argument_info!(state, dep.arg_w, origin_space)
            end
            return TypedDataDepsTaskArgument(arg_chunk, pos, may_alias, inplace_move, deps)
        else
            deps = [DataDepsTaskDependency(arg_chunk, dep) for dep in deps]
            map_or_ntuple(deps) do dep_idx
                dep = deps[dep_idx]
                # Populate argument info
                populate_argument_info!(state, dep.arg_w, origin_space)
            end
            return DataDepsTaskArgument(arg_chunk, pos, may_alias, inplace_move, deps)
        end
    end
end
function populate_argument_info!(state::DataDepsState, arg_w::ArgumentWrapper, origin_space::MemorySpace)
    # Initialize ownership and history
    if !haskey(state.arg_owner, arg_w)
        # N.B. This is valid (even if the backing data is up-to-date elsewhere),
        # because we only use this to track the "backstop" if any portion of the
        # target ainfo is not updated by the remainder (at which point, this
        # is thus the correct owner).
        state.arg_owner[arg_w] = origin_space

        # Initialize the overlap set
        state.arg_overlaps[arg_w] = Set{ArgumentWrapper}()
    end
    if !haskey(state.arg_history, arg_w)
        state.arg_history[arg_w] = Vector{HistoryEntry}()
    end

    # Calculate the ainfo (which will populate ainfo structures and merge history)
    aliasing!(state, origin_space, arg_w)
end
# N.B. arg_w must be the original argument wrapper, not a remote copy
function aliasing!(state::DataDepsState, target_space::MemorySpace, arg_w::ArgumentWrapper)
    if haskey(state.remote_arg_w, arg_w) && haskey(state.remote_arg_w[arg_w], target_space)
        remote_arg_w = @inbounds state.remote_arg_w[arg_w][target_space]
        remote_arg = remote_arg_w.arg
    else
        # Grab the remote copy of the argument, and calculate the ainfo
        remote_arg = get_or_generate_slot!(state, target_space, arg_w.arg)
        remote_arg_w = ArgumentWrapper(remote_arg, arg_w.dep_mod)
        get!(Dict{MemorySpace,ArgumentWrapper}, state.remote_arg_w, arg_w)[target_space] = remote_arg_w
    end

    # Check if we already have the result cached
    if haskey(state.ainfo_cache, remote_arg_w)
        return state.ainfo_cache[remote_arg_w]
    end

    # Calculate the ainfo
    ainfo = AliasingWrapper(aliasing(remote_arg, arg_w.dep_mod))

    # Cache the result
    state.ainfo_cache[remote_arg_w] = ainfo

    # Update the mapping of ainfo to argument and dep_mod
    if !haskey(state.ainfo_arg, ainfo)
        state.ainfo_arg[ainfo] = remote_arg_w
    else
        @assert state.ainfo_arg[ainfo] == remote_arg_w
    end

    # Populate info for the new ainfo
    populate_ainfo!(state, arg_w, ainfo, target_space)

    return ainfo
end
function populate_ainfo!(state::DataDepsState, original_arg_w::ArgumentWrapper, target_ainfo::AliasingWrapper, target_space::MemorySpace)
    if !haskey(state.ainfos_owner, target_ainfo)
        # Add ourselves to the lookup oracle
        ainfo_idx = push!(state.ainfos_lookup, target_ainfo)

        # Find overlapping ainfos
        overlaps = Set{AliasingWrapper}()
        push!(overlaps, target_ainfo)
        for other_ainfo in intersect(state.ainfos_lookup, target_ainfo; ainfo_idx)
            target_ainfo == other_ainfo && continue
            # Mark us and them as overlapping
            push!(overlaps, other_ainfo)
            push!(state.ainfos_overlaps[other_ainfo], target_ainfo)

            # Add overlapping history to our own
            other_remote_arg_w = state.ainfo_arg[other_ainfo]
            other_arg = state.remote_arg_to_original[other_remote_arg_w.arg]
            other_arg_w = ArgumentWrapper(other_arg, other_remote_arg_w.dep_mod)
            push!(state.arg_overlaps[original_arg_w], other_arg_w)
            push!(state.arg_overlaps[other_arg_w], original_arg_w)
            merge_history!(state, original_arg_w, other_arg_w)
        end
        state.ainfos_overlaps[target_ainfo] = overlaps

        # Initialize owner and readers
        state.ainfos_owner[target_ainfo] = nothing
        state.ainfos_readers[target_ainfo] = Pair{DTask,Int}[]
    end
end
function merge_history!(state::DataDepsState, arg_w::ArgumentWrapper, other_arg_w::ArgumentWrapper)
    history = state.arg_history[arg_w]
    @opcounter :merge_history
    @opcounter :merge_history_complexity length(history)
    largest_value_update!(length(history))
    origin_space = state.arg_origin[other_arg_w.arg]
    for other_entry in state.arg_history[other_arg_w]
        write_num_tuple = HistoryEntry(AliasingWrapper(NoAliasing()), origin_space, other_entry.write_num)
        range = searchsorted(history, write_num_tuple; by=x->x.write_num)
        if !isempty(range)
            # Find and skip duplicates
            match = false
            for source_idx in range
                source_entry = history[source_idx]
                if source_entry.ainfo == other_entry.ainfo &&
                    source_entry.space == other_entry.space &&
                    source_entry.write_num == other_entry.write_num
                    match = true
                    break
                end
            end
            match && continue

            # Insert at the first position
            idx = first(range)
        else
            # Insert at the last position
            idx = length(history) + 1
        end
        insert!(history, idx, other_entry)
    end
end
function truncate_history!(state::DataDepsState, arg_w::ArgumentWrapper)
    # FIXME: Do this continuously if possible
    if haskey(state.arg_history, arg_w) && length(state.arg_history[arg_w]) > 100000
        origin_space = state.arg_origin[arg_w.arg]
        @opcounter :truncate_history
        _, last_idx = compute_remainder_for_arg!(state, origin_space, arg_w, 0; compute_syncdeps=false)
        if last_idx > 0
            @opcounter :truncate_history_removed last_idx
            deleteat!(state.arg_history[arg_w], 1:last_idx)
        end
    end
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
function get_write_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    # We need to sync with both writers and readers
    _get_write_deps!(state, dest_space, ainfo, write_num, syncdeps)
    _get_read_deps!(state, dest_space, ainfo, write_num, syncdeps)
end
function get_read_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    # We only need to sync with writers, not readers
    _get_write_deps!(state, dest_space, ainfo, write_num, syncdeps)
end

function _get_write_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    ainfo.inner isa NoAliasing && return
    for other_ainfo in state.ainfos_overlaps[ainfo]
        other_task_write_num = state.ainfos_owner[other_ainfo]
        @dagdebug nothing :spawn_datadeps_sync "Considering sync with writer via $ainfo -> $other_ainfo"
        other_task_write_num === nothing && continue
        other_task, other_write_num = other_task_write_num
        write_num == other_write_num && continue
        @dagdebug nothing :spawn_datadeps_sync "Sync with writer via $ainfo -> $other_ainfo"
        push!(syncdeps, ThunkSyncdep(other_task))
    end
end
function _get_read_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    ainfo.inner isa NoAliasing && return
    for other_ainfo in state.ainfos_overlaps[ainfo]
        @dagdebug nothing :spawn_datadeps_sync "Considering sync with reader via $ainfo -> $other_ainfo"
        other_tasks = state.ainfos_readers[other_ainfo]
        for (other_task, other_write_num) in other_tasks
            write_num == other_write_num && continue
            @dagdebug nothing :spawn_datadeps_sync "Sync with reader via $ainfo -> $other_ainfo"
            push!(syncdeps, ThunkSyncdep(other_task))
        end
    end
end
function add_writer!(state::DataDepsState, arg_w::ArgumentWrapper, dest_space::MemorySpace, ainfo::AbstractAliasing, task, write_num)
    state.ainfos_owner[ainfo] = task=>write_num
    empty!(state.ainfos_readers[ainfo])

    # Clear the history for this target, since this is a new write event
    empty!(state.arg_history[arg_w])

    # Add our own history
    push!(state.arg_history[arg_w], HistoryEntry(ainfo, dest_space, write_num))

    # Find overlapping arguments and update their history
    for other_arg_w in state.arg_overlaps[arg_w]
        other_arg_w == arg_w && continue
        push!(state.arg_history[other_arg_w], HistoryEntry(ainfo, dest_space, write_num))
    end

    # Record the last place we were fully written to
    state.arg_owner[arg_w] = dest_space

    # Not necessary to assert a read, but conceptually it's true
    add_reader!(state, arg_w, dest_space, ainfo, task, write_num)
end
function add_reader!(state::DataDepsState, arg_w::ArgumentWrapper, dest_space::MemorySpace, ainfo::AbstractAliasing, task, write_num)
    push!(state.ainfos_readers[ainfo], task=>write_num)
end

# Make a copy of each piece of data on each worker
# memory_space => {arg => copy_of_arg}
isremotehandle(x) = false
isremotehandle(x::DTask) = true
isremotehandle(x::Chunk) = true
function generate_slot!(state::DataDepsState, dest_space, data)
    if data isa DTask
        data = fetch(data; raw=true)
    end
    # N.B. We do not perform any sync/copy with the current owner of the data,
    # because all we want here is to make a copy of some version of the data,
    # even if the data is not up to date.
    orig_space = memory_space(data)
    to_proc = first(processors(dest_space))
    from_proc = first(processors(orig_space))
    dest_space_args = get!(IdDict{Any,Any}, state.remote_args, dest_space)
    ALIASED_OBJECT_CACHE[] = get!(Dict{AbstractAliasing,Chunk}, state.ainfo_backing_chunk, dest_space)
    if orig_space == dest_space && (data isa Chunk || !isremotehandle(data))
        # Fast path for local data that's already in a Chunk or not a remote handle needing rewrapping
        data_chunk = tochunk(data, from_proc)
    else
        ctx = Sch.eager_context()
        id = rand(Int)
        @maybelog ctx timespan_start(ctx, :move, (;thunk_id=0, id, position=ArgPosition(), processor=to_proc), (;f=nothing, data))
        data_chunk = move_rewrap(from_proc, to_proc, data)
        @maybelog ctx timespan_finish(ctx, :move, (;thunk_id=0, id, position=ArgPosition(), processor=to_proc), (;f=nothing, data=data_chunk))
    end
    @assert memory_space(data_chunk) == dest_space "space mismatch! $dest_space (dest) != $(memory_space(data_chunk)) (actual) ($(typeof(data)) (data) vs. $(typeof(data_chunk)) (chunk)), spaces ($orig_space -> $dest_space)"
    dest_space_args[data] = data_chunk
    state.remote_arg_to_original[data_chunk] = data

    ALIASED_OBJECT_CACHE[] = nothing

    return dest_space_args[data]
end
function get_or_generate_slot!(state, dest_space, data)
    @assert !(data isa ArgumentWrapper)
    if !haskey(state.remote_args, dest_space)
        state.remote_args[dest_space] = IdDict{Any,Any}()
    end
    if !haskey(state.remote_args[dest_space], data)
        return generate_slot!(state, dest_space, data)
    end
    return state.remote_args[dest_space][data]
end
function move_rewrap(from_proc::Processor, to_proc::Processor, data)
    return aliased_object!(data) do data
        return remotecall_endpoint(identity, from_proc, to_proc, from_space, to_space, data)
    end
end
function remotecall_endpoint(f, from_proc, to_proc, orig_space, dest_space, data)
    to_w = root_worker_id(to_proc)
    if to_w == myid()
        data_converted = f(move(from_proc, to_proc, data))
        return tochunk(data_converted, to_proc, dest_space)
    end
    return remotecall_fetch(to_w, from_proc, to_proc, dest_space, data) do from_proc, to_proc, dest_space, data
        data_converted = f(move(from_proc, to_proc, data))
        return tochunk(data_converted, to_proc, dest_space)
    end
end
const ALIASED_OBJECT_CACHE = TaskLocalValue{Union{Dict{AbstractAliasing,Chunk}, Nothing}}(()->nothing)
@warn "Document these public methods" maxlog=1
# TODO: Use state to cache aliasing() results
function declare_aliased_object!(x; ainfo=aliasing(x, identity))
    cache = ALIASED_OBJECT_CACHE[]
    cache[ainfo] = x
end
function aliased_object!(x; ainfo=aliasing(x, identity))
    cache = ALIASED_OBJECT_CACHE[]
    if haskey(cache, ainfo)
        y = cache[ainfo]
    else
        @assert x isa Chunk "x must be a Chunk\nUse functor form of aliased_object!"
        cache[ainfo] = x
        y = x
    end
    return y
end
function aliased_object!(f, x; ainfo=aliasing(x, identity))
    cache = ALIASED_OBJECT_CACHE[]
    if haskey(cache, ainfo)
        y = cache[ainfo]
    else
        y = f(x)
        @assert y isa Chunk "Didn't get a Chunk from functor"
        cache[ainfo] = y
    end
    return y
end
function aliased_object_unwrap!(x::Chunk)
    y = unwrap(x)
    ainfo = aliasing(y, identity)
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