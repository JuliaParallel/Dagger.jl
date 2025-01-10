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

@warn "Switch ArgumentWrapper to contain just the argument, and add DependencyWrapper" maxlog=1
struct DataDepsState
    # The origin memory space of each argument
    # Used to track the original location of an argument, for final copy-from
    arg_origin::IdDict{Any,MemorySpace}

    # The mapping of memory space to argument to remote argument copies
    # Used to replace an argument with its remote copy
    remote_args::Dict{MemorySpace,IdDict{Any,Chunk}}

    # The mapping of remote argument to original argument
    remote_arg_to_original::IdDict{Any,Any}

    # The mapping of ainfo to argument and dep_mod
    # Used to lookup which argument and dep_mod a given ainfo is generated from
    # N.B. This is a mapping for remote argument copies
    ainfo_arg::Dict{AliasingWrapper,ArgumentWrapper}

    # The history of writes (direct or indirect) to each argument and dep_mod, in terms of ainfos directly written to, and the memory space they were written to
    # Updated when a new write happens on an overlapping ainfo
    # Used by remainder copies to track which portions of an argument and dep_mod were written to elsewhere, through another argument
    arg_history::Dict{ArgumentWrapper,Vector{Tuple{AliasingWrapper,MemorySpace,Int}}}

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

        arg_origin = IdDict{Any,MemorySpace}()
        remote_args = Dict{MemorySpace,IdDict{Any,Any}}()
        remote_arg_to_original = IdDict{Any,Any}()
        ainfo_arg = Dict{AliasingWrapper,ArgumentWrapper}()
        arg_owner = Dict{ArgumentWrapper,MemorySpace}()
        arg_overlaps = Dict{ArgumentWrapper,Set{ArgumentWrapper}}()
        ainfo_backing_chunk = Dict{MemorySpace,Dict{AbstractAliasing,Chunk}}()
        arg_history = Dict{ArgumentWrapper,Vector{Tuple{AliasingWrapper,MemorySpace,Int}}}()

        supports_inplace_cache = IdDict{Any,Bool}()
        ainfo_cache = Dict{ArgumentWrapper,AliasingWrapper}()

        ainfos_overlaps = Dict{AliasingWrapper,Set{AliasingWrapper}}()

        ainfos_owner = Dict{AliasingWrapper,Union{Pair{DTask,Int},Nothing}}()
        ainfos_readers = Dict{AliasingWrapper,Vector{Pair{DTask,Int}}}()

        return new(arg_origin, remote_args, remote_arg_to_original, ainfo_arg, arg_owner, arg_overlaps, ainfo_backing_chunk, arg_history,
                   supports_inplace_cache, ainfo_cache, ainfos_overlaps, ainfos_owner, ainfos_readers)
    end
end

# N.B. arg_w must be the original argument wrapper, not a remote copy
function aliasing!(state::DataDepsState, target_space::MemorySpace, arg_w::ArgumentWrapper)
    # Grab the remote copy of the argument, and calculate the ainfo
    remote_arg = get_or_generate_slot!(state, target_space, arg_w.arg)
    remote_arg_w = ArgumentWrapper(remote_arg, arg_w.dep_mod)

    # Check if we already have the result cached
    if haskey(state.ainfo_cache, remote_arg_w)
        return state.ainfo_cache[remote_arg_w]
    end

    # Calculate the ainfo
    ainfo = AliasingWrapper(aliasing(current_acceleration(), remote_arg, arg_w.dep_mod))

    # Cache the result
    state.ainfo_cache[remote_arg_w] = ainfo

    # Update the mapping of ainfo to argument and dep_mod
    state.ainfo_arg[ainfo] = remote_arg_w

    # Populate info for the new ainfo
    populate_ainfo!(state, arg_w, ainfo, target_space)

    return ainfo
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
function populate_task_info!(state::DataDepsState, spec::DTaskSpec, task::DTask)
    # Track the task's arguments and access patterns
    for (idx, _arg) in enumerate(spec.fargs)
        arg = value(_arg)

        # Unwrap In/InOut/Out wrappers and record dependencies
        arg, deps = unwrap_inout(arg)

        # Unwrap the Chunk underlying any DTask arguments
        arg = arg isa DTask ? fetch(arg; move_value=false, unwrap=false) : arg

        # Skip non-aliasing arguments
        type_may_alias(typeof(arg)) || continue

        # Track the origin space of the argument
        origin_space = memory_space(arg)
        state.arg_origin[arg] = origin_space
        state.remote_arg_to_original[arg] = arg

        # Populate argument info for all aliasing dependencies
        for (dep_mod, _, _) in deps
            aw = ArgumentWrapper(arg, dep_mod)
            populate_argument_info!(state, aw, origin_space)
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
        state.arg_history[arg_w] = Vector{Tuple{AliasingWrapper,MemorySpace,Int}}()
    end

    # Calculate the ainfo (which will populate ainfo structures and merge history)
    aliasing!(state, origin_space, arg_w)
end
function populate_ainfo!(state::DataDepsState, original_arg_w::ArgumentWrapper, target_ainfo::AliasingWrapper, target_space::MemorySpace)
    # Initialize owner and readers
    if !haskey(state.ainfos_owner, target_ainfo)
        overlaps = Set{AliasingWrapper}()
        push!(overlaps, target_ainfo)
        for other_ainfo in keys(state.ainfos_owner)
            target_ainfo == other_ainfo && continue
            if will_alias(target_ainfo, other_ainfo)
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
        end
        state.ainfos_overlaps[target_ainfo] = overlaps
        state.ainfos_owner[target_ainfo] = nothing
        state.ainfos_readers[target_ainfo] = Pair{DTask,Int}[]
    end
end
function merge_history!(state::DataDepsState, arg_w::ArgumentWrapper, other_arg_w::ArgumentWrapper)
    history = state.arg_history[arg_w]
    for (other_ainfo, other_space, write_num) in state.arg_history[other_arg_w]
        idx = findfirst(h->h[3] > write_num, history)
        if idx === nothing
            if isempty(history)
                idx = 1
            else
                idx = length(history) + 1
            end
        end
        insert!(history, idx, (other_ainfo, other_space, write_num))
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
supports_inplace_move(t::DTask) = supports_inplace_move(fetch(t; move_value=false, unwrap=false))
@warn "Fix this to work with MPI (can't call poolget on the wrong rank)" maxlog=1
function supports_inplace_move(c::Chunk)
    # FIXME
    return true
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
    push!(state.arg_history[arg_w], (ainfo, dest_space, write_num))

    # Find overlapping arguments and update their history
    for other_arg_w in state.arg_overlaps[arg_w]
        other_arg_w == arg_w && continue
        push!(state.arg_history[other_arg_w], (ainfo, dest_space, write_num))
    end

    # Record the last place we were fully written to
    state.arg_owner[arg_w] = dest_space

    # Not necessary to assert a read, but conceptually it's true
    add_reader!(state, arg_w, dest_space, ainfo, task, write_num)
end
function add_reader!(state::DataDepsState, arg_w::ArgumentWrapper, dest_space::MemorySpace, ainfo::AbstractAliasing, task, write_num)
    push!(state.ainfos_readers[ainfo], task=>write_num)
end

# FIXME: These should go in MPIExt.jl
const MPI_TID = ScopedValue{Int64}(0)
const MPI_UID = ScopedValue{Int64}(0)

# Make a copy of each piece of data on each worker
# memory_space => {arg => copy_of_arg}
isremotehandle(x) = false
isremotehandle(x::DTask) = true
isremotehandle(x::Chunk) = true
function generate_slot!(state::DataDepsState, dest_space, data)
    if data isa DTask
        data = fetch(data; move_value=false, unwrap=false)
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
        task = DATADEPS_CURRENT_TASK[]
        data_chunk = with(MPI_UID=>task.uid) do
            tochunk(data, from_proc)
        end
    else
        ctx = Sch.eager_context()
        id = rand(Int)
        @maybelog ctx timespan_start(ctx, :move, (;thunk_id=0, id, position=ArgPosition(), processor=to_proc), (;f=nothing, data))
        data_chunk = move_rewrap(from_proc, to_proc, orig_space, dest_space, data)
        @maybelog ctx timespan_finish(ctx, :move, (;thunk_id=0, id, position=ArgPosition(), processor=to_proc), (;f=nothing, data=data_chunk))
    end
    @assert memory_space(data_chunk) == dest_space "space mismatch! $dest_space (dest) != $(memory_space(data_chunk)) (actual) ($(typeof(data)) (data) vs. $(typeof(data_chunk)) (chunk)), spaces ($orig_space -> $dest_space)"
    dest_space_args[data] = data_chunk
    state.remote_arg_to_original[data_chunk] = data

    ALIASED_OBJECT_CACHE[] = nothing

    check_uniform(memory_space(dest_space_args[data]))
    check_uniform(processor(dest_space_args[data]))
    check_uniform(dest_space_args[data].handle)

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
function move_rewrap(from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, data)
    return aliased_object!(data) do data
        return remotecall_endpoint(identity, current_acceleration(), from_proc, to_proc, from_space, to_space, data)
    end
end
function remotecall_endpoint(f, ::Dagger.DistributedAcceleration, from_proc, to_proc, orig_space, dest_space, data)
    to_w = root_worker_id(to_proc)
    return remotecall_fetch(to_w, from_proc, to_proc, dest_space, data) do from_proc, to_proc, dest_space, data
        data_converted = f(move(from_proc, to_proc, data))
        return tochunk(data_converted, to_proc, dest_space)
    end
end
const ALIASED_OBJECT_CACHE = TaskLocalValue{Union{Dict{AbstractAliasing,Chunk}, Nothing}}(()->nothing)
@warn "Document these public methods" maxlog=1
# TODO: Use state to cache aliasing() results
function declare_aliased_object!(x; ainfo=aliasing(current_acceleration(), x, identity))
    cache = ALIASED_OBJECT_CACHE[]
    cache[ainfo] = x
end
function aliased_object!(x; ainfo=aliasing(current_acceleration(), x, identity))
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
function aliased_object!(f, x; ainfo=aliasing(current_acceleration(), x, identity))
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
    ainfo = aliasing(current_acceleration(), y, identity)
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
