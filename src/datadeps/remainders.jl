# Remainder tracking and computation functions

"""
    RemainderAliasing{S<:MemorySpace} <: AbstractAliasing

Represents the memory spans that remain after subtracting some regions from a base aliasing object.
This is used to perform partial data copies that only update the "remainder" regions.
"""
struct RemainderAliasing{S<:MemorySpace} <: AbstractAliasing
    space::S
    spans::Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}
end
RemainderAliasing(space::S, spans::Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}) where S =
    RemainderAliasing{S}(space, spans)

memory_spans(ra::RemainderAliasing) = ra.spans

Base.hash(ra::RemainderAliasing, h::UInt) = hash(ra.spans, hash(RemainderAliasing, h))
Base.:(==)(ra1::RemainderAliasing, ra2::RemainderAliasing) = ra1.spans == ra2.spans

# Add will_alias support for RemainderAliasing
function will_alias(x::RemainderAliasing, y::AbstractAliasing)
    return will_alias(memory_spans(x), memory_spans(y))
end

function will_alias(x::AbstractAliasing, y::RemainderAliasing)
    return will_alias(memory_spans(x), memory_spans(y))
end

function will_alias(x::RemainderAliasing, y::RemainderAliasing)
    return will_alias(memory_spans(x), memory_spans(y))
end

struct MultiRemainderAliasing <: AbstractAliasing
    remainders::Vector{<:RemainderAliasing}
end
MultiRemainderAliasing() = MultiRemainderAliasing(RemainderAliasing[])

memory_spans(mra::MultiRemainderAliasing) = vcat(memory_spans.(mra.remainders)...)

Base.hash(mra::MultiRemainderAliasing, h::UInt) = hash(mra.remainders, hash(MultiRemainderAliasing, h))
Base.:(==)(mra1::MultiRemainderAliasing, mra2::MultiRemainderAliasing) = mra1.remainders == mra2.remainders

#= FIXME: Integrate with main documentation
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

struct FullCopy end

"""
    compute_remainder_for_arg!(state::DataDepsState,
                               target_space::MemorySpace,
                               arg_w::ArgumentWrapper)

Computes what remainder regions need to be copied to `target_space` before a task can access `arg_w`.
Returns a `MultiRemainderAliasing` object representing the remainder, or `NoAliasing()` if no remainder needed.

The algorithm starts by collecting the memory spans of `arg_w` in `target_space` - this is the "remainder".
When this remainder is empty, the algorithm will be finished.
Additionally, a dictionary is created to store the source and destination
memory spans (for each source memory space) that will be used to create the
`MultiRemainderAliasing` object - this is the "tracker".

The algorithm walks backwards through the `arg_history` vector for `arg_w`
(which is an ordered list of all overlapping ainfos that were directy written to (potentially in a different memory space than `target_space`)
since the last time this `arg_w` was written to). If this ainfo is in `target_space`,
then it is not under consideration; it is simply subtraced from the remainder with `subtract_remainder!`,
and the algorithm goes to the next ainfo. Otherwise, the algorithm will consider this ainfo for tracking.

For each overlapping ainfo (which lives in a different memory space than `target_space`) to be tracked, there exists a corresponding "mirror" ainfo in
`target_space`, which is the equivalent of the overlapping ainfo, but in
`target_space`. This mirror ainfo is assumed to have an identical number of memory spans as the overlapping ainfo,
and each memory span is assumed to be identical in size, but not necessarily identical in address.

These three sets of memory spans (from the remainder, the overlapping ainfo, and the mirror ainfo) are then passed to `schedule_aliasing!`.
This call will subtract the spans of the mirror ainfo from the remainder (as the two live in the same memory space and thus can be directly compared),
and will update the remainder accordingly.
Additionaly, it will also use this subtraction to update the tracker, by adding the equivalent spans (mapped from mirror ainfo to overlapping ainfo) to the tracker as the source,
and the spans of the remainder as the destination.

If the history is exhausted without the remainder becoming empty, then the
remaining data in `target_space` is assumed to be up-to-date (as the latest write
to `arg_w` is the furthest back we need to consider).

Finally, the tracker is converted into a `MultiRemainderAliasing` object,
and returned.
"""
function compute_remainder_for_arg!(state::DataDepsState,
                                    target_space::MemorySpace,
                                    arg_w::ArgumentWrapper)
    # We may only need to schedule a full copy from the origin space to the
    # target space if this is the first time we've written to `arg_w`
    if isempty(state.arg_history[arg_w])
        owner_space = state.arg_owner[arg_w]
        if owner_space != target_space
            return FullCopy()
        else
            return NoAliasing()
        end
    end

    # Determine all memory spaces of the history
    spaces_set = Set{MemorySpace}()
    push!(spaces_set, target_space)
    push!(spaces_set, state.arg_owner[arg_w])
    for (_, space, _) in state.arg_history[arg_w]
        push!(spaces_set, space)
    end
    spaces = collect(spaces_set)
    N = length(spaces)
    println("Spaces: $spaces")

    # Lookup all memory spans for arg_w in these spaces
    target_ainfos = Vector{Vector{LocalMemorySpan}}()
    for space in spaces
        target_space_ainfo = aliasing(state, space, arg_w)
        spans = memory_spans(target_space_ainfo)
        push!(target_ainfos, LocalMemorySpan.(spans))
    end
    nspans = length(first(target_ainfos))

    # Create our remainder as an interval tree over all target ainfos
    remainder = IntervalTree{ManyMemorySpan{N}}(ManyMemorySpan{N}(ntuple(i -> target_ainfos[i][j], N)) for j in 1:nspans)

    # Create our tracker
    tracker = Dict{MemorySpace,Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}}()

    # Walk backwards through the history of writes to this target
    # other_ainfo is the overlapping ainfo that was written to
    # other_space is the memory space of the overlapping ainfo
    for idx in length(state.arg_history[arg_w]):-1:1
        (other_ainfo, other_space, _) = state.arg_history[arg_w][idx]
        print_tree_spans(remainder)
        if isempty(remainder)
            # All done!
            break
        end

        # Lookup all memory spans for arg_w in these spaces
        other_remote_arg_w = state.ainfo_arg[other_ainfo]
        other_arg_w = ArgumentWrapper(state.remote_arg_to_original[other_remote_arg_w.arg], other_remote_arg_w.dep_mod)
        other_ainfos = Vector{Vector{LocalMemorySpan}}()
        for space in spaces
            other_space_ainfo = aliasing(state, space, other_arg_w)
            spans = memory_spans(other_space_ainfo)
            push!(other_ainfos, LocalMemorySpan.(spans))
        end
        nspans = length(first(other_ainfos))
        other_many_spans = [ManyMemorySpan{N}(ntuple(i -> other_ainfos[i][j], N)) for j in 1:nspans]

        if other_space == target_space
            # Only subtract, this data is already up-to-date in target_space
            subtract_spans!(remainder, other_many_spans)
            continue
        end

        # Subtract from remainder and schedule copy in tracker
        other_space_idx = something(findfirst(==(other_space), spaces))
        target_space_idx = something(findfirst(==(target_space), spaces))
        tracker_other_space = get!(Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}, tracker, other_space)
        schedule_remainder!(tracker_other_space, other_space_idx, target_space_idx, remainder, other_many_spans)
    end

    if isempty(tracker)
        # No copies were scheduled, everything is up-to-date
        return NoAliasing()
    end

    # Return scheduled copies
    @show tracker
    mra = MultiRemainderAliasing()
    for (space, spans) in tracker
        push!(mra.remainders, RemainderAliasing(space, spans))
    end
    return mra
end

### Memory Span Set Operations for Remainder Computation

"""
    schedule_remainder!(tracker::Vector, source_space_idx::Int, dest_space_idx::Int, remainder::IntervalTree, other_many_spans::Vector{ManyMemorySpan{N}})

Calculates the difference between `remainder` and `other_many_spans`, subtracts
it from `remainder`, and then adds that difference to `tracker` as a scheduled
copy from `other_many_spans` to the subtraced portion of `remainder`.
"""
function schedule_remainder!(tracker::Vector, source_space_idx::Int, dest_space_idx::Int, remainder::IntervalTree, other_many_spans::Vector{ManyMemorySpan{N}}) where N
    diff = Vector{ManyMemorySpan{N}}()
    subtract_spans!(remainder, other_many_spans, diff)

    for span in diff
        source_span = span.spans[source_space_idx]
        dest_span = span.spans[dest_space_idx]
        push!(tracker, (source_span, dest_span))
    end

    return

    # FIXME: Remove the following

    # Group spans by memory space
    base_spans_by_space = group_spans_by_space(base_spans)
    subtract_spans_by_space = group_spans_by_space(spans_to_subtract)

    all_remainder_spans = MemorySpan[]

    all_spaces = Set{MemorySpace}()
    for space in keys(base_spans_by_space)
        push!(all_spaces, space)
    end
    for space in keys(subtract_spans_by_space)
        push!(all_spaces, space)
    end
    @info "All spaces: $all_spaces"
    for space in all_spaces
        S = typeof(space)
        space_base_spans = get(base_spans_by_space, space, MemorySpan{S}[])
        space_subtract_spans = get(subtract_spans_by_space, space, MemorySpan{S}[])
        remainder_spans = subtract_spans(space_base_spans, space_subtract_spans)
        append!(all_remainder_spans, remainder_spans)
    end

    if isempty(all_remainder_spans)
        return NoAliasing()
    end

    # If all spans are in the same space, create a typed RemainderAliasing
    spaces = unique(span.ptr.space for span in all_remainder_spans)
    if length(spaces) == 1
        space = first(spaces)
        S = typeof(space)
        typed_spans = Vector{MemorySpan{S}}(all_remainder_spans)
        return RemainderAliasing{S}(space, typed_spans)
    else
        @info "Constructing MultiRemainderAliasing"
        # Mixed spaces - use a more general approach
        # For now, create individual RemainderAliasing per space and combine into MultiRemainderAliasing
        sub_ainfos = RemainderAliasing[]
        spans_by_space = group_spans_by_space(all_remainder_spans)
        for (space, space_spans) in spans_by_space
            S = typeof(space)
            typed_spans = Vector{MemorySpan{S}}(space_spans)
            push!(sub_ainfos, RemainderAliasing{S}(space, typed_spans))
        end
        return MultiRemainderAliasing(sub_ainfos)
    end
end

"""
    subtract_spans(base_spans::Vector{<:MemorySpan}, subtract_spans::Vector{<:MemorySpan})

Computes the set difference: base_spans - subtract_spans.
Returns a vector of memory spans representing the parts of base_spans that do not
overlap with any span in subtract_spans.

This is used for remainder computation in datadeps: when we need to copy only the
parts of an object that haven't been updated by previous partial operations.
"""
function subtract_spans(base_spans::Vector{<:MemorySpan}, subtract_spans::Vector{<:MemorySpan})
    if isempty(base_spans)
        return MemorySpan[]
    end
    if isempty(subtract_spans)
        return copy(base_spans)
    end
    
    result = MemorySpan[]
    
    for base_span in base_spans
        # Start with the full base span
        remaining_spans = [base_span]
        
        # Subtract each overlapping span
        for sub_span in subtract_spans
            new_remaining = MemorySpan[]
            
            for remaining in remaining_spans
                # Find the intersection and compute the remaining parts
                append!(new_remaining, _subtract_single_span(remaining, sub_span))
            end
            
            remaining_spans = new_remaining
        end
        
        append!(result, remaining_spans)
    end
    
    return result
end

"""
    _subtract_single_span(base::MemorySpan{S}, subtract::MemorySpan{S}) where S

Helper function to subtract a single span from a base span.
Returns 0-2 spans representing the non-overlapping parts of base.
"""
function _subtract_single_span(base::MemorySpan{S}, subtract::MemorySpan{S}) where S
    # Check if spans are in the same memory space
    if base.ptr.space != subtract.ptr.space
        return [base]  # No overlap possible
    end

    base_start = base.ptr.addr
    base_end = base_start + base.len - 1
    sub_start = subtract.ptr.addr
    sub_end = sub_start + subtract.len - 1

    # No overlap
    if sub_end < base_start || sub_start > base_end
        return [base]
    end

    result = MemorySpan{S}[]

    # Left remainder (before the subtracted region)
    if base_start < sub_start
        left_len = min(sub_start - base_start, base.len)
        left_span = MemorySpan{S}(base.ptr, left_len)
        push!(result, left_span)
    end

    # Right remainder (after the subtracted region)
    if base_end > sub_end
        right_start_addr = max(sub_end + 1, base_start)
        right_len = base_end - right_start_addr + 1
        if right_len > 0
            right_ptr = RemotePtr{Cvoid,S}(right_start_addr, base.ptr.space)
            right_span = MemorySpan{S}(right_ptr, right_len)
            push!(result, right_span)
        end
    end

    return result
end

"""
    group_spans_by_space(spans::Vector{MemorySpan}) -> Dict

Groups memory spans by their memory space for easier processing.
"""
function group_spans_by_space(spans::Vector{<:MemorySpan})
    result = Dict{MemorySpace,Vector{<:MemorySpan}}()
    for span in spans
        space = span.ptr.space
        if !haskey(result, space)
            result[space] = MemorySpan[]
        end
        push!(result[space], span)
    end
    return result
end

### Remainder copy functions

"""
    enqueue_remainder_copy_to!(state::DataDepsState, f, target_ainfo::AliasingWrapper, remainder_aliasing, dep_mod, arg, idx,
                               our_space::MemorySpace, our_scope, task::DTask, write_num::Int)

Enqueues a copy operation to update the remainder regions of an object before a task runs.
"""
function enqueue_remainder_copy_to!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper, remainder_aliasing::MultiRemainderAliasing,
                                    f, idx, dest_scope, task, write_num::Int)
    for remainder in remainder_aliasing.remainders
        enqueue_remainder_copy_to!(state, dest_space, arg_w, remainder, f, idx, dest_scope, task, write_num)
    end
end
function enqueue_remainder_copy_to!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper, remainder_aliasing::RemainderAliasing,
                                    f, idx, dest_scope, task, write_num::Int)
    # Find the source space for the remainder data
    # We need to find where the best version of the target data lives that hasn't been
    # overwritten by more recent partial updates
    source_space = remainder_aliasing.space

    @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Enqueueing remainder copy-to for $(typeof(arg_w.arg))[$(arg_w.dep_mod)]: $source_space => $dest_space"

    # Get the source and destination arguments
    arg_dest = state.remote_args[dest_space][arg_w.arg]
    arg_source = get_or_generate_slot!(state, source_space, arg_w.arg)

    # Create a copy task for the remainder
    remainder_syncdeps = Set{Any}()
    target_ainfo = aliasing(state, dest_space, arg_w)
    get_write_deps!(state, dest_space, target_ainfo, task, write_num, remainder_syncdeps)

    @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Remainder copy-to has $(length(remainder_syncdeps)) syncdeps"

    # Launch the remainder copy task
    copy_task = Dagger.@spawn scope=dest_scope syncdeps=remainder_syncdeps meta=true Dagger.move!(remainder_aliasing, dest_space, source_space, arg_dest, arg_source)

    # This copy task becomes a new writer for the target region
    add_writer!(state, arg_w, dest_space, target_ainfo, copy_task, write_num)
end
"""
    enqueue_remainder_copy_from!(state::DataDepsState, target_ainfo::AliasingWrapper, arg, remainder_aliasing,
                                 origin_space::MemorySpace, origin_scope, write_num::Int)

Enqueues a copy operation to update the remainder regions of an object back to the original space.
"""
function enqueue_remainder_copy_from!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper, remainder_aliasing::MultiRemainderAliasing,
                                      dest_scope, write_num::Int)
    for remainder in remainder_aliasing.remainders
        enqueue_remainder_copy_from!(state, dest_space, arg_w, remainder, dest_scope, write_num)
    end
end
function enqueue_remainder_copy_from!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper, remainder_aliasing::RemainderAliasing,
                                      dest_scope, write_num::Int)
    dep_mod = arg_w.dep_mod

    # Find the source space for the remainder data
    # We need to find where the best version of the target data lives that hasn't been
    # overwritten by more recent partial updates
    source_space = remainder_aliasing.space

    @dagdebug nothing :spawn_datadeps "($(typeof(arg_w.arg)))[$dep_mod] Enqueueing remainder copy-from for: $source_space => $dest_space"

    # Get the source and destination arguments
    arg_dest = state.remote_args[dest_space][arg_w.arg]
    arg_source = get_or_generate_slot!(state, source_space, arg_w.arg)

    # Create a copy task for the remainder
    remainder_syncdeps = Set{Any}()
    target_ainfo = aliasing(state, dest_space, arg_w)
    get_write_deps!(state, dest_space, target_ainfo, nothing, write_num, remainder_syncdeps)

    @dagdebug nothing :spawn_datadeps "($(typeof(arg_w.arg)))[$dep_mod] Remainder copy-from has $(length(remainder_syncdeps)) syncdeps"

    # Launch the remainder copy task
    copy_task = Dagger.@spawn scope=dest_scope syncdeps=remainder_syncdeps meta=true Dagger.move!(remainder_aliasing, dest_space, source_space, arg_dest, arg_source)

    # This copy task becomes a new writer for the target region
    add_writer!(state, arg_w, dest_space, target_ainfo, copy_task, write_num)
end

# FIXME: Document me
function enqueue_copy_to!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper,
                          f, idx, dest_scope, task, write_num::Int)
    dep_mod = arg_w.dep_mod
    source_space = state.arg_owner[arg_w]
    target_ainfo = aliasing(state, dest_space, arg_w)

    @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Enqueueing full copy-to for $(typeof(arg_w.arg))[$(arg_w.dep_mod)]: $source_space => $dest_space"

    # Get the source and destination arguments
    arg_dest = state.remote_args[dest_space][arg_w.arg]
    arg_source = get_or_generate_slot!(state, source_space, arg_w.arg)

    # Create a copy task for the remainder
    copy_syncdeps = Set{Any}()
    target_ainfo = aliasing(state, dest_space, arg_w)
    get_write_deps!(state, dest_space, target_ainfo, task, write_num, copy_syncdeps)

    @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Full copy-to has $(length(copy_syncdeps)) syncdeps"

    # Launch the remainder copy task
    copy_task = Dagger.@spawn scope=dest_scope syncdeps=copy_syncdeps meta=true Dagger.move!(dep_mod, dest_space, source_space, arg_dest, arg_source)

    # This copy task becomes a new writer for the target region
    add_writer!(state, arg_w, dest_space, target_ainfo, copy_task, write_num)
end
function enqueue_copy_from!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper,
                            dest_scope, write_num::Int)
    dep_mod = arg_w.dep_mod
    source_space = state.arg_owner[arg_w]
    target_ainfo = aliasing(state, dest_space, arg_w)

    @dagdebug nothing :spawn_datadeps "($(typeof(arg_w.arg)))[$dep_mod] Enqueueing full copy-from: $source_space => $dest_space"

    # Get the source and destination arguments
    arg_dest = state.remote_args[dest_space][arg_w.arg]
    arg_source = get_or_generate_slot!(state, source_space, arg_w.arg)

    # Create a copy task for the remainder
    copy_syncdeps = Set{Any}()
    target_ainfo = aliasing(state, dest_space, arg_w)
    get_write_deps!(state, dest_space, target_ainfo, nothing, write_num, copy_syncdeps)

    @dagdebug nothing :spawn_datadeps "($(typeof(arg_w.arg)))[$dep_mod] Full copy-from has $(length(copy_syncdeps)) syncdeps"

    # Launch the remainder copy task
    copy_task = Dagger.@spawn scope=dest_scope syncdeps=copy_syncdeps meta=true Dagger.move!(dep_mod, dest_space, source_space, arg_dest, arg_source)

    # This copy task becomes a new writer for the target region
    add_writer!(state, arg_w, dest_space, target_ainfo, copy_task, write_num)
end

# Main copy function for RemainderAliasing
function move!(dep_mod::RemainderAliasing{S}, to_space::MemorySpace, from_space::MemorySpace, to::Chunk, from::Chunk) where S
    # Get the source data for each span
    copies = remotecall_fetch(root_worker_id(from_space), dep_mod) do dep_mod
        copies = Vector{UInt8}[]
        for (from_span, _) in dep_mod.spans
            copy = Vector{UInt8}(undef, from_span.len)
            from_ptr = Ptr{UInt8}(from_span.ptr)
            to_ptr = Ptr{UInt8}(pointer(copy))
            unsafe_copyto!(to_ptr, from_ptr, from_span.len)
            push!(copies, copy)
        end
        return copies
    end

    # Copy the data into the destination object
    for (copy, (_, to_span)) in zip(copies, dep_mod.spans)
        from_ptr = Ptr{UInt8}(pointer(copy))
        to_ptr = Ptr{UInt8}(to_span.ptr)
        unsafe_copyto!(to_ptr, from_ptr, to_span.len)
    end

    #= FIXME: Remove me
    # Try to detect common patterns
    if _is_contiguous_remainder(spans, to)
        _copy_contiguous_remainder(spans, to, from)
    elseif _is_strided_remainder(spans, to)
        _copy_strided_remainder(spans, to, from)
    else
        # Fall back to element-wise copy for complex patterns
        _copy_remainder_elementwise(spans, to, from)
    end
    =#

    return
end

# Helper function to check if remainder represents contiguous blocks
function _is_contiguous_remainder(spans::Vector{MemorySpan{S}}, arr::AbstractArray) where S
    # Check if all spans are contiguous within the array's memory
    if length(spans) <= 2  # Simple case: at most 2 contiguous blocks
        return true
    end
    return false
end

# Helper function to copy contiguous remainder blocks
function _copy_contiguous_remainder(spans::Vector{MemorySpan{S}}, to::AbstractArray{T}, from::AbstractArray{T}) where {S,T}
    arr_ptr = pointer(to)
    arr_start = UInt(arr_ptr)
    element_size = sizeof(T)

    for span in spans
        # Calculate the offset in terms of array elements
        offset_bytes = span.ptr.addr - arr_start
        offset_elements = Int(offset_bytes ÷ element_size)
        length_elements = Int(span.len ÷ element_size)

        if offset_elements >= 0 && offset_elements + length_elements <= length(to)
            # Safe to copy this span
            to_view = view(to, (offset_elements+1):(offset_elements+length_elements))
            from_view = view(from, (offset_elements+1):(offset_elements+length_elements))
            copyto!(to_view, from_view)
        end
    end
end

# Helper function to check if remainder represents strided patterns
function _is_strided_remainder(spans::Vector{MemorySpan{S}}, arr::AbstractArray) where S
    # For now, return false - we'll implement this for specific patterns as needed
    return false
end

# Helper function to copy strided remainder patterns
function _copy_strided_remainder(spans::Vector{MemorySpan{S}}, to::AbstractArray{T}, from::AbstractArray{T}) where {S,T}
    # Implementation for strided patterns - placeholder for now
    _copy_remainder_elementwise(spans, to, from)
end

# Element-wise copy for complex remainder patterns (slow but correct)
function _copy_remainder_elementwise(spans::Vector{MemorySpan{S}}, to::AbstractArray{T}, from::AbstractArray{T}) where {S,T}
    arr_ptr = pointer(to)
    arr_start = UInt(arr_ptr)
    element_size = sizeof(T)

    for span in spans
        offset_bytes = span.ptr.addr - arr_start
        offset_elements = Int(offset_bytes ÷ element_size)
        length_elements = Int(span.len ÷ element_size)

        # Copy element by element for safety
        for i in 1:length_elements
            idx = offset_elements + i
            if 1 <= idx <= length(to)
                @info "Copying element $idx of $length_elements: $(from[idx])"
                to[idx] = from[idx]
            end
        end
    end
end
