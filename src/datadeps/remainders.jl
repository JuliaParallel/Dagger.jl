# Remainder tracking and computation functions

"""
    RemainderAliasing{S<:MemorySpace} <: AbstractAliasing

Represents the memory spans that remain after subtracting some regions from a base aliasing object.
This is used to perform partial data copies that only update the "remainder" regions.
"""
struct RemainderAliasing{S<:MemorySpace} <: AbstractAliasing
    space::S
    spans::Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}
    ainfos::Vector{AliasingWrapper}
    syncdeps::Set{ThunkSyncdep}
end
RemainderAliasing(space::S, spans::Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}, ainfos::Vector{AliasingWrapper}, syncdeps::Set{ThunkSyncdep}) where S =
    RemainderAliasing{S}(space, spans, ainfos, syncdeps)

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
                                    arg_w::ArgumentWrapper,
                                    write_num::Int; compute_syncdeps::Bool=true)
    spaces_set = Set{MemorySpace}()
    push!(spaces_set, target_space)
    owner_space = state.arg_owner[arg_w]
    push!(spaces_set, owner_space)

    @label restart

    # Determine all memory spaces of the history
    for entry in state.arg_history[arg_w]
        push!(spaces_set, entry.space)
    end
    spaces = collect(spaces_set)
    N = length(spaces)

    # Lookup all memory spans for arg_w in these spaces
    target_ainfos = Vector{Vector{LocalMemorySpan}}()
    for space in spaces
        target_space_ainfo = aliasing!(state, space, arg_w)
        spans = memory_spans(target_space_ainfo)
        push!(target_ainfos, LocalMemorySpan.(spans))
    end
    nspans = length(first(target_ainfos))
    @assert all(==(nspans), length.(target_ainfos)) "Aliasing info for $(typeof(arg_w.arg))[$(arg_w.dep_mod)] has different number of spans in different memory spaces"

    # FIXME: This is a hack to ensure that we don't miss any history generated by aliasing(...)
    for entry in state.arg_history[arg_w]
        if !in(entry.space, spaces)
            @opcounter :compute_remainder_for_arg_restart
            @goto restart
        end
    end

    # We may only need to schedule a full copy from the origin space to the
    # target space if this is the first time we've written to `arg_w`
    if isempty(state.arg_history[arg_w])
        if owner_space != target_space
            return FullCopy(), 0
        else
            return NoAliasing(), 0
        end
    end

    # Create our remainder as an interval tree over all target ainfos
    VERIFY_SPAN_CURRENT_OBJECT[] = arg_w.arg
    remainder = IntervalTree{ManyMemorySpan{N}}(ManyMemorySpan{N}(ntuple(i -> target_ainfos[i][j], N)) for j in 1:nspans)
    for span in remainder
        verify_span(span)
    end

    # Create our tracker
    tracker = Dict{MemorySpace,Tuple{Vector{Tuple{LocalMemorySpan,LocalMemorySpan}},Vector{AliasingWrapper},Set{ThunkSyncdep}}}()

    # Walk backwards through the history of writes to this target
    # other_ainfo is the overlapping ainfo that was written to
    # other_space is the memory space of the overlapping ainfo
    last_idx = length(state.arg_history[arg_w])
    for idx in length(state.arg_history[arg_w]):-1:0
        if isempty(remainder)
            # All done!
            last_idx = idx
            break
        end

        if idx > 0
            other_entry = state.arg_history[arg_w][idx]
            other_ainfo = other_entry.ainfo
            other_space = other_entry.space
        else
            # If we've reached the end of the history, evaluate ourselves
            other_ainfo = aliasing!(state, owner_space, arg_w)
            other_space = owner_space
        end

        # Lookup all memory spans for arg_w in these spaces
        other_remote_arg_w = first(collect(state.ainfo_arg[other_ainfo]))
        other_arg_w = ArgumentWrapper(state.remote_arg_to_original[other_remote_arg_w.arg], other_remote_arg_w.dep_mod)
        other_ainfos = Vector{Vector{LocalMemorySpan}}()
        for space in spaces
            other_space_ainfo = aliasing!(state, space, other_arg_w)
            spans = memory_spans(other_space_ainfo)
            push!(other_ainfos, LocalMemorySpan.(spans))
        end
        nspans = length(first(other_ainfos))
        other_many_spans = [ManyMemorySpan{N}(ntuple(i -> other_ainfos[i][j], N)) for j in 1:nspans]
        foreach(other_many_spans) do span
            verify_span(span)
        end

        if other_space == target_space
            # Only subtract, this data is already up-to-date in target_space
            # N.B. We don't add to syncdeps here, because we'll see this ainfo
            # in get_write_deps!
            @opcounter :compute_remainder_for_arg_subtract
            subtract_spans!(remainder, other_many_spans)
            continue
        end

        # Subtract from remainder and schedule copy in tracker
        other_space_idx = something(findfirst(==(other_space), spaces))
        target_space_idx = something(findfirst(==(target_space), spaces))
        tracker_other_space = get!(tracker, other_space) do
            (Vector{Tuple{LocalMemorySpan,LocalMemorySpan}}(), Vector{AliasingWrapper}(), Set{ThunkSyncdep}())
        end
        @opcounter :compute_remainder_for_arg_schedule
        has_overlap = schedule_remainder!(tracker_other_space[1], other_space_idx, target_space_idx, remainder, other_many_spans)
        if compute_syncdeps && has_overlap
            @assert haskey(state.ainfos_owner, other_ainfo) "[idx $idx] ainfo $(typeof(other_ainfo)) has no owner"
            get_read_deps!(state, other_space, other_ainfo, write_num, tracker_other_space[3])
            push!(tracker_other_space[2], other_ainfo)
        end
    end
    VERIFY_SPAN_CURRENT_OBJECT[] = nothing

    if isempty(tracker) || all(tracked->isempty(tracked[1]), values(tracker))
        return NoAliasing(), 0
    end

    # Return scheduled copies and the index of the last ainfo we considered
    mra = MultiRemainderAliasing()
    for space in spaces
        if haskey(tracker, space)
            spans, ainfos, syncdeps = tracker[space]
            if !isempty(spans)
                push!(mra.remainders, RemainderAliasing(space, spans, ainfos, syncdeps))
            end
        end
    end
    @assert !isempty(mra.remainders) "Expected at least one remainder (spaces: $spaces, tracker spaces: $(collect(keys(tracker))))"
    return mra, last_idx
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
        @assert span_len(source_span) == span_len(dest_span) "Source and dest spans are not the same size: $(span_len(source_span)) != $(span_len(dest_span))"
        push!(tracker, (source_span, dest_span))
    end
    return !isempty(diff)
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
        @assert !isempty(remainder.spans)
        enqueue_remainder_copy_to!(state, dest_space, arg_w, remainder, f, idx, dest_scope, task, write_num)
    end
end
function enqueue_remainder_copy_to!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper, remainder_aliasing::RemainderAliasing,
                                    f, idx, dest_scope, task, write_num::Int)
    dep_mod = arg_w.dep_mod

    # Find the source space for the remainder data
    # We need to find where the best version of the target data lives that hasn't been
    # overwritten by more recent partial updates
    source_space = remainder_aliasing.space

    @dagdebug task.uid :spawn_datadeps "($(repr(f)))[$(idx-1)][$dep_mod] Enqueueing remainder copy-to for $(typeof(arg_w.arg))[$(arg_w.dep_mod)]: $source_space => $dest_space"

    # Get the source and destination arguments
    arg_dest = state.remote_args[dest_space][arg_w.arg]
    arg_source = get_or_generate_slot!(state, source_space, arg_w.arg)

    # Create a copy task for the remainder
    remainder_syncdeps = Set{Any}()
    target_ainfo = aliasing!(state, dest_space, arg_w)
    for syncdep in remainder_aliasing.syncdeps
        push!(remainder_syncdeps, syncdep)
    end
    empty!(remainder_aliasing.syncdeps) # We can't bring these to move!
    source_ainfos = copy(remainder_aliasing.ainfos)
    empty!(remainder_aliasing.ainfos)
    get_write_deps!(state, dest_space, target_ainfo, write_num, remainder_syncdeps)

    @dagdebug task.uid :spawn_datadeps "($(repr(f)))[$(idx-1)][$dep_mod] Remainder copy-to has $(length(remainder_syncdeps)) syncdeps"

    # Launch the remainder copy task
    ctx = Sch.eager_context()
    id = rand(UInt)
    @maybelog ctx timespan_start(ctx, :datadeps_copy, (;id), (;))
    copy_task = Dagger.@spawn scope=dest_scope exec_scope=dest_scope syncdeps=remainder_syncdeps meta=true Dagger.move!(remainder_aliasing, dest_space, source_space, arg_dest, arg_source)
    @maybelog ctx timespan_finish(ctx, :datadeps_copy, (;id), (;thunk_id=copy_task.uid, from_space=source_space, to_space=dest_space, arg_w, from_arg=arg_source, to_arg=arg_dest))

    # This copy task reads the sources and writes to the target
    for ainfo in source_ainfos
        add_reader!(state, arg_w, source_space, ainfo, copy_task, write_num)
    end
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
        @assert !isempty(remainder.spans)
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
    target_ainfo = aliasing!(state, dest_space, arg_w)
    for syncdep in remainder_aliasing.syncdeps
        push!(remainder_syncdeps, syncdep)
    end
    empty!(remainder_aliasing.syncdeps) # We can't bring these to move!
    source_ainfos = copy(remainder_aliasing.ainfos)
    empty!(remainder_aliasing.ainfos)
    get_write_deps!(state, dest_space, target_ainfo, write_num, remainder_syncdeps)

    @dagdebug nothing :spawn_datadeps "($(typeof(arg_w.arg)))[$dep_mod] Remainder copy-from has $(length(remainder_syncdeps)) syncdeps"

    # Launch the remainder copy task
    ctx = Sch.eager_context()
    id = rand(UInt)
    @maybelog ctx timespan_start(ctx, :datadeps_copy, (;id), (;))
    copy_task = Dagger.@spawn scope=dest_scope exec_scope=dest_scope syncdeps=remainder_syncdeps meta=true Dagger.move!(remainder_aliasing, dest_space, source_space, arg_dest, arg_source)
    @maybelog ctx timespan_finish(ctx, :datadeps_copy, (;id), (;thunk_id=copy_task.uid, from_space=source_space, to_space=dest_space, arg_w, from_arg=arg_source, to_arg=arg_dest))

    # This copy task reads the sources and writes to the target
    for ainfo in source_ainfos
        add_reader!(state, arg_w, source_space, ainfo, copy_task, write_num)
    end
    add_writer!(state, arg_w, dest_space, target_ainfo, copy_task, write_num)
end

# FIXME: Document me
function enqueue_copy_to!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper,
                          f, idx, dest_scope, task, write_num::Int)
    dep_mod = arg_w.dep_mod
    source_space = state.arg_owner[arg_w]
    target_ainfo = aliasing!(state, dest_space, arg_w)

    @dagdebug task.uid :spawn_datadeps "($(repr(f)))[$(idx-1)][$dep_mod] Enqueueing full copy-to for $(typeof(arg_w.arg))[$(arg_w.dep_mod)]: $source_space => $dest_space"

    # Get the source and destination arguments
    arg_dest = state.remote_args[dest_space][arg_w.arg]
    arg_source = get_or_generate_slot!(state, source_space, arg_w.arg)

    # Create a copy task for the remainder
    copy_syncdeps = Set{Any}()
    source_ainfo = aliasing!(state, source_space, arg_w)
    target_ainfo = aliasing!(state, dest_space, arg_w)
    get_read_deps!(state, source_space, source_ainfo, write_num, copy_syncdeps)
    get_write_deps!(state, dest_space, target_ainfo, write_num, copy_syncdeps)

    @dagdebug task.uid :spawn_datadeps "($(repr(f)))[$(idx-1)][$dep_mod] Full copy-to has $(length(copy_syncdeps)) syncdeps"

    # Launch the remainder copy task
    ctx = Sch.eager_context()
    id = rand(UInt)
    @maybelog ctx timespan_start(ctx, :datadeps_copy, (;id), (;))
    copy_task = Dagger.@spawn scope=dest_scope exec_scope=dest_scope syncdeps=copy_syncdeps meta=true Dagger.move!(dep_mod, dest_space, source_space, arg_dest, arg_source)
    @maybelog ctx timespan_finish(ctx, :datadeps_copy, (;id), (;thunk_id=copy_task.uid, from_space=source_space, to_space=dest_space, arg_w, from_arg=arg_source, to_arg=arg_dest))

    # This copy task reads the source and writes to the target
    add_reader!(state, arg_w, source_space, source_ainfo, copy_task, write_num)
    add_writer!(state, arg_w, dest_space, target_ainfo, copy_task, write_num)
end
function enqueue_copy_from!(state::DataDepsState, dest_space::MemorySpace, arg_w::ArgumentWrapper,
                            dest_scope, write_num::Int)
    dep_mod = arg_w.dep_mod
    source_space = state.arg_owner[arg_w]
    target_ainfo = aliasing!(state, dest_space, arg_w)

    @dagdebug nothing :spawn_datadeps "($(typeof(arg_w.arg)))[$dep_mod] Enqueueing full copy-from: $source_space => $dest_space"

    # Get the source and destination arguments
    arg_dest = state.remote_args[dest_space][arg_w.arg]
    arg_source = get_or_generate_slot!(state, source_space, arg_w.arg)

    # Create a copy task for the remainder
    copy_syncdeps = Set{Any}()
    source_ainfo = aliasing!(state, source_space, arg_w)
    target_ainfo = aliasing!(state, dest_space, arg_w)
    get_read_deps!(state, source_space, source_ainfo, write_num, copy_syncdeps)
    get_write_deps!(state, dest_space, target_ainfo, write_num, copy_syncdeps)

    @dagdebug nothing :spawn_datadeps "($(typeof(arg_w.arg)))[$dep_mod] Full copy-from has $(length(copy_syncdeps)) syncdeps"

    # Launch the remainder copy task
    ctx = Sch.eager_context()
    id = rand(UInt)
    @maybelog ctx timespan_start(ctx, :datadeps_copy, (;id), (;))
    copy_task = Dagger.@spawn scope=dest_scope exec_scope=dest_scope syncdeps=copy_syncdeps meta=true Dagger.move!(dep_mod, dest_space, source_space, arg_dest, arg_source)
    @maybelog ctx timespan_finish(ctx, :datadeps_copy, (;id), (;thunk_id=copy_task.uid, from_space=source_space, to_space=dest_space, arg_w, from_arg=arg_source, to_arg=arg_dest))

    # This copy task reads the source and writes to the target
    add_reader!(state, arg_w, source_space, source_ainfo, copy_task, write_num)
    add_writer!(state, arg_w, dest_space, target_ainfo, copy_task, write_num)
end

# Main copy function for RemainderAliasing
function move!(dep_mod::RemainderAliasing{S}, to_space::MemorySpace, from_space::MemorySpace, to::Chunk, from::Chunk) where S
    # TODO: Support direct copy between GPU memory spaces

    # Copy the data from the source object
    copies = remotecall_fetch(root_worker_id(from_space), from_space, dep_mod, from) do from_space, dep_mod, from
        len = sum(span_tuple->span_len(span_tuple[1]), dep_mod.spans)
        copies = Vector{UInt8}(undef, len)
        from_raw = unwrap(from)
        offset = UInt64(1)
        with_context!(from_space)
        GC.@preserve copies begin
            for (from_span, _) in dep_mod.spans
                read_remainder!(copies, offset, from_raw, from_span.ptr, from_span.len)
                offset += from_span.len
            end
        end
        @assert offset == len+UInt64(1)
        return copies
    end

    # Copy the data into the destination object
    offset = UInt64(1)
    to_raw = unwrap(to)
    GC.@preserve copies begin
        for (_, to_span) in dep_mod.spans
            write_remainder!(copies, offset, to_raw, to_span.ptr, to_span.len)
            offset += to_span.len
        end
        @assert offset == length(copies)+UInt64(1)
    end

    # Ensure that the data is visible
    Core.Intrinsics.atomic_fence(:release)

    return
end

function read_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, from::Array, from_ptr::UInt64, len::UInt64)
    elsize = sizeof(eltype(from))
    @assert len / elsize == round(UInt64, len / elsize) "Span length is not an integer multiple of the element size: $(len) / $(elsize) = $(len / elsize) (elsize: $elsize)"
    n = UInt64(len / elsize)
    from_offset_n = UInt64((from_ptr - UInt64(pointer(from))) / elsize) + UInt64(1)
    from_vec = reshape(from, prod(size(from)))::DenseVector{eltype(from)}
    # unsafe_wrap(Array, ...) doesn't like unaligned memory
    unsafe_copyto!(Ptr{eltype(from)}(pointer(copies, copies_offset)), pointer(from_vec, from_offset_n), n)
end
function read_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, from::DenseArray, from_ptr::UInt64, len::UInt64)
    elsize = sizeof(eltype(from))
    @assert len / elsize == round(UInt64, len / elsize) "Span length is not an integer multiple of the element size: $(len) / $(elsize) = $(len / elsize) (elsize: $elsize)"
    n = UInt64(len / elsize)
    from_offset_n = UInt64((from_ptr - UInt64(pointer(from))) / elsize) + UInt64(1)
    from_vec = reshape(from, prod(size(from)))::DenseVector{eltype(from)}
    copies_typed = unsafe_wrap(Vector{eltype(from)}, Ptr{eltype(from)}(pointer(copies, copies_offset)), n)
    copyto!(copies_typed, 1, from_vec, Int(from_offset_n), Int(n))
end
function read_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, from, from_ptr::UInt64, n::UInt64)
    real_from = find_object_holding_ptr(from, from_ptr)
    return read_remainder!(copies, copies_offset, real_from, from_ptr, n)
end

function write_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, to::Array, to_ptr::UInt64, len::UInt64)
    elsize = sizeof(eltype(to))
    @assert len / elsize == round(UInt64, len / elsize) "Span length is not an integer multiple of the element size: $(len) / $(elsize) = $(len / elsize) (elsize: $elsize)"
    n = UInt64(len / elsize)
    to_offset_n = UInt64((to_ptr - UInt64(pointer(to))) / elsize) + UInt64(1)
    to_vec = reshape(to, prod(size(to)))::DenseVector{eltype(to)}
    # unsafe_wrap(Array, ...) doesn't like unaligned memory
    unsafe_copyto!(pointer(to_vec, to_offset_n), Ptr{eltype(to)}(pointer(copies, copies_offset)), n)
end
function write_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, to::DenseArray, to_ptr::UInt64, len::UInt64)
    elsize = sizeof(eltype(to))
    @assert len / elsize == round(UInt64, len / elsize) "Span length is not an integer multiple of the element size: $(len) / $(elsize) = $(len / elsize) (elsize: $elsize)"
    n = UInt64(len / elsize)
    to_offset_n = UInt64((to_ptr - UInt64(pointer(to))) / elsize) + UInt64(1)
    to_vec = reshape(to, prod(size(to)))::DenseVector{eltype(to)}
    copies_typed = unsafe_wrap(Vector{eltype(to)}, Ptr{eltype(to)}(pointer(copies, copies_offset)), n)
    copyto!(to_vec, Int(to_offset_n), copies_typed, 1, Int(n))
end
function write_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, to, to_ptr::UInt64, n::UInt64)
    real_to = find_object_holding_ptr(to, to_ptr)
    return write_remainder!(copies, copies_offset, real_to, to_ptr, n)
end

# Remainder copies for common objects
for wrapper in (UpperTriangular, LowerTriangular, UnitUpperTriangular, UnitLowerTriangular, SubArray)
    @eval function read_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, from::$wrapper, from_ptr::UInt64, n::UInt64)
        read_remainder!(copies, copies_offset, parent(from), from_ptr, n)
    end
    @eval function write_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, to::$wrapper, to_ptr::UInt64, n::UInt64)
        write_remainder!(copies, copies_offset, parent(to), to_ptr, n)
    end
end
# N.B. We don't handle pointer aliasing in remainder copies
function read_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, from::Base.RefValue, from_ptr::UInt64, n::UInt64)
    @assert from_ptr == UInt64(Base.pointer_from_objref(from) + fieldoffset(typeof(from), 1)) "Pointer $from_ptr not found in RefValue"
    unsafe_copyto!(pointer(copies, copies_offset), Ptr{UInt8}(from_ptr), n)
end
function write_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, to::Base.RefValue, to_ptr::UInt64, n::UInt64)
    @assert to_ptr == UInt64(Base.pointer_from_objref(to) + fieldoffset(typeof(to), 1)) "Pointer $to_ptr not found in RefValue"
    unsafe_copyto!(Ptr{UInt8}(to_ptr), pointer(copies, copies_offset), n)
end

function find_object_holding_ptr(A::SparseMatrixCSC, ptr::UInt64)
    span = LocalMemorySpan(pointer(A.nzval), length(A.nzval)*sizeof(eltype(A.nzval)))
    if span_start(span) <= ptr <= span_end(span)
        return A.nzval
    end
    span = LocalMemorySpan(pointer(A.colptr), length(A.colptr)*sizeof(eltype(A.colptr)))
    if span_start(span) <= ptr <= span_end(span)
        return A.colptr
    end
    span = LocalMemorySpan(pointer(A.rowval), length(A.rowval)*sizeof(eltype(A.rowval)))
    @assert span_start(span) <= ptr <= span_end(span) "Pointer $ptr not found in SparseMatrixCSC"
    return A.rowval
end