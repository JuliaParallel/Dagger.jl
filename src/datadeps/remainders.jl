# Remainder tracking and computation functions

"""
    track_region_update!(state::DataDepsState, updated_ainfo::AliasingWrapper, base_ainfo::AliasingWrapper)

Records that a sub-region (updated_ainfo) of a base object (base_ainfo) has been updated.
This is used to track which parts of an object need remainder copying.
"""
function track_region_update!(state::DataDepsState, updated_ainfo::AliasingWrapper, base_ainfo::AliasingWrapper)
    # Record the mapping from sub-region to base
    state.region_to_base[updated_ainfo] = base_ainfo

    # Add this region to the set of updated regions for the base
    if !haskey(state.updated_regions, base_ainfo)
        state.updated_regions[base_ainfo] = Set{AliasingWrapper}()
    end
    push!(state.updated_regions[base_ainfo], updated_ainfo)
end

"""
    compute_remainder_for_task!(state::DataDepsState,
                                target_space::MemorySpace,
                                arg, dep_mod,
                                target_ainfo::AliasingWrapper)

Computes what remainder regions need to be copied before a task can access target_ainfo.
Returns the aliasing object representing the remainder, or NoAliasing() if no remainder needed.
"""
function compute_remainder_for_task!(state::DataDepsState,
                                     target_space::MemorySpace,
                                     arg, dep_mod,
                                     target_ainfo::AliasingWrapper)
    @assert target_ainfo == aliasing(state, state.remote_args[target_space][arg], dep_mod)

    # Find all overlapping regions that have been updated more recently than target_ainfo
    # ainfos_history contains all other ainfos that have written to some portion of target_ainfo
    updated_ainfos = Set{AliasingWrapper}()
    for other_ainfo in state.args_history[target_ainfo]
        # Check if this region has been written and is more up-to-date than target
        other_owner = state.ainfos_owner[other_ainfo]
        target_owner = state.ainfos_owner[target_ainfo]
        other_locality = state.data_locality[other_ainfo]
        target_locality = target_space
        @info "Checking $(typeof(target_ainfo.inner))\n$target_owner\n($target_locality)\nagainst\n$(typeof(other_ainfo.inner))\n$other_owner\n($other_locality):"
        @assert other_owner !== nothing
        if other_owner !== nothing && target_owner !== nothing
            _, other_write_num = other_owner
            _, target_write_num = target_owner
            #@info "$target_write_num < $other_write_num"
            @assert target_write_num < other_write_num "$target_write_num vs. $other_write_num"

            # If the other region was updated more recently, it represents updated data
            # that would be overwritten if we copy the full target_ainfo
            if other_write_num > target_write_num && other_locality != target_locality
                push!(updated_ainfos, other_ainfo)
            end
        elseif other_owner !== nothing && target_owner === nothing
            # Other region has been updated but target hasn't, so other is more recent
            if other_locality != target_locality
                push!(updated_ainfos, other_ainfo)
            end
        end
    end

    # Clear the history for this target, since we're done with it
    empty!(state.args_history[target_ainfo])

    if isempty(updated_ainfos)
        return NoAliasing()
    end

    # Compute the remainder: target_ainfo - updated_ainfos
    return compute_remainder_aliasing(target_ainfo, collect(updated_ainfos))
end

"""
    enqueue_remainder_copy_to!(state::DataDepsState, f, target_ainfo::AliasingWrapper, remainder_aliasing, dep_mod, arg, idx,
                               our_space::MemorySpace, our_scope, task::DTask, write_num::Int)

Enqueues a copy operation to update the remainder regions of an object before a task runs.
"""
function enqueue_remainder_copy_to!(state::DataDepsState, f, target_ainfo::AliasingWrapper, remainder_aliasing::MultiRemainderAliasing, arg, deps, idx,
                                    our_space::MemorySpace, our_scope, task, write_num::Int)
    for remainder in remainder_aliasing.remainders
        enqueue_remainder_copy_to!(state, f, target_ainfo, remainder, arg, deps, idx, our_space, our_scope, task, write_num)
    end
end
function enqueue_remainder_copy_to!(state::DataDepsState, f, target_ainfo::AliasingWrapper, remainder_aliasing, arg, deps, idx,
                                    our_space::MemorySpace, our_scope, task, write_num::Int)
    # Find the source space for the remainder data
    # We need to find where the best version of the target data lives that hasn't been
    # overwritten by more recent partial updates
    source_space = remainder_aliasing.space

    # If the source and destination are the same, no copy needed
    if source_space == our_space
        @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Skipped remainder copy-to (same space): $source_space"
        return
    end

    @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Enqueueing remainder copy-to for $target_ainfo: $source_space => $our_space"

    # Get the source and destination arguments
    arg_remote = state.remote_args[our_space][arg]
    arg_local = get_or_generate_slot!(state, source_space, arg, deps)

    # Create a copy task for the remainder
    remainder_scope = our_scope
    remainder_syncdeps = Set{Any}()
    get_write_deps!(state, our_space, target_ainfo, task, write_num, remainder_syncdeps)

    @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Remainder copy-to has $(length(remainder_syncdeps)) syncdeps"

    # Launch the remainder copy task
    remainder_copy = Dagger.@spawn scope=remainder_scope syncdeps=remainder_syncdeps meta=true Dagger.move!(remainder_aliasing, our_space, source_space, arg_remote, arg_local)

    # This copy task becomes a new writer for the target region
    add_writer!(state, target_ainfo, remainder_copy, write_num)

    state.data_locality[target_ainfo] = our_space
end
"""
    enqueue_remainder_copy_from!(state::DataDepsState, target_ainfo::AliasingWrapper, arg, remainder_aliasing,
                                 origin_space::MemorySpace, origin_scope, write_num::Int)

Enqueues a copy operation to update the remainder regions of an object back to the original space.
"""
function enqueue_remainder_copy_from!(state::DataDepsState, target_ainfo::AliasingWrapper, remainder_aliasing::MultiRemainderAliasing, arg,
                                      our_space::MemorySpace, our_scope, write_num::Int)
    for remainder in remainder_aliasing.remainders
        enqueue_remainder_copy_from!(state, target_ainfo, remainder, arg, our_space, our_scope, write_num)
    end
end
function enqueue_remainder_copy_from!(state::DataDepsState, target_ainfo::AliasingWrapper, arg, remainder_aliasing::RemainderAliasing,
                                      origin_space::MemorySpace, origin_scope, write_num::Int)
    # Find the source space for the remainder data
    # We need to find where the best version of the target data lives that hasn't been
    # overwritten by more recent partial updates
    source_space = remainder_aliasing.space

    # If the source and destination are the same, no copy needed
    #=if source_space == origin_space
        @dagdebug nothing :spawn_datadeps "Skipped remainder copy-from (same space): $source_space"
        return
    end=#

    @dagdebug nothing :spawn_datadeps "Enqueueing remainder copy-from for $target_ainfo: $source_space => $origin_space"

    # Get the source and destination arguments
    arg_remote = state.remote_args[source_space][arg]
    arg_local = state.remote_args[origin_space][arg]

    # Create a copy task for the remainder
    remainder_scope = origin_scope
    remainder_syncdeps = Set{Any}()
    get_write_deps!(state, origin_space, target_ainfo, nothing, write_num, remainder_syncdeps)

    @dagdebug nothing :spawn_datadeps "Remainder copy-from has $(length(remainder_syncdeps)) syncdeps"

    # Launch the remainder copy task
    remainder_copy = Dagger.@spawn scope=remainder_scope syncdeps=remainder_syncdeps meta=true Dagger.move!(remainder_aliasing, origin_space, source_space, arg_local, arg_remote)

    # This copy task becomes a new writer for the target region
    add_writer!(state, target_ainfo, remainder_copy, write_num)

    state.data_locality[target_ainfo] = origin_space
end

# FIXME: Remove me
function enqueue_copy_to!(state, f, dep_mod, arg, obj, idx, obj_idx, our_space, our_scope, task, write_num)
    ainfo = aliasing(state, obj, dep_mod)
    data_space = state.data_locality[ainfo]
    nonlocal = our_space != data_space
    if nonlocal
        # Add copy-to operation (depends on latest owner of arg)
        @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Enqueueing copy-to: $data_space => $our_space"
        aw = ArgumentWrapper(arg, dep_mod)
        arg_remote = state.remote_args[our_space][aw]
        get_or_generate_slot!(state, data_space, arg)
        if obj_idx == 0
            arg_local = state.remote_args[data_space][aw]
        else
            arg_local = state.ainfo_backing_chunk[data_space][aw][obj_idx]
        end
        copy_to_scope = our_scope
        copy_to_syncdeps = Set{Any}()
        get_write_deps!(state, our_space, ainfo, task, write_num, copy_to_syncdeps)
        @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] $(length(copy_to_syncdeps)) syncdeps"
        copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps meta=true Dagger.move!(dep_mod, our_space, data_space, arg_remote, arg_local)
        add_writer!(state, ainfo, copy_to, write_num)

        state.data_locality[ainfo] = our_space
    else
        @dagdebug nothing :spawn_datadeps "($(repr(f)))[$idx][$dep_mod] Skipped copy-to (local): $data_space"
    end
end