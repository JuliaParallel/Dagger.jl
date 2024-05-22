# Remote
function eager_submit_internal!(@nospecialize(payload))
    ctx = Dagger.Sch.eager_context()
    state = Dagger.Sch.EAGER_STATE[]
    task = current_task()
    tid = 0
    return eager_submit_internal!(ctx, state, task, tid, payload)
end
function eager_submit_internal!(ctx, state, task, tid, payload; uid_to_tid=Dict{UInt64,Int}())
    @nospecialize payload
    ntasks, uid, future, ref, f, args, options, reschedule = payload

    if uid isa Vector
        thunk_ids = Sch.ThunkID[]
        for i in 1:ntasks
            tid = eager_submit_internal!(ctx, state, task, tid,
                                         (1, uid[i], future[i], ref[i],
                                          f[i], args[i], options[i],
                                          false); uid_to_tid)
            push!(thunk_ids, tid)
            uid_to_tid[uid[i]] = tid.id
        end
        put!(state.chan, Sch.RescheduleSignal())
        return thunk_ids
    end

    id = next_id()

    timespan_start(ctx, :add_thunk, (;thunk_id=id), (;f, args, options))

    # Lookup DTask/ThunkID -> Thunk
    old_args = copy(args)
    args::Vector{Any}
    syncdeps = if haskey(options, :syncdeps)
        collect(options.syncdeps)
    else
        nothing
    end::Union{Vector{Any},Nothing}
    lock(Sch.EAGER_ID_MAP) do id_map
        for (idx, (pos, arg)) in enumerate(args)
            pos::Union{Symbol,Nothing}
            newarg = if arg isa DTask
                arg_uid = arg.uid
                arg_tid = if haskey(id_map, arg_uid)
                    id_map[arg_uid]
                else
                    uid_to_tid[arg_uid]
                end
                state.thunk_dict[arg_tid]
            elseif arg isa Sch.ThunkID
                arg_tid = arg.id
                state.thunk_dict[arg_tid]
            elseif arg isa Chunk
                # N.B. Different Chunks with the same DRef handle will hash to the same slot,
                # so we just pick an equivalent Chunk as our upstream
                if haskey(state.waiting_data, arg)
                    newarg = nothing
                    for other in keys(state.waiting_data)
                        if other isa Chunk && other.handle == arg.handle
                            newarg = other
                            break
                        end
                    end
                    @assert newarg !== nothing
                    arg = newarg::Chunk
                end
                WeakChunk(arg)
            else
                arg
            end
            @inbounds args[idx] = pos => newarg
        end
        if syncdeps === nothing
            return
        end
        for (idx, dep) in enumerate(syncdeps)
            newdep = if dep isa DTask
                tid = if haskey(id_map, dep.uid)
                    id_map[dep.uid]
                else
                    uid_to_tid[dep.uid]
                end
                state.thunk_dict[tid]
            elseif dep isa Sch.ThunkID
                tid = dep.id
                state.thunk_dict[tid]
            else
                dep
            end
            @inbounds syncdeps[idx] = newdep
        end
    end
    if syncdeps !== nothing
        options = merge(options, (;syncdeps))
    end

    GC.@preserve old_args args begin
        # Create the `Thunk`
        thunk = Thunk(f, args...; id, options...)

        # Create a `DRef` to `thunk` so that the caller can preserve it
        thunk_ref = poolset(thunk; size=64, device=MemPool.CPURAMDevice(),
                            destructor=UnrefThunkByUser(thunk))
        thunk_id = Sch.ThunkID(thunk.id, thunk_ref)

        # Attach `thunk` within the scheduler
        state.thunk_dict[thunk.id] = WeakThunk(thunk)
        Sch.reschedule_syncdeps!(state, thunk)
        @dagdebug thunk :submit "Added to scheduler"
        if future !== nothing
            # Ensure we attach a future before the thunk is scheduled
            Sch._register_future!(ctx, state, task, tid, (future, thunk_id, false))
            @dagdebug thunk :submit "Registered future"
        end
        if ref !== nothing
            # Preserve the `DTaskFinalizer` through `thunk`
            thunk.eager_ref = ref
        end
        state.valid[thunk] = nothing

        # Register Eager UID -> Sch TID
        lock(Sch.EAGER_ID_MAP) do id_map
            id_map[uid] = thunk.id
        end

        # Tell the scheduler that it has new tasks to schedule
        if reschedule
            put!(state.chan, Sch.RescheduleSignal())
        end

        timespan_finish(ctx, :add_thunk, (;thunk_id=id), (;f, args, options))

        return thunk_id
    end
end
struct UnrefThunkByUser
    thunk::Thunk
end
function (unref::UnrefThunkByUser)()
    Sch.errormonitor_tracked("unref thunk $(unref.thunk.id)", Threads.@spawn begin
        # This thunk is no longer referenced by the user, mark it as ready to be
        # cleaned up as eagerly as possible (or do so now)
        thunk = unref.thunk
        state = Sch.EAGER_STATE[]
        if state === nothing
            return
        end

        @lock state.lock begin
            if !Sch.delete_unused_task!(state, thunk)
                # Register for deletion upon thunk completion
                push!(state.thunks_to_delete, thunk)
            end
            # TODO: On success, walk down to children, as a fast-path
        end
    end)
end


# Local -> Remote
function eager_submit!(ntasks, uid, future, finalizer_ref, f, args, options)
    if Dagger.in_thunk()
        h = Dagger.sch_handle()
        return exec!(eager_submit_internal!, h, ntasks, uid, future, finalizer_ref, f, args, options, true)
    elseif myid() != 1
        return remotecall_fetch(1, (ntasks, uid, future, finalizer_ref, f, args, options, true)) do payload
            @nospecialize payload
            Sch.init_eager()
            state = Dagger.Sch.EAGER_STATE[]
            lock(state.lock) do
                eager_submit_internal!(payload)
            end
        end
    else
        Sch.init_eager()
        state = Dagger.Sch.EAGER_STATE[]
        return lock(state.lock) do
            eager_submit_internal!((ntasks, uid, future, finalizer_ref,
                                    f, args, options,
                                    true))
        end
    end
end

# Submission -> Local
function eager_process_elem_submission_to_local(id_map, x)
    @nospecialize x
    @assert !isa(x, Thunk) "Cannot use `Thunk`s in `@spawn`/`spawn`"
    if x isa Dagger.DTask && haskey(id_map, x.uid)
        return Sch.ThunkID(id_map[x.uid], x.thunk_ref)
    else
        return x
    end
end
# TODO: This can probably operate in-place
function eager_process_args_submission_to_local(id_map, spec::Pair{DTaskSpec,DTask})
    return Base.mapany(first(spec).args) do pos_x
        pos, x = pos_x
        return pos => eager_process_elem_submission_to_local(id_map, x)
    end
end
function eager_process_args_submission_to_local(id_map, specs::Vector{Pair{DTaskSpec,DTask}})
    return Base.mapany(specs) do spec
        eager_process_args_submission_to_local(id_map, spec)
    end
end
function eager_process_options_submission_to_local(id_map, options::NamedTuple)
    @nospecialize options
    if haskey(options, :syncdeps)
        raw_syncdeps = options.syncdeps
        syncdeps = Set{Any}()
        for raw_dep in raw_syncdeps
            push!(syncdeps, eager_process_elem_submission_to_local(id_map, raw_dep))
        end
        return merge(options, (;syncdeps))
    else
        return options
    end
end
function eager_spawn(spec::DTaskSpec)
    # Generate new DTask
    uid = eager_next_id()
    future = ThunkFuture()
    finalizer_ref = poolset(DTaskFinalizer(uid); device=MemPool.CPURAMDevice())

    # Return unlaunched DTask
    return DTask(uid, future, finalizer_ref)
end
function eager_launch!((spec, task)::Pair{DTaskSpec,DTask})
    # Lookup DTask -> ThunkID
    local args, options
    lock(Sch.EAGER_ID_MAP) do id_map
        args = eager_process_args_submission_to_local(id_map, spec=>task)
        options = eager_process_options_submission_to_local(id_map, spec.options)
    end

    # Submit the task
    thunk_id = eager_submit!(1,
                             task.uid, task.future, task.finalizer_ref,
                             spec.f, args, options)
    task.thunk_ref = thunk_id.ref
end
function eager_launch!(specs::Vector{Pair{DTaskSpec,DTask}})
    ntasks = length(specs)

    uids = [task.uid for (_, task) in specs]
    futures = [task.future for (_, task) in specs]
    finalizer_refs = [task.finalizer_ref for (_, task) in specs]

    # Get all functions, args/kwargs, and options
    all_fs = Any[spec.f for (spec, _) in specs]
    all_args = lock(Sch.EAGER_ID_MAP) do id_map
        # Lookup DTask -> ThunkID
        eager_process_args_submission_to_local(id_map, specs)
    end
    all_options = Any[spec.options for (spec, _) in specs]

    # Submit the tasks
    thunk_ids = eager_submit!(ntasks, uids, futures, finalizer_refs, all_fs, all_args, all_options)
    for i in 1:ntasks
        task = specs[i][2]
        task.thunk_ref = thunk_ids[i].ref
    end
end
