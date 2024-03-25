# Core Scheduler (Remote)
function eager_submit_core!(@nospecialize(payload))
    ctx = Dagger.Sch.eager_context()
    state = Dagger.Sch.EAGER_STATE[]
    task = current_task()
    tid = 0
    return eager_submit_core!(ctx, state, task, tid, payload)
end
function eager_submit_core!(ctx, state, task, tid, payload)
    @nospecialize payload
    ntasks, id, future, ref, f, args, options, world, reschedule = payload

    if id isa Vector
        thunk_refs = ThunkRef[]
        for i in 1:ntasks
            tref = eager_submit_internal!(ctx, state, task, tid,
                                          (1, id[i], future[i], ref[i],
                                           f[i], args[i], options[i], world[i],
                                           false))
            push!(thunk_refs, tref)
        end
        put!(state.chan, Sch.RescheduleSignal())
        return thunk_refs
    end

    id = next_id()

    timespan_start(ctx, :add_thunk, (;thunk_id=id), (;f, args, options))

    # Lookup EagerThunk/ThunkRef -> Thunk
    old_args = copy(args)
    args::Vector{Any}
    for (idx, (pos, arg)) in enumerate(args)
        pos::Union{Symbol,Nothing}
        newarg = if arg isa EagerThunk
            arg_tid = arg.id
            state.thunk_dict[arg_tid]
        elseif arg isa ThunkRef
            arg_tid = arg.id
            state.thunk_dict[arg_tid]
        elseif arg isa Chunk
            # N.B. Different Chunks with the same DRef handle will hash to the same slot,
            # so we just pick an equivalent Chunk as our upstream
            if haskey(state.waiting_data, arg)
                arg = only(filter(o->o isa Chunk && o.handle == arg.handle, keys(state.waiting_data)))::Chunk
            end
            WeakChunk(arg)
        else
            arg
        end
        @inbounds args[idx] = pos => newarg
    end
    syncdeps = if haskey(options, :syncdeps)
        collect(options.syncdeps)
    else
        nothing
    end::Union{Vector{Any},Nothing}
    if syncdeps !== nothing
        for (idx, dep) in enumerate(syncdeps)
            newdep = if dep isa EagerThunk
                tid = dep.id
                state.thunk_dict[tid]
            elseif dep isa ThunkRef
                tid = dep.id
                state.thunk_dict[tid]
            else
                dep
            end
            @inbounds syncdeps[idx] = newdep
        end
        Dagger.options_merge!(options, (;syncdeps))
    end

    GC.@preserve old_args args begin
        # Create the `Thunk`
        thunk = Thunk(f, args...; world, id, options)

        # Create a `DRef` to `thunk` so that the caller can preserve it
        thunk_dref = poolset(thunk; size=64, device=MemPool.CPURAMDevice(),
                            destructor=UnrefThunkByUser(thunk))
        thunk_ref = ThunkRef(thunk.id, thunk_dref)

        # Attach `thunk` within the scheduler
        state.thunk_dict[thunk.id] = WeakThunk(thunk)
        Sch.reschedule_syncdeps!(state, thunk)
        @dagdebug thunk :submit "Added to scheduler"
        if future !== nothing
            # Ensure we attach a future before the thunk is scheduled
            Sch._register_future!(ctx, state, task, tid, (future, thunk_ref, false))
            @dagdebug thunk :submit "Registered future"
        end
        if ref !== nothing
            # Preserve the `EagerThunkFinalizer` through `thunk`
            thunk.eager_ref = ref
        end
        state.valid[thunk] = nothing

        # Tell the scheduler that it has new tasks to schedule
        if reschedule
            put!(state.chan, Sch.RescheduleSignal())
        end

        timespan_finish(ctx, :add_thunk, (;thunk_id=id), (;f, args, options))

        return thunk_ref
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


# Local Scheduler
function eager_submit_local!(ntasks, id, future, finalizer_ref, f, args, options, world, scopes)
    if id isa Vector
        thunk_refs = Sch.ThunkRef[]
        for i in 1:ntasks
            tref = eager_submit_local!(ctx, state, task, tid,
                                       (1, id[i], future[i], ref[i],
                                        f[i], args[i], options[i], world[i],
                                        scopes[i]))
            push!(thunk_refs, tref)
        end
        return (true, thunk_refs)
    end

    # Create the `Thunk`
    thunk = Thunk(f, args...; world, options)

    # Create a `DRef` to `thunk` so that the caller can preserve it
    thunk_ref = poolset(thunk; size=64, device=MemPool.CPURAMDevice())
    thunk_id = Sch.ThunkID(thunk.id, thunk_ref)

    # FIXME: exec_inline (globally_terminates)
    exec_inline = false
    # FIXME: needs_tls
    needs_tls = true

    if exec_inline
        # Execute the function directly
        if needs_tls
            @warn "Perform TLS setup" maxlog=1
            set_tls!()
        end

        _args, kwargs = process_positional_args(args)
        error = false
        result = try
            Base.invoke_in_world(world, f, _args...; kwargs...)
        catch err
            error = true
            ThunkFailedException(thunk, thunk, err)
        end
        put!(future, result; error)
    else
        # Schedule the task to a processor run queue
        ctx = Sch.eager_context()
        state = Sch.EAGER_STATE[]
        # FIXME: Select a processor more intelligently
        to_proc = ThreadProc(myid(), Threads.threadid())
        local spec
        @lock state.lock begin
            sig = Sch.signature(thunk, state)
            time_util, alloc_util, occupancy = Sch.task_utilization(state, to_proc, options, sig)
            task_spec = Sch.prepare_fire_task!(ctx, state, thunk, to_proc, scopes, time_util, alloc_util, occupancy)
            @assert task_spec !== nothing
            state.thunk_dict[thunk.id] = WeakThunk(thunk)
            Sch.reschedule_syncdeps!(state, thunk)
            if future !== nothing
                # Ensure we attach a future before the thunk is scheduled
                Sch._register_future!(ctx, state, thunk, 0#=tid=#, (future, thunk_id, false))
                @dagdebug thunk :submit "Registered future"
            end
            if finalizer_ref !== nothing
                # Preserve the `EagerThunkFinalizer` through `thunk`
                thunk.eager_ref = finalizer_ref
            end
            state.valid[thunk] = nothing
        end
        uid = state.uid
        state = Sch.processor_queue(ctx, uid, to_proc, state.chan)
        Sch.processor_enqueue!(ctx, state, uid, to_proc, [task_spec])
    end

    return (true, thunk_ref)
end
function process_positional_args(all_args)
    args = Any[]
    kwargs = Pair{Symbol,Any}[]
    for (pos, arg) in all_args
        if arg isa Chunk || istask(arg)
            arg = fetch(arg)
        end
        if pos === nothing
            push!(args, arg)
        else
            push!(kwargs, pos=>arg)
        end
    end
    return (args, kwargs)
end

# Local -> Remote
function eager_submit!(ntasks, id, future, finalizer_ref, f, args, options, world, metadata)
    @warn "Split all_can_execute_locally by task" maxlog=1
    if all_can_execute_locally(f, args, options, world, metadata)
        # Send to the local scheduler
        Sch.init_eager()
        success, trefs = eager_submit_local!(ntasks, id, future, finalizer_ref, f, args, options, world)
        if !success
            @goto to_core
        end
        return trefs
    end

    # Send to the core scheduler
    @label to_core
    if in_thunk()
        h = Dagger.sch_handle()
        return exec!(eager_submit_core!, h, ntasks, id, future, finalizer_ref, f, args, options, world, true)
    elseif myid() != 1
        return remotecall_fetch(1, (ntasks, id, future, finalizer_ref, f, args, options, world, true)) do payload
            @nospecialize payload
            Sch.init_eager()
            state = Dagger.Sch.EAGER_STATE[]
            lock(state.lock) do
                eager_submit_core!(payload)
            end
        end
    else
        Sch.init_eager()
        state = Dagger.Sch.EAGER_STATE[]
        return lock(state.lock) do
            eager_submit_core!((ntasks, id, future, finalizer_ref,
                                f, args, options, world,
                                true))
        end
    end
end
function all_can_execute_locally(f, args, options, world, metadata)
    if myid() != 1
        # TODO: Remove this once core is distributed
        return false
    end
    if !(options isa Vector)
        return can_execute_locally(f, args, options, world, metadata)
    end
    for idx in 1:length(options)
        if !can_execute_locally(f[idx], args[idx], options[idx], world[idx], metadata[idx])
            return false
        end
    end
    return true
end
function can_execute_locally(f, args, options, world, metadata)
    # All arguments are constant or locally fulfilled
    for (_, arg) in args
        if arg isa Chunk
            if arg.handle.owner != myid()
                return false
            end
        elseif istask(arg)
            if arg isa EagerThunk
                if !isready(arg)
                    @warn "Allow if dependency is executing locally" maxlog=1
                    return false
                end
            else
                @warn "Handle ThunkID" maxlog=1
                return false
            end
        end
    end

    # At least one compatible processor exists locally
    @warn "OR the cost to move to core exceeds scheduling wait time" maxlog=1
    procs = get_processors(OSProc())
    sig = Sch.signature(f, args)

    any_proc_supported = false
    for proc in procs
        success, _ = Sch.can_use_proc(nothing, get_parent(proc), proc,
                                      options, metadata.scope)
        if success
            any_proc_supported = true
            break
        end
    end
    any_proc_supported || return false

    @warn "Don't disallow TLS" maxlog=1
    @warn "Don't disallow non-termination" maxlog=1
    Tf = sig[1]
    real_f = isdefined(Tf, :instance) ? Tf.instance : nothing
    effects = get_effects(sig, world)
    if !effects.notaskstate || !effects.terminates
        return false
    end

    return true
end
function get_effects(sig, world)
    h = hash(sig, world)
    @memoize h::UInt64 begin
        Base.infer_effects(sig[1].instance, (sig[2:end]...,); world)
    end::Core.Compiler.Effects
end

# Submission -> Local
function eager_process_elem_submission_to_local(x)
    @nospecialize x
    @assert !isa(x, Thunk) "Cannot use `Thunk`s in `@spawn`/`spawn`"
    if x isa Dagger.EagerThunk
        return ThunkRef(x.id, x.thunk_ref)
    else
        return x
    end
end
# TODO: This can probably operate in-place
function eager_process_args_submission_to_local(spec::Pair{EagerTaskSpec,EagerThunk})
    return Base.mapany(first(spec).args) do pos_x
        pos, x = pos_x
        return pos => eager_process_elem_submission_to_local(x)
    end
end
function eager_process_args_submission_to_local(specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    return Base.mapany(specs) do spec
        eager_process_args_submission_to_local(spec)
    end
end
function eager_process_options_submission_to_local!(options::Options)
    if haskey(options, :syncdeps)
        raw_syncdeps = options.syncdeps
        syncdeps = Set{Any}()
        for raw_dep in raw_syncdeps
            push!(syncdeps, eager_process_elem_submission_to_local(raw_dep))
        end
        Dagger.options_merge!(options, (;syncdeps))
    end
end
function EagerThunkMetadata(spec::EagerTaskSpec)
    arg_types = ntuple(i->chunktype(spec.args[i][2]), length(spec.args))
    return_type = Base._return_type(spec.f, Base.to_tuple_type(arg_types), spec.world)
    scope = Sch.calculate_scope(spec.f, spec.args, spec.options)
    return EagerThunkMetadata(return_type, scope)
end
chunktype(t::EagerThunk) = t.metadata.return_type
function eager_spawn(spec::EagerTaskSpec)
    # Generate new EagerThunk
    id = next_id()
    future = ThunkFuture()
    metadata = EagerThunkMetadata(spec)
    finalizer_ref = poolset(EagerThunkFinalizer(id); size=64, device=MemPool.CPURAMDevice())

    # Return unlaunched EagerThunk
    return EagerThunk(id, future, metadata, finalizer_ref)
end
function eager_launch!((spec, task)::Pair{EagerTaskSpec,EagerThunk})
    # Lookup EagerThunk -> ThunkRef
    args = eager_process_args_submission_to_local(spec=>task)
    eager_process_options_submission_to_local!(spec.options)

    # Submit the task
    thunk_ref = eager_submit!(1,
                              task.id, task.future, task.finalizer_ref,
                              spec.f, args, spec.options, spec.world,
                              task.metadata)
    task.thunk_ref = thunk_ref.ref::DRef
end
function eager_launch!(specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    ntasks = length(specs)

    ids = [task.id for (_, task) in specs]
    futures = [task.future for (_, task) in specs]
    finalizer_refs = [task.finalizer_ref for (_, task) in specs]

    # Get all functions, args/kwargs, and options
    all_fs = Any[spec.f for (spec, _) in specs]
    # Lookup EagerThunk -> ThunkRef
    all_args = eager_process_args_submission_to_local(specs)
    all_options = Option[spec.options for (spec, _) in specs]
    all_worlds = UInt64[spec.world for (spec, _) in specs]
    all_metadata = EagerThunkMetadata[task.metadata for (_, task) in specs]

    # Submit the tasks
    thunk_refs = eager_submit!(ntasks, ids, futures, finalizer_refs, all_fs, all_args, all_options, all_worlds, all_metadata)
    for i in 1:ntasks
        task = specs[i][2]
        task.thunk_ref = thunk_refs[i].ref::DRef
    end
end
