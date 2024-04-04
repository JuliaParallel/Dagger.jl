function eager_submit!(ntasks, id, future, finalizer_ref, f, args, options, world, metadata)
    Sch.init_eager()

    if all_can_execute_inline(f, args, options, world, metadata)
        #@info "Inline $id: $f"
        # Send to the inline scheduler
        if id isa ThunkID
            return eager_submit_inline!(id, future, finalizer_ref,
                                        f, args, options, world, metadata)
        else
            thunk_refs = Sch.ThunkRef[]
            for i in 1:ntasks
                tref = eager_submit_inline!(ntasks, id, future, finalizer_ref,
                                            f, args, options, world, metadata)
                push!(thunk_refs, tref)
            end
            return thunk_refs
        end
    end

    # Send to the core scheduler
    state = Sch.EAGER_STATE[]
    @lock state.lock begin
        if id isa ThunkID
            return eager_submit_core!(id, future, finalizer_ref,
                                      f, args, options, world, metadata)
        else
            thunk_refs = Sch.ThunkRef[]
            for i in 1:ntasks
                tref = eager_submit_core!(ntasks, id, future, finalizer_ref,
                                          f, args, options, world, metadata)
                push!(thunk_refs, tref)
            end
            return thunk_refs
        end
    end
end
function all_can_execute_inline(f, args, options, world::Vector, metadata)
    for idx in 1:length(options)
        if !can_execute_inline(f[idx], args[idx], options[idx], world[idx], metadata[idx])
            return false
        end
    end
    return true
end
function all_can_execute_inline(f, args, options, world::UInt64, metadata)
    return can_execute_inline(f, args, options, world, metadata)
end
function can_execute_inline(f, args, options, world, metadata)
    # FIXME: Check function cost - if it's too high, we should not inline
    # FIXME: Check that this processor isn't too busy (time to schedule+run locally < time to schedule remotely)

    # Ensure we precompile the core scheduler
    ccall(:jl_generating_output, Bool, ()) && return false

    # All arguments are constant or locally fulfilled
    for (_, arg) in args
        arg = unwrap_weak_checked(arg)
        if arg isa Chunk
            if arg.handle.owner != myid()
                return false
            end
        elseif istask(arg)
            if arg isa EagerThunk
                if !isready(arg)
                    return false
                end
            else
                return false
            end
        end
    end

    # At least one compatible processor exists locally
    procs = get_processors(OSProc())
    sig = metadata.signature

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

    # Check that task will terminate and not block indefinitely
    effects = get_effects(sig, world)
    effects.terminates || return false

    return true
end

# Core (Default) Scheduler
function eager_submit_core!(id::ThunkID, future, finalizer_ref, f, args, options, world, metadata)
    ctx = Sch.eager_context()
    state = Sch.EAGER_STATE[]

    timespan_start(ctx, :add_thunk, (;thunk_id=id), (;f, args, options))

    # Lookup EagerThunk/ThunkRef -> Thunk
    old_args = copy(args)
    eager_process_args_submission_core!(state, args)
    syncdeps = options.syncdeps !== nothing ? collect(options.syncdeps) : nothing
    syncdeps::Union{Vector{Any},Nothing}
    if syncdeps !== nothing
        for idx in 1:length(syncdeps)
            dep = syncdeps[idx]
            if dep isa EagerThunk
                tid = dep.id
                syncdeps[idx] = state.thunk_dict[tid]
            elseif dep isa ThunkRef
                tid = dep.id
                syncdeps[idx] = state.thunk_dict[tid]
            end
        end
        options.syncdeps = Set{Any}(syncdeps)
    end

    # Create the `Thunk`
    thunk = Thunk(f, args...; world, id, options)

    # Create a `DRef` to `thunk` so that the caller can preserve it
    thunk_dref = poolset(thunk; size=64, device=MemPool.CPURAMDevice(),
                         destructor=UnrefThunkByUser(thunk))
    thunk_ref = ThunkRef(thunk.id, thunk_dref)

    @warn "Set exec_local=true when all syncdeps are ready and scope is compatible with this worker" maxlog=1
    exec_local = false
    GC.@preserve old_args args begin
        if exec_local
            #@info "Local $id: $f"
            # Execute the task on a local processor
            local task_spec, to_proc
            @lock state.lock begin
                # Schedule the task
                task_sched = Sch.TaskSchedule(;to_assign=[thunk])
                Sch.schedule!(ctx, state, Sch.procs_to_use(ctx), task_sched)

                if isempty(task_sched.launch_groups)
                    # For some reason we failed to schedule
                    # FIXME: Set error state
                    error()
                end
                to_proc, _ = only(task_sched.launch_groups)

                # Register the task in the scheduler
                @assert register_thunk!(ctx, state, thunk, thunk_ref, future, finalizer_ref)
                deleteat!(state.ready, findfirst(t->t===thunk, state.ready))

                # Construct the TaskSpec
                sig = metadata.signature
                time_util, alloc_util, occupancy = Sch.task_utilization(state, to_proc, options, sig)
                task_spec = Sch.prepare_fire_task!(ctx, state, thunk, to_proc, metadata.scope, time_util, alloc_util, occupancy)
                @assert task_spec !== nothing
            end

            # Submit the task to a processor run queue
            uid = state.uid
            state = Sch.processor_queue(ctx, uid, to_proc, state.chan)
            Sch.processor_enqueue!(ctx, state, uid, to_proc, [task_spec])
        else
            #@info "Core $id: $f"
            # Hand the task to the core scheduler
            @lock state.lock begin
                register_thunk!(ctx, state, thunk, thunk_ref, future, finalizer_ref)
                put!(state.chan, Sch.RescheduleSignal())
            end
        end
    end

    timespan_finish(ctx, :add_thunk, (;thunk_id=id), (;f, args, options))

    return thunk_ref
end

# Optimized Inline Scheduler
function eager_submit_inline!(id::ThunkID, future, finalizer_ref, f, args, options, world, metadata)
    ctx = Sch.eager_context()
    state = Sch.EAGER_STATE[]

    to_proc = ThreadProc(myid(), Threads.threadid())

    # Create the `Thunk`
    @lock state.lock eager_process_args_submission_inline!(state, to_proc, args)
    thunk = Thunk(f, args...; world, id, options)

    # Create a `DRef` to `thunk` so that the caller can preserve it
    thunk_dref = poolset(thunk; size=64, device=MemPool.CPURAMDevice(),
                                destructor=UnrefThunkByUser(thunk))
    thunk_ref = Sch.ThunkRef(thunk.id, thunk_dref)

    # Use compiler effects to detect needed features
    sig = metadata.signature
    effects = get_effects(sig, world)
    needs_tls = @static if VERSION >= v"1.9-"
        !effects.notaskstate
    else
        true
    end

    # Execute the task directly
    if needs_tls
        @warn "Perform TLS setup" maxlog=1
        #set_tls!()
    end

    # Insert task into scheduler (but don't register the future, we'll set it below)
    @lock state.lock begin
        @assert register_thunk!(ctx, state, thunk, thunk_ref, nothing, finalizer_ref)
        deleteat!(state.ready, findfirst(t->t===thunk, state.ready))
        push!(state.running, thunk)
        state.running_on[thunk] = OSProc()
    end

    _args, kwargs = process_positional_args(args)
    schedule_model = @something(options.schedule_model, state.schedule_model)
    metrics = Sch.required_metrics_to_collect(schedule_model, :signature, :execute)
    error = false
    result = try
        Sch.with_metrics(metrics, :signature, :execute, sig) do
            if needs_tls
                execute!(to_proc, world, f, _args...; kwargs...)
            else
                Base.invoke_in_world(world, f, _args...; kwargs...)
            end
        end
    catch err
        error = true
        ThunkFailedException(thunk, thunk, err)
    end
    @lock state.lock begin
        Sch.cache_store!(state, thunk, result, error)
        Sch.finish_task!(ctx, state, thunk, error)
    end
    put!(future, result; error)

    return thunk_ref
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
function eager_process_args_submission_core!(state, args::Vector{Pair{Union{Symbol,Nothing},Any}})
    @assert islocked(state.lock)
    for idx in 1:length(args)
        pos, arg = args[idx]::Pair
        pos::Union{Symbol,Nothing}
        if arg isa EagerThunk
            arg_tid = arg.id
            args[idx] = pos => state.thunk_dict[arg_tid]
        elseif arg isa ThunkRef
            arg_tid = arg.id
            args[idx] = pos => state.thunk_dict[arg_tid]
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
            args[idx] = pos => WeakChunk(arg)
        end
    end
end
function eager_process_args_submission_inline!(state, to_proc::Processor, args::Vector{Pair{Union{Symbol,Nothing},Any}})
    eager_process_args_submission_core!(state, args)
    @assert islocked(state.lock)
    for idx in 1:length(args)
        pos, arg = args[idx]::Pair
        pos::Union{Symbol,Nothing}
        arg = unwrap_weak_checked(arg)
        if arg isa Thunk
            result, error = Sch.cache_lookup_checked(state, arg)
            @assert !error "FIXME"
            args[idx] = pos => move(to_proc, result)
        elseif arg isa Chunk
            args[idx] = pos => move(to_proc, arg)
        end
    end
end
function register_thunk!(ctx, state, thunk, thunk_ref, future, finalizer_ref)
    @assert islocked(state.lock)
    state.thunk_dict[thunk.id] = WeakThunk(thunk)
    ready = Sch.reschedule_syncdeps!(state, thunk)
    if future !== nothing
        # Ensure we attach a future before the thunk is scheduled
        Sch._register_future!(ctx, state, current_task(), 0#=tid=#, (future, thunk_ref, false#=check=#))
        @dagdebug thunk :submit "Registered future"
    end
    if finalizer_ref !== nothing
        # Preserve the `EagerThunkFinalizer` through `thunk`
        thunk.eager_ref = finalizer_ref
    end
    state.valid[thunk] = nothing
    return ready
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

function get_effects(sig, world)
    h = hash(sig, world)
    @memoize h::UInt64 begin
        f = isdefined(sig[1], :instance) ? sig[1].instance : sig[1]
        Base.infer_effects(f, (sig[2:end]...,); world)
    end::Core.Compiler.Effects
end
function get_return_type(sig, world)
    h = hash(sig, world)
    @memoize h::UInt64 begin
        f = isdefined(sig[1], :instance) ? sig[1].instance : sig[1]
        AT = Base.to_tuple_type(sig[2:end])
        return_type = Base._return_type(f, AT, world)
        return_type === Union{} ? Any : return_type
    end::Type
end

# Submission Preparation
eager_process_args_submission_to_local!(spec::Pair{EagerTaskSpec,EagerThunk}) =
    eager_process_args_submission_to_local!(spec[1].args)
function eager_process_args_submission_to_local!(args::Vector{Pair{Union{Symbol,Nothing},Any}})
    for idx in 1:length(args)
        pos, arg = args[idx]
        if arg isa EagerThunk
            ref = ThunkRef(arg.id, arg.thunk_ref)
            args[idx] = pos => ref
        end
    end
end
function eager_process_args_submission_to_local!(specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    for spec in specs
        eager_process_args_submission_to_local!(spec)
    end
end
function EagerThunkMetadata(spec::EagerTaskSpec)
    union_to_any(T) = T === Union{} ? Any : T
    sig = Sch.signature(spec.f, spec.args)
    return_type = get_return_type(sig, spec.world)
    scope = Sch.calculate_scope(spec.f, spec.args, spec.options)
    return EagerThunkMetadata(sig, return_type, scope)
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
    # Submit the task
    thunk_ref = eager_submit!(1,
                              task.id, task.future, task.finalizer_ref,
                              spec.f, spec.args, spec.options, spec.world,
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
    all_args = Vector{Pair{Union{Symbol,Nothing},Any}}[spec.args for (spec, _) in specs]
    all_options = Options[spec.options for (spec, _) in specs]
    all_worlds = UInt64[spec.world for (spec, _) in specs]
    all_metadata = EagerThunkMetadata[task.metadata for (_, task) in specs]

    # Submit the tasks
    thunk_refs = eager_submit!(ntasks, ids, futures, finalizer_refs, all_fs, all_args, all_options, all_worlds, all_metadata)
    for i in 1:ntasks
        task = specs[i][2]
        task.thunk_ref = thunk_refs[i].ref::DRef
    end
end
