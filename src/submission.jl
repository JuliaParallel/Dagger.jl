mutable struct PayloadOne
    uid::UInt
    future::ThunkFuture
    ref::DRef
    fargs::Vector{Argument}
    options::NamedTuple
    reschedule::Bool

    PayloadOne() = new()
    PayloadOne(uid::UInt, future::ThunkFuture, ref::DRef, fargs::Vector{Argument}, options::NamedTuple, reschedule::Bool) =
        new(uid, future, ref, fargs, options, reschedule)
end
mutable struct PayloadMulti
    ntasks::Int
    uid::Vector{UInt}
    future::Vector{ThunkFuture}
    ref::Vector{DRef}
    fargs::Vector{Vector{Argument}}
    options::Vector{NamedTuple}
    reschedule::Bool
end
const AnyPayload = Union{PayloadOne, PayloadMulti}
function payload_extract(f, payload::PayloadMulti, i::Integer)
    take_or_alloc!(PAYLOAD_ONE_CACHE[]; no_alloc=true) do p1
        p1.uid = payload.uid[i]
        p1.future = payload.future[i]
        p1.ref = payload.ref[i],
        p1.fargs = payload.fargs[i]
        p1.options = payload.options[i]
        p1.reschedule = true
        return f(p1)
    end
end
const PAYLOAD_ONE_CACHE = TaskLocalValue{ReusableCache{PayloadOne,Nothing}}(()->ReusableCache(PayloadOne, nothing, 1))

const THUNK_SPEC_CACHE = TaskLocalValue{ReusableCache{ThunkSpec,Nothing}}(()->ReusableCache(ThunkSpec, nothing, 1))

# Remote
function eager_submit_internal!(payload::AnyPayload)
    ctx = Dagger.Sch.eager_context()
    state = Dagger.Sch.EAGER_STATE[]
    task = current_task()
    tid = 0
    return eager_submit_internal!(ctx, state, task, tid, payload)
end
const UID_TO_TID_CACHE = TaskLocalValue{ReusableCache{Dict{UInt64,Int},Nothing}}(()->ReusableCache(Dict{UInt64,Int}, nothing, 1))
function eager_submit_internal!(ctx, state, task, tid, payload::AnyPayload; uid_to_tid=nothing)
    maybe_take_or_alloc!(UID_TO_TID_CACHE[], uid_to_tid; no_alloc=true) do uid_to_tid
        if payload isa PayloadMulti
            thunk_ids = Sch.ThunkID[]
            for i in 1:ntasks
                tid = payload_extract(payload, i) do p1
                    eager_submit_internal!(ctx, state, task, tid, p1;
                                           uid_to_tid)
                end
                push!(thunk_ids, tid)
                uid_to_tid[payload.uid[i]] = tid.id
            end
            put!(state.chan, Sch.RescheduleSignal())
            return thunk_ids
        end
        payload::PayloadOne

        uid, future, ref = payload.uid, payload.future, payload.ref
        fargs, options, reschedule = payload.fargs, payload.options, payload.reschedule

        id = next_id()

        @maybelog ctx timespan_start(ctx, :add_thunk, (;thunk_id=id), (;f=fargs[1], args=fargs[2:end], options))

        old_fargs = @reusable_vector :eager_submit_internal!_old_fargs Argument Argument(ArgPosition(), nothing) 32
        append!(old_fargs, Iterators.map(copy, fargs))
        syncdeps_vec = @reusable_vector :eager_submit_interal!_syncdeps_vec Any nothing 32
        if haskey(options, :syncdeps)
            append!(syncdeps_vec, options.syncdeps)
        end

        # Lookup DTask/ThunkID -> Thunk
        lock(Sch.EAGER_ID_MAP) do id_map
            for (idx, arg) in enumerate(fargs)
                if valuetype(arg) <: DTask
                    arg_uid = (value(arg)::DTask).uid
                    arg_tid = if haskey(id_map, arg_uid)
                        id_map[arg_uid]
                    else
                        uid_to_tid[arg_uid]
                    end
                    @inbounds fargs[idx] = Argument(arg.pos, state.thunk_dict[arg_tid])
                elseif valuetype(arg) <: Sch.ThunkID
                    arg_tid = (value(arg)::Sch.ThunkID).id
                    @inbounds fargs[idx] = Argument(arg.pos, state.thunk_dict[arg_tid])
                elseif valuetype(arg) <: Chunk
                    # N.B. Different Chunks with the same DRef handle will hash to the same slot,
                    # so we just pick an equivalent Chunk as our upstream
                    chunk = value(arg)::Chunk
                    if haskey(state.waiting_data, chunk)
                        newchunk = nothing
                        for other in keys(state.waiting_data)
                            if other isa Chunk && other.handle == chunk.handle
                                newchunk = other
                                break
                            end
                        end
                        @assert newchunk !== nothing
                        chunk = newchunk::Chunk
                    end
                    #=FIXME:UNIQUE=#
                    @inbounds fargs[idx] = Argument(arg.pos, WeakChunk(chunk))
                end
            end
            # TODO: Iteration protocol would be faster
            for idx in 1:length(syncdeps_vec)
                dep = syncdeps_vec[idx]
                if dep isa DTask
                    tid = if haskey(id_map, dep.uid)
                        id_map[dep.uid]
                    else
                        uid_to_tid[dep.uid]
                    end
                    @inbounds syncdeps_vec[idx] = state.thunk_dict[tid]
                elseif dep isa Sch.ThunkID
                    tid = dep.id
                    @inbounds syncdeps_vec[idx] = state.thunk_dict[tid]
                end
            end
        end
        #=FIXME:REALLOC=#
        if !isempty(syncdeps_vec)
            syncdeps = Set{Any}(syncdeps_vec)
        else
            if any(arg->istask(value(arg)), fargs)
                syncdeps = Set{Any}()
                for arg in fargs
                    if istask(value(arg))
                        push!(syncdeps, value(arg))
                    end
                end
            else
                syncdeps = EMPTY_SYNCDEPS
            end
        end

        GC.@preserve old_fargs fargs begin
            # Create the `Thunk`
            thunk = take_or_alloc!(THUNK_SPEC_CACHE[]; no_alloc=true) do thunk_spec
                thunk_spec.fargs = fargs
                thunk_spec.syncdeps = syncdeps
                thunk_spec.id = id
                thunk_spec.get_result = get(options, :get_result, false)
                thunk_spec.meta = get(options, :meta, false)
                thunk_spec.persist = get(options, :persist, false)
                thunk_spec.cache = get(options, :cache, false)
                if ref !== nothing
                    # Preserve the `DTaskFinalizer` through `Thunk`
                    thunk_spec.eager_ref = ref
                end
                toptions = Sch.ThunkOptions()
                for field in keys(options)
                    if hasfield(Sch.ThunkOptions, field)
                        setproperty!(toptions, field, getproperty(options, field))
                    end
                end
                thunk_spec.options = toptions
                thunk_spec.propagates = get(options, :propagates, ())
                return Thunk(thunk_spec)
            end

            # Create a `DRef` to `thunk` so that the caller can preserve it
            thunk_ref = poolset(thunk; size=64, device=MemPool.CPURAMDevice(),
                                destructor=UnrefThunkByUser(thunk))
            #=FIXME:UNIQUE=#
            thunk_id = Sch.ThunkID(thunk.id, thunk_ref)

            # Attach `thunk` within the scheduler
            state.thunk_dict[thunk.id] = WeakThunk(thunk)
            #=FIXME:REALLOC=#
            Sch.reschedule_syncdeps!(state, thunk)
            empty!(old_fargs) # reschedule_syncdeps! preserves all referenced tasks/chunks
            @dagdebug thunk :submit "Added to scheduler"
            if future !== nothing
                # Ensure we attach a future before the thunk is scheduled
                Sch._register_future!(ctx, state, task, tid, (future, thunk_id, false))
                @dagdebug thunk :submit "Registered future"
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

            @maybelog ctx timespan_finish(ctx, :add_thunk, (;thunk_id=id), (;f=fargs[1], args=fargs[2:end], options))

            return thunk_id
        end
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
            thunk.eager_accessible = false
            Sch.delete_unused_task!(state, thunk)
        end
    end)
end


# Local -> Remote
function eager_submit!(payload::AnyPayload)
    if Dagger.in_thunk()
        h = Dagger.sch_handle()
        return exec!(eager_submit_internal!, h, payload)
    elseif myid() != 1
        return remotecall_fetch(1, payload) do payload
            Sch.init_eager()
            state = Dagger.Sch.EAGER_STATE[]
            @lock state.lock begin
                eager_submit_internal!(payload)
            end
        end
    else
        Sch.init_eager()
        state = Dagger.Sch.EAGER_STATE[]
        return lock(state.lock) do
            eager_submit_internal!(payload)
        end
    end
end

# Submission -> Local
function eager_process_elem_submission_to_local!(id_map, arg::Argument)
    T = valuetype(arg)
    @assert !(T <: Thunk) "Cannot use `Thunk`s in `@spawn`/`spawn`"
    if T <: DTask && haskey(id_map, (value(arg)::DTask).uid)
        #=FIXME:UNIQUE=#
        arg.value = Sch.ThunkID(id_map[value(arg).uid], value(arg).thunk_ref)
    end
end
function eager_process_args_submission_to_local!(id_map, spec_pair::Pair{DTaskSpec,DTask})
    spec, task = spec_pair
    for arg in spec.fargs
        eager_process_elem_submission_to_local!(id_map, arg)
    end
end
function eager_process_args_submission_to_local!(id_map, spec_pairs::Vector{Pair{DTaskSpec,DTask}})
    for spec_pair in spec_pairs
        eager_process_args_submission_to_local!(id_map, spec_pair)
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
    local options
    lock(Sch.EAGER_ID_MAP) do id_map
        eager_process_args_submission_to_local!(id_map, spec=>task)
        options = eager_process_options_submission_to_local(id_map, spec.options)
    end

    # Submit the task
    #=FIXME:REALLOC=#
    thunk_id = eager_submit!(PayloadOne(task.uid, task.future, task.finalizer_ref,
                                        spec.fargs, options, true))
    task.thunk_ref = thunk_id.ref
end
function eager_launch!(specs::Vector{Pair{DTaskSpec,DTask}})
    ntasks = length(specs)

    #=FIXME:REALLOC_N=#
    uids = [task.uid for (_, task) in specs]
    futures = [task.future for (_, task) in specs]
    finalizer_refs = [task.finalizer_ref for (_, task) in specs]

    # Get all functions, args/kwargs, and options
    #=FIXME:REALLOC_N=#
    all_fargs = lock(Sch.EAGER_ID_MAP) do id_map
        # Lookup DTask -> ThunkID
        eager_process_args_submission_to_local!(id_map, specs)
        [spec.fargs for (spec, _) in specs]
    end
    all_options = Any[spec.options for (spec, _) in specs]

    # Submit the tasks
    #=FIXME:REALLOC=#
    thunk_ids = eager_submit!(PayloadMulti(ntasks, uids, futures, finalizer_refs,
                                           all_fargs, all_options))
    for i in 1:ntasks
        task = specs[i][2]
        task.thunk_ref = thunk_ids[i].ref
    end
end
