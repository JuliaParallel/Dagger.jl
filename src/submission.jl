mutable struct PayloadOne
    uid::UInt
    future::ThunkFuture
    fargs::Vector{Argument}
    options::Options
    reschedule::Bool

    PayloadOne() = new()
    PayloadOne(uid::UInt, future::ThunkFuture,
               fargs::Vector{Argument}, options::Options, reschedule::Bool) =
        new(uid, future, fargs, options, reschedule)
end
function unset!(p::PayloadOne, _)
    p.uid = 0
    p.future = EMPTY_PAYLOAD_ONE.future
    p.fargs = EMPTY_PAYLOAD_ONE.fargs
    p.options = EMPTY_PAYLOAD_ONE.options
    p.reschedule = false
end
const EMPTY_PAYLOAD_ONE = PayloadOne(UInt(0), ThunkFuture(), Argument[], Options(), false)
mutable struct PayloadMulti
    ntasks::Int
    uid::Vector{UInt}
    future::Vector{ThunkFuture}
    fargs::Vector{Vector{Argument}}
    options::Vector{Options}
    reschedule::Bool
end
const AnyPayload = Union{PayloadOne, PayloadMulti}
function payload_extract(f, payload::PayloadMulti, i::Integer)
    take_or_alloc!(PAYLOAD_ONE_CACHE[]) do p1
        p1.uid = payload.uid[i]
        p1.future = payload.future[i]
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
eager_submit_internal!(ctx, state, task, tid, payload::Tuple{<:AnyPayload}) =
    eager_submit_internal!(ctx, state, task, tid, payload[1])
const UID_TO_TID_CACHE = TaskLocalValue{ReusableCache{Dict{UInt64,Int},Nothing}}(()->ReusableCache(Dict{UInt64,Int}, nothing, 1))
@reuse_scope function eager_submit_internal!(ctx, state, task, tid, payload::AnyPayload; uid_to_tid=nothing)
    maybe_take_or_alloc!(UID_TO_TID_CACHE[], uid_to_tid) do uid_to_tid
        if payload isa PayloadMulti
            thunk_ids = Sch.ThunkID[]
            for i in 1:payload.ntasks
                tid = payload_extract(payload, i) do p1
                    eager_submit_internal!(ctx, state, task, tid, p1;
                                           uid_to_tid)
                end
                push!(thunk_ids, tid)
                uid_to_tid[payload.uid[i]] = tid.id
            end
            @lock state.lock begin
                put!(state.chan, Sch.RescheduleSignal())
            end
            return thunk_ids
        end
        payload::PayloadOne

        uid, future = payload.uid, payload.future
        fargs, options, reschedule = payload.fargs, payload.options, payload.reschedule

        id = next_id()

        @maybelog ctx timespan_start(ctx, :add_thunk, (;thunk_id=id), (;f=fargs[1], args=fargs[2:end], options, uid))

        old_fargs = @reusable_vector :eager_submit_internal!_old_fargs Argument Argument(ArgPosition(), nothing) 32
        old_fargs_cleanup = @reuse_defer_cleanup empty!(old_fargs)
        append!(old_fargs, Iterators.map(copy, fargs))

        syncdeps_vec = @reusable_vector :eager_submit_interal!_syncdeps_vec ThunkSyncdep ThunkSyncdep() 32
        syncdeps_vec_cleanup = @reuse_defer_cleanup empty!(syncdeps_vec)
        if options.syncdeps !== nothing
            append!(syncdeps_vec, options.syncdeps)
        end

        # Lookup DTask/ThunkID -> Thunk
        # FIXME: Don't lock if no DTask args
        lock(Sch.EAGER_ID_MAP) do id_map
            for (idx, arg) in enumerate(fargs)
                if valuetype(arg) <: DTask
                    arg_uid = (value(arg)::DTask).uid
                    arg_tid = if haskey(id_map, arg_uid)
                        id_map[arg_uid]
                    else
                        uid_to_tid[arg_uid]
                    end
                    @lock state.lock begin
                        @inbounds fargs[idx] = Argument(arg.pos, state.thunk_dict[arg_tid])
                    end
                elseif valuetype(arg) <: Sch.ThunkID
                    arg_tid = (value(arg)::Sch.ThunkID).id
                    @lock state.lock begin
                        @inbounds fargs[idx] = Argument(arg.pos, state.thunk_dict[arg_tid])
                    end
                elseif valuetype(arg) <: Chunk
                    # N.B. Different Chunks with the same DRef handle will hash to the same slot,
                    # so we just pick an equivalent Chunk as our upstream
                    chunk = value(arg)::Chunk
                    function find_equivalent_chunk(state, chunk::C) where {C<:Chunk}
                        @lock state.lock begin
                            if haskey(state.equiv_chunks, chunk.handle)
                                return state.equiv_chunks[chunk.handle]::C
                            else
                                state.equiv_chunks[chunk.handle] = chunk
                                return chunk
                            end
                        end
                    end
                    chunk = find_equivalent_chunk(state, chunk)
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
                    @lock state.lock begin
                        @inbounds syncdeps_vec[idx] = state.thunk_dict[tid]
                    end
                elseif dep isa Sch.ThunkID
                    tid = dep.id
                    @lock state.lock begin
                        @inbounds syncdeps_vec[idx] = state.thunk_dict[tid]
                    end
                elseif dep isa ThunkSyncdep
                    @assert dep.id !== nothing && dep.thunk === nothing
                    thunk = @lock state.lock state.thunk_dict[dep.id.id]
                    @inbounds syncdeps_vec[idx] = ThunkSyncdep(thunk)
                end
            end
        end
        if !isempty(syncdeps_vec) || any(arg->istask(value(arg)), fargs)
            if options.syncdeps === nothing
                options.syncdeps = Set{ThunkSyncdep}()
            else
                empty!(options.syncdeps)
            end
            syncdeps = options.syncdeps
            for dep in syncdeps_vec
                push!(syncdeps, dep)
            end
            for arg in fargs
                if istask(value(arg))
                    push!(syncdeps, ThunkSyncdep(value(arg)))
                end
            end
        end
        syncdeps_vec_cleanup()

        GC.@preserve old_fargs fargs begin
            # Create the `Thunk`
            thunk = take_or_alloc!(THUNK_SPEC_CACHE[]) do thunk_spec
                thunk_spec.fargs = fargs
                thunk_spec.id = id
                thunk_spec.options = options
                return Thunk(thunk_spec)
            end

            # Create a `DRef` to `thunk` so that the caller can preserve it
            thunk_ref = poolset(thunk; size=64, device=MemPool.CPURAMDevice(),
                                destructor=UnrefThunk(uid, thunk, state))
            #=FIXME:UNIQUE=#
            thunk_id = Sch.ThunkID(thunk.id, thunk_ref)

            @lock state.lock begin
                # Attach `thunk` within the scheduler
                state.thunk_dict[thunk.id] = WeakThunk(thunk)
                #=FIXME:REALLOC=#
                Sch.reschedule_syncdeps!(state, thunk)
                old_fargs_cleanup() # reschedule_syncdeps! preserves all referenced tasks/chunks
                n_upstreams = haskey(state.waiting, thunk) ? length(state.waiting[thunk]) : 0
                @dagdebug thunk :submit "Added to scheduler with $n_upstreams upstreams"
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
            end

            @assert options.syncdeps === nothing || all(dep->dep isa Dagger.ThunkSyncdep && dep.thunk isa Dagger.WeakThunk, options.syncdeps)
            @maybelog ctx timespan_finish(ctx, :add_thunk, (;thunk_id=id), (;f=fargs[1], args=fargs[2:end], options, uid))

            return thunk_id
        end
    end
end
struct UnrefThunk
    uid::UInt
    thunk::Thunk
    state
end
function (unref::UnrefThunk)()
    name = unref.uid != UInt(0) ? "unref DTask $(unref.uid) => Thunk $(unref.thunk.id)" : "unref Thunk $(unref.thunk.id)"
    Sch.errormonitor_tracked(name, Threads.@spawn begin
        if unref.uid != UInt(0)
            lock(Sch.EAGER_ID_MAP) do id_map
                delete!(id_map, unref.uid)
            end
        end

        # The associated DTask is no longer referenced by the user, so mark the
        # thunk as ready to be cleaned up as eagerly as possible (or do so now)
        thunk = unref.thunk
        state = unref.state
        @lock state.lock begin
            thunk.eager_accessible = false
            Sch.delete_unused_task!(state, thunk)
        end

        if unref.uid != UInt(0)
            # Cleanup EAGER_THUNK_STREAMS if this is a streaming DTask
            lock(Dagger.EAGER_THUNK_STREAMS) do global_streams
                if haskey(global_streams, unref.uid)
                    delete!(global_streams, unref.uid)
                end
            end
        end
    end)
end

# Local -> Remote
function eager_submit!(payload::AnyPayload)
    if Dagger.in_task()
        h = Dagger.sch_handle()
        return exec!(eager_submit_internal!, h, payload)
    elseif myid() != 1
        return remotecall_fetch(1, payload) do payload
            Sch.init_eager()
            eager_submit_internal!(payload)
        end
    else
        Sch.init_eager()
        return eager_submit_internal!(payload)
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
function eager_process_options_submission_to_local!(id_map, options::Options)
    if options.syncdeps !== nothing
        #=
        raw_syncdeps = options.syncdeps
        syncdeps = Set{Any}()
        for raw_dep in raw_syncdeps
            if raw_dep isa DTask
                push!(syncdeps, Sch.ThunkID(id_map[raw_dep.uid], raw_dep.thunk_ref))
            elseif raw_dep isa Sch.ThunkID
                push!(syncdeps, raw_dep)
            else
                error("Invalid syncdep type: $(typeof(raw_dep))")
            end
        end
        options.syncdeps = syncdeps
        =#
    end
end

function DTaskMetadata(spec::DTaskSpec)
    f = value(spec.fargs[1])
    f = f isa StreamingFunction ? f.f : f
    arg_types = ntuple(i->chunktype(value(spec.fargs[i+1])), length(spec.fargs)-1)
    return_type = Base.promote_op(f, arg_types...)
    return DTaskMetadata(return_type)
end

function eager_spawn(spec::DTaskSpec)
    # Generate new unlaunched DTask
    uid = eager_next_id()
    future = ThunkFuture()
    metadata = DTaskMetadata(spec)
    return DTask(uid, future, metadata)
end

chunktype(t::DTask) = t.metadata.return_type

function eager_launch!((spec, task)::Pair{DTaskSpec,DTask})
    # Assign a name, if specified
    eager_assign_name!(spec, task)

    # Lookup DTask -> ThunkID
    lock(Sch.EAGER_ID_MAP) do id_map
        eager_process_args_submission_to_local!(id_map, spec=>task)
        eager_process_options_submission_to_local!(id_map, spec.options)
    end

    # Submit the task
    #=FIXME:REALLOC=#
    thunk_id = eager_submit!(PayloadOne(task.uid, task.future,
                                        spec.fargs, spec.options, true))
    task.thunk_ref = thunk_id.ref
end
function eager_launch!(specs::Vector{Pair{DTaskSpec,DTask}})
    ntasks = length(specs)

    # Assign a name, if specified
    for (spec, task) in specs
        eager_assign_name!(spec, task)
    end

    #=FIXME:REALLOC_N=#
    uids = [task.uid for (_, task) in specs]
    futures = [task.future for (_, task) in specs]

    # Get all functions, args/kwargs, and options
    #=FIXME:REALLOC_N=#
    all_fargs = lock(Sch.EAGER_ID_MAP) do id_map
        # Lookup DTask -> ThunkID
        eager_process_args_submission_to_local!(id_map, specs)
        [spec.fargs for (spec, _) in specs]
    end
    all_options = Options[spec.options for (spec, _) in specs]

    # Submit the tasks
    #=FIXME:REALLOC=#
    thunk_ids = eager_submit!(PayloadMulti(ntasks, uids, futures,
                                           all_fargs, all_options, true))
    for i in 1:ntasks
        task = specs[i][2]
        task.thunk_ref = thunk_ids[i].ref
    end
end

function eager_assign_name!(spec::DTaskSpec, task::DTask)
    # Assign a name, if specified
    if spec.options.name !== nothing
        Dagger.logs_annotate!(task, spec.options.name)
    end
end
