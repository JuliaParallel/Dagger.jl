mutable struct PayloadOne
    uid::UInt
    future::ThunkFuture
    fargs::Vector{Argument}
    options::Options
    reschedule::Bool
    # Spawn-time Signature from typed args (before Argument erasure), or
    # `nothing` when it could not be computed (e.g. unresolved ThunkID deps).
    spawn_sig::Union{Signature,Nothing}

    PayloadOne() = new()
    PayloadOne(uid::UInt, future::ThunkFuture,
               fargs::Vector{Argument}, options::Options, reschedule::Bool,
               spawn_sig::Union{Signature,Nothing}=nothing) =
        new(uid, future, fargs, options, reschedule, spawn_sig)
end
function unset!(p::PayloadOne, _)
    p.uid = 0
    p.future = EMPTY_PAYLOAD_ONE.future
    p.fargs = EMPTY_PAYLOAD_ONE.fargs
    p.options = EMPTY_PAYLOAD_ONE.options
    p.reschedule = false
    p.spawn_sig = nothing
end
const EMPTY_PAYLOAD_ONE = PayloadOne(UInt(0), ThunkFuture(), Argument[], Options(), false, nothing)
mutable struct PayloadMulti
    ntasks::Int
    uid::Vector{UInt}
    future::Vector{ThunkFuture}
    fargs::Vector{Vector{Argument}}
    options::Vector{Options}
    reschedule::Bool
    spawn_sigs::Vector{Union{Signature,Nothing}}
end
const AnyPayload = Union{PayloadOne, PayloadMulti}
function payload_extract(f, payload::PayloadMulti, i::Integer)
    take_or_alloc!(PAYLOAD_ONE_CACHE[]) do p1
        p1.uid = payload.uid[i]
        p1.future = payload.future[i]
        p1.fargs = payload.fargs[i]
        p1.options = payload.options[i]
        p1.reschedule = true
        p1.spawn_sig = payload.spawn_sigs[i]
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
            # Each sub-thunk was submitted (and self-scheduled) by the recursive
            # eager_submit_internal! call above, so there is nothing central to
            # wake here.
            return thunk_ids
        end
        payload::PayloadOne

        uid, future = payload.uid, payload.future
        fargs, options, reschedule = payload.fargs, payload.options, payload.reschedule
        spawn_sig = payload.spawn_sig

        id = Int(uid)

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
                    lock(state.thunk_dict) do d
                        @inbounds fargs[idx] = Argument(arg.pos, d[arg_tid])
                    end
                elseif valuetype(arg) <: Sch.ThunkID
                    arg_tid = (value(arg)::Sch.ThunkID).id
                    lock(state.thunk_dict) do d
                        @inbounds fargs[idx] = Argument(arg.pos, d[arg_tid])
                    end
                elseif valuetype(arg) <: Chunk
                    # N.B. Different Chunks with the same DRef handle will hash to the same slot,
                    # so we just pick an equivalent Chunk as our upstream
                    chunk = value(arg)::Chunk
                    function find_equivalent_chunk(state, chunk::C) where {C<:Chunk}
                        lock(state.equiv_chunks) do ec
                            if haskey(ec, chunk.handle)
                                return ec[chunk.handle]::C
                            else
                                ec[chunk.handle] = chunk
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
                dep = syncdeps_vec[idx]::ThunkSyncdep
                @assert dep.id !== nothing && dep.thunk === nothing
                thunk = lock(state.thunk_dict) do d; d[dep.id.id]; end
                @inbounds syncdeps_vec[idx] = ThunkSyncdep(thunk)
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
                thunk_spec.spawn_sig = spawn_sig
                return Thunk(thunk_spec)
            end

            # Create a `DRef` to `thunk` so that the caller can preserve it
            thunk_ref = poolset(thunk; size=64, device=MemPool.CPURAMDevice(),
                                destructor=UnrefThunk(uid, thunk, state))
            #=FIXME:UNIQUE=#
            thunk_id = Sch.ThunkID(thunk.id, thunk_ref)

            # Thunks that become immediately ready during edge-wiring are
            # collected here and scheduled *after* releasing state.lock, so that
            # schedule_one! never runs under the submission lock.
            ready = Sch.Thunk[]
            @lock state.lock begin
                # Attach `thunk` within the scheduler
                lock(state.thunk_dict) do d
                    d[thunk.id] = WeakThunk(thunk)
                end
                # Hold a strong reference until the thunk reaches a terminal state
                # (released in `task_delete!`). The weak `thunk_dict` entry alone
                # cannot keep an unfinished thunk alive once the user drops its
                # `DTask`, which would let GC collect a thunk that still has
                # pending dependents and deadlock the scheduler.
                push!(state.strong_thunks, thunk)
                #=FIXME:REALLOC=#
                Sch.reschedule_syncdeps!(state, thunk, ready)
                old_fargs_cleanup() # reschedule_syncdeps! preserves all referenced tasks/chunks
                n_upstreams = @atomic thunk.pending_deps
                @dagdebug thunk :submit "Added to scheduler with $n_upstreams unresolved upstreams"
                if future !== nothing
                    # Ensure we attach a future before the thunk is scheduled
                    Sch._register_future!(ctx, state, task, tid, (future, thunk_id, false))
                    @dagdebug thunk :submit "Registered future"
                end
                @atomic thunk.valid = true

                # Register Eager UID -> Sch TID
                lock(Sch.EAGER_ID_MAP) do id_map
                    id_map[uid] = thunk.id
                end

                # Reset sch_accessible for all syncdeps
                if options.syncdeps !== nothing
                    for dep_weak in options.syncdeps
                        dep = unwrap_weak_checked(dep_weak)
                        @assert dep.eager_accessible "GC bug: lost eager reference to syncdep"
                        dep.sch_accessible = true
                    end
                end
                # N.B. No RescheduleSignal: scheduling is driven inline by
                # schedule_ready! below and by completion handlers, so there is
                # no central schedule! pass to wake.
            end
            # Place any immediately-ready thunk(s) with state.lock released.
            Sch.schedule_ready!(state, ready)

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
    # Best-effort GC cleanup invoked once per task on DRef finalization. Use a
    # plain `errormonitor` rather than `errormonitor_tracked`: the tracked list
    # is only consulted by precompile for cleanliness, not for halt/runtime
    # correctness, so tracking this high-frequency cleanup task only adds a
    # name-string interpolation, a locked push, and an extra monitor-task spawn
    # to every task's teardown.
    errormonitor(Threads.@spawn begin
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
function eager_process_elem_submission_to_local(id_map, arg::TypedArgument{T}) where T
    @assert !(T <: Thunk) "Cannot use `Thunk`s in `@spawn`/`spawn`"
    if T <: DTask && haskey(id_map, (value(arg)::DTask).uid)
        #=FIXME:UNIQUE=#
        tid = Sch.ThunkID(id_map[value(arg).uid], value(arg).thunk_ref)
        # Preserve the argument position by re-wrapping in a TypedArgument;
        # returning a bare ThunkID drops the position and breaks the later
        # `map(Argument, ...)` conversion in `eager_launch!`.
        return TypedArgument(arg.pos, tid)
    end
    return arg
end
function eager_process_args_submission_to_local!(id_map, spec::DTaskSpec{false})
    for arg in spec.fargs
        eager_process_elem_submission_to_local!(id_map, arg)
    end
end
function eager_process_args_submission_to_local(id_map, spec::DTaskSpec{true})
    return ntuple(i->eager_process_elem_submission_to_local(id_map, spec.fargs[i]), length(spec.fargs))
end

# Memoizes `Base.promote_op` return-type inference for eager task metadata.
# `promote_op` depends only on `typeof(f)` and the argument types (it forwards to
# `Core.Compiler.return_type` on the call signature), so the result is a pure
# function of the call-signature `Type` and can be cached across spawns. This
# avoids re-running the compiler's inference (~1.5KB allocated/spawn) for every
# repeated `(f, arg-types)` shape on the submission hot path.
const RETURN_TYPE_CACHE = LockedObject(Dict{Type,Type}())

function cached_return_type(@nospecialize(f), @nospecialize(arg_types::Tuple))
    # `Union{}` (the bottom type) is a legal inferred arg type — it arises when an
    # upstream task is inferred never to return — but it cannot appear as a tuple
    # field, so we can't form a `Tuple{...}` cache key for it. Such calls are rare,
    # so infer them directly rather than caching.
    for T in arg_types
        T === Union{} && return Base.promote_op(f, arg_types...)
    end
    key = Tuple{typeof(f), arg_types...}
    return lock(RETURN_TYPE_CACHE) do cache
        rt = get(cache, key, nothing)
        rt === nothing || return rt
        rt = Base.promote_op(f, arg_types...)
        cache[key] = rt
        return rt
    end
end

DTaskMetadata(spec::DTaskSpec) = DTaskMetadata(eager_metadata(spec.fargs))
function eager_metadata(fargs)
    f = value(fargs[1])
    f = f isa StreamingFunction ? f.f : f
    arg_types = ntuple(i->chunktype(value(fargs[i+1])), length(fargs)-1)
    return cached_return_type(f, arg_types)
end

# Spawn-time chunktype for signature construction. `DTask` advertises its
# inferred return type (usable before the task finishes); unresolved scheduler
# task refs (`Thunk`, `ThunkID`, …) cannot contribute a type yet.
# `Union{}` (bottom) is a legal inferred return for never-returning / erroring
# tasks but is not a `DataType`, so we refuse to seed and let schedule rebuild.
function _spawn_chunktype(@nospecialize(v))
    if v isa DTask
        T = chunktype(v)
        return T isa DataType ? T : nothing
    elseif istask(v)
        return nothing
    else
        T = chunktype(v)
        return T isa DataType ? T : nothing
    end
end

# Cache spawn-time Signatures by call-signature Type. Repeated spawns of the
# same (f, arg-types) shape (e.g. Datadeps tile kernels, spawn chains) reuse
# one Signature instead of reallocating Vector{DataType} + hashing each time.
const SPAWN_SIG_CACHE = LockedObject(Dict{DataType,Signature}())

"""
    signature_from_fargs(fargs) -> Union{Signature,Nothing}

Build a `Signature` from spawn-time arguments (`Tuple` of `TypedArgument` or
`AbstractVector` of `Argument`) before they are erased to `Vector{Argument}`.

Returns `nothing` if any argument is an unresolved task ref other than
`DTask` (whose `return_type` is used). Leaf tasks and graphs of `DTask`s can
therefore seed `Thunk.sig` at submit time and skip rebuilding the signature
during `schedule_one!`. Results are memoized in `SPAWN_SIG_CACHE`.
"""
function signature_from_fargs(fargs)
    n = length(fargs)
    n == 0 && return nothing

    # Collect chunktypes; refuse if any slot is unresolvable
    types = Vector{DataType}(undef, n)
    any_kw = false
    for i in 1:n
        T = _spawn_chunktype(value(fargs[i]))
        T === nothing && return nothing
        types[i] = T
        if i >= 2 && !ispositional(fargs[i])
            any_kw = true
        end
    end

    # Cache key: for the common no-kw path this is just Tuple{Tf,Targs...}
    # (same layout as Signature.sig). For kwargs, include kwcall + NamedTuple.
    key::DataType = if !any_kw
        Tuple{types...}
    else
        sig_kwarg_names = Symbol[]
        sig_kwarg_types = DataType[]
        pos_types = DataType[types[1]]
        for i in 2:n
            if ispositional(fargs[i])
                push!(pos_types, types[i])
            else
                push!(sig_kwarg_names, pos_kw(fargs[i]))
                push!(sig_kwarg_types, types[i])
            end
        end
        NT = NamedTuple{(sig_kwarg_names...,), Tuple{sig_kwarg_types...}}
        @static if isdefined(Core, :kwcall)
            Tuple{typeof(Core.kwcall), NT, pos_types...}
        else
            f_instance = types[1].instance
            Tuple{typeof(Core.kwfunc(f_instance)), NT, pos_types...}
        end
    end

    return lock(SPAWN_SIG_CACHE) do cache
        get!(cache, key) do
            # Build Signature.sig Vector only on cache miss
            if !any_kw
                return Signature(copy(types))
            end
            return Signature(DataType[key.parameters...])
        end
    end
end

function eager_spawn(spec::DTaskSpec)
    # Generate new unlaunched DTask
    uid = eager_next_id()
    future = ThunkFuture()
    metadata = DTaskMetadata(spec)
    return DTask(uid, future, metadata)
end

chunktype(t::DTask) = t.metadata.return_type

function eager_launch!(pair::DTaskPair)
    spec = pair.spec
    task = pair.task

    # Assign a name, if specified
    eager_assign_name!(spec, task)

    # Build spawn-time Signature from typed args *before* Argument erasure so
    # Thunk.sig can be seeded and schedule_one! can skip rebuilding it.
    spawn_sig = signature_from_fargs(spec.fargs)

    # Lookup DTask -> ThunkID
    fargs = lock(Sch.EAGER_ID_MAP) do id_map
        if is_typed(spec)
            return Argument[map(Argument, eager_process_args_submission_to_local(id_map, spec))...]
        else
            eager_process_args_submission_to_local!(id_map, spec)
            return spec.fargs
        end
    end

    # Submit the task
    #=FIXME:REALLOC=#
    thunk_id = eager_submit!(PayloadOne(task.uid, task.future,
                                        fargs, spec.options, true, spawn_sig))
    task.thunk_ref = thunk_id.ref
end
# FIXME: Don't convert Tuple to Vector{Argument}
function eager_launch!(pairs::Vector{DTaskPair})
    ntasks = length(pairs)

    # Assign a name, if specified
    for pair in pairs
        eager_assign_name!(pair.spec, pair.task)
    end

    #=FIXME:REALLOC_N=#
    uids = [pair.task.uid for pair in pairs]
    futures = [pair.task.future for pair in pairs]

    # Get all functions, args/kwargs, and options
    #=FIXME:REALLOC_N=#
    all_fargs = Vector{Vector{Argument}}(undef, ntasks)
    all_spawn_sigs = Vector{Union{Signature,Nothing}}(undef, ntasks)
    lock(Sch.EAGER_ID_MAP) do id_map
        for i in 1:ntasks
            spec = pairs[i].spec
            # Signature from typed args before Argument erasure
            all_spawn_sigs[i] = signature_from_fargs(spec.fargs)
            # Lookup DTask -> ThunkID
            if is_typed(spec)
                all_fargs[i] = Argument[map(Argument, eager_process_args_submission_to_local(id_map, spec))...]
            else
                eager_process_args_submission_to_local!(id_map, spec)
                all_fargs[i] = spec.fargs
            end
        end
    end
    all_options = Options[pair.spec.options for pair in pairs]

    # Submit the tasks
    #=FIXME:REALLOC=#
    thunk_ids = eager_submit!(PayloadMulti(ntasks, uids, futures,
                                           all_fargs, all_options, true, all_spawn_sigs))
    for i in 1:ntasks
        task = pairs[i].task
        task.thunk_ref = thunk_ids[i].ref
    end
end

function eager_assign_name!(spec::DTaskSpec, task::DTask)
    # Assign a name, if specified
    if spec.options.name !== nothing
        Dagger.logs_annotate!(task, spec.options.name)
    end
end
