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

# N.B. Different Chunks with the same DRef handle will hash to the same slot,
# so we just pick an equivalent Chunk as our upstream.
# Kept at top-level (rather than as a local function within the argument loop of
# `eager_submit_internal!`) so that it captures nothing: a local named function
# defined inside that loop forces a `Core.Box` allocation per iteration.
function find_equivalent_chunk(state, chunk::C) where {C<:Chunk}
    # `equiv_chunks` is a `WeakKeyDict{DRef,WeakRef}`; only
    # DRef-backed chunks participate. Other handles (e.g.
    # `MPIRef` under MPI) are not valid keys and manage their
    # own identity, so pass them through unchanged.
    # N.B. Values are WeakRefs: a strong Chunk value would root its own key
    # (chunk.handle === key), making every entry — and the data behind the
    # DRef — immortal.
    chunk.handle isa DRef || return chunk
    # N.B. Explicit lock/unlock rather than `lock(f, state.equiv_chunks)`: the
    # closure would capture `chunk` and allocate on every call. The critical
    # section is identical.
    lock(state.equiv_chunks)
    try
        ec = payload(state.equiv_chunks)
        existing = get(ec, chunk.handle, nothing)
        if existing !== nothing
            value = existing.value
            value === nothing || return value::C
        end
        ec[chunk.handle] = WeakRef(chunk)
        return chunk
    finally
        unlock(state.equiv_chunks)
    end
end

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
@reuse_scope function eager_submit_internal!(ctx, state, task, tid, payload::AnyPayload)
    if payload isa PayloadMulti
        thunk_ids = Sch.ThunkID[]
        for i in 1:payload.ntasks
            tid = payload_extract(payload, i) do p1
                eager_submit_internal!(ctx, state, task, tid, p1)
            end
            push!(thunk_ids, tid)
        end
        # Each sub-thunk was submitted (and self-scheduled) by the recursive
        # eager_submit_internal! call above, so there is nothing central to
        # wake here.
        return thunk_ids
    end
    payload::PayloadOne

    uid, future = payload.uid, payload.future
    fargs, options, reschedule = payload.fargs, payload.options, payload.reschedule

    # Eager DTask uid and Sch thunk id are the same value.
    id = Int(uid)

    @maybelog ctx timespan_start(ctx, :add_thunk, (;thunk_id=id), (;f=fargs[1], args=fargs[2:end], options, uid))

    # Keep the *values* of the original arguments alive across edge-wiring: the
    # loop below replaces `fargs` entries holding a `DTask`/`ThunkID`/`Chunk`
    # with a `Thunk`/`WeakChunk`, and nothing else roots the originals until
    # `reschedule_syncdeps!` has wired the edges (which preserves all referenced
    # tasks/chunks). Only GC-rooting is needed here -- the `Argument`/
    # `ArgPosition` wrappers themselves are never read back -- so push the bare
    # values instead of copying 2 objects per argument.
    old_fargs = @reusable_vector :eager_submit_internal!_old_fargs Any nothing 32
    for arg in fargs
        push!(old_fargs, value(arg))
    end

    syncdeps_vec = @reusable_vector :eager_submit_interal!_syncdeps_vec ThunkSyncdep ThunkSyncdep() 32
    if options.syncdeps !== nothing
        append!(syncdeps_vec, options.syncdeps)
    end

    # Lookup DTask/ThunkID -> Thunk
    # N.B. The `state.thunk_dict` critical sections below use explicit
    # lock/try/finally instead of `lock(f, state.thunk_dict)`: the `do` closures
    # capture `fargs`/`idx`/`arg` and so allocate once per argument. The locked
    # regions are unchanged (one acquisition per lookup), preserving both the
    # locking discipline and the lock ordering with `state.equiv_chunks` (which
    # is only ever taken with `thunk_dict` unheld).
    for (idx, arg) in enumerate(fargs)
        if valuetype(arg) <: DTask
            arg_tid = Int((value(arg)::DTask).uid)
            lock(state.thunk_dict)
            try
                d = Dagger.payload(state.thunk_dict)
                @inbounds fargs[idx] = Argument(arg.pos, d[arg_tid])
            finally
                unlock(state.thunk_dict)
            end
        elseif valuetype(arg) <: Sch.ThunkID
            arg_tid = (value(arg)::Sch.ThunkID).id
            lock(state.thunk_dict)
            try
                d = Dagger.payload(state.thunk_dict)
                @inbounds fargs[idx] = Argument(arg.pos, d[arg_tid])
            finally
                unlock(state.thunk_dict)
            end
        elseif valuetype(arg) <: Chunk
            chunk = find_equivalent_chunk(state, value(arg)::Chunk)
            #=FIXME:UNIQUE=#
            if chunk.handle isa DRef
                @inbounds fargs[idx] = Argument(arg.pos, WeakChunk(chunk))
            else
                # Non-DRef chunks (e.g. `MPIRef` under MPI) are not kept
                # alive by `equiv_chunks` (a `WeakKeyDict{DRef,Chunk}`, so
                # only DRef-backed wrappers get a strong keeper there).
                # Weakening such a chunk here would let its `Chunk` wrapper
                # be GC'd before the consuming task runs, expiring the
                # `WeakChunk` (observed on Julia 1.10, whose GC is more
                # eager). Keep a strong reference; it is released when the
                # task's `Thunk` is cleaned up.
                @inbounds fargs[idx] = Argument(arg.pos, chunk)
            end
        end
    end
    # TODO: Iteration protocol would be faster
    for idx in 1:length(syncdeps_vec)
        dep = syncdeps_vec[idx]::ThunkSyncdep
        @assert dep.id !== nothing && dep.thunk === nothing
        # N.B. Explicit lock/unlock (see above): a `do` closure here would
        # capture `dep` and allocate per syncdep.
        lock(state.thunk_dict)
        local thunk
        try
            thunk = Dagger.payload(state.thunk_dict)[dep.id.id]
        finally
            unlock(state.thunk_dict)
        end
        @inbounds syncdeps_vec[idx] = ThunkSyncdep(thunk)
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
    empty!(syncdeps_vec)

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

        # Thunks that become immediately ready during edge-wiring are
        # collected here and scheduled *after* releasing state.lock, so that
        # schedule_one! never runs under the submission lock.
        ready = Sch.Thunk[]
        @lock state.lock begin
            # Attach `thunk` within the scheduler
            # N.B. Explicit lock/unlock (see above) to avoid a closure capturing
            # `thunk`; the critical section is unchanged.
            lock(state.thunk_dict)
            try
                Dagger.payload(state.thunk_dict)[thunk.id] = WeakThunk(thunk)
            finally
                unlock(state.thunk_dict)
            end
            # Hold a strong reference until the thunk reaches a terminal state
            # (released in `task_delete!`). The weak `thunk_dict` entry alone
            # cannot keep an unfinished thunk alive once the user drops its
            # `DTask`, which would let GC collect a thunk that still has
            # pending dependents and deadlock the scheduler.
            push!(state.strong_thunks, thunk)
            #=FIXME:REALLOC=#
            Sch.reschedule_syncdeps!(state, thunk, ready)
            empty!(old_fargs) # reschedule_syncdeps! preserves all referenced tasks/chunks
            n_upstreams = @atomic thunk.pending_deps
            @dagdebug thunk :submit "Added to scheduler with $n_upstreams unresolved upstreams"
            if future !== nothing
                # Ensure we attach a future before the thunk is scheduled
                Sch._register_future!(ctx, state, task, tid, future, thunk_id, false)
                @dagdebug thunk :submit "Registered future"
            end
            @atomic thunk.valid = true

            # Reset sch_accessible for all syncdeps that can still finish.
            #
            # `sch_accessible` is a submission-window guard: it marks a syncdep
            # as undeletable from here until that syncdep's own `finish_task!`
            # clears it. Flagging an *already-finished* dep is therefore
            # unrecoverable -- its `finish_task!` has been and gone, so nothing
            # will ever clear the flag, and `delete_unused_task!` (which
            # requires `!sch_accessible`) can never fire again. The dep, and
            # every `Chunk` it holds as an argument, stays pinned in
            # `state.strong_thunks` for the life of the process.
            #
            # That is a real, unbounded leak whenever completed producers are
            # named as syncdeps, which Datadeps does routinely: its end-of-region
            # `unsafe_free!` tasks take syncdeps (`gather_free_syncdeps!`) on
            # compute tasks that have long since finished. It only bites when
            # Datadeps actually allocates copies -- i.e. when tasks read data
            # from another memory space -- so plain threaded runs (one shared
            # `CPURAMMemorySpace`, no copies, no free-tasks) never show it while
            # MPI leaks a full set of chunks per region.
            #
            # Skipping finished deps is safe, and is what the rest of the
            # scheduler already assumes: `reschedule_syncdeps!` (sch/util.jl)
            # tests the same `@atomic finished` and creates no edge, no
            # `pending_deps` increment, and no dependents entry for a finished
            # syncdep -- it is already satisfied and contributes nothing to
            # dataflow, so there is nothing to protect. Both this block and
            # `finish_task!` (via `handle_result!`) run under `state.lock`, so
            # the check cannot race with completion.
            #
            # A finished dep whose *value* is still needed downstream is kept
            # alive by the other flag instead: a consumer holding it as an
            # argument holds its `DTask`, so `eager_accessible` stays true, and
            # deletion requires both flags clear.
            if options.syncdeps !== nothing
                for dep_weak in options.syncdeps
                    dep = unwrap_weak_checked(dep_weak)
                    @assert dep.eager_accessible "GC bug: lost eager reference to syncdep"
                    if !(@atomic dep.finished)
                        dep.sch_accessible = true
                    end
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
struct UnrefThunk
    uid::UInt
    thunk::Thunk
    state
end
function unref_thunk!(unref::UnrefThunk)
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
    return
end
function (unref::UnrefThunk)()
    # Best-effort GC cleanup invoked once per `DTask` teardown, as the MemPool
    # destructor of the thunk's `DRef`. It is called from `datastore_delete`,
    # which frequently runs in a GC/finalizer context where task switches (and
    # therefore blocking locks) are illegal -- which is why this used to spawn a
    # fresh task per teardown (2 `Task`s + closures per `DTask`).
    #
    # Instead, hand the work to MemPool's existing long-lived `SEND_QUEUE`
    # reaper: `_enqueue_work(...; gc_context=true)` is finalizer-safe (it spins
    # on `trylock` of an unbounded channel with `GC.safepoint()`, never
    # blocking/yielding), reuses one task for all deferred teardown work, and is
    # already how MemPool defers its own device deletions from this same call
    # path. The work itself still runs on a regular task, so it may block on
    # `state.lock` as before.
    #
    # N.B. Cannot be a Dagger-owned reaper task+channel pair: a channel held in
    # a Dagger global with the reaper parked in its wait queue is reachable from
    # module state during `@compile_workload`, and Julia cannot serialize a live
    # `Task` into the package image.
    MemPool._enqueue_work(unref_thunk!, unref; gc_context=true)
    return
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
# Convert already-launched DTask args to ThunkID. Eager uid == Sch thunk id, so
# no side table is needed; `istaskstarted` tells us the task has been submitted.
function eager_process_elem_submission_to_local!(arg::Argument)
    T = valuetype(arg)
    @assert !(T <: Thunk) "Cannot use `Thunk`s in `@spawn`/`spawn`"
    if T <: DTask
        task = value(arg)::DTask
        if istaskstarted(task)
            #=FIXME:UNIQUE=#
            arg.value = Sch.ThunkID(Int(task.uid), task.thunk_ref)
        end
    end
end
function eager_process_elem_submission_to_local(arg::TypedArgument{T}) where T
    @assert !(T <: Thunk) "Cannot use `Thunk`s in `@spawn`/`spawn`"
    if T <: DTask
        task = value(arg)::DTask
        if istaskstarted(task)
            #=FIXME:UNIQUE=#
            tid = Sch.ThunkID(Int(task.uid), task.thunk_ref)
            # Preserve the argument position by re-wrapping in a TypedArgument;
            # returning a bare ThunkID drops the position and breaks the later
            # `map(Argument, ...)` conversion in `eager_launch!`.
            return TypedArgument(arg.pos, tid)
        end
    end
    return arg
end
function eager_process_args_submission_to_local!(spec::DTaskSpec{false})
    for arg in spec.fargs
        eager_process_elem_submission_to_local!(arg)
    end
end
function eager_process_args_submission_to_local(spec::DTaskSpec{true})
    # N.B. `map_or_ntuple` uses `ntuple(f, Val(length(xs)))` for tuples: with a
    # plain `Int` length, `ntuple` over a heterogeneous tuple is type-unstable
    # and boxes every element, while `Val` (which const-folds for a concrete
    # tuple type) unrolls and keeps each element's type.
    fargs = spec.fargs
    return map_or_ntuple(i->eager_process_elem_submission_to_local(fargs[i]), fargs)
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

function DTaskMetadata(spec::DTaskSpec)
    rt = spec.options.return_type
    if rt !== nothing && isconcretetype(rt) && rt !== Any
        return DTaskMetadata(rt)
    end
    return DTaskMetadata(eager_metadata(spec.fargs))
end
function eager_metadata(fargs)
    f = value(fargs[1])
    f = f isa StreamingFunction ? f.f : f
    arg_types = arg_chunktypes(fargs)
    return cached_return_type(f, arg_types)
end
# N.B. As in `eager_process_args_submission_to_local`, a `Val` length is used for
# tuple `fargs` (typed specs) so that `ntuple` unrolls over the heterogeneous
# tuple instead of boxing each element; `Vector` `fargs` (untyped specs) are
# dynamically-sized and keep the plain `Int` length.
@inline arg_chunktypes(fargs::Tuple) =
    ntuple(i->chunktype(value(fargs[i+1])), Val(length(fargs)-1))
arg_chunktypes(fargs::Vector) =
    ntuple(i->chunktype(value(fargs[i+1])), length(fargs)-1)

function eager_spawn(spec::DTaskSpec)
    # Generate new unlaunched DTask
    uid = eager_next_id()
    future = ThunkFuture()
    metadata = DTaskMetadata(spec)
    # Propagate inferred return type to options
    if isconcretetype(metadata.return_type)
        spec.options.return_type = metadata.return_type
    end
    return DTask(uid, future, metadata)
end

chunktype(t::DTask) = t.metadata.return_type

function eager_launch!(pair::DTaskPair)
    spec = pair.spec
    task = pair.task

    # Assign a name, if specified
    eager_assign_name!(spec, task)

    # Lookup DTask -> ThunkID
    fargs = if is_typed(spec)
        Argument[map(Argument, eager_process_args_submission_to_local(spec))...]
    else
        eager_process_args_submission_to_local!(spec)
        spec.fargs
    end

    # N.B. `spec.options.return_type` was already set to
    # `task.metadata.return_type` by `eager_spawn` (under the very same
    # `isconcretetype` guard, and `task.metadata` is the same object), so the
    # copy+assignment that used to live here was a no-op that allocated a fresh
    # `Options` per task. `spec.options` is likewise what the non-concrete branch
    # passed through unmodified, so submitting it directly is unchanged behavior.
    options = spec.options

    # Submit the task
    # N.B. `PayloadOne` is only read by `eager_submit_internal!`, which runs to
    # completion before `eager_submit!` returns (whether inline, via `exec!`, or
    # via `remotecall_fetch`), so it can be borrowed from the reusable cache
    # rather than freshly allocated (mirroring `payload_extract`).
    thunk_id = @take_or_alloc! PAYLOAD_ONE_CACHE[] PayloadOne p1 begin
        p1.uid = task.uid
        p1.future = task.future
        p1.fargs = fargs
        p1.options = options
        p1.reschedule = true
        eager_submit!(p1)
    end
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
    # Lookup DTask -> ThunkID
    all_fargs = map(pairs) do pair
        spec = pair.spec
        if is_typed(spec)
            return Argument[map(Argument, eager_process_args_submission_to_local(spec))...]
        else
            eager_process_args_submission_to_local!(spec)
            return spec.fargs
        end
    end
    # Propagate DTask return_type into options so created Thunks have chunktype for downstream inference
    all_options = Options[
        let opts = pair.spec.options
            isconcretetype(pair.task.metadata.return_type) ? (o = copy(opts); o.return_type = pair.task.metadata.return_type; o) : opts
        end
        for pair in pairs
    ]

    # Submit the tasks
    #=FIXME:REALLOC=#
    thunk_ids = eager_submit!(PayloadMulti(ntasks, uids, futures,
                                           all_fargs, all_options, true))
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
