### Dagger.synchronize: draining a DataDepsContext's deferred epilogue ###
#
# `spawn_datadeps(...; sync=false)` returns as soon as `f` has been planned,
# leaving write-back, frees, and waiting for everything to a later call in
# this file. Bare `Dagger.synchronize()` is always task-local -- it only ever
# touches the calling task's own `DataDepsContext` -- so that reaching outside
# the calling task is never implicit; `synchronize_task!`/`synchronize_all!`
# are the explicitly `!`-marked, wider-blast-radius alternatives.
#
# Order of operations, and why it's fixed (see `_do_synchronize!`):
#   1. Emit deferred write-back copies (`flush_pending_writeback!`).
#   2. Emit deferred frees (`flush_pending_frees!`), syncdeps computed against
#      the state as it now stands (which includes the write-backs from 1).
#   3. Quiesce planning queues (a no-op in this phase; see the N.B. below).
#   4. Wait for everything in flight, retaining/attributing failures.
#   5. Release retired `SlotReuseRegion`s and reclaim accel-specific per-task
#      state (e.g. MPI's `mpi_cleanup_tid`).
#   6. GPU fence (stub until a later phase).
#   7. Reset the context for the next epoch.
# Steps 1-2 must precede step 3 because the write-back/free tasks are
# themselves submitted through the planning queues -- quiescing first would
# strand them in a closed queue, either lost or deadlocked against step 4.

"""
    DataDepsPoisonedError <: Exception

Thrown by `spawn_datadeps` (or, mid-region, by [`apply_inflight_backpressure!`](@ref))
when a `DataDepsContext` has an unreported failure from an earlier region that
nobody has yet observed via a `synchronize`-family call. Only reachable under
`sync=false`: default settings always drain -- and thereby observe -- trailing
every region, so a caller using defaults never sees this.

Carries the original error and (if still available) its region's resolved
call-site backtrace, for diagnosis; call `Dagger.synchronize(; check_errors=true)`
(or let the failing region's own consumer do so) to clear the poison and see
the original failure directly.
"""
struct DataDepsPoisonedError <: Exception
    region::Int
    ex::Any
    bt::Union{Vector{Base.StackTraces.StackFrame},Nothing}
end
function Base.showerror(io::IO, e::DataDepsPoisonedError)
    print(io, "DataDepsPoisonedError: spawn_datadeps region $(e.region) failed and was never observed by a `synchronize`-family call; refusing to plan more work on top of it.\nUnderlying error: ")
    Base.showerror(io, e.ex)
    if e.bt !== nothing
        println(io, "\nRegion was queued from:")
        Base.show_backtrace(io, e.bt)
    end
end

"""
    DataDepsRegionError <: Exception

Wraps a task failure discovered by a `synchronize`-family call, annotated with
the `spawn_datadeps` region that queued the failing task and (if still
available) that region's resolved call-site backtrace.

Only used by the explicit `Dagger.synchronize`/`synchronize_task!`/
`synchronize_all!` API: the trailing, default-settings drain inside
`spawn_datadeps` itself rethrows the original error unwrapped, so
`sync=true` remains bit-for-bit identical to `spawn_datadeps`'s behavior
before this existed. Reach for `Dagger.Sch.unwrap_nested_exception` if you
need the original error programmatically; it unwraps this like it does
`CapturedException`/`RemoteException`/`DTaskFailedException`.
"""
struct DataDepsRegionError <: Exception
    region::Int
    ex::Any
    bt::Union{Vector{Base.StackTraces.StackFrame},Nothing}
end
function Base.showerror(io::IO, e::DataDepsRegionError)
    print(io, "DataDepsRegionError: a task queued by spawn_datadeps region $(e.region) failed:\n")
    Base.showerror(io, e.ex)
    if e.bt !== nothing
        println(io, "\nRegion was queued from:")
        Base.show_backtrace(io, e.bt)
    end
end
Sch.unwrap_nested_exception(err::DataDepsRegionError) = Sch.unwrap_nested_exception(err.ex)

"Resolve `ddctx.region_bt[region]`'s raw backtrace, or `nothing` if it's not (or no longer) recorded."
_resolve_region_bt(ddctx::DataDepsContext, region::Int) =
    haskey(ddctx.region_bt, region) ? stacktrace(ddctx.region_bt[region]) : nothing

"""
    flush_pending_writeback!(ddctx::DataDepsContext)

Emit the deferred write-back copies recorded in `ddctx.pending_writeback`
(see `distribute_tasks!`), then clear it. Must run with a `ContextQueue`
ambient as `:task_queue` (see `_do_synchronize!`) so the copy tasks it
launches are tracked in `ddctx.inflight`.

The skip condition (does the origin already hold a fully-current replica) is
evaluated here, against `ddctx.state` as it now stands -- not as it stood when
the write was recorded -- which is what lets an intervening region elide a
write-back that turned out to be unnecessary.
"""
function flush_pending_writeback!(ddctx::DataDepsContext;
                                  targets::Union{Set{ArgumentWrapper},Nothing}=nothing)
    isempty(ddctx.pending_writeback) && return
    state = ddctx.state
    write_num = ddctx.write_num
    # `targets === nothing` is the full flush; otherwise only the intersection
    # is emitted and the rest stays pending for a later call. Intersecting here
    # (rather than at the call site) keeps the `arg_w.hash` sort -- the thing
    # that makes emission order rank-uniform -- as the single ordering
    # authority for both modes.
    pending = targets === nothing ? ddctx.pending_writeback :
                                    intersect(ddctx.pending_writeback, targets)
    isempty(pending) && return
    @check_uniform(length(pending))
    for arg_w in sort(collect(pending); by=arg_w->arg_w.hash)
        @check_uniform(arg_w)
        arg = arg_w.arg
        origin_space = state.arg_origin[arg]
        current = get(state.arg_current, arg_w, nothing)
        if current !== nothing && origin_space in current
            remainder = NoAliasing()
        else
            remainder, _ = compute_remainder_for_arg!(state, origin_space, arg_w, write_num)
        end
        if remainder isa MultiRemainderAliasing
            origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
            enqueue_remainder_copy_from!(state, origin_space, arg_w, remainder, origin_scope, write_num)
        elseif remainder isa FullCopy
            origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
            enqueue_copy_from!(state, origin_space, arg_w, origin_scope, write_num)
        else
            @assert remainder isa NoAliasing "Expected NoAliasing, got $(typeof(remainder))"
            @dagdebug nothing :spawn_datadeps "Skipped copy-from (up-to-date): $origin_space"
            # N.B. Same gate as `@maybelog`, written out so the event `id`
            # (a `rand` call, plus the boxing it feeds) is only generated when
            # logging is actually enabled.
            sch_ctx = Sch.eager_context()
            if !(sch_ctx.log_sink isa TimespanLogging.NoOpLog)
                id = rand(UInt)
                timespan_start(sch_ctx, :datadeps_copy_skip, (;id), (;))
                timespan_finish(sch_ctx, :datadeps_copy_skip, (;id), (;thunk_id=0, from_space=origin_space, to_space=origin_space, arg_w, from_arg=arg, to_arg=arg))
            end
        end
    end
    ddctx.write_num = write_num + 1
    if targets === nothing
        empty!(ddctx.pending_writeback)
    else
        setdiff!(ddctx.pending_writeback, pending)
    end
    return
end

"""
    flush_pending_frees!(ddctx::DataDepsContext)

Free every Datadeps-allocated buffer in `ddctx.state`'s object cache that
isn't the user's own original data, computing each free's syncdeps against
`ddctx.state` as it now stands (after `flush_pending_writeback!`, so a
deferred write-back copy is itself synced against). Formerly the tail of
`distribute_tasks!`, run every region; now runs once per flush, over
whatever has accumulated in the object cache since the last one -- see
`pending_free`'s field comment (context.jl) for why eligibility is always
recomputed here rather than tracked incrementally.

Must run with a `ContextQueue` ambient as `:task_queue`, same as
`flush_pending_writeback!`.
"""
function flush_pending_frees!(ddctx::DataDepsContext)
    isempty(ddctx.pending_free) && return
    state = ddctx.state
    write_num = ddctx.write_num
    obj_cache = unwrap(state.ainfo_backing_chunk)
    chunk_to_ainfos = IdDict{Any,Vector{AliasingWrapper}}()
    for (ainfo, remote_arg_ws) in state.ainfo_arg
        for remote_arg_w in remote_arg_ws
            push!(get!(Vector{AliasingWrapper}, chunk_to_ainfos, remote_arg_w.arg), ainfo)
        end
    end
    freed = IdDict{Any,Nothing}()
    for remote_space in keys(obj_cache.values)
        remote_proc = first(processors(remote_space))
        free_scope = ExactScope(remote_proc)
        for (ainfo, remote_arg) in obj_cache.values[remote_space]
            is_original(obj_cache, remote_space, ainfo) && continue
            # Skip buffers handed to the slot cache: the next region over this
            # data expects to find them intact. Uses `ddctx.pending_retained_slots`,
            # not the scoped `SLOT_REUSE_REGION[]`/`slot_is_retained(slot)` that
            # hierarchical's own (never-deferred) free loop uses -- this flush can
            # run long after, and even outside of, the `spawn_datadeps` call whose
            # `retain_reusable_slots!` populated that scoped region, so the scoped
            # value is not reliably bound here. See `pending_retained_slots`'s
            # field comment (context.jl).
            slot_is_retained(ddctx, remote_arg) && continue
            haskey(freed, remote_arg) && continue
            freed[remote_arg] = nothing
            free_syncdeps = Set{ThunkSyncdep}()
            gather_free_syncdeps!(state, remote_space, ainfo, remote_arg, write_num, chunk_to_ainfos, free_syncdeps)
            Dagger.@spawn scope=free_scope syncdeps=free_syncdeps tag=datadeps_task_tag() Dagger.unsafe_free!(remote_arg)
        end
    end
    empty!(ddctx.pending_free)
    return
end

### Targeted synchronization: resolving `synchronize(args...)` to tracked data ###

"""
    _resolve_sync_targets(ddctx::DataDepsContext, args::Tuple) -> Union{Nothing,Set{ArgumentWrapper}}

Expand `args` into the set of tracked `ArgumentWrapper`s naming exactly the data
the caller asked to have usable, or `nothing` if that cannot be established --
in which case the caller must fall back to a full drain.

**`nothing` means "cannot narrow", never "nothing to do".** That distinction is
the whole safety argument here. An argument Datadeps has no record of might be
untracked (a plain local the region never saw, for which a narrow flush really
would be a legitimate no-op) or it might be tracked under a key this function
failed to derive -- and the two are indistinguishable from here. Treating the
ambiguous case as "nothing to wait for" would turn a resolution bug into a
silently unsynchronized read, which is precisely the failure mode that made the
`args` form a documented full-drain stub instead of shipping as a narrow flush.
Falling back means narrowing is *only ever* an optimization: it can fail to
speed things up, but it cannot fail to synchronize.

Resolution mirrors what `datadeps_tracked_args` (scheduling.jl) does during
planning, because it must land on the same keys planning used:

- A `DArray` is never itself tracked -- Datadeps tracks its individual
  `.chunks` -- so it expands to those. This is the case the plan called out as
  the more dangerous half of the stub: `synchronize(DA)` looking up the
  `DArray` object finds nothing at all.
- A `DTask` resolves through `fetch(t; raw=true)`, exactly as
  `populate_task_info!` did when it recorded the argument.
- Anything else is used as-is and looked up in `state.raw_arg_to_chunk`.

The `arg_chunk`-to-`ArgumentWrapper` step then takes *every* wrapper over that
chunk, across all `dep_mod`s, rather than trying to guess which sub-region the
caller meant: `synchronize(A)` means all of `A`.
"""
function _resolve_sync_targets(ddctx::DataDepsContext, args::Tuple)
    isempty(args) && return nothing
    isdefined(ddctx, :state) || return nothing
    state = ddctx.state
    # Resolved backing chunks, keyed by identity (matching `raw_arg_to_chunk`).
    chunks = IdDict{Any,Nothing}()
    for arg in args
        _collect_sync_chunks!(chunks, state, arg) || return nothing
    end
    isempty(chunks) && return nothing
    # One pass over `arg_owner` rather than a scan per argument: a DArray
    # expands to one chunk per tile, and the quadratic version showed up
    # immediately on anything tiled finely enough to be worth pipelining.
    targets = Set{ArgumentWrapper}()
    for arg_w in keys(state.arg_owner)
        haskey(chunks, arg_w.arg) && push!(targets, arg_w)
    end
    isempty(targets) && return nothing
    # Under uniform execution, *which* targets we resolved decides which
    # write-back copies get emitted -- a collective. A rank that resolved a
    # different set (or fell back to a full drain while others narrowed)
    # desynchronizes tags rather than just doing different amounts of work, so
    # make the disagreement raise here instead of deadlocking later. Hashed
    # over a sorted order, since `targets` is a `Set` and its iteration order
    # is not itself meaningful.
    @check_uniform(length(targets))
    @check_uniform(sort!(map(arg_w->arg_w.hash, collect(targets))))
    return targets
end

"Resolve one `synchronize` argument into `chunks`; `false` if it can't be resolved."
function _collect_sync_chunks!(chunks::IdDict{Any,Nothing}, state::DataDepsState, arg)
    if arg isa DArray
        # An empty DArray resolves vacuously; anything else must resolve every
        # tile, since a partially-resolved array is not a narrowing we can
        # honor (the unresolved tiles would go unsynchronized).
        for chunk in arg.chunks
            _collect_sync_chunks!(chunks, state, chunk) || return false
        end
        return true
    end
    leaf = arg isa DTask ? fetch(arg; raw=true) : arg
    arg_chunk = get(state.raw_arg_to_chunk, leaf, nothing)
    arg_chunk === nothing && return false
    chunks[arg_chunk] = nothing
    return true
end

"""
    _targeted_wait_tasks(state::DataDepsState, targets) -> IdDict{DTask,Nothing}

The tasks that must finish before `targets` are usable from plain code: every
writer and reader of every ainfo overlapping any replica of any target.

Waiting on the *last* writer of an ainfo transitively covers every earlier
one -- Dagger's scheduler will not start a task before its syncdeps finish, and
each writer took the previous writer as a syncdep -- so this set does not need
to be transitively closed by hand.

Iterating `state.remote_arg_w[arg_w]` walks every memory space the argument has
a replica in, not just its origin. A task running against an off-origin replica
is exactly the one a region-barrier-free pipeline leaves in flight, and it is
the one worth catching.
"""
function _targeted_wait_tasks(state::DataDepsState, targets::Set{ArgumentWrapper})
    out = IdDict{DTask,Nothing}()
    for arg_w in targets
        spaces = get(state.remote_arg_w, arg_w, nothing)
        spaces === nothing && continue
        for (_space, remote_arg_w) in spaces
            ainfo = get(state.ainfo_cache, remote_arg_w, nothing)
            ainfo === nothing && continue
            gather_overlap_tasks!(state, ainfo, out)
        end
    end
    return out
end

const _VALID_GPU_SYNC = (:fence, :block, :none)

"""
    _validate_gpu_sync(gpu_sync::Symbol) -> Symbol

Reject an invalid `gpu_sync` at the public API boundary.

Called by every entry point *before* the no-context early return, deliberately:
whether a caller's arguments are valid should not depend on whether this task
happens to have a live `DataDepsContext` right now. Otherwise
`synchronize(; gpu_sync=:bogus)` raises or silently succeeds depending on
nothing more than how recently something drained -- the sort of
state-dependent validation that lets a typo survive testing and surface much
later. The drain implementations re-check; the cost is one `in` over a
3-tuple.
"""
function _validate_gpu_sync(gpu_sync::Symbol)
    gpu_sync in _VALID_GPU_SYNC ||
        throw(ArgumentError("gpu_sync must be one of $_VALID_GPU_SYNC, got $(repr(gpu_sync))"))
    return gpu_sync
end

"""
    _gpu_sync_spaces!(state::DataDepsState, gpu_sync::Symbol) -> Nothing

Device-synchronize every memory space this context has placed data in, so that
kernels still queued when their Dagger task returned have actually run by the
time `synchronize` does.

Why it exists: a GPU `Dagger.execute!` does not synchronize its stream -- see
the "Synchronization must be done when accessing result or args" N.B. in
`ext/ROCExt.jl`/`ext/OpenCLExt.jl` -- it relies on the DtoH `move` to do it.
That covers the common case, where the write-back copy *is* a DtoH move. It
does not cover an argument whose *origin* is already device memory: the
write-back is then correctly elided (the origin already holds the current
replica), no `move` runs, and nothing in Dagger has synchronized anything by
the time `synchronize()` returns.

!!! note "Not demonstrated to be load-bearing"
    This closes a gap that is real on paper -- Dagger makes no guarantee here
    -- but an A/B of `gpu_sync=:none` against `:fence` on exactly that case
    (device-origin argument, elided write-back, 4M-element kernel heavy
    enough to still be running, 25 reps each in separate processes) found
    **0/25 stale reads either way** on ROCm/gfx1030. The likely reason is that
    the host's own readback is itself queued on the device and ordered after
    the kernel by the backend's stream semantics, so the window never opens
    for the access patterns tried. Treat this as defensive: it makes
    `gpu_sync` mean something and removes a documented assumption, but do not
    cite it as a bug fix, and do not assume `:none` is unsafe on the strength
    of this comment alone.

Spaces are derived from `state.remote_args` rather than tracked incrementally
(the `touched_spaces` field this replaces): its keys are exactly the spaces
slots have been generated in, `generate_slot!` records an entry even for the
same-space pass-through case, and deriving costs nothing on the planning hot
path -- where a per-task `push!` would have needed `ddctx.lock` to be safe
against parallel hierarchical partitions.

`gpu_synchronize(::Processor)` has a `nothing` fallback, so CPU spaces cost a
no-op call and need no filtering here.

`:fence` and `:block` currently do the same thing. Dagger's GPU layer exposes
only a blocking device synchronize; a true fence (record an event, make later
work depend on it, don't block the host) has no hook to hang off yet. The
parameter keeps them distinct because the *contracts* differ, so a real fence
can land under `:fence` without a breaking change -- and because `:none` is a
meaningful, and now genuinely unsafe, opt-out.
"""
function _gpu_sync_spaces!(state::DataDepsState, gpu_sync::Symbol)
    gpu_sync === :none && return
    for space in keys(state.remote_args)
        for proc in processors(space)
            if root_worker_id(proc) == myid()
                gpu_synchronize(proc)
            else
                # A value that crossed a serialization boundary was already
                # synchronized by the `move` that sent it (same reasoning as
                # `sync_with_context` in the GPU extensions), but a task that
                # ran remotely and left its result in remote device memory was
                # not -- so ask the owning worker to do it.
                remotecall_wait(gpu_synchronize, root_worker_id(proc), proc)
            end
        end
    end
    return
end

"""
    _do_synchronize!(ddctx; write_back, free, gpu_sync, check_errors, wrap_errors, from_owner) -> Nothing

The shared drain implementation behind `synchronize`/`synchronize_task!`/
`synchronize_all!` and `spawn_datadeps`'s own trailing `sync=true` call. See
the top of this file for the fixed step order.

`wrap_errors` distinguishes the internal, bit-for-bit-compatible caller
(`spawn_datadeps`, which must rethrow a discovered task failure completely
unwrapped so `sync=true` matches this function's pre-existence behavior
exactly) from the public API (which wraps in `DataDepsRegionError` to name the
failing region). `from_owner` is whether the calling task is `ddctx.owner`;
when it isn't (`synchronize_task!`/`synchronize_all!`), an `:fence` request is
downgraded to `:block`, since recording a fence is only meaningful on the
owner's own stream/task (see the GPU-fence phase's design once it lands) --
today this makes no observable difference, since both are no-ops.

Poisoning (`ddctx.err`/`err_region`) clears only when this call both captures
an error *and* is asked to (and does) report it (`check_errors=true`): a
`check_errors=false` drain still completes the drain (nothing is left
in-flight, buffers are freed, `state` is reset) but leaves the poison in
place, since nobody has actually been told about the failure yet.
"""
function _do_synchronize!(ddctx::DataDepsContext;
                          write_back::Bool, free::Bool, gpu_sync::Symbol,
                          check_errors::Bool, wrap_errors::Bool, from_owner::Bool)
    _validate_gpu_sync(gpu_sync)
    if !from_owner && gpu_sync === :fence
        gpu_sync = :block
    end

    err = nothing
    err_region = 0
    resolved_bt = nothing
    @lock ddctx.lock begin
        # Steps 1-2. Hierarchical leaves `ddctx.state` undef and always runs
        # its own write-back/free epilogue synchronously before
        # `spawn_datadeps` returns (it forces `sync=true`), so there is
        # nothing deferred to flush on that path.
        #
        # `flush_pending_writeback!`/`flush_pending_frees!` allocate tags
        # (`datadeps_task_tag()` -> `to_tag()`) and issue `check_uniform`
        # collectives under uniform (MPI/SPMD) execution, exactly like
        # planning does -- so they're guarded by the same process-global
        # token (`with_datadeps_planning_token`, context.jl), preventing a
        # concurrent planner (or another drain) on this rank from
        # interleaving its own tag/collective calls with these. A no-op
        # wrapper outside uniform execution.
        if isdefined(ddctx, :state)
            upper_queue = get_options(:task_queue, DefaultTaskQueue())
            flush_queue = ContextQueue(upper_queue, ddctx)
            with_options(; task_queue=flush_queue) do
                with_datadeps_planning_token() do
                    write_back && flush_pending_writeback!(ddctx)
                    free && flush_pending_frees!(ddctx)
                end
            end
        end

        # Step 3: quiesce planning queues. Nothing in this phase persists a
        # `BatchedEnqueueQueue`/`AsyncEnqueueQueue` past its own
        # `distribute_tasks_hierarchical!` call -- hierarchical always
        # finishes its own epilogue before `_spawn_datadeps` returns, since it
        # forces `sync=true` -- so there is nothing to quiesce here yet. This
        # becomes real once hierarchical planning is allowed to outlive a
        # region.

        # Step 4: wait for everything in flight. Only the *first* failure is
        # recorded, attributed via `task_region` to the region that actually
        # queued the failing task -- not necessarily the most recently
        # planned region, which may be a later, perfectly healthy one sharing
        # this context.
        drained_tasks = copy(ddctx.inflight)
        for task in ddctx.inflight
            try
                fetch(task; move_value=false, unwrap=false)
            catch task_err
                if ddctx.err === nothing
                    ddctx.err = task_err
                    ddctx.err_region = get(ddctx.task_region, task, ddctx.region_id)
                end
            end
            delete!(ddctx.task_region, task)
        end
        empty!(ddctx.inflight)

        # Step 5: release this epoch's retained per-region slot caches (safe
        # now that every task that could still be touching a checked-out slot
        # has been waited on above), and reclaim accel-specific per-task
        # state (under MPI, `mpi_cleanup_tids!`'s `_MPIREF_TID` sub-id
        # counters -- see the N.B. in `distribute_tasks!`: "reclaim at
        # wait_all" now means "reclaim at synchronize").
        #
        # This is safe even though `drained_tasks` can now span every region
        # planned on `ddctx` since the last full drain (not just "the" region,
        # now that regions pipeline): each tid's sub-counter is only ever
        # touched from two places, both keyed by that *task's own* uid --
        # `populate_task_info!`/`get_or_generate_slot!` during planning
        # (before the task is even enqueued for execution) and
        # `schedule_argument_move` during the task's own execution (before
        # `fetch` on it, just above, can return). So by the time a task
        # appears in `drained_tasks` and has been fetched, nothing can still
        # be generating sub-ids under its tid, regardless of how many other
        # (earlier or later) regions share this context. Copy/free tasks
        # never populate `_MPIREF_TID` at all -- `datadeps_task_tag()` runs
        # before they exist as tasks, so it always takes `take_ref_id!`'s
        # "generic" branch (the global, never-reclaimed `next_id()` counter),
        # not `next_ref_sub_id!`.
        for slots in ddctx.retiring_slots
            release_slot_reuse_region!(slots)
        end
        empty!(ddctx.retiring_slots)
        cleanup_tasks_accel!(current_acceleration(), drained_tasks)

        # Step 6: device-synchronize every space this context placed data in.
        # Must come after step 4 (waiting on the tasks) -- there is no point
        # synchronizing a stream before the task that queues onto it has even
        # been submitted -- and before step 7 resets `state`, which is where
        # the space list comes from.
        if isdefined(ddctx, :state)
            _gpu_sync_spaces!(ddctx.state, gpu_sync)
        end

        # Step 7: reset for the next epoch. Every task that could reference
        # the old `state`'s cache (copies, frees, the user's own tasks) has
        # now retired, so it's safe to start fresh.
        if isdefined(ddctx, :state)
            ddctx.state = DataDepsState()
        end
        empty!(ddctx.pending_free)
        empty!(ddctx.pending_retained_slots)
        # `pending_writeback` is already empty when `write_back` was true
        # (the flush above cleared it); if the caller asked to skip
        # write-back, leave it for the next flush to pick up.

        err = ddctx.err
        err_region = ddctx.err_region
        # Every region's tasks have retired by this point, so no backtrace is
        # needed except possibly the one we're about to report (or, if
        # `check_errors=false`, the one a *later* call will still need).
        for region in collect(keys(ddctx.region_bt))
            region == err_region && continue
            delete!(ddctx.region_bt, region)
        end
        if err !== nothing && check_errors
            resolved_bt = _resolve_region_bt(ddctx, err_region)
            delete!(ddctx.region_bt, err_region)
            # This call is about to report the failure to its caller --
            # that's what "observed" means. Clear the poison so a later
            # `spawn_datadeps` isn't refused for a failure its caller has
            # already been given the chance to handle. A drain that
            # swallows the error (`check_errors=false`) leaves it in place:
            # nobody has actually been told about it yet.
            ddctx.err = nothing
            ddctx.err_region = 0
        end
    end

    if err !== nothing && check_errors
        if wrap_errors
            throw(DataDepsRegionError(err_region, err, resolved_bt))
        else
            throw(err)
        end
    end
    return
end

"""
    _do_synchronize_targeted!(ddctx, targets; write_back, gpu_sync, check_errors, from_owner) -> Nothing

Partial drain: make exactly `targets` usable from plain code, leaving every
other in-flight task running and the context alive for further regions.

Steps, and what is deliberately *not* here compared to `_do_synchronize!`:

1. Emit deferred write-back copies, but only for targets
   (`flush_pending_writeback!(...; targets)`). The rest stay pending.
2. Wait on the target's dependency closure (`_targeted_wait_tasks`) plus the
   copies just emitted in step 1, and remove exactly those from `inflight`.
3. **No frees.** Whether a Datadeps-allocated buffer is dead depends on every
   region's reads and writes, not just the targeted one, so a narrow free is
   not a narrowing of the full free loop -- it is a different, harder question.
   Frees stay pending for the next full drain. This costs memory, not
   correctness, and it is the reason `free` is not a parameter here.
4. **No state reset, no slot release, no `region_bt` eviction.** All three are
   justified in `_do_synchronize!` by "every task that could reference this has
   retired", which is exactly what a partial drain does not establish.

`cleanup_tasks_accel!` *is* called, on the subset actually drained: the
argument for it in `_do_synchronize!` (step 5) is already per-task -- a tid's
sub-counter is only touched during that task's own planning and execution, so
fetching the task is what makes it reclaimable -- and does not depend on the
drain being total.

A discovered failure is recorded and reported exactly as in the full drain, but
poison is *not* cleared even when reported: other regions on this context are
still in flight and may yet fail, and a partial drain has not established that
the pipeline is healthy enough to keep planning on. The caller gets the error;
the context stays poisoned until something drains it fully.
"""
function _do_synchronize_targeted!(ddctx::DataDepsContext, targets::Set{ArgumentWrapper};
                                   write_back::Bool, gpu_sync::Symbol,
                                   check_errors::Bool, from_owner::Bool)
    _validate_gpu_sync(gpu_sync)
    if !from_owner && gpu_sync === :fence
        gpu_sync = :block
    end

    err = nothing
    err_region = 0
    resolved_bt = nothing
    @lock ddctx.lock begin
        state = ddctx.state
        # The closure is computed *before* the write-back copies are emitted:
        # those copies are themselves synced against the tasks below, so
        # including them here would be redundant, and they are picked up by
        # the `n_before` slice instead.
        wait_tasks = _targeted_wait_tasks(state, targets)

        n_before = length(ddctx.inflight)
        if write_back
            upper_queue = get_options(:task_queue, DefaultTaskQueue())
            flush_queue = ContextQueue(upper_queue, ddctx)
            with_options(; task_queue=flush_queue) do
                with_datadeps_planning_token() do
                    flush_pending_writeback!(ddctx; targets)
                end
            end
        end
        # Everything appended to `inflight` by the flush above is a write-back
        # copy for a target, so it belongs to the wait set by construction.
        for idx in (n_before+1):length(ddctx.inflight)
            wait_tasks[ddctx.inflight[idx]] = nothing
        end

        drained = DTask[]
        remaining = DTask[]
        for task in ddctx.inflight
            if haskey(wait_tasks, task)
                try
                    fetch(task; move_value=false, unwrap=false)
                catch task_err
                    if ddctx.err === nothing
                        ddctx.err = task_err
                        ddctx.err_region = get(ddctx.task_region, task, ddctx.region_id)
                    end
                end
                delete!(ddctx.task_region, task)
                push!(drained, task)
            else
                push!(remaining, task)
            end
        end
        copy!(ddctx.inflight, remaining)
        cleanup_tasks_accel!(current_acceleration(), drained)

        # Device-synchronize, same as `_do_synchronize!`'s step 6 and for the
        # same reason. Deliberately *not* narrowed to the targets' own spaces:
        # `gpu_synchronize` is a whole-device operation, so there is nothing
        # finer to ask for, and skipping a space would only be sound if we
        # could prove no target has a replica there.
        _gpu_sync_spaces!(state, gpu_sync)

        err = ddctx.err
        err_region = ddctx.err_region
        if err !== nothing && check_errors
            resolved_bt = _resolve_region_bt(ddctx, err_region)
        end
    end

    if err !== nothing && check_errors
        throw(DataDepsRegionError(err_region, err, resolved_bt))
    end
    return
end

"""
    Dagger.synchronize(; write_back=true, free=true, gpu_sync=:fence, check_errors=true)
    Dagger.synchronize(args...; kwargs...)

Drain the *calling task's* `DataDepsContext`: emit any deferred write-back
copies and frees left by earlier `spawn_datadeps(...; sync=false)` regions,
wait for everything in flight, and reset for the next epoch. A no-op if the
calling task has no context (nothing has ever called `spawn_datadeps` on it)
or the context is already fully drained.

Always task-local -- this never touches another task's context. Use
[`synchronize_task!`](@ref)/[`synchronize_all!`](@ref) for that; they are
separately named and `!`-marked so reaching beyond the calling task is never
implicit.

- `write_back`/`free`: whether to emit the corresponding deferred work at all
  (both default `true`). Setting either `false` leaves the corresponding
  pending set untouched for a later call to pick up.
- `gpu_sync`: `:fence` (default), `:block`, or `:none`. Accepted and
  validated now; real per-space GPU fencing is a later phase's work; until
  then this is a documented no-op regardless of the value passed (correctness
  never depended on it, since every task is already waited on synchronously
  above).
- `check_errors`: whether a discovered task failure is rethrown from this
  call (default `true`) or silently recorded and swallowed. Either way, the
  drain itself always completes fully; `check_errors=false` only changes
  whether *this* call raises, not whether the work happens. See
  `_do_synchronize!`'s docstring for exactly when this clears the context's
  poisoned state.

`Dagger.synchronize(A, B, ...)` restricts the drain to the named values: it
emits the deferred write-back for `A`/`B` only, waits only on the tasks that
have read or written them (across every memory space they have a replica in),
and leaves every other in-flight task running and the context alive for
further regions. This is what makes a pipeline usable -- you can take one
array's results without collapsing the pipeline behind it.

Three things the targeted form deliberately does *not* do, none of which
affect correctness:

- It never frees. Whether a Datadeps-allocated buffer is dead depends on every
  region's accesses, not just the named one, so frees stay pending for the
  next full drain. `free` is accepted for signature compatibility and ignored.
- It does not reset the context, so later regions keep their carried-over
  state and no data is re-copied.
- It does not clear a poisoned context. You get the error; a full
  `Dagger.synchronize()` is still required to clear it.

If any argument cannot be resolved to data this context tracks, the call falls
back to a full drain rather than guessing. Narrowing is therefore only ever an
optimization -- it can fail to be lazy, never fail to synchronize. A `DArray`
resolves to its individual chunks (Datadeps tracks those, not the `DArray`
object), so `synchronize(DA)` does the right thing.

!!! warning "MPI/SPMD uniformity"
    Under uniform (SPMD/MPI) execution, every rank must call `synchronize` with
    arguments naming the *same* data in the same order: which write-back copies
    get emitted is a collective decision, and a rank that narrows differently
    desynchronizes tags rather than merely doing less work. The resolved target
    count and each target are passed through `check_uniform`, so a mismatch
    raises rather than hangs when `Dagger.check_uniformity!(true)` is active.
"""
function synchronize(args...; write_back::Bool=true, free::Bool=true,
                     gpu_sync::Symbol=:fence, check_errors::Bool=true)
    _validate_gpu_sync(gpu_sync)
    ddctx = DATADEPS_CONTEXT[]
    ddctx === nothing && return nothing
    targets = _resolve_sync_targets(ddctx, args)
    if targets === nothing
        _do_synchronize!(ddctx; write_back, free, gpu_sync, check_errors,
                         wrap_errors=true, from_owner=true)
    else
        _do_synchronize_targeted!(ddctx, targets; write_back, gpu_sync,
                                  check_errors, from_owner=true)
    end
    maybe_drop_context!()
    return nothing
end

"""
    synchronize_task!(t::Task)
    synchronize_task!(t::Task, args...; kwargs...)

Drain `t`'s `DataDepsContext` from the calling task. Unlike bare
[`synchronize`](@ref), this reaches outside the calling task -- hence the
separate name and the `!` -- so use it when you need to know that *another*
task's pending Datadeps work has completed (its write-backs are visible, its
buffers are freed) before proceeding. A no-op if `t` has no live context.

Safe to call while `t` is still running: this only waits on `t`'s
*already-launched* Datadeps tasks and flushes its already-recorded pending
write-back/free sets, under `t`'s context lock (shared with `t`'s own
planning, so the two don't race). It does not, and cannot, stop `t` from
enqueuing more work concurrently.

`gpu_sync=:fence` is downgraded to `:block` here regardless of what's
requested: a fence is only meaningful recorded on the owning task's own
stream, and this call doesn't run on it. (No observable difference until a
later phase adds real per-space fencing.)
"""
function synchronize_task!(t::Task, args...; write_back::Bool=true, free::Bool=true,
                           gpu_sync::Symbol=:fence, check_errors::Bool=true)
    _validate_gpu_sync(gpu_sync)
    if t === current_task()
        return synchronize(args...; write_back, free, gpu_sync, check_errors)
    end
    ddctx = lock(DATADEPS_CONTEXT_REGISTRY) do reg
        get(reg, t, nothing)
    end
    ddctx === nothing && return nothing
    targets = _resolve_sync_targets(ddctx, args)
    if targets === nothing
        _do_synchronize!(ddctx; write_back, free, gpu_sync, check_errors,
                         wrap_errors=true, from_owner=false)
    else
        _do_synchronize_targeted!(ddctx, targets; write_back, gpu_sync,
                                  check_errors, from_owner=false)
    end
    return nothing
end

"""
    synchronize_all!()
    synchronize_all!(args...; kwargs...)

Drain every task's `DataDepsContext` currently registered in this process.
The widest-blast-radius form -- prefer [`synchronize`](@ref)/
[`synchronize_task!`](@ref) when the calling or a specific task's context is
what actually needs draining.

Every registered context is drained even if an earlier one fails; failures
are collected and, if `check_errors=true` (the default) and any occurred,
raised together as a `CompositeException` (or the single error directly, if
only one context failed) once every context has been given the chance to
drain.

Unlike [`synchronize`](@ref)/[`synchronize_task!`](@ref), `args` do **not**
narrow anything here. Draining every context completely is this function's
entire contract, and there is no useful reading of "drain everything, but only
partly" -- values naming data in one context would say nothing about the
others. They are accepted for signature symmetry and ignored; reach for the
narrower calls if you want a narrow drain.
"""
function synchronize_all!(args...; write_back::Bool=true, free::Bool=true,
                          gpu_sync::Symbol=:fence, check_errors::Bool=true)
    _validate_gpu_sync(gpu_sync)
    targets = lock(DATADEPS_CONTEXT_REGISTRY) do reg
        collect(values(reg))
    end
    errors = Any[]
    for ddctx in targets
        from_owner = ddctx.owner === current_task()
        try
            # Pass `check_errors` straight through: a context that isn't
            # asked to report stays poisoned (consistent with bare
            # `synchronize`), and simply never lands in `errors` below.
            _do_synchronize!(ddctx; write_back, free, gpu_sync, check_errors,
                             wrap_errors=true, from_owner)
        catch err
            push!(errors, err)
        end
    end
    if check_errors && !isempty(errors)
        length(errors) == 1 ? throw(errors[1]) : throw(CompositeException(errors))
    end
    return nothing
end

"""
    issynchronized() -> Bool

Whether the calling task's `DataDepsContext` (if any) is fully drained: no
in-flight tasks, nothing deferred, no unreported error. `true` if the calling
task has never created one.
"""
function issynchronized()
    ddctx = DATADEPS_CONTEXT[]
    ddctx === nothing && return true
    return @lock ddctx.lock context_drained(ddctx)
end

"""
    arm_task_exit_drain!(ddctx::DataDepsContext)

Spawn a lightweight watcher that drains `ddctx` once its owning task finishes,
so a context left with deferred work when its owning task returns doesn't
vanish silently -- there would otherwise be nobody left to ever call
`synchronize`. Called once per context, via `maybe_arm_drain_watcher!`
(`context.jl`), only once a context is found to have survived past a
`spawn_datadeps` call.

This runs on a *separate* task from the owner (Julia has no general "run this
right before a `Task` returns" hook to install after the fact), so it cannot
literally rethrow into the owner's own return path; a drain failure is
reported via `@error` instead, the same way an unexpectedly-failed background
task in this codebase (`errormonitor`) would be -- loudly, to stderr,
immediately when it happens, rather than the silence this exists to prevent.
If the owner itself failed, its own failure is already visible through
however it's being waited on/fetched; this only speaks up for a context left
with *unreported* work behind an otherwise-successful-looking task.

N.B. This polls `istaskdone(owner)` rather than `wait(owner)`. `wait` on an
arbitrary `Task` that nothing has ever waited on before -- the root/REPL task
being the common case here -- can lose a race in Julia's lazy initialization
of that task's `donenotify` condition and throw a bare `TypeError` instead of
either blocking or reporting the task's own failure (reproduces with plain
`Base.errormonitor` alone, nothing Dagger-specific). Polling sidesteps that
class of bug entirely, at the cost of a coarse-grained delay before this
notices the owner is done.

This is a best-effort primary mechanism precisely because Julia doesn't offer
a stronger hook here; the `finalizer` installed alongside it in
`DataDepsContext`'s constructor is the backstop for a task dropped without
ever being observed done at all.
"""
function arm_task_exit_drain!(ddctx::DataDepsContext)
    owner = ddctx.owner
    Threads.@spawn begin
        # The polling loop can outlive the process's own event loop (nothing
        # requires the owner to ever finish, e.g. a long-lived REPL/server
        # task); if `sleep` itself starts throwing, the runtime is shutting
        # down around us and there is no meaningful report left to make --
        # exit quietly rather than adding `@error` noise to a process that's
        # already on its way out.
        try
            while !istaskdone(owner)
                sleep(0.05)
            end
        catch
            return
        end
        try
            if !context_drained(ddctx)
                _do_synchronize!(ddctx; write_back=true, free=true, gpu_sync=:block,
                                 check_errors=true, wrap_errors=true, from_owner=false)
            end
        catch err
            @error "Dagger Datadeps task-exit drain failed" exception=(err, catch_backtrace())
        end
    end
    return nothing
end
