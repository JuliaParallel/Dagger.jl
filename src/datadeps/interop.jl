### Interop boundary: plain `Dagger.@spawn` touching Datadeps-tracked data ###
#
# A `spawn_datadeps(...; sync=false)` region returns before its work has run
# and before its results have been written back to where the caller's data
# lives. Anything downstream that is *also* a Datadeps region is fine -- the
# next region plans against the same `state` and sees the dependencies. A plain
# `Dagger.@spawn` is not: it knows nothing about the region, so it can read an
# argument whose current replica is still off-origin, or race a task that is
# still writing it.
#
# The documented rule (docs/src/datadeps.md) is that `sync=false` requires
# everything downstream to stay inside Dagger's tracking. This file makes the
# common case of that actually hold, rather than relying on the caller to
# remember: a plain task whose argument the calling task's context tracks gets
# (a) the pending write-back for that argument emitted, and (b) syncdeps on
# every task that has touched it, including that write-back.
#
# It is deliberately *not* a substitute for `synchronize()`. It covers plain
# Dagger tasks, not plain Julia code -- nothing can intercept `sum(A)` -- and
# it only ever looks at the *calling* task's context.
#
# ### Why this doesn't block
#
# The obvious implementation is "targeted-synchronize the arguments, then
# submit", which would turn `Dagger.@spawn` from a non-blocking call into one
# that waits on unrelated compute. It isn't necessary: `flush_pending_writeback!`
# *emits* the write-back copies without waiting for them, so the plain task can
# simply take those copy tasks as syncdeps and let the scheduler order it. The
# caller's `@spawn` returns as fast as it always did.

"""
    DATADEPS_INTEROP[] -> Bool

Whether a plain `Dagger.@spawn` should be checked against the calling task's
`DataDepsContext` and given syncdeps on any tracked argument it touches
(default `true`).

Turning this off restores the previous behavior, in which such a task was
submitted with no relationship to the region that produced its data. That is
only safe if every such boundary is hand-synchronized; the default is `true`
because a silently wrong numerical answer is a far worse failure than the cost
of the check.

The cost on the path that doesn't care -- any program with no live Datadeps
context on the calling task, which is every non-Datadeps program -- is one
`TaskLocalValue` read per submission.
"""
const DATADEPS_INTEROP = Ref(true)

"""
    datadeps_managed_queue(queue::AbstractTaskQueue) -> Bool

Whether `queue` means "we are inside Datadeps' own planning", in which case
interop must not interfere: the region already derives the full dependency
structure for these tasks, and injecting extra syncdeps from a half-built
`state` would at best duplicate that and at worst deadlock a task against a
copy that is waiting on it.

Covers the region's own queue, the context queue that wraps it (also used for
the flush copies in `_do_synchronize!`, which must not take syncdeps on
themselves), and anything stacked on top of either.
"""
datadeps_managed_queue(::AbstractTaskQueue) = false
datadeps_managed_queue(::DataDepsTaskQueue) = true
datadeps_managed_queue(::ContextQueue) = true
datadeps_managed_queue(q::WaitAllQueue) = datadeps_managed_queue(q.upper_queue)
datadeps_managed_queue(q::InOrderTaskQueue) = datadeps_managed_queue(q.upper_queue)

"""
    maybe_add_interop_deps!(queue::AbstractTaskQueue, spec::DTaskSpec) -> Nothing

Give `spec` syncdeps on the Datadeps work that owns any argument it touches,
emitting that argument's pending write-back first so the plain task sees the
data at its origin.

Checks are ordered by cost, cheapest first, because this runs on every task
submission in the process:

1. `DATADEPS_INTEROP[]` and `DATADEPS_CONTEXT[] === nothing` -- a `Ref` read
   and a `TaskLocalValue` read. Every non-Datadeps program stops here.
2. The ambient queue is Datadeps' own (see [`datadeps_managed_queue`](@ref)).
3. Per argument, an `IdDict` probe of `state.raw_arg_to_chunk` -- no aliasing
   query, no allocation for the miss case.

An argument that misses every probe is left alone. That is a deliberate
under-approximation, and the reason this is not a replacement for
`synchronize()`: identity lookup does not catch a plain `Array` that
`unsafe_wrap`s memory inside a tracked array, since it has a distinct
`objectid` and no `parent` chain. Catching that needs a real aliasing query
against `state.ainfos_lookup`, which is a range query per argument per
submission -- affordable at the boundary, but not something to enable
by default without measuring it on a submission-bound workload first.
"""
function maybe_add_interop_deps!(queue::AbstractTaskQueue, spec::DTaskSpec)
    DATADEPS_INTEROP[] || return
    ddctx = DATADEPS_CONTEXT[]
    ddctx === nothing && return
    datadeps_managed_queue(queue) && return
    isdefined(ddctx, :state) || return
    _add_interop_deps!(ddctx, spec)
    return
end

# Out-of-line so the hot path above stays small enough to inline: the check
# that matters for programs that never touch Datadeps is the two loads.
@noinline function _add_interop_deps!(ddctx::DataDepsContext, spec::DTaskSpec)
    state = ddctx.state
    targets = nothing
    for arg in spec.fargs
        v = value(arg)
        # A `DTask` argument is already a real dependency edge that Dagger's
        # scheduler enforces; nothing to add, and `fetch`ing it here to find
        # its chunk would block the submitting task.
        v isa DTask && continue
        v, _ = unwrap_inout(v)
        v isa DTask && continue
        type_may_alias(typeof(v)) || continue
        arg_chunk = get(state.raw_arg_to_chunk, v, nothing)
        arg_chunk === nothing && continue
        for arg_w in keys(state.arg_owner)
            if arg_w.arg === arg_chunk
                targets === nothing && (targets = Set{ArgumentWrapper}())
                push!(targets, arg_w)
            end
        end
    end
    targets === nothing && return

    @lock ddctx.lock begin
        # Wait set first, against the state as it stands *before* the flush --
        # the copies the flush emits are collected separately below, and would
        # otherwise be counted twice.
        wait_tasks = _targeted_wait_tasks(state, targets)

        # Emit (do not wait on) the pending write-back for these arguments, so
        # the plain task reads current data at the origin. Tracked in
        # `ddctx.inflight` via the `ContextQueue`, exactly as a targeted
        # `synchronize` would have.
        n_before = length(ddctx.inflight)
        upper_queue = get_options(:task_queue, DefaultTaskQueue())
        flush_queue = ContextQueue(upper_queue, ddctx)
        with_options(; task_queue=flush_queue) do
            with_datadeps_planning_token() do
                flush_pending_writeback!(ddctx; targets)
            end
        end
        for idx in (n_before+1):length(ddctx.inflight)
            wait_tasks[ddctx.inflight[idx]] = nothing
        end

        isempty(wait_tasks) && return
        opts = spec.options
        syncdeps = opts.syncdeps = @something(opts.syncdeps, Set{ThunkSyncdep}())
        for task in keys(wait_tasks)
            push!(syncdeps, ThunkSyncdep(task))
        end
    end
    return
end
