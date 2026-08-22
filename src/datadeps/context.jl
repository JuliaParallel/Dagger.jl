### DataDepsContext: task-local state shared across (eventually async) regions ###
#
# `spawn_datadeps` used to be a full barrier: `DataDepsState` was built fresh
# per region (`distribute_tasks!`, queue.jl) and dropped when the region ended,
# and the region epilogue restored the world (write everything back to origin,
# free every slot, wait for it all) precisely *because* nothing survived to
# the next region. Phase 3 introduced this container without changing that.
# Phase 4 is where it starts mattering: `state`, `inflight`, and the deferred
# write-back/free sets now genuinely survive across regions on the flat path,
# up to whichever `Dagger.synchronize()`-family call next drains them, and
# `spawn_datadeps(...; sync=false)` returns without waiting for any of it.
#
# `state`/`inflight`'s survival and the free loop's deferral are one change,
# not two: the free loop `unsafe_free!`s buffers that `state.ainfo_backing_chunk`
# still lists as live, so a state carried forward while frees still ran every
# region would hand out references to memory the *next* region's slots may
# already be reusing. See `distribute_tasks!` (queue.jl) and
# `flush_pending_writeback!`/`flush_pending_frees!` (this file) for how the two
# halves land together.
#
# The hierarchical path is explicitly out of scope for this phase (it keeps
# its own region-scoped `DataDepsState`s, reconciled at the end of every
# region as before) and forces `sync=true` -- see `spawn_datadeps`. `ctx.state`
# stays `#undef` there, exactly as in Phase 3.

"""
    DataDepsContext

Per-Julia-task home for Datadeps planning state, spanning every `spawn_datadeps`
region run by `owner` between two drains (`Dagger.synchronize`,
`synchronize_task!`, `synchronize_all!`, the task-exit drain, or the GC
finalizer backstop). See the top of this file for why this exists.

Reached via [`get_context!`](@ref) (creates on first use) and torn down by
[`maybe_drop_context!`](@ref) (once drained). Never construct one directly.
"""
mutable struct DataDepsContext
    # Persists across regions on the flat path: region N+1's planning consults
    # region N's `ainfos_owner`/`ainfos_readers`/`arg_current` directly, instead
    # of starting over. Reset to a fresh, empty `DataDepsState` at the end of
    # every full drain (see `_do_synchronize!`, step 7) -- by that point
    # everything has been written back and freed, so there is no history left
    # worth keeping, and starting fresh bounds this context's memory to one
    # "epoch" (one span between drains) rather than the process lifetime;
    # trimming a *single* long-running epoch's state is Phase 5's job.
    #
    # Left `#undef` by the constructor and never assigned on the hierarchical
    # path (an optimization to avoid an unused `tochunk` per region there,
    # since hierarchical plans against its own per-partition states and never
    # reads this field). Guard any access with `isdefined(ctx, :state)`.
    state::DataDepsState

    # Rebuilt fresh every region (both flat and hierarchical): `SlotReuseRegion`
    # eligibility is only valid for the region that computed it. What's new in
    # this phase is that a region's `SlotReuseRegion` cannot be released
    # (`release_slot_reuse_region!`) the moment its *planning* finishes anymore
    # -- the copy/free tasks that actually touch a checked-out slot now
    # outlive the region under `sync=false`, and releasing early would let a
    # concurrently-planning later region take a slot those tasks are still
    # writing into. So each region's `SlotReuseRegion` is hung off
    # `retiring_slots` at region end instead of released inline, and released
    # for real once that region's tasks are known to have drained (currently:
    # at the next full `_do_synchronize!`, which waits out all of `inflight`
    # first -- see step 5). `slots` itself always holds the *current* region's
    # (the one being actively planned).
    slots::SlotReuseRegion
    memo::ChunkAinfoMemo
    retiring_slots::Vector{SlotReuseRegion}

    # Monotonic across the owner's entire lifetime, never reset per region.
    # See the N.B. in `distribute_tasks!` (queue.jl). Now load-bearing across
    # regions, not just in preparation for it: `state` surviving means a later
    # region's `write_num` comparisons genuinely do see an earlier region's
    # entries.
    write_num::Int
    region_id::Int

    # Outstanding work, populated for real starting in this phase.
    inflight::Vector{DTask}
    # Which region (`region_id` at enqueue time) each inflight task belongs to,
    # so an error discovered while draining `inflight` can be attributed to the
    # region that actually queued the failing task -- not just "whatever the
    # most recently planned region happens to be", which could be a later,
    # perfectly healthy region on the same context. Entries are removed as
    # `inflight` is drained (successful or not), so this never grows past the
    # current in-flight set.
    task_region::IdDict{DTask,Int}
    pending_writeback::Set{ArgumentWrapper}
    # Marker that the flat path's object cache may hold buffers worth
    # re-examining for freeing at the next flush. Not a worklist of the
    # buffers themselves: freeing decisions are always recomputed from
    # scratch against the *current* `state` at flush time (a buffer safe to
    # free depends on every region's reads/writes up to that point, not just
    # the region that happened to record it), so keeping a separate per-buffer
    # list would either duplicate `state`'s own bookkeeping or go stale. This
    # is cheap enough to over-approximate: it is only ever consulted by
    # `maybe_drop_context!`/`issynchronized()` to answer "is there anything
    # left to flush", and cleared wholesale once a free flush completes.
    pending_free::IdDict{Any,Nothing}
    touched_spaces::Set{MemorySpace}        # unused before Phase 8
    entry_fences::Dict{MemorySpace,Any}     # unused before Phase 8

    # region id -> where `spawn_datadeps` was called, for error reporting.
    # Captured unconditionally at region entry.
    #
    # N.B. These are *raw* instruction pointers, not resolved `StackFrame`s.
    # `stacktrace(backtrace())` costs ~119us against ~4.6us for the raw
    # capture, which is not "microseconds against millisecond planning" for a
    # small region -- `@stencil` emits one region per expression per iteration
    # at ~8ms each, and resolving eagerly measured as a several-percent
    # regression across every stencil baseline. Call `stacktrace(bt)` at the
    # point of reporting instead; errors are rare and already slow. Evicted in
    # bulk once a full drain confirms every region up to `region_id` has
    # retired (see `_do_synchronize!`, step 7) -- at that point no in-flight
    # task can possibly need to attribute an error to any of them.
    region_bt::Dict{Int,Vector{Union{Ptr{Nothing},Base.InterpreterIP}}}

    # Control.
    #
    # Backpressure watermark: planning blocks (see `distribute_tasks!`) once
    # `length(inflight) > inflight_limit`, so a planner that outruns execution
    # doesn't queue up thousands of regions' worth of work under `sync=false`.
    inflight_limit::Int
    # First unreported failure and the region that queued the failing task
    # (see `task_region` above). Non-`nothing` poisons the context: further
    # planning (a new `spawn_datadeps` call) refuses to build on top of
    # possibly-inconsistent state until a drain has observed (and, in doing
    # so, cleared) it. See `_do_synchronize!` and the poisoning check in
    # `spawn_datadeps`.
    err::Union{Exception,Nothing}
    err_region::Int
    # Guards concurrent access from `synchronize_task!`/`synchronize_all!`
    # (a foreign task draining this context) racing the owner's own planning
    # or its own drain. Uncontended in the overwhelmingly common single-task
    # case, so this costs one uncontended lock/unlock per drain, not per task.
    lock::ReentrantLock
    owner::Task
    # Set once the task-exit-drain watcher has been spawned for this context,
    # so a context that outlives many regions doesn't accumulate one watcher
    # per region. See `get_context!`.
    drain_watcher_started::Bool

    function DataDepsContext(owner::Task=current_task())
        ctx = new()
        ctx.slots = SlotReuseRegion(Set{UInt}())
        ctx.memo = ChunkAinfoMemo()
        ctx.retiring_slots = SlotReuseRegion[]
        ctx.write_num = 1
        ctx.region_id = 0
        ctx.inflight = DTask[]
        ctx.task_region = IdDict{DTask,Int}()
        ctx.pending_writeback = Set{ArgumentWrapper}()
        ctx.pending_free = IdDict{Any,Nothing}()
        ctx.touched_spaces = Set{MemorySpace}()
        ctx.entry_fences = Dict{MemorySpace,Any}()
        ctx.region_bt = Dict{Int,Vector{Union{Ptr{Nothing},Base.InterpreterIP}}}()
        ctx.inflight_limit = DATADEPS_INFLIGHT_LIMIT[]
        ctx.err = nothing
        ctx.err_region = 0
        ctx.lock = ReentrantLock()
        ctx.owner = owner
        ctx.drain_watcher_started = false
        finalizer(_context_finalizer, ctx)
        return ctx
    end
end

"""
    _context_finalizer(ctx::DataDepsContext)

GC backstop for [`arm_task_exit_drain!`](@ref): if `ctx`'s owning task is
dropped without ever being waited on (so the drain watcher's `wait(owner)`
would itself never return) and `ctx` becomes unreachable with deferred work
still outstanding, this is the last chance to say so.

Finalizers run on an arbitrary thread during GC and must not block, throw,
switch tasks, or call MPI -- there is nobody left to catch an escaping
exception and a blocking wait here can stall the collector. So this only ever
takes non-blocking snapshots (`isready`, not `fetch`/`wait`) and reports via
`@error`; it can never drain `ctx` for real; only a `synchronize`-family call
or `arm_task_exit_drain!`'s watcher (both running as normal tasks) can do that.
"""
function _context_finalizer(ctx::DataDepsContext)
    try
        anything_pending = !isempty(ctx.inflight) || !isempty(ctx.pending_writeback) ||
                           !isempty(ctx.pending_free) || ctx.err !== nothing
        anything_pending || return
        unfinished = count(t -> !isready(t), ctx.inflight)
        bt = ctx.err_region != 0 ? _resolve_region_bt(ctx, ctx.err_region) : nothing
        @error "A Dagger DataDepsContext was garbage-collected with unresolved work; its owning task never called `Dagger.synchronize()` (or died without one)" region=ctx.err_region error=ctx.err unfinished_tasks=unfinished pending_writeback=length(ctx.pending_writeback) pending_free=length(ctx.pending_free) bt
    catch
        # A finalizer must never throw: there is nobody left to catch it.
    end
    return
end

"""
    DataDepsConcurrentPlanningError <: Exception

Thrown when a second Julia Task on this rank attempts to plan (`distribute_tasks!`/
`distribute_tasks_hierarchical!`) or flush (`flush_pending_writeback!`/
`flush_pending_frees!`, inside `_do_synchronize!`) a Datadeps region while
another Task on the same rank is already doing so, under `uniform_execution()`
(MPI/SPMD). See [`DATADEPS_PLANNING_TOKEN`](@ref) for why this is required and
why it errors instead of blocking.
"""
struct DataDepsConcurrentPlanningError <: Exception end
function Base.showerror(io::IO, ::DataDepsConcurrentPlanningError)
    print(io, "DataDepsConcurrentPlanningError: another Julia Task is already planning or flushing a Datadeps region on this rank under MPI/SPMD (uniform_execution()). Two Tasks doing so concurrently would allocate tags/MPIRefIDs (to_tag, next_ref_sub_id!) and issue check_uniform collectives in an order that depends on this rank's local thread scheduling -- which need not match the order on any other rank -- desyncing the SPMD program instead of merely raising here. Serialize planning across Tasks on this rank (e.g. by calling `Dagger.synchronize()` before starting the next region on another Task).")
end

"""
    DATADEPS_PLANNING_TOKEN

Process-global (i.e. per-rank, since each MPI rank is its own process) token
serializing the parts of Datadeps that allocate tags/`MPIRefID`s or issue
`check_uniform` collectives -- planning (`distribute_tasks!`/
`distribute_tasks_hierarchical!`, via [`with_datadeps_planning_token`](@ref) in
`queue.jl`) and flushing (`flush_pending_writeback!`/`flush_pending_frees!`
inside `_do_synchronize!`, via the same helper in `synchronize.jl`). These
calls are only rank-uniform if every rank performs them in the same relative
order; two Julia Tasks on one rank racing to do so (a live possibility now
that `spawn_datadeps(...; sync=false)` lets planning and flushing outlive a
single region and interleave with other Tasks' regions) would let this rank's
OS thread scheduler pick an order another rank's scheduler has no reason to
agree with -- a rank-dependent branch in spirit, just produced by scheduling
instead of by data. `uniform_execution()` is false for plain
Distributed/CPU/GPU execution, so ordinary multi-task use (independent async
pipelines on separate Tasks, per the "Multi-task" scenario) is unrestricted;
this only ever applies under MPI/SPMD.

Acquired with `trylock`, never blocking `lock`: a second Task finding the
token held gets [`DataDepsConcurrentPlanningError`](@ref) immediately, not a
hang that looks like every other MPI deadlock. See
[`with_datadeps_planning_token`](@ref).
"""
const DATADEPS_PLANNING_TOKEN = ReentrantLock()

"""
    with_datadeps_planning_token(f)

Run `f()` while holding [`DATADEPS_PLANNING_TOKEN`](@ref), but only under
`uniform_execution()` -- a no-op wrapper otherwise. Throws
[`DataDepsConcurrentPlanningError`](@ref) instead of blocking if another Task
already holds it.
"""
function with_datadeps_planning_token(f)
    uniform_execution() || return f()
    trylock(DATADEPS_PLANNING_TOKEN) || throw(DataDepsConcurrentPlanningError())
    try
        return f()
    finally
        unlock(DATADEPS_PLANNING_TOKEN)
    end
end

"""
Backpressure watermark for `DataDepsContext.inflight`: planning blocks once
this many launched tasks are outstanding, so a planner that outruns execution
under `sync=false` doesn't queue up thousands of regions' worth of work ahead
of it. Tunable; default is 4x the process's thread count, matching the same
rule-of-thumb `DATADEPS_BATCH_LIMIT` uses elsewhere in this directory.
"""
const DATADEPS_INFLIGHT_LIMIT = Ref(4 * Sys.CPU_THREADS)

"The calling task's `DataDepsContext`, or `nothing` if it hasn't created one."
const DATADEPS_CONTEXT = TaskLocalValue{Union{DataDepsContext,Nothing}}(Returns(nothing))

"""
Process-global, task-keyed registry of live `DataDepsContext`s, letting
`synchronize_task!`/`synchronize_all!` find a context given only the owning
`Task`. Weak on the task, so an entry is reaped automatically once its task
becomes unreachable, on top of the explicit removal in
[`maybe_drop_context!`](@ref) once a context drains on its own.
"""
const DATADEPS_CONTEXT_REGISTRY = LockedObject(Base.WeakKeyDict{Task,DataDepsContext}())

"Record `ctx` as `t`'s context in the process-global registry."
function register_context!(t::Task, ctx::DataDepsContext)
    lock(DATADEPS_CONTEXT_REGISTRY) do reg
        reg[t] = ctx
    end
    return ctx
end

"Remove `t`'s entry, once its context has drained (task death reaps it too)."
function deregister_context!(t::Task)
    lock(DATADEPS_CONTEXT_REGISTRY) do reg
        delete!(reg, t)
    end
    return nothing
end

"""
    context_drained(ctx::DataDepsContext) -> Bool

Whether `ctx` has nothing outstanding: no in-flight tasks, nothing deferred,
no unreported error. Callers that need a consistent snapshot against a
concurrent drain from another task should take `ctx.lock` around this.
"""
function context_drained(ctx::DataDepsContext)
    isempty(ctx.inflight) || return false
    isempty(ctx.pending_writeback) || return false
    isempty(ctx.pending_free) || return false
    ctx.err === nothing || return false
    return true
end

"""
    get_context!() -> DataDepsContext

The calling task's [`DataDepsContext`](@ref), creating and registering one on
first use.
"""
function get_context!()
    ctx = DATADEPS_CONTEXT[]
    if ctx === nothing
        t = current_task()
        ctx = DataDepsContext(t)
        DATADEPS_CONTEXT[] = ctx
        register_context!(t, ctx)
    end
    return ctx
end

"""
    maybe_arm_drain_watcher!(ddctx::DataDepsContext)

Arm the task-exit drain (`arm_task_exit_drain!`) the first time `ddctx`
survives past a `spawn_datadeps` call (i.e. it's still the calling task's
live, registered context by the time that call returns -- see the check at
the one call site, `spawn_datadeps`'s `finally`).

Deliberately *not* done unconditionally in `get_context!`: under the default
`sync=true`, a context is created and fully drained (and dropped, by
`maybe_drop_context!`) within the same `spawn_datadeps` call, over and over --
arming a watcher there would spawn one per call for the overwhelmingly common
case that never needs one, rather than once per context that actually
outlives a region.
"""
function maybe_arm_drain_watcher!(ddctx::DataDepsContext)
    ddctx.drain_watcher_started && return
    ddctx.drain_watcher_started = true
    arm_task_exit_drain!(ddctx)
    return
end

"""
    maybe_drop_context!()

Drop the calling task's context if it has fully drained (see
[`context_drained`](@ref)).

The common one-region-and-done caller (`sync=true`, the default) always ends
up here with a drained context -- `_spawn_datadeps` runs the full drain
trailing every region -- so it allocates no more than it did before this
container existed: the context it briefly held is dropped again immediately.
Under `sync=false`, a context can now outlive any single region, and this
check is what stops it from being dropped (and re-registered from scratch)
while there's still deferred work hanging off it.
"""
function maybe_drop_context!()
    ctx = DATADEPS_CONTEXT[]
    ctx === nothing && return
    (@lock ctx.lock context_drained(ctx)) || return
    DATADEPS_CONTEXT[] = nothing
    deregister_context!(ctx.owner)
    return
end

"""
    ContextQueue <: AbstractTaskQueue

The region's `:task_queue`, in place of `WaitAllQueue`. Collects every task
launched while it's ambient into `ctx.inflight`: the user's own
`Dagger.@spawn`s (forwarded through `DataDepsTaskQueue.upper_queue`), as well
as the copy and free tasks that `enqueue_copy_to!`/`enqueue_copy_from!`
(remainders.jl) and `flush_pending_frees!` (this file) launch *directly* via
`Dagger.@spawn`, bypassing `DataDepsTaskQueue` entirely. If those stopped being
collected here, nothing would ever wait for them -- see `WaitAllQueue`, the
queue this replaces, for the mechanism being preserved.

Also records each task's owning `region_id` into `ctx.task_region`, so a
failure discovered later (in `_do_synchronize!`'s wait loop) can be attributed
to the region that actually queued it.

Mutates `ctx.inflight`/`ctx.task_region` under `ctx.lock`, matching
`_do_synchronize!`: a foreign `synchronize_task!`/`synchronize_all!` drain
holds that same lock for the length of its drain, so a concurrent planner on
the owning task blocks behind it rather than racing its mutations of the same
fields. Uncontended in the overwhelmingly common single-task case, so this is
one uncontended lock/unlock per enqueued task, not a new bottleneck.
"""
struct ContextQueue <: AbstractTaskQueue
    upper_queue::AbstractTaskQueue
    ctx::DataDepsContext
end
function enqueue!(queue::ContextQueue, pair::DTaskPair)
    @lock queue.ctx.lock begin
        push!(queue.ctx.inflight, pair.task)
        queue.ctx.task_region[pair.task] = queue.ctx.region_id
    end
    enqueue!(queue.upper_queue, pair)
end
function enqueue!(queue::ContextQueue, pairs::Vector{DTaskPair})
    @lock queue.ctx.lock begin
        for pair in pairs
            push!(queue.ctx.inflight, pair.task)
            queue.ctx.task_region[pair.task] = queue.ctx.region_id
        end
    end
    enqueue!(queue.upper_queue, pairs)
end
