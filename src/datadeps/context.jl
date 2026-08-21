### DataDepsContext: task-local state shared across (eventually async) regions ###
#
# `spawn_datadeps` is currently a full barrier: `DataDepsState` is built fresh
# per region (`distribute_tasks!`, queue.jl) and dropped when the region ends,
# and the region epilogue restores the world (write everything back to origin,
# free every slot, wait for it all) precisely *because* nothing survives to
# the next region. `DataDepsContext` is the container that will eventually
# hold that state across region boundaries, letting a later region's planning
# consult an earlier region's `ainfos_owner`/`ainfos_readers`/`arg_current`
# instead of starting over.
#
# This file introduces the container only. Every region-scoped object it
# holds (`state`, `slots`, `memo`) is still rebuilt fresh at the start of
# every region -- see each field's comment below for why a longer lifetime
# isn't safe yet -- and `_spawn_datadeps` still waits for every launched task
# and runs the full write-back-and-free epilogue before returning, exactly as
# today. The one genuinely cross-region field in this phase is `write_num`,
# which becomes a monotonic, never-reset counter (see the N.B. in
# `distribute_tasks!`, queue.jl). Everything else exists so that later phases
# -- which stop doing the per-region resets -- are a smaller diff.

"""
    DataDepsContext

Per-Julia-task home for Datadeps planning state, spanning (eventually) every
`spawn_datadeps` region run by `owner`. See the top of this file for why this
exists and what does/doesn't change in this phase.

Reached via [`get_context!`](@ref) (creates on first use) and torn down by
[`maybe_drop_context!`](@ref) (once drained). Never construct one directly.
"""
mutable struct DataDepsContext
    # Carried across regions starting in a later phase. Left `#undef` by the
    # constructor: `distribute_tasks!` assigns a fresh one at the start of
    # every flat-path region (matching today's local `state = DataDepsState()`
    # exactly), and nothing reads this field before that first assignment.
    # Constructing one eagerly here would cost a `tochunk` on *every* region
    # regardless of path, including hierarchical ones that never consult this
    # field at all -- and hierarchical is the default, most-benchmarked path.
    state::DataDepsState

    # Rebuilt fresh every region (both flat and hierarchical), same as today.
    # `SlotReuseRegion`'s and `ChunkAinfoMemo`'s own docstrings explain why a
    # longer lifetime isn't yet safe: reuse eligibility and chunk-address
    # memoization are each only valid for the region that computed them.
    slots::SlotReuseRegion
    memo::ChunkAinfoMemo

    # Monotonic across the owner's entire lifetime, never reset per region.
    # See the N.B. in `distribute_tasks!` (queue.jl).
    write_num::Int
    region_id::Int

    # Outstanding work. Always empty at rest in this phase -- `_spawn_datadeps`
    # drains `inflight` fully, and nothing ever populates the other three,
    # before returning, so nothing here survives past one region. Populated
    # for real starting in Phase 4.
    inflight::Vector{DTask}
    pending_writeback::Set{ArgumentWrapper} # unused before Phase 4
    pending_free::IdDict{Any,Nothing}       # unused before Phase 4
    touched_spaces::Set{MemorySpace}        # unused before Phase 8
    entry_fences::Dict{MemorySpace,Any}     # unused before Phase 8

    # region id -> where `spawn_datadeps` was called, for Phase 4's error
    # reporting. Captured unconditionally at region entry.
    #
    # N.B. These are *raw* instruction pointers, not resolved `StackFrame`s.
    # `stacktrace(backtrace())` costs ~119us against ~4.6us for the raw
    # capture, which is not "microseconds against millisecond planning" for a
    # small region -- `@stencil` emits one region per expression per iteration
    # at ~8ms each, and resolving eagerly measured as a several-percent
    # regression across every stencil baseline. Call `stacktrace(bt)` at the
    # point of reporting instead; errors are rare and already slow.
    region_bt::Dict{Int,Vector{Union{Ptr{Nothing},Base.InterpreterIP}}}

    # Control -- unused before Phase 4.
    inflight_limit::Int
    err::Union{Exception,Nothing}
    err_region::Int
    lock::ReentrantLock
    owner::Task

    function DataDepsContext(owner::Task=current_task())
        ctx = new()
        ctx.slots = SlotReuseRegion(Set{UInt}())
        ctx.memo = ChunkAinfoMemo()
        ctx.write_num = 1
        ctx.region_id = 0
        ctx.inflight = DTask[]
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
        return ctx
    end
end

"""
Backpressure watermark for `DataDepsContext.inflight` (Phase 4): planning
blocks once this many launched tasks are outstanding, so a planner that
outruns execution doesn't queue up thousands of regions' worth of work. Unused
before Phase 4.
"""
const DATADEPS_INFLIGHT_LIMIT = Ref(4 * Sys.CPU_THREADS)

"The calling task's `DataDepsContext`, or `nothing` if it hasn't created one."
const DATADEPS_CONTEXT = TaskLocalValue{Union{DataDepsContext,Nothing}}(Returns(nothing))

"""
Process-global, task-keyed registry of live `DataDepsContext`s, letting Phase
4's `synchronize_task!`/`synchronize_all!` find a context given only the
owning `Task`. Weak on the task, so an entry is reaped automatically once its
task becomes unreachable, on top of the explicit removal in
[`maybe_drop_context!`](@ref) once a context drains on its own. Nothing
consults this registry yet.
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
    maybe_drop_context!()

Drop the calling task's context if it has fully drained: no outstanding
tasks, nothing deferred, no unreported error.

In this phase, `_spawn_datadeps` always waits out `inflight` and runs the full
epilogue before calling this, so a context is always drained by the time this
runs -- which is the point: the common one-region-and-done caller ends up
allocating no more than it did before this container existed, since the
context it briefly held is dropped again immediately. Starting in Phase 4,
once there's real deferred state, this check starts actually mattering and a
context can outlive a single region.
"""
function maybe_drop_context!()
    ctx = DATADEPS_CONTEXT[]
    ctx === nothing && return
    isempty(ctx.inflight) || return
    isempty(ctx.pending_writeback) || return
    isempty(ctx.pending_free) || return
    ctx.err === nothing || return
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
(remainders.jl) and the free loop (`distribute_tasks!`, queue.jl) launch
*directly* via `Dagger.@spawn`, bypassing `DataDepsTaskQueue` entirely. If
those stopped being collected here, the region would stop waiting for them --
see `WaitAllQueue`, the queue this replaces, for the mechanism being
preserved.
"""
struct ContextQueue <: AbstractTaskQueue
    upper_queue::AbstractTaskQueue
    ctx::DataDepsContext
end
function enqueue!(queue::ContextQueue, pair::DTaskPair)
    push!(queue.ctx.inflight, pair.task)
    enqueue!(queue.upper_queue, pair)
end
function enqueue!(queue::ContextQueue, pairs::Vector{DTaskPair})
    for pair in pairs
        push!(queue.ctx.inflight, pair.task)
    end
    enqueue!(queue.upper_queue, pairs)
end
