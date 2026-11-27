export LightTask, @light

# Light tasks: an opt-in fast path for fine-grained shared-memory work.  A light task is one small mutable object (function, arguments,
# result, atomic count of unfinished dependencies, lock-free stack of dependents) handed to Julia's own scheduler once its dependencies are
# done.  There is no Thunk, Chunk, MemPool, scope or processor in between, so it also has none of their features: no scopes or options,
# no Datadeps, no MPI/Distributed placement (a light task runs in the process that created it), no cancellation, no logging events.
# Arguments may be values, other `LightTask`s (dependencies) or `DTask`s (dependencies; a failed DTask fails the light task).
# `light_to_dtask(l)` waits for the light task and gives a DTask of its value (the other direction).
#
# Protocol (see AGENTS.md lesson 136): `left` starts at 1 (a guard) + one per dependency, BEFORE anything is registered; a refused
# registration (dependency already done) subtracts again; the guard is dropped last.  Only the thread that brings `left` to zero launches the
# task, exactly once.

const LIGHT_DONE = :__light_done__                 # sentinel in the dependents stack: the task has finished

# Test hook.  `light_point(name)` is called wherever another thread can interleave with the protocol; the default is `nothing` (one load and a
# branch).  The tests install a function there that yields, spins or sleeps at random, to make those windows wide.
const LIGHT_HOOK = Ref{Union{Nothing,Function}}(nothing)
@inline function light_point(p::Symbol)
    h = LIGHT_HOOK[]
    h === nothing || h(p)
    return nothing
end

struct LightNode                                   # immutable stack node (CAS on the head of the stack)
    target::Any                                    # a LightTask that waits for us, or an Event that a fetch waits on
    next::Any                                      # LightNode or nothing
end

struct LightFailed
    ex::Any
end

"""
    LightTask

The handle returned by [`@light`](@ref). Supports `wait`, `fetch` and `istaskdone`.
"""
mutable struct LightTask
    f::Any
    args::Any
    @atomic result::Any                            # the value, or a LightFailed
    @atomic left::Int
    @atomic deps::Any                              # nothing | LightNode | LIGHT_DONE
    LightTask(f, args, left) = new(f, args, nothing, left, nothing)
end

@inline light_isdone(t::LightTask) = (@atomic :acquire t.deps) === LIGHT_DONE

# register `waiter` (a LightTask or an Event) to be released when `d` finishes; false if d has already finished
function light_add_dependent!(d::LightTask, waiter)
    while true
        head = @atomic :acquire d.deps
        head === LIGHT_DONE && return false
        light_point(:cas)
        _, ok = @atomicreplace :acquire_release :monotonic d.deps head => LightNode(waiter, head)
        ok && return true
    end
end

# Start `f` as an ordinary Julia task on the default pool.  Unlike `Threads.@spawn` the task does not inherit the dynamic scope (ScopedValues,
# `with_options`, logger) of the thread that happens to release it: whichever task finishes a light task's last predecessor would otherwise
# decide which options the body sees.  A light task runs with no ambient scope.
function light_start(f)
    t = Task(f)
    set_task_migratable!(t)
    clear_task_scope!(t)
    schedule(t)
    return nothing
end

light_release!(w::Base.Event) = (notify(w); nothing)
function light_release!(w::LightTask)
    if (@atomic :acquire_release w.left -= 1) == 0
        light_point(:launch)
        light_start(() -> _light_run!(w))
    end
    return nothing
end

@inline _light_resolve(x) = x
@inline _light_resolve(x::LightTask) = @atomic :acquire x.result
@inline _light_resolve(x::DTask) = fetch(x)

function _light_run!(t::LightTask)
    args = t.args
    res = nothing
    failed = false
    for a in args
        if a isa LightTask
            r = @atomic :acquire a.result
            if r isa LightFailed
                res = r; failed = true; break
            end
        end
    end
    if !failed
        res = try
            # `invokelatest`: this task was spawned by the task that finished its last predecessor and has that task's world age, so a function
            # defined after that task started would be too new to call (seen in a full Distributed run). `t.f` is not inferred anyway.
            Base.invokelatest(t.f, map(_light_resolve, args)...)
        catch e
            LightFailed(CapturedException(e, catch_backtrace()))
        end
    end
    t.f = nothing; t.args = nothing                 # the result stays, the inputs can be collected
    @atomic :release t.result = res
    light_point(:result)
    n = @atomicswap :acquire_release t.deps = LIGHT_DONE
    while n isa LightNode
        light_point(:release)
        light_release!(n.target)
        n = n.next
    end
    return nothing
end

# a DTask has no completion hook: one Julia task waits for it (failure is reported when the light task resolves its argument)
function _light_wait_dtask(d::DTask, t::LightTask)
    try
        wait(d)
    catch
    end
    light_release!(t)
    return nothing
end

"""
    light_spawn(f, args...) -> LightTask

Run `f(args...)` as a light task once all `LightTask`/`DTask` arguments are done. See [`@light`](@ref).
"""
function light_spawn(f, args...)
    k = 0
    for a in args
        (a isa LightTask || a isa DTask) && (k += 1)
    end
    t = LightTask(f, args, 1 + k)
    for a in args
        light_point(:register)
        if a isa LightTask
            light_add_dependent!(a, t) || (@atomic :acquire_release t.left -= 1)   # cannot reach 0: the guard is held
        elseif a isa DTask
            light_start(() -> _light_wait_dtask(a, t))
        end
    end
    light_point(:guard)
    light_release!(t)                               # drop the guard
    return t
end

"""
    @light f(args...)

Opt-in fast path for fine-grained shared-memory tasks, with much less per-task overhead than
[`Dagger.@spawn`](@ref). Returns a [`LightTask`](@ref) that supports `wait`, `fetch` and
`istaskdone`. Arguments that are `LightTask`s or `DTask`s are dependencies and are passed as
their values. A light task has no scopes, options, Datadeps, MPI/Distributed placement,
cancellation or logging events, and keyword arguments are not supported. It always runs in the
process that created it.
"""
macro light(ex)
    (ex isa Expr && ex.head == :call) || throw(ArgumentError("@light expects a function call, e.g. @light f(x, y)"))
    any(a -> a isa Expr && (a.head === :parameters || a.head === :kw), ex.args) &&
        throw(ArgumentError("@light does not support keyword arguments; wrap them in a closure, e.g. @light (() -> f(x; k=1))()"))
    return esc(Expr(:call, GlobalRef(Dagger, :light_spawn), ex.args...))
end

function Base.wait(t::LightTask)
    light_isdone(t) && return nothing
    ev = Base.Event()
    # Inside a Dagger task, hand the processor back while waiting (exactly what `wait(::ThunkFuture)`
    # does), so that the tasks this light task depends on can run on it. Outside a task `thunk_yield(f)` is `f()`.
    light_point(:wait)
    light_add_dependent!(t, ev) && Dagger.Sch.thunk_yield(() -> wait(ev))
    return nothing
end
function Base.fetch(t::LightTask)
    wait(t)
    r = @atomic :acquire t.result
    r isa LightFailed && throw(r.ex)
    return r
end
Base.istaskdone(t::LightTask) = light_isdone(t)

_light_rethrow(ex) = throw(ex)

"""
    light_to_dtask(l::LightTask) -> DTask

A `DTask` whose value is the value of `l` (a failure of `l` becomes a failure of the DTask).
The caller waits (cooperatively) until `l` is done before the DTask is created. A `DTask` may
also `wait` or `fetch` a light task directly: it hands its processor back while it waits.
Not usable under MPI/Distributed.
"""
function light_to_dtask(l::LightTask)
    wait(l)
    r = @atomic :acquire l.result
    r isa LightFailed && return spawn(_light_rethrow, r.ex)
    return spawn(identity, r)
end
