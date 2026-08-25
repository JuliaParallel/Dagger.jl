"""
    set_task_migratable!(task) -> task

Allow `task` to run on any thread of the default pool.

Clearing `sticky` is not sufficient on its own. A hand-built `@task` has no
thread pool assigned, and `Base.enq_work` routes such a task onto the *current*
thread's work queue rather than the shared multi-queue, so it cannot be picked
up until the thread that scheduled it yields. `Threads.@spawn` sets both; task
pools built with `@task` must do the same or every task they spawn is pinned,
in effect, to whichever thread happened to schedule it.
"""
function set_task_migratable!(task::Task)
    task.sticky = false
    @static if isdefined(Base.Threads, :_spawn_set_thrpool)
        Base.Threads._spawn_set_thrpool(task, :default)
    end
    return task
end

"""
    clear_task_scope!(task) -> task

Drops the dynamic (`ScopedValues`) scope that `task` inherited from whichever
task happened to create it. Must be called before `task` is started.

Julia copies the creating task's scope into every new `Task`, which is right
for a task doing work on behalf of its creator, but wrong for Dagger's
long-lived and pooled tasks: those are created lazily, on whatever call first
needs them, and then serve every later caller for the rest of the session. A
task created inside e.g. `Dagger.with_options(scope=...)` would otherwise keep
observing those options forever -- and since `with_options` merges into the
ambient scoped options, that leaks into the options of every task it later
runs. Per-task option propagation is explicit (see `get_propagated_options`),
so these tasks want no ambient scope at all.

Where the scope lives depends on the Julia version. On 1.11+ it is a first-class
`Task` field (`task.scope`). On 1.10 there is no such field, so the
`ScopedValues.jl` compat package smuggles the scope through `task.logstate`
instead, wrapping the task's logger in a `ScopePayloadLogger` that carries the
scope alongside it (see `ScopedValues/src/payloadlogger.jl`); clearing
`logstate` is how you drop the scope there.

Either way the task also loses any logger it inherited and falls back to the
global one -- on 1.10 because the logger is the vehicle for the scope, and on
1.11+ because Base stores the current logger in a `ScopedValue` of its own
(`Base.CoreLogging.CURRENT_LOGSTATE`). That is deliberate: an inherited logger
is wrong on these tasks for exactly the same reason an inherited scope is -- a
pooled task would otherwise keep whichever `with_logger` block happened to
create it for the rest of the session.
"""
function clear_task_scope!(task::Task)
    @static if VERSION >= v"1.11-"
        task.scope = nothing
    else
        task.logstate = nothing
    end
    return task
end

function set_task_tid!(task::Task, tid::Integer)
    task.sticky = true
    ctr = 0
    while true
        ret = ccall(:jl_set_task_tid, Cint, (Any, Cint), task, tid-1)
        if ret == 1
            break
        elseif ret == 0
            yield()
        else
            error("Unexpected retcode from jl_set_task_tid: $ret")
        end
        ctr += 1
        if ctr > 10
            @warn "Setting task TID to $tid failed, giving up!"
            return
        end
    end
    @assert Threads.threadid(task) == tid "jl_set_task_tid failed!"
end

if isdefined(Base, :waitany)
import Base: waitany, waitall
else
# Vendored from Base
# License is MIT
waitany(tasks; throw=true) = _wait_multiple(tasks, throw)
waitall(tasks; failfast=true, throw=true) = _wait_multiple(tasks, throw, true, failfast)
function _wait_multiple(waiting_tasks, throwexc=false, all=false, failfast=false)
    tasks = Task[]

    for t in waiting_tasks
        t isa Task || error("Expected an iterator of `Task` object")
        push!(tasks, t)
    end

    if (all && !failfast) || length(tasks) <= 1
        exception = false
        # Force everything to finish synchronously for the case of waitall
        # with failfast=false
        for t in tasks
            _wait(t)
            exception |= istaskfailed(t)
        end
        if exception && throwexc
            exceptions = [TaskFailedException(t) for t in tasks if istaskfailed(t)]
            throw(CompositeException(exceptions))
        else
            return tasks, Task[]
        end
    end

    exception = false
    nremaining::Int = length(tasks)
    done_mask = falses(nremaining)
    for (i, t) in enumerate(tasks)
        if istaskdone(t)
            done_mask[i] = true
            exception |= istaskfailed(t)
            nremaining -= 1
        else
            done_mask[i] = false
        end
    end

    if nremaining == 0
        return tasks, Task[]
    elseif any(done_mask) && (!all || (failfast && exception))
        if throwexc && (!all || failfast) && exception
            exceptions = [TaskFailedException(t) for t in tasks[done_mask] if istaskfailed(t)]
            throw(CompositeException(exceptions))
        else
            return tasks[done_mask], tasks[.~done_mask]
        end
    end

    chan = Channel{Int}(Inf)
    sentinel = current_task()
    waiter_tasks = fill(sentinel, length(tasks))

    for (i, done) in enumerate(done_mask)
        done && continue
        t = tasks[i]
        if istaskdone(t)
            done_mask[i] = true
            exception |= istaskfailed(t)
            nremaining -= 1
            exception && failfast && break
        else
            waiter = @task put!(chan, i)
            waiter.sticky = false
            _wait2(t, waiter)
            waiter_tasks[i] = waiter
        end
    end

    while nremaining > 0
        i = take!(chan)
        t = tasks[i]
        waiter_tasks[i] = sentinel
        done_mask[i] = true
        exception |= istaskfailed(t)
        nremaining -= 1

        # stop early if requested, unless there is something immediately
        # ready to consume from the channel (using a race-y check)
        if (!all || (failfast && exception)) && !isready(chan)
            break
        end
    end

    close(chan)

    if nremaining == 0
        return tasks, Task[]
    else
        remaining_mask = .~done_mask
        for i in findall(remaining_mask)
            waiter = waiter_tasks[i]
            donenotify = tasks[i].donenotify::ThreadSynchronizer
            @lock donenotify Base.list_deletefirst!(donenotify.waitq, waiter)
        end
        done_tasks = tasks[done_mask]
        if throwexc && exception
            exceptions = [TaskFailedException(t) for t in done_tasks if istaskfailed(t)]
            throw(CompositeException(exceptions))
        else
            return done_tasks, tasks[remaining_mask]
        end
    end
end
end
