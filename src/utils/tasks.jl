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
