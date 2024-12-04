# DTask-level cancellation

mutable struct CancelToken
    @atomic cancelled::Bool
    @atomic graceful::Bool
    event::Base.Event
end
CancelToken() = CancelToken(false, false, Base.Event())
function cancel!(token::CancelToken; graceful::Bool=true)
    if !graceful
        @atomic token.graceful = false
    end
    @atomic token.cancelled = true
    notify(token.event)
    return
end
function is_cancelled(token::CancelToken; must_force::Bool=false)
    if token.cancelled[]
        if must_force && token.graceful[]
            # If we're only responding to forced cancellation, ignore graceful cancellations
            return false
        end
        return true
    end
    return false
end
Base.wait(token::CancelToken) = wait(token.event)
# TODO: Enable this for safety
#Serialization.serialize(io::AbstractSerializer, ::CancelToken) =
#    throw(ConcurrencyViolationError("Cannot serialize a CancelToken"))

const DTASK_CANCEL_TOKEN = TaskLocalValue{Union{CancelToken,Nothing}}(()->nothing)

function clone_cancel_token_remote(orig_token::CancelToken, wid::Integer)
    remote_token = remotecall_fetch(wid) do
        return poolset(CancelToken())
    end
    errormonitor_tracked("remote cancel_token communicator", Threads.@spawn begin
        wait(orig_token)
        @dagdebug nothing :cancel "Cancelling remote token on worker $wid"
        MemPool.access_ref(remote_token) do remote_token
            cancel!(remote_token)
        end
    end)
end

# Global-level cancellation

"""
    cancel!(task::DTask; force::Bool=false, graceful::Bool=true, halt_sch::Bool=false)

Cancels `task` at any point in its lifecycle, causing the scheduler to abandon
it.

# Keyword arguments
- `force`: If `true`, the task will be interrupted with an `InterruptException` (not recommended, this is unsafe).
- `graceful`: If `true`, the task will be allowed to finish its current execution before being cancelled; otherwise, it will be cancelled as soon as possible.
- `halt_sch`: If `true`, the scheduler will be halted after the task is cancelled (it will restart automatically upon the next `@spawn`/`spawn` call).

As an example, the following code will cancel task `t` before it finishes
executing:

```julia
t = Dagger.@spawn sleep(1000)
# We're bored, let's cancel `t`
Dagger.cancel!(t)
```

Cancellation allows the scheduler to free up execution resources for other
tasks which are waiting to run. Using `cancel!` is generally a much safer
alternative to Ctrl+C, as it cooperates with the scheduler and runtime and
avoids unintended side effects.
"""
function cancel!(task::DTask; force::Bool=false, graceful::Bool=true, halt_sch::Bool=false)
    tid = lock(Dagger.Sch.EAGER_ID_MAP) do id_map
        id_map[task.uid]
    end
    cancel!(tid; force, graceful, halt_sch)
end
function cancel!(tid::Union{Int,Nothing}=nothing;
                 force::Bool=false, graceful::Bool=true, halt_sch::Bool=false)
    remotecall_fetch(1, tid, force, halt_sch) do tid, force, halt_sch
        state = Sch.EAGER_STATE[]

        # Check that the scheduler isn't stopping or has already stopped
        if !isnothing(state) && !state.halt.set
            @lock state.lock _cancel!(state, tid, force, graceful, halt_sch)
        end
    end
end
function _cancel!(state, tid, force, graceful, halt_sch)
    @assert islocked(state.lock)

    # Get the scheduler uid
    sch_uid = state.uid

    # Cancel ready tasks
    for task in state.ready
        tid !== nothing && task.id != tid && continue
        @dagdebug tid :cancel "Cancelling ready task"
        state.cache[task] = DTaskFailedException(task, task, InterruptException())
        state.errored[task] = true
        Sch.set_failed!(state, task)
    end
    empty!(state.ready)

    # Cancel waiting tasks
    for task in keys(state.waiting)
        tid !== nothing && task.id != tid && continue
        @dagdebug tid :cancel "Cancelling waiting task"
        state.cache[task] = DTaskFailedException(task, task, InterruptException())
        state.errored[task] = true
        Sch.set_failed!(state, task)
    end
    empty!(state.waiting)

    # Cancel running tasks at the processor level
    wids = unique(map(root_worker_id, values(state.running_on)))
    for wid in wids
        remotecall_fetch(wid, tid, sch_uid, force) do _tid, sch_uid, force
            Dagger.Sch.proc_states(sch_uid) do states
                for (proc, state) in states
                    istate = state.state
                    any_cancelled = false
                    @lock istate.queue begin
                        for (tid, task) in istate.tasks
                            _tid !== nothing && tid != _tid && continue
                            task_spec = istate.task_specs[tid]
                            Tf = task_spec[6]
                            Tf === typeof(Sch.eager_thunk) && continue
                            istaskdone(task) && continue
                            any_cancelled = true
                            if force
                                @dagdebug tid :cancel "Interrupting running task ($Tf)"
                                Threads.@spawn Base.throwto(task, InterruptException())
                            else
                                @dagdebug tid :cancel "Cancelling running task ($Tf)"
                                # Tell the processor to just drop this task
                                task_occupancy = task_spec[4]
                                time_util = task_spec[2]
                                istate.proc_occupancy[] -= task_occupancy
                                istate.time_pressure[] -= time_util
                                push!(istate.cancelled, tid)
                                to_proc = istate.proc
                                put!(istate.return_queue, (myid(), to_proc, tid, (InterruptException(), nothing)))
                                cancel!(istate.cancel_tokens[tid]; graceful)
                            end
                        end
                    end
                    if any_cancelled
                        notify(istate.reschedule)
                    end
                end
            end
            return
        end
    end

    if halt_sch
        unlock(state.lock)
        try
            # Give tasks a moment to be processed
            sleep(0.5)

            # Halt the scheduler
            @dagdebug nothing :cancel "Halting the scheduler"
            notify(state.halt)
            put!(state.chan, (1, nothing, nothing, (Sch.SchedulerHaltedException(), nothing)))

            # Wait for the scheduler to halt
            @dagdebug nothing :cancel "Waiting for scheduler to halt"
            while Sch.EAGER_INIT[]
                sleep(0.1)
            end
            @dagdebug nothing :cancel "Scheduler halted"
        finally
            lock(state.lock)
        end
    end

    return
end
