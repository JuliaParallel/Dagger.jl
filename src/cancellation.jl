# DTask-level cancellation

mutable struct CancelToken
    @atomic cancelled::Bool
    @atomic graceful::Bool
    event::Base.Event
end
# N.B. `graceful` starts `true`: an as-yet-uncancelled token is considered
# graceful, and `cancel!(; graceful=true)` (the default, e.g. the cleanup
# cancellation issued when a task completes normally) leaves it that way. Only
# an explicit `cancel!(; graceful=false)` flips it to a forced cancellation.
# If this started `false`, every cancellation (including normal task-completion
# cleanup) would be seen as forced by `is_cancelled(; must_force=true)`,
# interrupting streaming drain threads mid-flight and dropping buffered values.
CancelToken() = CancelToken(false, true, Base.Event())
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

        # The eager scheduler may still be starting up, in which case
        # `EAGER_STATE` has not been published yet even though a scheduler is
        # (about to be) running. This is common under multithreading or high
        # load, where scheduler startup can take longer than the caller's
        # timeout. If we returned here we would silently drop the cancellation
        # request, leaving the to-be-cancelled task running forever (and any
        # `fetch` on it hanging). Instead, wait for the scheduler to finish
        # initializing (bounded by `EAGER_INIT`, the same condition that
        # `init_eager` waits on) so the cancellation is reliably delivered.
        if isnothing(state) && Sch.EAGER_INIT[]
            state = @lock Sch.EAGER_STATE_LOCK begin
                while Sch.EAGER_STATE[] === nothing && Sch.EAGER_INIT[]
                    wait(Sch.EAGER_STATE_LOCK)
                end
                Sch.EAGER_STATE[]
            end
        end

        # Check that the scheduler isn't stopping or has already stopped
        if !isnothing(state) && !state.halt.set
            @lock state.lock _cancel!(state, tid, force, graceful, halt_sch)
        end
    end
end
# Put a signal onto the scheduler's channel, tolerating the case where the
# scheduler has already exited and closed the channel.
function _put_sched_signal!(chan, signal)
    try
        put!(chan, signal)
    catch err
        err isa InvalidStateException && !isopen(chan) && return
        rethrow()
    end
    return
end

function _cancel!(state, tid, force, graceful, halt_sch)
    @assert islocked(state.lock)

    # Get the scheduler uid
    sch_uid = state.uid

    # Cancel ready tasks
    for task in state.ready
        tid !== nothing && task.id != tid && continue
        @dagdebug tid :cancel "Cancelling ready task"
        ex = DTaskFailedException(task, task, InterruptException())
        Sch.set_failed!(state, task; ex)
    end
    if tid === nothing
        empty!(state.ready)
    else
        idx = findfirst(t->t.id == tid, state.ready)
        idx !== nothing && deleteat!(state.ready, idx)
    end

    # Cancel waiting tasks
    for task in keys(state.waiting)
        tid !== nothing && task.id != tid && continue
        @dagdebug tid :cancel "Cancelling waiting task"
        ex = DTaskFailedException(task, task, InterruptException())
        Sch.set_failed!(state, task; ex)
    end
    if tid === nothing
        empty!(state.waiting)
    else
        if haskey(state.waiting, tid)
            delete!(state.waiting, tid)
        end
    end

    # Cancel running tasks at the processor level
    wids = unique(map(root_worker_id, values(state.running_on)))
    for wid in wids
        remotecall_fetch(wid, tid, sch_uid, force) do _tid, sch_uid, force
            states = Dagger.Sch.proc_states(sch_uid)
            MemPool.lock_read(states.lock) do
                for (proc, state) in states.dict
                    istate = state.state
                    any_cancelled = false
                    @lock istate.queue begin
                        for (tid, task) in istate.tasks
                            _tid !== nothing && tid != _tid && continue
                            task_spec = istate.task_specs[tid]
                            Tf = task_spec.Tf
                            Tf === typeof(Sch.eager_thunk) && continue
                            istaskdone(task) && continue
                            any_cancelled = true
                            if force
                                @dagdebug tid :cancel "Interrupting running task ($Tf)"
                                # Guard against the task finishing before the
                                # deferred throwto runs: throwing into an exited
                                # task is a fatal "attempt to switch to exited
                                # task" error.
                                Threads.@spawn istaskdone(task) || Base.throwto(task, InterruptException())
                            else
                                # Skip if already cancelled to avoid duplicate results in the scheduler queue
                                tid in istate.cancelled && continue
                                @dagdebug tid :cancel "Cancelling running task ($Tf)"
                                # Tell the processor to just drop this task
                                task_occupancy = task_spec.est_occupancy
                                time_util = task_spec.est_time_util
                                istate.proc_occupancy[] -= task_occupancy
                                istate.time_pressure[] -= time_util
                                push!(istate.cancelled, tid)
                                to_proc = istate.proc
                                put!(istate.return_queue, Sch.TaskResult(myid(), to_proc, tid, InterruptException(), nothing))
                                cancel!(istate.cancel_tokens[tid]; graceful)
                            end
                        end
                        # Also cancel tokens for tasks that have been dequeued but not yet
                        # recorded in istate.tasks (race window between token assignment and
                        # task registration). Post a pre-emptive result so the scheduler
                        # sees the cancellation immediately (critical when halt_sch=true,
                        # otherwise the scheduler may exit before DoTaskSpec posts its result).
                        if !force
                            for (tid, token) in istate.cancel_tokens
                                _tid !== nothing && tid != _tid && continue
                                haskey(istate.tasks, tid) && continue  # already handled above
                                tid in istate.cancelled && continue
                                @dagdebug tid :cancel "Cancelling pre-running task token"
                                any_cancelled = true
                                push!(istate.cancelled, tid)
                                to_proc = istate.proc
                                put!(istate.return_queue, Sch.TaskResult(myid(), to_proc, tid, InterruptException(), nothing))
                                cancel!(token; graceful)
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
    # The scheduler may already be exiting (and have closed its channel) by the
    # time we get here, so tolerate a closed channel when signaling it.
    _put_sched_signal!(state.chan, Sch.RescheduleSignal())

    if halt_sch
        unlock(state.lock)
        try
            # Wait (bounded) for the cancellation results to actually be
            # processed by the scheduler before halting it.
            #
            # Cancellation of a *running* task is asynchronous: we post an
            # `InterruptException` result to the processor's return queue and
            # rely on the scheduler loop to pick it up, store the result, and
            # finish the task. If we halt before that happens, `scheduler_exit`
            # resolves the still-pending futures with a generic
            # `SchedulingException` instead, so callers `fetch`ing a cancelled
            # task would observe the wrong exception.
            #
            # We wait until the running tasks have drained (their results
            # stored) rather than waiting on `state.futures`: the caller's
            # `fetch` may not have registered its future yet when we get here,
            # but once a task's result is stored, a later `fetch` picks it up
            # immediately (see `_register_future!`). We cap the wait so a task
            # that refuses to cancel cannot block the halt indefinitely.
            deadline = time() + 5.0
            while time() < deadline
                drained = @lock state.lock begin
                    isempty(state.running_on) && isempty(state.futures)
                end
                drained && break
                sleep(0.05)
            end

            # Halt the scheduler. Mark the halt as cancellation-induced so that
            # teardown resolves any futures we didn't manage to resolve above
            # with an `InterruptException` rather than a `SchedulingException`.
            @dagdebug nothing :cancel "Halting the scheduler"
            state.halt_cancelled[] = true
            notify(state.halt)
            _put_sched_signal!(state.chan, Sch.TaskResult(1, OSProc(), 0, Sch.SchedulerHaltedException(), nothing))

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
