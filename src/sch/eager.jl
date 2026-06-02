const EAGER_INIT = Threads.Atomic{Bool}(false)
# Condition variable used to synchronize EAGER_STATE changes.
# Waiters must hold this lock, check EAGER_STATE[], and wait in a loop.
const EAGER_STATE_LOCK = Threads.Condition()
const EAGER_ID_MAP = LockedObject(Dict{UInt64,Int}())
const EAGER_CONTEXT = Ref{Union{Context,Nothing}}(nothing)
const EAGER_STATE = Ref{Union{ComputeState,Nothing}}(nothing)

function eager_context()
    if EAGER_CONTEXT[] === nothing
        EAGER_CONTEXT[] = Context(procs())
    end
    return EAGER_CONTEXT[]
end

function init_eager()
    if myid() != 1
        throw(ConcurrencyViolationError("init_eager can only be called on worker 1"))
    end
    if Threads.atomic_xchg!(EAGER_INIT, true)
        # Secondary path: another caller is initializing or the scheduler is already running.
        # Wait (under the condition lock) for EAGER_STATE to become non-nothing (ready) or
        # for EAGER_INIT to become false (scheduler exited without becoming ready).
        @lock EAGER_STATE_LOCK begin
            while EAGER_STATE[] === nothing && EAGER_INIT[]
                wait(EAGER_STATE_LOCK)
            end
        end
        if EAGER_STATE[] === nothing
            throw(ConcurrencyViolationError("Eager scheduler failed to start"))
        end
        return
    end

    # Primary path: we won the CAS, so we're responsible for starting the scheduler.
    ctx = eager_context()
    # N.B. We use @async here to prevent the scheduler task from running on a
    # different thread than the one that is likely submitting work, as otherwise
    # the scheduler task might sleep while holding the scheduler lock and
    # prevent work submission until it wakes up. Further testing is needed.
    errormonitor_tracked("eager compute()", @async try
        sopts = SchedulerOptions(;allow_errors=true)
        opts = Dagger.Options((;scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)),
                                occupancy=Dict(Dagger.ThreadProc=>0),
                                time_util=Dict(Dagger.ThreadProc=>0)))
        Dagger.compute(ctx, Dagger._delayed(eager_thunk, opts)();
                       options=sopts)
    catch err
        # Scheduler halting is considered normal
        err isa SchedulerHaltedException && return

        iob = IOContext(IOBuffer(), :color=>true)
        println(iob, "Error in eager scheduler:")
        Base.showerror(iob, err)
        Base.show_backtrace(iob, catch_backtrace())
        println(iob)
        seek(iob.io, 0)
        write(stderr, iob)
    finally
        # Clear EAGER_INIT and EAGER_STATE together under the condition lock.
        # Doing both atomically under the lock prevents a race where a new
        # scheduler sets EAGER_STATE between our atomic_xchg! and our lock
        # acquisition: the new scheduler also needs the lock to set EAGER_STATE,
        # so it is forced to wait until after our cleanup, guaranteeing that
        # our EAGER_STATE=nothing write cannot overwrite the new state.
        @lock EAGER_STATE_LOCK begin
            Threads.atomic_xchg!(EAGER_INIT, false)
            EAGER_STATE[] = nothing
            notify(EAGER_STATE_LOCK; all=true)
        end
        lock(EAGER_ID_MAP) do id_map
            empty!(id_map)
        end
    end)

    # Wait for eager_thunk to set EAGER_STATE[].
    # Loop to handle spurious wakeups and wakeups from old-scheduler cleanup.
    @lock EAGER_STATE_LOCK begin
        while EAGER_STATE[] === nothing && EAGER_INIT[]
            wait(EAGER_STATE_LOCK)
        end
    end
    if EAGER_STATE[] === nothing
        throw(ConcurrencyViolationError("Eager scheduler failed to start"))
    end
end

function eager_thunk()
    exec!(Dagger.sch_handle()) do ctx, state, task, tid, _
        # Set EAGER_STATE and notify all waiters under the condition lock so that
        # init_eager's primary wait loop sees the new state atomically.
        @lock EAGER_STATE_LOCK begin
            EAGER_STATE[] = state
            notify(EAGER_STATE_LOCK; all=true)
        end
        return
    end
    wait(Dagger.Sch.EAGER_STATE[].halt)
end

"""
Allows a thunk to safely wait on another thunk by temporarily reducing its
effective occupancy to 0, which allows a newly-spawned task to run.
"""
function thunk_yield(f)
    if Dagger.in_task()
        h = sch_handle()
        tls = Dagger.get_tls()
        proc = Dagger.task_processor()
        proc_istate = proc_state(tls.sch_uid, proc).state
        task_occupancy = tls.task_spec.est_occupancy

        # Decrease our occupancy and inform the processor to reschedule
        lock(proc_istate.queue) do _
            proc_istate.proc_occupancy[] -= task_occupancy
            @assert 0 <= proc_istate.proc_occupancy[] <= typemax(UInt32)
        end
        notify(proc_istate.reschedule)
        try
            # Run the yielding code
            return f()
        finally
            # Wait for processor to have occupancy to run this task
            while true
                ready = lock(proc_istate.queue) do _
                    @assert 0 <= proc_istate.proc_occupancy[] <= typemax(UInt32)
                    if proc_has_occupancy(proc_istate.proc_occupancy[], task_occupancy)
                        proc_istate.proc_occupancy[] += task_occupancy
                        @assert 0 <= proc_istate.proc_occupancy[] <= typemax(UInt32)
                        return true
                    end
                    return false
                end
                ready && break
                yield()
            end
        end
    else
        return f()
    end
end

function _find_thunk(e::Dagger.DTask)
    tid = lock(EAGER_ID_MAP) do id_map
        id_map[e.uid]
    end
    lock(EAGER_STATE[].lock) do
        unwrap_weak_checked(EAGER_STATE[].thunk_dict[tid])
    end
end
Dagger.task_id(t::Dagger.DTask) = lock(EAGER_ID_MAP) do id_map
    id_map[t.uid]
end
