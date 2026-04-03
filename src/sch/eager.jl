const EAGER_INIT = Threads.Atomic{Bool}(false)
const EAGER_READY = Base.Event()
const EAGER_CONTEXT = Ref{Union{Context,Nothing}}(nothing)
const EAGER_STATE = Ref{Union{ComputeState,Nothing}}(nothing)

function eager_context()
    if EAGER_CONTEXT[] === nothing
        EAGER_CONTEXT[] = Context(procs())
    end
    return EAGER_CONTEXT[]
end

function init_eager()
    if Threads.atomic_xchg!(EAGER_INIT, true)
        wait(EAGER_READY)
        if EAGER_STATE[] === nothing
            throw(ConcurrencyViolationError("Eager scheduler failed to start"))
        end
        return
    end
    ctx = eager_context()
    # N.B. We use @async here to prevent the scheduler task from running on a
    # different thread than the one that is likely submitting work, as otherwise
    # the scheduler task might sleep while holding the scheduler lock and
    # prevent work submission until it wakes up. Further testing is needed.
    errormonitor_tracked("eager compute()", @async try
        sopts = SchedulerOptions(;allow_errors=true)
        opts = Dagger.Options((;scope=Dagger.ExactScope(Dagger.ThreadProc(myid(), 1)),
                                occupancy=Dict(Dagger.ThreadProc=>0),
                                time_util=Dict(Dagger.ThreadProc=>0)))
        Dagger.compute(ctx, Dagger._delayed(eager_thunk, opts)();
                       options=sopts)
    catch err
        # Scheduler halting is considered normal
        Sch.unwrap_nested_exception(err) isa SchedulerHaltedException && return

        iob = IOContext(IOBuffer(), :color=>true)
        println(iob, "Error in eager scheduler:")
        Base.showerror(iob, err)
        Base.show_backtrace(iob, catch_backtrace())
        println(iob)
        seek(iob.io, 0)
        write(stderr, iob)
    finally
        # N.B. Sequence order matters to ensure that observers can see that we failed to start
        EAGER_STATE[] = nothing
        notify(EAGER_READY)
        reset(EAGER_READY)
        Threads.atomic_xchg!(EAGER_INIT, false)
    end)
    wait(EAGER_READY)
    if EAGER_STATE[] === nothing
        throw(ConcurrencyViolationError("Eager scheduler failed to start"))
    end
end
function eager_thunk()
    exec!(Dagger.sch_handle()) do ctx, state, task, tid, _
        EAGER_STATE[] = state
        return
    end
    notify(EAGER_READY)
    wait(Dagger.Sch.EAGER_STATE[].halt)
end

function register_completion_watcher_eager!(thunk_id::TaskID, chan::RemoteChannel)
    @assert thunk_id.worker == myid()
    init_eager()
    register_completion_watcher!(EAGER_STATE[], thunk_id, chan)
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
    tid = e.uid
    lock(EAGER_STATE[].lock) do
        unwrap_weak_checked(EAGER_STATE[].thunk_dict[tid])
    end
end
Dagger.task_id(t::Dagger.DTask) = t.uid
