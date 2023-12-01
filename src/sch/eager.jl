const EAGER_INIT = Threads.Atomic{Bool}(false)
const EAGER_READY = Base.Event()
const EAGER_CONTEXT = Ref{Union{Context,Nothing}}(nothing)
const EAGER_STATE = Ref{Union{ComputeState,Nothing}}(nothing)

function eager_context()
    if EAGER_CONTEXT[] === nothing
        EAGER_CONTEXT[] = Context([procs()...])
    end
    return EAGER_CONTEXT[]
end

function init_eager()
    if Threads.atomic_xchg!(EAGER_INIT, true)
        wait(EAGER_READY)
        return
    end
    ctx = eager_context()
    errormonitor_tracked("eager compute()", Threads.@spawn try
        sopts = SchedulerOptions(;allow_errors=true)
        opts = Dagger.Options((;scope=Dagger.ExactScope(Dagger.ThreadProc(myid(), 1)),
                                occupancy=Dict(Dagger.ThreadProc=>0)))
        Dagger.compute(ctx, Dagger.delayed(eager_thunk, opts)(); options=sopts)
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
        reset(EAGER_READY)
        EAGER_STATE[] = nothing
        Threads.atomic_xchg!(EAGER_INIT, false)
    end)
    wait(EAGER_READY)
end
function eager_thunk()
    exec!(Dagger.sch_handle()) do ctx, state, task, tid, _
        EAGER_STATE[] = state
        return
    end
    notify(EAGER_READY)
    wait(Dagger.Sch.EAGER_STATE[].halt)
end

"""
Allows a thunk to safely wait on another thunk by temporarily reducing its
effective occupancy to 0, which allows a newly-spawned task to run.
"""
function thunk_yield(f)
    if Dagger.in_thunk()
        h = sch_handle()
        tls = Dagger.get_tls()
        proc = Dagger.thunk_processor()
        proc_istate = proc_states(tls.sch_uid) do states
            states[proc].state
        end
        task_occupancy = tls.task_spec.est_occupancy

        # Decrease our occupancy and inform the processor to reschedule
        lock(proc_istate.queue) do _
            proc_istate.proc_occupancy[] -= task_occupancy
        end
        notify(proc_istate.reschedule)
        try
            # Run the yielding code
            return f()
        finally
            # Wait for processor to have occupancy to run this task
            while true
                ready = lock(proc_istate.queue) do _
                    if proc_has_occupancy(proc_istate.proc_occupancy[], task_occupancy)
                        proc_istate.proc_occupancy[] += task_occupancy
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

eager_cleanup(t::Dagger.EagerThunkFinalizer) =
    errormonitor_tracked("eager_cleanup $(t.id)", Threads.@spawn eager_cleanup(t.id))
function eager_cleanup(id)
    state = EAGER_STATE[]
    if state === nothing
        # We might be exiting, this is fine
        return
    end
    lock(state.lock) do
        # N.B. cache and errored expire automatically
        delete!(state.thunk_dict, id)
    end
end

function _find_thunk(t::Dagger.EagerThunk)
    lock(EAGER_STATE[].lock) do
        unwrap_weak_checked(EAGER_STATE[].thunk_dict[t.id])
    end
end
