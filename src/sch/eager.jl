const EAGER_INIT = Threads.Atomic{Bool}(false)
const EAGER_READY = Base.Event()
const EAGER_ID_MAP = LockedObject(Dict{UInt64,Int}())
const EAGER_CONTEXT = Ref{Union{Context,Nothing}}(nothing)
const EAGER_STATE = Ref{Union{ComputeState,Nothing}}(nothing)

function eager_context()
    if EAGER_CONTEXT[] === nothing
        EAGER_CONTEXT[] = Context([myid(),workers()...])
    end
    return EAGER_CONTEXT[]
end

function init_eager()
    if myid() != 1
        throw(ConcurrencyViolationError("init_eager can only be called on worker 1"))
    end
    if Threads.atomic_xchg!(EAGER_INIT, true)
        wait(EAGER_READY)
        return
    end
    ctx = eager_context()
    errormonitor_tracked("eager compute()", Threads.@spawn try
        sopts = SchedulerOptions(;allow_errors=true)
        opts = Dagger.Options((;scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)),
                                occupancy=Dict(Dagger.ThreadProc=>0)))
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
        reset(EAGER_READY)
        EAGER_STATE[] = nothing
        lock(EAGER_ID_MAP) do id_map
            empty!(id_map)
        end
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
    if Dagger.in_task()
        h = sch_handle()
        tls = Dagger.get_tls()
        proc = Dagger.task_processor()
        proc_istate = proc_states(tls.sch_uid) do states
            states[proc].state
        end
        task_occupancy = tls.task_spec[4]

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

eager_cleanup(t::Dagger.DTaskFinalizer) =
    errormonitor_tracked("eager_cleanup $(t.uid)", Threads.@spawn eager_cleanup(EAGER_STATE[], t.uid))
function eager_cleanup(state, uid)
    tid = nothing
    lock(EAGER_ID_MAP) do id_map
        if !haskey(id_map, uid)
            return
        end
        tid = id_map[uid]
        delete!(id_map, uid)
    end
    tid === nothing && return
    lock(state.lock) do
        # N.B. cache and errored expire automatically
        delete!(state.thunk_dict, tid)
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
