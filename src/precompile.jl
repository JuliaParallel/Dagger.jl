@compile_workload begin
    system_uuid()
    add_processor_callback!("__cpu_thread_1__") do
        ThreadProc(1, 1)
    end
    Sch.__init__()

    # Initialize the scheduler, giving it 10 seconds to start
    t = Threads.@spawn Sch.init_eager()
    wait_ctr = 10
    while !istaskdone(t)
        sleep(1)
        wait_ctr -= 1
        wait_ctr == 0 && break
    end
    if !istaskdone(t)
        throw(ConcurrencyViolationError("Scheduler failed to start"))
    elseif istaskfailed(t)
        # Rethrow the error
        wait(t)
    end

    # FIXME: t1 = @spawn 1+1
    t1 = spawn(+, 1, 1)
    fetch(t1)
    t2 = spawn(+, 1, t1)
    fetch(t2)

    # Clean up refs
    t1 = nothing; t2 = nothing
    state = Sch.EAGER_STATE[]
    for i in 1:5
        length(state.thunk_dict) == 1 && break
        GC.gc()
        yield()
    end
    @assert length(state.thunk_dict) == 1

    # Halt scheduler
    notify(state.halt)
    put!(state.chan, (1, nothing, nothing, true, Sch.SchedulerHaltedException(), nothing))
    state = nothing

    # Wait for halt
    while Sch.EAGER_INIT[]
        sleep(0.5)
    end

    # Final clean-up
    Sch.EAGER_CONTEXT[] = nothing
    GC.gc(); yield()
    lock(Sch.ERRORMONITOR_TRACKED) do tracked
        if all(t->istaskdone(t) || istaskfailed(t), map(last, tracked))
            empty!(tracked)
            return
        end
        for (name, t) in tracked
            if t.state == :runnable
                @warn "Waiting on $name"
                Base.throwto(t, InterruptException())
            end
        end
    end
    MemPool.exit_hook()
    GC.gc()
    yield()
    @assert isempty(Sch.WORKER_MONITOR_CHANS)
    @assert isempty(Sch.WORKER_MONITOR_TASKS)
end
