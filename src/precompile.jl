@compile_workload begin
    system_uuid()
    add_processor_callback!("__cpu_thread_1__") do
        ThreadProc(1, 1)
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
    @assert length(state.thunk_dict) == 1 "Should have only 1 task running, but have $(length(state.thunk_dict)) tasks: $(collect(keys(state.thunk_dict)))"

    # Halt scheduler
    notify(state.halt)
    put!(state.chan, Sch.TaskResult(1, OSProc(), 0, Sch.SchedulerHaltedException(), nothing))
    state = nothing

    # Wait for halt
    while Sch.EAGER_INIT[]
        sleep(0.5)
    end

    # Final clean-up
    Sch.EAGER_CONTEXT[] = nothing
    GC.gc(); sleep(0.5)
    lock(Sch.ERRORMONITOR_TRACKED) do tracked
        if all(t->istaskdone(t) || istaskfailed(t), map(last, tracked))
            empty!(tracked)
            return
        end
        for (name, t) in tracked
            if t.state == :runnable
                @warn "Waiting on $name"
                Threads.@spawn Base.throwto(t, InterruptException())
            end
        end
    end
    MemPool.exit_hook()
    GC.gc()
    yield()
    @assert isempty(Sch.WORKER_MONITOR_CHANS)
    @assert isempty(Sch.WORKER_MONITOR_TASKS)
    ID_COUNTER[] = 1
end
