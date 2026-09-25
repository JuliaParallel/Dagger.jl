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

    # Compile the metrics cache's trim, which first runs when a process has
    # recorded more tasks than the cache's bound: one specialization per
    # metric storage, ~0.5 s in all. Left to run time, that lands in the
    # middle of a workload while `handle_result!` holds the scheduler lock --
    # and under MPI every rank pays it at a different task, so a job stalls
    # once per rank (measured: MPI Cholesky at 256² tiles on 4 nodes ran its
    # second through fourth calls at 1.6-2.4 s against 0.9 s afterwards).
    let cache = MT.MetricsCache(), old_bound = metrics_cache_max_tasks!(8)
        sig = Sch.signature(+, [Argument(1, 1), Argument(2, 1)])
        space = memory_space(1)
        m = TaskMetrics(UInt64(1), UInt64(1), Base.GC_Diff(Base.gc_num(), Base.gc_num()),
                        UInt64(1), UInt64(1), UInt64(1), space, space, UInt64(1))
        for k in 1:32
            apply_task_metrics!(cache, k, m, sig, ThreadProc(1, 1), 1)
        end
        metrics_cache_max_tasks!(old_bound)
        empty!(global_cost_summary())
    end

    # Clean up refs
    t1 = nothing; t2 = nothing
    state = Sch.EAGER_STATE[]
    for i in 1:5
        lock(state.thunk_dict) do d; length(d); end == 1 && break
        GC.gc()
        yield()
    end

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
    # Clear the precompile-time UUID cache so it is not baked into the compiled
    # image; __init__ re-populates it from the shared UUID file at load time.
    delete!(SYSTEM_UUIDS, myid())
end
