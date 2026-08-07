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

    # Exercise the tape planner and default cost models while enabled, so the
    # first user-facing enabled allocation does not pay a cold-compile spike.
    # Use `:lexical` here: `:backtrace` site IDs are session-local and the
    # precompile process's stack is not a useful key to bake into the image.
    # Avoid allocating real DArrays here — that would spawn tasks the cleanup
    # below is not sized to drain.
    let old_site_id = Tapes.CONFIG.site_id
        # `calibrate=false`: do not bake build-host FLOP/bandwidth rates into
        # the image; runtime `enable!` will measure on first use.
        Tapes.enable!(site_id=:lexical, calibrate=false)
        try
            Tapes.backtrace_hash()
            Tapes.explain(devnull, Float64, (128, 128))
            Tapes.plan_allocation(Float64, (64, 64); requested = AutoBlocks())
            Tapes.resolve_partitioning(Float64, (64, 64), AutoBlocks(), :arbitrary)
        finally
            Tapes.disable!()
            Tapes.clear!()
            Tapes.CONFIG.site_id = old_site_id
        end
    end

    # Clean up refs
    t1 = nothing; t2 = nothing
    state = Sch.EAGER_STATE[]
    for i in 1:5
        lock(state.thunk_dict) do d; length(d); end == 1 && break
        GC.gc()
        yield()
    end
    if lock(state.thunk_dict) do d; length(d); end > 1
        @warn "Precompile failed to clean up all tasks"
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
    # Clear the precompile-time UUID cache so it is not baked into the compiled
    # image; __init__ re-populates it from the shared UUID file at load time.
    delete!(SYSTEM_UUIDS, myid())
end
