# Stops the eager scheduler that a precompile workload started and removes its global state, so the
# precompilation process can exit. Used by this package's workload and by extensions that run their own.
# Callers must have dropped their references to the tasks they spawned.
function shutdown_precompile_workload!()
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

    # A small Datadeps region over chunks (the dependencies come from In/InOut/Out): the first region of a
    # process otherwise compiles the planner and the aliasing analysis, seconds on a laptop (it copies nothing:
    # both chunks and both tasks are in this process's one memory space; see below for the copy path)
    c1 = tochunk(zeros(4, 4))
    c2 = tochunk(zeros(4, 4))
    spawn_datadeps() do
        spawn(copyto!, InOut(c1), In(c2))
        spawn(copyto!, InOut(c2), In(c1))
    end
    fetch(c1); fetch(c2)
    c1 = nothing; c2 = nothing

    # The region above runs in one memory space, so it never copies, and the first region that does
    # compiles the copy path at run time. Run the parts of that path that need no second process, with
    # the types a copy of a `Matrix{Float64}` (or of a view of one) uses: the gather/scatter kernels of a
    # remainder copy, the whole-array `move!`, and the remainder planning on the spans of two and three
    # memory spaces. Slot creation and the copy tasks' remote calls still compile on first use.
    let A = zeros(8, 8), B = ones(8, 8), space = CPURAMMemorySpace(myid())
        V = view(A, 3:6, 3:6)
        W = view(B, 3:6, 3:6)
        GC.@preserve A B begin
            src = LocalMemorySpan.(memory_spans(aliasing(W)))
            dst = LocalMemorySpan.(memory_spans(aliasing(V)))
            spans = Tuple{LocalMemorySpan,LocalMemorySpan}[(s, d) for (s, d) in zip(src, dst)]
            buf = Vector{UInt8}(undef, sum(t -> span_len(t[1]), spans))
            for from in (B, W), to in (A, V)
                multi_span_gather!(buf, from, Iterators.map(first, spans))
                multi_span_scatter!(to, buf, Iterators.map(last, spans))
            end
        end
        move!(identity, space, space, A, B)
        move!(identity, space, space, V, W)
        for N in (2, 3)
            many(i) = ManyMemorySpan{N}(ntuple(k -> LocalMemorySpan(UInt(0x100000 * k + 64 * i), UInt(32)), N))
            tree = IntervalTree{ManyMemorySpan{N}}([many(i) for i in 1:8])
            others = [many(i) for i in 2:2:8]
            tracker = Tuple{LocalMemorySpan,LocalMemorySpan}[]
            schedule_remainder!(tracker, 1, N, tree, others)
            subtract_spans!(IntervalTree{ManyMemorySpan{N}}([many(i) for i in 1:8]), others)
        end
    end

    # Clean up refs
    t1 = nothing; t2 = nothing
    shutdown_precompile_workload!()
end
