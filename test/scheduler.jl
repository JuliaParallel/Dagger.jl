import Dagger: Chunk, Options
import Dagger.Sch: SchedulerOptions, SchedulerHaltedException, ComputeState, ThunkID, sch_handle

@everywhere begin
using Dagger
import Dagger: sch_handle, exec!, halt!, get_dag_ids, add_thunk!
function inc(x)
    x+1
end
function metrics_probe(x)
    x + 1
end
function checkwid(x...)
    @assert myid() == 1
    return 1
end
function checktid(x...)
    return 1
end
global pressure = Ref{Int}(0)
function checkpressure(x...)
    global pressure
    pressure[] += 1
    sleep(1)
    @assert pressure[] <= 4
    pressure[] -= 1
end
function dynamic_exec(x)
    h = sch_handle()
    Dagger.Sch.exec!(h) do ctx, state, task, tid, _
        if state isa ComputeState
            return 1
        else
            return 0
        end
    end
end
function dynamic_exec_err(x)
    h = sch_handle()
    Dagger.Sch.exec!(h) do ctx, state, _, _, _
        error("An error")
    end
end
function dynamic_halt(x)
    h = sch_handle()
    Dagger.Sch.halt!(h)
    return x
end
function dynamic_get_dag(x...)
    h = sch_handle()
    ids = Dagger.Sch.get_dag_ids(h)
    return ids
end
function dynamic_add_thunk(x)
    h = sch_handle()
    id = Dagger.Sch.add_thunk!(h, nothing=>x) do y
        y+1
    end
    wait(h, id)
    return fetch(h, id)
end
function dynamic_add_thunk_self_dominated(x)
    h = sch_handle()
    id = Dagger.Sch.add_thunk!(h, nothing=>h.thunk_id, nothing=>x) do y
        y+1
    end
    return fetch(h, id)
end
function dynamic_wait_fetch_multiple(x)
    h = sch_handle()
    ids = Dagger.Sch.get_dag_ids(h)
    id = nothing
    for key in keys(ids)
        while !isempty(ids[key])
            val = pop!(ids[key])
            if val == h.thunk_id
                id = key
            end
        end
    end
    wait(h, id)
    wait(h, id)
    fetch(h, id)
    fetch(h, id)
    x
end
function dynamic_fetch_self(x)
    h = sch_handle()
    return fetch(h, h.thunk_id)
end
function dynamic_fetch_dominated(x)
    h = sch_handle()
    ids = Dagger.Sch.get_dag_ids(h)
    did = pop!(ids[h.thunk_id])
    wait(h, did)
end
end

@testset "Scheduler" begin
    @testset "Scheduler options" begin
        @testset "single worker" begin
            options = SchedulerOptions(;single=1)
            a = delayed(checkwid)(1)
            b = delayed(checkwid)(2)
            c = delayed(checkwid)(a,b)

            @test collect(Context([1,workers()...]), c; options=options) == 1
        end
        if Threads.nthreads() == 1
            @warn "Threading tests running in serial"
        end
        @testset "proclist" begin
            options = SchedulerOptions(;proclist=[Dagger.ThreadProc])
            a = delayed(checktid)(1)
            b = delayed(checktid)(2)
            c = delayed(checktid)(a,b)

            @test collect(Context(), c; options=options) == 1
        end
        @testset "allow errors" begin
            options = SchedulerOptions(;allow_errors=true)
            a = delayed(error)("Test")
            ex = try
                collect(a)
            catch err
                err
            end
            @test Dagger.Sch.unwrap_nested_exception(ex) isa ErrorException
        end
    end
    @testset "Thunk options" begin
        @testset "meta" begin
            a = Dagger.@par rand(4)
            b = Dagger.@par meta=true (a->begin
                @assert a isa Dagger.Chunk
                Dagger.tochunk(myid())
            end)(a)
            @test collect(b) in procs()
        end
        @testset "single worker" begin
            options = Options(;single=1)
            a = delayed(checkwid; options=options)(1)

            @test collect(Context([1,workers()...]), a) == 1
        end
        @testset "proclist" begin
            options = Options(;proclist=[Dagger.ThreadProc])
            a = delayed(checktid; options=options)(1)

            @test collect(Context(), a) == 1
        end
        @everywhere Dagger.add_processor_callback!(()->FakeProc(), :fakeproc)
        @testset "proclist FakeProc" begin
            @test Dagger.iscompatible_arg(FakeProc(), nothing, Int) == true
            @test Dagger.iscompatible_arg(FakeProc(), nothing, FakeVal) == true
            @test Dagger.iscompatible_arg(FakeProc(), nothing, Float64) == false
            @test Dagger.default_enabled(Dagger.ThreadProc(1,1)) == true
            @test Dagger.default_enabled(FakeProc()) == false

            opts = Options(;proclist=[Dagger.ThreadProc])
            as = [delayed(identity; options=opts)(i) for i in 1:5]
            opts = Options(;proclist=[FakeProc], compute_scope=Dagger.AnyScope())
            b = delayed(fakesum; options=opts)(as...)

            @test collect(Context(), b) == FakeVal(57)
        end
        @everywhere Dagger.delete_processor_callback!(:fakeproc)
        @test_skip "procutil"
        #=
        @testset "procutil" begin
            opts = ThunkOptions(;procutil=Dict(Dagger.ThreadProc=>0.25))
            as = [delayed(checkpressure; options=opts)(i) for i in 1:30]
            b = delayed(checkpressure)(as...)
            collect(b)
        end
        =#
    end

    @testset "Modify workers in running job" begin
        # Test that we can add/remove workers while scheduler is running.
        # As this requires asynchronity, a flag is used to stall the tasks to
        # ensure workers are actually modified while the scheduler is working.

        setup = quote
            using Dagger, Distributed
            function _list_workers(ctx, state, task, tid, _)
                return procs(ctx)
            end
            # blocked is to guarantee that processing is not completed before we add new workers
            # Note: blocked is used in expressions below
            blocked = true
            function testfun(i)
                i <= 4 && return myid()
                # Wait for test to do its thing before we proceed
                if blocked
                    sleep(0.1) # just so we don't end up overflowing or something while waiting for workers to be added
                    # Here we would like to just wait to be rescheduled on another worker (which is not blocked)
                    # but this functionality does not exist, so instead we do this weird thing where we reschedule
                    # until we end up on a non-blocked worker
                    h = Dagger.Sch.sch_handle()
                    wkrs = Dagger.Sch.exec!(_list_workers, h)
                    t = Dagger.Sch.add_thunk!(testfun, h, nothing=>i)
                    return fetch(t)
                end
                return myid()
            end
        end

        if nprocs() > 1 # Skip if we've disabled workers
            @test_skip "Add new workers"
            #=
            @testset "Add new workers" begin
                ps = []
                try
                    ps1 = addprocs(2, exeflags="--project")
                    append!(ps, ps1)

                    @everywhere vcat(ps1, myid()) $setup

                    ctx = Context(ps1)
                    ts = delayed(vcat)((delayed(testfun)(i) for i in 1:10)...)

                    job = @async collect(ctx, ts)

                    while !istaskstarted(job)
                        sleep(0.001)
                    end

                    # Will not be added, so they should never appear in output
                    ps2 = addprocs(2, exeflags="--project")
                    append!(ps, ps2)

                    ps3 = addprocs(2, exeflags="--project")
                    append!(ps, ps3)
                    @everywhere ps3 $setup
                    addprocs!(ctx, ps3)
                    @test length(procs(ctx)) == 4

                    @everywhere ps3 blocked=false

                    ps_used = fetch(job)
                    @test ps_used isa Vector

                    @test any(p -> p in ps_used, ps1)
                    @test any(p -> p in ps_used, ps3)
                    @test !any(p -> p in ps2, ps_used)
                finally
                    wait(rmprocs(ps))
                end
            end
            =#

            @test_skip "Remove workers"
            #=@testset "Remove workers" begin
                ps = []
                try
                    ps1 = addprocs(4, exeflags="--project")
                    append!(ps, ps1)

                    @everywhere vcat(ps1, myid()) $setup

                    # Use single to force scheduler to make use of all workers since we assert it below
                    ts = delayed(vcat)((delayed(testfun; single=ps1[mod1(i, end)])(i) for i in 1:10)...)

                    # Use FilterLog as a callback function.
                    nprocs_removed = Ref(0)
                    first_rescheduled_thunk=Ref(false)
                    rmproctrigger = Dagger.FilterLog(Dagger.NoOpLog()) do event
                        if typeof(event) == Dagger.Event{:finish} && event.category === :cleanup_proc
                            nprocs_removed[] += 1
                        end
                        if typeof(event) == Dagger.Event{:start} && event.category === :add_thunk
                            first_rescheduled_thunk[] = true
                        end
                        return false
                    end

                    ctx = Context(ps1; log_sink=rmproctrigger)
                    job = @async collect(ctx, ts)

                    # Must wait for this or else we won't get callback for rmprocs!
                    # Timeout so we don't stall forever if something breaks
                    starttime = time()
                    while !first_rescheduled_thunk[] && (time() - starttime < 10.0)
                        sleep(0.1)
                    end
                    @test first_rescheduled_thunk[]

                    rmprocs!(ctx, ps1[3:end])
                    @test length(procs(ctx)) == 2

                    # Timeout so we don't stall forever if something breaks
                    starttime = time()
                    while (nprocs_removed[] < 2) && (time() - starttime < 10.0)
                        sleep(0.01)
                    end
                    # this will fail if we timeout. Verify that we get the logevent for :cleanup_proc
                    @test nprocs_removed[] >= 2

                    @everywhere ps1 blocked=false

                    res = fetch(job)
                    @test res isa Vector

                    @test res[1:4] |> unique |> sort == ps1
                    @test all(pid -> pid in ps1[1:2], res[5:end])
                finally
                    # Prints "From worker X:    IOError:" :/
                    wait(rmprocs(ps))
                end
            end=#

            @testset "Remove all workers throws" begin
                ps = []
                try
                    ps1 = addprocs(2, exeflags="--project")
                    append!(ps, ps1)

                    @everywhere vcat(ps1, myid()) $setup

                    ts = delayed(vcat)((delayed(testfun)(i) for i in 1:16)...)

                    ctx = Context(ps1)
                    job = @async collect(ctx, ts)

                    while !istaskstarted(job)
                        sleep(0.001)
                    end

                    rmprocs!(ctx, ps1)
                    @test length(procs(ctx)) == 0

                    @everywhere ps1 blocked=false
                    @test_throws TaskFailedException fetch(job)
                finally
                    wait(rmprocs(ps))
                end
            end
        end
    end
end

@testset "Scheduler algorithms" begin
    @testset "Signature Calculation" begin
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]) isa Dagger.Sch.Signature
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).sig == [typeof(+), Int, Int]
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).sig_nokw == [typeof(+), Int, Int]
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).hash ==
              Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).hash
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).hash_nokw ==
              Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).hash_nokw
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).hash !=
              Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(2, 2)]).hash_nokw
        if isdefined(Core, :kwcall)
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).sig == [typeof(Core.kwcall), @NamedTuple{a::Int64}, typeof(+), Int]
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).sig_nokw == [typeof(+), Int]
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash ==
                  Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash_nokw ==
                  Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash_nokw
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash !=
                  Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash_nokw
        else
            kw_f = Core.kwfunc(+)
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).sig == [typeof(kw_f), @NamedTuple{a::Int64}, typeof(+), Int]
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).sig_nokw == [typeof(+), Int]
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash ==
                  Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash_nokw ==
                  Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash_nokw
            @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash !=
                  Dagger.Sch.signature(+, [Dagger.Argument(1, 1), Dagger.Argument(:a, 2)]).hash_nokw
        end
        @test Dagger.Sch.signature(+, []).sig == [typeof(+)]
        @test Dagger.Sch.signature(+, []).sig_nokw == [typeof(+)]
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1)]).sig == [typeof(+), Int]
        @test Dagger.Sch.signature(+, [Dagger.Argument(1, 1)]).sig_nokw == [typeof(+), Int]

        c = Dagger.tochunk(1.0)
        @test Dagger.Sch.signature(*, [Dagger.Argument(1, c), Dagger.Argument(2, 3)]).sig == [typeof(*), Float64, Int]
        t = Dagger.@spawn 1+2
        @test Dagger.Sch.signature(/, [Dagger.Argument(1, t), Dagger.Argument(2, c), Dagger.Argument(3, 3)]).sig == [typeof(/), Int, Float64, Int]
    end

    @testset "Cost Estimation" begin
        # New function to hide from scheduler's function cost cache
        mynothing(args...) = nothing

        # New non-singleton struct to hide from `approx_size`
        struct MyStruct
            x::Int
        end

        state = Dagger.Sch.EAGER_STATE[]
        tproc1_1 = Dagger.ThreadProc(1, 1)
        tproc2_1 = Dagger.ThreadProc(first(workers()), 1)
        procs = [tproc1_1, tproc2_1]

        # Ensure that this worker has been used at least once
        fetch(Dagger.@spawn scope=Dagger.ExactScope(tproc2_1) 1+1)

        #pres1_1 = state.worker_time_pressure[1][tproc1_1]
        #pres2_1 = state.worker_time_pressure[first(workers())][tproc2_1]
        snap = MetricsTracker.snapshot(MetricsTracker.global_metrics_cache())
        observed_rate = Dagger.metrics_lookup_transfer_rate(snap, tproc2_1, first(workers()))
        tx_rate = observed_rate !== nothing ? observed_rate : Dagger.Sch.DEFAULT_TRANSFER_RATE
        tx_xfer_cost = 1e6
        sig_unknown_cost = 1e9

        for (args, tx_size) in [
            ([1, 2], 0),
            ([Dagger.tochunk(1), 2], sizeof(Int)),
            ([1, Dagger.tochunk(2)], sizeof(Int)),
            ([Dagger.tochunk(1), Dagger.tochunk(2)], 2*sizeof(Int)),
            # TODO: Why does this work? Seems slow
            ([Dagger.tochunk(MyStruct(1))], sizeof(MyStruct)),
            ([Dagger.tochunk(MyStruct(1)), Dagger.tochunk(1)], sizeof(MyStruct)+sizeof(Int)),
        ]
            for arg in args
                if arg isa Dagger.Chunk
                    aff = Dagger.affinity(arg)
                    @test aff[1] == OSProc(1)
                    @test aff[2] == MemPool.approx_size(MemPool.poolget(arg.handle))
                end
            end

            cargs = map(arg->MemPool.poolget(arg.handle), filter(arg->isa(arg, Chunk), args))
            est_tx_size = Dagger.Sch.impute_sum(map(MemPool.approx_size, cargs))
            @test est_tx_size == tx_size

            t = delayed(mynothing)(args...)
            Dagger.Sch.collect_task_inputs!(state, t)
            sorted_procs, costs = Dagger.Sch.estimate_task_costs(state, procs, t)

            @test tproc1_1 in sorted_procs
            @test tproc2_1 in sorted_procs
            if length(cargs) > 0
                @test sorted_procs[1] == tproc1_1
                @test sorted_procs[2] == tproc2_1
            end

            @test haskey(costs, tproc1_1)
            @test haskey(costs, tproc2_1)
            @test costs[tproc1_1] ≈ #=pres1_1 +=# sig_unknown_cost # All chunks are local, and this signature is unknown
            if nprocs() > 1
                @test costs[tproc2_1] ≈ (tx_size/tx_rate) + tx_xfer_cost + #=pres2_1 +=# sig_unknown_cost # All chunks are remote, and this signature is unknown
            end
        end

        @testset "Per-Processor Transfer Rate" begin
            wid = first(workers())
            cache = MetricsTracker.global_metrics_cache()
            test_local_id = 999_999_000
            test_remote_id = 999_999_001

            MetricsTracker.bulk_update!(cache) do c
                ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                proc_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.ProcessorMetric())
                worker_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.WorkerMetric())
                rate_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.TransferRateMetric())
                MetricsTracker.set_metric_value!(proc_storage, test_local_id, tproc1_1)
                MetricsTracker.set_metric_value!(worker_storage, test_local_id, 1)
                MetricsTracker.set_metric_value!(rate_storage, test_local_id, UInt64(2_000_000))
                MetricsTracker.set_metric_value!(proc_storage, test_remote_id, tproc2_1)
                MetricsTracker.set_metric_value!(worker_storage, test_remote_id, wid)
                MetricsTracker.set_metric_value!(rate_storage, test_remote_id, UInt64(500_000))
            end

            args = [Dagger.tochunk(1), Dagger.tochunk(2)]
            tx_size = 2 * sizeof(Int)
            t = delayed(mynothing)(args...)
            Dagger.Sch.collect_task_inputs!(state, t)
            _, costs = Dagger.Sch.estimate_task_costs(state, procs, t)

            if nprocs() > 1
                @test costs[tproc2_1] ≈ sig_unknown_cost + (tx_size / 500_000) + tx_xfer_cost
            end
            @test costs[tproc1_1] ≈ sig_unknown_cost

            MetricsTracker.bulk_update!(cache) do c
                ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                for m in (Dagger.ProcessorMetric(), Dagger.WorkerMetric(), Dagger.TransferRateMetric())
                    storage = MetricsTracker.get_or_create_storage!(ctx, m)
                    MetricsTracker.delete_metric_value!(storage, test_local_id)
                    MetricsTracker.delete_metric_value!(storage, test_remote_id)
                end
            end
        end

        @testset "Metrics Integration" begin
            cache = MetricsTracker.global_metrics_cache()
            pre_snap = MetricsTracker.snapshot(cache)
            pre_count = haskey(pre_snap.contexts, (Dagger, :execute!)) ?
                length(MetricsTracker.values_for_metric(pre_snap, Dagger, :execute!, MetricsTracker.TimeMetric())) :
                0

            for _ in 1:5
                fetch(Dagger.@spawn metrics_probe(7))
            end

            sleep(0.1)
            post_snap = MetricsTracker.snapshot(cache)
            @test haskey(post_snap.contexts, (Dagger, :execute!))
            time_values = MetricsTracker.values_for_metric(post_snap, Dagger, :execute!, MetricsTracker.TimeMetric())
            # The global cache is bounded to the most-recent METRICS_CACHE_MAX_TASKS
            # tasks, so the count grows by the 5 new tasks only up to that cap.
            @test length(time_values) >= min(pre_count + 5, Dagger.METRICS_CACHE_MAX_TASKS)
            @test length(time_values) <= Dagger.METRICS_CACHE_MAX_TASKS

            time_inferred = Base.return_types(MetricsTracker.lookup_value,
                Tuple{MetricsTracker.MetricsSnapshot, Module, Symbol, MetricsTracker.TimeMetric, Int})[1]
            @test time_inferred === Union{UInt64, Nothing}

            sig_values = MetricsTracker.values_for_metric(post_snap, Dagger, :execute!, Dagger.SignatureMetric())
            @test !isempty(sig_values)
            @test any(v -> v isa Vector && typeof(metrics_probe) in v, values(sig_values))

            proc_values = MetricsTracker.values_for_metric(post_snap, Dagger, :execute!, Dagger.ProcessorMetric())
            @test any(v -> v isa Dagger.Processor, values(proc_values))
        end

        @testset "Move Metric Types" begin
            @test MetricsTracker.metric_type(Dagger.FromSpaceMetric) === Union{Dagger.MemorySpace, Nothing}
            @test MetricsTracker.metric_type(Dagger.ToSpaceMetric) === Union{Dagger.MemorySpace, Nothing}
            @test MetricsTracker.metric_type(Dagger.MoveSizeMetric) === Union{UInt64, Nothing}
            @test MetricsTracker.metric_applies(Dagger.FromSpaceMetric(), Val{:execute!}())
            @test MetricsTracker.metric_applies(Dagger.ToSpaceMetric(), Val{:execute!}())
            @test MetricsTracker.metric_applies(Dagger.MoveSizeMetric(), Val{:execute!}())
        end

        @testset "_record_move_metrics! writes From/To/Size to cache" begin
            cache = MetricsTracker.MetricsCache()
            src_space = Dagger.memory_space(1)
            dst_space = Dagger.memory_space(1)
            test_key = 1234
            Dagger._record_move_metrics!(cache, test_key, src_space, dst_space, UInt64(4096))
            snap = MetricsTracker.snapshot(cache)
            @test MetricsTracker.lookup_value(snap, Dagger, :execute!, Dagger.FromSpaceMetric(), test_key) === src_space
            @test MetricsTracker.lookup_value(snap, Dagger, :execute!, Dagger.ToSpaceMetric(), test_key) === dst_space
            @test MetricsTracker.lookup_value(snap, Dagger, :execute!, Dagger.MoveSizeMetric(), test_key) == UInt64(4096)
        end

        @testset "_record_move_metrics! skips MoveSizeMetric when size is nothing" begin
            cache = MetricsTracker.MetricsCache()
            src_space = Dagger.memory_space(1)
            dst_space = Dagger.memory_space(1)
            test_key = 1235
            Dagger._record_move_metrics!(cache, test_key, src_space, dst_space, nothing)
            snap = MetricsTracker.snapshot(cache)
            @test MetricsTracker.lookup_value(snap, Dagger, :execute!, Dagger.FromSpaceMetric(), test_key) === src_space
            @test MetricsTracker.lookup_value(snap, Dagger, :execute!, Dagger.ToSpaceMetric(), test_key) === dst_space
            @test MetricsTracker.lookup_value(snap, Dagger, :execute!, Dagger.MoveSizeMetric(), test_key) === nothing
        end

        @testset "EXECUTE_METRICS_SPEC excludes move metrics" begin
            spec_metrics = Dagger.execute_metrics_spec().metrics
            @test !any(m -> m isa Dagger.FromSpaceMetric, spec_metrics)
            @test !any(m -> m isa Dagger.ToSpaceMetric, spec_metrics)
            @test !any(m -> m isa Dagger.MoveSizeMetric, spec_metrics)
            @test any(m -> m isa Dagger.SignatureMetric, spec_metrics)
            @test any(m -> m isa MetricsTracker.TimeMetric, spec_metrics)
        end

        @testset "DTaskTLS carries metrics_cache field" begin
            @test :metrics_cache in fieldnames(Dagger.DTaskTLS)
        end

        @testset "Move Lookups" begin
            cache = MetricsTracker.global_metrics_cache()
            src_space = Dagger.memory_space(1)
            dst_space = Dagger.memory_space(1)
            move_key_a = 888_888_001
            move_key_b = 888_888_002

            MetricsTracker.bulk_update!(cache) do c
                ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                from_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.FromSpaceMetric())
                to_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.ToSpaceMetric())
                size_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.MoveSizeMetric())
                time_storage = MetricsTracker.get_or_create_storage!(ctx, MetricsTracker.TimeMetric())
                MetricsTracker.set_metric_value!(from_storage, move_key_a, src_space)
                MetricsTracker.set_metric_value!(to_storage, move_key_a, dst_space)
                MetricsTracker.set_metric_value!(size_storage, move_key_a, UInt64(1_000_000))
                MetricsTracker.set_metric_value!(time_storage, move_key_a, UInt64(1_000_000))
                MetricsTracker.set_metric_value!(from_storage, move_key_b, src_space)
                MetricsTracker.set_metric_value!(to_storage, move_key_b, dst_space)
                MetricsTracker.set_metric_value!(size_storage, move_key_b, UInt64(3_000_000))
                MetricsTracker.set_metric_value!(time_storage, move_key_b, UInt64(3_000_000))
            end

            snap = MetricsTracker.snapshot(cache)
            t = Dagger.metrics_lookup_move_time(snap, src_space, dst_space)
            @test t isa UInt64 && t > 0
            r = Dagger.metrics_lookup_move_rate(snap, src_space, dst_space)
            @test r isa UInt64
            @test r ≈ round(UInt64, 4_000_000 / (4_000_000 / 1e9))

            MetricsTracker.bulk_update!(cache) do c
                ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                for m in (Dagger.FromSpaceMetric(), Dagger.ToSpaceMetric(),
                          Dagger.MoveSizeMetric(), MetricsTracker.TimeMetric())
                    storage = MetricsTracker.get_or_create_storage!(ctx, m)
                    MetricsTracker.delete_metric_value!(storage, move_key_a)
                    MetricsTracker.delete_metric_value!(storage, move_key_b)
                end
            end
        end

        @testset "metrics_lookup_move_* nothing on empty" begin
            empty_cache = MetricsTracker.MetricsCache()
            snap = MetricsTracker.snapshot(empty_cache)
            src_space = Dagger.memory_space(1)
            dst_space = Dagger.memory_space(1)
            @test Dagger.metrics_lookup_move_time(snap, src_space, dst_space) === nothing
            @test Dagger.metrics_lookup_move_rate(snap, src_space, dst_space) === nothing
        end

        @testset "Runtime Reducer Variants" begin
            cache = MetricsTracker.global_metrics_cache()
            test_sig = Any[typeof(MyStruct), MyStruct, Int64]
            test_proc = tproc1_1
            base_key = 777_777_000

            keys_used = Int[]
            try
                MetricsTracker.bulk_update!(cache) do c
                    ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                    sig_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.SignatureMetric())
                    proc_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.ProcessorMetric())
                    worker_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.WorkerMetric())
                    time_storage = MetricsTracker.get_or_create_storage!(ctx, MetricsTracker.ThreadTimeMetric())
                    for (i, t) in enumerate([UInt64(100), UInt64(200), UInt64(300), UInt64(500), UInt64(900)])
                        key = base_key + i
                        push!(keys_used, key)
                        MetricsTracker.set_metric_value!(sig_storage, key, test_sig)
                        MetricsTracker.set_metric_value!(proc_storage, key, test_proc)
                        MetricsTracker.set_metric_value!(worker_storage, key, 1)
                        MetricsTracker.set_metric_value!(time_storage, key, t)
                    end
                end
                snap = MetricsTracker.snapshot(cache)
                @test Dagger.metrics_lookup_runtime_min(snap, test_sig, test_proc, 1) == UInt64(100)
                @test Dagger.metrics_lookup_runtime_max(snap, test_sig, test_proc, 1) == UInt64(900)
                @test Dagger.metrics_lookup_runtime_mean(snap, test_sig, test_proc, 1) == UInt64(400)
                @test Dagger.metrics_lookup_runtime_median(snap, test_sig, test_proc, 1) == UInt64(300)
                @test Dagger.metrics_lookup_runtime(snap, test_sig, test_proc, 1) in [UInt64(100), UInt64(200), UInt64(300), UInt64(500), UInt64(900)]
                @test Dagger.metrics_lookup_runtime(snap, test_sig, test_proc, 1; reducer=Statistics.mean) == UInt64(400)

                empty_sig = Any[typeof(println)]
                @test Dagger.metrics_lookup_runtime_median(snap, empty_sig, test_proc, 1) === nothing
            finally
                MetricsTracker.bulk_update!(cache) do c
                    ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                    for m in (Dagger.SignatureMetric(), Dagger.ProcessorMetric(),
                              Dagger.WorkerMetric(), MetricsTracker.ThreadTimeMetric())
                        storage = MetricsTracker.get_or_create_storage!(ctx, m)
                        for k in keys_used
                            MetricsTracker.delete_metric_value!(storage, k)
                        end
                    end
                end
            end
        end

        @testset "Alloc Reducer Variants" begin
            cache = MetricsTracker.global_metrics_cache()
            test_sig = Any[typeof(MyStruct), MyStruct, Float64]
            test_proc = tproc1_1
            base_key = 666_666_000

            keys_used = Int[]
            try
                MetricsTracker.bulk_update!(cache) do c
                    ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                    sig_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.SignatureMetric())
                    proc_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.ProcessorMetric())
                    alloc_storage = MetricsTracker.get_or_create_storage!(ctx, MetricsTracker.AllocMetric())
                    for (i, allocd_bytes) in enumerate([100, 200, 300, 500, 900])
                        key = base_key + i
                        push!(keys_used, key)
                        MetricsTracker.set_metric_value!(sig_storage, key, test_sig)
                        MetricsTracker.set_metric_value!(proc_storage, key, test_proc)
                        gc_diff = Base.GC_Diff(allocd_bytes, 0, 0, 0, 0, 0, 0, 0, 0)
                        MetricsTracker.set_metric_value!(alloc_storage, key, gc_diff)
                    end
                end
                snap = MetricsTracker.snapshot(cache)
                @test Dagger.metrics_lookup_alloc_min(snap, test_sig, test_proc) == UInt64(100)
                @test Dagger.metrics_lookup_alloc_max(snap, test_sig, test_proc) == UInt64(900)
                @test Dagger.metrics_lookup_alloc_mean(snap, test_sig, test_proc) == UInt64(400)
                @test Dagger.metrics_lookup_alloc_median(snap, test_sig, test_proc) == UInt64(300)
            finally
                MetricsTracker.bulk_update!(cache) do c
                    ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                    for m in (Dagger.SignatureMetric(), Dagger.ProcessorMetric(),
                              MetricsTracker.AllocMetric())
                        storage = MetricsTracker.get_or_create_storage!(ctx, m)
                        for k in keys_used
                            MetricsTracker.delete_metric_value!(storage, k)
                        end
                    end
                end
            end
        end

        @testset "Move Time Reducer Variants" begin
            cache = MetricsTracker.global_metrics_cache()
            src_space = Dagger.memory_space(1)
            dst_space = Dagger.memory_space(1)
            base_key = 555_555_000

            keys_used = Int[]
            try
                MetricsTracker.bulk_update!(cache) do c
                    ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                    from_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.FromSpaceMetric())
                    to_storage = MetricsTracker.get_or_create_storage!(ctx, Dagger.ToSpaceMetric())
                    time_storage = MetricsTracker.get_or_create_storage!(ctx, MetricsTracker.TimeMetric())
                    for (i, t) in enumerate([UInt64(100), UInt64(200), UInt64(300), UInt64(500), UInt64(900)])
                        key = base_key + i
                        push!(keys_used, key)
                        MetricsTracker.set_metric_value!(from_storage, key, src_space)
                        MetricsTracker.set_metric_value!(to_storage, key, dst_space)
                        MetricsTracker.set_metric_value!(time_storage, key, t)
                    end
                end
                snap = MetricsTracker.snapshot(cache)
                @test Dagger.metrics_lookup_move_time(snap, src_space, dst_space) == UInt64(400)
                @test Dagger.metrics_lookup_move_time_median(snap, src_space, dst_space) == UInt64(300)
                @test Dagger.metrics_lookup_move_time_min(snap, src_space, dst_space) == UInt64(100)
                @test Dagger.metrics_lookup_move_time_max(snap, src_space, dst_space) == UInt64(900)
            finally
                MetricsTracker.bulk_update!(cache) do c
                    ctx = MetricsTracker.pending_context!(c, Dagger, :execute!, Int)
                    for m in (Dagger.FromSpaceMetric(), Dagger.ToSpaceMetric(),
                              MetricsTracker.TimeMetric())
                        storage = MetricsTracker.get_or_create_storage!(ctx, m)
                        for k in keys_used
                            MetricsTracker.delete_metric_value!(storage, k)
                        end
                    end
                end
            end
        end

        @testset "instrumented_move! records via TLS in real Dagger task" begin
            cache = MetricsTracker.global_metrics_cache()
            pre_snap = MetricsTracker.snapshot(cache)
            pre_count = if haskey(pre_snap.contexts, (Dagger, :execute!))
                length(MetricsTracker.values_for_metric(pre_snap, Dagger, :execute!, Dagger.FromSpaceMetric()))
            else
                0
            end

            src_data = rand(64)
            dst_data = zeros(64)
            src_chunk = Dagger.tochunk(src_data)
            dst_chunk = Dagger.tochunk(dst_data)
            src_space = Dagger.memory_space(src_chunk)
            dst_space = Dagger.memory_space(dst_chunk)

            move_task = Dagger.@spawn meta=true Dagger.instrumented_move!(identity, dst_space, src_space, dst_chunk, src_chunk)
            fetch(move_task)
            sleep(0.1)

            post_snap = MetricsTracker.snapshot(cache)
            from_values = MetricsTracker.values_for_metric(post_snap, Dagger, :execute!, Dagger.FromSpaceMetric())
            to_values = MetricsTracker.values_for_metric(post_snap, Dagger, :execute!, Dagger.ToSpaceMetric())
            size_values = MetricsTracker.values_for_metric(post_snap, Dagger, :execute!, Dagger.MoveSizeMetric())

            @test length(from_values) > pre_count
            @test any(v -> v === src_space, values(from_values))
            @test any(v -> v === dst_space, values(to_values))
            @test any(v -> v isa UInt64 && v > 0, values(size_values))
        end
    end
end

@testset "Dynamic Thunks" begin
    @testset "Exec" begin
        a = delayed(dynamic_exec)(2)
        @test collect(Context(), a) == 1
    end
    @testset "Exec Error" begin
        a = delayed(dynamic_exec_err)(1)
        try
            collect(Context(), a)
            @test false
        catch err
            @test err isa RemoteException
        end
    end
    @test_skip "Halt" #=begin
        a = delayed(dynamic_halt)(1)
        try
            collect(Context(), a)
            @test false
        catch err
            @test err isa SchedulerHaltedException
        end
    end=#
    @testset "DAG querying" begin
        a = delayed(identity)(1)
        b = delayed(x->x+2)(a)
        c = delayed(x->x-1)(a)
        d = delayed(dynamic_get_dag)(b, c)
        ids = collect(Context(), d)
        @test ids isa Dict
        @test length(keys(ids)) == 4

        a_id = ThunkID(a.id)
        b_id = ThunkID(b.id)
        c_id = ThunkID(c.id)
        d_id = ThunkID(d.id)

        @test haskey(ids, d_id)
        @test length(ids[d_id]) == 0 # no one waiting on our result

        @test haskey(ids, a_id)
        @test length(ids[a_id]) == 0 # b and c finished, our result was unneeded

        @test length(ids[b_id]) == 1 # d was still executing
        @test length(ids[c_id]) == 1 # d was still executing
        @test pop!(ids[b_id]) == d_id
        @test pop!(ids[c_id]) == d_id
    end
    @testset "Add Thunk" begin
        a = delayed(dynamic_add_thunk)(1)
        res = collect(Context(), a)
        @test res == 2
        @testset "self as input" begin
            a = delayed(dynamic_add_thunk_self_dominated)(1)
            @test_throws_unwrap (RemoteException, Dagger.Sch.DynamicThunkException) reason="Cannot fetch result of dominated thunk" collect(Context(), a)
        end
    end
    @testset "Fetch/Wait" begin
        @testset "multiple" begin
            a = delayed(dynamic_wait_fetch_multiple)(delayed(+)(1,2))
            @test collect(Context(), a) == 3
        end
        @testset "self" begin
            a = delayed(dynamic_fetch_self)(1)
            @test_throws_unwrap (RemoteException, Dagger.Sch.DynamicThunkException) reason="Cannot fetch own result" collect(Context(), a)
        end
        @testset "dominated" begin
            a = delayed(identity)(delayed(dynamic_fetch_dominated)(1))
            @test_throws_unwrap (RemoteException, Dagger.Sch.DynamicThunkException) reason="Cannot fetch result of dominated thunk" collect(Context(), a)
        end
    end
end

c1 = Dagger.tochunk(1)
c2 = Dagger.tochunk(2)
@everywhere begin
function testpresent(x,y)
    @assert haskey(Dagger.Sch.CHUNK_CACHE, $c1)
    @assert haskey(Dagger.Sch.CHUNK_CACHE, $c2)
    x+y
end
function testevicted(x)
    sleep(1)
    @assert !haskey(Dagger.Sch.CHUNK_CACHE, $c1)
    @assert !haskey(Dagger.Sch.CHUNK_CACHE, $c2)
    x
end
end

@test_skip "Chunk Caching"
#=
@testset "Chunk Caching" begin
    compute(delayed(testevicted)(delayed(testpresent)(c1,c2)))
end
=#

@testset "MemPool.approx_size" begin
    for (obj, size) in [
        (rand(100), 100*sizeof(Float64)),
        (rand(Float32, 100), 100*sizeof(Float32)),
        (rand(1:10, 100), 100*sizeof(Int)),
        (fill(:a, 10), missing),
        (fill("a", 10), missing),
        (fill('a', 10), missing),
    ]
        if size !== missing
            @test MemPool.approx_size(obj) == size
        else
            @test MemPool.approx_size(obj) !== nothing
        end
    end
end

@testset "NG Phase 2: concurrent future registration" begin
    # Register futures on the same DTask from many concurrent Julia tasks.
    # This exercises the futures_push! + has_result recheck in _register_future!:
    # some fetchers may race with the scheduler sealing the futures list on
    # finish, so they fall back to load_result directly.
    for _ in 1:20
        t = Dagger.spawn(identity, 42)
        vals = Vector{Any}(undef, 16)
        @sync for j in 1:16
            local j = j
            Threads.@spawn begin
                vals[j] = fetch(t)
            end
        end
        @test all(==(42), vals)
    end

    # Also confirm a future registered *after* the thunk has already finished
    # is fulfilled immediately (the has_result fast path in _register_future!).
    t = Dagger.spawn(identity, 99)
    wait(t)  # ensure finished
    @test fetch(t) == 99
    @test fetch(t) == 99  # third call — always works via has_result fast path
end

@testset "NG Phase 3 validate: dataflow counter invariants" begin
    # Enable the :validate dagdebug category to turn on the pending_deps
    # underflow and ready-push consistency assertions in schedule_dependents!.
    push!(Dagger.DAGDEBUG_CATEGORIES, :validate)
    try
        # Diamond DAG: source → (left, right) → sink
        # The dataflow counter on sink must reach exactly 0 (not go negative)
        # when both left and right finish.
        source = Dagger.spawn(identity, 7)
        left   = Dagger.spawn(+, source, 1)   # 8
        right  = Dagger.spawn(*, source, 2)   # 14
        sink   = Dagger.spawn(+, left, right) # 22
        @test fetch(sink) == 22

        # Fan-in with 4 upstreams — counter must decrement 4 times to 0.
        a = Dagger.spawn(identity, 1)
        b = Dagger.spawn(identity, 2)
        c = Dagger.spawn(identity, 3)
        d = Dagger.spawn(identity, 4)
        e = Dagger.spawn((w,x,y,z)->w+x+y+z, a, b, c, d)
        @test fetch(e) == 10
    finally
        delete!(Dagger.DAGDEBUG_CATEGORIES, :validate)
    end
end

@testset "NG Phase 4 stress: concurrent independent diamonds" begin
    # Spawn N independent diamond DAGs at the same time.
    #
    # Each diamond has the shape:
    #   source → left, source → right, (left, right) → sink
    #
    # Running many concurrently exercises the single-finisher CAS in
    # store_result!, the LockedObject wrapping of thunk_dict/equiv_chunks,
    # and the Treiber-list dependents seal under concurrent pressure.
    N = 64
    results = Vector{Any}(undef, N)
    @sync for i in 1:N
        local i = i  # capture loop variable
        Threads.@spawn begin
            # Use Dagger.spawn (function form) so we can pass a plain Int
            # as the starting value without wrapping it in a thunk.
            source = Dagger.spawn(identity, i)
            left   = Dagger.spawn(+, source, 1)
            right  = Dagger.spawn(*, source, 2)
            sink   = Dagger.spawn(+, left, right)
            results[i] = fetch(sink)
        end
    end
    for i in 1:N
        # source=i, left=i+1, right=2*i, sink=(i+1)+2*i = 3i+1
        @test results[i] == 3i + 1
    end
end

@testset "NG corrections: deferred scheduling & decentralized completion" begin
    # Regression for the deferred-scheduling/delete ordering bug:
    # schedule_one! (and thus collect_task_inputs!) for a freed dependent runs
    # *after* the upstream's finish_task!, which may delete the upstream and
    # clear its result. A dependency chain whose intermediate DTasks are NOT
    # retained by the caller (so they become eligible for deletion) must still
    # complete: the upstream's result is pushed into the dependent's input slot
    # at finish time (resolve_finished_input!) before deletion.
    @testset "long chain with unreferenced intermediates" begin
        function build_unref_chain(n)
            t = Dagger.spawn(identity, 0)
            for _ in 1:n
                t = Dagger.spawn(+, t, 1)  # previous DTask becomes unreferenced
            end
            return t
        end
        for n in (100, 500)
            @test fetch(build_unref_chain(n)) == n
        end
    end

    # Wide reduction tree: many completions fan in concurrently, exercising
    # concurrent finishers (local fast-path) placing freed dependents in
    # parallel via schedule_ready! outside state.lock.
    @testset "wide reduction tree" begin
        function reduce_tree(vals)
            level = Any[Dagger.spawn(identity, v) for v in vals]
            while length(level) > 1
                nxt = Any[]
                for j in 1:2:length(level)
                    if j < length(level)
                        a = level[j]; b = level[j+1]
                        push!(nxt, Dagger.spawn(+, a, b))
                    else
                        push!(nxt, level[j])
                    end
                end
                level = nxt
            end
            return level[1]
        end
        n = 1000
        @test fetch(reduce_tree(ones(Int, n))) == n
    end

    # Wide fan-out beyond INLINE_SCHEDULE_FANOUT_THRESHOLD: one source feeds
    # many dependents, so schedule_ready! must both inline-schedule and
    # @spawn-schedule freed dependents. resolve_finished_input! must make each
    # dependent's read of the (possibly deleted) source safe.
    @testset "wide fan-out" begin
        w = 4 * Dagger.Sch.INLINE_SCHEDULE_FANOUT_THRESHOLD
        source = Dagger.spawn(identity, 10)
        deps = [Dagger.spawn(+, source, i) for i in 1:w]
        for i in 1:w
            @test fetch(deps[i]) == 10 + i
        end
    end

    # Batch-mode (compute_dag) termination edge case: the final task of a
    # batch DAG finishes in-process via the local fast-path, driving
    # running_count to 0 on a worker thread while scheduler_run blocks on
    # take!(state.chan). handle_result! must wake the master so it exits rather
    # than hanging forever.
    @testset "batch-mode in-process termination" begin
        for _ in 1:5
            c = delayed(+)(delayed(+)(delayed(identity)(1), 2), 3)
            res = fetch(Threads.@spawn collect(c))  # bounded by the test harness
            @test res == 6
        end
        # A deeper batch chain that also finishes locally.
        chain = delayed(identity)(0)
        for _ in 1:50
            chain = delayed(+)(chain, 1)
        end
        @test collect(chain) == 50
    end
end

@testset "Cancellation" begin
    # Ready task cancellation
    start_time = time_ns()
    t = Dagger.@spawn scope=Dagger.scope(worker=1, thread=1) sleep(100)
    Dagger.cancel!(t)
    @test timedwait(()->istaskdone(t), 10) == :ok
    if istaskdone(t)
        @test_throws_unwrap (Dagger.DTaskFailedException, InterruptException) fetch(t)
        @test (time_ns() - start_time) * 1e-9 < 100
    end

    # Running task cancellation
    start_time = time_ns()
    t = Dagger.@spawn scope=Dagger.scope(worker=1, thread=1) sleep(100)
    sleep(0.1) # Give the scheduler a chance to schedule the task
    Dagger.cancel!(t)
    @test timedwait(()->istaskdone(t), 10) == :ok
    if istaskdone(t)
        @test_throws_unwrap (Dagger.DTaskFailedException, InterruptException) fetch(t)
        @test (time_ns() - start_time) * 1e-9 < 100
    end

    # Normal task execution
    start_time = time_ns()
    t = Dagger.@spawn scope=Dagger.scope(worker=1, thread=1) yield()
    @test timedwait(()->istaskdone(t), 10) == :ok
    if istaskdone(t)
        @test (time_ns() - start_time) * 1e-9 < 100
    end
end
