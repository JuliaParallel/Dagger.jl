import TimespanLogging
import TimespanLogging: Timespan, Event, Events, LocalEventLog, MultiEventLog
import Colors, GraphViz, DataFrames, Plots, JSON3

@testset "Logging" begin
    @testset "LocalEventLog" begin
        @testset "Basics" begin
            ctx = Context()
            log = LocalEventLog()
            ctx.log_sink = log

            X = rand(Float32, 4, 3)
            a = delayed(sum)(X)
            b = delayed(sum)(X)
            c = delayed(+)(a,b)

            compute(ctx, c)
            logs = TimespanLogging.get_logs!(log)
            @test logs isa Vector{Timespan}
            @test length(logs) > 0
            #= FIXME
            plan = Dagger.show_plan(logs)
            @test plan isa String
            @test occursin("digraph {", plan)
            @test occursin("Move:", plan)
            @test endswith(plan, "}\n")
            =#
        end

        @testset "Argument Merging" begin
            ctx = Context()
            log = LocalEventLog()
            ctx.log_sink = log

            X = rand(Float32, 4, 3)
            a = delayed(sum)(X)
            b = delayed(sum)(X)
            c = delayed(+)(a,b)

            X1 = rand(Float32, 5, 10)
            X2 = rand(Float32, 5, 10)
            f = delayed(sum)(X1)
            g = delayed(sum)(X2)
            h = delayed(+)(f,g)

            j = delayed(+)(c,h)
            collect(ctx, j)
            logs = TimespanLogging.get_logs!(log)
            # FIXME: plan = Dagger.show_plan(logs, j)
        end
    end
    @testset "MultiEventLog" begin
        @testset "enable_logging!" begin
            Dagger.enable_logging!(;metrics=true, tasknames=true, taskdeps=true, timeline=true)

            t = Dagger.@spawn 1+2
            fetch(Dagger.@spawn t*3)

            logs = Dagger.fetch_logs!()
            @test haskey(logs, 1)
            for consumer in (:core, :id, :timeline, :wsat, :loadavg, :bytes, :mem, :esat, :psat, :tasknames, :taskdeps)
                @test length(logs[1][consumer]) > 0
            end

            Dagger.disable_logging!()
        end

        @testset "Manual" begin
            ctx = Context()
            ml = MultiEventLog()
            ml[:core] = Events.CoreMetrics()
            ml[:id] = Events.IDMetrics()
            ml[:timeline] = Events.TimelineMetrics()
            ml[:wsat] = Dagger.Events.WorkerSaturation()
            ml[:loadavg] = Events.CPULoadAverages()
            ml[:bytes] = Dagger.Events.BytesAllocd()
            ml[:mem] = Events.MemoryFree()
            ml[:esat] = Events.EventSaturation()
            ml[:psat] = Dagger.Events.ProcessorSaturation()
            ctx.log_sink = ml

            X = rand(Float32, 4, 3)
            a = delayed(sum)(X)
            b = delayed(sum)(X)
            c = delayed(+)(a,b)
            compute(ctx, c)
            sleep(1)

            logs = TimespanLogging.get_logs!(ml)
            for w in keys(logs)
                len = length(logs[w][:core])
                if w == 1
                    @test len > 1
                end
                for c in (:core, :id, :timeline, :wsat, :loadavg, :bytes, :mem, :esat, :psat)
                    @test haskey(logs[w], c)
                    @test length(logs[w][c]) == len
                end

                for idx in 1:length(logs[w][:core])
                    category = logs[w][:core][idx].category
                    if category in (:scheduler_init, :scheduler_exit, :take, :assign_procs)
                        @test logs[w][:id][idx] === nothing
                    end
                    if category in (:add_thunk, :schedule, :finish, :move, :compute, :storage_wait, :storage_safe_scan, :enqueue)
                        @test logs[w][:id][idx] isa NamedTuple
                        @test haskey(logs[w][:id][idx], :thunk_id)
                        @test logs[w][:id][idx].thunk_id isa Int
                    end
                    if category in (:init_proc, :cleanup_proc, :fire, :proc_run_wait, :proc_run_fetch, :steal_local, :remove_procs, :handle_fault, :evict)
                        @test logs[w][:id][idx] isa NamedTuple
                        @test haskey(logs[w][:id][idx], :worker)
                        @test logs[w][:id][idx].worker isa Int
                    end
                    if category in (:proc_run_wait, :proc_run_fetch, :steal_local, :enqueue, :storage_wait, :move, :compute, :storage_safe_scan)
                        @test logs[w][:id][idx] isa NamedTuple
                        @test haskey(logs[w][:id][idx], :processor)
                        @test logs[w][:id][idx].processor isa Dagger.Processor
                    end
                end
            end
            @test length(keys(logs)) > 1

            l1 = logs[1]
            core = l1[:core]
            @test !any(isnothing, core)
            esat = l1[:esat]
            @test any(e->haskey(e, :scheduler_init), esat)
            @test any(e->haskey(e, :schedule), esat)
            @test any(e->haskey(e, :fire), esat)
            @test any(e->haskey(e, :take), esat)
            @test any(e->haskey(e, :finish), esat)
            if Threads.nthreads() == 1
                # Note: May one day be true as scheduler evolves
                @test !any(e->haskey(e, :compute), esat)
                @test !any(e->haskey(e, :move), esat)
                psat = l1[:psat]
                # Note: May become false
                @test all(e->length(e) == 0, psat)
            end

            had_psat_proc = 0
            for wo in filter(w->w != 1, keys(logs))
                lo = logs[wo]
                esat = lo[:esat]
                @test !any(e->haskey(e, :scheduler_init), esat)
                @test !any(e->haskey(e, :schedule), esat)
                @test !any(e->haskey(e, :fire), esat)
                @test !any(e->haskey(e, :take), esat)
                @test !any(e->haskey(e, :finish), esat)
                psat = lo[:psat]
                if any(e->length(e) > 0, psat)
                    had_psat_proc += 1
                    @test any(e->haskey(e, :compute), esat)
                    @test any(e->haskey(e, :move), esat)
                end
            end
            @test had_psat_proc > 0

            logs = TimespanLogging.get_logs!(ml)
            for w in keys(logs)
                for c in keys(logs[w])
                    @test isempty(logs[w][c])
                end
            end
        end
    end

    if VERSION >= v"1.9-"
        @testset "show_plan/render_plan built-in" begin
            Dagger.enable_logging!()

            A = distribute(rand(4, 4), Blocks(8, 8))
            sum(A)
            logs = Dagger.fetch_logs!()

            # GraphVizExt
            @test Dagger.render_logs(logs, :graphviz) !== nothing

            # PlotsExt
            @test Dagger.render_logs(logs, :plots_gantt) !== nothing

            # JSON3Ext
            @test Dagger.render_logs(logs, :chrome_trace) !== nothing
            
            Dagger.disable_logging!()
        end
    end
end
