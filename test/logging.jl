@testset "Logging" begin
    @testset "LocalEventLog" begin
        @testset "ArrayOp" begin
            ctx = Context()
            log = Dagger.LocalEventLog()
            ctx.log_sink = log

            sz = 2^4
            bsz = 2^2
            X = rand(Float32, sz, sz)
            XD = Distribute(Blocks(bsz, bsz), X)

            dag = XD*XD
            collect(ctx, dag)
            logs = Dagger.get_logs!(log)
            @test logs isa Vector{Dagger.Timespan}
            @test length(logs) > 0
            plan = Dagger.show_plan(logs)
            @test plan isa String
            @test occursin("digraph {", plan)
            @test occursin("Move:", plan)
            @test endswith(plan, "}\n")
        end

        @testset "Argument Merging" begin
            ctx = Context()
            log = Dagger.LocalEventLog()
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
            logs = Dagger.get_logs!(log)
            plan = Dagger.show_plan(logs, j)
        end

        @testset "Automatic Plan Rendering" begin
            x = compute(rand(Blocks(2,2),4,4))
            mktemp() do path, io
                ctx = Context(;log_sink=Dagger.LocalEventLog(),log_file=path)
                compute(ctx, x * x)
                plan = String(read(io))
                @test occursin("digraph {", plan)
                @test occursin("Move:", plan)
                @test endswith(plan, "}\n")
            end
        end
    end
    @testset "MultiEventLog" begin
        ctx = Context()
        ml = Dagger.MultiEventLog()
        ml[:core] = Dagger.Events.CoreMetrics()
        ml[:id] = Dagger.Events.IDMetrics()
        ml[:timeline] = Dagger.Events.TimelineMetrics()
        ml[:wsat] = Dagger.Events.WorkerSaturation()
        ml[:loadavg] = Dagger.Events.CPULoadAverages()
        ml[:bytes] = Dagger.Events.BytesAllocd()
        ml[:mem] = Dagger.Events.MemoryFree()
        ml[:esat] = Dagger.Events.EventSaturation()
        ml[:psat] = Dagger.Events.ProcessorSaturation()
        ctx.log_sink = ml

        A = rand(Blocks(4, 4), 16, 16)
        collect(ctx, A*A)

        logs = Dagger.get_logs!(ml)
        for w in keys(logs)
            len = length(logs[w][:core])
            if w == 1
                @test len > 1
            end
            for c in (:core, :id, :timeline, :wsat, :loadavg, :bytes, :mem, :esat, :psat)
                @test haskey(logs[w], c)
                @test length(logs[w][c]) == len
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
        # Note: May one day be true as scheduler evolves
        @test !any(e->haskey(e, :compute), esat)
        @test !any(e->haskey(e, :move), esat)
        psat = l1[:psat]
        # Note: May become false
        @test all(e->length(e) == 0, psat)

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

        logs = Dagger.get_logs!(ml)
        for w in keys(logs)
            for c in keys(logs[w])
                @test isempty(logs[w][c])
            end
        end
    end
end
