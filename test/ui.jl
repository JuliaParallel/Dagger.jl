@testset "UI" begin
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
    @test occursin("Comm:", plan)
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
        @test occursin("Comm:", plan)
        @test occursin("Move:", plan)
        @test endswith(plan, "}\n")
    end
end
end
