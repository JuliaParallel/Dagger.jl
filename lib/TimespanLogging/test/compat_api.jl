@testset "Legacy Context helpers" begin
    reset!()
    ctx = NullContext()
    @test TimespanLogging.get_logs!(ctx) == TimespanLogging.get_logs!(NoOpLog())
    timespan_start(ctx, :compute, 1, 2)
    timespan_finish(ctx, :compute, 1, 2)

    ctx = Ctx(LocalEventLog(), true)
    timespan_start(ctx, :compute, 1, 2)
    timespan_finish(ctx, :compute, 1, 2)
    logs = TimespanLogging.get_logs!(ctx.log_sink; raw=true)
    @test length(logs[1]) == 2
    @test typeof(TimespanLogging.get_logs!(ctx)) == typeof(TimespanLogging.get_logs!(ctx.log_sink))
end
