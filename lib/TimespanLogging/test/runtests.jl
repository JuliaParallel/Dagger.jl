using TimespanLogging
import TimespanLogging: NoOpLog, LocalEventLog, MultiEventLog
using Test

@testset "Contexts" begin
    struct NullContext end
    ctx = NullContext()
    @test TimespanLogging.log_sink(ctx) == NoOpLog()
    @test TimespanLogging.profile(ctx, 1, 2, 3) == false

    timespan_start(ctx, :compute, 1, 2)
    timespan_finish(ctx, :compute, 1, 2)

    @test TimespanLogging.get_logs!(ctx) == TimespanLogging.get_logs!(NoOpLog())

    struct Context
        log_sink
        profile::Bool
    end
    ctx = Context(LocalEventLog(), true)
    TimespanLogging.log_sink(ctx) = ctx.log_sink
    TimespanLogging.profile(ctx, xs...) = ctx.profile

    timespan_start(ctx, :compute, 1, 2)
    timespan_finish(ctx, :compute, 1, 2)

    logs = TimespanLogging.get_logs!(ctx.log_sink; raw=true)
    @test length(logs[1]) == 2

    @test typeof(TimespanLogging.get_logs!(ctx)) == typeof(TimespanLogging.get_logs!(ctx.log_sink))
end
