import Dagger.Sch: SchedulerOptions, ThunkOptions

function inc(x)
    x+1
end
@everywhere begin
function checkwid(x...)
    @assert myid() == 1
    return 1
end
end

@testset "Scheduler" begin
    @testset "Scheduler options: single worker" begin
        a = delayed(checkwid)(1)
        b = delayed(checkwid)(2)
        c = delayed(checkwid)(a,b)

        @test collect(Context(), c; options=SchedulerOptions(1)) == 1
    end
    @testset "Thunk options: single worker" begin
        options = ThunkOptions(1)
        a = delayed(checkwid; options=options)(1)

        @test collect(Context(), a) == 1
    end
end
