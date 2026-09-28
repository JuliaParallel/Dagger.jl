using Test
include(joinpath(@__DIR__, "compare.jl"))

@testset "Benchmark comparison" begin
    stats(median; width=0, allocs=100, memory=1000) =
        Dict("median" => median, "25" => median-width/2, "75" => median+width/2,
             "allocs" => allocs, "memory" => memory)
    base = Dict("suite/dagger/op" => stats(100))
    compare(current; kwargs...) = compare_benchmarks(base, Dict("suite/dagger/op" => current); kwargs...)
    @test only(compare(stats(150)).regressions) == ("suite/dagger/op", "time", 1.5)
    @test only(compare(stats(50)).improvements) == ("suite/dagger/op", "time", 0.5)
    @test isempty(compare(stats(110)).regressions)
    @test length(compare(stats(150; width=60)).within_noise) == 1
    @test isempty(compare(stats(150; width=60)).regressions)

    # Airspeed omits quartiles for a trial with one sample. In particular, a
    # missing band must not bypass the *other* revision's measured spread.
    one_sample = Dict("median" => 400, "allocs" => 150, "memory" => 1000)
    result = compare(one_sample)
    @test result.insufficient == [("suite/dagger/op", "time", 4.0)]
    @test result.regressions == [("suite/dagger/op", "allocs", 1.5)]
    reverse = compare_benchmarks(Dict("suite/dagger/op" => one_sample), base)
    @test only(reverse.insufficient)[3] == 0.25
    @test only(reverse.improvements)[2] == "allocs" # allocation improvement still counts
    @test length(compare(one_sample; noise_tolerance=0).regressions) == 2
    @test isempty(compare(stats(100; allocs=125)).regressions)
    @test only(compare(stats(100; memory=1500)).regressions)[2] == "memory"
    @test isempty(compare_benchmarks(base, Dict("new" => stats(100))).regressions)
    @test isempty(compare_benchmarks(Dict("time_to_load" => stats(100)),
                                   Dict("time_to_load" => stats(1000))).regressions)
    @test isempty(compare_benchmarks(Dict("op" => stats(0; allocs=0, memory=0)),
                                   Dict("op" => stats(100))).regressions)
end
