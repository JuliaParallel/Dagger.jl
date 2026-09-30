using Test
include(joinpath(@__DIR__, "compare.jl"))

@testset "Benchmark comparison" begin
    stats(median; width=0, allocs=100, memory=1000, samples=5) =
        Dict("median" => median, "25" => median-width/2, "75" => median+width/2,
             "allocs" => allocs, "memory" => memory, "samples" => samples)
    base = Dict("suite/dagger/op" => stats(100))
    compare(current; kwargs...) = compare_benchmarks(base, Dict("suite/dagger/op" => current); kwargs...)
    @test only(compare(stats(150)).regressions) == ("suite/dagger/op", "time", 1.5)
    @test only(compare(stats(50)).improvements) == ("suite/dagger/op", "time", 0.5)
    @test isempty(compare(stats(110)).regressions)
    @test length(compare(stats(150; width=60)).within_noise) == 1
    @test isempty(compare(stats(150; width=60)).regressions)

    # Airspeed omits quartiles for a trial with one sample. In particular, a
    # missing band must not bypass the *other* revision's measured spread.
    one_sample = Dict("median" => 400, "samples" => 1, "allocs" => 150, "memory" => 1000)
    result = compare(one_sample)
    @test result.insufficient == [("suite/dagger/op", "time", 4.0)]
    @test result.regressions == [("suite/dagger/op", "allocs", 1.5)]
    reverse = compare_benchmarks(Dict("suite/dagger/op" => one_sample), base)
    @test only(reverse.insufficient)[3] == 0.25
    @test only(reverse.improvements)[2] == "allocs" # allocation improvement still counts
    @test length(compare(one_sample; noise_tolerance=0).regressions) == 1
    @test isempty(compare(stats(100; allocs=125)).regressions)
    @test only(compare(stats(100; memory=1500)).regressions)[2] == "memory"
    @test isempty(compare_benchmarks(base, Dict("new" => stats(100))).regressions)
    @test isempty(compare_benchmarks(Dict("time_to_load" => stats(100)),
                                   Dict("time_to_load" => stats(1000))).regressions)
    @test isempty(compare_benchmarks(Dict("op" => stats(0; allocs=0, memory=0)),
                                   Dict("op" => stats(100))).regressions)
end

@testset "Sample count and independent confirmation" begin
    stats(t, n) = Dict("median"=>t, "25"=>t, "75"=>t, "samples"=>n)
    baseline = Dict("op"=>stats(100, 5))
    candidate(n) = Dict("op"=>stats(150, n))
    compare(n; kwargs...) = compare_benchmarks(baseline, candidate(n); kwargs...)
    @test isempty(compare(4).regressions)
    @test length(compare(4).insufficient) == 1
    @test isempty(compare(4; noise_tolerance=0).regressions)
    @test length(compare(5).regressions) == 1
    @test length(compare_benchmarks(candidate(4), baseline).insufficient) == 1
    @test length(compare_benchmarks(Dict("op"=>Dict("median"=>100)), candidate(5)).insufficient) == 1
    first = compare(5)
    @test confirm_timing_regressions(first, compare(5)).regressions == first.regressions
    for second in (compare(4), compare_benchmarks(baseline, baseline), compare_benchmarks(baseline, Dict()))
        result = confirm_timing_regressions(first, second)
        @test isempty(result.regressions)
        @test result.insufficient == first.regressions
    end
    allocation = (; first..., regressions=[("op", "allocs", 1.5)])
    @test confirm_timing_regressions(allocation, compare(4)).regressions == allocation.regressions
    flat = Dict("suite/op"=>Dict{String,Any}("median"=>100))
    raw = Dict("data"=>Dict("suite"=>Dict("data"=>Dict("op"=>Dict("times"=>[1,2,3,4], "params"=>Dict("samples"=>100))))))
    attach_sample_counts!(flat, raw)
    @test flat["suite/op"]["samples"] == 4
end
