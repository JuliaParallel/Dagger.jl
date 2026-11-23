using Test
include(joinpath(@__DIR__, "metg.jl"))

# A sweep of a 100-timestep, 4-wide graph (400 tasks). Every expected METG below
# is the value Task Bench's scripts/chart_metg.py prints for the same data,
# written out with `metg_write_log`, so these pin the analysis to Task Bench's.
const METG_TEST_POINTS = [
    (; iterations=1 << 20, elapsed=[4.0]),
    (; iterations=1 << 19, elapsed=[2.1, 2.2]),
    (; iterations=1 << 18, elapsed=[1.2, 1.25, 1.3]),
    (; iterations=1 << 17, elapsed=[0.8, 0.85, 0.9]),
    (; iterations=1 << 16, elapsed=[0.7, 0.72, 0.71]),
    (; iterations=1 << 15, elapsed=[0.72, 0.73, 0.74]),
]

@testset "METG as chart_metg.py computes it" begin
    metg_ms(; nodes=1, cores=4, peak_flops=1.4e10, threshold=0.5, points=METG_TEST_POINTS) =
        1e3 * metg_value(metg_table(points; tasks=400, nodes, cores, peak_flops), threshold)

    @test metg_ms() ≈ 8.104545170115738
    # No peak: efficiency relative to the largest tasks of the sweep
    @test metg_ms(; peak_flops=nothing) ≈ 7.976842105263158
    # The peak is per node
    @test metg_ms(; nodes=2, cores=2, peak_flops=7e9, threshold=0.8) ≈ 14.881685766586251
    # Cores the tasks did not run on are charged as well
    @test metg_ms(; cores=8, peak_flops=2.8e10, threshold=0.25) ≈ 16.209090340231477
    # The next smaller point has a larger granularity: no interpolation
    @test metg_ms(; threshold=0.3) ≈ 7.1

    table = metg_table(reverse(METG_TEST_POINTS); tasks=400, nodes=1, cores=4, peak_flops=1.4e10)
    @test [r.iterations for r in table] == [1 << e for e in 20:-1:15]
    @test [r.reps for r in table] == [1, 2, 3, 3, 3, 3]
    @test table[1].flops == 400 * (2 * 64 * 2^20 + 64)
    @test table[2].elapsed ≈ 2.15
    @test table[2].std ≈ 0.05
    @test table[2].time_per_task ≈ 2.15 / 400 * 4
    @test table[2].efficiency ≈ table[2].flops / 2.15 / 1.4e10

    # chart_metg.py's refusals
    @test_throws "no data above threshold" metg_ms(; peak_flops=1e12)
    @test_throws "no data below threshold" metg_ms(; threshold=0.1)
    @test_throws "suspiciously high efficiency" metg_ms(; peak_flops=5e9)
    rising = [(; iterations=1 << 11, elapsed=[1.0]), (; iterations=1 << 10, elapsed=[0.3])]
    @test_throws "no data following" metg_ms(; points=rising, peak_flops=400 * (2 * 64 * 2^11 + 64) / 0.4)
end

@testset "METG report from the suite's trials" begin
    ns(seconds) = 1e9 .* seconds
    key(leaf; layout="owners=2 procs=4", group="stencil_1d steps=100 width=4 pinned") =
        ["metg", "dagger", layout, group, leaf]
    iter(p) = "iter=2^$(lpad(trailing_zeros(p.iterations), 2, '0'))"
    sweep = [(key(iter(p)), ns(p.elapsed)) for p in METG_TEST_POINTS]
    # Reference run: its fastest sample makes a processor 3.5e9 FLOP/s
    reference = (key("iter=2^27"; group="peak"), ns([5.2, (2 * 64 * 2^27 + 64) / 3.5e9]))
    other = (["tasks", "dagger", "owners=2 procs=4", "pinned, round robin (n=256)"], ns([0.01]))
    leaves = [sweep; [reference, other]]

    io = IOBuffer()
    result = only(metg_report(io, leaves))
    @test result.cores == 4
    @test result.peak_flops ≈ 1.4e10
    @test 1e3 * result.metg ≈ 8.104545170115738
    @test result.sweep == (; dependence="stencil_1d", radix=nothing, steps=100, width=4, placement="pinned", bulk=false)
    text = String(take!(io))
    @test occursin("metg / dagger / owners=2 procs=4 / stencil_1d steps=100 width=4 pinned", text)
    @test occursin("400 tasks per run, charged to 1 node(s) x 4 cores", text)
    @test occursin("METG(50%) = 8.105 ms", text)

    # The reference rate is counted once per core, whatever ran the tasks
    @test 1e3 * only(metg_report(devnull, leaves; cores=8, threshold=0.25)).metg ≈ 16.209090340231477
    @test 1e3 * only(metg_report(devnull, leaves; nodes=2, cores=2, peak=7e9, threshold=0.8)).metg ≈ 14.881685766586251
    @test 1e3 * only(metg_report(devnull, leaves; peak=:sweep)).metg ≈ 7.976842105263158
    # Without a reference run among the results, fall back to the sweep itself
    io = IOBuffer()
    @test 1e3 * only(metg_report(io, sweep)).metg ≈ 7.976842105263158
    @test occursin("no reference run among the results", String(take!(io)))
    # A sweep that does not reach the threshold is reported, not thrown
    io = IOBuffer()
    @test only(metg_report(io, leaves; peak=1e12)).metg === nothing
    @test occursin("METG(50%) not determined: no data above threshold", String(take!(io)))

    # Task Bench's log format and file names
    mktempdir() do dir
        result = only(metg_report(devnull, leaves; logdir=dir))
        @test result.logpath == joinpath(dir, "dagger_ngraphs_1_type_stencil_1d_nodes_1.log")
        log = read(result.logpath, String)
        @test count("Elapsed Time", log) == sum(p -> length(p.elapsed), METG_TEST_POINTS)
        @test occursin(r"^\s*Iterations: 1048576$"m, log)
        @test occursin(r"^\s*Time Steps: 100$"m, log) && occursin(r"^\s*Max Width: 4$"m, log)
        @test occursin(r"^Total Tasks 400$"m, log) && occursin(r"^Total FLOPs 53687116800$"m, log)
        @test occursin(r"^Elapsed Time 4\.0 seconds$"m, log)

        variant = [(key(iter(p); group="nearest radix=5 steps=100 width=4 unscoped bulk"), ns(p.elapsed))
                   for p in METG_TEST_POINTS]
        result = only(metg_report(devnull, variant; nodes=2, logdir=dir))
        @test result.sweep.radix == "5" && result.sweep.placement == "unscoped" && result.sweep.bulk
        @test result.cores == 2
        @test basename(result.logpath) == "dagger_unscoped_bulk_ngraphs_1_type_nearest_radix_5_nodes_2.log"
    end

    # Several sweeps of one run share its reference
    both = [leaves; [(key(iter(p); group="fft steps=100 width=4 pinned"), ns(p.elapsed)) for p in METG_TEST_POINTS]]
    results = metg_report(devnull, both)
    @test [r.sweep.dependence for r in results] == ["fft", "stencil_1d"]
    @test all(r -> r.peak_flops ≈ 1.4e10, results)

    @test_throws "No metg sweep" metg_report(devnull, [other])
    @test_throws "Unrecognized metg benchmark group" metg_report(devnull, [(key("iter=2^10"; group="stencil_1d"), ns([1.0]))])
end

@testset "METG settings and formatting" begin
    names = "BENCHMARK_METG_" .* ["NODES", "CORES", "PEAK_FLOPS", "THRESHOLD", "LOGDIR"]
    withenv((name => nothing for name in names)...) do
        @test metg_config() == (; nodes=1, cores=nothing, peak=nothing, threshold=0.5, logdir=nothing)
        withenv("BENCHMARK_METG_NODES" => "2", "BENCHMARK_METG_CORES" => "32", "BENCHMARK_METG_PEAK_FLOPS" => "1.26e12",
                "BENCHMARK_METG_THRESHOLD" => "0.9", "BENCHMARK_METG_LOGDIR" => "logs") do
            @test metg_config() == (; nodes=2, cores=32, peak=1.26e12, threshold=0.9, logdir="logs")
        end
        withenv("BENCHMARK_METG_PEAK_FLOPS" => "sweep") do
            @test metg_config().peak === :sweep
        end
        withenv("BENCHMARK_METG_THRESHOLD" => "50") do
            @test_throws "BENCHMARK_METG_THRESHOLD" metg_config()
        end
        withenv("BENCHMARK_METG_NODES" => "0") do
            @test_throws "BENCHMARK_METG_NODES" metg_config()
        end
    end

    @test metg_time(2.5) == "2.5 s"
    @test metg_time(8.104545e-3) == "8.105 ms"
    @test metg_time(3.2e-5) == "32.0 μs"
    @test metg_time(4.5e-7) == "450.0 ns"
    @test metg_flops_per_task(1) == 192
end
