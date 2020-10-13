using Distributed
if haskey(ENV, "BENCHMARK_PROCS")
    np, nt = parse.(Ref(Int), split(ENV["BENCHMARK_PROCS"], ":"))
    for i in workers()
        addprocs(np; exeflags="-t $nt")
    end
else
    np = length(workers())
    nt = 1
end
if haskey(ENV, "BENCHMARK_REMOTES")
    remotes = split(ENV["BENCHMARK_REMOTES"], ":")
    if !isempty(remotes)
        for i in 1:np
            addprocs(remotes; exeflags="-t $nt")
        end
    end
end
if haskey(ENV, "BENCHMARK_PROJECT")
    using Pkg
    for i in workers()
        remotecall_fetch(Pkg.activate, i, ENV["BENCHMARK_PROJECT"])
    end
end
@everywhere using Dagger
import Dagger: Computation, reduceblock
using Dates, Random, Statistics, LinearAlgebra, InteractiveUtils
using JLD, Luxor, ProfileSVG

live = parse(Bool, get(ENV, "BENCHMARK_LIVE", "0"))
if live
    using Mux
else
    using FFMPEG, FileIO, ImageMagick
end
const RENDERS = Dict{Int,Dict}()

using BenchmarkTools

AmulB_compatible(x,y) =
    try; size(x), size(y), x * y; true; catch err false end

AmulB_compatible(x::Computation,y::Computation) = AmulB_compatible(domain(x), domain(y))

function AcmulB_compatible(x,y)
    AmulB_compatible(domain(x)', domain(y))
end

function array_suite_inner(ctx, X, Y, f=x->x)
    suite = BenchmarkGroup(["array"])

    suite["alloc"] = @benchmarkable (compute($f($X)); GC.gc())

    Y = compute(ctx, f(X))
    suite["X.+1.0"]  = @benchmarkable (compute($ctx, $f($Y.+1)); GC.gc())
    suite["X+X"]     = @benchmarkable (compute($ctx, $f($Y+$Y)); GC.gc())
    suite["X'"]      = @benchmarkable (compute($ctx, $f($Y')); GC.gc())
    suite["sin.(X)"] = @benchmarkable (compute($ctx, $f(sin.($Y))); GC.gc())

    if ndims(Y) == 2
        suite["X+X'"] = @benchmarkable (compute($ctx, $f($Y+$Y')); GC.gc())
        suite["X*X"]  = @benchmarkable (compute($ctx, $f($Y*$Y)); GC.gc())
        suite["X'*X"] = @benchmarkable (compute($ctx, $f($Y'*$Y)); GC.gc())
    end

    suite
end

function nnmf(X, W, H)
    # H update
    H = (H .* (W' * (X ./ (W * H)))
         ./ (sum(W; dims=1))')
    # W update
    W = (W .* ((X ./ (W * H)) * (H'))
         ./ (sum(H; dims=2)'))
    # error estimate
    X - W * H
end

theory_flops(nrow, ncol, nfeatures) = 11 * ncol * nrow * nfeatures + 2 * (ncol + nrow) * nfeatures

function array_suite(f=x->x; kwargs...)
    suite = BenchmarkGroup()

    #= TODO: Re-enable
    pairs = Set{Tuple}()
    for i in (1, 2, 5, 10, 64, 100, 1028)
        for bscale in (1, 0.5, 0.3, 0.25, 0.1)
            b = ceil(Int, i*bscale)
            if !((b,i) in pairs)
                push!(pairs, (b,i))
                X = zeros(Blocks(b), i)
                Y = compute(ctx, f(X))
                suite["vector $i/$b"] = array_suite_inner(ctx, X, Y, f)
            end
            if !(((b,b),(i,i)) in pairs)
                push!(pairs, ((b,b),(i,i)))
                X = zeros(Blocks(b,b), i, i)
                Y = compute(ctx, f(X))
                suite["matrix ($i,$i)/$b"] = array_suite_inner(ctx, X, Y, f)
            end
        end
    end
    =#

    for scale in (2 .^ (3:1:6))
        ncol = 2001 * scale
        nrow = 10002
        nfeatures = 12
        RENDERS[scale] = Dict{Int,Vector}()
        nw = length(workers())
        nsuite = BenchmarkGroup()
        while nw > 0
            ctx = Context(collect((1:nw) .+ 1); kwargs...)
            p = sum([length(Dagger.get_processors(OSProc(id))) for id in 2:(nw+1)])
            X = compute(rand(Blocks(nrow, ncol÷p), nrow, ncol))
            W = compute(rand(Blocks(nrow, ncol÷p), nrow, nfeatures))
            H = compute(rand(Blocks(nrow, ncol÷p), nfeatures, ncol))
            nsuite["Workers: $nw"] = @benchmarkable begin
                compute($ctx, $(nnmf(X, W, H)))
            end setup=begin
                _nw, _scale = $nw, $scale
                @info "Starting $_nw worker NNMF (scale by $_scale)"
                Dagger.show_gantt($ctx; width=1800, window_length=20, delay=2, port=4040, live=live)
            end teardown=begin
                Dagger.continue_rendering[] = false
                video_paths = take!(Dagger.render_results)
                video_data = Dict(key=>read(video_paths[key]) for key in keys(video_paths))
                push!(get!(()->[], RENDERS[$scale], $nw), video_data)
            end
            nw ÷= 2
        end
        suite["NNMF scaled by: $scale"] = nsuite
    end

    suite
end
function serial_suite()
    suite = BenchmarkGroup()

    for scale in (2 .^ (3:1:6))
        ncol = 2001 * scale
        nrow = 10002
        nfeatures = 12
        X = rand(nrow, ncol)
        W = rand(nrow, nfeatures)
        H = rand(nfeatures, ncol)
        suite["NNMF scaled by: $scale"] = @benchmarkable nnmf($X, $W, $H)
    end

    suite
end

function main()
    nw = length(workers())

    println("creating benchmarks")
    dagger_benchmarks = array_suite(; log_sink=Dagger.LocalEventLog(), profile=true)
    serial_benchmarks = serial_suite()

    println("running Dagger benchmarks")
    dagger_res = run(dagger_benchmarks; samples=5, seconds=10*60, gcsample=true)

    println("running Serial benchmarks")
    serial_res = run(serial_benchmarks; samples=5, seconds=10*60, gcsample=true)

    res = Dict("Dagger"=>dagger_res, "Serial"=>serial_res)

    #= TODO: Analyze SoL vs. Dagger
    @info "Dagger"
    res = run(suite)
    for key in keys(res)
        dagger_elapsed = mean(res[key].times) / (10^9)
        expected_seconds = (theory_flops(nrow, ncol, nfeatures) / pf)
        @show key dagger_elapsed (dagger_elaped/expected_seconds)
    end
    expected_seconds = (theory_flops(nrow, ncol, nfeatures) / pf)

    #@info "Dagger (scaled by $scale)"
    #dagger_elapsed = @elapsed compute(ctx, T)
    #@show dagger_elapsed (dagger_elapsed/expected_seconds)

    =#

    output = "result-$(np)workers-$(nt)threads-$(Dates.now()).jld"
    println("saving results in $output")
    JLD.save(output, "results", res, "peakflops", peakflops(), "renders", RENDERS)
    println("Done.")

    if length(ARGS) == 1
        compare_file = ARGS[1]
        println("Comparing with results in $compare_file")
        reference_results = JLD.load(compare_file, "result")
        @show judge(mean(reference_results), mean(res))
    end
end

main()
