using Distributed
if haskey(ENV, "BENCHMARK_PROCS")
    const np, nt = parse.(Ref(Int), split(ENV["BENCHMARK_PROCS"], ":"))
    for i in workers()
        addprocs(np; exeflags="-t $nt")
    end
else
    const np = length(workers())
    const nt = 1
end
if haskey(ENV, "BENCHMARK_REMOTES")
    const remotes = split(ENV["BENCHMARK_REMOTES"], ":")
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

const output_format = get(ENV, "BENCHMARK_OUTPUT_FORMAT", "jls")
if output_format == "jld"
    using JLD
elseif output_format == "jls"
    using Serialization
else
    error("Unknown output format: $output_format")
end

const render = get(ENV, "BENCHMARK_RENDER", "")
if render == "live"
    const live = true
    using Luxor, ProfileSVG
    using Mux
elseif render == "offline"
    const live = false
    using Luxor, ProfileSVG
    using FFMPEG, FileIO, ImageMagick
end
const RENDERS = Dict{Int,Dict}()

_benches = get(ENV, "BENCHMARK", "cpu,cpu+dagger")
const benches = []
for bench in split(_benches, ',')
    if endswith(bench, "+dagger")
        accel = split(bench, '+')[1]
        dagger = true
    else
        accel = bench
        dagger = false
    end
    push!(benches, (name=bench, accel=accel, dagger=dagger))
end

if any(x->x.accel == "cuda", benches)
    @everywhere using DaggerGPU, CUDA
elseif any(x->x.accel == "amdgpu", benches)
    @everywhere using DaggerGPU, AMDGPU
end

const scales = eval(Meta.parse(get(ENV, "BENCHMARK_SCALE", "1:5:50")))

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

function nmf_suite(; dagger, accel, kwargs...)
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

    X = Ref{Any}()
    W = Ref{Any}()
    H = Ref{Any}()

    for scale in scales
        ncol = 2001 * scale
        nrow = 1002
        nfeatures = 12

        if !dagger
            suite["NNMF scaled by: $scale"] = @benchmarkable begin
                nnmf($X[], $W[], $H[])
            end setup=begin
                if $accel == "cuda"
                    $X[] = CUDA.rand((Float32, $nrow, $ncol))
                    $W[] = CUDA.rand((Float32, $nrow, $nfeatures))
                    $H[] = CUDA.rand((Float32, $nfeatures, $ncol))
                elseif $accel == "amdgpu"
                    $X[] = ROCArray(rand(Float32, $nrow, $ncol))
                    $W[] = ROCArray(rand(Float32, $nrow, $nfeatures))
                    $H[] = ROCArray(rand(Float32, $nfeatures, $ncol))
                elseif $accel == "cpu"
                    $X[] = rand(Float32, $nrow, $ncol)
                    $W[] = rand(Float32, $nrow, $nfeatures)
                    $H[] = rand(Float32, $nfeatures, $ncol)
                end
            end teardown=begin
                $X[] = nothing
                $W[] = nothing
                $H[] = nothing
                @everywhere GC.gc()
            end
        else
            RENDERS[scale] = Dict{Int,Vector}()
            nw = length(workers())
            nsuite = BenchmarkGroup()
            while nw > 0
                opts = if accel == "cuda"
                    Dagger.Sch.SchedulerOptions(;proctypes=[
                        DaggerGPU.CuArrayDeviceProc
                    ])
                elseif accel == "amdgpu"
                    Dagger.Sch.SchedulerOptions(;proctypes=[
                        DaggerGPU.ROCArrayProc
                    ])
                elseif accel == "cpu"
                    Dagger.Sch.SchedulerOptions()
                end
                ctx = Context(collect((1:nw) .+ 1); kwargs...)
                p = sum([length(Dagger.get_processors(OSProc(id))) for id in 2:(nw+1)])
                nsuite["Workers: $nw"] = @benchmarkable begin
                    compute($ctx, nnmf($X[], $W[], $H[]); options=$opts)
                end setup=begin
                    _nw, _scale = $nw, $scale
                    @info "Starting $_nw worker NNMF (scale by $_scale)"
                    if render != ""
                        Dagger.show_gantt($ctx; width=1800, window_length=20, delay=2, port=4040, live=live)
                    end
                    if $accel == "cuda"
                        # FIXME: Allocate with CUDA.rand if possible
                        $X[] = Dagger.mapchunks(CUDA.cu, compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nrow, $ncol); options=$opts))
                        $W[] = Dagger.mapchunks(CUDA.cu, compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nrow, $nfeatures); options=$opts))
                        $H[] = Dagger.mapchunks(CUDA.cu, compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nfeatures, $ncol); options=$opts))
                    elseif $accel == "amdgpu"
                        $X[] = Dagger.mapchunks(ROCArray, compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nrow, $ncol); options=$opts))
                        $W[] = Dagger.mapchunks(ROCArray, compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nrow, $nfeatures); options=$opts))
                        $H[] = Dagger.mapchunks(ROCArray, compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nfeatures, $ncol); options=$opts))
                    elseif $accel == "cpu"
                        $X[] = compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nrow, $ncol); options=$opts)
                        $W[] = compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nrow, $nfeatures); options=$opts)
                        $H[] = compute(rand(Blocks($nrow, $ncol÷$p), Float32, $nfeatures, $ncol); options=$opts)
                    end
                end teardown=begin
                    if render != ""
                        Dagger.continue_rendering[] = false
                        video_paths = take!(Dagger.render_results)
                        try
                            video_data = Dict(key=>read(video_paths[key]) for key in keys(video_paths))
                            push!(get!(()->[], RENDERS[$scale], $nw), video_data)
                        catch
                        end
                    end
                    $X[] = nothing
                    $W[] = nothing
                    $H[] = nothing
                    @everywhere GC.gc()
                end
                break
                nw ÷= 2
            end
            suite["NNMF scaled by: $scale"] = nsuite
        end
    end

    suite
end

function main()
    nw = length(workers())
    output_prefix = "result-$(np)workers-$(nt)threads-$(Dates.now())"

    suites = Dict()
    for bench in benches
        name = bench.name
        println("creating $name benchmarks")
        suites[name] = if bench.dagger
            nmf_suite(; dagger=true, accel=bench.accel, log_sink=Dagger.LocalEventLog(), log_file=output_prefix*".dot", profile=false)
        else
            nmf_suite(; dagger=false, accel=bench.accel)
        end
    end
    res = Dict()
    for bench in benches
        name = bench.name
        println("running $name benchmarks")
        res[name] = try
            run(suites[name]; samples=5, seconds=10*60, gcsample=true)
        catch err
            @error "Error running $name benchmarks" exception=(err,catch_backtrace())
            nothing
        end
    end

    println("saving results in $output_prefix.$output_format")
    if output_format == "jld"
        JLD.save(output, "results", res, "peakflops", peakflops(), "renders", RENDERS)
    elseif output_format == "jls"
        outdict = Dict("results"=>res, "peakflops"=>peakflops(), "renders"=>RENDERS)
        open(output_prefix*".jls", "w") do io
            serialize(io, outdict)
        end
    end
    println("Done.")

    # TODO: Compare with multiple results
    if length(ARGS) == 1
        compare_file = ARGS[1]
        println("Comparing with results in $compare_file")
        reference_results = JLD.load(compare_file, "result")
        @show judge(mean(reference_results), mean(res))
    end
end

main()
