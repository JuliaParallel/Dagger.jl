usage = """
Dagger.jl Benchmarking Platform

This benchmarking platform is intended to test Dagger's performance on a wide range of benchmarks, and to determine where Dagger falls short on delivering near-optimal performance.

This script is configured with environment variables. For example:

BENCHMARK=nmf:raw,dagger+cuda julia benchmark.jl

The above invocation will run the NMF suite without Dagger on the CPU, and with Dagger on CUDA GPUs.

Environment Variables:
- BENCHMARK - Specifies the suites to benchmark, the execution method, and which acceleration mechanisms to use. The format is: "suite1:method1+accel1+accel2,method2+accel1,method3;suite2:method1,method2+accel1". Available execution and acceleration methods are described below.
- BENCHMARK_PROCS - Specifies the number of workers and threads to start. The format is: "numprocs:numthreads", which will start `numprocs` workers with `numthreads` threads each. Defaults to just the Julia process running this script, with however many threads it has available.
- BENCHMARK_REMOTES - Specifies the remote hosts to connect to, on which workers will be started. The format is the same as accepted by `Distributed.addprocs`.
- BENCHMARK_SCALE - The scaling factor to use for all suites, specified as a `UnitRange` or other Julia expression. Experimental, and subject to future removal.
- BENCHMARK_OUTPUT_FORMAT - Which output format to write results as. May be "none" for no output writing, "jls" for Julia's native `Serialization` format, or "jld", for the HDF5 format written by JLD.jl.
- BENCHMARK_VISUALIZE - Whether to run the `visualize.jl` script on the output results. May be any value that can parse as a `Bool`.
- BENCHMARK_RENDER - Which rendering mode to use. May be "live" to use the old (and soon to be removed) web renderer, "webdash" to use the DaggerWebDash renderer, or "offline" to use the old (and soon to be removed) offline renderer. The default of "" disables rendering.
- BENCHMARK_LIVE_PORT - Which port to use for web rendering. Defaults to port 8000.
- BENCHMARK_GRAPH - Whether to use dotviz graph rendering. Only useable if using "live" or "offline" rendering methods. Defaults to off, and may be any value that can parse as a `Bool`.
- BENCHMARK_PROFILE - Whether to enable real-time profiling. Defaults to off, and may be any value with parses as a `Bool`. Currently experimental and very, very slow.
- BENCHMARK_SAVE_LOGS - Whether to save logs collected at runtime to the output file. Defaults to off, and may be any value that can parse as a `Bool`.

Execution Methods:
- "raw" - Non-Dagger execution
- "dagger" - Dagger execution

Acceleration Methods:
- "cuda" - CUDA GPU acceleration
- "amdgpu" - AMD GPU acceleration
"""

if !haskey(ENV, "BENCHMARK")
    print(stderr, usage)
    exit(1)
end

const benches = Dict{String,Vector}()
const suites = Set{String}()
const accelerations = Set{String}()
for bench_spec in split(ENV["BENCHMARK"], ';')
    suite, bench_spec_methods = split(bench_spec, ':')
    if !isfile(joinpath(@__DIR__, "suites", suite*".jl"))
        error("Unknown benchmark suite: $suite")
    end
    push!(suites, suite)
    for method_spec in split(bench_spec_methods, ',')
        method, accels... = split(method_spec, '+')
        for accel in accels
            push!(accelerations, accel)
        end
        accels = String.(accels)
        _benches = get!(benches, suite, [])
        push!(_benches, (;method, accels))
    end
end

using Distributed
if haskey(ENV, "BENCHMARK_PROCS")
    const np, nt = parse.(Ref(Int), split(ENV["BENCHMARK_PROCS"], ":"))
    for i in workers()
        addprocs(np; exeflags="-t $nt")
    end
else
    const np = 1
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
@everywhere begin
    using Pkg
    Pkg.instantiate()
end
@everywhere using Dagger
import Dagger: Computation, reduceblock
using Dates, Random, Statistics, LinearAlgebra, InteractiveUtils

for accel in accelerations
    if accel == "cuda"
        @everywhere using DaggerGPU, CUDA
    elseif accel == "amdgpu"
        @everywhere using DaggerGPU, AMDGPU
    else
        error("Unknown acceleration: $accel")
    end
end

const scales = eval(Meta.parse(get(ENV, "BENCHMARK_SCALE", "1:5:50")))

const output_format = get(ENV, "BENCHMARK_OUTPUT_FORMAT", "none")
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
elseif render == "webdash"
    const live = true
    using DaggerWebDash
    import DaggerWebDash: LinePlot, GanttPlot, GraphPlot, ProfileViewer
    using DataFrames
elseif render == "offline"
    const live = false
    using Luxor, ProfileSVG
    using FFMPEG, FileIO, ImageMagick
end
const RENDERS = Dict{Int,Dict}()
const live_port = parse(Int, get(ENV, "BENCHMARK_LIVE_PORT", "8000"))

const graph = parse(Bool, get(ENV, "BENCHMARK_GRAPH", "0"))
if graph && render == "webdash"
    @warn "BENCHMARK_GRAPH=1 is not compatible with BENCHMARK_RENDER=webdash; disabling graphing"
end
const profile = parse(Bool, get(ENV, "BENCHMARK_PROFILE", "0"))
const savelogs = if parse(Bool, get(ENV, "BENCHMARK_SAVE_LOGS", "0"))
    if render == "live " || render == "offline"
        @warn "BENCHMARK_SAVE_LOGS=1 is incompatible with BENCHMARK_RENDER=live; disabling log saving"
        false
    else
        using DataFrames
        true
    end
else
    false
end

using BenchmarkTools

suite_setup = Dict{String,Function}()
for suite in suites
    suite_setup[suite] = include(joinpath(@__DIR__, "suites", suite*".jl"))
end

function main()
    nw = length(workers())
    output_prefix = "result-$(np)workers-$(nt)threads-$(Dates.now())"

    suite_trees = Dict()
    opts = (;profile=profile)
    if render == "live"
        opts = merge(opts, (;log_sink=Dagger.LocalEventLog()))
        if graph
            opts = merge(opts, (;log_file=output_prefix*".dot"))
        end
    elseif render == "webdash" || savelogs
        ml = Dagger.MultiEventLog()
        ml[:core] = Dagger.Events.CoreMetrics()
        ml[:id] = Dagger.Events.IDMetrics()
        # FIXME: ml[:timeline] = Dagger.Events.TimelineMetrics()
        profile && (ml[:profile] = DaggerWebDash.ProfileMetrics())
        ml[:wsat] = Dagger.Events.WorkerSaturation()
        ml[:loadavg] = Dagger.Events.CPULoadAverages()
        ml[:bytes] = Dagger.Events.BytesAllocd()
        ml[:mem] = Dagger.Events.MemoryFree()
        ml[:esat] = Dagger.Events.EventSaturation()
        ml[:psat] = Dagger.Events.ProcessorSaturation()
        lw = Dagger.Events.LogWindow(5*10^9, :core)
        logs_df = DataFrame([key=>[] for key in keys(ml.consumers)]...)
        ts = Dagger.Events.TableStorage(logs_df)
        push!(lw.creation_handlers, ts)
        if render == "webdash"
            d3r = DaggerWebDash.D3Renderer(live_port; seek_store=ts)
            push!(lw.creation_handlers, d3r)
            push!(lw.deletion_handlers, d3r)
            push!(d3r, GanttPlot(:core, :id, :timeline, :esat, :psat, "Overview"))
            # TODO: push!(d3r, ProfileViewer(:core, :profile, "Profile Viewer"))
            push!(d3r, LinePlot(:core, :wsat, "Worker Saturation", "Running Tasks"))
            push!(d3r, LinePlot(:core, :loadavg, "CPU Load Average", "Average Running Threads"))
            push!(d3r, LinePlot(:core, :bytes, "Allocated Bytes", "Bytes"))
            push!(d3r, LinePlot(:core, :mem, "Available Memory", "% Free"))
            #push!(d3r, GraphPlot(:core, :id, :timeline, :profile, "DAG"))
            ml.aggregators[:d3r] = d3r
        end
        ml.aggregators[:logwindow] = lw
        opts = merge(opts, (;log_sink=ml))
    end
    ctx = Context(collect(1:nw); opts...)
    Dagger.Sch.EAGER_CONTEXT[] = ctx
    for suite in keys(benches)
        for bench in benches[suite]
            name = "suite $suite, exec $(bench.method), accels $(bench.accels)"
            println("creating benchmarks for $name")
            suite_trees[name] = suite_setup[suite](ctx; method=bench.method, accels=bench.accels)
        end
    end
    if render == "live" || render == "offline"
        Dagger.show_gantt(ctx; width=1800, window_length=5, delay=2, port=live_port, live=live)
        if live
            # Make sure server code is compiled
            sleep(1)
            run(pipeline(`curl -s localhost:$live_port/`; stdout=devnull))
            if profile
                run(pipeline(`curl -s localhost:$live_port/profile`; stdout=devnull))
            end
            @info "Rendering started on port $live_port"
        end
    elseif render == "webdash"
        # Kick the webserver into gear
        collect(ctx, delayed(identity)(1))
        run(pipeline(`curl -s localhost:$live_port/index.html`; stdout=devnull))
        @info "Rendering started on port $live_port"
    end
    res = Dict{String,Any}()
    for name in keys(suite_trees)
        println("running benchmarks for $name")
        res[name] = try
            run(suite_trees[name]; samples=3, seconds=10*60, gcsample=true)
        catch err
            @error "Error running benchmarks for $name" exception=(err,catch_backtrace())
            nothing
        end
    end
    for name in sort(collect(keys(suite_trees)))
        if res[name] !== nothing
            println("benchmark results for $name: $(minimum(res[name]))")
        end
    end

    if output_format != "none"
        println("saving results in $output_prefix.$output_format")
        if output_format == "jld"
            JLD.save(output, "results", res, "peakflops", peakflops(), "renders", RENDERS, "logs", savelogs ? logs_df : nothing)
        elseif output_format == "jls"
            outdict = Dict("results"=>res, "peakflops"=>peakflops(), "renders"=>RENDERS, "logs"=>savelogs ? logs_df : nothing)
            open(output_prefix*".jls", "w") do io
                serialize(io, outdict)
            end
        end

        if parse(Bool, get(ENV, "BENCHMARK_VISUALIZE", "0"))
            run(`$(Base.julia_cmd()) $(joinpath(@__DIR__, "visualize.jl")) -- $(output_prefix*"."*output_format)`)
        end

        # TODO: Compare with multiple results
        if length(ARGS) == 1
            compare_file = ARGS[1]
            println("comparing with results in $compare_file")
            reference_results = JLD.load(compare_file, "result")
            @show judge(mean(reference_results), mean(res))
        end
    end

    println("Done!")
end

main()
