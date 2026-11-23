# METG (minimum effective task granularity), measured the way Task Bench does.
#
#   Slaughter et al., "Task Bench: A Parameterized Benchmark for Evaluating
#   Parallel Runtime Performance", SC20. https://github.com/StanfordLegion/task-bench
#
# Runs the "metg" suite (suites/metg.jl) through the orchestrator (benchmarks.jl)
# and derives from its trials what Task Bench's scripts/chart_metg.py derives
# from a log. Everything benchmarks.jl documents applies: BENCHMARK_PROCS and
# BENCHMARK_MPI_RANKS select the processes, BENCHMARK_METG_* the task graph.
# The environment needs Dagger, BenchmarkTools and JSON3 (and MPI for
# BENCHMARK_MPI_RANKS):
#
#     julia -t 16 --project=<env> benchmark/metg.jl
#     BENCHMARK_MPI_RANKS=4 julia -t 4 --project=<env> benchmark/metg.jl
#
# The analysis below needs none of them; `julia benchmark/metg_tests.jl` checks
# it against values computed by chart_metg.py.
#
# The suite runs a Task Bench task graph once per kernel iteration count `iter`
# of the sweep. For each `iter`, from the mean elapsed time of its samples:
#
#     task granularity = elapsed * nodes * cores / tasks
#     efficiency       = (FLOPs / elapsed / nodes) / peak FLOP/s per node
#
# METG(50%) is the smallest task granularity whose efficiency is at least 50%,
# linearly interpolated towards the next smaller `iter`.
#
# Peak FLOP/s
# -----------
# Task Bench divides by a constant of the machine: the best FLOP/s any system
# reached on it with large tasks. To compare with other systems on the same
# machine, or with published Task Bench results, measure that constant with Task
# Bench itself and pass it as BENCHMARK_METG_PEAK_FLOPS. Otherwise it is taken
# from the suite's own reference run, a long kernel task on every processor at
# once: the fastest sample gives the FLOP/s of one processor with the runtime's
# cost negligible, which is counted once per core.
#
# Cores
# -----
# Task Bench charges a system for every core of the node, including cores the
# runtime keeps for itself: its Legion runs execute tasks on all but two cores
# of a node and are still divided by the node's full peak. The default here
# charges for the processors tasks ran on. If Dagger was given fewer threads
# than the node has cores, set BENCHMARK_METG_CORES to the node's core count for
# Task Bench's accounting.
#
# Differences from Task Bench's runs
# ----------------------------------
# - Sampling is the orchestrator's: an unrecorded warm-up run, then up to
#   BENCHMARK_SAMPLES samples within BENCHMARK_SECONDS, instead of min(s + 1, 5)
#   repetitions of the s-th `iter`. The warm-up is Task Bench's practice too:
#   its MPI, Legion and HPX implementations run the graph once before the run
#   they time. A full sweep takes a while: the largest tasks run for minutes per
#   sample at 1000 timesteps, so under MPI raise BENCHMARK_PROC_TIMEOUT, which
#   bounds the whole run there.
# - chart_metg.py also rejects a log whose repetition count drops at the
#   threshold (a sign of a truncated batch job). Time-budgeted sampling can do
#   that legitimately, so the check is not applied here.
#
# Environment variables of the analysis (those of the suite are in benchmarks.jl)
# - BENCHMARK_METG_NODES: Number of nodes the processors are spread over.
#   Defaults to "1".
# - BENCHMARK_METG_CORES: Cores per node. Defaults to processors / nodes.
# - BENCHMARK_METG_PEAK_FLOPS: Peak FLOP/s *per node*, or "sweep" for
#   chart_metg.py's fallback: efficiency relative to the largest `iter` of the
#   sweep itself. Defaults to the reference run described above.
# - BENCHMARK_METG_THRESHOLD: Efficiency threshold. Defaults to "0.5".
# - BENCHMARK_METG_LOGDIR: When set, every sweep is also written there as a log
#   in Task Bench's report format, named as its chart scripts expect
#   (dagger_ngraphs_<n>_type_<type>_nodes_<nodes>.log), for charting next to other
#   systems. The report then gives the scripts/chart_metg.py command that
#   repeats this analysis on the log.

using Statistics

# FLOPs of one compute-bound task (`count_flops_per_task` in Task Bench's
# core/core.cc). Keep in step with `tb_kernel_compute` in suites/metg.jl.
metg_flops_per_task(iterations) = 2 * 64 * iterations + 64

"""
    metg_table(points; tasks, nodes, cores, peak_flops)

One row per swept iteration count, largest first, with the columns
chart_metg.py computes. `points` holds `(; iterations, elapsed)`, `elapsed`
being the seconds of every sample of a graph of `tasks` tasks. `peak_flops` is
per node; `nothing` selects the efficiency relative to the first row.
"""
function metg_table(points; tasks, nodes, cores, peak_flops)
    rows = map(sort(points; by=p -> p.iterations, rev=true)) do p
        (; p.iterations, reps=length(p.elapsed), elapsed=mean(p.elapsed),
           std=std(p.elapsed; corrected=false), flops=tasks * metg_flops_per_task(p.iterations))
    end
    return map(rows) do r
        efficiency = if peak_flops === nothing
            rows[1].elapsed / (r.elapsed * (rows[1].iterations / r.iterations))
        else
            r.flops / r.elapsed / nodes / peak_flops
        end
        (; r..., time_per_task=r.elapsed / tasks * nodes * cores,
           flops_per_second=r.flops / r.elapsed, efficiency)
    end
end

"""
    metg_value(table, threshold)

The minimum effective task granularity in seconds, as chart_metg.py determines
it; throws an `ErrorException` carrying chart_metg.py's message when the sweep
does not determine one.
"""
function metg_value(table, threshold)
    efficiency = [r.efficiency for r in table]
    granularity = [r.time_per_task for r in table]
    any(>=(threshold), efficiency) || error("no data above threshold, was run properly configured?")
    any(<(threshold), efficiency) || error("no data below threshold, maybe run was truncated?")
    all(<(1.5), efficiency) || error("suspiciously high efficiency over 150%, was run timed correctly?")

    # Smallest task granularity above the efficiency threshold
    above = findall(>=(threshold), efficiency)
    i = above[argmin(granularity[above])]
    i < length(table) || error("no data following the point above the threshold")

    # Linear interpolation if the subsequent data point is an improvement
    metg = granularity[i]
    if granularity[i + 1] < metg
        metg = granularity[i + 1] + (threshold - efficiency[i + 1]) *
            (granularity[i] - granularity[i + 1]) / (efficiency[i] - efficiency[i + 1])
    end
    return metg
end

metg_sig(x) = string(round(x; sigdigits=4))
function metg_time(seconds)
    for (unit, scale) in (("s", 1.0), ("ms", 1e-3), ("μs", 1e-6))
        seconds >= scale && return string(round(seconds / scale; sigdigits=4), " ", unit)
    end
    return string(round(seconds / 1e-9; sigdigits=4), " ns")
end

function metg_print_table(io::IO, table)
    header = ("iter", "reps", "elapsed (s)", "FLOP/s", "granularity", "efficiency")
    rows = [("2^$(trailing_zeros(r.iterations))", string(r.reps), metg_sig(r.elapsed), metg_sig(r.flops_per_second),
             metg_time(r.time_per_task), string(round(100 * r.efficiency; digits=1), "%")) for r in table]
    widths = [maximum(length, (header[c], (row[c] for row in rows)...)) for c in eachindex(header)]
    for row in (header, rows...)
        println(io, "    ", join((lpad(row[c], widths[c]) for c in eachindex(header)), "  "))
    end
end

# A sweep in Task Bench's report format, one report per sample, with the lines
# chart_metg.py reads: the configuration of every graph, then the totals
function metg_write_log(path, sweep, points)
    tasks = sweep.graphs * sweep.steps * sweep.width
    open(path, "w") do io
        for p in sort(points; by=p -> p.iterations, rev=true), elapsed in p.elapsed
            flops = tasks * metg_flops_per_task(p.iterations)
            print(io, """
                Running Task Benchmark
                  Configuration:
                """)
            for graph in 1:sweep.graphs
                print(io, """
                        Task Graph $graph:
                          Time Steps: $(sweep.steps)
                          Max Width: $(sweep.width)
                          Dependence Type: $(sweep.dependence)
                          Kernel:
                            Type: compute_bound
                            Iterations: $(p.iterations)
                          Output Bytes: 16
                    """)
            end
            print(io, """
                Total Tasks $tasks
                Total FLOPs $flops
                Total Bytes 0
                Elapsed Time $elapsed seconds
                FLOP/s $(flops / elapsed)
                B/s 0.0
                """)
        end
    end
    return path
end

# Group key of a sweep, as suites/metg.jl `metg_label` writes it
const METG_SWEEP_KEY = r"^(\w+?)(?: radix=(\d+))?(?: graphs=(\d+))? steps=(\d+) width=(\d+) (pinned|unscoped)( bulk)?$"

"""
    metg_report(io, leaves; nodes=1, cores=nothing, peak=nothing, threshold=0.5, logdir=nothing)

Prints the efficiency curve and METG of every metg sweep among `leaves`, pairs
of a benchmark's key path and its sample times in nanoseconds, and returns them.
"""
function metg_report(io::IO, leaves; nodes::Int=1, cores=nothing, peak=nothing, threshold::Float64=0.5, logdir=nothing)
    references = Dict{Tuple{String,String},Any}()
    sweeps = Dict{NTuple{3,String},Vector{Any}}()
    for (keypath, times) in leaves
        length(keypath) == 5 && keypath[1] == "metg" || continue
        m = match(r"^iter=2\^(\d+)$", keypath[5])
        m === nothing && continue
        point = (; iterations=1 << parse(Int, m[1]), elapsed=times ./ 1e9)
        if keypath[4] == "peak"
            references[(keypath[2], keypath[3])] = point
        else
            push!(get!(sweeps, (keypath[2], keypath[3], keypath[4]), Any[]), point)
        end
    end
    isempty(sweeps) && error("No metg sweep among the results (is `metg` in BENCHMARK?)")

    results = Any[]
    for key in sort!(collect(keys(sweeps)))
        method, layout, label = key
        points = sweeps[key]
        m = match(METG_SWEEP_KEY, label)
        procs = match(r"procs=(\d+)", layout)
        (m === nothing || procs === nothing) && error("Unrecognized metg benchmark group: $(join(key, " / "))")
        sweep = (; dependence=String(m[1]), radix=m[2], graphs=m[3] === nothing ? 1 : parse(Int, m[3]),
                   steps=parse(Int, m[4]), width=parse(Int, m[5]), placement=String(m[6]), bulk=m[7] !== nothing)
        tasks = sweep.graphs * sweep.steps * sweep.width
        ncores = cores === nothing ? parse(Int, procs[1]) ÷ nodes : cores
        ncores >= 1 || error("BENCHMARK_METG_NODES ($nodes) exceeds the $(procs[1]) processors of $(join(key, " / "))")

        reference = get(references, (method, layout), nothing)
        peak_flops, peak_source = if peak isa Real
            Float64(peak), "BENCHMARK_METG_PEAK_FLOPS"
        elseif peak === :sweep || reference === nothing
            nothing, nothing
        else
            rate = metg_flops_per_task(reference.iterations) / minimum(reference.elapsed)
            ncores * rate, "one kernel task per processor: $(metg_sig(rate)) FLOP/s each, counted for $ncores cores"
        end

        table = metg_table(points; tasks, nodes, cores=ncores, peak_flops)
        println(io)
        println(io, "metg / ", join(key, " / "))
        println(io, "  $tasks tasks per run", sweep.graphs == 1 ? "" : " in $(sweep.graphs) graphs",
                ", charged to $nodes node(s) x $ncores cores")
        if peak_flops === nothing
            println(io, "  efficiency relative to the largest tasks of the sweep",
                    peak === :sweep ? "" : " (no reference run among the results)")
        else
            println(io, "  peak $(metg_sig(peak_flops)) FLOP/s per node ($peak_source)")
        end
        println(io)
        metg_print_table(io, table)
        println(io)
        percent = let p = round(100 * threshold; digits=6)
            isinteger(p) ? string(Int(p)) : string(p)
        end
        metg = try
            metg_value(table, threshold)
        catch err
            err isa ErrorException || rethrow()
            println(io, "  METG($percent%) not determined: ", err.msg)
            nothing
        end
        metg === nothing || println(io, "  METG($percent%) = ", metg_time(metg))

        logpath = nothing
        if logdir !== nothing
            name = "dagger" * (sweep.placement == "pinned" ? "" : "_$(sweep.placement)") * (sweep.bulk ? "_bulk" : "") *
                "_ngraphs_$(sweep.graphs)_type_$(sweep.dependence)" * (sweep.radix === nothing ? "" : "_radix_$(sweep.radix)") *
                "_nodes_$nodes.log"
            logpath = metg_write_log(joinpath(mkpath(logdir), name), sweep, points)
            println(io, "  Task Bench log: $logpath")
            println(io, "    scripts/chart_metg.py -g $(sweep.graphs) -n $nodes -c $ncores -t $threshold",
                    peak_flops === nothing ? "" : " --peak-compute-bandwidth $peak_flops", " $logpath")
        end
        push!(results, (; method, layout, label, sweep, nodes, cores=ncores, peak_flops, table, metg, logpath))
    end
    return results
end

function metg_config()
    env(name) = get(ENV, "BENCHMARK_METG_" * name, "")
    nodes = isempty(env("NODES")) ? 1 : parse(Int, env("NODES"))
    cores = isempty(env("CORES")) ? nothing : parse(Int, env("CORES"))
    peak = isempty(env("PEAK_FLOPS")) ? nothing : env("PEAK_FLOPS") == "sweep" ? :sweep : parse(Float64, env("PEAK_FLOPS"))
    threshold = isempty(env("THRESHOLD")) ? 0.5 : parse(Float64, env("THRESHOLD"))
    logdir = isempty(env("LOGDIR")) ? nothing : env("LOGDIR")
    nodes >= 1 || error("BENCHMARK_METG_NODES must be positive")
    cores === nothing || cores >= 1 || error("BENCHMARK_METG_CORES must be positive")
    peak isa Real && !(peak > 0) && error("BENCHMARK_METG_PEAK_FLOPS must be positive")
    0 < threshold < 1.5 || error("BENCHMARK_METG_THRESHOLD is an efficiency, e.g. 0.5")
    return (; nodes, cores, peak, threshold, logdir)
end

# `SUITE` is the orchestrator's: every leaf wraps the trial measured out-of-process
metg_report_suite(config) =
    metg_report(stdout, [(String[string(k) for k in keypath], bench.trial.times)
                         for (keypath, bench) in BenchmarkTools.leaves(SUITE)]; config...)

function metg_main()
    # Read the analysis' settings before the run, not after it
    config = metg_config()
    get!(ENV, "BENCHMARK", "metg:dagger")
    # Runs the suites out-of-process and defines `SUITE`
    include(joinpath(@__DIR__, "benchmarks.jl"))
    Base.invokelatest(metg_report_suite, config)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    metg_main()
end
