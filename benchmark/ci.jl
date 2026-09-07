# Benchmark CI driver: compares the current checkout against `master` using
# AirspeedVelocity, prints a results table, renders comparison plots, and exits
# non-zero if any benchmark regresses by more than a threshold (default 10%).
#
# Intended to be invoked from `.buildkite/run_benchmarks.sh`, but can also be run
# locally:
#
#     julia benchmark/ci.jl
#
# Configuration (environment variables; see benchmark/benchmarks.jl for the rest
# of the BENCHMARK_* knobs which are forwarded to the benchmark runs):
# - BENCHMARK_BASE_REV: revision to compare against (default "master").
# - BENCHMARK_REGRESSION_THRESHOLD: fractional slowdown that fails CI
#   (default "0.10", i.e. 10%).
# - BENCHMARK_ALLOC_REGRESSION_THRESHOLD: fractional increase in allocation
#   count or allocated bytes that fails CI (default "0.25", i.e. 25%).
#   Allocation counts are near-deterministic, but Dagger's steady state has
#   some scheduling-dependent variance, so this is looser than the time gate
#   while still catching any real allocation regression.
# - BENCHMARK_NOISE_TOLERANCE: multiple of the reported timing spread that a
#   change must clear before it is called a regression or an improvement
#   (default "1.0"). See "Regression check" below; set to "0" to disable the
#   significance gate and report purely on the thresholds.
# - BENCHMARK_CI_THREADS: Julia threads for each benchmark run (default "4").
# - BENCHMARK_OUTPUT_DIR: where JSON/plots/report are written
#   (default "benchmark_results").

using Pkg
Pkg.activate(; temp=true)
# PlotlyKaleido is a transitive dep of AirspeedVelocity, but we need it as a
# direct dep here to render the comparison plots.
Pkg.add(["AirspeedVelocity", "PlotlyKaleido"])

using AirspeedVelocity
using AirspeedVelocity.Utils: benchmark, load_results
using AirspeedVelocity.TableUtils: create_table

const PROJECT_DIR = abspath(joinpath(@__DIR__, ".."))
const SCRIPT = joinpath(@__DIR__, "benchmarks.jl")
const BASE_REV = get(ENV, "BENCHMARK_BASE_REV", "master")
const CUR_REV = "dirty"  # the working-tree checkout (this PR/commit)
const THRESHOLD = parse(Float64, get(ENV, "BENCHMARK_REGRESSION_THRESHOLD", "0.10"))
const ALLOC_THRESHOLD = parse(Float64, get(ENV, "BENCHMARK_ALLOC_REGRESSION_THRESHOLD", "0.25"))
const NOISE_TOLERANCE = parse(Float64, get(ENV, "BENCHMARK_NOISE_TOLERANCE", "1.0"))
const CI_THREADS = get(ENV, "BENCHMARK_CI_THREADS", "4")
const OUTPUT_DIR = abspath(get(ENV, "BENCHMARK_OUTPUT_DIR", "benchmark_results"))

# Extra packages the benchmark suites need on top of Dagger + BenchmarkTools
# (these mirror benchmark/Project.toml for the default suites; they must be
# listed explicitly because we pass an explicit `--script`).
const EXTRA_PKGS = String[
    "Krylov", "SparseArrays", "LinearAlgebra", "Statistics",
    "Dates", "Random", "Distributed", "InteractiveUtils",
    # JSON3 is only a *weakdep* of Dagger, so it is not installed automatically
    # with Dagger; the orchestrator/worker need it for their file-based IPC.
    #"JSON3",
]
# MPI is only needed (and only installed) when the caller requests an MPI
# benchmark run (see benchmark/benchmarks.jl's BENCHMARK_MPI_RANKS), so the
# plain/Distributed CI runs don't pay for pulling in MPICH_jll.
if get(ENV, "BENCHMARK_MPI_RANKS", "0") != "0"
    push!(EXTRA_PKGS, "MPI")
end

mkpath(OUTPUT_DIR)

@info "Benchmarking $CUR_REV vs $BASE_REV" project = PROJECT_DIR script = SCRIPT

benchmark(
    "Dagger",
    [BASE_REV, CUR_REV];
    output_dir = OUTPUT_DIR,
    script = SCRIPT,
    path = PROJECT_DIR,
    extra_pkgs = EXTRA_PKGS,
    exeflags = `-t $CI_THREADS`,
    tune = false,
)

combined = load_results("Dagger", [BASE_REV, CUR_REV]; input_dir = OUTPUT_DIR)

# --- Results table ---------------------------------------------------------

table = create_table(combined; key = "median", add_ratio_col = true)
println("\nBenchmark results (median time):\n")
println(table)

# Allocations/memory table (the ratio column compares allocated bytes). Each
# BenchmarkTools trial records the allocation count and bytes of a single
# post-warmup evaluation, so these are near-deterministic per revision.
alloc_table = create_table(combined; key = "memory", add_ratio_col = true)
println("\nBenchmark results (allocations / memory):\n")
println(alloc_table)

# --- Comparison plots (best effort) ----------------------------------------

plot_files = String[]
try
    using AirspeedVelocity.PlotUtils: combined_plots
    using PlotlyKaleido: savefig, start
    plots = combined_plots(combined; npart = 10)
    start()
    for (i, p) in enumerate(plots)
        fname = joinpath(OUTPUT_DIR, "plot_Dagger_$i.png")
        savefig(p, fname; height = p.layout.height, width = p.layout.width)
        push!(plot_files, fname)
    end
    @info "Saved $(length(plot_files)) plot(s) to $OUTPUT_DIR"
catch err
    @warn "Plot generation failed; continuing without plots" exception = (err, catch_backtrace())
end

# --- Regression check ------------------------------------------------------

base = combined[BASE_REV]
cur = combined[CUR_REV]

# Each benchmark is gated on three metrics: median time, allocation count, and
# allocated bytes. Entries are (metric label, results key, threshold).
const METRICS = [
    ("time", "median", THRESHOLD),
    ("allocs", "allocs", ALLOC_THRESHOLD),
    ("memory", "memory", ALLOC_THRESHOLD),
]

# A change is only called a regression or an improvement when it is both *large*
# (past the metric's threshold) and *resolvable* (bigger than the run-to-run
# noise). Without the second condition a benchmark whose timing wanders by more
# than the threshold from run to run reports a regression or an improvement on
# essentially every build, which is how the benchmark job ends up red for no
# reason -- and, worse, trains everyone to ignore it.
#
# The noise model is the same one the results table prints, so the report can
# never contradict itself: timings are summarized as `median ± (q75 - q25)`, and
# a change counts only if those bands do not overlap. `BENCHMARK_NOISE_TOLERANCE`
# scales the band (0 disables the gate).
#
# Only timings carry a spread: each BenchmarkTools trial records the allocation
# count and byte count of a *single* post-warmup evaluation, so `allocs`/`memory`
# have no distribution to speak of and stay gated on their threshold alone.
"""
    noise_halfwidth(stats, key) -> Float64 or nothing

Half-width of the uncertainty band around `stats[key]`, or `nothing` when the
metric was not sampled repeatedly and so carries no spread information.
"""
function noise_halfwidth(stats, key)
    key == "median" || return nothing
    q25 = get(stats, "25", nothing)
    q75 = get(stats, "75", nothing)
    (q25 === nothing || q75 === nothing) && return nothing
    return NOISE_TOLERANCE * (q75 - q25)
end

regressions = Tuple{String,String,Float64}[]
improvements = Tuple{String,String,Float64}[]
within_noise = Tuple{String,String,Float64}[]
for (name, stats) in cur
    name == "time_to_load" && continue  # too noisy to gate on
    haskey(base, name) || continue
    for (label, key, threshold) in METRICS
        bm = get(base[name], key, nothing)
        cm = get(stats, key, nothing)
        (bm === nothing || cm === nothing || bm == 0) && continue
        ratio = cm / bm
        direction = if ratio > 1 + threshold
            :regression
        elseif ratio < 1 - threshold
            :improvement
        else
            continue
        end
        base_hw = noise_halfwidth(base[name], key)
        cur_hw = noise_halfwidth(stats, key)
        significant = if base_hw === nothing || cur_hw === nothing
            true  # no spread recorded: the threshold is all we have
        elseif direction === :regression
            cm - cur_hw > bm + base_hw
        else
            cm + cur_hw < bm - base_hw
        end
        if !significant
            push!(within_noise, (name, label, ratio))
        elseif direction === :regression
            push!(regressions, (name, label, ratio))
        else
            push!(improvements, (name, label, ratio))
        end
    end
end
sort!(regressions; by = last, rev = true)
sort!(improvements; by = last)
sort!(within_noise; by = last, rev = true)

pct(r) = string(round((r - 1) * 100; digits = 1), "%")

# --- Markdown report (for the Buildkite annotation / optional PR comment) ---

open(joinpath(OUTPUT_DIR, "report.md"), "w") do io
    println(io, "### Dagger benchmarks: `$CUR_REV` vs `$BASE_REV`")
    println(io)
    println(io, "#### Median time")
    println(io)
    println(io, table)
    println(io)
    println(io, "#### Allocations / memory")
    println(io)
    println(io, alloc_table)
    println(io)
    if !isempty(plot_files)
        println(io, "#### Plots")
        println(io)
        for f in plot_files
            # `artifact://` references render inline in Buildkite annotations.
            println(io, "![", basename(f), "](artifact://", relpath(f, dirname(OUTPUT_DIR)), ")")
        end
        println(io)
    end
    if isempty(regressions)
        println(io, "No time regressions beyond ", pct(1 + THRESHOLD),
                " or allocation regressions beyond ", pct(1 + ALLOC_THRESHOLD),
                " (timing changes inside the reported ±spread don't count) 🎉")
    else
        println(io, "#### ⚠️ Regressions (time > ", pct(1 + THRESHOLD),
                " and outside the reported ±spread; allocs/memory > ",
                pct(1 + ALLOC_THRESHOLD), ")")
        println(io)
        for (name, metric, r) in regressions
            println(io, "- `", name, "` (", metric, "): +", pct(r))
        end
    end
    if !isempty(improvements)
        println(io)
        println(io, "#### Improvements")
        println(io)
        for (name, metric, r) in improvements
            println(io, "- `", name, "` (", metric, "): ", pct(r))
        end
    end
    if !isempty(within_noise)
        # Listed, but deliberately not counted as either outcome: these cleared
        # their threshold while staying inside the run-to-run spread, so they
        # are drift to keep an eye on, not results.
        println(io)
        println(io, "<details><summary>Within noise (",
                length(within_noise), " metric(s) past threshold but inside the ",
                "±spread; not counted)</summary>")
        println(io)
        for (name, metric, r) in within_noise
            println(io, "- `", name, "` (", metric, "): ", pct(r))
        end
        println(io)
        println(io, "</details>")
    end
end

# --- Summary + exit status -------------------------------------------------

if !isempty(within_noise)
    println("\n$(length(within_noise)) metric(s) moved past their threshold but stayed within the measured spread (not counted):")
    for (name, metric, r) in within_noise
        println("  - $name ($metric): $(pct(r))")
    end
end

if isempty(regressions)
    println("\nNo benchmarks regressed (time > $(round(THRESHOLD * 100))%, allocs/memory > $(round(ALLOC_THRESHOLD * 100))%).")
    exit(0)
else
    println("\n$(length(regressions)) benchmark metric(s) regressed:")
    for (name, metric, r) in regressions
        println("  - $name ($metric): +$(pct(r))")
    end
    exit(1)
end
