# Benchmark CI driver: compares the current checkout against `master` using
# AirspeedVelocity, prints a results table, renders comparison plots, and exits
# non-zero if any benchmark regresses by more than a threshold (default 25%).
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
#   (default "0.25", i.e. 25%).
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
const THRESHOLD = parse(Float64, get(ENV, "BENCHMARK_REGRESSION_THRESHOLD", "0.25"))
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

regressions = Tuple{String,Float64}[]
improvements = Tuple{String,Float64}[]
for (name, stats) in cur
    name == "time_to_load" && continue  # too noisy to gate on
    haskey(base, name) || continue
    bm = get(base[name], "median", nothing)
    cm = get(stats, "median", nothing)
    (bm === nothing || cm === nothing || bm == 0) && continue
    ratio = cm / bm
    if ratio > 1 + THRESHOLD
        push!(regressions, (name, ratio))
    elseif ratio < 1 - THRESHOLD
        push!(improvements, (name, ratio))
    end
end
sort!(regressions; by = last, rev = true)
sort!(improvements; by = last)

pct(r) = string(round((r - 1) * 100; digits = 1), "%")

# --- Markdown report (for the Buildkite annotation / optional PR comment) ---

open(joinpath(OUTPUT_DIR, "report.md"), "w") do io
    println(io, "### Dagger benchmarks: `$CUR_REV` vs `$BASE_REV`")
    println(io)
    println(io, table)
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
        println(io, "No regressions beyond ", pct(1 + THRESHOLD), " 🎉")
    else
        println(io, "#### ⚠️ Regressions (> ", pct(1 + THRESHOLD), ")")
        println(io)
        for (name, r) in regressions
            println(io, "- `", name, "`: +", pct(r))
        end
    end
    if !isempty(improvements)
        println(io)
        println(io, "#### Improvements (> ", pct(1 + THRESHOLD), " faster)")
        println(io)
        for (name, r) in improvements
            println(io, "- `", name, "`: ", pct(r))
        end
    end
end

# --- Summary + exit status -------------------------------------------------

if isempty(regressions)
    println("\nNo benchmarks regressed by more than $(round(THRESHOLD * 100))%.")
    exit(0)
else
    println("\n$(length(regressions)) benchmark(s) regressed by more than $(round(THRESHOLD * 100))%:")
    for (name, r) in regressions
        println("  - $name: +$(pct(r))")
    end
    exit(1)
end
