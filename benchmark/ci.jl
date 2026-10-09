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
#   These are process-local minima across timed evaluations. Placement and
#   incomplete warmup can affect them, so investigate flags with repeated,
#   warmed runs rather than assuming they describe global allocation growth.
# - BENCHMARK_NOISE_TOLERANCE: multiple of the reported timing spread that a
#   change must clear before it is called a regression or an improvement
#   (default "1.0"). See "Regression check" below; set to "0" to disable the
#   spread gate; the five-sample and confirmation requirements still apply.
# - BENCHMARK_CI_THREADS: Julia threads for each benchmark run (default "4").
# - BENCHMARK_OUTPUT_DIR: where JSON/plots/report are written
#   (default "benchmark_results").

using Pkg
Pkg.activate(; temp=true)
# PlotlyKaleido is a transitive dep of AirspeedVelocity, but we need it as a
# direct dep here to render the comparison plots.
Pkg.add(["AirspeedVelocity", "PlotlyKaleido", "JSON3"])

using AirspeedVelocity
using JSON3
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

# Install the requested extension package in both Airspeed revision environments.
const gpu_packages = Dict("cuda" => "CUDA", "amdgpu" => "AMDGPU",
                          "oneapi" => "oneAPI", "metal" => "Metal", "opencl" => "OpenCL")
for spec in split(get(ENV, "BENCHMARK", ""), ';'), method in split(last(split(spec, ':')), ',')
    for accel in split(method, '+')[2:end]
        push!(EXTRA_PKGS, gpu_packages[accel])
        accel == "amdgpu" && push!(EXTRA_PKGS, "AMDGPU_LLVM_Backend_jll")
    end
end
if get(ENV, "BENCHMARK_OPENCL_SOFTWARE", "false") == "true"
    push!(EXTRA_PKGS, "pocl_jll")
end
unique!(EXTRA_PKGS)

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

include(joinpath(@__DIR__, "compare.jl"))
function load_sampled_results(dir)
    combined = load_results("Dagger", [BASE_REV, CUR_REV]; input_dir=dir)
    for rev in (BASE_REV, CUR_REV)
        raw = JSON3.read(read(joinpath(dir, "results_Dagger@$(replace(rev, '/' => '_')).json"), String), Dict{String,Any})
        attach_sample_counts!(combined[rev], raw)
    end
    return combined
end
combined = load_sampled_results(OUTPUT_DIR)

# --- Results table ---------------------------------------------------------

table = create_table(combined; key = "median", add_ratio_col = true)
println("\nBenchmark results (median time):\n")
println(table)

# Allocations/memory table (the ratio column compares allocated bytes). Each
# BenchmarkTools trial records the minimum allocation count and bytes across
# timed evaluations; it does not retain an allocation distribution.
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

# Require five actual samples in both revisions, then confirm timing flags in
# fresh processes with revision order reversed to expose run-order effects.
comparison_options = (; threshold=THRESHOLD, alloc_threshold=ALLOC_THRESHOLD,
                        noise_tolerance=NOISE_TOLERANCE)
comparison = compare_benchmarks(base, cur; comparison_options...)
confirmation = nothing
if any(entry -> entry[2] == "time", comparison.regressions)
    confirmation_dir = joinpath(OUTPUT_DIR, "confirmation")
    mkpath(confirmation_dir)
    @info "Confirming timing regressions with a fresh comparison in reversed revision order"
    benchmark("Dagger", [CUR_REV, BASE_REV]; output_dir=confirmation_dir,
              script=SCRIPT, path=PROJECT_DIR, extra_pkgs=EXTRA_PKGS,
              exeflags=`-t $CI_THREADS`, tune=false)
    confirmation = load_sampled_results(confirmation_dir)
    repeated = compare_benchmarks(confirmation[BASE_REV], confirmation[CUR_REV]; comparison_options...)
    comparison = confirm_timing_regressions(comparison, repeated)
end
(; regressions, improvements, within_noise, insufficient) = comparison

pct(r) = string(round((r - 1) * 100; digits = 1), "%")

# --- Per-job summary (counts at a glance, no need to open the detail lists) -
#
# A "job" is a benchmark's `suite/method[+accels]` prefix -- e.g.
# `stencil/dagger` -- which is exactly one entry of the `BENCHMARK` spec
# (`suite:method+accel,...;...`). Grouping at that level keeps the summary to
# one row per suite/method combination actually run, regardless of how many
# individual benchmarks or metrics it contains.
job_of(name) = join(split(name, '/')[1:min(2, end)], "/")

job_names = Set{String}()
for (name, _) in cur
    name == "time_to_load" && continue
    push!(job_names, job_of(name))
end

function job_tally(entries)
    counts = Dict{String,Int}()
    for (name, _metric, _ratio) in entries
        j = job_of(name)
        counts[j] = get(counts, j, 0) + 1
    end
    return counts
end
job_regression_counts = job_tally(regressions)
job_improvement_counts = job_tally(improvements)
job_noise_counts = job_tally(within_noise)
job_insufficient_counts = job_tally(insufficient)

# Worst jobs (most regressions, then fewest improvements) sort to the top.
job_summary_rows = [(j, get(job_regression_counts, j, 0), get(job_improvement_counts, j, 0),
                      get(job_noise_counts, j, 0), get(job_insufficient_counts, j, 0)) for j in job_names]
sort!(job_summary_rows; by = r -> (-r[2], -r[3], r[1]))

function job_summary_table(io, rows)
    println(io, "| Job | Regressions | Improvements | Within noise | Inconclusive time |")
    println(io, "|:---|---:|---:|---:|---:|")
    for (job, nreg, nimp, nnoise, ninsufficient) in rows
        marker = nreg > 0 ? " ⚠️" : (nimp > 0 ? " ✅" : "")
        println(io, "| `", job, "`", marker, " | ", nreg, " | ", nimp, " | ", nnoise, " | ", ninsufficient, " |")
    end
end

# A machine-readable summary lets CI aggregate suites without parsing Markdown
# or duplicating the regression rules in the PR-comment renderer.
entries_json(entries) = [(; name, metric, ratio) for (name, metric, ratio) in entries]
summary = (;
    schema_version=1, base_revision=BASE_REV, current_revision=CUR_REV,
    thresholds=(; time=THRESHOLD, allocations=ALLOC_THRESHOLD, noise=NOISE_TOLERANCE, min_samples=5),
    timing_confirmation_run=confirmation !== nothing,
    jobs=[(; name, regressions=nreg, improvements=nimp, within_noise=nnoise,
             insufficient=ninsufficient) for (name, nreg, nimp, nnoise, ninsufficient) in job_summary_rows],
    regressions=entries_json(regressions), improvements=entries_json(improvements),
    within_noise=entries_json(within_noise), insufficient=entries_json(insufficient),
)
open(io -> JSON3.write(io, summary), joinpath(OUTPUT_DIR, "summary.json"), "w")

# --- Markdown report (for the Buildkite annotation / optional PR comment) ---

open(joinpath(OUTPUT_DIR, "report.md"), "w") do io
    println(io, "### Dagger benchmarks: `$CUR_REV` vs `$BASE_REV`")
    println(io)
    println(io, "#### Summary by job")
    println(io)
    job_summary_table(io, job_summary_rows)
    println(io)
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
    println(io)
    if !isempty(insufficient)
        println(io, "#### Inconclusive timing changes")
        println(io)
        println(io, "These changes had fewer than five timed samples in at least one revision, lacked a measured spread, or were not confirmed by the independent run; they are not counted as improvements or regressions.")
        println(io)
        for (name, metric, r) in insufficient
            println(io, "- `", name, "` (", metric, "): ", pct(r))
        end
        println(io)
    end
    if confirmation !== nothing
        println(io, "#### Independent confirmation (reversed revision order)")
        println(io)
        println(io, "Timing regressions are counted only when both comparisons agree, with at least five samples per revision.")
        println(io)
        println(io, create_table(confirmation; key="median", add_ratio_col=true))
        println(io)
    end
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
end

# --- Summary + exit status -------------------------------------------------

println("\nSummary by job:\n")
job_summary_table(stdout, job_summary_rows)

if !isempty(within_noise)
    println("\n$(length(within_noise)) metric(s) moved past their threshold but stayed within the measured spread (not counted):")
    for (name, metric, r) in within_noise
        println("  - $name ($metric): $(pct(r))")
    end
end

if !isempty(insufficient)
    println("\n$(length(insufficient)) timing change(s) are inconclusive: fewer than five samples, missing spread, or no independent confirmation.")
end

if isempty(regressions)
    println("\nNo confirmed benchmark regressions (time > $(round(THRESHOLD * 100))%, allocs/memory > $(round(ALLOC_THRESHOLD * 100))%).")
    exit(0)
else
    println("\n$(length(regressions)) benchmark metric(s) regressed:")
    for (name, metric, r) in regressions
        println("  - $name ($metric): +$(pct(r))")
    end
    exit(1)
end
