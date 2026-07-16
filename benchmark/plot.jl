# Quick plots for the JSON results produced by *standalone* execution of
# `benchmarks.jl` (i.e. run directly, NOT via AirspeedVelocity -- see the
# `BENCHMARK_JSON_OUTPUT` env var documented at the top of benchmarks.jl).
# Renders one PNG per (suite, benchmark) pair, with one line per method.
#
# For proper before/after comparisons across revisions, use `benchmark/ci.jl`
# instead, which drives AirspeedVelocity directly and produces its own plots.
#
# Usage:
#   julia --project=benchmark benchmark/plot.jl [path/to/results.json] [output_dir]
#
# `path/to/results.json` defaults to "benchmark_results/results_standalone.json",
# the location `benchmarks.jl` writes to by default. `output_dir` defaults to
# a "plots" subdirectory next to the results JSON.

using JSON3
using Plots

const path = length(ARGS) >= 1 ? ARGS[1] : joinpath("benchmark_results", "results_standalone.json")
isfile(path) ||
    error("No such results file: $path\n" *
          "Run `benchmark/run.sh` first (or pass the path to an existing results JSON).")

const OUTDIR = length(ARGS) >= 2 ? ARGS[2] : joinpath(dirname(path), "plots")

data = JSON3.read(read(path, String))

# Parse the matrix dimension N out of a suite group key like "N=4096 (block 512)".
function parse_N(key::AbstractString)
    m = match(r"^N=(\d+)", key)
    return m === nothing ? nothing : parse(Int, m.captures[1])
end

struct Point
    N::Int
    seconds::Float64
end

# (suite, benchmark name) => method => points, for results with a scale (N) dimension.
scaled = Dict{Tuple{String,String},Dict{String,Vector{Point}}}()
# (suite, method, benchmark name) => seconds, for results with no scale dimension
# (e.g. the legacy "dtable" suite).
unscaled = Dict{Tuple{String,String,String},Float64}()

for entry in data.results
    kp = String.(entry.keypath)
    length(kp) >= 2 || continue
    suite, method = kp[1], kp[2]
    seconds = entry.min_time_ns / 1e9
    N = length(kp) >= 3 ? parse_N(kp[3]) : nothing
    if N === nothing
        name = join(kp[3:end], " / ")
        isempty(name) && continue
        unscaled[(suite, method, name)] = seconds
    else
        name = join(kp[4:end], " / ")
        by_method = get!(scaled, (suite, name), Dict{String,Vector{Point}}())
        push!(get!(by_method, method, Point[]), Point(N, seconds))
    end
end

if isempty(scaled) && isempty(unscaled)
    println("No results found in $path")
    exit(0)
end

# Turn "suite / benchmark name" into a filesystem-safe filename.
sanitize(s) = replace(s, r"[^A-Za-z0-9]+" => "_")

saved = String[]
if !isempty(scaled)
    mkpath(OUTDIR)
    for ((suite, name), by_method) in sort(collect(scaled); by=first)
        all_pts = collect(Iterators.flatten(values(by_method)))
        # A log scale needs strictly positive values on both axes; fall back
        # to linear otherwise.
        can_log = all(p -> p.N > 0, all_pts) && all(p -> p.seconds > 0, all_pts)
        plt = plot(; title="$suite / $name", xlabel="N", ylabel="time (s)",
                     legend=:outertopright,
                     xscale=can_log ? :log10 : :identity,
                     yscale=can_log ? :log10 : :identity)
        for (method, pts) in sort(collect(by_method); by=first)
            sort!(pts; by=p -> p.N)
            unique!(p -> p.N, pts)
            plot!(plt, [p.N for p in pts], [p.seconds for p in pts];
                  label=method, marker=:circle)
        end
        fname = joinpath(OUTDIR, "$(sanitize(suite))_$(sanitize(name)).png")
        savefig(plt, fname)
        push!(saved, fname)
    end
end

if !isempty(saved)
    println("Saved $(length(saved)) plot(s) to $(abspath(OUTDIR)):")
    foreach(f -> println("  $f"), saved)
end

if !isempty(unscaled)
    println("\nOther results (no scale dimension):\n")
    for ((suite, method, name), seconds) in sort(collect(unscaled); by=first)
        println("  $suite / $method / $name => $(round(seconds; digits=6)) s")
    end
end
