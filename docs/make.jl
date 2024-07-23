using Dagger, TimespanLogging, DaggerWebDash, GraphViz, Plots, DataFrames
const GraphVizExt = something(Base.get_extension(Dagger, :GraphVizExt))
const PlotsExt = something(Base.get_extension(Dagger, :PlotsExt))
using Documenter
import Documenter.Remotes: GitHub

makedocs(;
    modules = [Dagger, TimespanLogging, DaggerWebDash, GraphVizExt, PlotsExt],
    authors = "JuliaParallel and contributors",
    repo = GitHub("JuliaParallel", "Dagger.jl"),
    sitename = "Dagger.jl",
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical = "https://JuliaParallel.github.io/Dagger.jl",
        assets = String["assets/favicon.ico"],
    ),
    pages = [
        "Home" => "index.md",
        "Use Cases" => [
            "Parallel Nested Loops" => "use-cases/parallel-nested-loops.md",
        ],
        "Task Spawning" => "task-spawning.md",
        "Data Management" => "data-management.md",
        "Distributed Arrays" => "darray.md",
        "Scopes" => "scopes.md",
        "Processors" => "processors.md",
        "Task Queues" => "task-queues.md",
        "Datadeps" => "datadeps.md",
        "Option Propagation" => "propagation.md",
        "Logging and Visualization" => [
            "Logging: Basics" => "logging.md",
            "Logging: Visualization" => "logging-visualization.md",
            "Logging: Advanced" => "logging-advanced.md",
        ],
        "Checkpointing" => "checkpointing.md",
        "Benchmarking" => "benchmarking.md",
        "Dynamic Scheduler Control" => "dynamic.md",
        "Scheduler Internals" => "scheduler-internals.md",
        "Dagger API" => [
            "Types" => "api-dagger/types.md",
            "Functions and Macros" => "api-dagger/functions.md",
        ],
        "TimespanLogging API" => [
            "Types" => "api-timespanlogging/types.md",
            "Functions and Macros" => "api-timespanlogging/functions.md",
        ],
        "DaggerWebDash API" => [
            "Types" => "api-daggerwebdash/types.md",
            "Functions and Macros" => "api-daggerwebdash/functions.md",
        ],
    ],
    warnonly=[:missing_docs]
)

deploydocs(;
    repo="github.com/JuliaParallel/Dagger.jl",
)
