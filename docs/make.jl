using Dagger
using Documenter

makedocs(;
    modules = [Dagger],
    authors = "JuliaParallel and contributors",
    repo = "https://github.com/JuliaParallel/Dagger.jl/blob/{commit}{path}#L{line}",
    sitename = "Dagger.jl",
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical = "https://JuliaParallel.github.io/Dagger.jl",
        assets = String[],
    ),
    pages = [
        "Home" => "index.md",
        "Processors" => "processors.md",
        "Checkpointing" => "checkpointing.md",
        "Scopes" => "scopes.md",
        "Dynamic Scheduler Control" => "dynamic.md",
        "Logging and Graphing" => "logging.md",
        "Benchmarking" => "benchmarking.md",
        "Scheduler Internals" => "scheduler-internals.md",
        "Distributed Table" => "dtable.md",
        "API" => [
            "Types" => "api/types.md",
            "Functions and Macros" => "api/functions.md",
        ]
    ]
)

deploydocs(;
    repo="github.com/JuliaParallel/Dagger.jl",
)

