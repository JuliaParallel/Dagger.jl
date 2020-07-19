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
        "Scheduler Internals" => "scheduler-internals.md",
    ]
)

deploydocs(;
    repo="github.com/JuliaParallel/Dagger.jl",
)

