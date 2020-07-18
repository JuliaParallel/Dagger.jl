using Documenter, Dagger

makedocs(
    sitename = "Dagger",
    pages = [
        "Home" => "index.md",
        "Processors" => "processors.md",
        "Scheduler Internals" => "scheduler-internals.md",
    ]
)
