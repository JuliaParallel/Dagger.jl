using Dagger
using Documenter
using Literate
using Test

const EXAMPLE_DIR = joinpath(@__DIR__, "src", "examples")

function link_example(content)
    edit_url = match(r"EditURL = \"(.+?)\"", content)[1]
    footer = match(r"^(---\n\n\*This page was generated using)"m, content)[1]
    content = replace(
        content, footer => "[View this file on Github]($(edit_url)).\n\n" * footer
    )
    return content
end

function literate_examples()
    for file in readdir(EXAMPLE_DIR)
        if !endswith(file, ".jl")
            continue
        end
        filename = joinpath(EXAMPLE_DIR, file)
        # # `include` the file to test it before `#src` lines are removed. It is
        # # in a testset to isolate local variables between files.
        # @testset "$(file)" begin
        #     include(filename)
        # end
        Literate.markdown(
            filename,
            EXAMPLE_DIR;
            documenter = true,
            postprocess = link_example,
            execute=false,
        )
    end
    return nothing
end

literate_examples()

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
        "Logging and Graphing" => "logging.md",
        "Dynamic Scheduler Control" => "dynamic.md",
        "Examples" => map(
            file -> joinpath("examples", file),
            filter(
                file -> endswith(file, ".md"),
                sort(readdir(EXAMPLE_DIR)),
            )
        ),
    ]
)

deploydocs(;
    repo="github.com/JuliaParallel/Dagger.jl",
)

