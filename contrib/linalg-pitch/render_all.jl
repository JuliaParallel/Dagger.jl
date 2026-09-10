# Regenerator for the collaborator-pitch videos.
#
#   julia --project=contrib/linalg-pitch contrib/linalg-pitch/render_all.jl
#   julia --project=contrib/linalg-pitch contrib/linalg-pitch/render_all.jl --solve-only
#   julia --project=contrib/linalg-pitch contrib/linalg-pitch/render_all.jl heat elasticity
#
# Does not run the Dagger test suite. Small local solves only.

const PITCH = @__DIR__
push!(LOAD_PATH, joinpath(PITCH, "src"))

using Dates
using Pkg
Pkg.activate(PITCH)
Pkg.instantiate()

include(joinpath(PITCH, "src", "style.jl"))
include(joinpath(PITCH, "src", "ops.jl"))
include(joinpath(PITCH, "sims", "heat.jl"))
include(joinpath(PITCH, "sims", "elasticity.jl"))
include(joinpath(PITCH, "sims", "convection.jl"))
include(joinpath(PITCH, "sims", "multiphysics.jl"))
include(joinpath(PITCH, "sims", "unstructured.jl"))
include(joinpath(PITCH, "sims", "mixedprec.jl"))

const SIMS = (
    "heat"         => sim_heat,
    "elasticity"   => sim_elasticity,
    "convection"   => sim_convection,
    "multiphysics" => sim_multiphysics,
    "unstructured" => sim_unstructured,
    "mixedprec"    => sim_mixedprec,
)

function parse_args(args)
    solve_only = "--solve-only" in args
    names = [a for a in args if !startswith(a, "-")]
    return solve_only, names
end

function write_results(metas)
    path = joinpath(PITCH, "results.md")
    open(path, "w") do io
        println(io, "# Pitch solve results")
        println(io)
        println(io, "- SHA: `", git_sha(), "`")
        println(io, "- Generated: ", Dates.now())
        println(io)
        println(io, "| Clip | API | n | Blocks | ‖r‖/‖b‖ | iters | solve s |")
        println(io, "|---|---|---|---|---|---|---|")
        for m in metas
            println(io, "| ", m.name, " | ", m.api, " | ", m.n, " | ",
                    fmt_blocks(m.blocks), " | ",
                    string(round(m.rel; sigdigits=3)), " | ",
                    get(m, :niter, "—"), " | ",
                    string(round(m.tsolve; digits=2)), " |")
        end
    end
    return path
end

function main(args)
    solve_only, names = parse_args(args)
    wanted = isempty(names) ? collect(first.(SIMS)) : names
    known = Dict(SIMS)
    metas = []
    failed = String[]
    apply_pitch_theme!()
    @info "linalg-pitch" sha=git_sha() solve_only wanted
    for name in wanted
        haskey(known, name) || (push!(failed, name * " (unknown)"); continue)
        try
            @info "running" name
            m = known[name](; solve_only)
            push!(metas, m)
        catch err
            @error "simulation failed" name exception=(err, catch_backtrace())
            push!(failed, name * ": " * sprint(showerror, err))
        end
    end
    if !isempty(metas)
        write_results(metas)
    end
    isempty(failed) || @warn "failures" failed
    return metas, failed
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
