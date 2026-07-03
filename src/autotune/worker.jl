# worker.jl - benchmark worker entrypoint.
#
# Usage: julia --project=<env> --threads=N [-p M] worker.jl trials.toml outdir
#
# The benchmark driver normally generates and spawns an identical script
# itself; this committed copy exists for manual runs and debugging (e.g.
# re-running a crashed group under rr or a profiler).
#
# Resolution order for the Autotune module:
#   1. Dagger.Autotune (the integrated form), if Dagger loads and has it.
#   2. Standalone include of the sources next to this file, or from
#      ENV["DAGGER_AUTOTUNE_SRC"] if set.

srcdir = get(ENV, "DAGGER_AUTOTUNE_SRC", @__DIR__)
autotune = try
    Base.require(Main, :Dagger).Autotune
catch
    include(joinpath(srcdir, "Autotune.jl"))
    Main.Autotune
end
autotune.run_worker(ARGS[1], ARGS[2])
