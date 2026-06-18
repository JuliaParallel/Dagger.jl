import Preferences: load_preference, set_preferences!

@testset "Preferences" begin
    # The subprocess must resolve packages and preferences exactly like this test
    # process: it has to load Dagger *and* read the distributed-package preference
    # from the same LocalPreferences.toml that `set_distributed_package!` writes
    # to (i.e. the active project's). Under `Pkg.test()` the active project is a
    # temporary sandbox combining the root and test projects (not the repo root
    # or test dir), and that sandbox is where the preferences land. Rather than
    # hand-build a load path (which misses the sandbox, and whose hardcoded `:`
    # separator is wrong on Windows), mirror the parent's fully-resolved load
    # path. `Base.load_path()` already expands `@` to the active project, so the
    # subprocess sees Dagger and the written preferences in both `Pkg.test()` and
    # direct `runtests.jl` runs.
    load_path = join(Base.load_path(), Sys.iswindows() ? ';' : ':')
    base_cmd = `$(Base.julia_cmd()) --startup-file=no -E 'using Dagger; parentmodule(Dagger.myid)'`
    cmd = addenv(base_cmd, "JULIA_LOAD_PATH" => load_path)
    try
        # Disabling the precompilation workload shaves off over half the time
        # this test takes.
        set_preferences!(Dagger, "precompile_workload" => false; force=true)

        cd(dirname(Base.active_project())) do
            Dagger.set_distributed_package!("Distributed")
            @test readchomp(cmd) == "Distributed"

            Dagger.set_distributed_package!("DistributedNext")
            @test readchomp(cmd) == "DistributedNext"
        end
    finally
        set_preferences!(Dagger, "precompile_workload" => true; force=true)
        # Reset distributed package to the default to avoid affecting subsequent runs
        Dagger.set_distributed_package!("Distributed")
    end
end
