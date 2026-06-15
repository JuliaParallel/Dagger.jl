import Preferences: load_preference, set_preferences!

@testset "Preferences" begin
    # The subprocess needs the repo root in its load path to find Dagger,
    # since test/Project.toml only has Dagger in [extras], not [deps].
    repo_root = joinpath(@__DIR__, "..")
    load_path = string(repo_root, ":", @__DIR__, ":@:@v#.#:@stdlib")
    base_cmd = `$(Base.julia_cmd()) --startup-file=no --project -E 'using Dagger; parentmodule(Dagger.myid)'`
    #cmd = addenv(base_cmd, "JULIA_LOAD_PATH" => load_path)
    cmd = base_cmd
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
