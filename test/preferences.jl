import Preferences: load_preference, set_preferences!

@testset "Preferences" begin
    cmd = `$(Base.julia_cmd()) --startup-file=no --project -E 'using Dagger; parentmodule(Dagger.myid)'`

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
    end
end
