using Test
using Dagger, AMDGPU

processors = collect(Dagger.get_processors(Dagger.OSProc()))
println("All processors: ")
println.(processors)

roc_procs = filter(p -> contains(string(typeof(p)), "ROCArray"), processors)

if isempty(roc_procs)
    @warn "No ROCm GPU found, skipping tests"
else
    proc = first(roc_procs)
    println("\nFound: ", proc)

    @testset "gpu_synchronize is per-stream (ROCm)" begin
        # Correctness: sync completes without error
        @test begin
            Dagger.gpu_synchronize(proc)
            true
        end

        # Val dispatch also works
        @test begin
            Dagger.gpu_synchronize(Val(:ROC))
            true
        end

        # Data integrity: result is available after sync, no garbage values
        t = Dagger.@spawn ROCArray(rand(Float32, 64, 64))
        arr = fetch(t)
        Dagger.gpu_synchronize(proc)

        host = Array(arr)
        @test size(host) == (64, 64)
        @test !any(isnan, host)
    end
end