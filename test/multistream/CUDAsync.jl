using Test
using Dagger, CUDA

processors = collect(Dagger.get_processors(Dagger.OSProc()))
proc = first(filter(p -> contains(string(typeof(p)), "CuArray"), processors))
println("Found: ", proc)

@testset "gpu_synchronize is per-stream (CUDA)" begin
    @test begin
        Dagger.gpu_synchronize(proc)
        true
    end

    @test begin
        Dagger.gpu_synchronize(Val(:CUDA))
        true
    end

    t = Dagger.@spawn CuArray(rand(Float32, 64, 64))
    arr = fetch(t)
    Dagger.gpu_synchronize(proc)

    host = Array(arr)
    @test size(host) == (64, 64)
    @test !any(isnan, host)
end