@testset "Disk caching setup on multiple processes (single machine)" begin
    runs = []
    for p in 0:3
        j = if p == 0
            Cmd(`julia --startup-file=no`)
        else
            Cmd(`julia --startup-file=no -p $p`)
        end
        withenv("JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR"=>nothing,
                "JULIA_MEMPOOL_EXPERIMENTAL_MEMORY_BOUND"=>nothing,
                "JULIA_MEMPOOL_EXPERIMENTAL_DISK_CACHE"=>nothing,
                "JULIA_MEMPOOL_EXPERIMENTAL_DISK_BOUND"=>nothing,
                "JULIA_MEMPOOL_EXPERIMENTAL_ALLOCATOR_KIND"=>nothing) do
            push!(runs, run(`$j cache_setup_test.jl`; wait=true))
        end
    end
    wait.(runs)
    @test all(getproperty.(runs, :exitcode) .== 0)
end
