@testset "cache" begin
    @everywhere GC.gc()
    #= set available memory to 80KB on each worker
    @everywhere empty!(MemPool.datastore)
    @everywhere empty!(MemPool.lru_order)
    @everywhere MemPool.max_memsize[] = 8*10^4
    =#

    thunks1 = map(delayed(_ -> rand(10^3), cache=true), workers())
    sum1 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks1)...)
    thunks2 = map(delayed(-), thunks1)
    sum2 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks2)...)
    s1 = collect(sum1)
    @test -s1 == collect(sum2)
    @test s1 == collect(sum1)
    @test -collect(sum1) == collect(sum2)

    # Issue #246
    z = delayed(identity)(10)
    zz = delayed(identity; cache=true)(10)
    @test collect(z) == collect(z)
    @test collect(zz) == collect(zz)
end
