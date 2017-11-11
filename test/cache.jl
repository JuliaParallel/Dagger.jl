using Base.Test
using Dagger
using MemPool
using Base.Test

@testset "cache" begin
    @everywhere gc(true)
    # set available memory to 80KB on each worker
    @everywhere empty!(MemPool.datastore)
    @everywhere empty!(MemPool.lru_order)
    @everywhere MemPool.max_memsize[] = 8*10^4

    thunks1 = map(delayed(_ -> rand(10^3), cache=true), workers())
    sum1 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks1)...)
    thunks2 = map(delayed(-), thunks1)
    sum2 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks2)...)
    s1 = collect(sum1)
    @test -s1 == collect(sum2)
    @test s1 == collect(sum1)
    @test -collect(sum1) == collect(sum2)

    thunks1 = map(delayed(_ -> rand(10^4), cache=true), workers())
    sum1 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks1)...)
    s1 = collect(sum1)
    thunks2 = map(delayed(_ -> rand(10^4), cache=true), workers())
    sum2 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks2)...)
    s2 = collect(sum2) # this should evict thunk1s from memory
    @test s1 != collect(sum1)
    @test s2 != collect(sum2)
end
