using Base.Test
using Dagger

@testset "cache" begin
    @everywhere gc(true)
    # set available memory to 8MB on each worker
    test_extra = 8*10^6
    map(workers()) do pid
        pid=>remotecall_fetch(pid) do
            totsz = sum(map(first, values(Dagger._mymem)))
            Dagger.MAX_MEMORY[] = totsz + test_extra
        end
    end

    thunks1 = map(delayed(_ -> rand(10^5), cache=true), workers())
    sum1 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks1)...)
    thunks2 = map(delayed(-), thunks1)
    sum2 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks2)...)
    s1 = collect(sum1)
    @test -s1 == collect(sum2)
    @test s1 == collect(sum1)
    @test -collect(sum1) == collect(sum2)

    thunks1 = map(delayed(_ -> rand(10^6), cache=true), workers())
    sum1 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks1)...)
    thunks2 = map(delayed(-), thunks1)
    sum2 = delayed((x...)->sum([x...]))(map(delayed(sum), thunks2)...)
    s1 = collect(sum1) # this should evict thunk1s from memory
    @test -s1 != collect(sum2)
end
