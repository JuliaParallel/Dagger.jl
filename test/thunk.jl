
@testset "@par macro" begin
    @par t = 1+2

    @test t == Thunk(+, (1,2), id=:t)

    @par begin
        a = 3
        b = a+3
    end
    @test b == Thunk(+, (3, 3), id=:b)
end
