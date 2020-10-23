import Dagger: @par

@testset "@par" begin
    @testset "per-call" begin
        x = 2
        a = @par x + x
        @test a isa Dagger.Thunk
        b = @par sum([x,1,2])
        c = @par a * b
        @test collect(c) == 20
    end

    @testset "block" begin
        c = @par begin
            x = 2
            a = x + x
            b = sum([x,1,2])
            c = a * b
        end
        @test x isa Int
        @test a isa Dagger.Thunk
        @test c isa Dagger.Thunk
        @test collect(c) == 20
    end
end
