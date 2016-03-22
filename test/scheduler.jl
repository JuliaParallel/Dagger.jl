
#### test set 2 begin

function inc(x)
    x+1
end

@testset "Scheduler" begin
    @testset "order" begin
        @par begin
            a = 1
            b = inc(a)
            c = inc(b)
        end

        deps = dependents(c)
        @test deps == Dict(a => Set([b]), b => Set([c]), c=>Set())
        @test noffspring(deps) == Dict(a=>2, b=>1, c=>0)
        @test order([c], noffspring(deps)) == Dict(a => 3, b => 2, c => 1)
    end
    @testset begin
        @par begin
            a = 1
            b = 2
            c = inc(a)
            d = b+c
        end
        deps = dependents(d)
        @test noffspring(deps) == Dict(a => 2, b => 1, c => 1, d => 0)
        @test order([d], noffspring(deps)) == Dict(d=>1, c=>3, b=>2, a=>4)
    end
    @testset "simple compute" begin
        @par begin
            a = 1
            b = 2
            c = inc(a)
            d = b+c
        end

        deps = dependents(d)
        @test noffspring(deps) == Dict(a => 2, b => 1, c => 1, d => 0)
        @test order([d], noffspring(deps)) == Dict(d=>1, c=>3, b=>2, a=>4)

        @test compute(Context(), d) == 4
    end
end

