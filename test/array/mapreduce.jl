function test_mapreduce(f, init_func; no_init=true, zero_init=zero,
                        types=(Int32, Int64, Float32, Float64),
                        cmp=isapprox)
    @testset "$T" for T in types
        X = init_func(Blocks(10, 10), T, 100, 100)
        inits = ()
        if no_init
            inits = (inits..., nothing)
        end
        if zero_init !== nothing
            inits = (inits..., zero_init(T))
        end
        @testset "dims=$dims" for dims in (Colon(), 1, 2, (1,), (2,))
            @testset "init=$init" for init in inits
                if init === nothing
                    if dims == Colon()
                        @test cmp(f(X; dims), f(collect(X); dims))
                    else
                        @test cmp(collect(f(X; dims)), f(collect(X); dims))
                    end
                else
                    if dims == Colon()
                        @test cmp(f(X; dims, init), f(collect(X); dims, init))
                    else
                        @test cmp(collect(f(X; dims, init)), f(collect(X); dims, init))
                    end
                end
            end
        end
    end
end

# Base
@testset "reduce" test_mapreduce((X; dims, init=Base._InitialValue())->reduce(+, X; dims, init), ones)
@testset "mapreduce" test_mapreduce((X; dims, init=Base._InitialValue())->mapreduce(x->x+1, +, X; dims, init), ones)
@testset "sum" test_mapreduce(sum, ones)
@testset "prod" test_mapreduce(prod, rand)
@testset "minimum" test_mapreduce(minimum, rand)
@testset "maximum" test_mapreduce(maximum, rand)
@testset "extrema" test_mapreduce(extrema, rand; cmp=Base.:(==), zero_init=T->(zero(T), zero(T)))

# Statistics
@testset "mean" test_mapreduce(mean, rand; zero_init=nothing, types=(Float32, Float64))
@testset "var" test_mapreduce(var, rand; zero_init=nothing, types=(Float32, Float64))
@testset "std" test_mapreduce(std, rand; zero_init=nothing, types=(Float32, Float64))
