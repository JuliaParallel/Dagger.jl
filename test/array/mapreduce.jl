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

# A single partition's full reduction is a bare scalar; `collect` must wrap it.
@testset "single-partition scalar" begin
    X = ones(Blocks(100, 100), 100, 100)
    @test sum(X) == 10000
    @test prod(X) == 1
    @test mapreduce(identity, +, X) == 10000
end
@testset "prod" test_mapreduce(prod, rand)
@testset "minimum" test_mapreduce(minimum, rand)
@testset "maximum" test_mapreduce(maximum, rand)
@testset "extrema" test_mapreduce(extrema, rand; cmp=Base.:(==), zero_init=T->(zero(T), zero(T)))

# Statistics
@testset "mean" test_mapreduce(mean, rand; zero_init=nothing, types=(Float32, Float64))
@testset "var" test_mapreduce(var, rand; zero_init=nothing, types=(Float32, Float64))
@testset "std" test_mapreduce(std, rand; zero_init=nothing, types=(Float32, Float64))

# The result type follows `f`, not only the input eltype
function test_mapreduce_type(f, A; cmp=isapprox)
    X = distribute(A, Blocks(10, 10))
    @testset "dims=$dims" for dims in (Colon(), 1, 2)
        expected = f(A; dims)
        if dims == Colon()
            actual = f(X; dims)
            @test typeof(actual) == typeof(expected)
            @test cmp(actual, expected)
        else
            actual = f(X; dims)
            @test eltype(actual) == eltype(expected)
            @test cmp(collect(actual), expected)
        end
    end
end
@testset "result type follows f" begin
    A = rand(20, 20)
    C = rand(ComplexF64, 20, 20)
    @testset "sum (Bool)" test_mapreduce_type((X; dims)->sum(x->x>0.5, X; dims), A)
    @testset "sum (real of complex)" test_mapreduce_type((X; dims)->sum(abs2, X; dims), C)
    @testset "sum (complex of real)" test_mapreduce_type((X; dims)->sum(x->complex(x, 1.0), X; dims), A)
    @testset "prod (Float32)" test_mapreduce_type((X; dims)->prod(x->Float32(x)+1, X; dims), A)
    @testset "mapreduce" test_mapreduce_type((X; dims)->mapreduce(abs2, +, X; dims), C)
    @testset "maximum" test_mapreduce_type((X; dims)->maximum(abs, X; dims), C)
    @testset "count" test_mapreduce_type((X; dims)->count(x->x>0.5, X; dims), A)
    @testset "sum (Bool, Float64 init)" test_mapreduce_type((X; dims)->sum(x->x>0.5, X; dims, init=0.0), A)
    @testset "sum (Int, Float64 init)" test_mapreduce_type((X; dims)->sum(X; dims, init=0.0), rand(1:9, 20, 20))
    @testset "mean" test_mapreduce_type(mean, A)
    @testset "var" test_mapreduce_type(var, A)
    @testset "std" test_mapreduce_type(std, A)
    @testset "extrema" test_mapreduce_type(extrema, A; cmp=Base.:(==))
    @testset "extrema (real of complex)" test_mapreduce_type((X; dims)->extrema(abs, X; dims), C; cmp=Base.:(==))
end

# Reducing `Bool`s across partitions broadcasts into a `BitArray`
@testset "Bool results" begin
    A = rand(20, 20)
    @testset "all" test_mapreduce_type((X; dims)->all(x->x>0.05, X; dims), A; cmp=Base.:(==))
    @testset "any" test_mapreduce_type((X; dims)->any(x->x>0.95, X; dims), A; cmp=Base.:(==))
    @testset "prod" test_mapreduce_type((X; dims)->prod(x->x>0.05, X; dims), A; cmp=Base.:(==))
    @testset "maximum" test_mapreduce_type((X; dims)->maximum(x->x>0.95, X; dims), A; cmp=Base.:(==))
end
