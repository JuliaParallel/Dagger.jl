# A struct whose fields are bits or plain Arrays of bits travels between ranks as raw buffers (automatic
# `inplace_mpi_parts`), anything else as before; values and types must arrive unchanged either way.
struct RawBundle
    keys::Vector{UInt64}
    scale::Float64
    grid::Matrix{Float32}
    n::Int
end
struct MixedBundle                     # a String and a Dict next to an array: the arrays go raw, the rest in the header
    name::String
    v::Vector{Float64}
    tags::Dict{Symbol,Int}
end
struct InnerParts
    xs::Vector{Float64}
    id::Int
end
struct NestedBundle                    # an immutable struct holding arrays inside a struct: flattened
    inner::InnerParts
    ys::Vector{Int32}
    label::String
end
struct NoArrays                        # nothing to send raw
    name::String
    a::Int
end
struct SharedBundle                    # one array reachable twice: must stay shared after the transfer
    a::Vector{Float64}
    b::Vector{Float64}
end
struct GuardedBundle                   # an inner constructor with an invariant: rebuilt without calling it, like Serialization
    v::Vector{Float64}
    total::Float64
    GuardedBundle(v) = new(v, sum(v))
end
raw_make(n::Int) = RawBundle(collect(UInt64, 1:n), 2.5, fill(Float32(n), 3, 4), n)
raw_check(b::RawBundle) = (b.keys == collect(UInt64, 1:b.n)) && b.scale == 2.5 && b.grid == fill(Float32(b.n), 3, 4)
mixed_make(n::Int) = MixedBundle("m$n", fill(1.5, n), Dict(:a => n))
mixed_check(b::MixedBundle) = b.name == "m" * string(length(b.v)) && all(==(1.5), b.v) && b.tags == Dict(:a => length(b.v))
nested_make(n::Int) = NestedBundle(InnerParts(fill(3.0, n), n), fill(Int32(7), 2n), "nest")
nested_check(b::NestedBundle) = b.inner.id == length(b.inner.xs) && all(==(3.0), b.inner.xs) && length(b.ys) == 2 * b.inner.id && b.label == "nest"
shared_make(n::Int) = (v = fill(4.0, n); SharedBundle(v, v))
shared_check(b::SharedBundle) = b.a === b.b && all(==(4.0), b.a)
guard_make(n::Int) = GuardedBundle(fill(2.0, n))
guard_check(b::GuardedBundle) = b.total == 2.0 * length(b.v)

@testset "Automatic raw transport of structs of arrays" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    pinned(r) = Dagger.ExactScope(last(filter(p -> p.rank == r, cpus)))
    other = nranks - 1
    @testset "the decomposition" begin
        @test Dagger.inplace_mpi_parts(raw_make(5)) !== nothing
        @test Dagger.inplace_mpi_parts(mixed_make(5)) !== nothing        # the String and the Dict ride in the header
        @test Dagger.inplace_mpi_parts(nested_make(5)) !== nothing
        @test length(first(Dagger.inplace_mpi_parts(nested_make(5)))) == 2   # both arrays, the inner struct is flattened
        @test Dagger.inplace_mpi_parts(NoArrays("x", 1)) === nothing
        @test Dagger.inplace_mpi_parts(shared_make(5)) === nothing       # shared array: left to serialize
        @test Dagger.inplace_mpi_parts(3.0) === nothing
        parts, header = Dagger.inplace_mpi_parts(raw_make(5))
        @test length(parts) == 2 && parts[1] isa Vector{UInt64} && parts[2] isa Matrix{Float32}
        b = Dagger.inplace_mpi_build(RawBundle, Dagger.inplace_mpi_alloc(RawBundle, header), header)
        @test size(b.keys) == (5,) && size(b.grid) == (3, 4) && b.n == 5 && b.scale == 2.5
    end
    for (name, make, check, ret) in (("eligible struct", raw_make, raw_check, RawBundle),
                                     ("struct with a String and a Dict", mixed_make, mixed_check, MixedBundle),
                                     ("nested struct", nested_make, nested_check, NestedBundle),
                                     ("shared array", shared_make, shared_check, SharedBundle),
                                     ("inner constructor", guard_make, guard_check, GuardedBundle))
        @testset "$name moves between ranks" begin
            for n in (3, 100_000)
                a = Dagger.@spawn scope=pinned(0) return_type=ret make(n)
                ok = Dagger.@spawn scope=pinned(other) return_type=Bool check(a)
                @test fetch(ok)
            end
        end
    end
    MPI.Barrier(comm)
end
