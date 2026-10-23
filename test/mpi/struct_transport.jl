import Serialization
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
struct ViewBundle                      # an array next to a view: only the array goes raw
    a::Vector{Float64}
    v::SubArray{Float64,2,Matrix{Float64},Tuple{UnitRange{Int},UnitRange{Int}},false}
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
struct PartlyUndefined                 # an inner constructor that leaves a field unset: cannot be read field by field
    v::Vector{Float64}
    note::String
    PartlyUndefined(v) = new(v)
end
struct Padded                          # padding between the fields: an array of these is left to serialize
    x::Float32
    y::Int8
end
struct PaddedBundle
    a::Vector{Padded}
    empty::Matrix{Float64}
    n::Union{Nothing,Vector{Int}}      # a Union field rides in the header
end
struct OwnSerialized                   # a type with its own `serialize` keeps it
    v::Vector{Float64}
end
Serialization.serialize(s::Serialization.AbstractSerializer, x::OwnSerialized) =
    (Serialization.serialize_type(s, OwnSerialized); Serialization.serialize(s, x.v .* 2))
Serialization.deserialize(s::Serialization.AbstractSerializer, ::Type{OwnSerialized}) =
    OwnSerialized(Serialization.deserialize(s))
struct ViewSharing                     # a view of the array next to it: the sharing must survive
    a::Vector{Float64}
    v::SubArray{Float64,1,Vector{Float64},Tuple{UnitRange{Int}},true}
end
padded_make(n::Int) = PaddedBundle([Padded(Float32(i), Int8(i % 100)) for i in 1:n], zeros(0, 3), n > 3 ? collect(1:n) : nothing)
padded_check(b::PaddedBundle) = b.a == [Padded(Float32(i), Int8(i % 100)) for i in 1:length(b.a)] && size(b.empty) == (0, 3) &&
                                (length(b.a) > 3 ? b.n == collect(1:length(b.a)) : b.n === nothing)
undef_make(n::Int) = PartlyUndefined(fill(1.0, n))
undef_check(b::PartlyUndefined) = all(==(1.0), b.v) && !isdefined(b, :note)
tuple_make(n::Int) = (fill(2.0, n), n, "tuple", (collect(Int16, 1:n),))
tuple_check(t::Tuple) = t[1] == fill(2.0, t[2]) && t[3] == "tuple" && t[4][1] == collect(Int16, 1:t[2])
own_make(n::Int) = OwnSerialized(fill(1.0, n))
own_check(b::OwnSerialized) = all(==(2.0), b.v)
view_make(n::Int) = (a = fill(1.0, n); ViewSharing(a, view(a, 1:n)))
view_check(b::ViewSharing) = (b.v[1] = 9.0; b.a[1] == 9.0)       # still the same memory

# Random values built only from arrays of bits, strings and integers inside nested tuples: whatever the
# decomposition does, the value that arrives must be `isequal` to the one sent, with the same type.
function random_value(rng, depth=0)
    eltypes = (Int8, UInt16, Int32, Float32, Float64, ComplexF64, Bool)
    items = Any[]
    for _ in 1:rand(rng, 1:4)
        r = rand(rng)
        if r < 0.5
            T = rand(rng, eltypes)
            dims = ntuple(_ -> rand(rng, 0:5), rand(rng, 1:3))
            push!(items, T === Bool ? rand(rng, Bool, dims...) : rand(rng, T, dims...))
        elseif r < 0.7
            push!(items, rand(rng, Int))
        elseif r < 0.85
            push!(items, randstring(rng, rand(rng, 0:6)))
        elseif depth < 2
            push!(items, random_value(rng, depth + 1))
        end
    end
    return rand(rng, Bool) ? Tuple(items) : NamedTuple{ntuple(i -> Symbol("f", i), length(items))}(Tuple(items))
end
function local_roundtrip(x)
    r = Dagger.inplace_mpi_parts(x)
    r === nothing && return x
    parts, header = r
    io = IOBuffer(); Serialization.serialize(io, header); seekstart(io)
    header = Serialization.deserialize(io)
    T = typeof(x)
    alloc = Dagger.inplace_mpi_alloc(T, header)
    foreach((d, s) -> copyto!(d, s), alloc, parts)
    return Dagger.inplace_mpi_build(T, alloc, header)
end
random_make(seed::Int) = random_value(Random.MersenneTwister(seed))

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
        # An array wrapper is not decomposed: its array field is the whole parent
        big = rand(64, 64)
        @test Dagger.inplace_mpi_parts(view(big, 1:8, 1:8)) === nothing
        @test Dagger.inplace_mpi_parts(reshape(view(big, :, 1), 8, 8)) === nothing
        @test Dagger.inplace_mpi_parts(PermutedDimsArray(big, (2, 1))) === nothing
        # ...also not as a field of a struct: the view rides in the serialized header
        vb = ViewBundle(collect(1.0:4.0), view(big, 1:2, 1:2))
        vparts, vheader = Dagger.inplace_mpi_parts(vb)
        @test length(vparts) == 1 && vparts[1] === vb.a
        parts, header = Dagger.inplace_mpi_parts(raw_make(5))
        @test length(parts) == 2 && parts[1] isa Vector{UInt64} && parts[2] isa Matrix{Float32}
        b = Dagger.inplace_mpi_build(RawBundle, Dagger.inplace_mpi_alloc(RawBundle, header), header)
        @test size(b.keys) == (5,) && size(b.grid) == (3, 4) && b.n == 5 && b.scale == 2.5
    end
    @testset "edge cases fall back to serialize or round-trip exactly" begin
        @test Dagger.inplace_mpi_parts(undef_make(3)) === nothing           # an undefined field cannot be read
        @test Dagger.inplace_mpi_parts((Padded[Padded(1, 2)], [1.0])) !== nothing   # the other array still goes raw
        @test length(first(Dagger.inplace_mpi_parts((Padded[Padded(1, 2)], [1.0])))) == 1
        @test Dagger.inplace_mpi_parts(own_make(3)) === nothing             # its own `serialize` is kept
        @test Dagger.inplace_mpi_parts(view_make(3)) === nothing            # array and a view of it: sharing kept
        let a = collect(1.0:4.0)
            @test Dagger.inplace_mpi_parts(ViewBundle(a, view(reshape(rand(4, 4), 4, 4), 1:2, 1:2))) !== nothing
            @test Dagger.inplace_mpi_parts((a, reshape(a, 2, 2))) === nothing   # a reshape shares the memory
            @test Dagger.inplace_mpi_parts((a, a)) === nothing
        end
        @test Dagger.inplace_mpi_parts(Union{Nothing,Vector{Int}}[nothing][1]) === nothing
        b = local_roundtrip(padded_make(10)); @test padded_check(b) && b isa PaddedBundle
        b = local_roundtrip(tuple_make(4)); @test tuple_check(b)
        b = local_roundtrip(padded_make(2)); @test padded_check(b)
    end
    @testset "random values keep their type and contents" begin
        for seed in 1:300
            x = random_make(seed)
            y = local_roundtrip(x)
            @test typeof(y) === typeof(x)
            @test isequal(y, x)
        end
    end
    for (name, make, check, ret) in (("eligible struct", raw_make, raw_check, RawBundle),
                                     ("struct with a String and a Dict", mixed_make, mixed_check, MixedBundle),
                                     ("nested struct", nested_make, nested_check, NestedBundle),
                                     ("shared array", shared_make, shared_check, SharedBundle),
                                     ("inner constructor", guard_make, guard_check, GuardedBundle),
                                     ("padded elements, empty array, Union field", padded_make, padded_check, PaddedBundle),
                                     ("undefined field", undef_make, undef_check, PartlyUndefined),
                                     ("own serialize", own_make, own_check, OwnSerialized))
        @testset "$name moves between ranks" begin
            for n in (3, 100_000)
                a = Dagger.@spawn scope=pinned(0) return_type=ret make(n)
                ok = Dagger.@spawn scope=pinned(other) return_type=Bool check(a)
                @test fetch(ok)
            end
        end
    end
    @testset "tuples and random values move between ranks" begin
        a = Dagger.@spawn scope=pinned(0) return_type=Any tuple_make(1000)
        @test fetch(Dagger.@spawn scope=pinned(other) return_type=Bool tuple_check(a))
        for seed in (3, 17, 42, 99, 123)
            a = Dagger.@spawn scope=pinned(0) return_type=Any random_make(seed)
            r = Dagger.@spawn scope=pinned(other) return_type=Any identity(a)
            @test isequal(fetch(r), random_make(seed))
        end
    end
    MPI.Barrier(comm)
end
