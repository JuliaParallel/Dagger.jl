using Random

# Hide Future in a module to not mess with Distributed.Future
module RandJump
    using Random
    using Future

    function randjump(rng::AbstractRNG, n::Integer)
        new_rng = copy(rng)
        for _ in 1:n
            rand(new_rng)
        end
        return new_rng
    end
    randjump(rng::MersenneTwister, n::Integer) = Future.randjump(rng, n)
end # module RandJump

function Random.rand!(dm::DArray{T}) where T
    # Advance the RNG to satisy the rand API
    rand()
    return map!(_ -> rand(T), dm)
end
function Random.randn!(dm::DArray{T}) where T
    # Advance the RNG to satisy the rand API
    randn()
    return map!(_ -> randn(T), dm)
end

function Random.rand!(rng::AbstractRNG, A::DArray{T}) where T
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = RandJump.randjump(rng, part_sz)
            new_rng = copy(rng)
            Dagger.@spawn map!(_->rand(new_rng, T), InOut(Ac), Ac)
        end
    end
    return A
end
function Random.randn!(rng::AbstractRNG, A::DArray{T}) where T
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = RandJump.randjump(rng, part_sz)
            new_rng = copy(rng)
            Dagger.@spawn map!(_->randn(new_rng, T), InOut(Ac), Ac)
        end
    end
    return A
end
