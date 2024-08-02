Random.rand!(dm::DArray{T}) where T = map!(_ -> rand(T), dm)
Random.randn!(dm::DArray{T}) where T = map!(_ -> randn(T), dm)

randfork(rng::R, n::Integer) where {R<:AbstractRNG} =
    R(abs(rand(copy(rng), Int) + n))

function Random.rand!(rng::AbstractRNG, A::DArray{T}) where T
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = randfork(rng, part_sz)
            Dagger.@spawn map!(_->rand(rng, T), InOut(Ac), Ac)
        end
    end
    return A
end
function Random.randn!(rng::AbstractRNG, A::DArray{T}) where T
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = randfork(rng, part_sz)
            Dagger.@spawn map!(_->randn(rng, T), InOut(Ac), Ac)
        end
    end
    return A
end
