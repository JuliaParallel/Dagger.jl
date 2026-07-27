module DistributionsExt

if isdefined(Base, :get_extension)
    using Distributions
else
    using ..Distributions
end

using Dagger, Random
import Dagger: chunks, imap!, InOut, randfork

# Default-RNG path: each task uses Julia's task-local default RNG, so concurrent
# chunk tasks do not share mutable RNG state.
Random.rand!(s::Sampleable, A::DArray{T}) where T = map!(_ -> rand(s), A)

# Explicit-RNG paths must fork a distinct RNG per chunk (and bind it in `let`),
# matching `Random.rand!(::AbstractRNG, ::DArray)`. Otherwise `map!` would capture
# one shared RNG in a single closure and race under multithreading — corrupting
# RNG state and yielding out-of-range / NaN values.
function Random.rand!(rng::AbstractRNG, s::Sampleable{Univariate}, A::DArray{T}) where T
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = randfork(rng, part_sz)
            let rng = rng, s = s
                Dagger.@spawn imap!(InOut(_ -> rand(rng, s)), InOut(Ac))
            end
        end
    end
    return A
end
function Random.rand!(rng::AbstractRNG, s::Sampleable{ArrayLikeVariate{M}}, A::DArray{T}) where {M,T}
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = randfork(rng, part_sz)
            let rng = rng, s = s
                Dagger.@spawn imap!(InOut(_ -> rand(rng, s)), InOut(Ac))
            end
        end
    end
    return A
end

end # module DistributionsExt
