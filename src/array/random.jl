Random.rand!(dm::DArray{T}) where T = map!(_ -> rand(T), dm)
Random.randn!(dm::DArray{T}) where T = map!(_ -> randn(T), dm)

randfork(rng::R, n::Integer) where {R<:AbstractRNG} =
    R(abs(rand(copy(rng), Int) + n))

function Random.rand!(rng::AbstractRNG, A::DArray{T}) where T
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = randfork(rng, part_sz)
            # N.B. Bind the forked RNG in a fresh `let` scope so each spawned
            # task captures its own RNG object. Otherwise the closure captures
            # the loop-reassigned (and thus boxed, shared) `rng` variable, so
            # all block tasks would run against a single RNG concurrently --
            # racing on its non-thread-safe mutable state (which can corrupt it
            # and throw, e.g. a `BoundsError`) and using identical seeds.
            let rng = rng
                Dagger.@spawn imap!(InOut(_->rand(rng, T)), InOut(Ac))
            end
        end
    end
    return A
end
function Random.randn!(rng::AbstractRNG, A::DArray{T}) where T
    part_sz = prod(map(length, first(A.subdomains).indexes))
    Dagger.spawn_datadeps() do
        for Ac in chunks(A)
            rng = randfork(rng, part_sz)
            # See `rand!` above: a per-iteration `let` gives each task its own
            # RNG, avoiding a data race on a shared RNG under multithreading.
            let rng = rng
                Dagger.@spawn imap!(InOut(_->randn(rng, T)), InOut(Ac))
            end
        end
    end
    return A
end
