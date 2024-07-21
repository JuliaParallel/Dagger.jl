using Random

Random.rand!(dm::DArray{T}) where T = map!(_ -> rand(T), dm)
Random.rand!(rng::AbstractRNG, dm::DArray{T}) where T = map!(_ -> rand(rng, T), dm)
Random.randn!(dm::DArray{T}) where T = map!(_ -> randn(T), dm)
Random.randn!(rng::AbstractRNG, dm::DArray{T}) where T = map!(_ -> randn(rng, T), dm)
