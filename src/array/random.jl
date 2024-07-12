using Random

Random.rand!(dm::DArray{T}) where T = map!(() -> rand(T), dm)
Random.rand!(rng::AbstractRNG, dm::DArray{T}) where T = map!(() -> rand(rng), dm)
Random.randn!(dm::DArray{T}) where T = map!(() -> randn(), dm)
Random.randn!(rng::AbstractRNG, dm::DArray{T}) where T = map!(() -> randn(rng), dm)
