using Statistics, Distributions

Random.rand!(dm::DArray{T}) where T = map!(() -> rand(T), dm)
Random.rand!(s::Sampleable, dm::DArray{T}) where T = map!(() -> rand(s), dm)
Random.rand!(s::Sampleable{Univariate}, dm::DArray{T, N, B}) where T = map!(() -> rand(s), dm)
Random.rand!(s::Sampleable{ArrayLikeVariate{M}}, dm::DArray{T}) where {M, T} = map!(() -> rand(s), dm)
Random.rand!(rng::AbstractRNG, dm::DArray{T}) where T = map!(() -> rand(rng), dm)
Random.randn!(dm::DArray{T}) where T = map!(() -> randn(), dm)
Random.randn!(rng::AbstractRNG, dm::DArray{T}) where T = map!(() -> randn(rng), dm)
