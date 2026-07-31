module DistributionsExt

if isdefined(Base, :get_extension)
    using Distributions
else
    using ..Distributions
end

using Dagger, Random

Random.rand!(s::Sampleable, A::DArray{T}) where T = map!(_ -> rand(s), A)
Random.rand!(rng::AbstractRNG, s::Sampleable{Univariate}, A::DArray{T}) where T = map!(_ -> rand(rng, s), A)
Random.rand!(rng::AbstractRNG, s::Sampleable{ArrayLikeVariate{M}}, A::DArray{T}) where {M, T} = map!(_ -> rand(rng, s), A)

end # module DistributionsExt
