module DistributionsExt

if isdefined(Base, :get_extension)
    using Distributions
else
    using ..Distributions
end

using Dagger, Random

Random.rand!(s::Sampleable, dm::DArray{T}) where T = map!(() -> rand(s), dm)
Random.rand!(s::Sampleable{Univariate}, dm::DArray{T}) where T = map!(() -> rand(s), dm)
Random.rand!(s::Sampleable{ArrayLikeVariate{M}}, dm::DArray{T}) where {M, T} = map!(() -> rand(s), dm)

end # module DistributionsExt