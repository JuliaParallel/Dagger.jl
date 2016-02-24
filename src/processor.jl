export OSProc, Context

abstract Processor

"""
OS process - contains pid returned by `addprocs`
"""
immutable OSProc <: Processor
    pid::Int
end

"""
A context represents a set of processors to use
for a papply operation.
"""
immutable Context
    procs::Array{Processor}
end
Context() = Context(map(OSProc, workers()))
procs(c::Context) = c.procs

#gather(x::AbstractPart) = gather(Context(), x)


#####

#=

"""
Affinity maps processors to various parts of a part.
"""
immutable Affinity
    procs::Vector
    parts::Domain
end

"""
A `AffineParts` represents a distributed object
at various processors according to an `Affinity`

Fields:
    - affinity: Affinity of subdomains
    - parts: A `Cat` where data is already moved to
             respect the affinity.
"""
immutable AffineParts <: AbstractPart
    affinity::Affinity
    parts::AbstractPart
end

affinity(a::AffineParts) = a.affinity
parts(a::AffineParts) = a.parts

parttype(c::AffineParts) = parttype(parts(c))
partsize(c::AffineParts) = partsize(parts(c))
domain(c::AffineParts)    = affinity(c).parts
gather(ctx, c::AffineParts) = gather(ctx, parts(c))

partition(c::AffineParts) = partition(parts(c))
=#
