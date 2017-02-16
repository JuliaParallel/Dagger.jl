import Base.procs
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
type Context
    procs::Array{Processor}
    log_sink::Any
    profile::Bool
end

function Context(xs)
    Context(xs, NoOpLog(), false) # By default don't log events
end
Context(xs::Array{Int}) = Context(map(OSProc, xs))
Context() = Context(workers())
procs(c::Context) = c.procs

"""
Write a log event
"""
function write_event(ctx::Context, event::Event)
    write_event(ctx.log_sink, event)
end
#gather(x::AbstractChunk) = gather(Context(), x)


#####

#=

"""
Affinity maps processors to various chunks of a chunk.
"""
immutable Affinity
    procs::Vector
    chunks::Domain
end

"""
A `AffineParts` represents a distributed object
at various processors according to an `Affinity`

Fields:
    - affinity: Affinity of subdomains
    - chunks: A `Cat` where data is already moved to
             respect the affinity.
"""
immutable AffineParts <: AbstractChunk
    affinity::Affinity
    chunks::AbstractChunk
end

affinity(a::AffineParts) = a.affinity
chunks(a::AffineParts) = a.chunks

parttype(c::AffineParts) = parttype(chunks(c))
partsize(c::AffineParts) = partsize(chunks(c))
domain(c::AffineParts)    = affinity(c).chunks
gather(ctx, c::AffineParts) = gather(ctx, chunks(c))

partition(c::AffineParts) = partition(chunks(c))
=#
