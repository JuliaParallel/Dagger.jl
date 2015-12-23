module ComputeFramework

using Compat

export compute, gather

"A node in the computation graph"
abstract AbstractNode
"A node that has computed/loaded data in it"
abstract DataNode <: AbstractNode
"A compute node"
abstract ComputeNode <: AbstractNode


"""
    compute(ctx, n::ComputeNode)

Turn a AbstractNode into a DataNode by computing it
"""
function compute(ctx, n::ComputeNode)
    error("Don't know how to compute $(typeof(n))")
end
function compute(ctx, n::DataNode)
    n
end

"""
    gather(ctx, n::DataNode)

Collate a DataNode to return a result
"""
function gather(ctx, n::DataNode)
    error("Don't know how to gather $(typeof(n))")
end
function gather(ctx, n::ComputeNode)
    gather(ctx, compute(ctx, n))
end

"""
Promote a data node from one type to another.
Essentially, relayout / re-arrange to come to
a common ground for the next computation.
"""
function promote_dnode(ctx, a::DataNode, b::DataNode)
    error("Don't know how to promote $(typeof(a)) and $(typeof(a))")
end
function promote_dnode{T<:DataNode}(ctx, a::T, b::T)
    (a, b)
end

function convert_dnode{T<:DataNode}(ctx, to::Type{T}, from)
    error("Don't know how to convert $(typeof(from)) to $(T)")
end

function convert_dnode{T<:DataNode}(ctx, to::Type{T}, from::T)
    from
end

# The layout abstract type

"""
A layout pattern. Implements `slice` and `gather` methods
"""
abstract AbstractLayout

include("compute-nodes.jl")
include("layout.jl")
include("context.jl")
include("accumulator.jl")
include("macros.jl")
include("optimize.jl")

include("dist-memory.jl")
include("file-nodes.jl")

include("redistribute.jl")

include("show.jl")

end # module
