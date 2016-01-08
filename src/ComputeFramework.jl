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
A layout pattern. Implements `partition` and `gather` methods
"""
abstract AbstractLayout

include("dist-data.jl")
include("layout.jl")
include("redistribute.jl")

include("basic-nodes.jl")
include("getindex.jl")
include("map-reduce.jl")
include("sort.jl")

include("context.jl")
include("accumulator.jl")
include("macros.jl")
include("optimize.jl")

include("file-nodes.jl")


include("show.jl")

end # module
