module DaggerGraphs

using Dagger
import Dagger: Chunk
using Graphs
import Tables

export DGraph

include("dgraph.jl")
include("adjlist.jl")
include("edgeiter.jl")
include("tables.jl")

function DGraph{T}(x; kwargs...) where T
    if Tables.istable(x)
        return fromtable(T, x; kwargs...)
    end
    throw(ArgumentError("Cannot convert a $(typeof(x)) to a DGraph"))
end
DGraph(x; kwargs...) = DGraph{Int}(x; kwargs...)

end # module DaggerGraphs
