module DaggerGraphs

using Dagger
import Dagger: Chunk
using Distributed
using Graphs
import OffsetArrays: OffsetArray
import Tables

export DGraph

include("dgraph.jl")
include("adjlist.jl")
include("edgeiter.jl")
include("io.jl")
include("tables.jl")

end # module DaggerGraphs
