module DaggerGraphs

using Dagger
import Dagger: Chunk, LockedObject, @safe_lock1
using Distributed
using Graphs
import OffsetArrays: OffsetArray
import Tables
using ScopedValues
using LRUCache

export DGraph

include("dgraph.jl")
include("adjlist.jl")
include("edgeiter.jl")
include("operations.jl")
include("io.jl")
include("tables.jl")

end # module DaggerGraphs
