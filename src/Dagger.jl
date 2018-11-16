module Dagger

using Distributed, SharedArrays

import Base: collect, adjoint, reduce
import Distributed: procs

using LinearAlgebra
import LinearAlgebra: transpose

const PLUGINS = Dict{Symbol,Any}()
const PLUGIN_CONFIGS = Dict{Symbol,String}(
    :scheduler => "Dagger.Sch"
)

include("lib/util.jl")
include("lib/logging.jl")

# Distributed data
include("processor.jl")
include("thunk.jl")
include("domain.jl")
include("chunks.jl")

# Task scheduling
include("compute.jl")
include("scheduler.jl")

# Array computations
include("array/darray.jl")
include("array/alloc.jl")
include("array/map-reduce.jl")

# File IO
include("file-io.jl")

include("array/operators.jl")
include("array/getindex.jl")
include("array/setindex.jl")
include("array/matrix.jl")
include("array/sparse_partition.jl")
include("array/sort.jl")

include("array/show.jl")

include("ui/graph.jl")

end # module
