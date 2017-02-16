module Dagger

using Compat

include("lib/util.jl")
include("lib/logging.jl")

# Data and slices of data
include("domain.jl")
include("lib/blocked-domains.jl")
include("chunks.jl")

# Task scheduling
include("processor.jl")
include("thunk.jl")
include("compute.jl")

# File IO
#include("sparse.jl")
include("file-io.jl")

# Array computations
include("array/lazy-array.jl")
include("array/alloc.jl")
include("array/map-reduce.jl")

include("array/operators.jl")
include("array/getindex.jl")
include("array/setindex.jl")
include("array/matrix.jl")
include("array/sparse_partition.jl")
include("array/sort.jl")

include("array/show.jl")

include("ui/graph.jl")

end # module
