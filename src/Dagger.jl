module Dagger

using Compat

include("lib/util.jl")
include("lib/logging.jl")
include("lib/dumbref.jl")

# Data and slices of data
include("thunk.jl")
include("domain.jl")
include("chunks.jl")

# Task scheduling
include("processor.jl")
include("compute.jl")

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
