module ComputeFramework

using Compat

include("lib/util.jl")
include("basics/logging.jl")

# Data and sub-data
include("basics/domain.jl")
include("basics/partition.jl")
include("basics/data.jl")

# Task scheduling
include("basics/processor.jl")
include("basics/thunk.jl")
include("basics/compute.jl")

# File IO
#include("sparse.jl")
include("basics/file-io.jl")

# Array computations
include("array/map-reduce.jl")
include("array/read-delim.jl")

include("array/alloc.jl")
include("array/operators.jl")
include("array/getindex.jl")
include("array/matrix.jl")

include("array/show.jl")

include("ui/graph.jl")

end # module
