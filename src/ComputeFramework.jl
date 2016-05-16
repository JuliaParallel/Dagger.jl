module ComputeFramework

using Compat

include("util.jl")
include("logging.jl")

# Data and sub-data
include("domain.jl")
include("partition.jl")
include("part.jl")

# Task scheduling
include("processor.jl")
include("thunk.jl")
include("compute.jl")

# Extras
#include("sparse.jl")
include("file-io.jl")
include("map-reduce.jl")
include("read-delim.jl")

# Array computations
include("array/alloc.jl")
include("array/operators.jl")
include("array/getindex.jl")
include("array/matrix.jl")

include("show.jl")

end # module
