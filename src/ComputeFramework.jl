module ComputeFramework

include("util.jl")

# Data and sub-data
include("domain.jl")
include("partition.jl")
include("part.jl")

# Task scheduling
include("processor.jl")
include("thunk.jl")
include("scheduler.jl")

# Extras
include("sparse.jl")
include("alloc.jl")
include("operators.jl")
#include("file-io.jl")

end # module
