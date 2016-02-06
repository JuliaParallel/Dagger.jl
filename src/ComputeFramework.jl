module ComputeFramework

using Compat

include("lib/util.jl")

include("domain.jl")
include("partition.jl")
include("chunk.jl")
include("processor.jl")

include("chunk-reader.jl")
include("distribute.jl")

# include("dist-data.jl")
# include("layout.jl")
# include("redistribute.jl")

# include("basic-nodes.jl")
# include("getindex.jl")
# include("map-reduce.jl")
# include("operators.jl")
# include("sort.jl")
#
# include("context.jl")
# include("accumulator.jl")
# include("macros.jl")
# include("optimize.jl")
#
# include("file-nodes.jl")
# include("matrix-ops.jl")


# include("show.jl")

end # module
