module DaggerMPI

using Dagger, SparseArrays, Random
import Dagger: DArray

import Statistics: sum, prod, mean
import Base: reduce, fetch, cat, prod, sum
using MPI

export MPIBlocks, MPIParallelBlocks

include("blocks.jl")
include("parallel-blocks.jl")

end # module
