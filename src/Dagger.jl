module Dagger

using Distributed, SharedArrays

import Base: collect, adjoint, reduce
import Distributed: procs

using LinearAlgebra
import LinearAlgebra: transpose

using Requires

const PLUGINS = Dict{Symbol,Any}()
const PLUGIN_CONFIGS = Dict{Symbol,String}(
    :scheduler => "Dagger.Sch"
)

include("lib/util.jl")
include("lib/logging.jl")

# Distributed data
include("processor.jl")
include("thunk.jl")
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

include("ui/graph.jl")

function __init__()
    @require Luxor="ae8d54c2-7ccd-5906-9d76-62fc9837b5bc" begin
        @require Mux="a975b10e-0019-58db-a62f-e48ff68538c9" begin
            # Gantt chart server
            include("ui/gantt.jl")
        end
    end
    @static if VERSION >= v"1.3.0-DEV.573"
        for tid in 1:Threads.nthreads()
            push!(PROCESSOR_CALLBACKS, proc->ThreadProc(myid(), tid))
        end
    else
        push!(PROCESSOR_CALLBACKS, proc->ThreadProc(myid(), 1))
    end
end

end # module
