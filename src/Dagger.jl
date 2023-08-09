module Dagger

using Distributed, SharedArrays, Serialization

using MemPool

import Base: collect, adjoint, reduce
import Distributed: procs

using LinearAlgebra
import LinearAlgebra: transpose

using UUIDs

import ContextVariablesX

using Requires
using MacroTools
using TimespanLogging

include("lib/util.jl")
include("utils/dagdebug.jl")

# Distributed data
include("options.jl")
include("processor.jl")
include("scopes.jl")
include("eager_thunk.jl")
include("queue.jl")
include("thunk.jl")
include("submission.jl")
include("chunks.jl")

# Task scheduling
include("compute.jl")
include("utils/clock.jl")
include("utils/system_uuid.jl")
include("utils/locked-object.jl")
include("utils/caching.jl")
include("sch/Sch.jl"); using .Sch

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

# Other
include("ui/graph-core.jl")
include("ui/gantt-common.jl")
include("ui/gantt-text.jl")

# Logging
include("lib/logging-events.jl")

# For interoperation of gantt-luxor and video:
_combine_gantt_images(::Any, ::Any, ::Any, ::Any) = ("", "")

# For interoperation of gantt-luxor and profilesvg:
_prof_to_svg(::Any, ::Any, ::Any; kwargs...) = nothing

@static if !isdefined(Base, :get_extension)
    using Requires
end

function __init__()
    # Initialize system UUID
    system_uuid()

    @static if !isdefined(Base, :get_extension)
        @require Colors="5ae59095-9a9b-59fe-a467-6f913c188581" begin
            include("../ext/DaggerColorsExt.jl")
            @require Luxor="ae8d54c2-7ccd-5906-9d76-62fc9837b5bc" begin
                # Gantt chart renderer
                include("../ext/DaggerLuxorExt.jl")
            end
        end
        @require Mux="a975b10e-0019-58db-a62f-e48ff68538c9" begin
            # Gantt chart HTTP server
            include("../ext/DaggerMuxExt.jl")
        end

        @require ProfileSVG="132c30aa-f267-4189-9183-c8a63c7e05e6" begin
            # Profile renderer
            include("../ext/DaggerProfileSVGExt.jl")
        end
        @require FFMPEG="c87230d0-a227-11e9-1b43-d7ebe4e7570a" begin
            # Video generator
            include("../ext/DaggerFFMPEGExt.jl")
        end
    end

    for tid in 1:Threads.nthreads()
        add_processor_callback!("__cpu_thread_$(tid)__") do
            ThreadProc(myid(), tid)
        end
    end
end

end # module
