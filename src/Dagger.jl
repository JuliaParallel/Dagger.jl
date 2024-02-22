module Dagger

import Serialization
import Serialization: AbstractSerializer, serialize, deserialize
import SparseArrays: sprand, SparseMatrixCSC

import MemPool
import MemPool: DRef, FileRef, poolget, poolset

import Base: collect, reduce
import Distributed
import Distributed: Future, RemoteChannel, myid, workers, nworkers, procs, remotecall, remotecall_wait, remotecall_fetch

import LinearAlgebra
import LinearAlgebra: Adjoint, BLAS, Diagonal, Bidiagonal, Tridiagonal, LAPACK, LowerTriangular, PosDefException, Transpose, UpperTriangular, UnitLowerTriangular, UnitUpperTriangular, diagind, ishermitian, issymmetric

import UUIDs: UUID, uuid4

if !isdefined(Base, :ScopedValues)
    import ScopedValues: ScopedValue, with
else
    import Base.ScopedValues: ScopedValue, with
end

if !isdefined(Base, :get_extension)
    import Requires: @require
end

import TimespanLogging
import TimespanLogging: timespan_start, timespan_finish

include("lib/util.jl")
include("utils/dagdebug.jl")

# Distributed data
include("utils/locked-object.jl")
include("utils/tasks.jl")

import MacroTools: @capture
include("options.jl")
include("processor.jl")
include("threadproc.jl")
include("context.jl")
include("utils/processors.jl")
include("task-tls.jl")
include("scopes.jl")
include("utils/scopes.jl")
include("dtask.jl")
include("queue.jl")
include("thunk.jl")
include("submission.jl")
include("chunks.jl")
include("memory-spaces.jl")

# Task scheduling
include("compute.jl")
include("utils/clock.jl")
include("utils/system_uuid.jl")
include("utils/caching.jl")
include("sch/Sch.jl"); using .Sch

# Data dependency task queue
include("datadeps.jl")

# Array computations
include("array/darray.jl")
include("array/alloc.jl")
include("array/map-reduce.jl")
include("array/copy.jl")

# File IO
include("file-io.jl")

include("array/operators.jl")
include("array/indexing.jl")
include("array/setindex.jl")
include("array/matrix.jl")
include("array/sparse_partition.jl")
include("array/sort.jl")
include("array/linalg.jl")
include("array/mul.jl")
include("array/cholesky.jl")

# Visualization
include("visualization.jl")
include("ui/gantt-common.jl")
include("ui/gantt-text.jl")

# Logging
include("utils/logging-events.jl")
include("utils/logging.jl")

# Precompilation
import PrecompileTools: @compile_workload
include("precompile.jl")

function __init__()
    # Initialize system UUID
    system_uuid()

    @static if !isdefined(Base, :get_extension)
        @require Graphs="86223c79-3864-5bf0-83f7-82e725a168b6" begin
            @require GraphViz="f526b714-d49f-11e8-06ff-31ed36ee7ee0" begin
                include(joinpath(dirname(@__DIR__), "ext", "GraphVizExt.jl"))
            end
        end
        @require Colors="5ae59095-9a9b-59fe-a467-6f913c188581" begin
            include(joinpath(dirname(@__DIR__), "ext", "GraphVizSimpleExt.jl"))
            # TODO: Move to Pkg extensions
            @require Luxor="ae8d54c2-7ccd-5906-9d76-62fc9837b5bc" begin
                # Gantt chart renderer
                include("ui/gantt-luxor.jl")
            end
            @require Mux="a975b10e-0019-58db-a62f-e48ff68538c9" begin
                # Gantt chart HTTP server
                include("ui/gantt-mux.jl")
            end
        end
        # TODO: Move to Pkg extensions
        @require ProfileSVG="132c30aa-f267-4189-9183-c8a63c7e05e6" begin
            # Profile renderer
            include("ui/profile-profilesvg.jl")
        end
        @require FFMPEG="c87230d0-a227-11e9-1b43-d7ebe4e7570a" begin
            @require FileIO="5789e2e9-d7fb-5bc7-8068-2c6fae9b9549" begin
                # Video generator
                include("ui/video.jl")
            end
        end
    end
    for tid in 1:Threads.nthreads()
        add_processor_callback!("__cpu_thread_$(tid)__") do
            ThreadProc(myid(), tid)
        end
    end
end

end # module
