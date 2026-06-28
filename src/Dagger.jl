module Dagger

import Serialization
import Serialization: AbstractSerializer, serialize, deserialize
import SparseArrays: sprand, SparseMatrixCSC

import MemPool
import MemPool: DRef, FileRef, poolget, poolset

import Base: collect, reduce, view
import NextLA
import LinearAlgebra
import LinearAlgebra: Adjoint, BLAS, Diagonal, Bidiagonal, Tridiagonal, LAPACK, LU, LowerTriangular, PosDefException, Transpose, UpperTriangular, UnitLowerTriangular, UnitUpperTriangular, Cholesky, diagind, ishermitian, issymmetric, I
import Random
import Random: AbstractRNG

import UUIDs: UUID, uuid4

if !isdefined(Base, :ScopedValues)
    import ScopedValues: ScopedValue, @with, with
else
    import Base.ScopedValues: ScopedValue, @with, with
end
import TaskLocalValues: TaskLocalValue

import TimespanLogging
import TimespanLogging: timespan_start, timespan_finish

import Adapt

import GPUArraysCore

import Preferences: @load_preference, @set_preferences!

if @load_preference("distributed-package") == "DistributedNext"
    import DistributedNext
    import DistributedNext: Future, RemoteChannel, myid, workers, nworkers, procs, remotecall, remotecall_wait, remotecall_fetch, check_same_host
else
    import Distributed
    import Distributed: Future, RemoteChannel, myid, workers, nworkers, procs, remotecall, remotecall_wait, remotecall_fetch, check_same_host
end

import MacroTools: @capture, prewalk

import KernelAbstractions
import KernelAbstractions: @kernel, @index
import Adapt

include("lib/util.jl")
include("utils/dagdebug.jl")

# Distributed data
include("utils/locked-object.jl")
include("utils/tasks.jl")
include("utils/reuse.jl")
include("processor.jl")
include("threadproc.jl")
include("sch_options.jl")
include("context.jl")
include("utils/processors.jl")
include("scopes.jl")
include("utils/scopes.jl")
include("chunks.jl")
include("utils/signature.jl")
include("thunkid.jl")
include("utils/lfucache.jl")
include("options.jl")
include("dtask.jl")
include("cancellation.jl")
include("task-tls.jl")
include("argument.jl")
include("queue.jl")
include("thunk.jl")
include("utils/fetch.jl")
include("utils/chunks.jl")
include("utils/logging.jl")
include("submission.jl")
abstract type MemorySpace end
include("utils/memory-span.jl")
include("utils/interval_tree.jl")
include("memory-spaces.jl")

# Task scheduling
include("compute.jl")
include("utils/clock.jl")
include("utils/system_uuid.jl")
include("utils/caching.jl")
include("sch/Sch.jl"); using .Sch

# Data dependency task queue
include("datadeps/aliasing.jl")
include("datadeps/chunkview.jl")
include("datadeps/remainders.jl")
include("datadeps/scheduling.jl")
include("datadeps/queue.jl")

# Stencils
include("utils/haloarray.jl")
include("array/stencil.jl")

# Streaming
include("stream.jl")
include("stream-buffers.jl")
include("stream-transfer.jl")

# File IO
include("file-io.jl")

# Array computations
include("array/darray.jl")
include("array/alloc.jl")
include("array/map-reduce.jl")
include("array/copy.jl")
include("array/random.jl")
include("array/operators.jl")
include("array/indexing.jl")
include("array/setindex.jl")
include("array/matrix.jl")
include("array/sparse_partition.jl")
include("array/sort.jl")
include("array/permute.jl")
include("array/linalg.jl")
include("array/mul.jl")
include("array/cholesky.jl")
include("array/trsm.jl")
include("array/lu.jl")
include("array/qr.jl")

# GPU
include("gpu.jl")

# Logging
include("utils/logging-events.jl")

# Visualization
include("visualization.jl")
include("ui/gantt-common.jl")
include("ui/gantt-text.jl")
include("utils/viz.jl")

"""
    set_distributed_package!(value[="Distributed|DistributedNext"])

Set a [preference](https://github.com/JuliaPackaging/Preferences.jl) for using
either the Distributed.jl stdlib or DistributedNext.jl. You will need to restart
Julia after setting a new preference.
"""
function set_distributed_package!(value)
    MemPool.set_distributed_package!(value)
    TimespanLogging.set_distributed_package!(value)

    @set_preferences!("distributed-package" => value)
    @info "Dagger.jl preference has been set, restart your Julia session for this change to take effect!"
end

# Precompilation
import PrecompileTools: @compile_workload
include("precompile.jl")

function __init__()
    # Clear any precompile-cached UUID for this process: the precompile workload
    # runs system_uuid() and the resulting SYSTEM_UUIDS entry gets baked into
    # the compiled image. Without clearing it here, get!() would return that
    # stale build-time UUID instead of reading the actual runtime UUID file,
    # causing mismatches between process 1 and workers.
    delete!(SYSTEM_UUIDS, myid())
    system_uuid()


    for tid in 1:Threads.nthreads()
        add_processor_callback!("__cpu_thread_$(tid)__") do
            ThreadProc(myid(), tid)
        end
    end

    # Set up @dagdebug categories, if specified
    try
        if haskey(ENV, "JULIA_DAGGER_DEBUG")
            empty!(DAGDEBUG_CATEGORIES)
            for category in split(ENV["JULIA_DAGGER_DEBUG"], ",")
                if category != ""
                    push!(DAGDEBUG_CATEGORIES, Symbol(category))
                end
            end
        end
    catch err
        @warn "Error parsing JULIA_DAGGER_DEBUG" exception=err
    end
end

end # module
