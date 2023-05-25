module DaggerMPI
using Dagger
using MPI

bcastDict = Base.Dict{Int32, MPI.Buffer}()


struct MPIProcessor{P,C} <: Dagger.Processor
    proc::P
    comm::MPI.Comm
    color_algo::C
end

struct SimpleColoring end
function (sc::SimpleColoring)(comm, key)
    return UInt64(rem(key, MPI.Comm_size(comm)))
end

const MPI_PROCESSORS = Ref{Int}(-1)

const PREVIOUS_PROCESSORS = Set()

function initialize(comm::MPI.Comm=MPI.COMM_WORLD; color_algo=SimpleColoring())
    @assert MPI_PROCESSORS[] == -1 "DaggerMPI already initialized"

    # Force eager_thunk to run
    fetch(Dagger.@spawn 1+1)

    MPI.Init(; finalize_atexit=false)
    procs = Dagger.get_processors(OSProc())
    i = 0
    empty!(Dagger.PROCESSOR_CALLBACKS)
    empty!(Dagger.OSPROC_PROCESSOR_CACHE)
    for proc in procs
        Dagger.add_processor_callback!("mpiprocessor_$i") do
            return MPIProcessor(proc, comm, color_algo)
        end
        i += 1
    end
    MPI_PROCESSORS[] = i

    # FIXME: Hack to populate new processors
    Dagger.get_processors(OSProc())

    return nothing
end

function finalize()
    @assert MPI_PROCESSORS[] > -1 "DaggerMPI not yet initialized"
    for i in 1:MPI_PROCESSORS[]
        Dagger.delete_processor_callback!("mpiprocessor_$i")
    end
    empty!(Dagger.PROCESSOR_CALLBACKS)
    empty!(Dagger.OSPROC_PROCESSOR_CACHE)
    i = 1
   for proc in PREVIOUS_PROCESSORS
        Dagger.add_processor_callback!("old_processor_$i") do
            return proc
        end
        i += 1
    end
    empty!(PREVIOUS_PROCESSORS)
    MPI.Finalize()
    MPI_PROCESSORS[] = -1
end

"References a value stored on some MPI rank."
struct MPIColoredValue{T}
    color::UInt64
    value::T
    comm::MPI.Comm
end

Dagger.get_parent(proc::MPIProcessor) = Dagger.OSProc()
Dagger.default_enabled(proc::MPIProcessor) = true


"Busy-loop Irecv that yields to other tasks."
function recv_yield(src, tag, comm)
    while true 
        (got, msg, stat) = MPI.Improbe(src, tag, comm, MPI.Status)
        if got
            count = MPI.Get_count(stat, UInt8)
            buf = Array{UInt8}(undef, count)
            req = MPI.Imrecv!(MPI.Buffer(buf), msg)
            while true
                finish = MPI.Test(req)
                if finish
                    value = MPI.deserialize(buf)
                    return value
                end
                yield()
            end
        end
        # TODO: Sigmoidal backoff
        yield()
    end
end

function Dagger.execute!(proc::MPIProcessor, f, args...)
    rank = MPI.Comm_rank(proc.comm)
    tag = abs(Base.unsafe_trunc(Int32, Dagger.get_task_hash() >> 32))
    color = proc.color_algo(proc.comm, tag)
    if rank == color
        @debug "[$rank] Executing $f on $tag"
        return MPIColoredValue(color, Dagger.execute!(proc.proc, f, args...), proc.comm)
    end
    # Return nothing, we won't use this value anyway
    @debug "[$rank] Skipped $f on $tag"
    return MPIColoredValue(color, nothing, proc.comm)
end

function Dagger.move(from_proc::MPIProcessor, to_proc::MPIProcessor, x::Dagger.Chunk)
    @assert from_proc.comm == to_proc.comm "Mixing different MPI communicators is not supported"
    @assert Dagger.chunktype(x) <: MPIColoredValue
    x_value = fetch(x)
    rank = MPI.Comm_rank(from_proc.comm)
    tag = abs(Base.unsafe_trunc(Int32, Dagger.get_task_hash(:input) >> 32))
    other_tag = abs(Base.unsafe_trunc(Int32, Dagger.get_task_hash(:self) >> 32))
    other = from_proc.color_algo(from_proc.comm, other_tag)
    if x_value.color == rank == other
        # We generated and will use this input
        return Dagger.move(from_proc.proc, to_proc.proc, x_value.value)
    elseif x_value.color == rank
        # We generated this input
        @debug "[$rank] Starting P2P send to [$other] from $tag to $other_tag"
        MPI.isend(x_value.value, other, tag, from_proc.comm)
        @debug "[$rank] Finished P2P send to [$other] from $tag to $other_tag"
        return Dagger.move(from_proc.proc, to_proc.proc, x_value.value)
    elseif other == rank
        # We will use this input
        @debug "[$rank] Starting P2P recv from $tag to $other_tag"
        value = recv_yield(x_value.color, tag, from_proc.comm)
        @debug "[$rank] Finished P2P recv from $tag to $other_tag"
        return Dagger.move(from_proc.proc, to_proc.proc, value)
    else
        # We didn't generate and will not use this input
        return nothing
    end
end

function Dagger.move(from_proc::MPIProcessor, to_proc::Dagger.Processor, x::Dagger.Chunk)
    @assert Dagger.chunktype(x) <: MPIColoredValue
    x_value = fetch(x)
    rank = MPI.Comm_rank(from_proc.comm)
    tag = abs(Base.unsafe_trunc(Int32, Dagger.get_task_hash(:input) >> 32)) 
    if rank == x_value.color
        # FIXME: Broadcast send
        @sync for other in 0:(MPI.Comm_size(from_proc.comm)-1)
            other == rank && continue
            @async begin
                @debug "[$rank] Starting bcast send to [$other] on $tag"
                MPI.isend(x_value.value, other, tag, from_proc.comm)
                @debug "[$rank] Finished bcast send to [$other] on $tag"
            end
        end
        return Dagger.move(from_proc.proc, to_proc, x_value.value)
    else
        @debug "[$rank] Starting bcast recv on $tag"
        value = recv_yield(x_value.color, tag, from_proc.comm)
        @debug "[$rank] Finished bcast recv on $tag"
        return Dagger.move(from_proc.proc, to_proc, value)
    end
end

function Dagger.move(from_proc::Dagger.Processor, to_proc::MPIProcessor, x::Dagger.Chunk)
    @assert !(Dagger.chunktype(x) <: MPIColoredValue)
    rank = MPI.Comm_rank(to_proc.comm)
    return MPIColoredValue(rank, Dagger.move(from_proc, to_proc.proc, x), from_proc.comm)
end

end # module
