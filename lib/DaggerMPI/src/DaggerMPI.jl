module DaggerMPI
using Dagger
import Base: reduce, fetch, cat
using MPI

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

#const BCAST_VALUES = Dict{Any, Any}()

const PREVIOUS_PROCESSORS = Set()


function initialize(comm::MPI.Comm=MPI.COMM_WORLD; color_algo=SimpleColoring())
    @assert MPI_PROCESSORS[] == -1 "DaggerMPI already initialized"

    # Force eager_thunk to run
    fetch(Dagger.@spawn 1+1)

    MPI.Init(; finalize_atexit=false)
    procs = Dagger.get_processors(OSProc())
    i = 0
    #=empty!(Dagger.PROCESSOR_CALLBACKS)
    empty!(Dagger.OSPROC_PROCESSOR_CACHE)
    for proc in procs
        Dagger.add_processor_callback!("mpiprocessor_$i") do
            return MPIProcessor(proc, comm, color_algo)
        end
        i += 1
    end=#
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

export MPIBlocks

struct MPIBlocks{N} <: Dagger.AbstractSingleBlocks{N}
    blocksize::NTuple{N, Int}
end
MPIBlocks(xs::Int...) = MPIBlocks(xs)

function Dagger.distribute(::Type{A}, 
                           x::Union{AbstractArray, Nothing}, 
                           dist::MPIBlocks, 
                           comm::MPI.Comm=MPI.COMM_WORLD, 
                           root::Integer=0) where {A<:AbstractArray{T, N}} where {T, N}
    isroot = MPI.Comm_rank(comm) == root

    #TODO: Make better load balancing

    data = Array{T, N}(undef, dist.blocksize)
    if isroot
        cs = Array{T, N}(undef, size(x))
        parts =  partition(dist, domain(x))
        idx = 1
        for part in parts
            cs[idx:(idx - 1 + prod(dist.blocksize))] = x[part]
            idx += prod(dist.blocksize)
        end
        MPI.Scatter!(MPI.UBuffer(cs, div(length(cs), MPI.Comm_size(comm))), data, comm, root=root)
    else
        MPI.Scatter!(nothing, data, comm, root=root)
    end

    data = Dagger.tochunk(data)

    Dagger.DArray(
        T,
        domain(data),
        domain(data),
        data,
        dist
       )
end

function Dagger.distribute(::Type{A}, 
                    dist::MPIBlocks, 
                    comm::MPI.Comm=MPI.COMM_WORLD, 
                    root::Integer=0) where {A<:AbstractArray{T, N}} where {T, N}
    distribute(A, nothing, dist, comm, root)
end

function Dagger.distribute(x::AbstractArray, 
                    dist::MPIBlocks, 
                    comm::MPI.Comm=MPI.COMM_WORLD, 
                    root::Integer=0)
    distribute(typeof(x), x, dist, comm, root)
end      

export MPIBlocks, distribute

function Base.reduce(f::Function, x::Dagger.DArray{T,N,MPIBlocks{N},F}; comm=MPI.COMM_WORLD, root=nothing, dims=nothing, acrossranks::Bool=true) where {T,N,F} 
    if dims === nothing
        if !acrossranks
            return fetch(reduce_async(f,x))
        elseif root === nothing
            return MPI.Allreduce(fetch(reduce_async(f,x)), f, comm)
        else 
            return MPI.Reduce(fetch(reduce_async(f,x)), f, comm; root)
        end 
    else
        if dims isa Int 
            dims = (dims,)
        end
        d = reduce(x.domain, dims=dims)
        ds = reduce(x.subdomains[1], dims=dims)
        if !acrossranks   
            thunks = Dagger.spawn(b->reduce(f, b, dims=dims), x.chunks[1])
            return Dagger.DArray(T, 
                                 d, 
                                 ds, 
                                 thunks,
                                 x.partitioning)
        else
            tmp = collect(reduce(f, x, comm=comm, root=root, dims=dims, acrossranks=false))
            if root === nothing
                h = UInt(0)
                for dim in 1:N
                    if dim in dims
                        continue
                    end
                    h = hash(x.subdomains[1].indexes[dim], h)
                end
                h = abs(Base.unsafe_trunc(Int32, h))
                newc = MPI.Comm_split(comm, h, MPI.Comm_rank(comm))
                chunks = Dagger.tochunk(reshape(MPI.Allreduce(tmp, f, newc), size(tmp)))
            else
                rcvbuf = MPI.Reduce(tmp, f, comm; root)
                if root === MPI.Comm_rank(comm)
                    chunks = Dagger.tochunk(reshape(rcvbuf, size(tmp)))
                    return Dagger.DArray(T,
                                         d,
                                         ds,
                                         chunks,
                                         x.partitioning)
                end
                return nothing
            end
        end
    end
end

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
        @debug "[$rank] Starting bcast send on $tag"
        @sync for other in 0:(MPI_Comm_size(from_proc.comm)-1)
            other == rank && continue
            @async begin
                @debug "[$rank] Starting bcast send on $tag"
                MPI.isend(x_value.value, other, tag, from_proc.comm)
                @debug "[$rank] Finished bcast send on $tag"
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
