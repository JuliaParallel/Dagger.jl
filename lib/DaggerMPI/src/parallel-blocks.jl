struct MPIParallelBlocks{N} <: Dagger.AbstractSingleBlocks{N} end

function wrap_chunk(chunk::Dagger.Chunk, N::Integer)
    chunks = Array{Any, N}(undef, ntuple(_->1, N))
    chunks[1] = chunk
    return chunks
end

function _finish_allocation(f::Function, dist::MPIParallelBlocks, dims::NTuple{N,Int}, comm::MPI.Comm, root::Integer) where N
    d = ArrayDomain(map(x->1:x, dims))
    s = Dagger.DomainBlocks(ntuple(_->1, N),
                            ntuple(i->[dims[i]], N))
    data = f(dims)
    chunk = Dagger.tochunk(data)
    return Dagger.DArray(eltype(data), d, s, wrap_chunk(chunk, N), dist)
end

function Base.rand(dist::MPIParallelBlocks, ::Type{ET}, dims, comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0) where {ET}
    rank = MPI.Comm_rank(comm)
    s = rand(UInt)
    f(block) = rand(MersenneTwister(s+rank), ET, block)
    _finish_allocation(f, dist, dims, comm, root)
end
Base.rand(dist::MPIParallelBlocks, t::Type, dims::Integer...; comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0) = rand(dist, t, dims, comm, root)
Base.rand(dist::MPIParallelBlocks, dims::Integer...; comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0) = rand(dist, Float64, dims, comm, root)
Base.rand(dist::MPIParallelBlocks, dims::Tuple, comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0) = rand(dist, Float64, dims, comm, root)

function Dagger.distribute(data::AbstractArray{T,N}, dist::MPIParallelBlocks{N}) where {T,N}
    dims = size(data)
    d = ArrayDomain(map(x->1:x, dims))
    s = Dagger.DomainBlocks(ntuple(_->1, N),
                            ntuple(i->[dims[i]], N))
    chunk = Dagger.tochunk(data)
    return Dagger.DArray(T, d, s, wrap_chunk(chunk, N), dist)
end

function Base.collect(x::DArray{T,N,<:MPIParallelBlocks} where {T,N})
    return fetch(only(x.chunks))
end

function Base.map!(f::Function,
                   x::Dagger.DArray{T1,N1,MPIParallelBlocks{N1}} where {T1,N1},
                   y::Dagger.DArray{T2,N2,MPIParallelBlocks{N2}} where {T2,N2})
    x_local = fetch(x.chunks[1])
    y_local = fetch(x.chunks[1])
    map!(f, x_local, y_local)
end

function Base.reduce(f::Function, x::Dagger.DArray{T,N,MPIParallelBlocks{N}};
                     dims=:,
                     comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=false) where {T,N}
    if !acrossranks
        return fetch(Dagger.@spawn reduce(f, x.chunks[1]; dims=dims))
    end
    if dims == Base.:(:)
        localpart = fetch(Dagger.reduce_async(f,x))
        if root === nothing
            return MPI.Allreduce(localpart, f, comm)
        else
            return MPI.Reduce(localpart, f, comm; root)
        end
    elseif dims === nothing
        localpart = fetch(x.chunks[1])
        if root === nothing
            return MPI.Allreduce(localpart, f, comm)
        else
            return MPI.Reduce(localpart, f, comm; root)
        end
        return MPI
    else
        error("Not yet implemented")
    end
end
function reduce!(f::Function, x::Dagger.DArray{T,N,MPIParallelBlocks{N}};
                 comm=MPI.COMM_WORLD) where {T,N}
    localpart = fetch(x.chunks[1])
    MPI.Allreduce!(localpart, f, comm)
    return localpart
end
