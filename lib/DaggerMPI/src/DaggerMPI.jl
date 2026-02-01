module DaggerMPI
using Dagger
import Base: reduce, fetch, cat
using MPI

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

    # TODO: Make better load balancing

    data = Array{T, N}(undef, dist.blocksize)
    if isroot
        cs = Array{T, N}(undef, size(x))
        parts = partition(dist, domain(x))
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

    return Dagger.DArray(T, domain(data), domain(data), data, dist)
end

function Dagger.distribute(::Type{A},
                           dist::MPIBlocks,
                           comm::MPI.Comm=MPI.COMM_WORLD,
                           root::Integer=0) where {A<:AbstractArray{T, N}} where {T, N}
    return distribute(A, nothing, dist, comm, root)
end

function Dagger.distribute(x::AbstractArray,
                           dist::MPIBlocks,
                           comm::MPI.Comm=MPI.COMM_WORLD,
                           root::Integer=0)
    return distribute(typeof(x), x, dist, comm, root)
end

function Base.reduce(f::Function, x::Dagger.DArray{T,N,MPIBlocks{N}};
                     dims=nothing,
                     comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=true) where {T,N}
    if dims === nothing
        if !acrossranks
            return fetch(Dagger.reduce_async(f,x))
        elseif root === nothing
            return MPI.Allreduce(fetch(Dagger.reduce_async(f,x)), f, comm)
        else
            return MPI.Reduce(fetch(Dagger.reduce_async(f,x)), f, comm; root)
        end
    else
        if dims isa Int
            dims = (dims,)
        end
        d = reduce(x.domain, dims=dims)
        ds = reduce(x.subdomains[1], dims=dims)
        if !acrossranks
            thunks = Dagger.spawn(b->reduce(f, b, dims=dims), x.chunks[1])
            return Dagger.DArray(T, d, ds, thunks, x.partitioning; concat=x.concat)
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
                if root != MPI.Comm_rank(comm)
                    return nothing
                end
                chunks = Dagger.tochunk(reshape(rcvbuf, size(tmp)))
            end
            return Dagger.DArray(T, d, ds, chunks, x.partitioning; concat=x.concat)
        end
    end
end

end # module
