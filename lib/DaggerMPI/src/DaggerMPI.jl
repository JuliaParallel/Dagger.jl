module DaggerMPI
using Dagger
import Base: reduce, fetch, cat
using MPI

export MPIBlocks

struct MPIBlocks{N} <: Dagger.AbstractSingleBlocks{N}
    blocksize::NTuple{N, Union{Int, Nothing}}
end
MPIBlocks(xs::Union{Int,Nothing}...) = MPIBlocks(xs)

function Dagger.distribute(::Type{A},
                           x::Union{AbstractArray, Nothing},
                           dist::MPIBlocks,
                           comm::MPI.Comm=MPI.COMM_WORLD,
                           root::Integer=0) where {A<:AbstractArray{T, N}} where {T, N}
    isroot = MPI.Comm_rank(comm) == root
    csz = MPI.Comm_size(comm)
    if any(isnothing, dist.blocksize)
        newdims = map(collect(dist.blocksize)) do d
            something(d, 1)
        end
        if isroot
            for i in 1:N
                if dist.blocksize[i] !== nothing
                    continue
                end
                if csz * prod(newdims) >= length(x)
                    break
                end
                newdims[i] = min(size(x, i),  cld(length(x), csz * prod(newdims)))
             end
        end
        newdims = MPI.bcast(newdims, comm, root=root)
        dist = MPIBlocks(newdims...)
    end
    d = MPI.bcast(domain(x), comm, root=root)
    # TODO: Make better load balancing
    data = Array{T,N}(undef, dist.blocksize)
    if isroot
        cs = Array{T, N}(undef, size(x))
        #TODO: deal with uneven partitions(scatterv possibly)
        @assert prod(dist.blocksize) * csz == length(x) "Cannot match length of array and number of ranks"
        parts = partition(dist, domain(x))
        idx = 1
        for part in parts
            step = prod(map(length, part.indexes))
            cs[idx:(idx - 1 + step)] = x[part]
            idx += step
        end
        MPI.Scatter!(MPI.UBuffer(cs, div(length(cs), MPI.Comm_size(comm))), data, comm, root=root)
    else
        MPI.Scatter!(nothing, data, comm, root=root)
    end

    data = Dagger.tochunk(data)

    return Dagger.DArray(T, d, domain(data), data, dist)
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

function Base.collect(x::Dagger.DArray{T,N,MPIBlocks{N}};
                     comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=true) where {T,N}
    if !acrossranks
        a = fetch(x)
        if isempty(x.chunks)
            return Array{eltype(d)}(undef, size(x)...)
        end

        dimcatfuncs = [(d...) -> d.concat(d..., dims=i) for i in 1:ndims(x)]
        Dagger.treereduce_nd(dimcatfuncs, asyncmap(fetch, a.chunks))
    else
        datasnd = collect(x, acrossranks=false)
        if root === nothing
            tmp = MPI.Allgather(datasnd, comm)
        else
            tmp = MPI.Gather(datasnd, comm, root=root)
            if tmp === nothing
                return
            end
        end
        return(reshape(tmp, size(x.domain)))
    end
end



end # module
