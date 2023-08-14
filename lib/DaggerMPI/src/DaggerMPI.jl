module DaggerMPI
using Dagger, SparseArrays, Random
import Statistics: sum, prod, mean
import Base: reduce, fetch, cat, prod, sum
using MPI

export MPIBlocks

struct MPIBlocks{N} <: Dagger.AbstractSingleBlocks{N}
    blocksize::NTuple{N, Union{Int, Nothing}}
end
MPIBlocks(xs::Union{Int,Nothing}...) = MPIBlocks(xs)

function _defineBlocks(dist::MPIBlocks, comm::MPI.Comm, root::Integer, dims::Tuple)
    isroot = MPI.Comm_rank(comm) == root
    newdims = map(collect(dist.blocksize)) do d
        something(d, 1)
    end
    csz = MPI.Comm_size(comm)
    if isroot
        homogenous = map(dist.blocksize) do i 
            if i !== nothing 
                if i < (prod(dims) / csz) ^ (1/length(dims))
                    return true
                else
                    return false
                end
            else
                return false
            end
        end
        if isinteger((prod(dims) / csz) ^ (1 / length(dims))) && !any(homogenous)   
            for i in 1:length(dims)
                if dist.blocksize[i] === nothing
                    newdims[i] = (prod(dims) / csz) ^ (1 / length(dims))
                end
            end
        else
            for i in 1:length(dims)
                if dist.blocksize[i] !== nothing
                    continue
                end
                if csz * prod(newdims) >= prod(dims)
                    break
                end
                newdims[i] = min(dims[i],  cld(prod(dims), csz * prod(newdims)))
             end
         end
    end
    newdims = MPI.bcast(newdims, comm, root=root)
    MPIBlocks(newdims...)
end

function _finish_allocation(f::Function, dist::MPIBlocks, dims, comm::MPI.Comm, root::Integer)
    if any(isnothing, dist.blocksize)
       dist =  _defineBlocks(dist, comm, root, dims)
    end
    rnk = MPI.Comm_rank(comm)
    d = ArrayDomain(map(x->1:x, dims))
    data = f(dist.blocksize)
    data = Dagger.tochunk(data)
    ds = partition(dist, d)[rnk + 1]
    return Dagger.DArray(eltype, d, ds, data, dist)
end

function Dagger.distribute(::Type{A},
                           x::Union{AbstractArray, Nothing},
                           dist::MPIBlocks,
                           comm::MPI.Comm=MPI.COMM_WORLD,
                           root::Integer=0) where {A<:AbstractArray{T, N}} where {T, N}
    rnk = MPI.Comm_rank(comm)
    isroot = rnk == root
    csz = MPI.Comm_size(comm)
    if any(isnothing, dist.blocksize)
        dist = _defineBlocks(dist, comm, root, size(x))
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
        MPI.Scatter!(MPI.UBuffer(cs, div(length(cs), csz)), data, comm, root=root)
    else
        MPI.Scatter!(nothing, data, comm, root=root)
    end

    data = Dagger.tochunk(data)
    ds = partition(dist, d)[rnk + 1] 
    return Dagger.DArray(T, d, ds, data, dist)
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
            return Dagger.DArray(T, d, ds, thunks, x.partitioning, x.concat)
        else
            tmp = collect(reduce(f, x, comm=comm, root=root, dims=dims, acrossranks=false), acrossranks = false)
            h = UInt(0)
            for dim in 1:N
                if dim in dims
                    continue
                end
                h = hash(x.subdomains[1].indexes[dim], h)
            end
            h = abs(Base.unsafe_trunc(Int32, h))
            newc = MPI.Comm_split(comm, h, MPI.Comm_rank(comm))
            if root === nothing 
                chunks = Dagger.tochunk(reshape(MPI.Allreduce(tmp, f, newc), size(tmp)))
            else
                rcvbuf = MPI.Reduce(tmp, f, newc, root=root)
                if root != MPI.Comm_rank(comm)
                    return nothing
                end
                chunks = reshape(rcvbuf, size(tmp))
                chunks = Dagger.tochunk(chunks)
            end
            return Dagger.DArray(T, d, ds, chunks, x.partitioning, x.concat)
        end
    end
end

function Base.rand(dist::MPIBlocks, eltype::Type, dims, comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0)  
    rank = MPI.Comm_rank(comm) 
    s = rand(UInt)
    f(block) = rand(MersenneTwister(s+rank), eltype, block)
    _finish_allocation(f, dist, dims, comm, root)
end
Base.rand(dist::MPIBlocks, t::Type, dims::Integer...; comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0) = rand(dist, t, dims, comm, root)
Base.rand(dist::MPIBlocks, dims::Integer...; comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0) = rand(dist, Float64, dims, comm, root)
Base.rand(dist::MPIBlocks, dims::Tuple, comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0) = rand(dist, Float64, dims, comm, root)

function Base.randn(dist::MPIBlocks, dims, comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0)
    rank = MPI.Comm_rank(comm) 
    s = rand(UInt)
    f(block) = randn(MersenneTwister(s+rank), Float64, block)
    _finish_allocation(f, dist, dims, comm, root)
end
Base.randn(p::MPIBlocks, dims::Integer...) = randn(p, dims)

function Base.ones(dist::MPIBlocks, eltype::Type, dims, comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0)
    f(blocks) = ones(eltype, blocks)
    _finish_allocation(f, dist, dims, comm, root)
end
Base.ones(dist::MPIBlocks, t::Type, dims::Integer...) = ones(dist, t, dims)
Base.ones(dist::MPIBlocks, dims::Integer...) = ones(dist, Float64, dims)
Base.ones(dist::MPIBlocks, dims::Tuple) = ones(dist, Float64, dims)

function Base.zeros(dist::MPIBlocks, eltype::Type, dims, comm::MPI.Comm=MPI.COMM_WORLD, root::Integer=0)
    f(blocks) = zeros(eltype, blocks) 
    _finish_allocation(f, dist, dims, comm, root)
end
Base.zeros(dist::MPIBlocks, t::Type, dims::Integer...) = zeros(dist, t, dims)
Base.zeros(dist::MPIBlocks, dims::Integer...) = zeros(dist, Float64, dims)
Base.zeros(dist::MPIBlocks, dims::Tuple) = zeros(dist, Float64, dims)

sum(x::Dagger.DArray{T,N,MPIBlocks{N}}; dims::Union{Int,Nothing}=nothing, comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=true) where {T,N} = reduce(+, x, dims=dims, comm=comm, root=root, acrossranks=acrossranks) 

prod(x::Dagger.DArray{T,N,MPIBlocks{N}}; dims::Union{Int,Nothing}=nothing, comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=true) where {T,N} = reduce(*, x, dims=dims, comm=comm, root=root, acrossranks=acrossranks) 

#mean(x::Dagger.DArray{T,N,MPIBlocks{N}}; dims::Union{Int,Nothing}=nothing, comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=true) where {T,N} = reduce(mean, x, dims=dims, comm=comm, root=root, acrossranks=acrossranks)

function Base.collect(x::Dagger.DArray{T,N,MPIBlocks{N}};
                     comm=MPI.COMM_WORLD, root=nothing, acrossranks::Bool=true) where {T,N}
    if !acrossranks
        if isempty(x.chunks)
            return Array{eltype(d)}(undef, size(x)...)
        end
        dimcatfuncs = [(d...) -> x.concat(d..., dims=i) for i in 1:ndims(x)]
        Dagger.treereduce_nd(dimcatfuncs, asyncmap(fetch, x.chunks))
    else
        csz = MPI.Comm_size(comm)
        datasnd = collect(x, acrossranks=false)
        domsnd = collect(x.subdomains[1].indexes)
        if root === nothing
            datatmp = MPI.Allgather(datasnd, comm)
            domtmp = MPI.Allgather(domsnd, comm)
        else
            datatmp = MPI.Gather(datasnd, comm, root=root)
            if datatmp === nothing
                return
            end
        end
        idx = 1
        domtmp = reshape(domtmp, (N, csz))
        doms = Array{Vector{UnitRange{Int64}}}(undef, csz)
        domused = Array{Vector{UnitRange{Int64}}}(undef, csz)
        for i in 1:csz
            doms[i] = domtmp[:, i]
            domused[i] = [0:0 for j in 1:N]
        end
        step = Int(length(datatmp) / csz)
        data = Array{eltype(datatmp), N}(undef, size(x.domain))
        for i in 1:csz
            if doms[i] in domused
                idx += step
                continue
            end
            data[doms[i]...] = datatmp[idx:(idx - 1 + step)]
            domused[i] = doms[i]
            idx += step
        end
        return(reshape(data, size(x.domain)))
    end
end



end # module

#TODO: sort, broadcast ops, mat-mul, setindex, getindex, mean
