export ParallelBlocks

using Statistics

struct ParallelBlocks{N} <: Dagger.AbstractSingleBlocks{N}
    n::Int
end
ParallelBlocks(n::Integer) = ParallelBlocks{0}(n)
ParallelBlocks{N}(dist::ParallelBlocks) where N = ParallelBlocks{N}(dist.n)
ParallelBlocks() = ParallelBlocks(Dagger.num_processors())

Base.convert(::Type{ParallelBlocks{N}}, dist::ParallelBlocks) where N =
    ParallelBlocks{N}(dist.n)

wrap_chunks(chunks::Vector{<:Dagger.Chunk}, N::Integer, dist::ParallelBlocks) =
    wrap_chunks(chunks, N, dist.n)
wrap_chunks(chunks::Vector{<:Dagger.Chunk}, N::Integer, n::Integer) =
    convert(Array{Any}, reshape(chunks, ntuple(i->i == 1 ? n : 1, N)))

function _finish_allocation(f::Function, dist::ParallelBlocks, dims::NTuple{N,Int}) where N
    d = ArrayDomain(map(x->1:x, dims))
    s = reshape([d for _ in 1:dist.n],
                ntuple(i->i == 1 ? dist.n : 1, N))
    data = [f(dims) for _ in 1:dist.n]
    dist = ParallelBlocks{N}(dist)
    chunks = wrap_chunks(map(Dagger.tochunk, data), N, dist)
    return Dagger.DArray(eltype(first(data)), d, s, chunks, dist)
end

for fn in [:rand, :randn, :zeros, :ones]
    @eval begin
        function Base.$fn(dist::ParallelBlocks, ::Type{ET}, dims::Dims) where {ET}
            f(block) = $fn(ET, block)
            _finish_allocation(f, dist, dims)
        end
        Base.$fn(dist::ParallelBlocks, T::Type, dims::Integer...) = $fn(dist, T, dims)
        Base.$fn(dist::ParallelBlocks, T::Type, dims::Tuple) = $fn(dist, T, dims)
        Base.$fn(dist::ParallelBlocks, dims::Integer...) = $fn(dist, Float64, dims)
        Base.$fn(dist::ParallelBlocks, dims::Tuple) = $fn(dist, Float64, dims)
    end
end
# FIXME: sprand

function Dagger.distribute(data::AbstractArray{T,N}, dist::ParallelBlocks) where {T,N}
    dims = size(data)
    d = ArrayDomain(map(x->1:x, dims))
    s = Dagger.DomainBlocks(ntuple(_->1, N),
                            ntuple(i->[dims[i]], N))
    chunks = [Dagger.tochunk(copy(data)) for _ in 1:dist.n]
    new_dist = ParallelBlocks{N}(dist)
    return Dagger.DArray(T, d, s, wrap_chunks(chunks, N, dist), new_dist)
end

_invalid_call_pblocks(f::Symbol) =
    error("`$f` is not valid for a `DArray` partitioned with `ParallelBlocks`.\nConsider `Dagger.pmap($f, x)` instead.")

Base.collect(::Dagger.DArray{T,N,<:ParallelBlocks} where {T,N}) =
    _invalid_call_pblocks(:collect)
Base.getindex(::Dagger.DArray{T,N,<:ParallelBlocks} where {T,N}, x...) =
    _invalid_call_pblocks(:getindex)
Base.setindex!(::Dagger.DArray{T,N,<:ParallelBlocks} where {T,N}, value, x...) =
    _invalid_call_pblocks(:setindex!)

function pmap(f::Function, A::Dagger.DArray{T,N,ParallelBlocks{N}}) where {T,N}
    # TODO: Chunks might not be `Array`s
    # FIXME
    #AT = Array{T,N}
    #ET = eltype(Base.promote_op(f, AT))
    ET = Any
    new_chunks = map(A.chunks) do chunk
        Dagger.@spawn f(chunk)
    end
    return DArray(ET, A.domain, A.subdomains, new_chunks, A.partitioning)
end
# FIXME: More useful `show` method
Base.show(io::IO, ::MIME"text/plain", A::Dagger.DArray{T,N,ParallelBlocks{N}}) where {T,N} =
    print(io, typeof(A))
pfetch(A::Dagger.DArray{T,N,ParallelBlocks{N}}) where {T,N} =
    map(fetch, A.chunks)
pcollect(A::Dagger.DArray{T,N,ParallelBlocks{N}}) where {T,N} =
    map(collect, pfetch(A))

function Base.map(f::Function, A::Dagger.DArray{T,N,ParallelBlocks{N}}) where {T,N}
    ET = Base.promote_op(f, T)
    new_chunks = map(A.chunks) do chunk
        Dagger.@spawn map(f, chunk)
    end
    return DArray(ET, A.domain, A.subdomains, new_chunks, A.partitioning)
end
function Base.map!(f::Function,
                   x::Dagger.DArray{T1,N1,ParallelBlocks{N1}} where {T1,N1},
                   y::Dagger.DArray{T2,N2,ParallelBlocks{N2}} where {T2,N2})
    x_dist = x.partitioning
    y_dist = y.partitioning
    if x_dist.n != y_dist.n
        throw(ArgumentError("Can't `map!` over non-matching `ParallelBlocks` distributions: $(x_dist.n) != $(y_dist.n)"))
    end
    @sync for i in 1:x_dist.n
        Dagger.@spawn map!(f, x.chunks[i], y.chunks[i])
    end
end

#=
function Base.reduce(f::Function, x::Dagger.DArray{T,N,ParallelBlocks{N}};
                     dims=:) where {T,N}
    error("Out-of-place Reduce")
    if dims == Base.:(:)
        localpart = fetch(Dagger.reduce_async(f, x))
        return MPI.Allreduce(localpart, f, comm)
    elseif dims === nothing
        localpart = fetch(x.chunks[1])
        return MPI.Allreduce(localpart, f, comm)
    else
        error("Not yet implemented")
    end
end
=#
function allreduce!(op::Function, x::Dagger.DArray{T,N,ParallelBlocks{N}}; nchunks::Integer=0) where {T,N}
    if nchunks == 0
        nchunks = x.partitioning.n
    end
    @assert nchunks == x.partitioning.n "Number of chunks must match the number of partitions"

    # Split each chunk along the last dimension
    chunk_size = cld(size(x, ndims(x)), nchunks)
    chunk_dist = Blocks(ntuple(i->i == N ? chunk_size : size(x, i), N))
    chunk_ds = partition(chunk_dist, x.subdomains[1])
    num_par_chunks = length(x.chunks)

    # Allocate temporary buffer
    y = copy(x)

    # Ring-reduce into temporary buffer
    Dagger.spawn_datadeps() do
        for j in 1:length(chunk_ds)
            for i in 1:num_par_chunks
                for step in 1:(num_par_chunks-1)
                    from_idx = i
                    to_idx = mod1(i+step, num_par_chunks)
                    from_chunk = x.chunks[from_idx]
                    to_chunk = y.chunks[to_idx]
                    sd = chunk_ds[mod1(j+i-1, length(chunk_ds))].indexes
                    # FIXME: Specify aliasing based on `sd`
                    Dagger.@spawn _reduce_view!(op,
                                                InOut(to_chunk), sd,
                                                In(from_chunk), sd)
                end
            end
        end

        # Copy from temporary buffer back to origin
        for i in 1:num_par_chunks
            Dagger.@spawn copyto!(Out(x.chunks[i]), In(y.chunks[i]))
        end
    end

    return x
end
function _reduce_view!(op, to, to_view, from, from_view)
    to_viewed = view(to, to_view...)
    from_viewed = view(from, from_view...)
    reduce!(op, to_viewed, from_viewed)
    return
end
function reduce!(op, to, from)
    to .= op.(to, from)
end

function Statistics.mean!(A::Dagger.DArray{T,N,ParallelBlocks{N}}) where {T,N}
    allreduce!(+, A)
    len = length(A.chunks)
    map!(x->x ./ len, A, A)
    return A
end
