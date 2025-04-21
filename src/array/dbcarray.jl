export DBCArray, DBCVector, DBCMatrix, 

### darray.jl

mutable struct DBCArray{T,N,B,F} <: ArrayOp{T, N}
    darray::DArray{T,N,B,F}
    pdomain::AbstractArray{Dagger.Processor, N}
    # function DBCArray{T,N,B,F}(domain, subdomains, chunks, partitioning::B, concat::Function, pdomain) where {T,N,B,F}
    #     new{T,N,B,F}(domain, subdomains, chunks, partitioning, concat, pdomain)
    # end
end

const WrappedDBCArray{T,N} = Union{<:DBCArray{T,N}, Transpose{<:DBCArray{T,N}}, Adjoint{<:DBCArray{T,N}}}
const WrappedDBCMatrix{T} = WrappedDBCArray{T,2}
const WrappedDBCVector{T} = WrappedDBCArray{T,1}
const DBCMatrix{T} = DBCArray{T,2}
const DBCVector{T} = DBCArray{T,1}

# DBCArray{T, N}(domain, subdomains, chunks, partitioning, concat=cat, pdomain) where {T,N} = DBCArray(T, domain, subdomains, chunks, partitioning, concat)

# function DArray(T, domain::DArrayDomain{N},
#     subdomains::AbstractArray{DArrayDomain{N}, N},
#     chunks::AbstractArray{<:Any, N}, partitioning::B, concat=cat) where {N,B<:AbstractBlocks{N}}
# DArray{T,N,B,typeof(concat)}(domain, subdomains, chunks, partitioning, concat)
# end

# function DArray(T, domain::DArrayDomain{N},
    #     subdomains::DArrayDomain{N},
    #     chunks::Any, partitioning::B, concat=cat) where {N,B<:AbstractSingleBlocks{N}}
    # _subdomains = Array{DArrayDomain{N}, N}(undef, ntuple(i->1, N)...)
    # _subdomains[1] = subdomains
    # _chunks = Array{Any, N}(undef, ntuple(i->1, N)...)
    # _chunks[1] = chunks
    # DArray{T,N,B,typeof(concat)}(domain, _subdomains, _chunks, partitioning, concat)
# end

function DBCArray(A::DArray{T,N,B,F}, pdomain::AbstractArray{Dagger.Processor, N}) where {T,N,B,F}
    
    all_procs = collect(Iterators.flatten(Dagger.get_processors(OSProc(w)) for w in procs()))
    missing = filter(p -> p ∉ all_procs, pdomain)
    isempty(missing) || error("Missing processors: $missing")
    
    Ac = fetch(A.chunks)
    Ac_copy = similar(A.chunks)

    for idx in CartesianIndices(A.chunks)
        proc = pdomain[mod1.(Tuple(idx), size(pdomain))...]
        Ac_copy[idx] = Dagger.@spawn scope=Dagger.ExactScope(proc) identity(Ac[idx])
        # new_chunks[idx] = Dagger.@spawn scope=Dagger.ExactScope(proc) Dagger.tochunk(old_chunks[idx], proc)
    end
    
    # Construct new DArray with updated chunks
    A_copy = DArray{T,N,B,F}(A.domain, A.subdomains, Ac_copy, A.partitioning, A.concat)

    return DBCArray{T,N,B,F}(A_copy, pdomain)
end

domain(d::DBCArray) = domain(d.darray)
chunks(d::DBCArray) = chunks(d.darray)
domainchunks(d::DBCArray) = domainchunks(d.darray)
size(x::DBCArray) = size(domain(x))
stage(ctx, c::DBCArray) = stage(ctx, c.darray)
pdomain(A::DBCArray) = A.pdomain

function Base.collect(d::DBCArray; tree=false)
    return collect(d.darray; tree=tree)
end

Base.wait(A::DBCArray) = wait(A.darray.chunks)

function Base.show(io::IO, ::MIME"text/plain", A::DBCArray{T,N,B,F}) where {T,N,B,F}
    nparts = N > 0 ? size(A.darray.chunks) : 1
    partsize = N > 0 ? A.darray.partitioning.blocksize : 1
    nprocs = N > 0 ? size(A.pdomain) : 1
    write(io, " with $(join(nparts, 'x')) partitions of size $(join(partsize, 'x')) distributed to $(join(nprocs, 'x')) processors:")
    pct_complete = 100 * (sum(c->c isa Chunk ? true : isready(c), A.darray.chunks) / length(A.darray.chunks))
    if pct_complete < 100
        println(io)
        printstyled(io, "~$(round(Int, pct_complete))% completed"; color=:yellow)
    end
    println(io)
    Base.print_array(IOContext(io, :compact=>true), ColorArray(A.darray))
end

# function Base.similar(A::DArray{T,N} where T, ::Type{S}, dims::Dims{N}) where {S,N}
#     d = ArrayDomain(map(x->1:x, dims))
#     p = A.partitioning
#     a = AllocateArray(S, AllocateUndef{S}(), false, d, partition(p, d), p)
#     return _to_darray(a)
# end

Base.copy(x::DBCArray{T,N,B,F}) where {T,N,B,F} =  DBCArray{T,N,B,F}(x.darray, x.pdomain)

Base.:(/)(x::DBCArray{T,N,B,F}, y::U) where {T<:Real, U<:Real, N, B, F} = DBCArray(x.darray / y, x.pdomain)

# function Base.view(c::DArray, d)
#     subchunks, subdomains = lookup_parts(c, chunks(c), domainchunks(c), d)
#     d1 = alignfirst(d)
#     DArray(eltype(c), d1, subdomains, subchunks, c.partitioning, c.concat)
# end

# Base.fetch(c::DBCArray{T,N,B,F}) where {T,N,B,F} = c

# function Base.:(==)(x::ArrayOp{T,N}, y::AbstractArray{S,N}) where {T,S,N}
#     collect(x) == y
# end

# function Base.:(==)(x::AbstractArray{T,N}, y::ArrayOp{S,N}) where {T,S,N}
#     return collect(x) == y
# end

function logs_annotate!(ctx::Context, A::DBCArray, name::Union{String,Symbol})
    for (idx, chunk) in enumerate(A.darray.chunks)
        sd = A.subdomains[idx]
        Dagger.logs_annotate!(ctx, chunk, name*'['*join(sd.indexes, ',')*']')
    end
end

# function mapchunks(f, d::DArray{T,N,F}) where {T,N,F}
#     chunks = map(d.chunks) do chunk
#         owner = get_parent(chunk.processor).pid
#         remotecall_fetch(mapchunk, owner, f, chunk)
#     end
#     DArray{T,N,F}(d.domain, d.subdomains, chunks, d.concat)
# end


### matrix.jl

function copydiag(f, A::DBCArray{T, 2}) where T
    Ac = A.darray.chunks
    Ac_copy = Matrix{Any}(undef, size(Ac, 2), size(Ac, 1))
    _copytile(f, Ac) = copy(f(Ac))
    for idx in CartesianIndices(Ac)
        proc = A.pdomain[mod1.(Tuple(idx), size(A.pdomain))...]
        Ac_copy[idx'] = Dagger.@spawn scope=Dagger.ExactScope(proc) _copytile(f, Ac[idx])
    end
    Ad_copy = DArray{T,N,B,F}(ArrayDomain(1:size(A,2), 1:size(A,1)), A.darray.subdomains', Ac_copy, A.darray.partitioning, A.darray.concat)
    return DBCArray(Ad_copy, A.pdomain)
end

Base.fetch(A::Adjoint{T, <:DBCArray{T, 2}}) where T = copydiag(Adjoint, parent(A))
Base.fetch(A::Transpose{T, <:DBCArray{T, 2}}) where T = copydiag(Transpose, parent(A))
Base.copy(A::Adjoint{T, <:DBCArray{T, 2}}) where T = fetch(A)
Base.copy(A::Transpose{T, <:DBCArray{T, 2}}) where T = fetch(A)
Base.collect(A::Adjoint{T, <:DBCArray{T, 2}}) where T = collect(copy(A))
Base.collect(A::Transpose{T, <:DBCArray{T, 2}}) where T = collect(copy(A))

(*)(a::DBCArray, b::Vector) = DBCArray((a.darray)*b, a.pdomain)

# Base.power_by_squaring(x::DBCArray{T,N,B,F}, i::Int) where {T,N,B,F} = foldl(*, ntuple(_ -> x, i))


# indexing.jl

Base.getindex(A::DBCArray{T,N}, idx::NTuple{N,Int}) where {T,N} = getindex(A.darray, idx)

Base.getindex(A::DBCArray, idx::Integer...) = getindex(A.darray, idx)
Base.getindex(A::DBCArray, idx::Integer) = getindex(A.darray, idx)
Base.getindex(A::DBCArray, idx::CartesianIndex) = getindex(A.darray, idx)
Base.getindex(A::DBCArray{T,N}, idxs::Dims{S}) where {T,N,S} = getindex(A.darray, idxs)

Base.setindex!(A::DBCArray{T,N}, value, idx::NTuple{N,Int}) where {T,N} = setindex!(A.darray, value, idx)
Base.setindex!(A::DBCArray, value, idx::Integer...) = setindex!(A.darray, value, idx)
Base.setindex!(A::DArray, value, idx::Integer) = setindex!(A.darray, value,idx)
Base.setindex!(A::DArray, value, idx::CartesianIndex) = setindex!(A.darray, value, idx)
Base.setindex!(A::DBCArray{T,N}, value, idxs::Dims{S}) where {T,N,S} = setindex!(A.darray, value, idxs)
