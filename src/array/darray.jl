import Base: ==, fetch
using Serialization
import Serialization: serialize, deserialize

###### Array Domains ######

"""
    ArrayDomain{N}

An `N`-dimensional domain over an array.
"""
struct ArrayDomain{N}
    indexes::NTuple{N, Any}
end
include("../lib/domain-blocks.jl")


ArrayDomain(xs...) = ArrayDomain(xs)
ArrayDomain(xs::Array) = ArrayDomain((xs...,))

indexes(a::ArrayDomain) = a.indexes
chunks(a::ArrayDomain{N}) where {N} = DomainBlocks(
    ntuple(i->first(indexes(a)[i]), Val(N)), map(x->[length(x)], indexes(a)))

(==)(a::ArrayDomain, b::ArrayDomain) = indexes(a) == indexes(b)
Base.getindex(arr::AbstractArray, d::ArrayDomain) = arr[indexes(d)...]

function intersect(a::ArrayDomain, b::ArrayDomain)
    if a === b
        return a
    end
    ArrayDomain(map((x, y) -> _intersect(x, y), indexes(a), indexes(b)))
end

function project(a::ArrayDomain, b::ArrayDomain)
    map(indexes(a), indexes(b)) do p, q
        q .- (first(p) - 1)
    end |> ArrayDomain
end

function getindex(a::ArrayDomain, b::ArrayDomain)
    ArrayDomain(map(getindex, indexes(a), indexes(b)))
end

"""
    alignfirst(a) -> ArrayDomain

Make a subdomain a standalone domain.

# Example
```julia-repl
julia> alignfirst(ArrayDomain(11:25, 21:100))
ArrayDomain((1:15), (1:80))
```
"""
alignfirst(a::ArrayDomain) =
    ArrayDomain(map(r->1:length(r), indexes(a)))

function size(a::ArrayDomain, dim)
    idxs = indexes(a)
    length(idxs) < dim ? 1 : length(idxs[dim])
end
size(a::ArrayDomain) = map(length, indexes(a))
length(a::ArrayDomain) = prod(size(a))
ndims(a::ArrayDomain) = length(size(a))
isempty(a::ArrayDomain) = length(a) == 0


"""
    domain(x::AbstractArray) -> ArrayDomain

The domain of an array is an ArrayDomain.
"""
domain(x::AbstractArray) = ArrayDomain([1:l for l in size(x)])


abstract type ArrayOp{T, N} <: AbstractArray{T, N} end
Base.IndexStyle(::Type{<:ArrayOp}) = IndexCartesian()


collect(x::ArrayOp) = collect(fetch(x))

_to_darray(x::ArrayOp) = stage(Context(global_context()), x)::DArray
Base.fetch(x::ArrayOp) = fetch(_to_darray(x))

collect(x::Computation) = collect(fetch(x))

Base.fetch(x::Computation) = fetch(stage(Context(global_context()), x))

function Base.show(io::IO, ::MIME"text/plain", x::ArrayOp)
    write(io, string(typeof(x)))
    write(io, string(size(x)))
end

function Base.show(io::IO, x::ArrayOp)
    m = MIME"text/plain"()
    show(io, m, x)
end

export BlockPartition, Blocks

abstract type AbstractBlocks{N} end

abstract type AbstractMultiBlocks{N}<:AbstractBlocks{N} end

abstract type AbstractSingleBlocks{N}<:AbstractBlocks{N} end

struct Blocks{N} <: AbstractMultiBlocks{N}
    blocksize::NTuple{N, Int}
end

"""
    Blocks(xs...)

Indicates the size of an array operation, specified as `xs`, whose length
indicates the number of dimensions in the resulting array.
"""
Blocks(xs::Int...) = Blocks(xs)


"""
    DArray{T,N,F}(domain, subdomains, chunks, concat)
    DArray(T, domain, subdomains, chunks, [concat=cat])

An N-dimensional distributed array of element type T, with a concatenation function of type F.

# Arguments
- `T`: element type
- `domain::ArrayDomain{N}`: the whole ArrayDomain of the array
- `subdomains::AbstractArray{ArrayDomain{N}, N}`: a `DomainBlocks` of the same dimensions as the array
- `chunks::AbstractArray{Union{Chunk,Thunk}, N}`: an array of chunks of dimension N
- `concat::F`: a function of type `F`. `concat(x, y; dims=d)` takes two chunks `x` and `y`
  and concatenates them along dimension `d`. `cat` is used by default.
"""
mutable struct DArray{T,N,B<:AbstractBlocks{N},F} <: ArrayOp{T, N}
    domain::ArrayDomain{N}
    subdomains::AbstractArray{ArrayDomain{N}, N}
    chunks::AbstractArray{Any, N}
    partitioning::B
    concat::F
    function DArray{T,N,B,F}(domain, subdomains, chunks, partitioning::B, concat::Function) where {T,N,B,F}
        new{T,N,B,F}(domain, subdomains, chunks, partitioning, concat)
    end
end

WrappedDArray{T,N} = Union{<:DArray{T,N}, Transpose{<:DArray{T,N}}, Adjoint{<:DArray{T,N}}}
WrappedDMatrix{T} = WrappedDArray{T,2}
WrappedDVector{T} = WrappedDArray{T,1}
DMatrix{T} = DArray{T,2}
DVector{T} = DArray{T,1}


# mainly for backwards-compatibility
DArray{T, N}(domain, subdomains, chunks, partitioning, concat=cat) where {T,N} =
    DArray(T, domain, subdomains, chunks, partitioning, concat)

function DArray(T, domain::ArrayDomain{N},
                subdomains::AbstractArray{ArrayDomain{N}, N},
                chunks::AbstractArray{<:Any, N}, partitioning::B, concat=cat) where {N,B<:AbstractMultiBlocks{N}}
    DArray{T,N,B,typeof(concat)}(domain, subdomains, chunks, partitioning, concat)
end

function DArray(T, domain::ArrayDomain{N},
                subdomains::ArrayDomain{N},
                chunks::Any, partitioning::B, concat=cat) where {N,B<:AbstractSingleBlocks{N}}
    _subdomains = Array{ArrayDomain{N}, N}(undef, ntuple(i->1, N)...)
    _subdomains[1] = subdomains
    _chunks = Array{Any, N}(undef, ntuple(i->1, N)...)
    _chunks[1] = chunks
    DArray{T,N,B,typeof(concat)}(domain, _subdomains, _chunks, partitioning, concat)
end

domain(d::DArray) = d.domain
chunks(d::DArray) = d.chunks
domainchunks(d::DArray) = d.subdomains
size(x::DArray) = size(domain(x))
stage(ctx, c::DArray) = c

function Base.collect(d::DArray; tree=false)
    a = fetch(d)
    if isempty(d.chunks)
        return Array{eltype(d)}(undef, size(d)...)
    end

    dimcatfuncs = [(x...) -> d.concat(x..., dims=i) for i in 1:ndims(d)]
    if tree
        collect(fetch(treereduce_nd(map(x -> ((args...,) -> Dagger.@spawn x(args...)) , dimcatfuncs), a.chunks)))
    else
        treereduce_nd(dimcatfuncs, asyncmap(fetch, a.chunks))
    end
end

function (==)(x::ArrayOp, y::ArrayOp)
    x === y || reduce((a,b)->a&&b, map(==, x, y))
end

function Base.hash(x::ArrayOp, i::UInt)
    7*objectid(x)-2
end

function Base.isequal(x::ArrayOp, y::ArrayOp)
    x === y
end

function Base.similar(x::DArray{T,N}) where {T,N}
    alloc(idx, sz) = Array{T,N}(undef, sz)
    thunks = [Dagger.@spawn alloc(i, size(x)) for (i, x) in enumerate(x.subdomains)]
    return DArray(T, x.domain, x.subdomains, thunks, x.partitioning, x.concat)
end

function Base.similar(A::DArray{T,N} where T, ::Type{S}, dims::Dims{N}) where {S,N}
    d = ArrayDomain(map(x->1:x, dims))
    p = A.partitioning
    a = AllocateArray(S, (_, _, x...) -> Array{S,N}(undef, x...), d, partition(p, d), p)
    return _to_darray(a)
end

Base.copy(x::DArray{T,N,B,F}) where {T,N,B,F} =
    map(identity, x)::DArray{T,N,B,F}

# Because OrdinaryDiffEq uses `Base.promote_op(/, ::DArray, ::Real)`
Base.:(/)(x::DArray{T,N,B,F}, y::U) where {T<:Real,U<:Real,N,B,F} =
    (x ./ y)::DArray{Base.promote_op(/, T, U),N,B,F}

"""
    view(c::DArray, d)

A `view` of a `DArray` chunk returns a `DArray` of `Thunk`s.
"""
function Base.view(c::DArray, d)
    subchunks, subdomains = lookup_parts(chunks(c), domainchunks(c), d)
    d1 = alignfirst(d)
    DArray(eltype(c), d1, subdomains, subchunks, c.partitioning, c.concat)
end

function group_indices(cumlength, idxs,at=1, acc=Any[])
    at > length(idxs) && return acc
    f = idxs[at]
    fidx = searchsortedfirst(cumlength, f)
    current_block = (get(cumlength, fidx-1,0)+1):cumlength[fidx]
    start_at = at
    end_at = at
    for i=(at+1):length(idxs)
        if idxs[i] in current_block
            end_at += 1
            at += 1
        else
            break
        end
    end
    push!(acc, fidx=>idxs[start_at:end_at])
    group_indices(cumlength, idxs, at+1, acc)
end

function group_indices(cumlength, idx::Int)
    group_indices(cumlength, [idx])
end

function group_indices(cumlength, idxs::AbstractRange)
    f = searchsortedfirst(cumlength, first(idxs))
    l = searchsortedfirst(cumlength, last(idxs))
    out = cumlength[f:l]
    isempty(out) && return []
    out[end] = last(idxs)
    map(=>, f:l, map(UnitRange, vcat(first(idxs), out[1:end-1].+1), out))
end

_cumsum(x::AbstractArray) = length(x) == 0 ? Int[] : cumsum(x)
function lookup_parts(ps::AbstractArray, subdmns::DomainBlocks{N}, d::ArrayDomain{N}) where N
    groups = map(group_indices, subdmns.cumlength, indexes(d))
    sz = map(length, groups)
    pieces = Array{Any}(undef, sz)
    for i = CartesianIndices(sz)
        idx_and_dmn = map(getindex, groups, i.I)
        idx = map(x->x[1], idx_and_dmn)
        dmn = ArrayDomain(map(x->x[2], idx_and_dmn))
        pieces[i] = Dagger.@spawn getindex(ps[idx...], project(subdmns[idx...], dmn))
    end
    out_cumlength = map(g->_cumsum(map(x->length(x[2]), g)), groups)
    out_dmn = DomainBlocks(ntuple(x->1,Val(N)), out_cumlength)
    pieces, out_dmn
end

"""
    Base.fetch(c::DArray)

If a `DArray` tree has a `Thunk` in it, make the whole thing a big thunk.
"""
function Base.fetch(c::DArray{T}) where T
    if any(istask, chunks(c))
        thunks = chunks(c)
        sz = size(thunks)
        dmn = domain(c)
        dmnchunks = domainchunks(c)
        return fetch(Dagger.spawn(Options(meta=true), thunks...) do results...
            t = eltype(fetch(results[1]))
            DArray(t, dmn, dmnchunks, reshape(Any[results...], sz),
                   c.partitioning, c.concat)
        end)
    else
        return c
    end
end

Base.@deprecate_binding Cat DArray
Base.@deprecate_binding ComputedArray DArray

export Distribute, distribute

struct Distribute{T,N,B<:AbstractBlocks} <: ArrayOp{T, N}
    domainchunks
    partitioning::B
    data::AbstractArray{T,N}
end

size(x::Distribute) = size(domain(x.data))

Base.@deprecate BlockPartition Blocks


Distribute(p::Blocks, data::AbstractArray) =
    Distribute(partition(p, domain(data)), p, data)

function Distribute(domainchunks::DomainBlocks{N}, data::AbstractArray{T,N}) where {T,N}
    p = Blocks(ntuple(i->first(domainchunks.cumlength[i]), N))
    Distribute(domainchunks, p, data)
end

function Distribute(data::AbstractArray{T,N}) where {T,N}
    nprocs = sum(w->length(Dagger.get_processors(OSProc(w))),
                 Distributed.procs())
    p = Blocks(ntuple(i->max(cld(size(data, i), nprocs), 1), N))
    return Distribute(partition(p, domain(data)), p, data)
end

function stage(ctx::Context, d::Distribute)
    if isa(d.data, ArrayOp)
        # distributing a distributed array
        x = stage(ctx, d.data)
        if d.domainchunks == domainchunks(x)
            return x # already properly distributed
        end
        Nd = ndims(x)
        T = eltype(d.data)
        concat = x.concat
        cs = map(d.domainchunks) do idx
            chunks = stage(ctx, x[idx]).chunks
            shape = size(chunks)
            # TODO: fix hashing
            #hash = uhash(idx, Base.hash(Distribute, Base.hash(d.data)))
            Dagger.spawn(shape, chunks...) do shape, parts...
                if prod(shape) == 0
                    return Array{T}(undef, shape)
                end
                dimcatfuncs = [(x...) -> concat(x..., dims=i) for i in 1:length(shape)]
                ps = reshape(Any[parts...], shape)
                collect(treereduce_nd(dimcatfuncs, ps))
            end
        end
    else
        cs = map(d.domainchunks) do c
            # TODO: fix hashing
            #hash = uhash(c, Base.hash(Distribute, Base.hash(d.data)))
            Dagger.@spawn identity(d.data[c])
        end
    end
    return DArray(eltype(d.data),
                  domain(d.data),
                  d.domainchunks,
                  cs,
                  d.partitioning)
end

function distribute(x::AbstractArray, dist::Blocks)
    _to_darray(Distribute(dist, x))
end

function distribute(x::AbstractArray{T,N}, n::NTuple{N}) where {T,N}
    p = map((d, dn)->ceil(Int, d / dn), size(x), n)
    distribute(x, Blocks(p))
end

function distribute(x::AbstractVector, n::Int)
    distribute(x, (n,))
end

function distribute(x::AbstractVector, n::Vector{<:Integer})
    distribute(x, DomainBlocks((1,), (cumsum(n),)))
end

function Base.:(==)(x::ArrayOp{T,N}, y::AbstractArray{S,N}) where {T,S,N}
    collect(x) == y
end

function Base.:(==)(x::AbstractArray{T,N}, y::ArrayOp{S,N}) where {T,S,N}
    return collect(x) == y
end

# TODO: Allow `f` to return proc
mapchunk(f, chunk) = tochunk(f(poolget(chunk.handle)))
function mapchunks(f, d::DArray{T,N,F}) where {T,N,F}
    chunks = map(d.chunks) do chunk
        owner = get_parent(chunk.processor).pid
        remotecall_fetch(mapchunk, owner, f, chunk)
    end
    DArray{T,N,F}(d.domain, d.subdomains, chunks, d.concat)
end
