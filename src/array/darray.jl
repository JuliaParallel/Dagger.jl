import Base: ==
using Compat


###### Array Domains ######

if VERSION >= v"0.6.0-dev"
    # TODO: Fix this better!
    immutable ArrayDomain{N}
        indexes::NTuple{N, Any}
    end
else
    immutable ArrayDomain{N}
        indexes::NTuple{N}
    end
end

include("../lib/domain-blocks.jl")


ArrayDomain(xs...) = ArrayDomain(xs)
ArrayDomain(xs::Array) = ArrayDomain((xs...,))

indexes(a::ArrayDomain) = a.indexes
chunks{N}(a::ArrayDomain{N}) = DomainBlocks(
    ntuple(i->first(indexes(a)[i]), Val{N}), map(x->[length(x)], indexes(a)))

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
        q - (first(p) - 1)
    end |> ArrayDomain
end

function getindex(a::ArrayDomain, b::ArrayDomain)
    ArrayDomain(map(getindex, indexes(a), indexes(b)))
end

"""
    alignfirst(a)

Make a subdomain a standalone domain. For example,

    alignfirst(ArrayDomain(11:25, 21:100))
    # => ArrayDomain((1:15), (1:80))
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


"The domain of an array is a ArrayDomain"
domain(x::AbstractArray) = ArrayDomain([1:l for l in size(x)])


@compat abstract type ArrayOp{T, N} <: AbstractArray{T, N} end
@compat Base.IndexStyle(::Type{<:ArrayOp}) = IndexCartesian()

compute(ctx, x::ArrayOp) =
    compute(ctx, cached_stage(ctx, x)::DArray)

collect(ctx::Context, x::ArrayOp) =
    collect(ctx, compute(ctx, x))

collect(x::ArrayOp) = collect(Context(), x)

@compat function Base.show(io::IO, ::MIME"text/plain", x::ArrayOp)
    write(io, string(typeof(x)))
    write(io, string(size(x)))
end

function Base.show(io::IO, x::ArrayOp)
    m = MIME"text/plain"()
    @compat show(io, m, x)
end

"""
`DArray{T,N,F}(domain, subdomains, chunks, concat)`

An N-dimensional distributed array of element type T.

- `domain`: the whole ArrayDomain of the array
- `subdomains`: a `DomainBlocks` of the same dimensions as the array
- `chunks`: an array of chunks of dimension N
- `concat`: a function of type `F`. `concat(d, x, y)` takes two chunks `x` and `y`
            and concatenates them along dimension `d`. `cat` is used by default.
"""
type DArray{T,N,F} <: ArrayOp{T, N}
    domain::ArrayDomain{N}
    subdomains::AbstractArray{ArrayDomain{N}, N}
    chunks::AbstractArray{Union{Chunk,Thunk}, N}
    concat::F
end

# mainly for backwards-compatibility
(::Type{DArray{T, N}}){T,N}(domain, subdomains, chunks) = DArray(T, domain, subdomains, chunks)


"""
`DArray(T, domain, subdomains, chunks, [concat=cat])`

Creates a distributed array of element type T.

- `T`: element type

rest of the arguments are the same as the DArray constructor.
"""
function DArray{N}(T, domain::ArrayDomain{N},
                subdomains::AbstractArray{ArrayDomain{N}, N},
                chunks::AbstractArray{<:Any, N}, concat=cat)
    DArray{T, N, typeof(concat)}(domain, subdomains, chunks, concat)
end

domain(d::DArray) = d.domain
chunks(d::DArray) = d.chunks
domainchunks(d::DArray) = d.subdomains
size(x::DArray) = size(domain(x))
stage(ctx, c::DArray) = c

function collect(ctx::Context, d::DArray; tree=false)
    a = compute(ctx, d, persist=false)
    ps_input = chunks(a)

    if isempty(d.chunks)
        return Array{eltype(d)}(size(d)...)
    end

    dimcatfuncs = [(x...) -> d.concat(i, x...) for i in 1:ndims(d)]
    if tree
        collect(treereduce_nd(delayed.(dimcatfuncs), d.chunks))
    else
        treereduce_nd(dimcatfuncs, asyncmap(collect, d.chunks))
    end
end

function (==)(x::ArrayOp, y::ArrayOp)
    x === y || reduce((a,b)->a&&b, map(==, x, y))
end

function Base.hash(x::ArrayOp, i::UInt64)
    7*object_id(x)-2
end

function Base.isequal(x::ArrayOp, y::ArrayOp)
    x === y
end

"""
`view` of a `DArray` chunk returns a `DArray` of thunks
"""
function Base.view(c::DArray, d)
    subchunks, subdomains = lookup_parts(chunks(c), domainchunks(c), d)
    d1 = alignfirst(d)
    DArray(eltype(c), d1, subdomains, subchunks)
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

function group_indices(cumlength, idxs::Range)
    f = searchsortedfirst(cumlength, first(idxs))
    l = searchsortedfirst(cumlength, last(idxs))
    out = cumlength[f:l]
    out[end] = last(idxs)
    out-=(f-1)
    map(=>, f:l, map(UnitRange, vcat(first(idxs), out[1:end-1]+1), out))
end

_cumsum(x::AbstractArray) = length(x) == 0 ? Int[] : cumsum(x)
function lookup_parts{N}(ps::AbstractArray, subdmns::DomainBlocks{N}, d::ArrayDomain{N})
    groups = map(group_indices, subdmns.cumlength, indexes(d))
    sz = map(length, groups)
    pieces = Array{Union{Chunk,Thunk}}(sz)
    for i = CartesianRange(sz)
        idx_and_dmn = map(getindex, groups, i.I)
        idx = map(x->x[1], idx_and_dmn)
        dmn = ArrayDomain(map(x->x[2], idx_and_dmn))
        pieces[i] = delayed(getindex)(ps[idx...], project(subdmns[idx...], dmn))
    end
    out_cumlength = map(g->_cumsum(map(x->length(x[2]), g)), groups)
    out_dmn = DomainBlocks(ntuple(x->1,Val{N}), out_cumlength)
    pieces, out_dmn
end


"""
A DArray object may contain a thunk in it, in which case
we first turn it into a Thunk object and then compute it.
"""
function compute(ctx, x::DArray; persist=true)
    thunk = thunkize(ctx, x, persist=persist)
    if isa(thunk, Thunk)
        compute(ctx, thunk)
    else
        x
    end
end

"""
If a DArray tree has a Thunk in it, make the whole thing a big thunk
"""
function thunkize(ctx, c::DArray; persist=true)
    if any(istask, chunks(c))
        thunks = chunks(c)
        sz = size(thunks)
        dmn = domain(c)
        dmnchunks = domainchunks(c)
        if persist
            foreach(persist!, thunks)
        end
        Thunk(thunks...; meta=true) do results...
            t = eltype(results[1])
            DArray(t, dmn, dmnchunks,
                                  reshape(Union{Chunk,Thunk}[results...], sz))
        end
    else
        c
    end
end

global _stage_cache = WeakKeyDict{Context, Dict}()
"""
A memoized version of stage. It is important that the
tasks generated for the same DArray have the same
identity, for example:

    A = rand(Blocks(100,100), Float64, 1000, 1000)
    compute(A+A')

must not result in computation of A twice.
"""
function cached_stage(ctx, x)
    cache = if !haskey(_stage_cache, ctx)
        _stage_cache[ctx] = Dict()
    else
        _stage_cache[ctx]
    end

    if haskey(cache, x)
        cache[x]
    else
        cache[x] = stage(ctx, x)
    end
end

Base.@deprecate_binding Cat DArray
Base.@deprecate_binding ComputedArray DArray

export Distribute, distribute

immutable Distribute{N, T} <: ArrayOp{N, T}
    domainchunks
    data::Union{Chunk, Thunk}
end

Distribute(dmn, data) =
    Distribute(dmn, persist!(tochunk(data)))

Distribute(d, p::Union{Chunk, Thunk}) =
    Distribute{eltype(chunktype(p)), ndims(d)}(d, p)

size(x::Distribute) = size(domain(x.data))

export BlockPartition, Blocks

immutable Blocks{N}
    blocksize::NTuple{N, Int}
end
Blocks(xs::Int...) = Blocks(xs)

Base.@deprecate BlockPartition Blocks


Distribute(p::Blocks, data) =
    Distribute(partition(p, domain(data)), data)

function stage(ctx, d::Distribute)
    DArray(
           eltype(chunktype(d.data)),
           domain(d.data),
           d.domainchunks,
           map(c -> delayed(identity)(d.data[c]), d.domainchunks)
    )
end

function distribute(x::AbstractArray, dist)
    compute(Distribute(dist, x))
end

function distribute{T,N}(x::AbstractArray{T,N}, n::NTuple{N})
    p = map((d, dn)->ceil(Int, d / dn), size(x), n)
    distribute(x, Blocks(p))
end

function distribute(x::AbstractVector, n::Int)
    distribute(x, (n,))
end

function distribute(x::AbstractVector, n::Vector)
    distribute(x, DomainBlocks((1,), n))
end

function Base.:(==){T,S,N}(x::ArrayOp{T,N}, y::AbstractArray{S,N})
    collect(x) == y
end

function Base.:(==){T,S,N}(x::AbstractArray{T,N}, y::ArrayOp{S,N})
    return collect(x) == y
end
