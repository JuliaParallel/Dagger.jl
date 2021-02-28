import Base: ==
using Serialization
import Serialization: serialize, deserialize

###### Array Domains ######

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

compute(ctx, x::ArrayOp; options=nothing) =
    compute(ctx, cached_stage(ctx, x)::DArray; options=options)

collect(ctx::Context, x::ArrayOp; options=nothing) =
    collect(ctx, compute(ctx, x; options=options); options=options)

collect(x::ArrayOp; options=nothing) = collect(Context(), x; options=options)

function Base.show(io::IO, ::MIME"text/plain", x::ArrayOp)
    write(io, string(typeof(x)))
    write(io, string(size(x)))
end

function Base.show(io::IO, x::ArrayOp)
    m = MIME"text/plain"()
    show(io, m, x)
end

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
mutable struct DArray{T,N,F} <: ArrayOp{T, N}
    domain::ArrayDomain{N}
    subdomains::AbstractArray{ArrayDomain{N}, N}
    chunks::AbstractArray{Union{Chunk,Thunk}, N}
    concat::F
    freed::Threads.Atomic{UInt8}
    function DArray{T,N,F}(domain, subdomains, chunks, concat::Function) where {T, N,F}
        new(domain, subdomains, chunks, concat, Threads.Atomic{UInt8}(0))
    end
end

function free_chunks(chunks)
    @sync for c in chunks
        if c isa Chunk{<:Any, DRef}
            # increment refcount on the master node
            @async free!(c.handle)
        elseif c isa Thunk
            free_chunks(c.inputs)
        end
    end
end

function free!(x::DArray)
    freed = Bool(Threads.atomic_cas!(x.freed, UInt8(0), UInt8(1)))
    !freed && @async Dagger.free_chunks(x.chunks)
    nothing
end

# mainly for backwards-compatibility
DArray{T, N}(domain, subdomains, chunks) where {T,N} = DArray(T, domain, subdomains, chunks)

function DArray(T, domain::ArrayDomain{N},
             subdomains::AbstractArray{ArrayDomain{N}, N},
             chunks::AbstractArray{<:Any, N}, concat=cat) where N
    DArray{T, N, typeof(concat)}(domain, subdomains, chunks, concat)
end

domain(d::DArray) = d.domain
chunks(d::DArray) = d.chunks
domainchunks(d::DArray) = d.subdomains
size(x::DArray) = size(domain(x))
stage(ctx, c::DArray) = c

function collect(ctx::Context, d::DArray; tree=false, options=nothing)
    a = compute(ctx, d; options=options)

    if isempty(d.chunks)
        return Array{eltype(d)}(undef, size(d)...)
    end

    dimcatfuncs = [(x...) -> d.concat(x..., dims=i) for i in 1:ndims(d)]
    if tree
        collect(treereduce_nd(delayed.(dimcatfuncs), a.chunks))
    else
        treereduce_nd(dimcatfuncs, asyncmap(collect, a.chunks))
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
    pieces = Array{Union{Chunk,Thunk}}(undef, sz)
    for i = CartesianIndices(sz)
        idx_and_dmn = map(getindex, groups, i.I)
        idx = map(x->x[1], idx_and_dmn)
        dmn = ArrayDomain(map(x->x[2], idx_and_dmn))
        pieces[i] = delayed(getindex)(ps[idx...], project(subdmns[idx...], dmn))
    end
    out_cumlength = map(g->_cumsum(map(x->length(x[2]), g)), groups)
    out_dmn = DomainBlocks(ntuple(x->1,Val(N)), out_cumlength)
    pieces, out_dmn
end


"""
A DArray object may contain a thunk in it, in which case
we first turn it into a Thunk object and then compute it.
"""
function compute(ctx::Context, x::DArray; persist=true, options=nothing)
    thunk = thunkize(ctx, x, persist=persist)
    if isa(thunk, Thunk)
        compute(ctx, thunk; options=options)
    else
        x
    end
end

"""
If a DArray tree has a Thunk in it, make the whole thing a big thunk
"""
function thunkize(ctx::Context, c::DArray; persist=true)
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
function cached_stage(ctx::Context, x)
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

struct Distribute{T, N} <: ArrayOp{T, N}
    domainchunks
    data::AbstractArray{T,N}
end

size(x::Distribute) = size(domain(x.data))

export BlockPartition, Blocks

struct Blocks{N}
    blocksize::NTuple{N, Int}
end
Blocks(xs::Int...) = Blocks(xs)

Base.@deprecate BlockPartition Blocks


Distribute(p::Blocks, data::AbstractArray) =
    Distribute(partition(p, domain(data)), data)

function stage(ctx::Context, d::Distribute)
    if isa(d.data, ArrayOp)
        # distributing a dsitributed array
        x = cached_stage(ctx, d.data)
        if d.domainchunks == domainchunks(x)
            return x # already properly distributed
        end
        Nd = ndims(x)
        T = eltype(d.data)
        concat = x.concat
        cs = map(d.domainchunks) do idx
            chunks = cached_stage(ctx, x[idx]).chunks
            shape = size(chunks)
            (delayed() do shape, parts...
                if prod(shape) == 0
                    return Array{T}(undef, shape)
                end
                dimcatfuncs = [(x...) -> concat(x..., dims=i) for i in 1:length(shape)]
                ps = reshape(Any[parts...], shape)
                collect(treereduce_nd(dimcatfuncs, ps))
            end)(shape, chunks...)
        end
    else
        cs = map(c -> delayed(identity)(d.data[c]), d.domainchunks)
    end

    DArray(
           eltype(d.data),
           domain(d.data),
           d.domainchunks,
           cs
    )
end

function distribute(x::AbstractArray, dist)
    compute(Distribute(dist, x))
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
