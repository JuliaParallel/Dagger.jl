import Base: ==
using Compat

@compat abstract type ArrayOp{T, N} <: AbstractArray{T, N} end
@compat Base.IndexStyle(::Type{<:ArrayOp}) = IndexCartesian()

compute(ctx, x::ArrayOp) =
    compute(ctx, cached_stage(ctx, x)::DArray)

gather(ctx, x::ArrayOp) =
    gather(ctx, cached_stage(ctx, x)::DArray)

@compat function Base.show(io::IO, ::MIME"text/plain", x::ArrayOp)
    write(io, string(typeof(x)))
    write(io, string(size(x)))
end

function Base.show(io::IO, x::ArrayOp)
    m = MIME"text/plain"()
    @compat show(io, m, x)
end

type DArray{T,N} <: ArrayOp{T, N}
    domain::ArrayDomain{N}
    subdomains::AbstractArray
    chunks::AbstractArray
end

domain(d::DArray) = d.domain
chunks(d::DArray) = d.chunks
domainchunks(d::DArray) = d.subdomains
size(x::DArray) = size(domain(x))
stage(ctx, c::DArray) = c

function gather(ctx, d::DArray)
    a = compute(ctx, d, persist=false)
    ps_input = chunks(a)
    ps = Array{Any}(size(ps_input))
    @sync for i in 1:length(ps_input)
        @async ps[i] = gather(ctx, ps_input[i])
    end
    if isempty(ps)
        emptyarray(Array{eltype(d), ndims(d)}, size(d)...)
    else
        cat_data(typeof(ps[1]), domain(a), domainchunks(a), ps)
    end
end

function emptyarray{T<:Array}(::Type{T}, dims...)
    T(dims...)
end

function emptyarray{Tv,Ti}(::Type{SparseMatrixCSC{Tv,Ti}}, m,n)
    spzeros(Tv, Ti, m, n)
end

function emptyarray{Tv,Ti}(::Type{SparseVector{Tv,Ti}}, n)
    SparseVector(n, Ti[], Tv[])
end

function cat_data{T<:AbstractArray}(::Type{T}, dom, subdoms, ps)

    if isempty(ps)
        return emptyarray(T, size(dom)...)
    end

    arr = similar(ps[1], size(dom)...)

    for (d, chunk) in zip(subdoms, ps)
        setindex!(arr, chunk, indexes(d)...)
    end
    arr
end

function cat_data{T<:SparseMatrixCSC}(::Type{T}, dom, ps)

    if isempty(ps)
        @assert isempty(dom)
        return spzeros(T.parameters..., size(dom)...)
    end

    m, n = size(chunks(dom))

    psT = Any[ps[j,i] for i=1:size(ps,2), j=1:size(ps,1)]
    hvcat(ntuple(x->n, m), psT...)
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
`view` of a `Cat` chunk returns a `Cat` of view chunks
"""
function Base.view(c::DArray, d)
    subchunks, subdomains = lookup_parts(chunks(c), domainchunks(c), d)
    if length(subchunks) == 1
        subchunks[1]
    else
        d1 = alignfirst(d)
        DArray{eltype(c),ndims(d1)}(d1, subdomains, subchunks)
    end
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
    pieces = Array{AbstractChunk}(sz)
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
            DArray{t, ndims(dmn)}(dmn, dmnchunks,
                                  reshape(AbstractChunk[results...], sz))
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
