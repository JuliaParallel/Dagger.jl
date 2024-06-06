import Base: ==, fetch

export DArray, DVector, DMatrix, Blocks, AutoBlocks
export distribute


###### Array Domains ######

"""
    ArrayDomain{N}

An `N`-dimensional domain over an array.
"""
struct ArrayDomain{N,T<:Tuple}
    indexes::T
end

ArrayDomain(xs::T) where T<:Tuple = ArrayDomain{length(xs),T}(xs)
ArrayDomain(xs::NTuple{N,Base.OneTo}) where N =
    ArrayDomain{N,NTuple{N,UnitRange{Int}}}(ntuple(i->UnitRange(xs[i]), N))
ArrayDomain(xs::NTuple{N,Int}) where N =
    ArrayDomain{N,NTuple{N,UnitRange{Int}}}(ntuple(i->xs[i]:xs[i], N))
ArrayDomain(::Tuple{}) = ArrayDomain{0,Tuple{}}(())
ArrayDomain(xs...) = ArrayDomain((xs...,))
ArrayDomain(xs::Array) = ArrayDomain((xs...,))

include("../lib/domain-blocks.jl")

indexes(a::ArrayDomain) = a.indexes
chunks(a::ArrayDomain{N}) where {N} = DomainBlocks(
    ntuple(i->first(indexes(a)[i]), Val(N)), map(x->[length(x)], indexes(a)))

(==)(a::ArrayDomain, b::ArrayDomain) = indexes(a) == indexes(b)
Base.getindex(arr::AbstractArray, d::ArrayDomain) = arr[indexes(d)...]
Base.getindex(arr::AbstractArray{T,0} where T, d::ArrayDomain{0}) = arr

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

const DArrayDomain{N} = ArrayDomain{N, NTuple{N, UnitRange{Int}}}

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
    domain::DArrayDomain{N}
    subdomains::AbstractArray{DArrayDomain{N}, N}
    chunks::AbstractArray{Any, N}
    partitioning::B
    concat::F
    function DArray{T,N,B,F}(domain, subdomains, chunks, partitioning::B, concat::Function) where {T,N,B,F}
        new{T,N,B,F}(domain, subdomains, chunks, partitioning, concat)
    end
end

const WrappedDArray{T,N} = Union{<:DArray{T,N}, Transpose{<:DArray{T,N}}, Adjoint{<:DArray{T,N}}}
const WrappedDMatrix{T} = WrappedDArray{T,2}
const WrappedDVector{T} = WrappedDArray{T,1}
const DMatrix{T} = DArray{T,2}
const DVector{T} = DArray{T,1}

# mainly for backwards-compatibility
DArray{T, N}(domain, subdomains, chunks, partitioning, concat=cat) where {T,N} =
    DArray(T, domain, subdomains, chunks, partitioning, concat)

function DArray(T, domain::DArrayDomain{N},
                subdomains::AbstractArray{DArrayDomain{N}, N},
                chunks::AbstractArray{<:Any, N}, partitioning::B, concat=cat) where {N,B<:AbstractBlocks{N}}
    DArray{T,N,B,typeof(concat)}(domain, subdomains, chunks, partitioning, concat)
end

function DArray(T, domain::DArrayDomain{N},
                subdomains::DArrayDomain{N},
                chunks::Any, partitioning::B, concat=cat) where {N,B<:AbstractSingleBlocks{N}}
    _subdomains = Array{DArrayDomain{N}, N}(undef, ntuple(i->1, N)...)
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

    if ndims(d) == 0
        return fetch(a.chunks[1])
    end

    dimcatfuncs = [(x...) -> d.concat(x..., dims=i) for i in 1:ndims(d)]
    if tree
        collect(fetch(treereduce_nd(map(x -> ((args...,) -> Dagger.@spawn x(args...)) , dimcatfuncs), a.chunks)))
    else
        treereduce_nd(dimcatfuncs, asyncmap(fetch, a.chunks))
    end
end

### show

#= FIXME
@static if isdefined(Base, :AnnotatedString)
    # FIXME: Import StyledStrings
    struct ColorElement{T}
        color::Symbol
        value::T
    end
    function Base.show(io::IO, ::MIME"text/plain", x::ColorElement)
        print(io, styled"{(foreground=$(x.color)):$(x.value)}")
    end
else
=#
    struct ColorElement{T}
        color::Symbol
        value::Union{Some{T},Nothing}
    end
    function Base.show(io::IO, ::MIME"text/plain", x::ColorElement)
        if x.value !== nothing
            printstyled(io, something(x.value); color=x.color)
        else
            printstyled(io, "..."; color=x.color)
        end
    end
    Base.alignment(io::IO, x::ColorElement) =
        Base.alignment(io, something(x.value, "..."))
#end
Base.show(io::IO, x::ColorElement) = show(io, MIME("text/plain"), x)
struct ColorArray{T,N} <: DenseArray{T,N}
    A::DArray{T,N}
    color_map::Vector{Symbol}
    seen_values::Dict{NTuple{N,Int},Union{Some{T},Nothing}}
    function ColorArray(A::DArray{T,N}) where {T,N}
        colors = [:red, :green, :yellow, :blue, :magenta, :cyan]
        color_map = [colors[mod1(idx, length(colors))] for idx in 1:length(A.chunks)]
        return new{T,N}(A, color_map, Dict{NTuple{N,Int},Union{Some{T},Nothing}}())
    end
end
Base.size(A::ColorArray) = size(A.A)
Base.getindex(A::ColorArray, idx::Integer) = getindex(A, (idx,))
Base.getindex(A::ColorArray, idxs::Integer...) = getindex(A, (idxs...,))
function Base.getindex(A::ColorArray{T,N}, idxs::NTuple{N,Int}) where {T,N}
    sd_idx_tuple, _ = partition_for(A.A, idxs)
    sd_idx = CartesianIndex(sd_idx_tuple)
    sd_idx_linear = LinearIndices(A.A.chunks)[sd_idx]
    if !haskey(A.seen_values, idxs)
        chunk = A.A.chunks[sd_idx]
        if chunk isa Chunk || isready(chunk)
            value = A.seen_values[idxs] = Some(getindex(A.A, idxs))
        else
            # Show a placeholder instead
            value = A.seen_values[idxs] = nothing
        end
    else
        value = A.seen_values[idxs]
    end
    if value !== nothing
        color = A.color_map[sd_idx_linear]
    else
        color = :light_black
    end
    return ColorElement{T}(color, value)
end
function Base.getindex(A::ColorArray{T,N}, idxs::Dims{S}) where {T,N,S}
    if S > N
        if all(idxs[(N+1):end] .== 1)
            return getindex(A, idxs[1:N])
        else
            throw(BoundsError(A, idxs))
        end
    elseif S < N
        throw(BoundsError(A, idxs))
    end
end
function Base.show(io::IO, ::MIME"text/plain", A::DArray{T,N}) where {T,N}
    if N == 1
        write(io, "$(length(A))-element ")
        write(io, string(DVector{T}))
    elseif N == 2
        write(io, "$(join(size(A), 'x')) ")
        write(io, string(DMatrix{T}))
    elseif N == 0
        write(io, "0-dimensional ")
        write(io, "DArray{$T, $N}")
    else
        write(io, "$(join(size(A), 'x')) ")
        write(io, "DArray{$T, $N}")
    end
    nparts = N > 0 ? size(A.chunks) : 1
    partsize = N > 0 ? A.partitioning.blocksize : 1
    write(io, " with $(join(nparts, 'x')) partitions of size $(join(partsize, 'x')):")
    pct_complete = 100 * (sum(c->c isa Chunk ? true : isready(c), A.chunks) / length(A.chunks))
    if pct_complete < 100
        println(io)
        printstyled(io, "~$(round(Int, pct_complete))% completed"; color=:yellow)
    end
    println(io)
    # FIXME: with_index_caching(1) do
        Base.print_array(IOContext(io, :compact=>true), ColorArray(A))
    # end
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
    subchunks, subdomains = lookup_parts(c, chunks(c), domainchunks(c), d)
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
function lookup_parts(A::DArray, ps::AbstractArray, subdmns::DomainBlocks{N}, d::ArrayDomain{N}) where N
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
    return pieces, out_dmn
end
function lookup_parts(A::DArray, ps::AbstractArray, subdmns::DomainBlocks{N}, d::ArrayDomain{S}) where {N,S}
    if S != 1
        throw(BoundsError(A, d.indexes))
    end
    inds = CartesianIndices(A)[d.indexes...]
    new_d = ntuple(i->first(inds).I[i]:last(inds).I[i], N)
    return lookup_parts(A, ps, subdmns, ArrayDomain(new_d))
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

"""
    AutoBlocks

Automatically determines the size and number of blocks for a distributed array.
This may construct any kind of `Dagger.AbstractBlocks` partitioning.
"""
struct AutoBlocks end
function auto_blocks(dims::Dims{N}) where N
    # TODO: Allow other partitioning schemes
    np = num_processors()
    p = N > 0 ? cld(dims[end], np) : 1
    return Blocks(ntuple(i->i == N ? p : dims[i], N))
end
auto_blocks(A::AbstractArray{T,N}) where {T,N} = auto_blocks(size(A))

distribute(A::AbstractArray) = distribute(A, AutoBlocks())
distribute(A::AbstractArray{T,N}, dist::Blocks{N}) where {T,N} =
    _to_darray(Distribute(dist, A))
distribute(A::AbstractArray, ::AutoBlocks) = distribute(A, auto_blocks(A))
function distribute(x::AbstractArray{T,N}, n::NTuple{N}) where {T,N}
    p = map((d, dn)->ceil(Int, d / dn), size(x), n)
    distribute(x, Blocks(p))
end
distribute(x::AbstractVector, n::Int) = distribute(x, (n,))
distribute(x::AbstractVector, n::Vector{<:Integer}) =
    distribute(x, DomainBlocks((1,), (cumsum(n),)))

DVector(A::AbstractVector{T}, part::Blocks{1}) where T = distribute(A, part)
DMatrix(A::AbstractMatrix{T}, part::Blocks{2}) where T = distribute(A, part)
DArray(A::AbstractArray{T,N}, part::Blocks{N}) where {T,N} = distribute(A, part)

DVector(A::AbstractVector{T}) where T = DVector(A, AutoBlocks())
DMatrix(A::AbstractMatrix{T}) where T = DMatrix(A, AutoBlocks())
DArray(A::AbstractArray) = DArray(A, AutoBlocks())

DVector(A::AbstractVector{T}, ::AutoBlocks) where T = DVector(A, auto_blocks(A))
DMatrix(A::AbstractMatrix{T}, ::AutoBlocks) where T = DMatrix(A, auto_blocks(A))
DArray(A::AbstractArray, ::AutoBlocks) = DArray(A, auto_blocks(A))

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
