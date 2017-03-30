
import Base: view
export chunk, gather

"""
A Chunk is a recipe to read an object. The Chunk type holds information about the
domain spanned by the chunk on its own and metadata to read the chunk from its
memory / storage / network location.

`gather(reader, handle)` will bring the data to memory on the caller
"""
@compat abstract type AbstractChunk end

chunks(x::AbstractChunk) = x
affinity(::AbstractChunk) = []

"""
    gather(context, chunk::AbstractChunk)

Get the data stored in a chunk
"""
function gather end


###### Chunk ######

"""
A chunk with some data
"""
type Chunk{T, H} <: AbstractChunk
    chunktype::Type{T}
    domain
    handle::H
    persist::Bool
end

domain(c::Chunk) = c.domain
chunktype(c::Chunk) = c.chunktype
persist!(t::Chunk) = (t.persist=true; t)
shouldpersist(p::Chunk) = t.persist
affinity(c::Chunk) = affinity(c.handle)

function gather(ctx, chunk::Chunk)
    # delegate fetching to handle by default.
    gather(ctx, chunk.handle)
end


### ChunkIO
gather(ctx, ref::MemToken) = fetch(ref)
affinity(c::MemToken) = [OSProc(c.where)]

"""
Create a chunk from a sequential object.
"""
function tochunk(x; persist=false)
    ref = make_token(x)
    Chunk(typeof(x), domain(x), ref, true)
end
tochunk(x::AbstractChunk) = x


"""
A **view** into an AbstractChunk

Fields:
 - domain: The domain of the viewed chunk on its own
 - subdomain: The subdomain in `chunk`
 - chunk: The chunk being viewed
"""
type View{T<:AbstractChunk} <: AbstractChunk
    chunktype::Type
    domain::Domain
    subdomain::Domain
    chunk::T
end

domain(c::View) = c.domain
chunktype(c::View) = c.chunktype
persist!(x::View) = persist!(x.chunk)
affinity(c::View) = affinity(c.chunk)

function gather(ctx, s::View)
    # A View{T<:Chunk{X}} can try to make this efficient for X
    gather(ctx, s.chunk)[s.subdomain]
end
function gather{X}(ctx, s::View{Chunk{X, MemToken}})
    ref = s.chunk.handle
    pid = ref.where
    let d = s.subdomain
        remotecall_fetch(x -> fetch(x)[d], pid, ref)
    end
end

"""
    `view(a::Chunk, d::Domain)`

Returns the `View` object which represents a view chunk of `a`
"""
function view(p::Chunk, d::Domain, T=chunktype(p))

    if domain(p) == d
        return p
    end

    View(T, alignfirst(d), d, p)
end
Base.getindex(x::AbstractChunk, idx::Domain) = view(x, idx)

function view(s::View, d)
    dprime = s.subdomain[d] # collapse subindex
    view(s.chunk, dprime)
end

"""
A collection of Parts put together to form a bigger logical chunk

Fields:
 - chunktype: The type of the data represented by the Cat
 - domain: The domain of the concatenated Chunk
 - chunks: the chunks which form the chunks of the Cat
"""
type Cat{T} <: AbstractChunk
    chunktype::Type{T}
    domain::Domain
    domainchunks
    chunks
end

domain(c::Cat) = c.domain
chunktype(c::Cat) = c.chunktype
chunks(x::Cat) = x.chunks
domainchunks(x::Cat) = x.domainchunks
persist!(x::Cat) = (for p in chunks(x); persist!(p); end)
affinity(c::Cat) = [Set(reduce(vcat, map(affinity, c.chunks)))...]

function gather(ctx, chunk::Cat)
    ps_input = chunks(chunk)
    ps = Array{chunktype(chunk)}(size(ps_input))
    @sync for i in 1:length(ps_input)
        @async ps[i] = gather(ctx, ps_input[i])
    end
    cat_data(chunktype(chunk), domain(chunk), domainchunks(chunk), ps)
end

"""
`view` of a `Cat` chunk returns a `Cat` of view chunks
"""
function view(c::Cat, d)
    subchunks, subdomains = lookup_parts(chunks(c), domainchunks(c), d)
    if length(subchunks) == 1
        subchunks[1]
    else
        Cat(chunktype(c), alignfirst(d), subdomains, subchunks)
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
        pieces[i] = view(ps[idx...], project(subdmns[idx...], dmn))
    end
    out_cumlength = map(g->_cumsum(map(x->length(x[2]), g)), groups)
    out_dmn = DomainBlocks(ntuple(x->1,Val{N}), out_cumlength)
    pieces, out_dmn
end

function free!(x::Cat, force=true)
    for p in chunks(x)
        free!(p, force)
    end
end
# Check to see if the node is set to persist
# if it is foce can override it
function free!{X}(s::Chunk{X, MemToken}, force=true)
    if force || !s.persist
        release_token(s.handle)
    end
end
free!(s::AbstractChunk, force=true) = nothing
free!(s::View, force=true) = nothing


Base.@deprecate_binding AbstractPart AbstractChunk
Base.@deprecate_binding Part Chunk
Base.@deprecate_binding Sub  View
Base.@deprecate parts(args...) chunks(args...)
Base.@deprecate part(args...) tochunk(args...)
Base.@deprecate parttype(args...) chunktype(args...)
