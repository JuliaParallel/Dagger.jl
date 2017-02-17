
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

"""
    gather(context, chunk::AbstractChunk)

Get the data stored in a chunk
"""
function gather end


###### Chunk ######

@compat abstract type ChunkIO end
"""
A chunk with some data
"""
type Chunk{I<:ChunkIO} <: AbstractChunk
    parttype::Type
    domain::Domain
    handle::I
    persist::Bool
end

domain(c::Chunk) = c.domain
parttype(c::Chunk) = c.parttype
persist!(t::Chunk) = (t.persist=true; t)
shouldpersist(p::Chunk) = t.persist
function gather(ctx, chunk::Chunk)
    # delegate fetching to handle by default.
    gather(ctx, chunk.handle)
end


### ChunkIO

immutable DistMem <: ChunkIO
    ref::MemToken
end
gather(ctx, io::DistMem) = fetch(io.ref)

"""
Create a chunk from a sequential object.
"""
function tochunk(x)
    ref = make_token(x)
    Chunk(typeof(x), domain(x), DistMem(ref), false)
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
    parttype::Type
    domain::Domain
    subdomain::Domain
    chunk::T
end

domain(c::View) = c.domain
parttype(c::View) = c.parttype
persist!(x::View) = persist!(x.chunk)

function gather(ctx, s::View)
    # A View{T<:Chunk{X}} can try to make this efficient for X
    gather(ctx, s.chunk)[s.subdomain]
end
# optimized subindexing on DistMem
function gather(ctx, s::View{Chunk{DistMem}})
    ref = s.chunk.handle.ref
    pid = ref.where
    let d = s.subdomain
        remotecall_fetch(x -> fetch(x)[d], pid, ref)
    end
end

"""
    `view(a::Chunk, d::Domain)`

Returns the `View` object which represents a view chunk of `a`
"""
function view(p::Chunk, d::Domain, T=parttype(p))

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
 - parttype: The type of the data represented by the Cat
 - domain: The domain of the concatenated Chunk
 - chunks: the chunks which form the chunks of the Cat
"""
type Cat <: AbstractChunk
    parttype::Type
    domain::Domain
    domainchunks
    chunks
end

domain(c::Cat) = c.domain
parttype(c::Cat) = c.parttype
chunks(x::Cat) = x.chunks
domainchunks(x::Cat) = x.domainchunks
persist!(x::Cat) = (for p in chunks(x); persist!(p); end)

function gather(ctx, chunk::Cat)
    ps_input = chunks(chunk)
    ps = Array{parttype(chunk)}(size(ps_input))
    @sync for i in 1:length(ps_input)
        @async ps[i] = gather(ctx, ps_input[i])
    end
    cat_data(parttype(chunk), domain(chunk), domainchunks(chunk), ps)
end

"""
`view` of a `Cat` chunk returns a `Cat` of view chunks
"""
function view(c::Cat, d)
    subchunks, subdomains = lookup_parts(chunks(c), domainchunks(c), d)
    if length(subchunks) == 1
        subchunks[1]
    else
        Cat(parttype(c), alignfirst(d), subdomains, subchunks)
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
function lookup_parts{N}(ps::AbstractArray, subdmns::BlockedDomains{N}, d::ArrayDomain{N})
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
    out_dmn = BlockedDomains(ntuple(x->1,Val{N}), out_cumlength)
    pieces, out_dmn
end

function free!(x::Cat, force=true)
    for p in chunks(x)
        free!(p, force)
    end
end
# Check to see if the node is set to persist
# if it is foce can override it
function free!(s::Chunk{DistMem}, force=true)
    if force || !s.persist
        release_token(s.handle.ref)
    end
end
free!(s::AbstractChunk, force=true) = nothing
free!(s::View, force=true) = nothing

Base.@deprecate_binding AbstractPart AbstractChunk
Base.@deprecate_binding Part Chunk
Base.@deprecate_binding Sub  View
Base.@deprecate parts(args...) chunks(args...)
Base.@deprecate part(args...) tochunk(args...)
