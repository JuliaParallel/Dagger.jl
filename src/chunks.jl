
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
function unrelease{T}(c::Chunk{T,MemToken})
    if unrelease_token(c.handle)
        Nullable{Any}(c)
    else
        Nullable{Any}()
    end
end
unrelease(c::Chunk) = c

function gather(ctx, chunk::Chunk)
    # delegate fetching to handle by default.
    gather(ctx, chunk.handle)
end


### ChunkIO
function gather(ctx, ref::MemToken)
    res = fetch(ref)
    if isnull(res)
        throw(KeyError(ref))
    else
        get(res)
    end
end
affinity(c::MemToken) = [OSProc(c.where)=>c.size]

"""
Create a chunk from a sequential object.
"""
function tochunk(x; persist=false)
    ref = make_token(x)
    Chunk(typeof(x), domain(x), ref, persist)
end
tochunk(x::AbstractChunk) = x

# Check to see if the node is set to persist
# if it is foce can override it
function free!{X}(s::Chunk{X, MemToken}; force=true, cache=false)
    if force || !s.persist
        release_token(s.handle, cache)
    end
end
free!(x; force=true,cache=false) = x # catch-all for non-chunks


Base.@deprecate_binding AbstractPart AbstractChunk
Base.@deprecate_binding Part Chunk
Base.@deprecate parts(args...) chunks(args...)
Base.@deprecate part(args...) tochunk(args...)
Base.@deprecate parttype(args...) chunktype(args...)
