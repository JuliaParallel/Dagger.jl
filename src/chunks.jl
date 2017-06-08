
export chunk, gather

export domain, UnitDomain, project, alignfirst, ArrayDomain

import Base: isempty, getindex, intersect,
             ==, size, length, ndims

"""
    domain(x::T)

Returns metadata about `x`. This metadata will be in the `domain`
field of a Chunk object when an object of type `T` is created as
the result of evaluating a Thunk.
"""
function domain end

"""
Default domain -- has no information about the value
"""
immutable UnitDomain end

"""
If no `domain` method is defined on an object, then
we use the `UnitDomain` on it. A `UnitDomain` is indivisible.
"""
domain(x::Any) = UnitDomain()

###### Chunk ######

"""
A chunk with some data
"""
type Chunk{T, H}
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
tochunk(x::Union{Chunk, Thunk}) = x

# Check to see if the node is set to persist
# if it is foce can override it
function free!{X}(s::Chunk{X, MemToken}; force=true, cache=false)
    if force || !s.persist
        release_token(s.handle, cache)
    end
end
free!(x; force=true,cache=false) = x # catch-all for non-chunks


Base.@deprecate_binding AbstractPart Union{Chunk, Thunk}
Base.@deprecate_binding Part Chunk
Base.@deprecate parts(args...) chunks(args...)
Base.@deprecate part(args...) tochunk(args...)
Base.@deprecate parttype(args...) chunktype(args...)
