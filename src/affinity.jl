export domain, UnitDomain, project, alignfirst, ArrayDomain

import Base: isempty, getindex, intersect, ==, size, length, ndims

"""
    domain(x::T)

Returns metadata about `x`. This metadata will be in the `domain`
field of a Chunk object when an object of type `T` is created as
the result of evaluating a Thunk.
"""
function domain end

"""
    UnitDomain

Default domain -- has no information about the value
"""
struct UnitDomain end

"""
If no `domain` method is defined on an object, then
we use the `UnitDomain` on it. A `UnitDomain` is indivisible.
"""
domain(x::Any) = UnitDomain()

### ChunkIO
affinity(r::DRef) = OSProc(r.owner)=>r.size
# this previously returned a vector with all machines that had the file cached
# but now only returns the owner and size, for consistency with affinity(::DRef),
# see #295
affinity(r::FileRef) = OSProc(1)=>r.size
