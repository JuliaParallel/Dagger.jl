abstract AbstractChunk
abstract ChunkReader

"""
A chunk is a self-contained object. The Chunk type holds information about the
domain spanned by the chunk on its own and metadata to read the chunk from its
memory / storage / network location.

Fields:

 - domain: The domain that spans the whole chunk
 - reader: A ChunkReader type which specifies how to read the chunk into memory
 - handle: Some handle to give to the reader while loading

`load(proc, reader, handle)` will bring the data to memory on the processor
"""
type Chunk{R<:ChunkReader} <: AbstractChunk
    domain::Domain
    size::Int
    reader::R
    handle::Any
end

"""
A view into an AbstractChunk used to create a smaller logical chunk

Fields:
 - domain: The domain of this chunk on its own
 - subdomain: The subdomain viewd in `chunk`
 - chunk: The chunk being viewed
"""
type SubChunk <: AbstractChunk
    domain::Domain
    size::Int
    subdomain::Domain
    chunk::AbstractChunk
end

"""
A collection of Chunks put together to form a bigger logical chunk

Fields:
 - domain: The domain of the combined chunk standing on its own
 - layout: The layout scheme used to divide child chunks into a big chunk
 - subdomains: The subdomains spanned by the child chunks
 - children: the chunks which form the parts of the SuperChunk
"""
type SuperChunk <: AbstractChunk
    domain::DomainBranch
    partition::PartitionScheme
    children::AbstractArray
end

"""
Get the domain of a chunk on its own
"""
domain(c::AbstractChunk) = c.domain
size(c::AbstractChunk) = c.size
size(c::SuperChunk) = sum(map(size, c.children))


######## Getting data from a chunk ##########

"""
    load(context, chunk::AbstractChunk)

Get the data stored in a chunk
"""
function load end

function load(ctx, chunk::Chunk)
    load(ctx, chunk.reader, chunk.handle)
end

function load(ctx, chunk::SubChunk)
    load(ctx, chunk.chunk)[chunk.subdomain]
end

function load(ctx, chunk::SuperChunk)
    c = chunk.children[1]
    i = 2
    while i <= length(chunk.children)
        c = cat(ctx, chunk.layout, c, chunk.children[i])
        i += 1
    end
    c
end

"""
    save(context, file, chunk::AbstractChunk)
Save a chunk to a file.
"""
function save end
