include("lib/block-io.jl")
include("lib/chunk-split.jl")

import Base: split, readlines, eltype
export TextFile, FileNode

### A recipe to read a file
immutable FileNode <: ComputeNode
    file::AbstractString
    mode::AbstractString
    chunksize::Int # At each process, bring in these many bytes at once
end
FileNode(x, m) = FileNode(x, m, 128*1024*1024)
FileNode(x) = FileNode(x, "r") # 32MB chunks
TextFile(x, args...) = FileNode(x, args...)
Base.convert(::Type{FileNode}, x::AbstractString) = FileNode(x)

# The Data node
immutable FileDataNode <: DataNode
    refs::Vector
    chunksize::Int
end

function compute(ctx, f::FileNode, delim::Union{Char, Void}=nothing)
    @show sz = stat(f.file).size

    targets = chunk_targets(ctx, f)
    @show ranges = map(x->x[1], slice_indexes(ctx, (sz,), CutDimension{1}(), targets))
    refs = Pair[targets[i] => @spawnat targets[i] begin
            BlockIO(open(f.file, f.mode), ranges[i], delim)
        end for i in 1:length(targets)]

    FileDataNode(refs, f.chunksize)
end

immutable SplitNode <: ComputeNode
    delim::Char
    input::FileNode
end

"""
Read lines from disjoint layouts of a file on different processes.
"""
readlines(f::FileNode) = SplitNode('\n', f)
"""
Split on occurance of a char and read disjoint blocks of the same file
"""
split(f::FileNode, char) = SplitNode(char, f)

function compute(ctx, node::MapPartNode{Tuple{SplitNode}})
    data = compute(ctx, node.input[1].input) # Compute FileNode
    delim = node.input[1].delim
    chunksize = data.chunksize
    targets = chunk_targets(ctx, node)

    refs = Pair[pid => @spawnat pid begin
            node.f(ChunkedSplitter(fetch(ref), delim, chunksize))
        end for (pid, ref) in data.refs]

    DistMemory(refs, CutDimension{1}())
end

function compute(ctx, f::SplitNode)
    # Just use the map implementation by default
    compute(ctx, mappart(x->x, f))
end

### WIP: Read array data from a file ###

immutable FileArray{T, L<:AbstractLayout} <: ComputeNode
    dims::Tuple
    layout::L
    input::FileNode
    # check for isbits type
end

FileArray(T, dims, input) = FileArray{T}(dims, input)
eltype{T}(f::FileArray{T}) = T

function compute(ctx, fa::FileArray)
    @assert isbits(eltype(fa))

    # For each chunk target figure out an offset
    # todo: enforce targets to be on the same machine
    targets = chunk_targets(ctx, fa)

    idx_chunks = slice_indexes(ctx, fa.dims, fa.layout, targets)
    chunk_sizes = [prod(map(length, chunk)) for chunk in idx_chunks] .* sizeof(T)

    f = open(fa.input.file, "r+")

    # just make sure the file is created first
    array = Mmap.mmap(f, Array{eltype(fa), length(fa.dims)}, fa.dims)

    lastbytes = cumsum(chunk_sizes)
    offsets = vcat(0, 1:lastbytes[end-1]) + 1

    fname = fa.input.file

    refs = Pair[targets[i] => @spawnat targets[i] begin
            f = open(fname, "r+")
            seek(f, offsets[i])
        end for i in 1:length(targets)]

    DistMemory(refs, fa.layout)
end

