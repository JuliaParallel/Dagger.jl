"""
Compute node for Files/IO.
Computing these will result in a `SharedMemory` data node with
the offsets on the data node as specified by the Partition.
"""

immutable FileBlocks{P <: AbstractPartition} <: ComputeNode
    uri::AbstractString #Change to URI
    partition::P
end

FileBlocks(uri::AbstractString) = FileBlocks(uri, BytesPartition{nworkers()}())

"""
Data Node for Shared Memory. (Dist File?)
This contains the mmaped file handle, the offset mapping to the processes and the
partition.
This node basically means the data has been created into an mmaped file and we
have a pid -> offset/BlockIO ref mapping set up.
"""
immutable SharedMemory{P<:AbstractPartition} <: DataNode
    sharedarray::Array #Entire mmaped file
    offsets::Vector #Mapping btwn PIDs and the offsets they are responsible for.
    refs::Vector #RemoteRefs to the mmaps on each process.
    partition::P
end

function gather(ctx, n::SharedMemory)
    #This returns the entire mmap. Partition does not matter.
    #Write to a file and return the file handle here?
    n.sharedarray
end

##### Compute #####

"""
Computing a FileBlock, will create a SharedMemory with the specified partition.
"""
function compute(ctx, x::FileBlocks)
    targets = chunk_targets(ctx, x)
    f = open(x.uri) #Opens a file handle to the file on the FileSystem
    #Reading them as bytes.
    sharedarray = Mmap.mmap(f, Vector{UInt8}, filesize(f)-1, shared=true)
    offsets = slice(ctx, x.uri, x.partition, targets) #Expects a vector of ranges
    refs = Pair[
        (targets[i] => remotecall(
                targets[i], 
                (x, y) -> Mmap.mmap(x, Vector{UInt8}, (y.stop - y.start + 1,), y.start),
                x.uri,
                offsets[i]
            )
        )
        for i in 1:length(targets)
        ]
    SharedMemory(sharedarray, offsets, refs, x.partition)
end

"""
Computing a MapPartNode on a SharedMemory datanode will return a DistMemory
datanode distributed among the targets of SharedMemory.
"""
function compute{N, T<:SharedMemory}(ctx, node::MapPartNode{NTuple{N, T}})
    refsets = zip(map(x -> map(y->y[2], refs(x)), node.input)...) |> collect
    pids = map(x->x[1], refs(node.input[1]))
    pid_chunks = zip(pids, map(tuplize, refsets)) |> collect

    let f = node.f
        futures = Pair[pid => @spawnat pid f(map(fetch, rs)...)
                        for (pid, rs) in pid_chunks]
        DistMemory(futures, node.input[1].partition)
    end
end

##### Partition ####

immutable BytesPartition{n} <: AbstractPartition end
bytespartition(n) = BytesPartition{n}

function byte_splits(startpos, fsize, parts)
    len = fsize - startpos
    starts = len >= parts ?
        round(Int, linspace(0, len, parts+1)) :
        [[0:len;], zeros(Int, parts-len);]
    map((x,y) -> x:y, starts[1:end-1], starts[2:end] .- 1)
end

function slice{n}(ctx, uri, ::BytesPartition{n}, targets)
    f = open(uri)
    offsets = byte_splits(position(f), filesize(f), length(targets)) 
end

#Doesn't work nicely. Maybe create an mmaped file and grow it with the others?
#Or return all the remoterefs to the mmaps rather than concatenating them?
function gather{n}(ctx, p::BytesPartition{n}, xs::Vector)
    warn("BytesPartition should be used with SharedMemory only.") 
end
