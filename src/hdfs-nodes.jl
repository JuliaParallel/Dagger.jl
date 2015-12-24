import Base: split, readlines, eltype, open
export HDFSFileNode

### read HDFS files
immutable HDFSFileNode <: ComputeNode
    host::AbstractString
    port::Int
    path::AbstractString
end

workerip(id::Int) = workerip(Base.worker_from_id(id))
workerip(l::LocalProcess) = l.bind_addr
workerip(w::Base.Worker) = get(w.config.bind_addr)

file(f::HDFSFileNode) = HDFSFile(HDFSClient(f.host,f.port), f.path)
open(f::HDFSFileNode) = open(file(f))

function blkopen(node, offset::UInt, endoffset::UInt, delim)
    try
        println("opening hdfs://$(node.host):$(node.port)$(node.path) at offset $offset:$endoffset with delim '$delim'")
        bio = BlockIO(open(node), offset:endoffset, delim)
        return bio
    catch ex
        println("Exception $ex")
        rethrow(ex)
    end
end

function compute(ctx, node::HDFSFileNode, delim::Union{Char, Void}=nothing)
    hfile = file(node)
    finfo = stat(hfile)
    isfile(finfo) || error("Only HDFS files supported for now")
    fsz = filesize(finfo)
    (fsz > 0) || error("HDFS file is empty")
    block_sz = finfo.block_sz

    blks = hdfs_blocks(hfile)
   
    pidmap = Dict{AbstractString, Int}()
    pids = chunk_targets(ctx, node)
    for pid in pids
        pidmap[workerip(pid)] = pid
    end

    refs = Pair[]
    for (offset, ips) in blks
        pid = 0
        # for now, allocate block to the first matching node, or a random node if there's no worker there
        for ip in ips
            if haskey(pidmap, ip)
                pid = pidmap[ip]
                break
            end
        end
        (pid == 0) && (pid = pids[randperm(length(pids))[1]])
        endoffset = min(offset+block_sz, fsz)
        ref = remotecall(blkopen, pid, node, offset+1, endoffset, delim)
        push!(refs, pid => ref)
    end
    FileDataNode(refs, block_sz)
end

immutable HDFSSplitNode <: ComputeNode 
    delim::Char
    input::HDFSFileNode
end
split(f::HDFSFileNode, char) = HDFSSplitNode(char, f)

function compute(ctx, f::HDFSSplitNode)
    # Just use the map implementation by default
    compute(ctx, mappart(x->x, f))
end

readlines(f::HDFSFileNode) = SplitNode('\n', f)

function compute(ctx, node::MapPartNode{Tuple{HDFSSplitNode}})
    data = compute(ctx, node.input[1].input) # Compute HDFSFileNode
    delim = node.input[1].delim
    chunksize = data.chunksize
    targets = chunk_targets(ctx, node)

    refs = Pair[pid => @spawnat pid begin
            node.f(ChunkedSplitter(fetch(ref), delim, chunksize))
        end for (pid, ref) in data.refs]

    DistMemory(refs, CutDimension{1}())
end
