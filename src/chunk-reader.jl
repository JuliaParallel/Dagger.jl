immutable DistMem <: ChunkReader end

load(ctx, ::DistMem, handle::RemoteRef) = fetch(handle)

function move(ctx, proc::OSProc, chunk::Chunk{DistMem})
    handle = remotecall(proc.pid, fetch, chunk.handle)
    Chunk(chunk.domain, chunk.reader, handle)
end

