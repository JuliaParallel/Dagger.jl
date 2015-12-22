import Base: start, next, done

export ChunkedSplitter

immutable ChunkedSplitter
    file::IO
    delim::Char
    chunksize::Int
end

type ChunkIterState
    offset::Int
    idx::Int
    parts::Array
end

function start(c::ChunkedSplitter)
    tic()
    @show firstchunk = BlockIO(c.file, 1:c.chunksize, c.delim)
    ret = ChunkIterState(1, 1, split(readall(firstchunk), c.delim))
    t = toq()
    println("spltting : ", t)
    ret
end

done(c::ChunkedSplitter, s::ChunkIterState) = s.offset + s.idx - 1 > filesize(c.file)

function next(c::ChunkedSplitter, s::ChunkIterState)
    nxt = if s.idx >= length(s.parts)
        nextoffset = s.offset + c.chunksize
        nextchunk = BlockIO(c.file, nextoffset:(nextoffset+c.chunksize-1), c.delim)
        s.parts = split(readall(nextchunk), c.delim)
        s.offset = nextoffset
        s.idx = 2
        s.parts[1], s
    else
        s.idx += 1
        s.parts[s.idx], s
    end
end

function mapreduce(f, op, v0, c::ChunkedSplitter)
    s = start(c)
    acc = v0
    while !done(c, s)
        _, s = next(c,s)
        acc = mapreduce(f, op, acc, s.parts)
        s.idx = length(s.parts)
    end
    acc
end

function mapreducebykey_seq(f, op, v0, c::ChunkedSplitter, acc=Dict())
    @show v0
    s = start(c)
    while !done(c, s)
        _, s = next(c,s)
        acc = mapreducebykey_seq(f, op, v0, s.parts, acc)
        s.idx = length(s.parts)
    end
    acc
end
