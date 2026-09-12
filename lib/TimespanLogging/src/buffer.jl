"""
Chunk capacity for per-thread log buffers. Full chunks are linked and
published; the open chunk holds a partial tail. Tests that overflow this
capacity exercise the publish path.
"""
const CHUNK_CAPACITY = 256

"""
Default hard cap on published slabs per `ChunkList` (preference
`max_chunks`, default 1024). Override one category with
`max_chunks_<symbol>` (e.g. `max_chunks_compute = 4096`).
1024 × 256 = 262144 events per thread per list; further events overwrite
the open chunk and increment `dropped`.
"""
const MAX_CHUNKS = Int(@load_preference("max_chunks", 1024))

function max_chunks(::Type{C}) where C <: LogCategory
    key = string("max_chunks_", category_symbol(C))
    v = load_preference(TimespanLogging, key, nothing)
    return v === nothing ? MAX_CHUNKS : Int(v)
end

mutable struct LogChunk{E}
    const events::Vector{E}
    len::Int
    next::Union{LogChunk{E}, Nothing}
end

function LogChunk{E}(cap::Int=CHUNK_CAPACITY) where E
    return LogChunk{E}(Vector{E}(undef, cap), 0, nothing)
end

"""
    ChunkList{E}

Single-writer growable list of event slabs. The writer fills `open`; when
it is full the slab is prepended to `published` and a new `open` is
allocated. `lock` is a per-list spinlock: only the owning thread writes,
and `steal!` takes it briefly during `get_logs!`.
"""
mutable struct ChunkList{E}
    open::LogChunk{E}
    published::Union{LogChunk{E}, Nothing}
    npublished::Int
    dropped::Int
    lock::Threads.SpinLock
    const max_chunks::Int
end

function ChunkList{E}(max_chunks::Int=MAX_CHUNKS) where E
    return ChunkList{E}(LogChunk{E}(), nothing, 0, 0, Threads.SpinLock(), max_chunks)
end

@inline function push_event!(list::ChunkList{E}, ev::E) where E
    @lock list.lock begin
        chunk = list.open
        n = chunk.len
        if n == length(chunk.events)
            if list.npublished >= list.max_chunks
                # Bound memory: reuse the open slab instead of allocating.
                list.dropped += n
                n = 0
            else
                chunk.next = list.published
                list.published = chunk
                list.npublished += 1
                chunk = LogChunk{E}()
                list.open = chunk
                n = 0
            end
        end
        @inbounds chunk.events[n + 1] = ev
        chunk.len = n + 1
    end
    return nothing
end

"""
    steal!(list) -> (open, published)

Detach the current chain in O(1) (plus a new empty open chunk) so the
writer can continue. `published` is newest-first.
"""
function steal!(list::ChunkList{E}) where E
    @lock list.lock begin
        open = list.open
        pub = list.published
        list.published = nothing
        list.npublished = 0
        list.open = LogChunk{E}()
        return (open, pub)
    end
end

function chunk_count(open::LogChunk, pub::Union{LogChunk, Nothing})
    n = 1
    while pub !== nothing
        n += 1
        pub = pub.next
    end
    return n
end

function event_count(open::LogChunk, pub::Union{LogChunk, Nothing})
    n = open.len
    while pub !== nothing
        n += pub.len
        pub = pub.next
    end
    return n
end

"""
Append stolen chunks to `out` in chronological order (oldest first).
"""
function collect_events!(out::Vector{E}, open::LogChunk{E}, pub::Union{LogChunk{E}, Nothing}) where E
    # `published` is newest-first; reverse onto a small stack.
    npub = 0
    p = pub
    while p !== nothing
        npub += 1
        p = p.next
    end
    if npub > 0
        stack = Vector{LogChunk{E}}(undef, npub)
        p = pub
        i = npub
        while p !== nothing
            @inbounds stack[i] = p
            i -= 1
            p = p.next
        end
        for c in stack
            if c.len > 0
                append!(out, @view c.events[1:c.len])
            end
        end
    end
    if open.len > 0
        append!(out, @view open.events[1:open.len])
    end
    return out
end
