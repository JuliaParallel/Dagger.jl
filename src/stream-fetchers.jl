struct RemoteFetcher end
function stream_fetch_values!(::Type{RemoteFetcher}, T, store_ref::Chunk{Store_remote}, buffer::Blocal, id::UInt) where {Store_remote, Blocal}
    if store_ref.handle.owner == myid()
        store = fetch(store_ref)::Store_remote
        while !isfull(buffer)
            value = take!(store, id)::T
            put!(buffer, value)
        end
    else
        thunk_id = STREAM_THUNK_ID[]
        values = remotecall_fetch(store_ref.handle.owner, store_ref.handle, id, T, Store_remote) do store_ref, id, T, Store_remote
            STREAM_THUNK_ID[] = thunk_id
            store = MemPool.poolget(store_ref)::Store_remote
            values = T[]
            while !isempty(store, id)
                value = take!(store, id)::T
                push!(values, value)
            end
            return values
        end::Vector{T}
        for value in values
            put!(buffer, value)
        end
    end
end
