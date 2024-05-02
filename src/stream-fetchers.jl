struct RemoteFetcher end
function stream_fetch_values!(::Type{RemoteFetcher}, T, store_ref::Chunk{Store_remote}, buffer::Blocal, id::UInt) where {Store_remote, Blocal}
    thunk_id = STREAM_THUNK_ID[]
    @dagdebug thunk_id :stream "fetching values"
    @label fetch_values
    # FIXME: Pass buffer free space
    # TODO: It would be ideal if we could wait on store.lock, but get unlocked during migration
    values = MemPool.access_ref(store_ref.handle, id, T, Store_remote, thunk_id) do store, id, T, Store_remote, thunk_id
        if !isopen(store)
            throw(InvalidStateException("Stream is closed", :closed))
        end
        @dagdebug thunk_id :stream "trying to fetch values at $(myid())"
        store::Store_remote
        in_store = store
        STREAM_THUNK_ID[] = thunk_id
        values = T[]
        while !isempty(store, id)
            value = take!(store, id)::T
            push!(values, value)
        end
        return values
    end::Vector{T}
    if isempty(values)
        @goto fetch_values
    end

    @dagdebug thunk_id :stream "fetched $(length(values)) values"
    for value in values
        put!(buffer, value)
    end
end
