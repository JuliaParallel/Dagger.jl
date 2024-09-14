struct RemoteFetcher end
function stream_pull_values!(::Type{RemoteFetcher}, T, store_ref::Chunk{Store_remote}, buffer::Blocal, id::UInt) where {Store_remote, Blocal}
    thunk_id = STREAM_THUNK_ID[]
    @dagdebug thunk_id :stream "fetching values"

    values = T[]
    free_space = length(buffer.buffer) - length(buffer)
    while isempty(values)
        # FIXME: Pass buffer free space
        # TODO: It would be ideal if we could wait on store.lock, but get unlocked during migration
        values = MemPool.access_ref(store_ref.handle, id, T, Store_remote, thunk_id, free_space) do store, id, T, Store_remote, thunk_id, free_space
            if !isopen(store)
                throw(InvalidStateException("Stream is closed", :closed))
            end
            @dagdebug thunk_id :stream "trying to fetch values at $(myid())"
            store::Store_remote
            in_store = store
            STREAM_THUNK_ID[] = thunk_id
            values = T[]
            while !isempty(store, id) && length(values) < free_space
                value = take!(store, id)::T
                @dagdebug thunk_id :stream "fetched $value"
                push!(values, value)
            end
            return values
        end::Vector{T}

        # We explicitly yield in the loop to allow other tasks to run. This
        # matters on single-threaded instances because MemPool.access_ref()
        # might not yield when accessing data locally, which can cause this loop
        # to spin forever.
        yield()
        task_may_cancel!()
    end

    @dagdebug thunk_id :stream "fetched $(length(values)) values"
    for value in values
        put!(buffer, value)
    end
end
function stream_push_values!(::Type{RemoteFetcher}, T, store_ref::Store_remote, buffer::Blocal, id::UInt) where {Store_remote, Blocal}
    sleep(0.1)
end
