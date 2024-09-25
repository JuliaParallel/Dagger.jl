struct RemoteChannelFetcher
    chan::RemoteChannel
    RemoteChannelFetcher() = new(RemoteChannel())
end
const _THEIR_TID = TaskLocalValue{Int}(()->0)
function stream_push_values!(fetcher::RemoteChannelFetcher, T, our_store::StreamStore, their_stream::Stream, buffer)
    our_tid = STREAM_THUNK_ID[]
    our_uid = our_store.uid
    their_uid = their_stream.uid
    if _THEIR_TID[] == 0
        _THEIR_TID[] = remotecall_fetch(1) do
            lock(Sch.EAGER_ID_MAP) do id_map
                id_map[their_uid]
            end
        end
    end
    their_tid = _THEIR_TID[]
    @dagdebug our_tid :stream_push "taking output value: $our_tid -> $their_tid"
    value = try
        take!(buffer)
    catch
        close(fetcher.chan)
        rethrow()
    end
    @lock our_store.lock notify(our_store.lock)
    @dagdebug our_tid :stream_push "pushing output value: $our_tid -> $their_tid"
    try
        put!(fetcher.chan, value)
    catch err
        if err isa InvalidStateException && !isopen(fetcher.chan)
            @dagdebug our_tid :stream_push "channel closed: $our_tid -> $their_tid"
            throw(InterruptException())
        end
        rethrow(err)
    end
    @dagdebug our_tid :stream_push "finished pushing output value: $our_tid -> $their_tid"
end
function stream_pull_values!(fetcher::RemoteChannelFetcher, T, our_store::StreamStore, their_stream::Stream, buffer)
    our_tid = STREAM_THUNK_ID[]
    our_uid = our_store.uid
    their_uid = their_stream.uid
    if _THEIR_TID[] == 0
        _THEIR_TID[] = remotecall_fetch(1) do
            lock(Sch.EAGER_ID_MAP) do id_map
                id_map[their_uid]
            end
        end
    end
    their_tid = _THEIR_TID[]
    @dagdebug our_tid :stream_pull "pulling input value: $their_tid -> $our_tid"
    value = try
        take!(fetcher.chan)
    catch err
        if err isa InvalidStateException && !isopen(fetcher.chan)
            @dagdebug our_tid :stream_pull "channel closed: $their_tid -> $our_tid"
            throw(InterruptException())
        end
        rethrow(err)
    end
    @dagdebug our_tid :stream_pull "putting input value: $their_tid -> $our_tid"
    try
        put!(buffer, value)
    catch
        close(fetcher.chan)
        rethrow()
    end
    @lock our_store.lock notify(our_store.lock)
    @dagdebug our_tid :stream_pull "finished putting input value: $their_tid -> $our_tid"
end

struct Protocol
    ip::IPAddr
    port::Integer
end
struct TCP
    protocol::Protocol
    TCP(ip::IPAddr, port::Integer) = new(Protocol(ip,port))
end
struct UDP
    protocol::Protocol
    UDP(ip::IPAddr, port::Integer) = new(Protocol(ip,port))
end

#= FIXME
struct NATS
    protocol::Protocol
    topic::String
    NATS(ip::IPAddr, topic::String) = new(Protocol(ip, 4222), topic)
    NATS(ip::IPAddr, port::Integer, topic::String) = new(Protocol(ip, port), topic)
end
struct MQTT
    protocol::Protocol
    topic::String
    MQTT(ip::IPAddr, topic::String) = new(Protocol(ip, 1883), topic)
    MQTT(ip::IPAddr, port::Integer, topic::String) = new(Protocol(ip, port), topic)
end
struct ZeroMQ
    protocol::Protocol
    ZeroMQ(ip::IPAddr, topic::String) = new(Protocol(ip, 1883), topic)
    ZeroMQ(ip::IPAddr, port::Integer, topic::String) = new(Protocol(ip, port), topic)
end
=#

#= TODO: Remove me
# This is a bad implementation because it wants to sleep on the remote side to
# wait for values, but this isn't semantically valid when done with MemPool.access_ref
struct RemoteFetcher end
function stream_push_values!(::Type{RemoteFetcher}, T, our_store::StreamStore, their_stream::Stream, buffer)
    sleep(1)
end
function stream_pull_values!(::Type{RemoteFetcher}, T, our_store::StreamStore, their_stream::Stream, buffer)
    id = our_store.uid
    thunk_id = STREAM_THUNK_ID[]
    @dagdebug thunk_id :stream "fetching values"

    free_space = capacity(buffer) - length(buffer)
    if free_space == 0
        @dagdebug thunk_id :stream "waiting for drain of full input buffer"
        yield()
        task_may_cancel!()
        wait_for_nonfull_input(our_store, their_stream.uid)
        return
    end

    values = T[]
    while isempty(values)
        values, closed = MemPool.access_ref(their_stream.store_ref.handle, id, T, thunk_id, free_space) do their_store, id, T, thunk_id, free_space
            @dagdebug thunk_id :stream "trying to fetch values at worker $(myid())"
            STREAM_THUNK_ID[] = thunk_id
            values = T[]
            @dagdebug thunk_id :stream "trying to fetch with free_space: $free_space"
            wait_for_nonempty_output(their_store, id)
            if isempty(their_store, id) && !isopen(their_store, id)
                @dagdebug thunk_id :stream "remote stream is closed, returning"
                return values, true
            end
            while !isempty(their_store, id) && length(values) < free_space
                value = take!(their_store, id)::T
                @dagdebug thunk_id :stream "fetched $value"
                push!(values, value)
            end
            return values, false
        end::Tuple{Vector{T},Bool}
        if closed
            throw(InterruptException())
        end

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
=#
