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
        # N.B. We don't close the buffer to allow for eventual reconnection
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
        # N.B. We don't close the buffer to allow for eventual reconnection
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
