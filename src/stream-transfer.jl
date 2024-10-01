using Sockets

abstract type AbstractNetworkTransfer end

struct RemoteChannelFetcher <: AbstractNetworkTransfer
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
    # addr::Union{IPAddr, String}
    port::Integer
end
struct TCP <: AbstractNetworkTransfer
    task::Union{DTask, Nothing}
    protocol::Protocol
    TCP(task=nothing; ip::IPAddr, port::Integer) = new(task, Protocol(ip,port))
end
struct UDP <: AbstractNetworkTransfer
    task::Union{DTask, Nothing}
    protocol::Protocol
    UDP(task=nothing; ip::IPAddr, port::Integer) = new(task, Protocol(ip,port))
end

struct NATS <: AbstractNetworkTransfer
    task::Union{DTask, Nothing}
    protocol::Protocol
    topic::String
    NATS(task=nothing; ip::IPAddr, port::Integer, topic::String) = new(task, Protocol(ip, port), topic)
end
struct MQTT <: AbstractNetworkTransfer
    task::Union{DTask, Nothing}
    protocol::Protocol
    topic::String
    MQTT(task=nothing; ip::IPAddr, port::Integer, topic::String) = new(task, Protocol(ip, port), topic)
end
struct ZeroMQ <: AbstractNetworkTransfer
    task::Union{DTask, Nothing}
    protocol::Protocol
    topic::String
    ZeroMQ(task=nothing; ip::IPAddr, port::Integer=1883, topic::String) = new(task, Protocol(ip, port), topic)
end

# UDP dispatch
new_fetcher(udp::UDP) = UDP(;ip=udp.protocol.ip, port=udp.protocol.port)
fetcher_task(udp::UDP) = udp.task::DTask
function stream_pull_values!(udp::UDP, T, our_store::StreamStore, their_stream::Stream, buffer)
    thunk_id = STREAM_THUNK_ID[]
    @dagdebug thunk_id :stream "fetching values"

    udpsock = UDPSocket()
    Sockets.bind(udpsock, udp.protocol.ip, udp.protocol.port)

    while true
        free_space = capacity(buffer) - length(buffer)
        if free_space == 0
            yield()
            task_may_cancel!()
            continue
        end

        values = T[]
        sender, data = recvfrom(udpsock)

        if length(data) == 0
            @dagdebug thunk_id :stream "No data received"
            yield()
            task_may_cancel!()
            continue
        end

        values = reinterpret(T, data)
        @dagdebug thunk_id :stream "fetched $(length(values)) values"
        for value in values
            put!(buffer, value)
        end
        println("Received with UDP!")
    end
    close(udpsock)
end
function stream_push_values!(udp::UDP, T, our_store::StreamStore, their_stream::Stream, buffer)
    thunk_id = STREAM_THUNK_ID[]
    @dagdebug thunk_id :stream "pushing values"

    udpsock = UDPSocket()
    while true
        if isempty(buffer)
            yield()
            task_may_cancel!()
            continue
        end
        values = T[]
        value = take!(buffer)::T
        push!(values, value)
        send(udpsock, udp.protocol.ip, udp.protocol.port, values)
        @dagdebug thunk_id :stream "pushed $(length(values)) values"
        println("Sent with UDP!")
    end
    close(udpsock)
end

# TCP dispatch
new_fetcher(tcp::TCP) = TCP(;ip=tcp.protocol.ip, port=tcp.protocol.port)
fetcher_task(tcp::TCP) = tcp.task::DTask
function stream_pull_values!(tcp::TCP, T, our_store::StreamStore, their_stream::Stream, buffer)
    thunk_id = STREAM_THUNK_ID[]
    @dagdebug thunk_id :stream "fetching values"

    connection = nothing
    @label pull_values_TCP
    try
        server = listen(tcp.protocol.ip, tcp.protocol.port)
        connection = accept(server)
    catch e
        if isa(e, Base.IOError)
            println("Failed to connect to $(tcp.protocol.ip):$(tcp.protocol.port) for IOError")
        elseif isa(e, Base.UVError)
            println("Failed to connect to $(tcp.protocol.ip):$(tcp.protocol.port) for UVError")
        end
        connection = nothing
    end

    if connection === nothing
        sleep(1)
        @goto pull_values_TCP
    end

    while true

        free_space = capacity(buffer) - length(buffer)
        if free_space == 0
            yield()
            task_may_cancel!()
            continue
        end

        values = T[]
        data = readavailable(connection)
        values = reinterpret(T, data)
        @dagdebug thunk_id :stream "fetched $(length(values)) values"

        for value in values
            put!(buffer, value)
        end
        println("Received with TCP!")
    end

    close(connection)
end
function stream_push_values!(tcp::TCP, T, our_store::StreamStore, their_stream::Stream, buffer)
    thunk_id = STREAM_THUNK_ID[]
    @dagdebug thunk_id :stream "pushing values"

    connection = nothing
    @label push_values_TCP
    try
        connection = connect(tcp.protocol.ip, tcp.protocol.port)
    catch e
        if isa(e, Base.IOError)
            println("Failed to connect to $(tcp.protocol.ip):$(tcp.protocol.port) for IOError")
        elseif isa(e, Base.UVError)
            println("Failed to connect to $(tcp.protocol.ip):$(tcp.protocol.port) for UVError")
        end
        connection = nothing
    end
    if connection === nothing
        @goto push_values_TCP
    end
    # write(connection, length(values))
    while true
        if isempty(buffer)
            yield()
            task_may_cancel!()
            continue
        end
        values = T[]
        value = take!(buffer)::T
        push!(values, value)
        write(connection, values)
    end
    close(connection)
end

# NATS dispatch
new_fetcher(nats::NATS) = NATS(;ip=nats.protocol.ip, port=nats.protocol.port, topic=nats.topic)
fetcher_task(nats::NATS) = nats.task::DTask
function stream_pull_values!(nats::NATS, T, our_store::StreamStore, their_stream::Stream, buffer)
    nc = nothing
    @label pull_values_NATS
    try
        nc = NATS.connect("nats://$(nats.protocol.ip):$(nats.protocol.port)")
    catch e
        println("Failed connecting to NATS at $(nats.protocol.ip):$(nats.protocol.port).")
        nc = nothing
    end

    if nc === nothing
        sleep(1)
        @goto pull_values_NATS
    end

    function msg_handler(msg, T, buffer)
        # data = Vector{UInt8}(msg.payload)
        iob = IOBuffer()
        # length = read(buf, Int)

        # value_data = read(iob, sizeof(T) * length)
        value_data = read(iob)
        values = reinterpret(T, value_data)

        for value in values
            put!(buffer, value)
        end
    end
    while true
        free_space = capacity(buffer) - length(buffer)
        if free_space == 0
            yield()
            task_may_cancel!()
            continue
        end
        sub = subscribe(msg_handler, nc, nats.topic)
    end
end
function stream_push_values!(nats::NATS, T, our_store::StreamStore, their_stream::Stream, buffer)
    nc = nothing
    @label push_values_NATS
    try
        println("nats://$(nats.protocol.ip):$(nats.protocol.port)")
        nc = NATS.connect("nats://$(nats.protocol.ip):$(nats.protocol.port)")
    catch e
        println("Failed connecting to NATS at $(nats.protocol.ip):$(nats.protocol.port).")
        nc = nothing
    end

    if nc === nothing
        sleep(1)
        @goto push_values_NATS
    end
    while true
        if isempty(buffer)
            yield()
            task_may_cancel!()
            continue
        end
        iob = IOBuffer()
        # write(iob, length(values))
        values = T[]
        value = take!(buffer)::T
        push!(values, value)
        write(iob, values)
        data = String(take!(iob))
        publish(nc, nats.topic, data)
    end
end
# MQTT dispatch
new_fetcher(mqtt::MQTT) = MQTT(;ip=mqtt.protocol.ip, port=mqtt.protocol.port, topic=mqtt.topic)
fetcher_task(mqtt::MQTT) = mqtt.task::DTask
function stream_pull_values!(mqtt::MQTT, T, our_store::StreamStore, their_stream::Stream, buffer)
    client = nothing
    @label pull_values_MQTT
    try
        client = Mosquitto.Client(mqtt.protocol.ip, mqtt.protocol.port)
    catch e
        println("Failed connecting to MQTT Broker at $(mqtt.protocol.ip):$(mqtt.protocol.port).")
        client = nothing
    end
    if client === nothing
        sleep(1)
        @goto pull_values_MQTT
    end
    subscribe(client, mqtt.topic)
    while true
        free_space = capacity(buffer) - length(buffer)
        if free_space == 0
            yield()
            task_may_cancel!()
            continue
        end

        Mosquitto.loop(client; timeout=500, ntimes=10)
        msg_channel = get_messages_channel(client)

        while !isempty(msg_channel)
            msg = take!(msg_channel)
            data = reinterpret(T, msg.payload)
            for value in data
                put!(buffer,data)
            end
        end
    end
end
function stream_push_values!(mqtt::MQTT, T, our_store::StreamStore, their_stream::Stream, buffer)
    client = nothing
    @label push_values_MQTT
    try
        client = Mosquitto.Client(mqtt.protocol.ip, mqtt.protocol.port)
    catch e
        println("Failed connecting to MQTT Broker at $(mqtt.protocol.ip):$(mqtt.protocol.port).")
        client = nothing
    end
    if client === nothing
        sleep(1)
        @goto push_values_MQTT
    end
    while true
        if isempty(buffer)
            yield()
            task_may_cancel!()
            continue
        end
        values = T[]
        value = take!(buffer)::T
        push!(values, value)
        data = reinterpret(UInt8, values)
        publish(client, mqtt.topic, data; retain=true)

    end
end
new_fetcher(zmq::ZeroMQ) = ZeroMQ(;ip=zmq.protocol.ip, port=zmq.protocol.port, topic=zmq.topic)
fetcher_task(zmq::ZeroMQ) = zmq.task::DTask
function stream_pull_values!(zmq::ZeroMQ, T, our_store::StreamStore, their_stream::Stream, buffer)
    socket = Socket(Context(), PUB)
    @label push_values_ZMQ
    try
        bind(socket, "tcp::$(zmq.protocol.ip):$(zmq.protocol.port)")
    catch e
        println("Failed connecting via ZeroMQ to $(zmq.protocol.ip):$(zmq.protocol.port).")
        socket = nothing
    end
    if socket === nothing
        sleep(5)
        @goto push_values_ZMQ
    end
    while true
        if isempty(buffer)
            yield()
            task_may_cancel!()
            continue
        end
        send(socket, values)
    end
    close(socket)
end
function stream_push_values!(zmq::ZeroMQ, T, our_store::StreamStore, their_stream::Stream, buffer)
    socket = Socket(Context(), REQ)
    @label pull_values_MQTT
    try
        bind(socket, "tcp::$(zmq.protocol.ip):$(zmq.protocol.port)")
    catch e
        println("Failed connecting via ZeroMQ to $(zmq.protocol.ip):$(zmq.protocol.port).")
        socket = nothing
    end
    if socket === nothing
        sleep(5)
        @goto pull_values_MQTT
    end
    values = recv(socket, Vector{T})

    for value in values
        put!(buffer, values)
    end
end

#= TODO: Remove me
# This is a bad implementation because it wants to sleep on the remote side to
# wait for values, but this isn't semantically valid when done with MemPool.access_ref
struct RemoteFetcher <: AbstractNetworkTransfer end
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
