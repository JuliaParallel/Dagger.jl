"""
A buffer that drops all elements put into it. Only to be used as the output
buffer for a task - will throw if attached as an input.
"""
struct DropBuffer{T} end
DropBuffer{T}(_) where T = DropBuffer{T}()
Base.isempty(::DropBuffer) = true
isfull(::DropBuffer) = false
Base.put!(::DropBuffer, _) = nothing
Base.take!(::DropBuffer) = error("Cannot `take!` from a DropBuffer")

"A process-local buffer backed by a `Channel{T}`."
struct ChannelBuffer{T}
    channel::Channel{T}
    len::Int
    count::Threads.Atomic{Int}
    ChannelBuffer{T}(len::Int=1024) where T =
        new{T}(Channel{T}(len), len, Threads.Atomic{Int}(0))
end
Base.isempty(cb::ChannelBuffer) = isempty(cb.channel)
isfull(cb::ChannelBuffer) = cb.count[] == cb.len
function Base.put!(cb::ChannelBuffer{T}, x) where T
    put!(cb.channel, convert(T, x))
    Threads.atomic_add!(cb.count, 1)
end
function Base.take!(cb::ChannelBuffer)
    take!(cb.channel)
    Threads.atomic_sub!(cb.count, 1)
end

"A cross-worker buffer backed by a `RemoteChannel{T}`."
struct RemoteChannelBuffer{T}
    channel::RemoteChannel{Channel{T}}
    len::Int
    count::Threads.Atomic{Int}
    RemoteChannelBuffer{T}(len::Int=1024) where T =
        new{T}(RemoteChannel(()->Channel{T}(len)), len, Threads.Atomic{Int}(0))
end
Base.isempty(cb::RemoteChannelBuffer) = isempty(cb.channel)
isfull(cb::RemoteChannelBuffer) = cb.count[] == cb.len
function Base.put!(cb::RemoteChannelBuffer{T}, x) where T
    put!(cb.channel, convert(T, x))
    Threads.atomic_add!(cb.count, 1)
end
function Base.take!(cb::RemoteChannelBuffer)
    take!(cb.channel)
    Threads.atomic_sub!(cb.count, 1)
end

"A process-local ring buffer."
mutable struct ProcessRingBuffer{T}
    read_idx::Int
    write_idx::Int
    @atomic count::Int
    buffer::Vector{T}
    function ProcessRingBuffer{T}(len::Int=1024) where T
        buffer = Vector{T}(undef, len)
        return new{T}(1, 1, 0, buffer)
    end
end
Base.isempty(rb::ProcessRingBuffer) = (@atomic rb.count) == 0
isfull(rb::ProcessRingBuffer) = (@atomic rb.count) == length(rb.buffer)
Base.length(rb::ProcessRingBuffer) = @atomic rb.count
function Base.put!(rb::ProcessRingBuffer{T}, x) where T
    len = length(rb.buffer)
    while (@atomic rb.count) == len
        yield()
        task_may_cancel!()
    end
    to_write_idx = mod1(rb.write_idx, len)
    rb.buffer[to_write_idx] = convert(T, x)
    rb.write_idx += 1
    @atomic rb.count += 1
end
function Base.take!(rb::ProcessRingBuffer)
    while (@atomic rb.count) == 0
        yield()
        task_may_cancel!()
    end
    to_read_idx = rb.read_idx
    rb.read_idx += 1
    @atomic rb.count -= 1
    to_read_idx = mod1(to_read_idx, length(rb.buffer))
    return rb.buffer[to_read_idx]
end

"""
`take!()` all the elements from a buffer and put them in a `Vector`.
"""
function collect!(rb::ProcessRingBuffer{T}) where T
    output = Vector{T}(undef, rb.count)
    for i in 1:rb.count
        output[i] = take!(rb)
    end

    return output
end

#= TODO
"A server-local ring buffer backed by shared-memory."
mutable struct ServerRingBuffer{T}
    read_idx::Int
    write_idx::Int
    @atomic count::Int
    buffer::Vector{T}
    function ServerRingBuffer{T}(len::Int=1024) where T
        buffer = Vector{T}(undef, len)
        return new{T}(1, 1, 0, buffer)
    end
end
Base.isempty(rb::ServerRingBuffer) = (@atomic rb.count) == 0
function Base.put!(rb::ServerRingBuffer{T}, x) where T
    len = length(rb.buffer)
    while (@atomic rb.count) == len
        yield()
    end
    to_write_idx = mod1(rb.write_idx, len)
    rb.buffer[to_write_idx] = convert(T, x)
    rb.write_idx += 1
    @atomic rb.count += 1
end
function Base.take!(rb::ServerRingBuffer)
    while (@atomic rb.count) == 0
        yield()
    end
    to_read_idx = rb.read_idx
    rb.read_idx += 1
    @atomic rb.count -= 1
    to_read_idx = mod1(to_read_idx, length(rb.buffer))
    return rb.buffer[to_read_idx]
end
=#

#=
"A TCP-based ring buffer."
mutable struct TCPRingBuffer{T}
    read_idx::Int
    write_idx::Int
    @atomic count::Int
    buffer::Vector{T}
    function TCPRingBuffer{T}(len::Int=1024) where T
        buffer = Vector{T}(undef, len)
        return new{T}(1, 1, 0, buffer)
    end
end
Base.isempty(rb::TCPRingBuffer) = (@atomic rb.count) == 0
function Base.put!(rb::TCPRingBuffer{T}, x) where T
    len = length(rb.buffer)
    while (@atomic rb.count) == len
        yield()
    end
    to_write_idx = mod1(rb.write_idx, len)
    rb.buffer[to_write_idx] = convert(T, x)
    rb.write_idx += 1
    @atomic rb.count += 1
end
function Base.take!(rb::TCPRingBuffer)
    while (@atomic rb.count) == 0
        yield()
    end
    to_read_idx = rb.read_idx
    rb.read_idx += 1
    @atomic rb.count -= 1
    to_read_idx = mod1(to_read_idx, length(rb.buffer))
    return rb.buffer[to_read_idx]
end
=#

#=
"""
A flexible puller which switches to the most efficient buffer type based
on the sender and receiver locations.
"""
mutable struct UniBuffer{T}
    buffer::Union{ProcessRingBuffer{T}, Nothing}
end
function initialize_stream_buffer!(::Type{UniBuffer{T}}, T, send_proc, recv_proc, buffer_amount) where T
    if buffer_amount == 0
        error("Return NullBuffer")
    end
    send_osproc = get_parent(send_proc)
    recv_osproc = get_parent(recv_proc)
    if send_osproc.pid == recv_osproc.pid
        inner = RingBuffer{T}(buffer_amount)
    elseif system_uuid(send_osproc.pid) == system_uuid(recv_osproc.pid)
        inner = ProcessBuffer{T}(buffer_amount)
    else
        inner = RemoteBuffer{T}(buffer_amount)
    end
    return UniBuffer{T}(buffer_amount)
end

struct LocalPuller{T,B}
    buffer::B{T}
    id::UInt
    function LocalPuller{T,B}(id::UInt, buffer_amount::Integer) where {T,B}
        buffer = initialize_stream_buffer!(B, T, buffer_amount)
        return new{T,B}(buffer, id)
    end
end
function Base.take!(pull::LocalPuller{T,B}) where {T,B}
    if pull.buffer === nothing
        pull.buffer = 
        error("Return NullBuffer")
    end
    value = take!(pull.buffer)
end
function initialize_input_stream!(stream::Stream{T,B}, id::UInt, send_proc::Processor, recv_proc::Processor, buffer_amount::Integer) where {T,B}
    local_buffer = remotecall_fetch(stream.ref.handle.owner, stream.ref.handle, id) do ref, id
        local_buffer, remote_buffer = initialize_stream_buffer!(B, T, send_proc, recv_proc, buffer_amount)
        ref.buffers[id] = remote_buffer
        return local_buffer
    end
    stream.buffer = local_buffer
    return stream
end
=#
