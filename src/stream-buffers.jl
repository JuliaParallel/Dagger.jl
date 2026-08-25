"A process-local ring buffer."
mutable struct ProcessRingBuffer{T}
    read_idx::Int
    write_idx::Int
    @atomic count::Int
    buffer::Vector{T}
    @atomic open::Bool
    function ProcessRingBuffer{T}(len::Int=1024) where T
        buffer = Vector{T}(undef, len)
        return new{T}(1, 1, 0, buffer, true)
    end
end
Base.isempty(rb::ProcessRingBuffer) = (@atomic rb.count) == 0
isfull(rb::ProcessRingBuffer) = (@atomic rb.count) == length(rb.buffer)
capacity(rb::ProcessRingBuffer) = length(rb.buffer)
Base.length(rb::ProcessRingBuffer) = @atomic rb.count
Base.isopen(rb::ProcessRingBuffer) = @atomic rb.open
function Base.close(rb::ProcessRingBuffer)
    @atomic rb.open = false
end
function Base.put!(rb::ProcessRingBuffer{T}, x) where T
    while isfull(rb)
        yield()
        if !isopen(rb)
            throw(InvalidStateException("ProcessRingBuffer is closed", :closed))
        end
        task_may_cancel!(; must_force=true)
    end
    to_write_idx = mod1(rb.write_idx, length(rb.buffer))
    rb.buffer[to_write_idx] = convert(T, x)
    rb.write_idx += 1
    # Publish the slot only once it holds the value: `count` is the sole
    # handshake with the consumer, and a consumer that sees the increment is
    # entitled to read this slot immediately.
    @atomic rb.count += 1
end
function Base.take!(rb::ProcessRingBuffer)
    while isempty(rb)
        yield()
        if !isopen(rb) && isempty(rb)
            throw(InvalidStateException("ProcessRingBuffer is closed", :closed))
        end
        if task_cancelled() && isempty(rb)
            # We respect a graceful cancellation only if the buffer is empty.
            # Otherwise, we may have values to continue communicating.
            task_may_cancel!()
        end
        task_may_cancel!(; must_force=true)
    end
    to_read_idx = mod1(rb.read_idx, length(rb.buffer))
    value = rb.buffer[to_read_idx]
    rb.read_idx += 1
    # Release the slot only once the value is safely in hand: the producer
    # treats a decremented `count` as free space, and when the buffer was full
    # the slot it writes next is exactly this one.
    @atomic rb.count -= 1
    return value
end

"""
`take!()` all the elements from a buffer and put them in a `Vector`.
"""
function collect!(rb::ProcessRingBuffer{T}) where T
    # Snapshot the count once: a concurrent producer can only grow it, and
    # re-reading it per iteration could ask for more values than are available
    # (blocking in `take!`) or silently skip values that arrived meanwhile.
    n = length(rb)
    output = Vector{T}(undef, n)
    for i in 1:n
        output[i] = take!(rb)
    end

    return output
end
