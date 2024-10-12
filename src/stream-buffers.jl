"""
A buffer that drops all elements put into it.
"""
mutable struct DropBuffer{T}
    open::Bool
    DropBuffer{T}() where T = new{T}(true)
end
DropBuffer{T}(_) where T = DropBuffer{T}()
Base.isempty(::DropBuffer) = true
isfull(::DropBuffer) = false
capacity(::DropBuffer) = typemax(Int)
Base.length(::DropBuffer) = 0
Base.isopen(buf::DropBuffer) = buf.open
function Base.close(buf::DropBuffer)
    buf.open = false
end
function Base.put!(buf::DropBuffer, _)
    if !isopen(buf)
        throw(InvalidStateException("DropBuffer is closed", :closed))
    end
    task_may_cancel!(; must_force=true)
    yield()
    return
end
function Base.take!(buf::DropBuffer)
    while true
        if !isopen(buf)
            throw(InvalidStateException("DropBuffer is closed", :closed))
        end
        task_may_cancel!(; must_force=true)
        yield()
    end
end

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
