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
"""
Number of `yield()`s a blocked `put!`/`take!` spends before it parks, and how
long it parks for. The spin keeps the common hand-off cheap (the peer is
usually one yield away); the park is what makes progress *guaranteed*.
"""
const RINGBUFFER_SPINS = 100
const RINGBUFFER_PARK_SECONDS = 0.001

"""
    ringbuffer_wait!(spins::Int) -> Int

Wait for the peer to free a slot or supply a value, without monopolising the
thread. Returns the updated spin count.

A bare `yield()` loop is *not* safe here. Julia permanently marks a task
`sticky` the moment it schedules any sticky task — an `@async`, which the
streaming transport does — and says so itself in `Base.enq_work`: "XXX: Ideally
we would be able to unset this". A sticky task that yields re-enqueues itself
into its *thread-local* workqueue, and `trypoptask` drains that queue before it
ever consults the multiqueue where `Threads.@spawn`ed tasks live. So a sticky
task spinning on `yield()` stops its thread from ever picking up a spawned task
— and once every default thread is spinning that way, the drain tasks a stream
spawns to actually move its values never start at all. That is a permanent
deadlock, not a slowdown: observed as a hard hang of the fan-out topologies on
4-thread CI, with the drain tasks reporting `started=false` after two minutes.

`sleep` genuinely deschedules the task, so the thread's local queue empties and
the spawned tasks get to run. Cancellation stays poll-based (each iteration
still re-checks `isopen` and `task_may_cancel!`), just at park granularity.
"""
@inline function ringbuffer_wait!(spins::Int)
    if spins < RINGBUFFER_SPINS
        yield()
    else
        sleep(RINGBUFFER_PARK_SECONDS)
    end
    return spins + 1
end

function Base.put!(rb::ProcessRingBuffer{T}, x) where T
    spins = 0
    while isfull(rb)
        spins = ringbuffer_wait!(spins)
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
    spins = 0
    while isempty(rb)
        spins = ringbuffer_wait!(spins)
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
