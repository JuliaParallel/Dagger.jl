"""
Filter out traces from the Julia Profile.jl buffer.

Each trace in the buffer has the following structure (all UInt64):
- Stack frames (variable number)
- Thread ID
- Task ID  
- CPU cycle clock
- Thread state (1 = awake, 2 = sleeping)
- Null word
- Null word

The trace ends are marked by two consecutive null words (0x0).
"""
function filter_traces!(f, buffer::Vector{UInt64}, lidata)
    if length(buffer) < 6
        return 0  # Buffer too small to contain even one complete trace
    end

    filtered_count = 0
    i = 1

    while i <= length(buffer)
        # Find the end of the current trace by looking for two consecutive nulls
        trace_start = i
        trace_end = find_trace_end(buffer, i)

        if trace_end == -1
            # No complete trace found from this position
            error("Failed to find trace end for $i")
            break
        end

        # Extract trace metadata (last 6 elements before the two nulls)
        if trace_end - trace_start < 5
            # Trace too short to have proper metadata
            i = trace_end + 1
            continue
        end

        # Check if the trace should be filtered
        do_filter = f(buffer, lidata, trace_start, trace_end)::Bool

        # If the trace should be filtered, null out the entire trace
        if do_filter
            for j in trace_start:trace_end
                buffer[j] = 0x0
            end
            filtered_count += 1
        end

        # Move to the next trace
        i = trace_end + 1
    end

    return filtered_count
end
"""
Find the end of a trace starting from start_idx.
Returns the index of the second null word, or -1 if not found.
"""
function find_trace_end(buffer::Vector{UInt64}, start_idx::Int)
    i = start_idx
    while i < length(buffer)
        if buffer[i] == 0x0 && buffer[i + 1] == 0x0
            return i + 1  # Return index of second null
        end
        i += 1
    end

    return -1  # No complete trace found
end

"Count total number of trace entries in the buffer."
function count_traces(buffer::Vector{UInt64})
    count = 0

    filter_traces!(buffer, lidata) do buffer, lidata, trace_start, trace_end
        count += 1
        return false
    end

    return count
end

"Parse profile buffer and null out traces from sleeping threads."
function filter_sleeping_traces!(buffer::Vector{UInt64}, lidata)
    return filter_traces!(buffer, lidata) do buffer, lidata, trace_start, trace_end
        # The structure before the two nulls is:
        # [...stack frames...][thread_id][task_id][cpu_cycles][thread_state][null][null]
        thread_state_idx = trace_end - 2  # thread_state is 4th from end (before 2 nulls + 1 other field)
        thread_state = buffer[thread_state_idx]
        return thread_state == 2
    end
end

"Parse profile buffer and null out traces without calls to a slowlock path."
function filter_for_slowlock_traces!(buffer::Vector{UInt64}, lidata)
    return filter_traces!(buffer, lidata) do buffer, lidata, trace_start, trace_end
        # Find slowlock frames
        slowlock = false
        frames_end = trace_end - 6
        for j in trace_start:frames_end
            slowlock && break
            ptr = buffer[j]
            for frame in lidata[ptr]
                if occursin("slowlock", string(frame))
                    slowlock = true
                    break
                end
            end
        end
        return !slowlock
    end
end

"Parse profile buffer and keep only traces from a specific thread."
function filter_for_thread!(buffer::Vector{UInt64}, lidata, thread)
    return filter_traces!(buffer, lidata) do buffer, lidata, trace_start, trace_end
        # The structure before the two nulls is:
        # [...stack frames...][thread_id][task_id][cpu_cycles][thread_state][null][null]
        thread_id_idx = trace_end - 5  # thread_id is 5th from end (before 2 nulls + 1 other field)
        thread_id = buffer[thread_id_idx]
        return thread_id+1 == thread
    end
end

"""
Filters out traces from the Julia Profile.jl buffer. Performs:
- Removal of sleeping thread traces
- If slocklock is true, also remove traces that do not call into a slowlock path

Args:
    buffer: Vector{UInt64} containing profile trace data
    
Returns:
    (filtered_count, total_traces) tuple
"""
function filter_traces_multi!(buffer::Vector{UInt64}, lidata;
                              slocklock::Bool=false, thread=nothing)
    total_traces = count_traces(buffer)
    sleeping_count = filter_sleeping_traces!(buffer, lidata)
    if slocklock
        slowlock_count = filter_for_slowlock_traces!(buffer, lidata)
    else
        slowlock_count = 0
    end
    if thread !== nothing
        thread_count = filter_for_thread!(buffer, lidata, thread)
    else
        thread_count = 0
    end

    #= Find the last double-zero in the buffer and truncate the buffer there
    last_zero = 1
    idx = 1
    while idx < length(buffer)
        if buffer[idx] == 0x0 && buffer[idx + 1] == 0x0
            last_zero = idx
            break
        end
        idx += 1
    end
    deleteat!(buffer, last_zero:length(buffer))
    =#

    return (;total_traces, sleeping_count, slowlock_count, thread_count)
end