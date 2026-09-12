module TimespanLogging

export enable!, disable!, reset!, steal_typed, steal_legacy, steal_all_old_events
export EventRecord, LegacyEvent, LogCategory, category_id, category_symbol, event_type
export CHUNK_CAPACITY, MAX_CHUNKS, max_chunks, NoOpLog, ActiveLog, LocalEventLog, MultiEventLog

include("types.jl")
include("category.jl")
include("buffer.jl")
include("runtime.jl")
include("emit.jl")
include("collect.jl")
include("compat.jl")
include("extras.jl")

function __init__()
    GC_PLACEHOLDER[] = Base.gc_num()
    n = max(Threads.maxthreadid(), 1)
    THREAD_STATES[] = Vector{Union{ThreadState,Nothing}}(nothing, n)
    return nothing
end

end # module
