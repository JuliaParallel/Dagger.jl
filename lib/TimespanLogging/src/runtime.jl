# A zeroed-looking GC_Num captured at init so unsampled events still have a
# valid field without calling `gc_num()` on the emit path.
const GC_PLACEHOLDER = Ref{Base.GC_Num}()

"""
    LegacyEvent

Record produced by the `timespan_start(ctx, ::Symbol, ...)` compatibility
API. Ids and timelines stay `Any` (call sites still build NamedTuples);
the win on this path is lock-free TLS storage and deferred consumers.
"""
struct LegacyEvent
    phase::UInt8
    timestamp::UInt64
    category::Symbol
    id::Any
    timeline::Any
    gc_num::Base.GC_Num
end

mutable struct ThreadState
    const tid::Int
    const legacy::ChunkList{LegacyEvent}
    # One ChunkList per registered category, created lazily.
    const typed::Vector{Any}
end

function ThreadState(tid::Int)
    return ThreadState(tid, ChunkList{LegacyEvent}(), Any[nothing for _ in 1:64])
end

const THREAD_STATES_LOCK = Threads.SpinLock()
const THREAD_STATES = Ref{Vector{Union{ThreadState,Nothing}}}(Union{ThreadState,Nothing}[nothing])

# Bitset of enabled typed categories. `typemax(UInt64)` enables all.
# `0` means "no category filter": emit is gated only by the call-site sink.
const ENABLED_BITS = Threads.Atomic{UInt64}(0)
const CAPTURE_GC = Threads.Atomic{Bool}(false)

# Consumers / aggregators installed by `enable!` for `ActiveLog`.
const INSTALLED_CONSUMERS = Ref{Dict{Symbol,Any}}(Dict{Symbol,Any}())
const INSTALLED_AGGREGATORS = Ref{Dict{Symbol,Any}}(Dict{Symbol,Any}())

function _ensure_thread_states!(tid::Int)
    states = THREAD_STATES[]
    if tid <= length(states)
        return states
    end
    @lock THREAD_STATES_LOCK begin
        states = THREAD_STATES[]
        if tid > length(states)
            newlen = max(tid, Threads.maxthreadid())
            newv = Vector{Union{ThreadState,Nothing}}(nothing, newlen)
            copyto!(newv, states)
            THREAD_STATES[] = newv
            states = newv
        end
    end
    return states
end

@inline function thread_state()
    tid = Threads.threadid()
    states = _ensure_thread_states!(tid)
    s = @inbounds states[tid]
    if s === nothing
        s = ThreadState(tid)
        @inbounds states[tid] = s
    end
    return s::ThreadState
end

function typed_buffer(::Type{C}, st::ThreadState) where C <: LogCategory
    id = Int(category_id(C)) + 1
    typed = st.typed
    buf = typed[id]
    if buf === nothing
        E = event_type(C)
        buf = ChunkList{E}(max_chunks(C))
        typed[id] = buf
    end
    return buf::ChunkList{event_type(C)}
end

"""
    enable!(; categories=nothing, capture_gc=false, consumers=..., aggregators=...)

Install the process-local runtime. `categories` is `nothing` (all bits) or
an iterator of `LogCategory` types. Broadcasts to `workers()` when called
from worker 1.
"""
function enable!(; categories=nothing,
                  capture_gc::Bool=false,
                  consumers::Dict{Symbol,Any}=Dict{Symbol,Any}(),
                  aggregators::Dict{Symbol,Any}=Dict{Symbol,Any}())
    bits = UInt64(0)
    if categories === nothing
        bits = typemax(UInt64)
    else
        for C in categories
            bits |= UInt64(1) << category_id(C)
        end
    end
    Threads.atomic_xchg!(ENABLED_BITS, bits)
    Threads.atomic_xchg!(CAPTURE_GC, capture_gc)
    INSTALLED_CONSUMERS[] = consumers
    INSTALLED_AGGREGATORS[] = aggregators
    return nothing
end

function disable!()
    Threads.atomic_xchg!(ENABLED_BITS, UInt64(0))
    Threads.atomic_xchg!(CAPTURE_GC, false)
    INSTALLED_CONSUMERS[] = Dict{Symbol,Any}()
    INSTALLED_AGGREGATORS[] = Dict{Symbol,Any}()
    return nothing
end

"""
    reset!()

Disable logging and drop every thread buffer. For tests.
"""
function reset!()
    disable!()
    @lock THREAD_STATES_LOCK begin
        states = THREAD_STATES[]
        for i in 1:length(states)
            states[i] = nothing
        end
    end
    return nothing
end

@inline category_enabled(::Type{C}) where C <: LogCategory = begin
    bits = ENABLED_BITS[]
    bits == 0 && return false
    return (bits & (UInt64(1) << category_id(C))) != 0
end

@inline logging_enabled() = ENABLED_BITS[] != 0

@inline function logging_enabled(ctx)
    sink = log_sink(ctx)
    return !(sink isa NoOpLog)
end

@inline function logging_enabled(ctx, ::Type{C}) where C <: LogCategory
    log_sink(ctx) isa NoOpLog && return false
    bits = ENABLED_BITS[]
    # No filter installed: any non-NoOp sink records every category.
    bits == 0 && return true
    return (bits & (UInt64(1) << category_id(C))) != 0
end

log_sink(ctx) = NoOpLog()
profile(ctx, category, id, tl) = false
