### Remote pointer type

struct RemotePtr{T,S<:MemorySpace} <: Ref{T}
    addr::UInt
    space::S
end
RemotePtr{T}(addr::UInt, space::S) where {T,S} = RemotePtr{T,S}(addr, space)
RemotePtr{T}(ptr::Ptr{V}, space::S) where {T,V,S} = RemotePtr{T,S}(UInt(ptr), space)
RemotePtr{T}(ptr::Ptr{V}) where {T,V} = RemotePtr{T}(UInt(ptr), CPURAMMemorySpace(myid()))
# FIXME: Don't hardcode CPURAMMemorySpace
RemotePtr(addr::UInt) = RemotePtr{Cvoid}(addr, CPURAMMemorySpace(myid()))
RemotePtr(addr::UInt, space::S) where {S<:MemorySpace} = RemotePtr{Cvoid,S}(addr, space)
Base.convert(::Type{RemotePtr}, x::Ptr{T}) where T =
    RemotePtr(UInt(x), CPURAMMemorySpace(myid()))
Base.convert(::Type{<:RemotePtr{V}}, x::Ptr{T}) where {V,T} =
    RemotePtr{V}(UInt(x), CPURAMMemorySpace(myid()))
Base.convert(::Type{UInt}, ptr::RemotePtr) = ptr.addr
Base.:+(ptr::RemotePtr{T}, offset::Integer) where T = RemotePtr{T}(ptr.addr + offset, ptr.space)
Base.:-(ptr::RemotePtr{T}, offset::Integer) where T = RemotePtr{T}(ptr.addr - offset, ptr.space)
function Base.isless(ptr1::RemotePtr, ptr2::RemotePtr)
    @assert ptr1.space == ptr2.space
    return ptr1.addr < ptr2.addr
end

### Generic memory spans

struct MemorySpan{S}
    ptr::RemotePtr{Cvoid,S}
    len::UInt
end
MemorySpan(ptr::RemotePtr{Cvoid,S}, len::Integer) where S =
    MemorySpan{S}(ptr, UInt(len))
MemorySpan{S}(addr::UInt, len::Integer) where S =
    MemorySpan{S}(RemotePtr{Cvoid,S}(addr), UInt(len))
Base.isless(a::MemorySpan, b::MemorySpan) = a.ptr < b.ptr
Base.isempty(x::MemorySpan) = x.len == 0
span_start(span::MemorySpan) = span.ptr.addr
span_len(span::MemorySpan) = span.len
span_end(span::MemorySpan) = span.ptr.addr + span.len
spans_overlap(span1::MemorySpan, span2::MemorySpan) =
    span_start(span1) < span_end(span2) && span_start(span2) < span_end(span1)
function span_diff(span1::MemorySpan, span2::MemorySpan)
    @assert span1.ptr.space == span2.ptr.space
    start = max(span_start(span1), span_start(span2))
    stop = min(span_end(span1), span_end(span2))
    start_ptr = RemotePtr(start, span1.ptr.space)
    if start < stop
        len = stop - start
        return MemorySpan(start_ptr, len)
    else
        return MemorySpan(start_ptr, 0)
    end
end

### More space-efficient memory spans

struct LocalMemorySpan
    ptr::UInt
    len::UInt
end
LocalMemorySpan(span::MemorySpan) = LocalMemorySpan(span.ptr.addr, span.len)
Base.isempty(x::LocalMemorySpan) = x.len == 0
span_start(span::LocalMemorySpan) = span.ptr
span_len(span::LocalMemorySpan) = span.len
span_end(span::LocalMemorySpan) = span.ptr + span.len
spans_overlap(span1::LocalMemorySpan, span2::LocalMemorySpan) =
    span_start(span1) < span_end(span2) && span_start(span2) < span_end(span1)
function span_diff(span1::LocalMemorySpan, span2::LocalMemorySpan)
    start = max(span_start(span1), span_start(span2))
    stop = min(span_end(span1), span_end(span2))
    if start < stop
        len = stop - start
        return LocalMemorySpan(start, len)
    else
        return LocalMemorySpan(start, 0)
    end
end

# FIXME: Store the length separately, since it's shared by all spans
struct ManyMemorySpan{N}
    spans::NTuple{N,LocalMemorySpan}
end
Base.isempty(x::ManyMemorySpan) = all(isempty, x.spans)
span_start(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_start(span.spans[i]), N))
span_len(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_len(span.spans[i]), N))
span_end(span::ManyMemorySpan{N}) where N = ManyPair(ntuple(i -> span_end(span.spans[i]), N))
spans_overlap(span1::ManyMemorySpan{N}, span2::ManyMemorySpan{N}) where N =
    # N.B. The spans are assumed to be the same length and relative offset
    spans_overlap(span1.spans[1], span2.spans[1])
function span_diff(span1::ManyMemorySpan{N}, span2::ManyMemorySpan{N}) where N
    verify_span(span1)
    verify_span(span2)
    span = ManyMemorySpan(ntuple(i -> span_diff(span1.spans[i], span2.spans[i]), N))
    matches = ntuple(i->span1.spans[i].ptr == span2.spans[i].ptr, Val(N))
    @assert !(any(matches) && !all(matches)) "Spans only partially match:\n  Span1: $span1\n  Span2: $span2\n  Result: $span\nWhile processing $(typeof(VERIFY_SPAN_CURRENT_OBJECT[]))"
    @assert allequal(span_len, span.spans) "Uneven span_diff result:\n  Span1: $span1\n  Span2: $span2\n  Result: $span\nWhile processing $(typeof(VERIFY_SPAN_CURRENT_OBJECT[]))"
    verify_span(span)
    return span
end
const VERIFY_SPAN_CURRENT_OBJECT = TaskLocalValue{Any}(()->nothing)
function verify_span(span::ManyMemorySpan{N}) where N
    @assert allequal(span_len, span.spans) "All spans must be the same: $(map(span_len, span.spans))\nWhile processing $(typeof(VERIFY_SPAN_CURRENT_OBJECT[]))"
end

struct ManyPair{N} <: Unsigned
    pairs::NTuple{N,UInt}
end
Base.promote_rule(::Type{ManyPair}, ::Type{T}) where {T<:Integer} = ManyPair
Base.convert(::Type{ManyPair{N}}, pair::ManyPair{N}) where N = pair
Base.convert(::Type{ManyPair{N}}, x::T) where {T<:Integer,N} = ManyPair(ntuple(i -> x, N))
Base.convert(::Type{ManyPair}, x::ManyPair) = x
Base.:+(x::ManyPair{N}, y::ManyPair{N}) where N = ManyPair(ntuple(i -> x.pairs[i] + y.pairs[i], N))
Base.:-(x::ManyPair{N}, y::ManyPair{N}) where N = ManyPair(ntuple(i -> x.pairs[i] - y.pairs[i], N))
Base.:-(x::ManyPair) = error("Can't negate a ManyPair")
Base.:(==)(x::ManyPair, y::ManyPair) = x.pairs == y.pairs
Base.isless(x::ManyPair, y::ManyPair) = x.pairs[1] < y.pairs[1]
Base.:(<)(x::ManyPair, y::ManyPair) = x.pairs[1] < y.pairs[1]
Base.string(x::ManyPair) = "ManyPair($(x.pairs))"
Base.show(io::IO, x::ManyPair) = print(io, string(x))

ManyMemorySpan{N}(start::ManyPair{N}, len::ManyPair{N}) where N =
    ManyMemorySpan{N}(ntuple(i -> LocalMemorySpan(start.pairs[i], len.pairs[i]), N))

### Memory spans with ownership info

struct LocatorMemorySpan{T}
    span::LocalMemorySpan
    owner::T
end
LocatorMemorySpan{T}(start::UInt64, len::UInt64) where T = # For interval tree
    LocatorMemorySpan{T}(LocalMemorySpan(start, len), 0)
Base.isempty(x::LocatorMemorySpan) = span_len(x.span) == 0
span_start(x::LocatorMemorySpan) = span_start(x.span)
span_end(x::LocatorMemorySpan) = span_end(x.span)
span_len(x::LocatorMemorySpan) = span_len(x.span)
spans_overlap(span1::LocatorMemorySpan{T}, span2::LocatorMemorySpan{T}) where T =
    spans_overlap(span1.span, span2.span)
function span_diff(span1::LocatorMemorySpan{T}, span2::LocatorMemorySpan{T}) where T
    span = LocatorMemorySpan(span_diff(span1.span, span2.span), 0)
    verify_span(span)
    return span
end
function verify_span(span::LocatorMemorySpan{T}) where T
    verify_span(span.span)
end