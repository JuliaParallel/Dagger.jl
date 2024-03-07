abstract type MemorySpace end

struct CPURAMMemorySpace <: MemorySpace
    owner::Int
end
root_worker_id(space::CPURAMMemorySpace) = space.owner

memory_space(x) = CPURAMMemorySpace(myid())
function memory_space(x::Chunk)
    proc = processor(x)
    if proc isa OSProc
        # TODO: This should probably be programmable
        return CPURAMMemorySpace(proc.pid)
    else
        return only(memory_spaces(proc))
    end
end
memory_space(x::EagerThunk) =
    memory_space(fetch(x; raw=true))

memory_spaces(::P) where {P<:Processor} =
    throw(ArgumentError("Must define `memory_spaces` for `$P`"))
memory_spaces(proc::ThreadProc) =
    Set([CPURAMMemorySpace(proc.owner)])
processors(::S) where {S<:MemorySpace} =
    throw(ArgumentError("Must define `processors` for `$S`"))
processors(space::CPURAMMemorySpace) =
    Set(proc for proc in get_processors(OSProc(space.owner)) if proc isa ThreadProc)

### In-place Data Movement

function unwrap(x::Chunk)
    @assert root_worker_id(x.processor) == myid()
    MemPool.poolget(x.handle)
end
function move!(dep_mod, to_space::MemorySpace, from_space::MemorySpace, to::Chunk, from::Chunk)
    to_w = root_worker_id(to_space)
    remotecall_wait(to_w, dep_mod, to_space, from_space, to, from) do dep_mod, to_space, from_space, to, from
        to_raw = unwrap(to)
        from_w = root_worker_id(from_space)
        # FIXME: Use dep_mod to fetch with less memory usage
        from_raw = to_w == from_w ? unwrap(from) : remotecall_fetch(unwrap, from_w, from)
        move!(dep_mod, to_space, from_space, to_raw, from_raw)
    end
    return
end
function move!(dep_mod, to_space::MemorySpace, from_space::MemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    move!(to_space, from_space, dep_mod(to), dep_mod(from))
end
function move!(to_space::MemorySpace, from_space::MemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    copyto!(to, from)
    return
end

function move!(::Type{<:Diagonal}, to_space::MemorySpace, from_space::MemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    copyto!(view(to, diagind(to)), view(from, diagind(from)))
    return
end
# FIXME: Bidiagonal (need direction specified in type)
function move!(::Type{<:Tridiagonal}, to_space::MemorySpace, from_space::MemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    copyto!(view(to, diagind(to, -1)), view(from, diagind(from, -1)))
    copyto!(view(to, diagind(to)), view(from, diagind(from)))
    copyto!(view(to, diagind(to, 1)), view(from, diagind(from, 1)))
    return
end

### Aliasing and Memory Spans

may_alias(::MemorySpace, ::MemorySpace) = true
may_alias(space1::CPURAMMemorySpace, space2::CPURAMMemorySpace) = space1.owner == space2.owner

struct RemotePtr{T,S<:MemorySpace} <: Ref{T}
    addr::UInt
    space::S
end
RemotePtr{T}(addr::UInt, space::S) where {T,S} = RemotePtr{T,S}(addr, space)
RemotePtr{T}(ptr::Ptr{V}, space::S) where {T,V,S} = RemotePtr{T,S}(UInt(ptr), space)
RemotePtr{T}(ptr::Ptr{V}) where {T,V} = RemotePtr{T}(UInt(ptr), CPURAMMemorySpace(myid()))
Base.convert(::Type{RemotePtr}, x::Ptr{T}) where T =
    RemotePtr(UInt(x), CPURAMMemorySpace(myid()))
Base.convert(::Type{<:RemotePtr{V}}, x::Ptr{T}) where {V,T} =
    RemotePtr{V}(UInt(x), CPURAMMemorySpace(myid()))
Base.:+(ptr::RemotePtr{T}, offset::Integer) where T = RemotePtr{T}(ptr.addr + offset, ptr.space)
Base.:-(ptr::RemotePtr{T}, offset::Integer) where T = RemotePtr{T}(ptr.addr - offset, ptr.space)
function Base.isless(ptr1::RemotePtr, ptr2::RemotePtr)
    @assert ptr1.space == ptr2.space
    return ptr1.addr < ptr2.addr
end

struct MemorySpan
    ptr::RemotePtr{Cvoid}
    len::UInt
end

abstract type AbstractAliasing end
memory_spans(::T) where T<:AbstractAliasing = throw(ArgumentError("Must define `memory_spans` for `$T`"))
memory_spans(x) = memory_spans(aliasing(x))
memory_spans(x, T) = memory_spans(aliasing(x, T))

struct NoAliasing <: AbstractAliasing end
memory_spans(::NoAliasing) = MemorySpan[]
struct UnknownAliasing <: AbstractAliasing end
memory_spans(::UnknownAliasing) = [MemorySpan(C_NULL, typemax(UInt))]

aliasing(x, T) = aliasing(T(x))
aliasing(x) = isbits(x) ? NoAliasing() : UnknownAliasing()
aliasing(x::Chunk) = remotecall_fetch(root_worker_id(x.processor), x) do x
    aliasing(unwrap(x))
end
aliasing(x::EagerThunk) = aliasing(fetch(x; raw=true))

struct ContiguousAliasing <: AbstractAliasing
    span::MemorySpan
end
memory_spans(a::ContiguousAliasing) = [a.span]
struct IteratedAliasing{T} <: AbstractAliasing
    x::T
end
function aliasing(x::Array{T}) where T
    if isbitstype(T)
        return ContiguousAliasing(MemorySpan(pointer(x), sizeof(T)*length(x)))
    else
        # FIXME: Also ContiguousAliasing of container
        return IteratedAliasing(x)
    end
end
aliasing(x::Array{T,0}) where T = NoAliasing()
aliasing(x::Transpose) = aliasing(parent(x))
aliasing(x::Adjoint) = aliasing(parent(x))

struct StridedAliasing{T,N} <: AbstractAliasing
    ptr::RemotePtr{Cvoid}
    lengths::NTuple{N,Int}
    strides::NTuple{N,Int}
end
function memory_spans(a::StridedAliasing{T,N}) where {T,N}
    spans = MemorySpan[]
    _memory_spans(a, spans, a.ptr, N)
    return spans
end
function _memory_spans(a::StridedAliasing{T}, spans, ptr, dim) where T
    lengths = a.lengths
    strides = a.strides

    if dim > 1
        for i in 1:lengths[dim]
            _memory_spans(a, spans, ptr, dim-1)
            ptr += sizeof(T)*strides[dim]
        end
    else
        push!(spans, MemorySpan(ptr, sizeof(T)*lengths[1]))
        return
    end

    return spans
end
function aliasing(x::SubArray{T}) where T
    if isbitstype(T)
        return StridedAliasing{T,ndims(x)}(RemotePtr{Cvoid}(pointer(x)),
                                           size(x), strides(parent(x)))
    else
        # FIXME: Also ContiguousAliasing of container
        return IteratedAliasing(x)
    end
end

struct TriangularAliasing{T} <: AbstractAliasing
    ptr::RemotePtr{Cvoid}
    stride::Int
    isupper::Bool
    diagonal::Bool
end
function memory_spans(a::TriangularAliasing{T}) where T
    spans = MemorySpan[]
    ptr = a.ptr
    for i in 1:a.stride
        if a.isupper
            span = MemorySpan(ptr, sizeof(T)*(i-(1-a.diagonal)))
        else
            diag_ptr = ptr + sizeof(T)*(i-1+(1-a.diagonal))
            span = MemorySpan(diag_ptr, sizeof(T)*(a.stride-i+1-(1-a.diagonal)))
        end
        if span.len > 0
            push!(spans, span)
        end
        ptr += sizeof(T) * a.stride
    end
    return spans
end
aliasing(x::UpperTriangular{T}) where T =
    TriangularAliasing{T}(pointer(parent(x)), size(parent(x), 1), true, true)
aliasing(x::LowerTriangular{T}) where T =
    TriangularAliasing{T}(pointer(parent(x)), size(parent(x), 1), false, true)
aliasing(x::UnitUpperTriangular{T}) where T =
    TriangularAliasing{T}(pointer(parent(x)), size(parent(x), 1), true, false)
aliasing(x::UnitLowerTriangular{T}) where T =
    TriangularAliasing{T}(pointer(parent(x)), size(parent(x), 1), false, false)

struct DiagonalAliasing{T} <: AbstractAliasing
    ptr::RemotePtr{Cvoid}
    stride::Int
end
function memory_spans(a::DiagonalAliasing{T}) where T
    spans = MemorySpan[]
    ptr = a.ptr
    for i in 1:a.stride
        push!(spans, MemorySpan(ptr, sizeof(T)))
        ptr += sizeof(T) * (a.stride+1)
    end
    return spans
end
aliasing(x::AbstractMatrix{T}, ::Type{Diagonal}) where T =
    DiagonalAliasing{T}(pointer(parent(x)), size(parent(x), 1))
# FIXME: Bidiagonal
# FIXME: Tridiagonal

function will_alias(x, y)
    for x_span in memory_spans(x), y_span in memory_spans(y)
        if will_alias(x_span, y_span)
            return true
        end
    end
    return false
end
function will_alias(x_span::MemorySpan, y_span::MemorySpan)
    may_alias(x_span.ptr.space, y_span.ptr.space) || return false
    # FIXME: Allow pointer conversion instead of just failing
    @assert x_span.ptr.space == y_span.ptr.space
    x_end = x_span.ptr + x_span.len - 1
    y_end = y_span.ptr + y_span.len - 1
    return x_span.ptr <= y_end && y_span.ptr <= x_end
end

function aliases_with(x, others)
    x_space = memory_space(x)
    matched = Any[]
    for other in others
        other_space = memory_space(other)
        if may_alias(x_space, other_space)
            x_aliasing = aliasing(x)
            other_aliasing = aliasing(other)
            if x_aliasing isa NoAliasing || other_aliasing isa NoAliasing
                continue
            end
            if x_aliasing isa UnknownAliasing || other_aliasing isa Unknown_Aliasing
                push!(matched, other)
            end
            if will_alias(aliasing(x), aliasing(other))
                push!(matched, other)
            end
        end
    end
    return matched
end

overlaps_all(ainfo1, ainfo2) =
    overlaps_all(memory_spans(ainfo1), memory_spans(ainfo2))
function overlaps_all(spans1::Vector{MemorySpan}, spans2::Vector{MemorySpan})
    # FIXME: This is the *worst* way to do this
    error("FIXME")
    for span1 in spans1, span2 in spans2
        if !overlaps(span1, span2)
            return false
        end
    end
    return true
end
