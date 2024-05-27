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
memory_space(x::DTask) =
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
        # TODO: Use dep_mod to fetch with less memory usage
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

struct MemorySpan{S}
    ptr::RemotePtr{Cvoid,S}
    len::UInt
end
MemorySpan(ptr::RemotePtr{Cvoid,S}, len::Integer) where S =
    MemorySpan{S}(ptr, UInt(len))

abstract type AbstractAliasing end
memory_spans(::T) where T<:AbstractAliasing = throw(ArgumentError("Must define `memory_spans` for `$T`"))
memory_spans(x) = memory_spans(aliasing(x))
memory_spans(x, T) = memory_spans(aliasing(x, T))

struct NoAliasing <: AbstractAliasing end
memory_spans(::NoAliasing) = MemorySpan{CPURAMMemorySpace}[]
struct UnknownAliasing <: AbstractAliasing end
memory_spans(::UnknownAliasing) = [MemorySpan{CPURAMMemorySpace}(C_NULL, typemax(UInt))]

aliasing(x, T) = aliasing(T(x))
aliasing(x) = isbits(x) ? NoAliasing() : UnknownAliasing()
aliasing(x::Chunk, T) = remotecall_fetch(root_worker_id(x.processor), x, T) do x, T
    aliasing(unwrap(x), T)
end
aliasing(x::Chunk) = remotecall_fetch(root_worker_id(x.processor), x) do x
    aliasing(unwrap(x))
end
aliasing(x::DTask, T) = aliasing(fetch(x; raw=true), T)
aliasing(x::DTask) = aliasing(fetch(x; raw=true))

struct ContiguousAliasing{S} <: AbstractAliasing
    span::MemorySpan{S}
end
memory_spans(a::ContiguousAliasing{S}) where S = MemorySpan{S}[a.span]
struct IteratedAliasing{T} <: AbstractAliasing
    x::T
end
function aliasing(x::Array{T}) where T
    if isbitstype(T)
        S = CPURAMMemorySpace
        return ContiguousAliasing(MemorySpan{S}(pointer(x), sizeof(T)*length(x)))
    else
        # FIXME: Also ContiguousAliasing of container
        #return IteratedAliasing(x)
        return UnknownAliasing()
    end
end
aliasing(x::Transpose) = aliasing(parent(x))
aliasing(x::Adjoint) = aliasing(parent(x))

struct StridedAliasing{T,N,S} <: AbstractAliasing
    ptr::RemotePtr{Cvoid,S}
    lengths::NTuple{N,Int}
    strides::NTuple{N,Int}
end
function memory_spans(a::StridedAliasing{T,N,S}) where {T,N,S}
    spans = MemorySpan{S}[]
    _memory_spans(a, spans, a.ptr, N)
    return spans
end
function _memory_spans(a::StridedAliasing{T,N,S}, spans, ptr, dim) where {T,N,S}
    lengths = a.lengths
    strides = a.strides

    if dim > 1
        for i in 1:lengths[dim]
            _memory_spans(a, spans, ptr, dim-1)
            ptr += sizeof(T)*strides[dim]
        end
    else
        push!(spans, MemorySpan{S}(ptr, sizeof(T)*lengths[1]))
        return
    end

    return spans
end
function aliasing(x::SubArray{T}) where T
    if isbitstype(T)
        S = CPURAMMemorySpace
        return StridedAliasing{T,ndims(x),S}(RemotePtr{Cvoid}(pointer(x)),
                                             size(x), strides(parent(x)))
    else
        # FIXME: Also ContiguousAliasing of container
        #return IteratedAliasing(x)
        return UnknownAliasing()
    end
end
#= TODO: Fix and enable strided aliasing optimization
function will_alias(x::StridedAliasing{T,N,S}, y::StridedAliasing{T,N,S}) where {T,N,S}
    # TODO: Upgrade Contiguous/StridedAlising to same number of dims
    for dim in 1:N
        # FIXME: Adjust ptrs to common base
        x_span = MemorySpan{S}(x.ptr, sizeof(T)*x.strides[dim])
        y_span = MemorySpan{S}(y.ptr, sizeof(T)*y.strides[dim])
        @show dim x_span y_span
        if !will_alias(x_span, y_span)
            return false
        end
    end
    return true
end
=#

struct TriangularAliasing{T,S} <: AbstractAliasing
    ptr::RemotePtr{Cvoid,S}
    stride::Int
    isupper::Bool
    diagonal::Bool
end
function memory_spans(a::TriangularAliasing{T,S}) where {T,S}
    spans = MemorySpan{S}[]
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
    TriangularAliasing{T,CPURAMMemorySpace}(pointer(parent(x)), size(parent(x), 1), true, true)
aliasing(x::LowerTriangular{T}) where T =
    TriangularAliasing{T,CPURAMMemorySpace}(pointer(parent(x)), size(parent(x), 1), false, true)
aliasing(x::UnitUpperTriangular{T}) where T =
    TriangularAliasing{T,CPURAMMemorySpace}(pointer(parent(x)), size(parent(x), 1), true, false)
aliasing(x::UnitLowerTriangular{T}) where T =
    TriangularAliasing{T,CPURAMMemorySpace}(pointer(parent(x)), size(parent(x), 1), false, false)

struct DiagonalAliasing{T,S} <: AbstractAliasing
    ptr::RemotePtr{Cvoid,S}
    stride::Int
end
function memory_spans(a::DiagonalAliasing{T,S}) where {T,S}
    spans = MemorySpan{S}[]
    ptr = a.ptr
    for i in 1:a.stride
        push!(spans, MemorySpan(ptr, sizeof(T)))
        ptr += sizeof(T) * (a.stride+1)
    end
    return spans
end
function aliasing(x::AbstractMatrix{T}, ::Type{Diagonal}) where T
    ptr = reinterpret(Ptr{Cvoid}, pointer(parent(x)))
    S = memory_space(x)
    rptr = RemotePtr{Cvoid}(ptr, S)
    return DiagonalAliasing{T,typeof(S)}(rptr, size(parent(x), 1))
end
# FIXME: Bidiagonal
# FIXME: Tridiagonal

function will_alias(x, y)
    x isa NoAliasing || y isa NoAliasing && return false
    x isa UnknownAliasing || y isa UnknownAliasing && return true
    # FIXME: Support mixed-space span sets (for nested data structures)
    x_spans = memory_spans(x)::Vector{<:MemorySpan}
    y_spans = memory_spans(y)::Vector{<:MemorySpan}
    return will_alias(x_spans, y_spans)
end
function will_alias(x_spans::Vector{MemorySpan{Sx}}, y_spans::Vector{MemorySpan{Sy}}) where {Sx,Sy}
    # Quick check if spaces can alias
    if !isempty(x_spans) && !isempty(y_spans)
        x_space = x_spans[1].ptr.space
        y_space = y_spans[1].ptr.space
        if !may_alias(x_space, y_space)
            return false
        end
    end

    # Check all spans against each other
    for x_span in x_spans, y_span in y_spans
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
