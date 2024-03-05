abstract type MemorySpace end

struct CPURAMMemorySpace <: MemorySpace
    owner::Int
end

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

#= TODO
function move!(to_space::MemorySpace, from_space::MemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    #if to_space.owner == from_space.owner
        copyto!(to, from)
    #else
    #    error("Cross-worker CPU->CPU move! not yet implemented")
    #end
    return
end
=#

may_alias(::MemorySpace, ::MemorySpace) = true
may_alias(space1::CPURAMMemorySpace, space2::CPURAMMemorySpace) = space1.owner == space2.owner

struct MemorySpan
    ptr::Ptr{Cvoid}
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

function aliasing(x, T)
    @warn "Define aliasing for $T($(typeof(x)))" maxlog=1
    return aliasing(T(x))
end
aliasing(x) = isbits(x) ? NoAliasing() : UnknownAliasing()
aliasing(x::Chunk) = aliasing(fetch(x))
aliasing(x::EagerThunk) = aliasing(fetch(x))

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
    ptr::Ptr{Cvoid}
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
        return StridedAliasing{T,ndims(x)}(reinterpret(Ptr{Cvoid}, pointer(x)),
                                           size(x), strides(parent(x)))
    else
        # FIXME: Also ContiguousAliasing of container
        return IteratedAliasing(x)
    end
end

struct TriangularAliasing{T} <: AbstractAliasing
    ptr::Ptr{Cvoid}
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
    ptr::Ptr{Cvoid}
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
