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
move!(dep_mod, to_space::MemorySpace, from_space::MemorySpace, to::T, from::F) where {T,F} =
    throw(ArgumentError("No `move!` implementation defined for $F -> $T"))
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
function move!(dep_mod, to_space::MemorySpace, from_space::MemorySpace, to::Base.RefValue{T}, from::Base.RefValue{T}) where {T}
    to[] = from[]
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

type_may_alias(::Type{String}) = false
type_may_alias(::Type{Symbol}) = false
type_may_alias(::Type{<:Type}) = false
type_may_alias(::Type{C}) where C<:Chunk{T} where T = type_may_alias(T)
function type_may_alias(::Type{T}) where T
    if isbitstype(T)
        return false
    elseif ismutabletype(T)
        return true
    elseif isstructtype(T)
        for FT in fieldtypes(T)
            type_may_alias(FT) && return true
        end
    end
    return false
end

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

warn_unknown_aliasing(T) =
    @warn "Cannot resolve aliasing for object of type $T\nExecution may become sequential"

struct CombinedAliasing <: AbstractAliasing
    sub_ainfos::Vector{AbstractAliasing}
end
function memory_spans(ca::CombinedAliasing)
    # FIXME: Don't hardcode CPURAMMemorySpace
    all_spans = MemorySpan{CPURAMMemorySpace}[]
    for sub_a in ca.sub_ainfos
        append!(all_spans, memory_spans(sub_a))
    end
    return all_spans
end
Base.:(==)(ca1::CombinedAliasing, ca2::CombinedAliasing) =
    ca1.sub_ainfos == ca2.sub_ainfos
Base.hash(ca1::CombinedAliasing, h::UInt) =
    hash(ca1.sub_ainfos, hash(CombinedAliasing, h))

struct ObjectAliasing <: AbstractAliasing
    ptr::Ptr{Cvoid}
    sz::UInt
end
function ObjectAliasing(x::T) where T
    @nospecialize x
    ptr = pointer_from_objref(x)
    sz = sizeof(T)
    return ObjectAliasing(ptr, sz)
end
function memory_spans(oa::ObjectAliasing)
    rptr = RemotePtr{Cvoid}(oa.ptr)
    span = MemorySpan{CPURAMMemorySpace}(rptr, oa.sz)
    return [span]
end

aliasing(x, T) = aliasing(T(x))
function aliasing(x::T) where T
    if isbits(x)
        return NoAliasing()
    elseif isstructtype(T)
        as = AbstractAliasing[]
        # If the object itself is mutable, it can alias
        if ismutabletype(T)
            push!(as, ObjectAliasing(x))
        end
        # Check all object fields (recursive)
        for field in fieldnames(T)
            sub_as = aliasing(getfield(x, field))
            if sub_as isa NoAliasing
                continue
            elseif sub_as isa CombinedAliasing
                append!(as, sub_as.sub_ainfos)
            else
                push!(as, sub_as)
            end
        end
        return CombinedAliasing(as)
    else
        warn_unknown_aliasing(T)
        return UnknownAliasing()
    end
end
aliasing(::String) = NoAliasing() # FIXME: Not necessarily true
aliasing(::Symbol) = NoAliasing()
aliasing(::Type) = NoAliasing()
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
        warn_unknown_aliasing(T)
        return UnknownAliasing()
    end
end
aliasing(x::Transpose) = aliasing(parent(x))
aliasing(x::Adjoint) = aliasing(parent(x))

struct StridedAliasing{T,N,S} <: AbstractAliasing
    base_ptr::RemotePtr{Cvoid,S}
    ptr::RemotePtr{Cvoid,S}
    base_inds::NTuple{N,UnitRange{Int}}
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
function aliasing(x::SubArray{T,N,A}) where {T,N,A<:Array}
    if isbitstype(T)
        S = CPURAMMemorySpace
        return StridedAliasing{T,ndims(x),S}(RemotePtr{Cvoid}(pointer(parent(x))),
                                             RemotePtr{Cvoid}(pointer(x)),
                                             parentindices(x),
                                             size(x), strides(parent(x)))
    else
        # FIXME: Also ContiguousAliasing of container
        #return IteratedAliasing(x)
        warn_unknown_aliasing(T)
        return UnknownAliasing()
    end
end
function will_alias(x::StridedAliasing{T,N,S}, y::StridedAliasing{T,N,S}) where {T,N,S}
    if x.base_ptr != y.base_ptr
        # FIXME: Conservatively incorrect via `unsafe_wrap` and friends
        return false
    end

    for dim in 1:N
        if ((x.base_inds[dim].stop) < (y.base_inds[dim].start) || (y.base_inds[dim].stop) < (x.base_inds[dim].start))
            return false
        end
    end

    return true
end
# FIXME: Upgrade Contiguous/StridedAlising to same number of dims

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
