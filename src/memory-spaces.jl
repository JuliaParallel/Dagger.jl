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
    @assert x.handle.owner == myid()
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

abstract type AbstractAliasing end
memory_spans(::T) where T<:AbstractAliasing = throw(ArgumentError("Must define `memory_spans` for `$T`"))
memory_spans(x) = memory_spans(aliasing(x))
memory_spans(x, T) = memory_spans(aliasing(x, T))

### Type-generic aliasing info wrapper

mutable struct AliasingWrapper <: AbstractAliasing
    inner::AbstractAliasing
    hash::UInt64
    AliasingWrapper(inner::AbstractAliasing) = new(inner, hash(inner))
end
memory_spans(x::AliasingWrapper) = memory_spans(x.inner)
equivalent_structure(x::AliasingWrapper, y::AliasingWrapper) =
    x.hash == y.hash || equivalent_structure(x.inner, y.inner)
Base.hash(x::AliasingWrapper, h::UInt64) = hash(x.hash, h)
Base.isequal(x::AliasingWrapper, y::AliasingWrapper) = x.hash == y.hash
Base.:(==)(x::AliasingWrapper, y::AliasingWrapper) = x.hash == y.hash
will_alias(x::AliasingWrapper, y::AliasingWrapper) = will_alias(x.inner, y.inner)

### Small dictionary type

struct SmallDict{K,V} <: AbstractDict{K,V}
    keys::Vector{K}
    vals::Vector{V}
end
SmallDict{K,V}() where {K,V} = SmallDict{K,V}(Vector{K}(), Vector{V}())
function Base.getindex(d::SmallDict{K,V}, key) where {K,V}
    key_idx = findfirst(==(convert(K, key)), d.keys)
    if key_idx === nothing
        throw(KeyError(key))
    end
    return @inbounds d.vals[key_idx]
end
function Base.setindex!(d::SmallDict{K,V}, val, key) where {K,V}
    key_conv = convert(K, key)
    key_idx = findfirst(==(key_conv), d.keys)
    if key_idx === nothing
        push!(d.keys, key_conv)
        push!(d.vals, convert(V, val))
    else
        d.vals[key_idx] = convert(V, val)
    end
    return val
end
Base.haskey(d::SmallDict{K,V}, key) where {K,V} = in(convert(K, key), d.keys)
Base.keys(d::SmallDict) = d.keys
Base.length(d::SmallDict) = length(d.keys)
Base.iterate(d::SmallDict) = iterate(d, 1)
Base.iterate(d::SmallDict, state) = state > length(d.keys) ? nothing : (d.keys[state] => d.vals[state], state+1)

### Type-stable lookup structure for AliasingWrappers

struct AliasingLookup
    # The set of memory spaces that are being tracked
    spaces::Vector{MemorySpace}
    # The set of AliasingWrappers that are being tracked
    # One entry for each AliasingWrapper
    ainfos::Vector{AliasingWrapper}
    # The memory spaces for each AliasingWrapper
    # One entry for each AliasingWrapper
    ainfos_spaces::Vector{Vector{Int}}
    # The spans for each AliasingWrapper in each memory space
    # One entry for each AliasingWrapper
    spans::Vector{SmallDict{Int,Vector{LocalMemorySpan}}}
    # The set of AliasingWrappers that only exist in a single memory space
    # One entry for each AliasingWrapper
    ainfos_only_space::Vector{Int}
    # The bounding span for each AliasingWrapper in each memory space
    # One entry for each AliasingWrapper
    bounding_spans::Vector{SmallDict{Int,LocalMemorySpan}}
    # The interval tree of the bounding spans for each AliasingWrapper
    # One entry for each MemorySpace
    bounding_spans_tree::Vector{IntervalTree{LocatorMemorySpan{Int},UInt64}}

    AliasingLookup() = new(MemorySpace[],
                           AliasingWrapper[],
                           Vector{Int}[],
                           SmallDict{Int,Vector{LocalMemorySpan}}[],
                           Int[],
                           SmallDict{Int,LocalMemorySpan}[],
                           IntervalTree{LocatorMemorySpan{Int},UInt64}[])
end
function Base.push!(lookup::AliasingLookup, ainfo::AliasingWrapper)
    # Update the set of memory spaces and spans,
    # and find the bounding spans for this AliasingWrapper
    spaces_set = Set{MemorySpace}(lookup.spaces)
    self_spaces_set = Set{Int}()
    spans = SmallDict{Int,Vector{LocalMemorySpan}}()
    for span in memory_spans(ainfo)
        space = span.ptr.space
        if !in(space, spaces_set)
            push!(spaces_set, space)
            push!(lookup.spaces, space)
            push!(lookup.bounding_spans_tree, IntervalTree{LocatorMemorySpan{Int}}())
        end
        space_idx = findfirst(==(space), lookup.spaces)
        push!(self_spaces_set, space_idx)
        spans_in_space = get!(Vector{LocalMemorySpan}, spans, space_idx)
        push!(spans_in_space, LocalMemorySpan(span))
    end
    push!(lookup.ainfos_spaces, collect(self_spaces_set))
    push!(lookup.spans, spans)

    # Update the set of AliasingWrappers
    push!(lookup.ainfos, ainfo)
    ainfo_idx = length(lookup.ainfos)

    # Check if the AliasingWrapper only exists in a single memory space
    if length(self_spaces_set) == 1
        space_idx = only(self_spaces_set)
        push!(lookup.ainfos_only_space, space_idx)
    else
        push!(lookup.ainfos_only_space, 0)
    end

    # Add the bounding spans for this AliasingWrapper
    bounding_spans = SmallDict{Int,LocalMemorySpan}()
    for space_idx in keys(spans)
        space_spans = spans[space_idx]
        bound_start = minimum(span_start, space_spans)
        bound_end = maximum(span_end, space_spans)
        bounding_span = LocalMemorySpan(bound_start, bound_end - bound_start)
        bounding_spans[space_idx] = bounding_span
        insert!(lookup.bounding_spans_tree[space_idx], LocatorMemorySpan(bounding_span, ainfo_idx))
    end
    push!(lookup.bounding_spans, bounding_spans)

    return ainfo_idx
end
struct AliasingLookupFinder
    lookup::AliasingLookup
    ainfo::AliasingWrapper
    ainfo_idx::Int
    spaces_idx::Vector{Int}
    to_consider::Vector{Int}
end
Base.eltype(::AliasingLookupFinder) = AliasingWrapper
Base.IteratorSize(::AliasingLookupFinder) = Base.SizeUnknown()
# FIXME: We should use a Dict{UInt,Int} to find the ainfo_idx instead of linear search
function Base.intersect(lookup::AliasingLookup, ainfo::AliasingWrapper; ainfo_idx=nothing)
    if ainfo_idx === nothing
        ainfo_idx = something(findfirst(==(ainfo), lookup.ainfos))
    end
    spaces_idx = lookup.ainfos_spaces[ainfo_idx]
    to_consider_spans = LocatorMemorySpan{Int}[]
    for space_idx in spaces_idx
        bounding_spans_tree = lookup.bounding_spans_tree[space_idx]
        self_bounding_span = LocatorMemorySpan(lookup.bounding_spans[ainfo_idx][space_idx], 0)
        find_overlapping!(bounding_spans_tree, self_bounding_span, to_consider_spans; exact=false)
    end
    to_consider = Int[locator.owner for locator in to_consider_spans]
    @assert all(to_consider .> 0)
    return AliasingLookupFinder(lookup, ainfo, ainfo_idx, spaces_idx, to_consider)
end
Base.iterate(finder::AliasingLookupFinder) = iterate(finder, 1)
function Base.iterate(finder::AliasingLookupFinder, cursor_ainfo_idx)
    ainfo_spaces = nothing
    cursor_space_idx = 1

    # New ainfos enter here
    @label ainfo_restart

    # Check if we've exhausted all ainfos
    if cursor_ainfo_idx > length(finder.to_consider)
        return nothing
    end
    ainfo_idx = finder.to_consider[cursor_ainfo_idx]

    # Find the appropriate memory spaces for this ainfo
    if ainfo_spaces === nothing
        ainfo_spaces = finder.lookup.ainfos_spaces[ainfo_idx]
    end

    # New memory spaces (for the same ainfo) enter here
    @label space_restart

    # Check if we've exhausted all memory spaces for this ainfo, and need to move to the next ainfo
    if cursor_space_idx > length(ainfo_spaces)
        cursor_ainfo_idx += 1
        ainfo_spaces = nothing
        cursor_space_idx = 1
        @goto ainfo_restart
    end

    # Find the currently considered memory space for this ainfo
    space_idx = ainfo_spaces[cursor_space_idx]

    # Check if this memory space is part of our target ainfo's spaces
    if !(space_idx in finder.spaces_idx)
        cursor_space_idx += 1
        @goto space_restart
    end

    # Check if this ainfo's bounding span is part of our target ainfo's bounding span in this memory space
    other_ainfo_bounding_span = finder.lookup.bounding_spans[ainfo_idx][space_idx]
    self_bounding_span = finder.lookup.bounding_spans[finder.ainfo_idx][space_idx]
    if !spans_overlap(other_ainfo_bounding_span, self_bounding_span)
        cursor_space_idx += 1
        @goto space_restart
    end

    # We have a overlapping bounds in the same memory space, so check if the ainfos are aliasing
    # This is the slow path!
    other_ainfo = finder.lookup.ainfos[ainfo_idx]
    aliasing = will_alias(finder.ainfo, other_ainfo)
    if !aliasing
        cursor_ainfo_idx += 1
        ainfo_spaces = nothing
        cursor_space_idx = 1
        @goto ainfo_restart
    end

    # We overlap, so return the ainfo and the next ainfo index
    return other_ainfo, cursor_ainfo_idx+1
end

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

function aliasing(x, dep_mod)
    if dep_mod isa Symbol
        return aliasing(getfield(x, dep_mod))
    end
    return aliasing(dep_mod(x))
end
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
will_alias(x::ContiguousAliasing{S}, y::ContiguousAliasing{S}) where S =
    will_alias(x.span, y.span)
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
                                             size(x), strides(x))
    else
        # FIXME: Also ContiguousAliasing of container
        #return IteratedAliasing(x)
        warn_unknown_aliasing(T)
        return UnknownAliasing()
    end
end
function will_alias(x::StridedAliasing{T1,N1,S1}, y::StridedAliasing{T2,N2,S2}) where {T1,T2,N1,N2,S1,S2}
    # Check if the base pointers are the same
    # FIXME: Conservatively incorrect via `unsafe_wrap` and friends
    if x.base_ptr != y.base_ptr
        return false
    end

    if T1 === T2 && N1 == N2 && may_alias(x.base_ptr.space, y.base_ptr.space)
        # Check if the base indices overlap
        for dim in 1:N1
            if ((x.base_inds[dim].stop) < (y.base_inds[dim].start) || (y.base_inds[dim].stop) < (x.base_inds[dim].start))
                return false
            end
        end
        return true
    else
        return invoke(will_alias, Tuple{Any, Any}, x, y)
    end
end
# TODO: We need to validate that the StridedAliasing is actually a subview of the ContiguousAliasing
will_alias(x::StridedAliasing{T,N,S}, y::ContiguousAliasing{S}) where {T,N,S} =
    x.base_ptr == y.span.ptr
will_alias(x::ContiguousAliasing{S}, y::StridedAliasing{T,N,S}) where {T,N,S} =
    will_alias(y, x)
# FIXME: Upgrade StridedAlisings to same number of dims

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
