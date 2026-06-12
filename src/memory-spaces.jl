struct CPURAMMemorySpace <: MemorySpace
    owner::Int
end
CPURAMMemorySpace() = CPURAMMemorySpace(myid())
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

### Memory capacity and usage tracking

"""
    memory_capacity(space::MemorySpace) -> UInt64

Return the total memory capacity, in bytes, of `space`. The query runs on the
worker that owns `space` and is cached per-process (capacity is assumed static).

To support a new `MemorySpace`, define [`local_memory_capacity`](@ref), which is
always invoked on the owning worker.
"""
function memory_capacity(space::MemorySpace)
    return get!(MEMORY_CAPACITY_CACHE, space) do
        w = root_worker_id(space)
        if w == myid()
            return local_memory_capacity(space)
        else
            return remotecall_fetch(local_memory_capacity, w, space)
        end
    end::UInt64
end
const MEMORY_CAPACITY_CACHE = Dict{MemorySpace,UInt64}()

"""
    memory_available(space::MemorySpace) -> UInt64

Return the currently-available (free) memory, in bytes, of `space`. The query
runs on the worker that owns `space` and is never cached.

To support a new `MemorySpace`, define [`local_memory_available`](@ref), which is
always invoked on the owning worker.
"""
function memory_available(space::MemorySpace)
    w = root_worker_id(space)
    if w == myid()
        return local_memory_available(space)
    else
        return remotecall_fetch(local_memory_available, w, space)
    end::UInt64
end

"""
    local_memory_capacity(space::MemorySpace) -> UInt64

Worker-local implementation of [`memory_capacity`](@ref). Always called on the
worker that owns `space`. New `MemorySpace` types must define this.
"""
function local_memory_capacity end

"""
    local_memory_available(space::MemorySpace) -> UInt64

Worker-local implementation of [`memory_available`](@ref). Always called on the
worker that owns `space`. New `MemorySpace` types must define this.
"""
function local_memory_available end

# Physical RAM is the natural capacity for CPU spaces. We use the physical
# memory (not the virtual/total) so the budget reflects what can actually be
# resident before the OS starts swapping or the OOM-killer fires.
local_memory_capacity(::CPURAMMemorySpace) =
    UInt64(@static VERSION >= v"1.8-" ? Sys.total_physical_memory() : Sys.total_memory())
local_memory_available(::CPURAMMemorySpace) = UInt64(Sys.free_memory())

"""
    data_size(x) -> UInt64

Best-effort estimate of how many bytes `x` occupies in its memory space. For
`Chunk`s this reads the backing `DRef`/`FileRef` size (exact, free); for raw
data it falls back to `sizeof`/`summarysize`.
"""
data_size(c::Chunk) =
    c.handle isa Union{DRef,FileRef} ? UInt64(c.handle.size) : data_size(poolget(c.handle))
data_size(x::DTask) = data_size(fetch(x; raw=true))
data_size(x::DenseArray) = UInt64(sizeof(x))
data_size(@nospecialize x) = UInt64(Base.summarysize(x))

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

may_alias(::MemorySpace, ::MemorySpace) = false
may_alias(space1::M, space2::M) where M<:MemorySpace = space1 == space2
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

error_unknown_aliasing(T) =
    throw(ConcurrencyViolationError("Cannot resolve aliasing for object of type $T, execution may become sequential"))
error_unknown_aliasing(T, x) =
    throw(ConcurrencyViolationError("Cannot resolve aliasing for object of type $T (element of $(typeof(x))), execution may become sequential"))

struct CombinedAliasing <: AbstractAliasing
    sub_ainfos::Vector{AbstractAliasing}
end
function memory_spans(ca::CombinedAliasing)
    # FIXME: Don't hardcode CPURAMMemorySpace
    if length(ca.sub_ainfos) == 0
        return MemorySpan{CPURAMMemorySpace}[]
    end
    all_spans = memory_spans(ca.sub_ainfos[1])
    for sub_a in ca.sub_ainfos[2:end]
        append!(all_spans, memory_spans(sub_a))
    end
    return all_spans
end
Base.:(==)(ca1::CombinedAliasing, ca2::CombinedAliasing) =
    ca1.sub_ainfos == ca2.sub_ainfos
Base.hash(ca1::CombinedAliasing, h::UInt) =
    hash(ca1.sub_ainfos, hash(CombinedAliasing, h))

struct ObjectAliasing{S<:MemorySpace} <: AbstractAliasing
    ptr::RemotePtr{Cvoid,S}
    sz::UInt
end
ObjectAliasing(ptr::RemotePtr{Cvoid,S}, sz::Integer) where {S<:MemorySpace} =
    ObjectAliasing{S}(ptr, UInt(sz))
function ObjectAliasing(x::T) where T
    @nospecialize x
    ptr = RemotePtr{Cvoid}(pointer_from_objref(x))
    sz = sizeof(T)
    return ObjectAliasing(ptr, sz)
end
function memory_spans(oa::ObjectAliasing{S}) where S
    span = MemorySpan{S}(oa.ptr, oa.sz)
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
        error_unknown_aliasing(T)
        return UnknownAliasing()
    end
end
aliasing(::String) = NoAliasing() # FIXME: Not necessarily true
aliasing(::Symbol) = NoAliasing()
aliasing(::Type) = NoAliasing()

"""
    aliases_as_whole(x) -> Bool

Whether `x` -- either an object, or an already-computed aliasing -- must be
treated as a single, indivisible unit, i.e. it is never safe to alias (or to
consider current) only *part* of it.

For an object: containers whose backing storage may be reallocated or resized on
write (such as `DSparseArray`) return `true`. This causes [`aliasing_root`](@ref)
to resolve any view/wrapper of them to the whole container, so all access funnels
through the container's own `aliasing`. By contract, such a container's `aliasing`
must be a bare [`ObjectAliasing`](@ref).

For an aliasing: a bare `ObjectAliasing` (the aliasing of a whole-object
container) reports `true`. Datadeps uses this to copy such arguments as a whole
(a `FullCopy`) rather than computing per-span remainders, since they can never be
*partially* current in a memory space.
"""
aliases_as_whole(@nospecialize x) = false
aliases_as_whole(ainfo::AliasingWrapper) = aliases_as_whole(ainfo.inner)
aliases_as_whole(::ObjectAliasing) = true

"""
    aliasing_root(x)

Resolve `x` down to the whole-object container it wraps: peel array wrappers
(views, transposes, adjoints, reshapes, permutations, ...) off of `x` until
reaching a container that must alias as a whole (see [`aliases_as_whole`](@ref)),
and return that container. If no such container is wrapped, return `x` unchanged.

This relies solely on the standard `Base.parent` interface, so it transparently
handles *any* array wrapper without per-wrapper `aliasing` methods: a new wrapper
type needs no special-casing here, and a new whole-object container only needs to
define [`aliases_as_whole`](@ref) (plus its own `aliasing` method, which must
return a bare `ObjectAliasing`). A trapping `Base.pointer` on such containers
guards against a wrapper slipping through and being misinterpreted as strided
memory.

!!! warning
    The resolved object's *identity* defines its aliasing, so this is only
    meaningful in the memory space where `x` physically resides. Never return its
    result across a worker boundary and *then* compute `aliasing` -- the transfer
    copies the object and changes its aliasing. Use [`aliasing_unwrapped`](@ref),
    which fuses the two operations so they always run together.
"""
aliasing_root(@nospecialize x) = x
function aliasing_root(x::AbstractArray)
    aliases_as_whole(x) && return x
    p = parent(x)
    # `Base.parent` returns the argument itself for non-wrapper arrays, which
    # terminates the recursion.
    p === x && return x
    root = aliasing_root(p)
    return aliases_as_whole(root) ? root : x
end

"""
    aliasing_unwrapped(x[, dep_mod])

Compute the `aliasing` of `x`, first resolving (via [`aliasing_root`](@ref)) any
whole-object container that `x` wraps. This is the entry point Datadeps uses to
compute the aliasing of a (possibly wrapped) argument.

Unwrapping and `aliasing` are intentionally fused into a single call so they
always execute in the same place. It MUST be evaluated where `x` physically lives
-- e.g. *inside* a `Chunk`'s `remotecall_fetch` block -- because the unwrapped
object's identity determines its aliasing; returning the unwrapped object across
a worker boundary first would copy it and silently change the result.
"""
aliasing_unwrapped(x) = aliasing(aliasing_root(x))
aliasing_unwrapped(x, dep_mod) = aliasing(aliasing_root(x), dep_mod)

# --- Swap-tolerant (synthetic-address) aliasing for swap-managed Chunks ------
#
# The aliasing oracle keys overlaps on raw memory addresses (`pointer(...)`),
# which is exact but assumes a datum never moves. When a `Chunk`'s `DRef` is
# managed by a swap-capable allocator (`MemPool.SimpleRecencyAllocator`), the
# user has explicitly opted into the data being relocated to/from disk, so its
# live pointer is not stable and must not key the oracle. For such chunks we
# rebase the computed spans onto a *synthetic, per-DRef* address range that is
# stable across swaps and disjoint from every other DRef's range (and from real
# heap addresses), so overlap analysis stays correct and stable regardless of
# residency, and the data may be freely swapped between/within tasks (bounding
# RAM) without invalidating the dependency graph. Chunks whose `DRef` is *not*
# swap-managed keep the exact real-pointer analysis unchanged.

# Synthetic addresses sit far above real heap addresses (< ~2^48), so a synthetic
# span never collides with a real one in a space that mixes both; each DRef gets
# a 2^40-byte slot (>> any single allocation), so distinct DRefs never overlap.
const _SYNTH_ALIAS_BASE = UInt(1) << 60
const _SYNTH_ALIAS_STRIDE = UInt(1) << 40
const _synth_alias_lock = Base.Threads.SpinLock()
const _synth_alias_slots = Dict{Int,UInt}()
const _synth_alias_next = Ref{UInt}(0)

# A stable synthetic base address for `ref`, assigned once per DRef id on this
# worker. Different ids get disjoint, non-overlapping ranges.
function _synthetic_base(ref::DRef)
    Base.@lock _synth_alias_lock begin
        return get!(_synth_alias_slots, ref.id) do
            slot = _synth_alias_next[]
            _synth_alias_next[] = slot + 1
            return _SYNTH_ALIAS_BASE + slot * _SYNTH_ALIAS_STRIDE
        end
    end
end

# Whether `ref`'s data is managed by a swap-capable allocator (so its location
# may change and real pointers must not key the oracle). Queried on the owner.
function _swap_managed(ref::DRef)
    ref.owner == myid() || return false
    state = MemPool.with_lock(()->get(MemPool.datastore, ref.id, nothing), MemPool.datastore_lock)
    state === nothing && return false
    return MemPool.storage_read(state).root isa MemPool.SimpleRecencyAllocator
end

# If `x`'s DRef is swap-managed, rebase `ainfo`'s addresses (computed against the
# live backing buffer) onto `x`'s stable synthetic range; otherwise return as-is.
# The rebase anchor is taken from `ainfo` itself (`_ainfo_anchor`), i.e. the base
# of the buffer the spans are built on -- *not* from the wrapper object -- so any
# array wrapper (views, reshapes, ...) is handled uniformly: its aliasing already
# resolves to addresses relative to its parent buffer, and we rebase that parent
# base onto the synthetic range.
function _maybe_synthetic_aliasing(x::Chunk, ainfo)
    (x.handle isa DRef && _swap_managed(x.handle)) || return ainfo
    # No addresses to rebase; the result is residency-independent already.
    (ainfo isa NoAliasing || ainfo isa UnknownAliasing) && return ainfo
    delta = _synthetic_base(x.handle) - _ainfo_anchor(ainfo)
    return _rebase_aliasing(ainfo, delta)
end

function aliasing(x::Chunk, T)
    @assert x.handle isa DRef
    compute = function (x, T)
        obj = unwrap(x)
        root = aliasing_root(obj)
        return _maybe_synthetic_aliasing(x, aliasing(root, T))
    end
    if root_worker_id(x.processor) == myid()
        return compute(x, T)
    end
    return remotecall_fetch(compute, root_worker_id(x.processor), x, T)
end
function aliasing(x::Chunk)
    compute = function (x)
        obj = unwrap(x)
        root = aliasing_root(obj)
        return _maybe_synthetic_aliasing(x, aliasing(root))
    end
    if root_worker_id(x.processor) == myid()
        return compute(x)
    end
    return remotecall_fetch(compute, root_worker_id(x.processor), x)
end
aliasing(x::DTask, T) = aliasing(fetch(x; raw=true), T)
aliasing(x::DTask) = aliasing(fetch(x; raw=true))

function aliasing(x::Base.RefValue{T}) where T
    addr = UInt(Base.pointer_from_objref(x) + fieldoffset(typeof(x), 1))
    ptr = RemotePtr{Cvoid}(addr, CPURAMMemorySpace(myid()))
    ainfo = ObjectAliasing(ptr, sizeof(x))
    if isassigned(x) && type_may_alias(T) && type_may_alias(typeof(x[]))
        return CombinedAliasing([ainfo, aliasing(x[])])
    else
        return CombinedAliasing([ainfo])
    end
end

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
        error_unknown_aliasing(T, x)
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
function aliasing(x::SubArray{T,N}) where {T,N}
    if isbitstype(T)
        p = parent(x)
        space = memory_space(p)
        S = typeof(space)
        parent_ptr = RemotePtr{Cvoid}(UInt64(pointer(p)), space)
        ptr = RemotePtr{Cvoid}(UInt64(pointer(x)), space)
        NA = ndims(p)
        raw_inds = parentindices(x)
        inds = ntuple(i->raw_inds[i] isa Integer ? (raw_inds[i]:raw_inds[i]) : UnitRange(raw_inds[i]), NA)
        sz = ntuple(i->length(inds[i]), NA)
        return StridedAliasing{T,NA,S}(parent_ptr,
                                       ptr,
                                       inds,
                                       sz,
                                       strides(p))
    else
        # FIXME: Also ContiguousAliasing of container
        #return IteratedAliasing(x)
        error_unknown_aliasing(T, x)
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

# --- Synthetic-address rebasing (see swap-tolerant aliasing above) -----------
#
# `_ainfo_anchor(ainfo)` is the reference real address that the synthetic base
# maps to: the base of the single backing buffer `ainfo`'s spans are built on. We
# read it from the computed `ainfo` rather than from the wrapper object, so a
# view/wrapper anchors on its *parent* buffer's base (e.g. a `StridedAliasing`'s
# `base_ptr`) -- the whole point of being able to handle generic array wrappers.
# `_rebase_aliasing(ainfo, delta)` then shifts every address in `ainfo` by
# `delta = synthetic_base - anchor` (a large positive `UInt`, since synthetic
# addresses sit far above real ones), which is valid because MemPool relocates
# the whole buffer as one unit, so every span moves by the same amount and only
# their relative offsets (preserved here) matter. The concrete ainfo type is kept
# so all existing `will_alias`/`memory_spans` dispatch is unchanged.
#
# The anchor is the *minimum* address the ainfo references, so all rebased
# addresses land at or above the synthetic base (within the DRef's slot).
_ainfo_anchor(a::ContiguousAliasing) = a.span.ptr.addr
_ainfo_anchor(a::StridedAliasing) = a.base_ptr.addr
_ainfo_anchor(a::TriangularAliasing) = a.ptr.addr
_ainfo_anchor(a::DiagonalAliasing) = a.ptr.addr
_ainfo_anchor(a::ObjectAliasing) = a.ptr.addr
function _ainfo_anchor(a::CombinedAliasing)
    isempty(a.sub_ainfos) && return UInt(0)
    return minimum(_ainfo_anchor, a.sub_ainfos)
end
_ainfo_anchor(a::AbstractAliasing) =
    throw(ConcurrencyViolationError("Swap-tolerant aliasing not supported for ainfo of type $(typeof(a)); only address-based aliasings (contiguous/strided/triangular/diagonal/object) over a single backing buffer are supported"))

_rebase_aliasing(a::ContiguousAliasing, delta::UInt) =
    ContiguousAliasing(MemorySpan(a.span.ptr + delta, a.span.len))
_rebase_aliasing(a::StridedAliasing{T,N,S}, delta::UInt) where {T,N,S} =
    StridedAliasing{T,N,S}(a.base_ptr + delta, a.ptr + delta, a.base_inds, a.lengths, a.strides)
_rebase_aliasing(a::TriangularAliasing{T,S}, delta::UInt) where {T,S} =
    TriangularAliasing{T,S}(a.ptr + delta, a.stride, a.isupper, a.diagonal)
_rebase_aliasing(a::DiagonalAliasing{T,S}, delta::UInt) where {T,S} =
    DiagonalAliasing{T,S}(a.ptr + delta, a.stride)
_rebase_aliasing(a::ObjectAliasing{S}, delta::UInt) where S =
    ObjectAliasing{S}(a.ptr + delta, a.sz)
_rebase_aliasing(a::CombinedAliasing, delta::UInt) =
    CombinedAliasing(AbstractAliasing[_rebase_aliasing(s, delta) for s in a.sub_ainfos])
_rebase_aliasing(a::NoAliasing, ::UInt) = a
_rebase_aliasing(a::UnknownAliasing, ::UInt) = a
_rebase_aliasing(a::AbstractAliasing, ::UInt) =
    throw(ConcurrencyViolationError("Swap-tolerant aliasing not supported for ainfo of type $(typeof(a))"))

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
    @assert x_span.ptr.space == y_span.ptr.space "Memory spans are in different spaces: $(x_span.ptr.space) vs. $(y_span.ptr.space)"
    x_end = x_span.ptr + x_span.len - 1
    y_end = y_span.ptr + y_span.len - 1
    return x_span.ptr <= y_end && y_span.ptr <= x_end
end

### Unsafe Free

unsafe_free!(x::Chunk) = remotecall_fetch(root_worker_id(x), x) do x
    unsafe_free!(unwrap(x))
    return
end
unsafe_free!(x::DTask) = unsafe_free!(fetch(x; raw=true))
# CPU `Array`s can only be freed if we allocated their backing memory via
# `Libc.malloc` (see `alloc_libc_array`); freeing arbitrary Julia-allocated
# `Array`s is not possible (and would be unsafe). `_libc_finalize!` only frees
# pointers present in our registry, so this is a no-op for plain `Array`s.
unsafe_free!(x::Array) = _libc_finalize!(x)
unsafe_free!(x) = nothing # Do nothing by default
