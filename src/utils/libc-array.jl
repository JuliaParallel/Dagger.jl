# Libc-backed `Array` allocations
#
# Memory allocated by Julia's built-in `Array` allocator cannot be explicitly
# freed; we must wait for the GC to reclaim it. This makes `unsafe_free!` a
# no-op for CPU `Array`s, in contrast to GPU arrays (which support eager,
# explicit freeing). To give Datadeps the ability to eagerly free large CPU
# buffers, we allocate the backing memory with `Libc.malloc` and wrap it in an
# `Array` via `unsafe_wrap`. Every allocation is recorded in a per-process
# registry keyed by `objectid`, so `unsafe_free!` only ever calls `Libc.free` on
# memory we are certain we own (never on a user's plain `Array`).
#
# The registry must *not* be keyed by the malloc pointer alone: after an eager
# `unsafe_free!`, the Julia `Array` object still holds a dangling data pointer
# and its finalizer may run later. If malloc reuses that address for a newer
# Libc-backed array in the meantime, a pointer-keyed finalizer would steal the
# new registration and free live memory (heap corruption / use-after-free).
# Keying by `objectid` keeps the eager-free and finalizer paths tied to the
# specific `Array` instance.

const LIBC_ARRAY_LOCK = Threads.SpinLock()
# objectid(A) => (ptr, nbytes)
const LIBC_ARRAY_ALLOCS = Dict{UInt,Tuple{Ptr{Cvoid},Int}}()

# Bookkeeping for benchmarking/diagnostics (bytes).
const LIBC_ARRAY_TOTAL_BYTES = Ref{Int}(0)
const LIBC_ARRAY_LIVE_BYTES = Ref{Int}(0)
const LIBC_ARRAY_PEAK_BYTES = Ref{Int}(0)
const LIBC_ARRAY_NUM_ALLOCS = Ref{Int}(0)

function _libc_register!(A::Array, ptr::Ptr{Cvoid}, nbytes::Integer)
    id = objectid(A)
    @lock LIBC_ARRAY_LOCK begin
        @assert !haskey(LIBC_ARRAY_ALLOCS, id) "Array $(id) is already registered as Libc-backed"
        LIBC_ARRAY_ALLOCS[id] = (ptr, Int(nbytes))
        LIBC_ARRAY_TOTAL_BYTES[] += nbytes
        LIBC_ARRAY_NUM_ALLOCS[] += 1
        LIBC_ARRAY_LIVE_BYTES[] += nbytes
        LIBC_ARRAY_PEAK_BYTES[] = max(LIBC_ARRAY_PEAK_BYTES[], LIBC_ARRAY_LIVE_BYTES[])
    end
    return
end

# Returns the registered `(ptr, nbytes)` after removing it, or `nothing`.
function _libc_unregister!(A::Array)
    id = objectid(A)
    @lock LIBC_ARRAY_LOCK begin
        if haskey(LIBC_ARRAY_ALLOCS, id)
            ptr_nbytes = LIBC_ARRAY_ALLOCS[id]
            delete!(LIBC_ARRAY_ALLOCS, id)
            LIBC_ARRAY_LIVE_BYTES[] -= ptr_nbytes[2]
            return ptr_nbytes
        end
        return nothing
    end
end

"""
    libc_array_stats() -> NamedTuple

Return diagnostics about Libc-backed `Array` allocations made by Dagger on the
current process: `total_bytes` (cumulative bytes ever allocated), `live_bytes`
(currently-live bytes), `peak_bytes` (high-water mark of live bytes), and
`num_allocs` (number of allocations).
"""
libc_array_stats() = @lock LIBC_ARRAY_LOCK (;
    total_bytes = LIBC_ARRAY_TOTAL_BYTES[],
    live_bytes = LIBC_ARRAY_LIVE_BYTES[],
    peak_bytes = LIBC_ARRAY_PEAK_BYTES[],
    num_allocs = LIBC_ARRAY_NUM_ALLOCS[],
)

"""
    reset_libc_array_stats!()

Reset the cumulative/peak counters reported by [`libc_array_stats`](@ref).
`live_bytes` is left untouched (it reflects genuinely-live allocations).
"""
function reset_libc_array_stats!()
    @lock LIBC_ARRAY_LOCK begin
        LIBC_ARRAY_TOTAL_BYTES[] = 0
        LIBC_ARRAY_NUM_ALLOCS[] = 0
        LIBC_ARRAY_PEAK_BYTES[] = LIBC_ARRAY_LIVE_BYTES[]
    end
    return
end

"""
    is_libc_allocated(A) -> Bool

Return `true` if `A` is a CPU `Array` whose backing memory was allocated by
Dagger via `Libc.malloc` (and can therefore be eagerly freed by
[`unsafe_free!`](@ref)).
"""
function is_libc_allocated(A::Array)
    id = objectid(A)
    @lock LIBC_ARRAY_LOCK return haskey(LIBC_ARRAY_ALLOCS, id)
end
is_libc_allocated(@nospecialize(x)) = false

"""
    alloc_libc_array(T, dims...) -> Array{T}

Allocate an `Array{T}` whose backing memory comes from `Libc.malloc`, allowing
it to be eagerly freed with [`unsafe_free!`](@ref). `T` must be an `isbits` type,
since `Libc`-managed memory cannot safely hold GC-tracked references. A
finalizer is attached as a safety net, so the memory is still reclaimed if
`unsafe_free!` is never called.
"""
alloc_libc_array(::Type{T}, dims::Integer...) where {T} =
    alloc_libc_array(T, convert(Dims, dims))
function alloc_libc_array(::Type{T}, dims::Dims{N}) where {T,N}
    isbitstype(T) || throw(ArgumentError("alloc_libc_array requires an isbits element type, got $T"))
    nbytes = prod(dims) * sizeof(T)
    if nbytes == 0
        # `Libc.malloc(0)` is implementation-defined; fall back to a normal
        # (empty) Array, which has nothing to free anyway.
        return Array{T,N}(undef, dims)
    end
    ptr = Libc.malloc(nbytes)
    ptr == C_NULL && throw(OutOfMemoryError())
    A = unsafe_wrap(Array{T,N}, Ptr{T}(ptr), dims; own=false)
    _libc_register!(A, ptr, nbytes)
    finalizer(_libc_finalize!, A)
    return A
end

function _libc_finalize!(A::Array)
    ptr_nbytes = _libc_unregister!(A)
    if ptr_nbytes !== nothing
        Libc.free(ptr_nbytes[1])
    end
    return
end

"""
    libc_backed(x)

If `x` is a CPU `Array` with an `isbits` element type that is not already
Libc-backed, return a Libc-backed copy of it (which can later be eagerly freed
via [`unsafe_free!`](@ref)); otherwise return `x` unchanged. Used by Datadeps to
make its internal buffer copies freeable.
"""
function libc_backed(A::Array{T,N}) where {T,N}
    isbitstype(T) || return A
    is_libc_allocated(A) && return A
    B = alloc_libc_array(T, size(A))
    copyto!(B, A)
    return B
end
libc_backed(@nospecialize(x)) = x
