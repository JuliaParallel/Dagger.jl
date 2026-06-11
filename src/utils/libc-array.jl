# Libc-backed `Array` allocations
#
# Memory allocated by Julia's built-in `Array` allocator cannot be explicitly
# freed; we must wait for the GC to reclaim it. This makes `unsafe_free!` a
# no-op for CPU `Array`s, in contrast to GPU arrays (which support eager,
# explicit freeing). To give Datadeps the ability to eagerly free large CPU
# buffers, we allocate the backing memory with `Libc.malloc` and wrap it in an
# `Array` via `unsafe_wrap`. Every pointer allocated this way is recorded in a
# per-process registry, so `unsafe_free!` only ever calls `Libc.free` on memory
# we are certain we own (never on a user's plain `Array`).

const LIBC_ARRAY_LOCK = Threads.SpinLock()
const LIBC_ARRAY_PTRS = Set{Ptr{Cvoid}}()

# Bookkeeping for benchmarking/diagnostics (bytes).
const LIBC_ARRAY_TOTAL_BYTES = Ref{Int}(0)
const LIBC_ARRAY_LIVE_BYTES = Ref{Int}(0)
const LIBC_ARRAY_PEAK_BYTES = Ref{Int}(0)
const LIBC_ARRAY_NUM_ALLOCS = Ref{Int}(0)

function _libc_register!(ptr::Ptr{Cvoid}, nbytes::Integer)
    @lock LIBC_ARRAY_LOCK begin
        push!(LIBC_ARRAY_PTRS, ptr)
        LIBC_ARRAY_TOTAL_BYTES[] += nbytes
        LIBC_ARRAY_NUM_ALLOCS[] += 1
        LIBC_ARRAY_LIVE_BYTES[] += nbytes
        LIBC_ARRAY_PEAK_BYTES[] = max(LIBC_ARRAY_PEAK_BYTES[], LIBC_ARRAY_LIVE_BYTES[])
    end
    return
end

# Returns `true` if `ptr` was registered (and removes it), `false` otherwise.
function _libc_unregister!(ptr::Ptr{Cvoid}, nbytes::Integer)
    @lock LIBC_ARRAY_LOCK begin
        if ptr in LIBC_ARRAY_PTRS
            delete!(LIBC_ARRAY_PTRS, ptr)
            LIBC_ARRAY_LIVE_BYTES[] -= nbytes
            return true
        end
        return false
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
    ptr = Ptr{Cvoid}(pointer(A))
    @lock LIBC_ARRAY_LOCK return ptr in LIBC_ARRAY_PTRS
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
    _libc_register!(ptr, nbytes)
    A = unsafe_wrap(Array{T,N}, Ptr{T}(ptr), dims; own=false)
    finalizer(_libc_finalize!, A)
    return A
end

function _libc_finalize!(A::Array)
    ptr = Ptr{Cvoid}(pointer(A))
    if _libc_unregister!(ptr, sizeof(A))
        Libc.free(ptr)
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
