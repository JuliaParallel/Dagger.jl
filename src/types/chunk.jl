"""
    Chunk

A reference to a piece of data located on a remote worker. `Chunk`s are
typically created with `Dagger.tochunk(data)`, and the data can then be
accessed from any worker with `collect(::Chunk)`. `Chunk`s are
serialization-safe, and use distributed refcounting (provided by
`MemPool.DRef`) to ensure that the data referenced by a `Chunk` won't be GC'd,
as long as a reference exists on some worker.

Each `Chunk` is associated with a given `Dagger.Processor`, which is (in a
sense) the processor that "owns" or contains the data. Calling
`collect(::Chunk)` will perform data movement and conversions defined by that
processor to safely serialize the data to the calling worker.

Each `Chunk` also records the `Dagger.MemorySpace` (the `space` field) that the
data resides in — for example `CPURAMMemorySpace` for host memory, a GPU memory
space for device arrays, or `MPIMemorySpace` under `MPIAcceleration`. The memory
space is set from the `space` argument to [`tochunk`](@ref) (or derived from the
current acceleration when omitted), and is used by datadeps for aliasing and
data-movement decisions, so it must reflect where the data actually lives.

## Constructors
See [`tochunk`](@ref).
"""
mutable struct Chunk{T, H, P<:Processor, S<:AbstractScope, M<:MemorySpace}
    chunktype::Type{T}
    domain
    handle::H
    processor::P
    scope::S
    space::M
end
