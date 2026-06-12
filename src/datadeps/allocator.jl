# Datadeps storage allocator
#
# Datadeps bounds the peak CPU-RAM footprint of its argument copies ("slots") by
# spilling live-but-not-immediately-needed copies to disk and reloading them on
# demand. Rather than serializing to temp files by hand (and freeing buffers
# behind MemPool's back), we route slot copies through a dedicated MemPool
# `StorageDevice` -- `DatadepsDevice` -- so that:
#
#   * spilling/reloading reuses MemPool's storage protocol (and participates in
#     the shared logical-memory accounting, so we never overcommit RAM against
#     other allocators such as `SimpleRecencyAllocator`);
#   * `poolpin`/`poolunpin` integrate naturally: a pinned slot is resident and
#     never spilled, and `poolpin` transparently swaps a spilled slot back in;
#   * unlike `SimpleRecencyAllocator`, the device makes *no* automatic eviction
#     decisions. Datadeps' static Belady planner decides exactly what to spill
#     and when (via [`datadeps_spill!`](@ref)), so MemPool's own recency
#     allocator can never relocate a live buffer behind the aliasing oracle.
#
# In-memory data is held as a Libc-backed `Array` (see `libc-array.jl`) so that a
# spill can *eagerly* free the buffer (`unsafe_free!`) instead of waiting for the
# GC. Disk storage is delegated to a `GenericFileDevice` leaf.

const _DATADEPS_DISK_DEVICE = Ref{Any}(nothing)

"""
    datadeps_disk_device() -> DatadepsDevice

The per-worker `DatadepsDevice` instance, created lazily on first use. Slot
copies created by Datadeps on this worker are managed by this device.
"""
function datadeps_disk_device()
    dev = _DATADEPS_DISK_DEVICE[]
    if dev === nothing
        dev = DatadepsDevice()
        _DATADEPS_DISK_DEVICE[] = dev
    end
    return dev::DatadepsDevice
end

"""
    DatadepsDevice <: MemPool.StorageDevice

A MemPool storage device that manages the in-memory ⟷ disk residency of
Datadeps argument copies under direct Datadeps control. See the file header for
the design rationale.

`disk` is the lower (disk-backed) device used to hold spilled data; the upper
device is always CPU RAM. The device performs no automatic eviction: Datadeps
drives spilling explicitly via [`datadeps_spill!`](@ref), and reloading happens
on demand through `read_from_device` (e.g. triggered by `poolpin`/`poolget`).
"""
struct DatadepsDevice <: MemPool.StorageDevice
    disk::MemPool.GenericFileDevice
end
function DatadepsDevice(dir::String=joinpath(tempdir(), ".dagger-datadeps", string(getpid())))
    return DatadepsDevice(MemPool.SerializationFileDevice(dir))
end

MemPool.storage_resources(dev::DatadepsDevice) =
    Set{MemPool.StorageResource}([MemPool.CPURAMResource(), MemPool.storage_resources(dev.disk)...])
MemPool.externally_varying(::DatadepsDevice) = false
MemPool.initial_leaf_device(::DatadepsDevice) = MemPool.CPURAMDevice()
MemPool.isretained(::MemPool.RefState, ::DatadepsDevice) = false

function MemPool.storage_capacity(dev::DatadepsDevice, res::MemPool.StorageResource)
    if res isa MemPool.CPURAMResource
        return MemPool.storage_capacity(res)
    elseif res in MemPool.storage_resources(dev.disk)
        return MemPool.storage_capacity(dev.disk, res)
    else
        throw(ArgumentError("Invalid storage resource $res for device $dev"))
    end
end
function MemPool.storage_available(dev::DatadepsDevice, res::MemPool.StorageResource)
    if res isa MemPool.CPURAMResource
        return MemPool.storage_available(res)
    elseif res in MemPool.storage_resources(dev.disk)
        return MemPool.storage_available(dev.disk, res)
    else
        throw(ArgumentError("Invalid storage resource $res for device $dev"))
    end
end
function MemPool.storage_utilized(dev::DatadepsDevice, res::MemPool.StorageResource)
    if res isa MemPool.CPURAMResource
        return MemPool.storage_utilized(res)
    elseif res in MemPool.storage_resources(dev.disk)
        return MemPool.storage_utilized(dev.disk, res)
    else
        throw(ArgumentError("Invalid storage resource $res for device $dev"))
    end
end

# Block until any in-flight transition on `state` has completed (the
# `StorageState`'s event is notified). Accessing a field forces the wait.
function _datadeps_sync(state::MemPool.RefState)
    sstate = MemPool.storage_read(state)
    sstate.leaves  # field access waits on the state's event
    return sstate
end

# At `poolset`: the data is already resident in memory; just account for it.
function MemPool.write_to_device!(dev::DatadepsDevice, state::MemPool.RefState, ref_id::Int)
    sstate = MemPool.storage_read(state)
    if sstate.data === nothing
        # Restored directly onto a disk leaf (rare); load it into memory.
        MemPool.read_from_device(dev, state, ref_id, false)
    else
        MemPool.mem_reserve!(state.size; force=true)
    end
    return
end

# Swap-in: if the data is resident, return it; otherwise reload it from the disk
# leaf into a fresh Libc-backed buffer and account for it. Triggered by
# `poolget`, and by `poolpin` (which calls this to ensure residency).
function MemPool.read_from_device(dev::DatadepsDevice, state::MemPool.RefState, ref_id::Int, ret::Bool)
    sstate = MemPool.storage_read(state)
    if sstate.data !== nothing
        ret && return something(sstate.data)
        return
    end
    # Deserialize from the disk leaf (this installs the data in `sstate.data`).
    data = MemPool.read_from_device(dev.disk, state, ref_id, true)
    # Re-back with Libc-managed memory so a later spill can eagerly free it,
    # then drop the disk leaf (the in-memory copy is now the source of truth).
    data = libc_backed(data)
    notify(MemPool.storage_rcu!(state) do s
        MemPool.StorageState(s; data=Some{Any}(data))
    end)
    MemPool.delete_from_device!(dev.disk, state, ref_id)
    MemPool.mem_reserve!(state.size; force=true)
    ret && return data
    return
end

# Total deletion (e.g. DRef finalization): free the in-memory buffer eagerly and
# release its budget, and remove any disk leaf.
function MemPool.delete_from_device!(dev::DatadepsDevice, state::MemPool.RefState, ref_id::Int)
    sstate = MemPool.storage_read(state)
    arr = sstate.data
    if arr !== nothing
        notify(MemPool.storage_rcu!(state) do s
            MemPool.StorageState(s; data=nothing)
        end)
        unsafe_free!(something(arr))
        MemPool.mem_release!(state.size)
    end
    if findfirst(l->l.device === dev.disk, sstate.leaves) !== nothing
        MemPool.delete_from_device!(dev.disk, state, ref_id)
    end
    return
end

"""
    datadeps_spill!(state::MemPool.RefState, ref_id::Int) -> Bool

Spill the in-memory data managed by a [`DatadepsDevice`](@ref) to disk and
eagerly free its in-memory buffer, releasing its share of the logical-memory
budget. Returns `true` if a spill was performed, `false` if the data was already
on disk. The data is reloaded transparently (and re-accounted) on the next
`poolget`/`poolpin`.

It is an error to spill a pinned ref; callers must `poolunpin` first.
"""
function datadeps_spill!(dev::DatadepsDevice, state::MemPool.RefState, ref_id::Int)
    sstate = MemPool.storage_read(state)
    sstate.data === nothing && return false
    if MemPool.ispinned(state)
        throw(ConcurrencyViolationError("Cannot spill pinned DRef $ref_id; call `poolunpin` first"))
    end
    arr = something(sstate.data)
    # Serialize to the disk leaf, and wait for the write to complete before
    # freeing the in-memory buffer (so we never free data still being written).
    MemPool.write_to_device!(dev.disk, state, ref_id)
    _datadeps_sync(state)
    notify(MemPool.storage_rcu!(state) do s
        MemPool.StorageState(s; data=nothing)
    end)
    unsafe_free!(arr)
    MemPool.mem_release!(state.size)
    return true
end

# Convenience wrappers operating on a `DRef` (run on the owning worker).
function datadeps_spill!(ref::DRef)
    if ref.owner != myid()
        return remotecall_fetch(datadeps_spill!, ref.owner, ref)
    end
    state = MemPool.with_lock(()->MemPool.datastore[ref.id], MemPool.datastore_lock)
    dev = MemPool.storage_read(state).root
    dev isa DatadepsDevice || return false
    return datadeps_spill!(dev, state, ref.id)
end
datadeps_spill!(c::Chunk) =
    c.handle isa DRef ? datadeps_spill!(c.handle) : false

# --- Pin lifecycle + freeing for Datadeps slots/originals -------------------
#
# Datadeps pins both user-provided originals and its own slot copies while it
# relies on their in-memory residency (so MemPool's swap-to-disk machinery
# cannot move them and invalidate the pointer-based aliasing oracle, and so a
# slot in active use is never spilled). Pinning a remote ref routes to the
# owning worker. `poolpin` of a spilled `DatadepsDevice` ref also swaps it in.

datadeps_pin!(c::Chunk) = (poolpin(c; remote=true); return)
datadeps_unpin!(c::Chunk) = (poolunpin(c; remote=true); return)
datadeps_pin!(@nospecialize(x)) = nothing
datadeps_unpin!(@nospecialize(x)) = nothing

"""
    datadeps_free!(ref::DRef)
    datadeps_free!(c::Chunk)

Free a Datadeps slot copy: unpin it if still pinned (balancing the lifecycle
pin), then release its in-memory buffer and its share of the logical-memory
budget. For refs managed by a [`DatadepsDevice`](@ref) this goes through the
device's deletion (eager free + budget release + disk cleanup); otherwise it
falls back to eagerly freeing the underlying array.
"""
function datadeps_free!(ref::DRef)
    if ref.owner != myid()
        return remotecall_fetch(datadeps_free!, ref.owner, ref)
    end
    state = MemPool.with_lock(()->MemPool.datastore[ref.id], MemPool.datastore_lock)
    MemPool.ispinned(state) && MemPool.poolunpin(ref)
    sstate = MemPool.storage_read(state)
    dev = sstate.root
    data = sstate.data
    if data !== nothing
        # Eagerly free the in-memory buffer (as plain `unsafe_free!` does) and,
        # for `DatadepsDevice` refs, release its share of the shared budget. We
        # deliberately leave the `StorageState` intact (rather than nulling the
        # data) so a later stale `poolget` for pointer/span/aliasing queries
        # still returns the (now-freed) `Array` object, matching the prior
        # `unsafe_free!`-based behavior. The `DRef` finalizer fully tears it down.
        unsafe_free!(something(data))
        dev isa DatadepsDevice && MemPool.mem_release!(state.size)
    elseif dev isa DatadepsDevice
        # Spilled to disk: drop the on-disk data (no in-memory buffer/budget).
        MemPool.delete_from_device!(dev, state, ref.id)
    end
    return
end
function datadeps_free!(c::Chunk)
    if c.handle isa DRef
        datadeps_free!(c.handle)
    else
        unsafe_free!(c)
    end
    return
end
datadeps_free!(@nospecialize(x)) = (unsafe_free!(x); nothing)
