module ROCExt

export ROCArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap, ROCArrayDeviceProc
import MemPool: DRef, poolget
import Distributed: myid, remotecall_fetch
import LinearAlgebra
using KernelAbstractions, Adapt

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

if isdefined(Base, :get_extension)
    import AMDGPU
else
    import ..AMDGPU
end
import AMDGPU: HIPDevice, HIPContext, HIPStream, ROCArray, ROCBackend
import AMDGPU.HIP: HIPEvent
import AMDGPU: devices, context, context!, stream, stream!
import AMDGPU: rocBLAS, rocSOLVER

# ROCArrayDeviceProc is defined in Dagger so ROCSparseArraysExt can dispatch
# on it without reaching into this extension (load order is unspecified).
Dagger.@gpuproc(ROCArrayDeviceProc, ROCArray)

"Represents the memory space of a single ROCm GPU's VRAM."
struct ROCVRAMMemorySpace <: Dagger.MemorySpace
    owner::Int
    device_id::Int
end
Dagger.root_worker_id(space::ROCVRAMMemorySpace) = space.owner
Dagger.memory_space(x::ROCArray) =
    ROCVRAMMemorySpace(myid(), AMDGPU.device(x).device_id)
function Dagger.aliasing(x::ROCArray{T}) where T
    space = Dagger.memory_space(x)
    S = typeof(space)
    # N.B. Not `pointer(x)`: that takes AMDGPU stream ownership of `x` for the
    # calling task's stream, synchronizing the stream `x` was last used on and
    # re-stamping ownership as a side effect. Aliasing only needs the address.
    rptr = Dagger.RemotePtr{Cvoid}(UInt64(_raw_rocaddr(x)), space)
    return Dagger.ContiguousAliasing(Dagger.MemorySpan{S}(rptr, sizeof(T)*length(x)))
end

# Device addresses for aliasing, read without `pointer` (see above).
function _buffer_key(x::ROCArray)
    mem = x.buf[].mem
    return UInt(mem isa AMDGPU.Mem.HIPBuffer ? mem.ptr : mem.dev_ptr)
end
# `x.offset` counts elements in older AMDGPU.jl (e.g. 2.1) and bytes in newer
# ones (e.g. 2.8). Ask this version's own `derive`, once: deriving a view one
# `Float64` in reports an offset of 1 or of 8.
const OFFSET_IN_BYTES = Ref{Union{Bool,Nothing}}(nothing)
function _offset_in_bytes()
    inbytes = OFFSET_IN_BYTES[]
    inbytes === nothing || return inbytes
    probe = ROCArray{Float64}(undef, 0)
    inbytes = AMDGPU.GPUArrays.derive(Float64, probe, (0,), 1).offset == sizeof(Float64)
    OFFSET_IN_BYTES[] = inbytes
    return inbytes
end
_raw_rocaddr(x::ROCArray) =
    _buffer_key(x) + UInt(_offset_in_bytes() ? x.offset : x.offset * Base.elsize(x))
Dagger.data_address(x::ROCArray) = UInt64(_raw_rocaddr(x))

# MPI (SPMD) integration: aliasing spans broadcast from an owner rank must be
# stamped with that rank so same-device addresses on different ranks never
# falsely alias (every rank has myid() == 1 under SPMD)
Dagger.mpi_remap_space(space::ROCVRAMMemorySpace, owner::Int) =
    ROCVRAMMemorySpace(owner, space.device_id)
Dagger.value_memory_space(x::ROCArray) = Dagger.memory_space(x)

# Page-lock host staging buffers (~2x DtoH/HtoD bandwidth), unregistering
# them from a GC finalizer.
Dagger.gpu_memory_kind(::ROCArray) = :ROC
Dagger.gpu_memory_kind(::ROCVRAMMemorySpace) = :ROC
# N.B. Not `AMDGPU.Mem.pin`/`unpin`: they take AMDGPU's registration lock (a
# `ReentrantLock`), and a finalizer that finds it contended throws "task switch
# not allowed from inside gc finalizer". The unregistration is then lost, while
# GC frees the memory anyway: a freed range stays registered with the driver,
# and a later HtoD copy from memory the allocator placed there segfaulted
# inside `hipMemcpyWithStream`. We register with HIP directly, under our own
# `PIN_LOCK`, and our finalizer only `trylock`s, and on contention re-arms
# itself, which keeps the buffer alive until a later GC can unregister it.
const PIN_LOCK = ReentrantLock()
# Base pointers of buffers registered by `pin_buffer!`
const FINALIZER_PINS = Set{Ptr{Cvoid}}()
function Dagger.pin_buffer!(::Val{:ROC}, buf::DenseArray)
    isempty(buf) && return
    ptr = Ptr{Cvoid}(pointer(buf))
    @lock PIN_LOCK begin
        # Registered by someone else, and page-locked either way
        AMDGPU.Mem.is_pinned(ptr) && return
        AMDGPU.HIP.hipHostRegister(ptr, sizeof(buf), AMDGPU.HIP.hipHostRegisterMapped)
        push!(FINALIZER_PINS, ptr)
    end
    finalizer(_unpin_finalizer, buf)
    return
end
function _unpin_finalizer(buf)
    if !trylock(PIN_LOCK)
        finalizer(_unpin_finalizer, buf)
        return
    end
    try
        ptr = Ptr{Cvoid}(pointer(buf))
        ptr in FINALIZER_PINS || return
        delete!(FINALIZER_PINS, ptr)
        AMDGPU.HIP.hipHostUnregister(ptr)
    catch err
        Core.println("Dagger ROCExt: failed to unregister a pinned host buffer: ", err)
    finally
        unlock(PIN_LOCK)
    end
    return
end

# N.B. HtoD sources are not registered: HIP stages pageable uploads through
# its own pinned buffers at the same bandwidth (~11 GB/s here, 1-64 MiB), and
# registering the caller's array for one copy corrupted data. Under 2-GPU
# Datadeps GEMM, tiles read back NaN in 6 of 6 stress runs while
# `hipHostRegister`/`hipHostUnregister` bracketed each upload of a host tile
# that other tasks were concurrently reading (a private copy, which nobody
# else touches, was clean but 10x slower to upload).

function Dagger.unsafe_free!(x::ROCArray)
    AMDGPU.unsafe_free!(x)
    return
end

# N.B. The returned `Set`s are cached and shared (mirroring the CPU caches in
# `src/memory-spaces.jl`); callers must not mutate them. No invalidation
# needed: a worker's device topology is fixed for the process lifetime.
const MEMORY_SPACES_CACHE = Dagger.LockedObject(Dict{ROCArrayDeviceProc,Set{ROCVRAMMemorySpace}}())
function Dagger.memory_spaces(proc::ROCArrayDeviceProc)
    Dagger.@safe_lock1 MEMORY_SPACES_CACHE cache begin
        value = get(cache, proc, nothing)
        if value === nothing
            value = Set([ROCVRAMMemorySpace(proc.owner, proc.device_id)])
            cache[proc] = value
        end
        return value
    end
end
const SPACE_PROCESSORS_CACHE = Dagger.LockedObject(Dict{ROCVRAMMemorySpace,Set{ROCArrayDeviceProc}}())
function Dagger.processors(space::ROCVRAMMemorySpace)
    Dagger.@safe_lock1 SPACE_PROCESSORS_CACHE cache begin
        value = get(cache, space, nothing)
        if value === nothing
            value = Set([ROCArrayDeviceProc(space.owner, space.device_id)])
            cache[space] = value
        end
        return value
    end
end

# A Chunk already resident on this exact GPU unwraps via a local `poolget`
# with no GPU API calls, so its scheduler move may run inline (like a local
# CPU move). Everything else — host values, CPU or other-device Chunks —
# stays async so uploads/transfers can overlap.
Dagger.argument_move_may_inline(to_proc::ROCArrayDeviceProc, @nospecialize(value)) =
    value isa Dagger.Chunk && Dagger.processor(value) == to_proc

function to_device(proc::ROCArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return DEVICES[proc.device_id]
end
function to_context(proc::ROCArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return CONTEXTS[proc.device_id]
end
to_context(handle::Integer) = CONTEXTS[handle]
to_context(dev::HIPDevice) = to_context(dev.device_id)

function with_context!(handle::Integer)
    context!(CONTEXTS[handle])
    AMDGPU.device!(DEVICES[handle])
    stream!(STREAMS[handle])
end
function with_context!(proc::ROCArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device_id)
end
function with_context!(space::ROCVRAMMemorySpace)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device_id)
end
Dagger.with_context!(proc::ROCArrayDeviceProc) = with_context!(proc)
Dagger.with_context!(space::ROCVRAMMemorySpace) = with_context!(space)
Dagger.with_context(f, x::Union{ROCArrayDeviceProc,ROCVRAMMemorySpace}) = with_context(f, x)

"""
    task_stream_slot()
    restore_stream_slot!(old_stream)

Save and restore the running task's default HIP stream.

`AMDGPU.stream()` creates a stream when the current task doesn't have one yet,
so using it to record what to restore would allocate a HIP stream for every
Dagger task that touches a ROC processor. Those streams live until they are
finalized, and a run goes through enough tasks that stream creation itself
eventually stalls inside the driver. Reading the task-local slot directly
reports "no stream" as `nothing` instead of manufacturing one.
"""
function task_stream_slot()
    state = AMDGPU.task_local_state()
    state === nothing && return nothing
    return state.streams[AMDGPU.device_id(state.device)]
end
function restore_stream_slot!(old_stream)
    if old_stream !== nothing
        stream!(old_stream)
        return
    end
    # The task had no stream of its own; put the slot back the way we found it
    # rather than leaving it pointing at Dagger's per-device stream.
    state = AMDGPU.task_local_state()
    state === nothing && return
    state.streams[AMDGPU.device_id(state.device)] = nothing
    return
end

function with_context(f, x)
    old_ctx = context()
    old_device = AMDGPU.device()
    old_stream = task_stream_slot()

    with_context!(x)
    try
        f()
    finally
        context!(old_ctx)
        AMDGPU.device!(old_device)
        restore_stream_slot!(old_stream)
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    with_context(x) do
        AMDGPU.synchronize()
    end
end
function sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    if Dagger.root_worker_id(x) == myid()
        _sync_with_context(x)
    else
        # Do nothing, as we have received our value over a serialization
        # boundary, which should synchronize for us
    end
end

# Allocations
# FIXME: Avoids some segfaults in rocRAND
fake_rand(::Type{T}, dims::NTuple{N}) where {T,N} = ROCArray(rand(T, dims))
fake_randn(::Type{T}, dims::NTuple{N}) where {T,N} = ROCArray(randn(T, dims))
Dagger.allocate_array_func(::ROCArrayDeviceProc, ::typeof(rand)) = fake_rand
Dagger.allocate_array_func(::ROCArrayDeviceProc, ::typeof(randn)) = fake_randn
Dagger.allocate_array_func(::ROCArrayDeviceProc, ::typeof(ones)) = AMDGPU.ones
Dagger.allocate_array_func(::ROCArrayDeviceProc, ::typeof(zeros)) = AMDGPU.zeros
struct AllocateUndef{S} end
(::AllocateUndef{S})(T, dims::Dims{N}) where {S,N} = ROCArray{S,N}(undef, dims)
Dagger.allocate_array_func(::ROCArrayDeviceProc, ::Dagger.AllocateUndef{S}) where S = AllocateUndef{S}()

# In-place
# N.B. These methods assume that later operations will implicitly or
# explicitly synchronize with their associated stream
function Dagger.move!(to_space::Dagger.CPURAMMemorySpace, from_space::ROCVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if Dagger.root_worker_id(from_space) == myid()
        sync_with_context(from_space)
        with_context!(from_space)
    end
    if from isa DenseArray
        copyto!(to, from)
    else
        # A strided device view: gather it with a kernel first, since
        # `copyto!` would index it element by element from the host
        dense = similar(from, size(from))
        dense .= from
        copyto!(to, Array(dense))
    end
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::ROCVRAMMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    if to isa DenseArray
        copyto!(to, from)
    else
        # A strided device view: upload densely, then scatter with a kernel
        dense = ROCArray{T,N}(undef, size(from))
        copyto!(dense, from isa DenseArray ? from : collect(from))
        to .= dense
    end
    return
end
function Dagger.move!(to_space::ROCVRAMMemorySpace, from_space::ROCVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if to_space != from_space && to_space.owner == from_space.owner == myid()
        peer_move!(to_space, from_space, to, from)
        return
    end
    sync_with_context(from_space)
    with_context!(to_space)
    if Dagger.needs_multi_span_copy(to_space, from_space, to, from)
        Dagger.multi_span_move!(to, from)
    else
        copyto!(to, from)
    end
    return
end

### Same-process copies between two GPUs
# Unlike CUDA.jl, HIP does not stage these copies through pageable host
# memory: without peer access (RX 6900 XTs over PCIe have none), AMDGPU's
# `copyto!` and `hipMemcpyPeerAsync` both reach ~5.5 GB/s on PCIe 3.0. But the
# old path synchronized the source's whole device stream first and queued the
# copy on the destination's device stream, so every transfer waited for both
# GPUs' unrelated kernels, and Datadeps' remainder copies took a host round
# trip instead.
#
# The copy runs on the destination's `COPY_STREAMS` entry rather than on the
# device stream every Dagger task uses, so it overlaps that device's kernels.
# Ordering is per allocation (`BUFFER_EVENTS`): the copy stream waits only for
# the source's last write and the destination's last write and read, not for
# everything queued on either device -- the source GPU is usually busy
# *reading* the very panel being sent. The copying task then waits on the host
# for the copy alone, so when Datadeps sees the copy task finish the copy is
# complete: later consumers, writers of the source, frees and host reads need
# no further ordering, and every existing synchronization point stays valid.
# Only raw addresses are read (as `Dagger.aliasing` does): `pointer` would
# take AMDGPU stream ownership and synchronize the array's last stream.
_rocdevice(x::ROCArray) = AMDGPU.device(x).device_id

const COPY_STREAMS = Dict{Int, HIPStream}()

# Outstanding device-stream work per allocation, keyed by base address.
# `write`/`read` are events recorded on the owning device's stream after the
# last task that wrote/read the allocation there; `nothing` means nothing
# outstanding. An allocation with no entry has an unknown history, and a copy
# waits on its whole device stream. Entries for freed allocations go stale;
# a stale event only makes a copy into a reused address wait longer.
#
# A peer copy leaves its destination complete, and the copy task's own record
# in `execute!` need not then add a device-stream event to it, which would
# make later copies out of it wait for unrelated kernels. So the copy stamps
# the entry with its task (`filled_by`), and only that task's record skips
# the write. The stamp must name the task: state like "skip the next write"
# outlives a freed allocation, and swallows the first write to the next
# allocation at its address -- a copy out of that one then reads stale data.
mutable struct BufferEvents
    write::Union{HIPEvent,Nothing}
    read::Union{HIPEvent,Nothing}
    # The `COPY_TASK` whose peer copy last completed the allocation (0: none)
    filled_by::UInt64
end
# The `move!` task running on this thread of control, numbered per `execute!`
# (0 outside one). Scoped, so `move!`'s own helper tasks (a remotecall to this
# worker runs on a fresh task) still see it.
const COPY_TASK = Dagger.ScopedValue{UInt64}(UInt64(0))
const COPY_TASK_COUNT = Threads.Atomic{UInt64}(0)
const BUFFER_EVENTS = Dict{UInt, BufferEvents}()
# Per device, the event after the last task with an argument that may hold
# device memory but is not an array of this backend (so any allocation of
# the device may have been touched); copies wait on it as well.
const UNTRACKED_EVENTS = Dict{Int, HIPEvent}()
const BUFFER_EVENTS_LOCK = ReentrantLock()
const BUFFER_EVENTS_PRUNE_AT = Ref(4096)

# Events are created on the stream's device
_record_on(dev::Int, stream::HIPStream) =
    with_context(() -> HIPEvent(stream), dev)

_tracked_buffer(x::ROCArray) = x
function _tracked_buffer(x)
    x isa AbstractArray || return nothing
    s = Dagger.storage_array(x)
    return s isa ROCArray ? s : nothing
end
_maybe_device_memory(x) =
    !(isbits(x) || x isa Union{Symbol,AbstractString,Function,Type,Module} ||
      (x isa Array && isbitstype(eltype(x))))

_local_value(x) = x
# A remote chunk is in no allocation of this process
_local_value(x::Chunk) = Dagger.is_local(Dagger.current_acceleration(), x.handle) ? unwrap(x) : nothing

# Caller holds `BUFFER_EVENTS_LOCK`
function _note_use!(dev::Int, ev::HIPEvent, x, written::Bool, copy_task::UInt64=UInt64(0))
    buf = _tracked_buffer(x)
    if buf === nothing
        _maybe_device_memory(x) && (UNTRACKED_EVENTS[dev] = ev)
        return
    end
    # e.g. Datadeps' `unsafe_free!` tasks; the stale entry is pruned later
    buf.buf.freed && return
    _rocdevice(buf) == dev || return
    key = _buffer_key(buf)
    entry = get(BUFFER_EVENTS, key, nothing)
    if written
        if entry === nothing
            BUFFER_EVENTS[key] = BufferEvents(ev, nothing, 0)
        elseif copy_task != 0 && entry.filled_by == copy_task
            # Filled by this very task's peer copy, which is complete
        else
            entry.write = ev
            entry.read = nothing
            entry.filled_by = 0
        end
    elseif entry !== nothing
        # Reads on one stream are ordered, so the latest stands for all
        entry.read = ev
    end
    # (A read of an allocation with no entry stays unknown: a later copy
    # into it then waits on the whole device stream, which covers the read.)
    return
end

_event_pending(ev::Union{HIPEvent,Nothing}) = ev !== nothing && !AMDGPU.HIP.isdone(ev)
function _prune_buffer_events!()
    length(BUFFER_EVENTS) < BUFFER_EVENTS_PRUNE_AT[] && return
    filter!(BUFFER_EVENTS) do (_, entry)
        _event_pending(entry.write) || _event_pending(entry.read)
    end
    BUFFER_EVENTS_PRUNE_AT[] = max(4096, 2 * length(BUFFER_EVENTS))
    return
end

# Record what a task launched on `dev` used, once its work is enqueued.
# `writes` lists the written positional arguments (the `writes` option);
# `nothing` (no Datadeps) treats every argument as possibly written.
function _record_task!(dev::Int, @nospecialize(f), @nospecialize(args::Tuple), @nospecialize(result),
                       writes::Union{Vector{Int},Nothing}, copy_task::UInt64)
    ev = _record_on(dev, STREAMS[dev])
    @lock BUFFER_EVENTS_LOCK begin
        if Dagger.is_move_task(f) && length(args) == 5
            # move!(dep_mod, to_space, from_space, to, from), whose array
            # arguments arrive as `Chunk`s (Datadeps spawns copies `meta`)
            _note_use!(dev, ev, _local_value(args[4]), true, copy_task)
            _note_use!(dev, ev, _local_value(args[5]), false)
        else
            for i in eachindex(args)
                _note_use!(dev, ev, args[i], writes === nothing || i in writes)
            end
        end
        _note_use!(dev, ev, result, true)
        _prune_buffer_events!()
    end
    return
end

# A fresh allocation on `dev`'s stream: copies into it wait for the
# allocation only (it may reuse memory freed by work still queued there)
function _note_alloc!(dev::Int, x::ROCArray)
    ev = _record_on(dev, STREAMS[dev])
    @lock BUFFER_EVENTS_LOCK begin
        BUFFER_EVENTS[_buffer_key(x)] = BufferEvents(ev, nothing, 0)
    end
    return x
end
# A fresh allocation with no outstanding device-stream work
function _note_complete!(x::ROCArray)
    @lock BUFFER_EVENTS_LOCK begin
        BUFFER_EVENTS[_buffer_key(x)] = BufferEvents(nothing, nothing, 0)
    end
    return x
end

# `x`'s outstanding events now, before ordering work after them
function _events_snapshot(x::ROCArray)
    @lock BUFFER_EVENTS_LOCK begin
        entry = get(BUFFER_EVENTS, _buffer_key(x), nothing)
        return entry === nothing ? (nothing, nothing) : (entry.write, entry.read)
    end
end
# `x` was filled by work that completed after the events in `snapshot`: clear
# those, but keep any recorded since (another task may write another part of
# the same allocation meanwhile), and stamp the entry for this task's record.
function _note_filled!(x::ROCArray, snapshot)
    copy_task = COPY_TASK[]
    @lock BUFFER_EVENTS_LOCK begin
        key = _buffer_key(x)
        entry = get(BUFFER_EVENTS, key, nothing)
        if entry === nothing
            BUFFER_EVENTS[key] = BufferEvents(nothing, nothing, copy_task)
        else
            entry.write === snapshot[1] && (entry.write = nothing)
            entry.read === snapshot[2] && (entry.read = nothing)
            entry.filled_by = copy_task
        end
    end
    return x
end

# Events a copy reading `from` (on device `s`) into `to` (on device `d`) must
# wait for, and the snapshot of `to`'s events they include (`_note_filled!`)
function _copy_dependencies(s::Int, from::ROCArray, d::Int, to::ROCArray)
    evs = HIPEvent[]
    local to_snapshot
    @lock BUFFER_EVENTS_LOCK begin
        entry = get(BUFFER_EVENTS, _buffer_key(from), nothing)
        if entry === nothing
            push!(evs, _record_on(s, STREAMS[s]))
        elseif entry.write !== nothing
            push!(evs, entry.write)
        end
        haskey(UNTRACKED_EVENTS, s) && push!(evs, UNTRACKED_EVENTS[s])
        entry = get(BUFFER_EVENTS, _buffer_key(to), nothing)
        if entry === nothing
            push!(evs, _record_on(d, STREAMS[d]))
            to_snapshot = (nothing, nothing)
        else
            entry.write !== nothing && push!(evs, entry.write)
            entry.read !== nothing && push!(evs, entry.read)
            to_snapshot = (entry.write, entry.read)
        end
        haskey(UNTRACKED_EVENTS, d) && push!(evs, UNTRACKED_EVENTS[d])
    end
    # Work AMDGPU tracks on some other stream (e.g. code outside Dagger)
    for (dev, buf) in ((s, from), (d, to))
        managed = buf.buf[]
        if managed.dirty && managed.stream.stream != STREAMS[dev].stream &&
           managed.stream.stream != COPY_STREAMS[dev].stream
            push!(evs, _record_on(dev, managed.stream))
        end
    end
    return evs, to_snapshot
end

# Copy `lens[i]` bytes from `src_ptrs[i]` (inside `from`, on `from_space`)
# to `dst_ptrs[i]` (inside `to`, on `to_space`); returns once complete.
function peer_copy_spans!(to_space::ROCVRAMMemorySpace, from_space::ROCVRAMMemorySpace,
                          to::ROCArray, from::ROCArray, dst_ptrs, src_ptrs, lens)
    isempty(lens) && return
    d, s = to_space.device_id, from_space.device_id
    @assert _rocdevice(to) == d && _rocdevice(from) == s
    copy_stream = COPY_STREAMS[d]
    deps, to_snapshot = _copy_dependencies(s, from, d, to)
    # Only raw addresses reach the copy: keep both arrays (e.g. the packed
    # staging buffers, referenced nowhere else) alive until it completes
    GC.@preserve to from begin
        with_context(d) do
            for ev in deps
                AMDGPU.HIP.hipStreamWaitEvent(copy_stream.stream, ev.handle, 0)
            end
            # (HIP numbers devices from 0, AMDGPU from 1)
            for i in eachindex(lens)
                len = lens[i]
                len == 0 && continue
                AMDGPU.HIP.hipMemcpyPeerAsync(Ptr{Cvoid}(dst_ptrs[i]), d - 1,
                                              Ptr{Cvoid}(src_ptrs[i]), s - 1,
                                              len, copy_stream.stream)
            end
            AMDGPU.HIP.synchronize(HIPEvent(copy_stream))
        end
    end
    # Everything the copy waited for is complete too
    _note_filled!(to, to_snapshot)
    return
end

# Spans below this average size are packed into one contiguous buffer on each
# side first: every peer copy without P2P is a host-staged transfer with its
# own fixed cost, so a halo's thousands of short column segments would
# otherwise cost thousands of round trips.
const PEER_PACK_MAX_AVG_BYTES = 256 * 1024
const PEER_PACK_MIN_SPANS = 16

function peer_copy_span_pairs!(to_space::ROCVRAMMemorySpace, from_space::ROCVRAMMemorySpace,
                               to_s::ROCArray{T}, from_s::ROCArray{T},
                               dst_ptrs::Vector{UInt64}, src_ptrs::Vector{UInt64},
                               lens::Vector{UInt64}) where T
    n = length(lens)
    total = sum(lens; init=UInt64(0))
    total == 0 && return
    if n < PEER_PACK_MIN_SPANS || total ÷ n >= PEER_PACK_MAX_AVG_BYTES
        peer_copy_spans!(to_space, from_space, to_s, from_s, dst_ptrs, src_ptrs, lens)
        return
    end
    elsize = sizeof(T)
    @assert total % elsize == 0
    nelem = Int(total ÷ elsize)
    # Gather on the source device, copy one buffer, scatter on the destination.
    # (`multi_span_copy!` synchronizes its device after each kernel.)
    src_packed = with_context(from_space) do
        packed = ROCArray{T}(undef, nelem)
        base = UInt64(_raw_rocaddr(packed))
        offs = UInt64(0)
        packed_ptrs = Vector{UInt64}(undef, n)
        for i in 1:n
            packed_ptrs[i] = base + offs
            offs += lens[i]
        end
        Dagger.multi_span_copy!(packed, from_s, packed_ptrs, src_ptrs, lens)
        _note_complete!(packed)
    end
    dst_packed = with_context(() -> _note_alloc!(to_space.device_id, ROCArray{T}(undef, nelem)), to_space)
    peer_copy_spans!(to_space, from_space, dst_packed, src_packed,
                     UInt64[_raw_rocaddr(dst_packed)], UInt64[_raw_rocaddr(src_packed)], UInt64[total])
    to_snapshot = _events_snapshot(to_s)
    with_context(to_space) do
        base = UInt64(_raw_rocaddr(dst_packed))
        offs = UInt64(0)
        packed_ptrs = Vector{UInt64}(undef, n)
        for i in 1:n
            packed_ptrs[i] = base + offs
            offs += lens[i]
        end
        Dagger.multi_span_copy!(to_s, dst_packed, dst_ptrs, packed_ptrs, lens)
    end
    # The scatter ran on (and synchronized) the stream `to_s`'s events are on.
    # The packing buffers die here: forget them, so their addresses come back
    # with no history.
    _note_filled!(to_s, to_snapshot)
    @lock BUFFER_EVENTS_LOCK begin
        delete!(BUFFER_EVENTS, _buffer_key(src_packed))
        delete!(BUFFER_EVENTS, _buffer_key(dst_packed))
    end
    return
end

function peer_move!(to_space::ROCVRAMMemorySpace, from_space::ROCVRAMMemorySpace,
                    to::AbstractArray{T}, from::AbstractArray{T}) where T
    if to isa ROCArray && from isa ROCArray
        @assert length(to) == length(from)
        peer_copy_spans!(to_space, from_space, to, from, UInt64[_raw_rocaddr(to)],
                         UInt64[_raw_rocaddr(from)], UInt64[sizeof(from)])
        return
    end
    to_spans = Dagger.memory_spans(Dagger.aliasing(to))
    from_spans = Dagger.memory_spans(Dagger.aliasing(from))
    @assert length(to_spans) == length(from_spans)
    n = length(to_spans)
    dst_ptrs = Vector{UInt64}(undef, n)
    src_ptrs = Vector{UInt64}(undef, n)
    lens = Vector{UInt64}(undef, n)
    for i in 1:n
        @assert Dagger.span_len(to_spans[i]) == Dagger.span_len(from_spans[i])
        dst_ptrs[i] = UInt64(Dagger.span_start(to_spans[i]))
        src_ptrs[i] = UInt64(Dagger.span_start(from_spans[i]))
        lens[i] = UInt64(Dagger.span_len(from_spans[i]))
    end
    peer_copy_span_pairs!(to_space, from_space, Dagger.storage_array(to),
                          Dagger.storage_array(from), dst_ptrs, src_ptrs, lens)
    return
end

# Out-of-place copy of `x` to another GPU of this process (complete on return).
# The source device is read from `x` itself, which is what the copy reads.
function _peer_copy_out(to_proc::ROCArrayDeviceProc, x::ROCArray)
    to_space = only(Dagger.memory_spaces(to_proc))
    from_space = ROCVRAMMemorySpace(myid(), _rocdevice(x))
    to_arr = with_context(() -> _note_alloc!(to_proc.device_id, similar(x)), to_proc)
    peer_move!(to_space, from_space, to_arr, x)
    return to_arr
end

# Remainder copies between two GPUs of this process (see `remainders.jl`).
function Dagger.device_remainder_copy!(to_space::ROCVRAMMemorySpace, from_space::ROCVRAMMemorySpace,
                                       to_s::ROCArray{T}, from_s::ROCArray{T},
                                       spans::Vector{Tuple{Dagger.LocalMemorySpan,Dagger.LocalMemorySpan}}) where T
    n = length(spans)
    dst_ptrs = Vector{UInt64}(undef, n)
    src_ptrs = Vector{UInt64}(undef, n)
    lens = Vector{UInt64}(undef, n)
    for i in 1:n
        src_span, dst_span = spans[i]
        @assert src_span.len == dst_span.len
        dst_ptrs[i] = dst_span.ptr
        src_ptrs[i] = src_span.ptr
        lens[i] = src_span.len
    end
    peer_copy_span_pairs!(to_space, from_space, to_s, from_s, dst_ptrs, src_ptrs, lens)
    return true
end

# Out-of-place HtoD
function Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x)
    with_context(to_proc) do
        arr = adapt(ROCArray, x)
        AMDGPU.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    cpu_data = remotecall_fetch(unwrap, from_w, x)
    # A chunk labelled with a host processor can already hold device memory:
    # Datadeps rebuilding a view of a host array on a GPU passes the parent
    # it already moved there. Treating that as host memory throws.
    cpu_data isa ROCArray && return Dagger.move(from_proc, to_proc, cpu_data)
    with_context(to_proc) do
        arr = adapt(ROCArray, cpu_data)
        AMDGPU.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::ROCArray)
    if AMDGPU.device(x) == to_device(to_proc)
        return x
    end
    with_context(to_proc) do
        _x = similar(x)
        copyto!(_x, x)
        AMDGPU.synchronize()
        return _x
    end
end

# Out-of-place DtoH
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        AMDGPU.synchronize()
        _x = x isa DenseArray && isbitstype(eltype(x)) ?
             Dagger.pinned_host_array(x) : adapt(Array, x)
        AMDGPU.synchronize()
        return _x
    end
end
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::CPUProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    remotecall_fetch(from_w, x) do x
        arr = unwrap(x)
        return Dagger.move(from_proc, to_proc, arr)
    end
end
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::CPUProc, x::ROCArray{T,N}) where {T,N}
    with_context(AMDGPU.device(x).device_id) do
        AMDGPU.synchronize()
        _x = Dagger.pinned_host_array(x)
        AMDGPU.synchronize()
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::ROCArrayDeviceProc, x::Dagger.Chunk{T}) where T<:ROCArray
    if from_proc == to_proc
        # Same process and GPU, no change
        arr = unwrap(x)
        with_context(AMDGPU.synchronize, from_proc)
        return arr
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy
        return _peer_copy_out(to_proc, unwrap(x))
    else
        # Different node, use DtoH, serialization, HtoD (pinned host staging)
        host_copy = remotecall_fetch(from_proc.owner, from_proc, x) do from_proc, x
            return with_context(from_proc) do
                Dagger.pinned_host_array(unwrap(x))
            end
        end
        return with_context(to_proc) do
            arr = ROCArray(host_copy)
            AMDGPU.synchronize()
            return arr
        end
    end
end

function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::ROCArrayDeviceProc, x::ROCArray)
    if from_proc == to_proc
        with_context(AMDGPU.synchronize, from_proc)
        return x
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        return _peer_copy_out(to_proc, x)
    else
        host_copy = remotecall_fetch(from_proc.owner, from_proc, x) do from_proc, x
            return with_context(from_proc) do
                Dagger.pinned_host_array(x)
            end
        end
        return with_context(to_proc) do
            arr = ROCArray(host_copy)
            AMDGPU.synchronize()
            return arr
        end
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

# Task execution
function Dagger.execute!(proc::ROCArrayDeviceProc, f, args...; kwargs...)
    @nospecialize f args kwargs
    tls = Dagger.get_tls()
    spec = tls.task_spec
    writes = spec isa Dagger.Sch.TaskSpec && spec.options !== nothing ?
             spec.options.writes : nothing
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        with_context!(proc)
        copy_task = Dagger.is_move_task(f) ?
                    Threads.atomic_add!(COPY_TASK_COUNT, UInt64(1)) + UInt64(1) : UInt64(0)
        result = if copy_task == 0
            Base.@invokelatest f(args...; kwargs...)
        else
            Dagger.with(COPY_TASK => copy_task) do
                Base.@invokelatest f(args...; kwargs...)
            end
        end
        _record_task!(proc.device_id, f, args, result, writes, copy_task)
        # N.B. Synchronization must be done when accessing result or args
        return result
    end

    try
        fetch(task)
    catch err
        stk = current_exceptions(task)
        err, frames = stk[1]
        rethrow(CapturedException(err, frames))
    end
end

# Adapt BLAS/LAPACK functions
import LinearAlgebra: BLAS, LAPACK
_keep_blas_functions = Set(["iamax"])
for lib in [BLAS, LAPACK]
    for name in names(lib; all=true)
        name == nameof(lib) && continue
        startswith(string(name), '#') && continue
        if !endswith(string(name), '!') && !any(endswith(string(name), func) for func in _keep_blas_functions)
            continue
        end

        for roclib in [rocBLAS, rocSOLVER]
            if name in names(roclib; all=true)
                fn = getproperty(lib, name)
                rocfn = getproperty(roclib, name)
                @eval Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, ::$(typeof(fn))) = $rocfn
            end
        end
    end
end

# Adapt RefValue
Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Base.RefValue) =
    Dagger.GPURef(Dagger.move(from_proc, to_proc, x[]), only(Dagger.memory_spaces(to_proc)))
Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::CPUProc, x::Dagger.GPURef{T,ROCVRAMMemorySpace} where T) =
    Ref(Dagger.move(from_proc, to_proc, x[]))
function Dagger.move!(dep_mod, to_space::CPURAMMemorySpace, from_space::ROCVRAMMemorySpace, to::Base.RefValue, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::ROCVRAMMemorySpace, from_space::CPURAMMemorySpace, to::Dagger.GPURef, from::Base.RefValue)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::ROCVRAMMemorySpace, from_space::ROCVRAMMemorySpace, to::Dagger.GPURef, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end

# Adapt HaloArray
ROCArray(H::Dagger.HaloArray) = convert(ROCArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:ROCArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.halos),
                     H.halo_width;
                     own_center=H.own_center)
Adapt.adapt_structure(to::AMDGPU.Runtime.Adaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.halos),
                     H.halo_width;
                     own_center=H.own_center)
function Dagger.inner_stencil_proc!(::ROCArrayDeviceProc, f, output, read_vars)
    Dagger.gpu_stencil_sweep!(f, output, read_vars)
    return
end

Dagger.gpu_processor(::Val{:ROC}) = ROCArrayDeviceProc
Dagger.gpu_can_compute(::Val{:ROC}) = AMDGPU.functional()
Dagger.gpu_kernel_backend(proc::ROCArrayDeviceProc) = ROCBackend()
Dagger.gpu_with_device(f, proc::ROCArrayDeviceProc) =
    AMDGPU.device!(f, AMDGPU.devices()[proc.device_id])
function Dagger.gpu_synchronize(proc::ROCArrayDeviceProc)
    with_context(proc) do
        AMDGPU.synchronize()
    end
end
function Dagger.gpu_synchronize(::Val{:ROC})
    for dev in AMDGPU.devices()
        _sync_with_context(ROCArrayDeviceProc(myid(), dev.device_id))
    end
end

Dagger.to_scope(::Val{:rocm_gpu}, sc::NamedTuple) =
    Dagger.to_scope(Val{:rocm_gpus}(), merge(sc, (;rocm_gpus=[sc.rocm_gpu])))
Dagger.to_scope(::Val{:rocm_gpus}, sc::NamedTuple) =
    Dagger.gpu_scope(ROCArrayDeviceProc, proc->proc.device_id, sc.rocm_gpus, sc)
Dagger.scope_key_precedence(::Val{:rocm_gpu}) = 3
Dagger.scope_key_precedence(::Val{:rocm_gpus}) = 3

# MPI data plane: pass ROCArrays to MPI directly when the library is ROCm-aware
# (DAGGER_MPI_GPU_DIRECT=0/1 forces the decision; detection is best-effort)
const MPI_GPU_DIRECT = Ref{Union{Nothing,Bool}}(nothing)
function mpi_gpu_direct_enabled()
    v = MPI_GPU_DIRECT[]
    v !== nothing && return v
    env = get(ENV, "DAGGER_MPI_GPU_DIRECT", "")
    v = if !isempty(env)
        something(tryparse(Bool, env), false)
    else
        Dagger.mpi_library_gpu_aware(:ROC)
    end
    MPI_GPU_DIRECT[] = v
    return v
end
Dagger.mpi_device_direct(x::AMDGPU.StridedROCArray) = mpi_gpu_direct_enabled()
Dagger.mpi_device_sync(x::ROCArray) = AMDGPU.HIP.device_synchronize()
Dagger.mpi_device_sync(::ROCVRAMMemorySpace) = AMDGPU.HIP.device_synchronize()

# Same-node device IPC via hipIpc*: pool-backed ROCArrays may not be
# exportable, so the sender stages into a hipMalloc allocation and ships its
# 64-byte handle; the receiver maps it and copies device-to-device.
# DAGGER_IPC=0 disables the path.
const GPU_IPC = Ref{Union{Nothing,Bool}}(nothing)
function ipc_enabled()
    v = GPU_IPC[]
    v !== nothing && return v
    v = something(tryparse(Bool, get(ENV, "DAGGER_IPC", "true")), true)
    GPU_IPC[] = v
    return v
end
Dagger.ipc_eligible(::ROCVRAMMemorySpace, ::ROCVRAMMemorySpace) = ipc_enabled()

struct HIPIpcInfo{T,N}
    handle::AMDGPU.HIP.hipIpcMemHandle_t
    shape::Dims{N}
end
# Token keeps a hipMalloc'd staging allocation alive until ipc_release!
struct HIPIpcToken
    ptr::Ptr{Cvoid}
    bytesize::Int
end
function Dagger.ipc_export(value::AMDGPU.StridedROCArray{T,N}) where {T,N}
    with_context!(Dagger.memory_space(value))
    nbytes = sizeof(value)
    ptr_ref = Ref{Ptr{Cvoid}}()
    AMDGPU.HIP.hipMalloc(ptr_ref, nbytes)
    staged = unsafe_wrap(ROCArray, Ptr{T}(ptr_ref[]), size(value); own=false)
    copyto!(staged, value)
    AMDGPU.HIP.device_synchronize()
    handle_ref = Ref{AMDGPU.HIP.hipIpcMemHandle_t}()
    AMDGPU.HIP.hipIpcGetMemHandle(handle_ref, ptr_ref[])
    return HIPIpcInfo{T,N}(handle_ref[], size(value)), HIPIpcToken(ptr_ref[], nbytes)
end
function Dagger.ipc_release!(token::HIPIpcToken)
    token.ptr == C_NULL && return
    AMDGPU.HIP.hipFree(token.ptr)
    return
end
function _ipc_open(info::HIPIpcInfo{T,N}) where {T,N}
    ptr_ref = Ref{Ptr{Cvoid}}()
    AMDGPU.HIP.hipIpcOpenMemHandle(ptr_ref, info.handle,
                                   AMDGPU.HIP.hipIpcMemLazyEnablePeerAccess)
    src = unsafe_wrap(ROCArray, Ptr{T}(ptr_ref[]), info.shape; own=false)
    return src, ptr_ref[]
end
function Dagger.ipc_copyto!(dest::AMDGPU.StridedROCArray{T,N}, info::HIPIpcInfo{T,N}) where {T,N}
    @assert size(dest) == info.shape "IPC shape mismatch: $(size(dest)) != $(info.shape)"
    with_context!(Dagger.memory_space(dest))
    src, raw = _ipc_open(info)
    try
        copyto!(dest, src)
        AMDGPU.HIP.device_synchronize()
    finally
        AMDGPU.HIP.hipIpcCloseMemHandle(raw)
    end
    return dest
end
function Dagger.ipc_materialize(info::HIPIpcInfo{T,N}) where {T,N}
    dest = ROCArray{T,N}(undef, info.shape)
    return Dagger.ipc_copyto!(dest, info)
end

# A stream that does not synchronize with the null stream, on the current device
function _nonblocking_stream()
    ref = Ref{AMDGPU.HIP.hipStream_t}()
    AMDGPU.HIP.hipStreamCreateWithFlags(ref, AMDGPU.HIP.hipStreamNonBlocking)
    return HIPStream(ref[])
end

const DEVICES = Dict{Int, HIPDevice}()
const CONTEXTS = Dict{Int, HIPContext}()
const STREAMS = Dict{Int, HIPStream}()

function __init__()
    if AMDGPU.functional()
        for device_id in 1:length(AMDGPU.devices())
            dev = AMDGPU.devices()[device_id]
            @debug "Registering ROCm GPU processor with Dagger: $dev"
            Dagger.add_processor_callback!("rocarray_device_$device_id") do
                proc = ROCArrayDeviceProc(myid(), device_id)
                DEVICES[dev.device_id] = dev
                ctx = HIPContext(dev)
                CONTEXTS[dev.device_id] = ctx
                context!(ctx) do
                    STREAMS[dev.device_id] = HIPStream()
                    COPY_STREAMS[dev.device_id] = _nonblocking_stream()
                end
                return proc
            end
        end
    end
end

end # module ROCExt
