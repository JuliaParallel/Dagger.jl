module CUDAExt

export CuArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap
import MemPool: DRef, poolget
import Distributed: myid, remotecall_fetch
import LinearAlgebra
using KernelAbstractions, Adapt

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

if isdefined(Base, :get_extension)
    import CUDA
else
    import ..CUDA
end
import CUDA: CuDevice, CuContext, CuStream, CuArray, CUDABackend, CuEvent
import CUDA: devices, attribute, context, context!, stream, stream!
import CUDA: CUBLAS, CUSOLVER

using UUIDs

"Represents a single CUDA GPU device."
struct CuArrayDeviceProc <: Dagger.Processor
    owner::Int
    device::Int
    device_uuid::UUID
end
Dagger.get_parent(proc::CuArrayDeviceProc) = Dagger.OSProc(proc.owner)
Dagger.root_worker_id(proc::CuArrayDeviceProc) = proc.owner
Base.show(io::IO, proc::CuArrayDeviceProc) =
    print(io, "CuArrayDeviceProc(worker $(proc.owner), device $(proc.device), uuid $(proc.device_uuid))")
Dagger.short_name(proc::CuArrayDeviceProc) = "W: $(proc.owner), CUDA: $(proc.device)"
Dagger.@gpuproc(CuArrayDeviceProc, CuArray)

"Represents the memory space of a single CUDA GPU's VRAM."
struct CUDAVRAMMemorySpace <: Dagger.MemorySpace
    owner::Int
    device::Int
    device_uuid::UUID
end
Dagger.root_worker_id(space::CUDAVRAMMemorySpace) = space.owner
function Dagger.memory_space(x::CuArray)
    dev = CUDA.device(x)
    device_id = dev.handle
    device_uuid = CUDA.uuid(dev)
    return CUDAVRAMMemorySpace(myid(), device_id, device_uuid)
end
function Dagger.aliasing(x::CuArray{T}) where T
    space = Dagger.memory_space(x)
    S = typeof(space)
    cuptr = with_context(x) do
        pointer(x)
    end
    rptr = Dagger.RemotePtr{Cvoid}(UInt64(cuptr), space)
    return Dagger.ContiguousAliasing(Dagger.MemorySpan{S}(rptr, sizeof(T)*length(x)))
end

function Dagger.unsafe_free!(x::CuArray)
    CUDA.unsafe_free!(x)
    return
end

Dagger.memory_spaces(proc::CuArrayDeviceProc) = Set([CUDAVRAMMemorySpace(proc.owner, proc.device, proc.device_uuid)])
Dagger.processors(space::CUDAVRAMMemorySpace) = Set([CuArrayDeviceProc(space.owner, space.device, space.device_uuid)])

function to_device(proc::CuArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return DEVICES[proc.device]
end
function to_context(proc::CuArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return CONTEXTS[proc.device]
end
to_context(handle::Integer) = CONTEXTS[handle]
to_context(dev::CuDevice) = to_context(dev.handle)  

function with_context!(handle::Integer, stream_idx = 1)
    context!(CONTEXTS[handle])
    stream!(STREAMS[handle][stream_idx])
end
function with_context!(proc::CuArrayDeviceProc, stream_idx = 1)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device, stream_idx)
end
function with_context!(space::CUDAVRAMMemorySpace, stream_idx = 1)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device, stream_idx)
end
function  with_context!(array::CuArray, stream_idx = 1)
    with_context!(CUDA.device(array).handle, stream_idx)
end
Dagger.with_context!(proc::CuArrayDeviceProc) = with_context!(proc)
Dagger.with_context!(space::CUDAVRAMMemorySpace) = with_context!(space)
function with_context(f, x, stream_idx = 1)
    exist = CUDA.task_local_state() !== nothing

    if exist
        old_ctx = context()
        old_stream = stream()
    end

    with_context!(x, stream_idx)
    try
        f()
    finally
        if exist
            context!(old_ctx)
            stream!(old_stream)
        end
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    caller_stream = stream()
    with_context(x) do
        ev = acquire_event!()
        CUDA.record(ev, stream())
        CUDA.wait(ev, caller_stream)
        park_event!(ev)
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
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(rand)) = CUDA.rand
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(randn)) = CUDA.randn
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(ones)) = CUDA.ones
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(zeros)) = CUDA.zeros
struct AllocateUndef{S} end
(::AllocateUndef{S})(T, dims::Dims{N}) where {S,N} = CuArray{S,N}(undef, dims)
Dagger.allocate_array_func(::CuArrayDeviceProc, ::Dagger.AllocateUndef{S}) where S = AllocateUndef{S}()

# In-place
# N.B. These methods assume that later operations will implicitly or
# explicitly synchronize with their associated stream
function Dagger.move!(to_space::Dagger.CPURAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    # if Dagger.root_worker_id(from_space) == myid()
    #     sync_with_context(from_space)
    #     with_context!(from_space)
    # end
    copyto!(to, from)
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::CUDAVRAMMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    copyto!(to, from)
    return
end
function Dagger.move!(to_space::CUDAVRAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    #sync_with_context(from_space)
    #with_context!(to_space)
    copyto!(to, from)
    return
end

# Out-of-place HtoD
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x)
    with_context(to_proc) do
        arr = adapt(CuArray, x)
        CUDA.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    cpu_data = remotecall_fetch(unwrap, from_w, x)
    with_context(to_proc) do
        arr = adapt(CuArray, cpu_data)
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::CuArray)
    if CUDA.device(x) == to_device(to_proc)
        return x
    end
    with_context(to_proc) do
        _x = similar(x)
        copyto!(_x, x)
        return _x
    end
end

# Out-of-place DtoH
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        CUDA.synchronize()
        return adapt(Array, x)
    end
end
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    remotecall_fetch(from_w, x) do x
        arr = unwrap(x)
        return Dagger.move(from_proc, to_proc, arr)
    end
end
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x::CuArray{T,N}) where {T,N}
    with_context(from_proc) do
        CUDA.synchronize()
        _x = Array{T,N}(undef, size(x))
        copyto!(_x, x)
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc, x::Dagger.Chunk{T}) where T<:CuArray
    if from_proc == to_proc
        # Same process and GPU, no change.
        # Stream ordering guarantees safety; no sync needed.
        return unwrap(x)
        
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy
        from_arr = unwrap(x)
        ev = with_context(from_proc) do
            ev = acquire_event!()
            CUDA.record(ev, stream())
            return ev
        end

        return with_context(to_proc) do
            CUDA.wait(ev, stream())
            park_event!(ev)
            to_arr = similar(from_arr)
            copyto!(to_arr, from_arr)
            return to_arr
        end
    elseif Dagger.system_uuid(from_proc.owner) == Dagger.system_uuid(to_proc.owner) && from_proc.device_uuid == to_proc.device_uuid
        # Same node, we can use IPC
        ipc_handle, eT, shape = remotecall_fetch(from_proc.owner, x) do x
            arr = unwrap(x)
            ipc_handle_ref = Ref{CUDA.CUipcMemHandle}()
            GC.@preserve arr begin
                CUDA.cuIpcGetMemHandle(ipc_handle_ref, pointer(arr))
            end
            (ipc_handle_ref[], eltype(arr), size(arr))
        end
        r_ptr = Ref{CUDA.CUdeviceptr}()
        CUDA.device!(from_proc.device) do
            CUDA.cuIpcOpenMemHandle(r_ptr, ipc_handle, CUDA.CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS)
        end
        ptr = Base.unsafe_convert(CUDA.CuPtr{eT}, r_ptr[])
        arr = unsafe_wrap(CuArray, ptr, shape; own=false)
        finalizer(arr) do arr
            CUDA.cuIpcCloseMemHandle(pointer(arr))
        end
        if from_proc.device_uuid != to_proc.device_uuid
            return CUDA.device!(to_proc.device) do
                to_arr = similar(arr)
                copyto!(to_arr, arr)
                to_arr
            end
        else
            return arr
        end        
    else
        # Different node, use DtoH, serialization, HtoD
        host_copy = remotecall_fetch(from_proc.owner, from_proc, x) do from_proc, x
            return with_context(from_proc) do
                CUDA.synchronize() 
                Array(unwrap(x))
            end
        end
        return with_context(to_proc) do
            return CuArray(host_copy)
        end
    end
end

function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc, x::CuArray)
    if from_proc == to_proc
        return x
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        
        ev = with_context(from_proc) do
            ev = acquire_event!()
            CUDA.record(ev, stream())
            return ev
        end

        return with_context(to_proc) do
            CUDA.wait(ev, stream())
            park_event!(ev)
            to_arr = similar(x)
            copyto!(to_arr, x)
            return to_arr
        end

    else
        host_copy = with_context(from_proc) do
            CUDA.synchronize()
            return Array(x)
        end

        return with_context(to_proc) do
            return CuArray(host_copy)
        end
    end
end

# Out-of-place move for LinearAlgebra wrappers (UpperTriangular, LowerTriangular, etc.)
# Unwraps the parent CuArray, moves it to the target device, and rewraps.
# This fixes "cannot take the GPU address of inaccessible device memory" when
# norm/isapprox fetches a wrapper chunk that lives on a different GPU.
for W in (:UpperTriangular, :LowerTriangular, :UnitUpperTriangular, :UnitLowerTriangular)
    @eval function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc,
                               x::LinearAlgebra.$W{T,<:CuArray}) where T
        moved = Dagger.move(from_proc, to_proc, parent(x))
        return LinearAlgebra.$W(moved)
    end
    @eval function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc,
                               x::LinearAlgebra.$W)
        moved = Dagger.move(from_proc, to_proc, parent(x))
        return LinearAlgebra.$W(moved)
    end
    @eval function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc,
                               x::LinearAlgebra.$W{T,<:CuArray}) where T
        moved = Dagger.move(from_proc, to_proc, parent(x))
        return LinearAlgebra.$W(moved)
    end
end
for W in (:Adjoint, :Transpose)
    @eval function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc,
                               x::LinearAlgebra.$W{T,<:CuArray}) where T
        moved = Dagger.move(from_proc, to_proc, parent(x))
        return LinearAlgebra.$W(moved)
    end
    @eval function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc,
                               x::LinearAlgebra.$W)
        moved = Dagger.move(from_proc, to_proc, parent(x))
        return LinearAlgebra.$W(moved)
    end
    @eval function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc,
                               x::LinearAlgebra.$W{T,<:CuArray}) where T
        moved = Dagger.move(from_proc, to_proc, parent(x))
        return LinearAlgebra.$W(moved)
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

const ROUNDROBIN = Dict{Int, Threads.Atomic{Int}}()
# Per-stream queues of completion CuEvents — device occupancy for :sdq / :locality.
const STREAM_INFLIGHT = Dict{Int, Vector{Vector{CuEvent}}}()
const INFLIGHT_LOCK = ReentrantLock()
const STREAM_STRATEGY = Ref{Symbol}(:roundrobin)
# Monotonic per-stream generation (bumped when a task is assigned to the stream).
const STREAM_GEN = Dict{Int, Vector{UInt64}}()
# LAST_WAITED[consumer_dev][consumer_s][(pdev, pstr)] = STREAM_GEN at last wait.
const LAST_WAITED = Dict{Int, Vector{Dict{Tuple{Int,Int}, UInt64}}}()

# Pooled CuEvents (DISABLE_TIMING). After wait/record enqueue, park until isdone
# before reuse — re-recording a still-waited event would break happens-before.
const EVENT_POOL_LOCK = ReentrantLock()
const EVENT_FREE = CuEvent[]
const EVENT_PENDING = CuEvent[]

_new_event() = CuEvent(CUDA.EVENT_DISABLE_TIMING)

function _drain_pending_events_unlocked!()
    i = 1
    while i <= length(EVENT_PENDING)
        if CUDA.isdone(EVENT_PENDING[i])
            push!(EVENT_FREE, EVENT_PENDING[i])
            deleteat!(EVENT_PENDING, i)
        else
            i += 1
        end
    end
end

function acquire_event!()
    lock(EVENT_POOL_LOCK) do
        _drain_pending_events_unlocked!()
        return isempty(EVENT_FREE) ? _new_event() : pop!(EVENT_FREE)
    end
end

function park_event!(ev::CuEvent)
    lock(EVENT_POOL_LOCK) do
        push!(EVENT_PENDING, ev)
        _drain_pending_events_unlocked!()
    end
    return
end

"""
    stream_strategy!(s::Symbol)

Set the stream distribution strategy: `:roundrobin`, `:random`,
`:sdq` (shortest stream queue), or `:locality` (run a task on the stream that
produced its inputs, falling back to shortest-queue when that stream is
overloaded or no on-device producer exists). Also settable via the
`DAGGER_CUDA_STREAM_STRATEGY` environment variable at load time.
"""
function stream_strategy!(s::Symbol)
    s in (:roundrobin, :random, :sdq, :locality) ||
        throw(ArgumentError("unknown stream strategy: $s (use :roundrobin, :random, :sdq, or :locality)"))
    STREAM_STRATEGY[] = s
end

# Follow the producer stream unless it's this many in-flight events deeper than
# the shortest queue; raised vs host-atomic depths so device-side occupancy
# does not abandon locality too eagerly.
const LOCALITY_SLACK = Ref(8)

function stream_depth(dev::Int, i::Int)
    done = CuEvent[]
    depth = lock(INFLIGHT_LOCK) do
        q = STREAM_INFLIGHT[dev][i]
        while !isempty(q) && CUDA.isdone(q[1])
            push!(done, popfirst!(q))
        end
        length(q)
    end
    for ev in done
        park_event!(ev)
    end
    return depth
end

# Purge completed markers and return all stream depths under one lock.
function stream_depths!(dev::Int)
    n = length(STREAM_INFLIGHT[dev])
    done = CuEvent[]
    depths = lock(INFLIGHT_LOCK) do
        ds = Vector{Int}(undef, n)
        for i in 1:n
            q = STREAM_INFLIGHT[dev][i]
            while !isempty(q) && CUDA.isdone(q[1])
                push!(done, popfirst!(q))
            end
            ds[i] = length(q)
        end
        ds
    end
    for ev in done
        park_event!(ev)
    end
    return depths
end

# Shortest in-flight stream index for `dev` (device-side occupancy).
function sdq_stream(dev::Int, n::Int)
    depths = stream_depths!(dev)
    return argmin(depths)
end

function mark_stream_complete!(dev::Int, s::Int)
    ev = acquire_event!()
    CUDA.record(ev, STREAMS[dev][s])
    lock(INFLIGHT_LOCK) do
        push!(STREAM_INFLIGHT[dev][s], ev)
    end
    return
end

# Producer task uid for a syncdep, handling both the ThunkID-wrapped form
# (`.id.id`) and the post-submission thunk-wrapped form (`.id === nothing`).
# Matches the key written by `execute!` (`deps[task_id()]`), since `task_id()`
# and `Thunk.id` are both the task uid.
function producer_uid(s)
    s.id !== nothing && return s.id.id
    return Dagger.unwrap_weak(s).id
end

# Locality: the on-device producer stream carrying the most of this task's
# inputs, unless it's overloaded relative to the shortest queue.
function locality_stream(dev::Int, local_sync, deps)
    n = length(STREAMS[dev])
    local_sync === nothing && return sdq_stream(dev, n)
    counts = nothing
    for syncdep in local_sync
        entry = get(deps, producer_uid(syncdep), nothing)
        entry === nothing && continue
        (sdev, sstr) = entry
        sdev == dev || continue                 # cross-device producer ⇒ not stream-local
        counts === nothing && (counts = zeros(Int, n))
        counts[sstr] += 1
    end
    counts === nothing && return sdq_stream(dev, n)  # no on-device producer to follow
    depths = stream_depths!(dev)
    maxc = maximum(counts)
    # Among majority-count ties, prefer the shallowest queue (not lowest index).
    best = 0
    bestd = typemax(Int)
    for i in 1:n
        counts[i] == maxc || continue
        if depths[i] < bestd
            bestd = depths[i]
            best = i
        end
    end
    minidx = argmin(depths)
    return (depths[best] - depths[minidx]) > LOCALITY_SLACK[] ? minidx : best
end

# `local_sync`/`deps` are only consulted by the :locality strategy.
function pick_stream(dev::Int, local_sync=nothing, deps=nothing)
    n = length(STREAMS[dev])
    s = STREAM_STRATEGY[]
    if s == :roundrobin
        return mod1(Threads.atomic_add!(ROUNDROBIN[dev], 1), n)
    elseif s == :random
        return rand(1:n)
    elseif s == :locality
        return locality_stream(dev, local_sync, deps)
    else # :sdq
        return sdq_stream(dev, n)
    end
end

# Collect unique foreign producer (dev, stream) keys for cross-stream waits.
# Skips producers this consumer stream already waited at the current generation
# (LAST_WAITED / STREAM_GEN). Must run under the SYNCDEPS lock.
function collect_wait_list!(wait_list::Vector{Tuple{Int,Int}},
                            mydev::Int, s::Int, local_sync, deps)
    empty!(wait_list)
    local_sync === nothing && return wait_list
    seen = Set{Tuple{Int,Int}}()
    lw = LAST_WAITED[mydev][s]
    for syncdep in local_sync
        # Absent ⇒ producer was not a GPU task, so Dagger's host-side
        # completion already ordered us against it.
        entry = get(deps, producer_uid(syncdep), nothing)
        isnothing(entry) && continue
        (dev, stream) = entry
        # Same stream ⇒ already ordered in-order; no event needed.
        (dev == mydev && stream == s) && continue
        key = (dev, stream)
        key in seen && continue
        gen = STREAM_GEN[dev][stream]
        get(lw, key, UInt64(0)) >= gen && continue
        push!(seen, key)
        push!(wait_list, key)
    end
    return wait_list
end

function issue_cross_stream_waits!(wait_list::Vector{Tuple{Int,Int}}, mydev::Int, s::Int)
    for (dev, stream) in wait_list
        Threads.atomic_add!(_EVENT_COUNT, 1)
        ev = acquire_event!()
        CUDA.record(ev, STREAMS[dev][stream])
        CUDA.wait(ev, STREAMS[mydev][s])
        park_event!(ev)
        # Snapshot gen under SYNCDEPS so it stays consistent with collect skips.
        lock(SYNCDEPS) do _
            LAST_WAITED[mydev][s][(dev, stream)] = STREAM_GEN[dev][stream]
        end
    end
    return
end

"""
    clear_stream_syncdeps!()

Drop producer→stream map entries and reclaim completed pooled events / in-flight
completion markers. Safe after a `spawn_datadeps` region has fully waited.
"""
function clear_stream_syncdeps!()
    lock(SYNCDEPS) do m
        empty!(m)
        for (_, gens) in STREAM_GEN
            fill!(gens, UInt64(0))
        end
        for (_, caches) in LAST_WAITED
            for d in caches
                empty!(d)
            end
        end
    end
    retired = CuEvent[]
    lock(INFLIGHT_LOCK) do
        for (_, qs) in STREAM_INFLIGHT
            for q in qs
                append!(retired, q)
                empty!(q)
            end
        end
    end
    lock(EVENT_POOL_LOCK) do
        append!(EVENT_PENDING, retired)
        _drain_pending_events_unlocked!()
    end
    return
end
Dagger.clear_gpu_stream_syncdeps!(::Val{:CUDA}) = clear_stream_syncdeps!()

# Task execution
function Dagger.execute!(proc::CuArrayDeviceProc, f, args...; kwargs...)
    @nospecialize f args kwargs
    tls = Dagger.get_tls()
    mydev = proc.device
    mytid = Dagger.task_id()
    # N.B. `Dagger.get_options()` only carries the *propagated* options (those
    # named in `options.propagates`, empty by default), so it never holds
    # :syncdeps — the real set lives on the TLS task spec.
    local_sync = tls.task_spec.options.syncdeps
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        wait_list = Tuple{Int,Int}[]
        # Under SYNCDEPS: pick stream, snapshot unique waits, register tid.
        # Bump STREAM_GEN at assign (before launch) so concurrent collectors
        # cannot skip a wait against in-flight producer work.
        # CUDA record/wait happens *outside* the lock so launches do not serialize
        # on the mutex during driver calls.
        cr_str = lock(SYNCDEPS) do deps
            s = pick_stream(mydev, local_sync, deps)
            collect_wait_list!(wait_list, mydev, s, local_sync, deps)
            deps[mytid] = (mydev, s)
            STREAM_GEN[mydev][s] += UInt64(1)
            return s
        end
        issue_cross_stream_waits!(wait_list, mydev, cr_str)
        with_context!(proc, cr_str)

        result = Base.@invokelatest f(args...; kwargs...)
        # Device occupancy for :sdq / :locality (completion after work is enqueued).
        mark_stream_complete!(mydev, cr_str)
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

        for culib in [CUBLAS, CUSOLVER]
            if name in names(culib; all=true)
                fn = getproperty(lib, name)
                cufn = getproperty(culib, name)
                @eval Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, ::$(typeof(fn))) = $cufn
            end
        end
    end
end

# Adapt RefValue
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Base.RefValue) =
    Dagger.GPURef(Dagger.move(from_proc, to_proc, x[]), only(Dagger.memory_spaces(to_proc)))
Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x::Dagger.GPURef{T,CUDAVRAMMemorySpace} where T) =
    Ref(Dagger.move(from_proc, to_proc, x[]))
function Dagger.move!(dep_mod, to_space::CPURAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::Base.RefValue, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::CUDAVRAMMemorySpace, from_space::CPURAMMemorySpace, to::Dagger.GPURef, from::Base.RefValue)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::CUDAVRAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::Dagger.GPURef, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end

# Adapt HaloArray
CuArray(H::Dagger.HaloArray) = convert(CuArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:CuArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.halos),
                     H.halo_width)
Adapt.adapt_structure(to::CUDA.KernelAdaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.halos),
                     H.halo_width)
function Dagger.inner_stencil_proc!(::CuArrayDeviceProc, f, output, read_vars)
    Dagger.Kernel(_inner_stencil!)(f, output, read_vars; ndrange=size(output))
    return
end
@kernel function _inner_stencil!(f, output, read_vars)
    idx = @index(Global, Cartesian)
    f(idx, output, read_vars)
end

Dagger.gpu_processor(::Val{:CUDA}) = CuArrayDeviceProc
Dagger.gpu_can_compute(::Val{:CUDA}) = CUDA.has_cuda()
Dagger.gpu_kernel_backend(::CuArrayDeviceProc) = CUDABackend()
Dagger.gpu_with_device(f, proc::CuArrayDeviceProc) =
    CUDA.device!(f, proc.device)
function Dagger.gpu_synchronize(proc::CuArrayDeviceProc)
    @assert !Dagger.in_task()
    user_stream = stream()

    with_context(proc) do
        for proc_stream in STREAMS[proc.device]
            ev = acquire_event!()
            CUDA.record(ev, proc_stream)
            CUDA.wait(ev, user_stream)
            park_event!(ev)
        end

    end
end
function Dagger.gpu_synchronize(::Val{:CUDA})
    for dev in CUDA.devices()
        proc = CuArrayDeviceProc(myid(), dev.handle, CUDA.uuid(dev))
        Dagger.gpu_synchronize(proc)
    end
end

Dagger.to_scope(::Val{:cuda_gpu}, sc::NamedTuple) =
    Dagger.to_scope(Val{:cuda_gpus}(), merge(sc, (;cuda_gpus=[sc.cuda_gpu])))
Dagger.scope_key_precedence(::Val{:cuda_gpu}) = 1
function Dagger.to_scope(::Val{:cuda_gpus}, sc::NamedTuple)
    if haskey(sc, :worker)
        workers = Int[sc.worker]
    elseif haskey(sc, :workers) && sc.workers != Colon()
        workers = sc.workers
    else
        workers = map(gproc->gproc.pid, Dagger.procs(Dagger.Sch.eager_context()))
    end
    scopes = Dagger.ExactScope[]
    dev_ids = sc.cuda_gpus
    for worker in workers
        procs = Dagger.get_processors(Dagger.OSProc(worker))
        for proc in procs
            proc isa CuArrayDeviceProc || continue
            if dev_ids == Colon() || proc.device+1 in dev_ids
                scope = Dagger.ExactScope(proc)
                push!(scopes, scope)
            end
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:cuda_gpus}) = 1

const DEVICES = Dict{Int, CuDevice}()
const CONTEXTS = Dict{Int, CuContext}()
const STREAMS = Dict{Int, Vector{CuStream}}()
const SYNCDEPS = Dagger.LockedObject(Dict{Int, Tuple{Int,Int}}())

# Cross-stream sync events actually recorded — telemetry read by test/benchmark.jl.
const _EVENT_COUNT = Threads.Atomic{Int}(0)

function __init__()
    if haskey(ENV, "DAGGER_CUDA_STREAM_STRATEGY")
        stream_strategy!(Symbol(ENV["DAGGER_CUDA_STREAM_STRATEGY"]))
    end
    if CUDA.has_cuda()
        for dev in CUDA.devices()
            ROUNDROBIN[dev.handle] = Threads.Atomic{Int}(1)
            @debug "Registering CUDA GPU processor with Dagger: $dev"
            Dagger.add_processor_callback!("cuarray_device_$(dev.handle)") do
                proc = CuArrayDeviceProc(myid(), dev.handle, CUDA.uuid(dev))
                DEVICES[dev.handle] = dev
                ctx = context(dev)
                CONTEXTS[dev.handle] = ctx
                context!(ctx) do
                    # num_sm = Int(CUDA.attribute(dev, CUDA.DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT))
                    num_streams = 16
                    STREAMS[dev.handle] = [CuStream() for _ in 1:num_streams]
                    STREAM_INFLIGHT[dev.handle] = [CuEvent[] for _ in 1:num_streams]
                    STREAM_GEN[dev.handle] = zeros(UInt64, num_streams)
                    LAST_WAITED[dev.handle] = [Dict{Tuple{Int,Int}, UInt64}() for _ in 1:num_streams]
                end
                return proc
            end
        end
    end
end

end # module CUDAExt