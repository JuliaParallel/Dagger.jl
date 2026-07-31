module ROCExt

export ROCArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap
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
import AMDGPU: devices, context, context!, stream, stream!
import AMDGPU: rocBLAS, rocSOLVER

struct ROCArrayDeviceProc <: Dagger.Processor
    owner::Int
    device_id::Int
end
Dagger.get_parent(proc::ROCArrayDeviceProc) = Dagger.OSProc(proc.owner)
Dagger.root_worker_id(proc::ROCArrayDeviceProc) = proc.owner
Base.show(io::IO, proc::ROCArrayDeviceProc) =
    print(io, "ROCArrayDeviceProc(worker $(proc.owner), device $(proc.device_id))")
Dagger.short_name(proc::ROCArrayDeviceProc) = "W: $(proc.owner), ROCm: $(proc.device_id)"
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
    gpu_ptr = pointer(x)
    rptr = Dagger.RemotePtr{Cvoid}(UInt64(gpu_ptr), space)
    return Dagger.ContiguousAliasing(Dagger.MemorySpan{S}(rptr, sizeof(T)*length(x)))
end

function Dagger.unsafe_free!(x::ROCArray)
    AMDGPU.unsafe_free!(x)
    return
end

Dagger.memory_spaces(proc::ROCArrayDeviceProc) = Set([ROCVRAMMemorySpace(proc.owner, proc.device_id)])
Dagger.processors(space::ROCVRAMMemorySpace) = Set([ROCArrayDeviceProc(space.owner, space.device_id)])

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

function with_context!(handle::Integer, stream_idx = 1)
    context!(CONTEXTS[handle])
    AMDGPU.device!(DEVICES[handle])
    stream!(STREAMS[handle][stream_idx])
end
function with_context!(proc::ROCArrayDeviceProc, stream_idx = 1)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device_id, stream_idx)
end
function with_context!(space::ROCVRAMMemorySpace, stream_idx = 1)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device_id, stream_idx)
end
Dagger.with_context!(proc::ROCArrayDeviceProc) = with_context!(proc)
Dagger.with_context!(space::ROCVRAMMemorySpace) = with_context!(space)
function with_context(f, x, stream_idx = 1)
    old_ctx = context()
    old_device = AMDGPU.device()
    # N.B. Never call `AMDGPU.stream()` here: it lazily creates a task-local HIPStream
    # (~8 MiB of host RAM, freed only when the task is GC'd) for any task that doesn't
    # already have one. Dagger runs one Julia task per Dagger task, so that is one fresh
    # stream per task and tens of GB of host RAM for a large datadeps DAG.
    old_stream = AMDGPU.task_local_state().streams[old_device.device_id]

    with_context!(x, stream_idx)
    try
        f()
    finally
        context!(old_ctx)
        AMDGPU.device!(old_device)
        old_stream === nothing || stream!(old_stream)
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    caller_stream = stream()
    with_context(x) do
        ev = acquire_event!()
        record_on!(ev, stream())
        stream_wait_event(caller_stream, ev)
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
    copyto!(to, from)
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::ROCVRAMMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    copyto!(to, from)
    return
end
function Dagger.move!(to_space::ROCVRAMMemorySpace, from_space::ROCVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    sync_with_context(from_space)
    with_context!(to_space)
    copyto!(to, from)
    return
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
        _x = adapt(Array, x)
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
        _x = Array{T,N}(undef, size(x))
        copyto!(_x, x)
        AMDGPU.synchronize()
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::ROCArrayDeviceProc, x::Dagger.Chunk{T}) where T<:ROCArray
    if from_proc == to_proc
        # Same process and GPU, no change.
        # Stream ordering (via syncdeps in execute!) guarantees safety; no sync needed.
        return unwrap(x)
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy.
        # Chain the copy behind the producer stream via a cross-stream event
        # instead of host-blocking, so other streams keep running.
        from_arr = unwrap(x)
        ev = with_context(from_proc) do
            ev = acquire_event!()
            record_on!(ev, stream())
            return ev
        end
        return with_context(to_proc) do
            stream_wait_event(stream(), ev)
            park_event!(ev)
            to_arr = similar(from_arr)
            copyto!(to_arr, from_arr)
            return to_arr
        end
    else
        # Different node, use DtoH, serialization, HtoD
        host_copy = remotecall_fetch(from_proc.owner, from_proc, x) do from_proc, x
            return with_context(from_proc) do
                Array(unwrap(x))
            end
        end
        return with_context(to_proc) do
            return ROCArray(host_copy)
        end
    end
end

function Dagger.move(from_proc::ROCArrayDeviceProc, to_proc::ROCArrayDeviceProc, x::ROCArray)
    if from_proc == to_proc
        # Stream ordering (via syncdeps in execute!) guarantees safety; no sync needed.
        return x
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        ev = with_context(from_proc) do
            ev = acquire_event!()
            record_on!(ev, stream())
            return ev
        end
        return with_context(to_proc) do
            stream_wait_event(stream(), ev)
            park_event!(ev)
            to_arr = similar(x)
            copyto!(to_arr, x)
            return to_arr
        end
    else
        host_copy = remotecall_fetch(from_proc.owner, from_proc, x) do from_proc, x
            return with_context(from_proc) do
                Array(unwrap(x))
            end
        end
        return with_context(to_proc) do
            return ROCArray(host_copy)
        end
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

# Cross-stream synchronization helpers (device-side, non host-blocking).
# `record_on!` marks the given stream; `stream_wait_event` makes `waiting`
# defer until that mark completes. HIP equivalents of CUDA.record / CUDA.wait.
function record_on!(ev::AMDGPU.HIP.HIPEvent, s::HIPStream)
    ev.stream = s.stream
    AMDGPU.HIP.record(ev)
    return ev
end
function stream_wait_event(waiting::HIPStream, ev)
    AMDGPU.HIP.hipStreamWaitEvent(waiting, ev, 0)
    return
end

const ROUNDROBIN = Dict{Int, Threads.Atomic{Int}}()
# Per-stream queues of completion HIPEvents — device occupancy for :sdq / :locality.
const STREAM_INFLIGHT = Dict{Int, Vector{Vector{AMDGPU.HIP.HIPEvent}}}()
const INFLIGHT_LOCK = ReentrantLock()
const STREAM_STRATEGY = Ref{Symbol}(:roundrobin)
const STREAM_GEN = Dict{Int, Vector{UInt64}}()
const LAST_WAITED = Dict{Int, Vector{Dict{Tuple{Int,Int}, UInt64}}}()

# Pooled HIPEvents (DisableTiming). Park until isdone before reuse.
const EVENT_POOL_LOCK = ReentrantLock()
const EVENT_FREE = AMDGPU.HIP.HIPEvent[]
const EVENT_PENDING = AMDGPU.HIP.HIPEvent[]

# Dummy stream handle for pooled events before first record_on! (updated per use).
_new_event() = AMDGPU.HIP.HIPEvent(HIPStream().stream; do_record=false, timing=false)

function _drain_pending_events_unlocked!()
    i = 1
    while i <= length(EVENT_PENDING)
        if AMDGPU.HIP.isdone(EVENT_PENDING[i])
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

function park_event!(ev::AMDGPU.HIP.HIPEvent)
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
`DAGGER_ROCM_STREAM_STRATEGY` environment variable at load time.
"""
function stream_strategy!(s::Symbol)
    s in (:roundrobin, :random, :sdq, :locality) ||
        throw(ArgumentError("unknown stream strategy: $s (use :roundrobin, :random, :sdq, or :locality)"))
    STREAM_STRATEGY[] = s
end

const LOCALITY_SLACK = Ref(8)

function stream_depth(dev::Int, i::Int)
    done = AMDGPU.HIP.HIPEvent[]
    depth = lock(INFLIGHT_LOCK) do
        q = STREAM_INFLIGHT[dev][i]
        while !isempty(q) && AMDGPU.HIP.isdone(q[1])
            push!(done, popfirst!(q))
        end
        length(q)
    end
    for ev in done
        park_event!(ev)
    end
    return depth
end

function stream_depths!(dev::Int)
    n = length(STREAM_INFLIGHT[dev])
    done = AMDGPU.HIP.HIPEvent[]
    depths = lock(INFLIGHT_LOCK) do
        ds = Vector{Int}(undef, n)
        for i in 1:n
            q = STREAM_INFLIGHT[dev][i]
            while !isempty(q) && AMDGPU.HIP.isdone(q[1])
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

function sdq_stream(dev::Int, n::Int)
    depths = stream_depths!(dev)
    return argmin(depths)
end

function mark_stream_complete!(dev::Int, s::Int)
    ev = acquire_event!()
    record_on!(ev, STREAMS[dev][s])
    lock(INFLIGHT_LOCK) do
        push!(STREAM_INFLIGHT[dev][s], ev)
    end
    return
end

function producer_uid(s)
    s.id !== nothing && return s.id.id
    return Dagger.unwrap_weak(s).id
end

function locality_stream(dev::Int, local_sync, deps)
    n = length(STREAMS[dev])
    local_sync === nothing && return sdq_stream(dev, n)
    counts = nothing
    for syncdep in local_sync
        entry = get(deps, producer_uid(syncdep), nothing)
        entry === nothing && continue
        (sdev, sstr) = entry
        sdev == dev || continue
        counts === nothing && (counts = zeros(Int, n))
        counts[sstr] += 1
    end
    counts === nothing && return sdq_stream(dev, n)
    depths = stream_depths!(dev)
    maxc = maximum(counts)
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

function collect_wait_list!(wait_list::Vector{Tuple{Int,Int}},
                            mydev::Int, s::Int, local_sync, deps)
    empty!(wait_list)
    local_sync === nothing && return wait_list
    seen = Set{Tuple{Int,Int}}()
    lw = LAST_WAITED[mydev][s]
    for syncdep in local_sync
        entry = get(deps, producer_uid(syncdep), nothing)
        isnothing(entry) && continue
        (dev, stream_idx) = entry
        (dev == mydev && stream_idx == s) && continue
        key = (dev, stream_idx)
        key in seen && continue
        gen = STREAM_GEN[dev][stream_idx]
        get(lw, key, UInt64(0)) >= gen && continue
        push!(seen, key)
        push!(wait_list, key)
    end
    return wait_list
end

function issue_cross_stream_waits!(wait_list::Vector{Tuple{Int,Int}}, mydev::Int, s::Int)
    for (dev, stream_idx) in wait_list
        Threads.atomic_add!(_EVENT_COUNT, 1)
        ev = acquire_event!()
        record_on!(ev, STREAMS[dev][stream_idx])
        stream_wait_event(STREAMS[mydev][s], ev)
        park_event!(ev)
        lock(SYNCDEPS) do _
            LAST_WAITED[mydev][s][(dev, stream_idx)] = STREAM_GEN[dev][stream_idx]
        end
    end
    return
end

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
    retired = AMDGPU.HIP.HIPEvent[]
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
Dagger.clear_gpu_stream_syncdeps!(::Val{:ROC}) = clear_stream_syncdeps!()

# Task execution
function Dagger.execute!(proc::ROCArrayDeviceProc, f, args...; kwargs...)
    @nospecialize f args kwargs
    tls = Dagger.get_tls()
    mydev = proc.device_id
    mytid = Dagger.task_id()
    # N.B. `Dagger.get_options()` only carries the *propagated* options (those
    # named in `options.propagates`, empty by default), so it never holds
    # :syncdeps — reading it there silently disabled all cross-stream sync. The
    # real set lives on the TLS task spec.
    local_sync = tls.task_spec.options.syncdeps
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        wait_list = Tuple{Int,Int}[]
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
    finally
        # AMDGPU caches a rocBLAS handle in task-local storage and only returns it to the
        # idle pool from a finalizer on the Julia task. One task per Dagger task means
        # hundreds of live handles per datadeps DAG (and, with a non-zero
        # ROCBLAS_DEVICE_MEMORY_SIZE, a workspace each until VRAM runs out). Run that
        # finalizer now that the task is done so the handles get recycled: measured
        # 380 live handles -> 4.
        finalize(task)
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
                     H.halo_width)
Adapt.adapt_structure(to::AMDGPU.Runtime.Adaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.halos),
                     H.halo_width)
function Dagger.inner_stencil_proc!(::ROCArrayDeviceProc, f, output, read_vars)
    Dagger.Kernel(_inner_stencil!)(f, output, read_vars; ndrange=size(output))
    return
end
@kernel function _inner_stencil!(f, output, read_vars)
    idx = @index(Global, Cartesian)
    f(idx, output, read_vars)
end

Dagger.gpu_processor(::Val{:ROC}) = ROCArrayDeviceProc
Dagger.gpu_can_compute(::Val{:ROC}) = AMDGPU.functional()
Dagger.gpu_kernel_backend(proc::ROCArrayDeviceProc) = ROCBackend()
Dagger.gpu_with_device(f, proc::ROCArrayDeviceProc) =
    AMDGPU.device!(f, AMDGPU.devices()[proc.device_id])
function Dagger.gpu_synchronize(proc::ROCArrayDeviceProc)
    @assert !Dagger.in_task()
    user_stream = stream()
    with_context(proc) do
        for proc_stream in STREAMS[proc.device_id]
            ev = acquire_event!()
            record_on!(ev, proc_stream)
            stream_wait_event(user_stream, ev)
            park_event!(ev)
        end
    end
end
function Dagger.gpu_synchronize(::Val{:ROC})
    for dev in AMDGPU.devices()
        Dagger.gpu_synchronize(ROCArrayDeviceProc(myid(), dev.device_id))
    end
end

Dagger.to_scope(::Val{:rocm_gpu}, sc::NamedTuple) =
    Dagger.to_scope(Val{:rocm_gpus}(), merge(sc, (;rocm_gpus=[sc.rocm_gpu])))
function Dagger.to_scope(::Val{:rocm_gpus}, sc::NamedTuple)
    if haskey(sc, :worker)
        workers = Int[sc.worker]
    elseif haskey(sc, :workers) && sc.workers != Colon()
        workers = sc.workers
    else
        workers = map(gproc->gproc.pid, Dagger.procs(Dagger.Sch.eager_context()))
    end
    scopes = Dagger.ExactScope[]
    dev_ids = sc.rocm_gpus
    for worker in workers
        procs = Dagger.get_processors(Dagger.OSProc(worker))
        for proc in procs
            proc isa ROCArrayDeviceProc || continue
            if dev_ids == Colon() || proc.device_id in dev_ids
                scope = Dagger.ExactScope(proc)
                push!(scopes, scope)
            end
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:rocm_gpu}) = 2
Dagger.scope_key_precedence(::Val{:rocm_gpus}) = 1

const DEVICES = Dict{Int, HIPDevice}()
const CONTEXTS = Dict{Int, HIPContext}()
const STREAMS = Dict{Int, Vector{HIPStream}}()
const SYNCDEPS = Dagger.LockedObject(Dict{Int, Tuple{Int,Int}}())

# Cross-stream sync events actually recorded — telemetry read by test/benchmark.jl.
const _EVENT_COUNT = Threads.Atomic{Int}(0)

function __init__()
    if haskey(ENV, "DAGGER_ROCM_STREAM_STRATEGY")
        stream_strategy!(Symbol(ENV["DAGGER_ROCM_STREAM_STRATEGY"]))
    end
    if AMDGPU.functional()
        for device_id in 1:length(AMDGPU.devices())
            dev = AMDGPU.devices()[device_id]
            ROUNDROBIN[dev.device_id] = Threads.Atomic{Int}(1)
            @debug "Registering ROCm GPU processor with Dagger: $dev"
            Dagger.add_processor_callback!("rocarray_device_$device_id") do
                proc = ROCArrayDeviceProc(myid(), device_id)
                DEVICES[dev.device_id] = dev
                ctx = HIPContext(dev)
                CONTEXTS[dev.device_id] = ctx
                context!(ctx) do
                    num_streams = 8
                    STREAMS[dev.device_id] = [HIPStream() for _ in 1:num_streams]
                    STREAM_INFLIGHT[dev.device_id] = [AMDGPU.HIP.HIPEvent[] for _ in 1:num_streams]
                    STREAM_GEN[dev.device_id] = zeros(UInt64, num_streams)
                    LAST_WAITED[dev.device_id] = [Dict{Tuple{Int,Int}, UInt64}() for _ in 1:num_streams]
                end
                return proc
            end
        end
    end
end

end # module ROCExt
