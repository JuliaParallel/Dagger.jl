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
    # Synchronize the device before releasing the buffer to guard
    # against `unsafe_free!` racing with a concurrent `move!` copy-task
    # on the same buffer. See detailed rationale in the matching CUDA
    # implementation at `ext/CUDAExt.jl` — same class of Dagger
    # aliasing-machinery edge case, same defense at the correctness
    # boundary. `AMDGPU.synchronize()` waits for the current device's
    # queue to drain; fast no-op when nothing is pending.
    #
    # ROCExt does not define `with_context!(::ROCArray)` (CUDAExt does,
    # ext/CUDAExt.jl:92), so hop through the array's `MemorySpace` to
    # pick the correct device context before syncing.
    with_context(Dagger.memory_space(x)) do
        AMDGPU.synchronize()
    end
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
    old_stream = stream()

    with_context!(x, stream_idx)
    try
        f()
    finally
        context!(old_ctx)
        AMDGPU.device!(old_device)
        stream!(old_stream)
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    caller_stream = stream()
    with_context(x) do
        ev = record_event(stream())
        stream_wait_event(caller_stream, ev)
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
            record_event(stream())
        end
        return with_context(to_proc) do
            stream_wait_event(stream(), ev)
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
            record_event(stream())
        end
        return with_context(to_proc) do
            stream_wait_event(stream(), ev)
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
# `record_event` marks the given stream; `stream_wait_event` makes `waiting`
# defer until that mark completes. HIP equivalents of CUDA.record / CUDA.wait.
record_event(s::HIPStream) = AMDGPU.HIP.HIPEvent(s)  # constructor records on `s`
function stream_wait_event(waiting::HIPStream, ev)
    AMDGPU.HIP.hipStreamWaitEvent(waiting, ev, 0)
    return
end

const ROUNDROBIN = Dict{Int, Threads.Atomic{Int}}()
# Per-stream count of tasks assigned but not yet finished.
# ponytail: host-side occupancy, not true device queue depth; upgrade to
# HIPEvent polling if SDQ decisions look off in benchmarks.
const STREAM_QUEUES = Dict{Int, Vector{Threads.Atomic{Int}}}()
const STREAM_STRATEGY = Ref{Symbol}(:roundrobin)

"""
    stream_strategy!(s::Symbol)

Set the stream distribution strategy: `:roundrobin`, `:random`, or
`:sdq` (shortest stream queue). Also settable via the
`DAGGER_ROCM_STREAM_STRATEGY` environment variable at load time.
"""
function stream_strategy!(s::Symbol)
    s in (:roundrobin, :random, :sdq) ||
        throw(ArgumentError("unknown stream strategy: $s (use :roundrobin, :random, or :sdq)"))
    STREAM_STRATEGY[] = s
end

function pick_stream(dev::Int)
    n = length(STREAMS[dev])
    s = STREAM_STRATEGY[]
    if s == :roundrobin
        return mod1(Threads.atomic_add!(ROUNDROBIN[dev], 1), n)
    elseif s == :random
        return rand(1:n)
    else # :sdq
        return argmin(i -> STREAM_QUEUES[dev][i][], 1:n)
    end
end

# Task execution
function Dagger.execute!(proc::ROCArrayDeviceProc, f, args...; kwargs...)
    @nospecialize f args kwargs
    opt = Dagger.get_options()
    tls = Dagger.get_tls()
    mydev = proc.device_id
    cr_str = pick_stream(mydev)
    Threads.atomic_add!(STREAM_QUEUES[mydev][cr_str], 1)
    mytid = Dagger.task_id()
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        with_context!(proc, cr_str)
        lock(SYNCDEPS) do deps
            local_sync = Dagger._has_option(opt, :syncdeps) ? Dagger.get_options(:syncdeps) : nothing
            if !isnothing(local_sync)
                local_sync = map(syncdep -> syncdep.id.id, collect(local_sync))
                for syncdep in local_sync
                    (dev, stream_idx) = deps[syncdep]
                    ev = record_event(STREAMS[dev][stream_idx])
                    stream_wait_event(STREAMS[mydev][cr_str], ev)
                end
            end
            deps[mytid] = (mydev, cr_str)
        end

        result = try
            Base.@invokelatest f(args...; kwargs...)
        finally
            Threads.atomic_sub!(STREAM_QUEUES[mydev][cr_str], 1)
        end
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
                # Companion to the `move` above: the scheduler's cost lookup
                # needs the same CPU->GPU function mapping, but at the type
                # level and without a value to dispatch on.
                @eval Dagger._translate_fn_for(::Type{$(typeof(fn))}, ::ROCArrayDeviceProc) = $(typeof(rocfn))
            end
        end
    end
end

# Array-type half of the signature translation; mirrors CUDAExt.
_translate_type_for(::Type{Matrix{T}}) where T = ROCArray{T,2}
_translate_type_for(::Type{Vector{T}}) where T = ROCArray{T,1}
_translate_type_for(::Type{Array{T,N}}) where {T,N} = ROCArray{T,N}
# Scalars and other non-array arguments cross unchanged.
_translate_type_for(::Type{T}) where {T<:Union{Number,Char,Symbol,Function}} = T
_translate_type_for(::Type) = nothing

function Dagger._translate_sig_for(sig::Vector, proc::ROCArrayDeviceProc)
    isempty(sig) && return nothing
    out = Vector{Any}(undef, length(sig))
    fn = Dagger._translate_fn_for(sig[1], proc)
    fn === nothing && return nothing
    out[1] = fn
    for i in 2:length(sig)
        t = sig[i]
        t isa Type || return nothing
        mapped = _translate_type_for(t)
        mapped === nothing && return nothing
        out[i] = mapped
    end
    return out
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
            ev = record_event(proc_stream)
            stream_wait_event(user_stream, ev)
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

# See `Dagger.proc_concurrency` — ROCm equivalent of the CUDA stream count.
function Dagger.proc_concurrency(proc::ROCArrayDeviceProc)
    streams = get(STREAMS, proc.device_id, nothing)
    return streams === nothing ? 1 : max(1, length(streams))
end
const SYNCDEPS = Dagger.LockedObject(Dict{Int, Tuple{Int,Int}}())

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
                    STREAM_QUEUES[dev.device_id] = [Threads.Atomic{Int}(0) for _ in 1:num_streams]
                end
                return proc
            end
        end
    end
end

end # module ROCExt
