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
        # We don't track which round-robin stream produced this data in the
        # move path, so make the caller wait on *every* stream of the device.
        # Recording on `stream()` alone only ever caught STREAMS[dev][1], which
        # is almost always idle -> the wait was a no-op and the copy raced the
        # real producer stream.
        for s in STREAMS[x.device]
            s === caller_stream && continue
            ev = CUDA.CuEvent()
            CUDA.record(ev, s)
            CUDA.wait(ev, caller_stream)
        end
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
    if Dagger.root_worker_id(from_space) == myid()
        sync_with_context(from_space)
        with_context!(from_space)
    end
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
    sync_with_context(from_space)
    with_context!(to_space)
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
            ev = CUDA.CuEvent()
            CUDA.record(ev, stream())
            return ev
        end
        
        return with_context(to_proc) do
            CUDA.wait(ev, stream()) 
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
            ev = CUDA.CuEvent()
            CUDA.record(ev, stream())
            return ev
        end
        
        return with_context(to_proc) do
            CUDA.wait(ev, stream())
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

# Task execution
function Dagger.execute!(proc::CuArrayDeviceProc, f, args...; kwargs...)
    @nospecialize f args kwargs
    opt = Dagger.get_options()
    tls = Dagger.get_tls()
    mydev = proc.device
    cr_str = mod1(Threads.atomic_add!(ROUNDROBIN[mydev], 1), length(STREAMS[mydev]))
    mytid = Dagger.task_id()
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        with_context!(proc, cr_str)
        lock(SYNCDEPS) do deps
            local_sync = Dagger._has_option(opt, :syncdeps) ? Dagger.get_options(:syncdeps) : nothing
            if !isnothing(local_sync)
                local_sync = map(syncdep -> syncdep.id.id, collect(local_sync))
                for syncdep in local_sync
                    (dev, stream) = deps[syncdep]
                    ev = CUDA.CuEvent()
                    CUDA.record(ev, STREAMS[dev][stream])
                    CUDA.wait(ev, STREAMS[mydev][cr_str]) #cr_str is an Int not a custream            
                end
            end
            deps[mytid] = (mydev, cr_str)
        end
        
        result = Base.@invokelatest f(args...; kwargs...)
        # Block this task's thread until *its* stream has actually finished.
        # This is the backpressure that bounds memory: the scheduler only
        # treats the task as done (and frees its input chunks via unsafe_free!)
        # once the producing kernel has completed, so freed buffers are no
        # longer pinned on a still-running stream. Concurrency is preserved
        # because other tasks run concurrently on their own streams/threads.
        # CUDA.synchronize(STREAMS[mydev][cr_str])
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
    with_context(proc) do
        # Host-blocking sync. This is a barrier: callers (e.g. reclaim/GC paths)
        # rely on all GPU work being *finished* on return, not merely chained on
        # the host stream via a device-side CUDA.wait (which never blocks the
        # host and let reclaim() run while kernels were still in flight).
        for proc_stream in STREAMS[proc.device]
            CUDA.synchronize(proc_stream)
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

function __init__()
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
                    num_sm = 4
                    #Int(CUDA.attribute(dev, CUDA.DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT))
                    num_streams =  num_sm
                    STREAMS[dev.handle] = [CuStream() for _ in 1:num_streams]
                end
                return proc
            end
        end
    end
end

end # module CUDAExt