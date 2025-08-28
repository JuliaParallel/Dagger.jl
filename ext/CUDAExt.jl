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
import CUDA: CuDevice, CuContext, CuStream, CuArray, CUDABackend
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

function with_context!(handle::Integer)
    context!(CONTEXTS[handle])
    stream!(STREAMS[handle])
end
function with_context!(proc::CuArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device)
end
function with_context!(space::CUDAVRAMMemorySpace)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device)
end
function with_context(f, x)
    old_ctx = context()
    old_stream = stream()

    with_context!(x)
    try
        f()
    finally
        context!(old_ctx)
        stream!(old_stream)
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    with_context(x) do
        CUDA.synchronize()
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
        CUDA.synchronize()
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
        CUDA.synchronize()
        return _x
    end
end

# Out-of-place DtoH
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        CUDA.synchronize()
        _x = adapt(Array, x)
        CUDA.synchronize()
        return _x
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
        CUDA.synchronize()
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc, x::Dagger.Chunk{T}) where T<:CuArray
    if from_proc == to_proc
        # Same process and GPU, no change
        arr = unwrap(x)
        with_context(CUDA.synchronize, from_proc)
        return arr
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy
        from_arr = unwrap(x)
        with_context(CUDA.synchronize, from_proc)
        return with_context(to_proc) do
            to_arr = similar(from_arr)
            copyto!(to_arr, from_arr)
            CUDA.synchronize()
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
        return CuArray(remotecall_fetch(from_proc.owner, x) do x
            Array(unwrap(x))
        end)
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

# Task execution
function Dagger.execute!(proc::CuArrayDeviceProc, world::UInt64, f, args...; kwargs...)
    @nospecialize f args kwargs
    tls = Dagger.get_tls()
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        with_context!(proc)
        result = Base.invoke_in_world(world, f, args...; kwargs...)
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
for lib in [BLAS, LAPACK]
    for name in names(lib; all=true)
        name == nameof(lib) && continue
        startswith(string(name), '#') && continue
        endswith(string(name), '!') || continue

        for culib in [CUBLAS, CUSOLVER]
            if name in names(culib; all=true)
                fn = getproperty(lib, name)
                cufn = getproperty(culib, name)
                @eval Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, ::$(typeof(fn))) = $cufn
            end
        end
    end
end

CuArray(H::Dagger.HaloArray) = convert(CuArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:CuArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.edges),
                     C.(H.corners),
                     H.halo_width)
Adapt.adapt_structure(to::CUDA.KernelAdaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.edges),
                     adapt.(Ref(to), H.corners),
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
    with_context(proc) do
        CUDA.synchronize()
    end
end
function Dagger.gpu_synchronize(::Val{:CUDA})
    for dev in CUDA.devices()
        _sync_with_context(CuArrayDeviceProc(myid(), dev.handle, CUDA.uuid(dev)))
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
const STREAMS = Dict{Int, CuStream}()

function __init__()
    if CUDA.has_cuda()
        for dev in CUDA.devices()
            @debug "Registering CUDA GPU processor with Dagger: $dev"
            Dagger.add_processor_callback!("cuarray_device_$(dev.handle)") do
                proc = CuArrayDeviceProc(myid(), dev.handle, CUDA.uuid(dev))
                DEVICES[dev.handle] = dev
                ctx = context(dev)
                CONTEXTS[dev.handle] = ctx
                context!(ctx) do
                    STREAMS[dev.handle] = stream()
                end
                return proc
            end
        end
    end
end

end # module CUDAExt
