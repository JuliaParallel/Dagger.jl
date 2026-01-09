module IntelExt

export oneArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap
import MemPool: DRef, poolget
import Distributed: myid, remotecall_fetch
import LinearAlgebra
using KernelAbstractions, Adapt

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

if isdefined(Base, :get_extension)
    import oneAPI
else
    import ..oneAPI
end
import oneAPI: ZeDevice, ZeDriver, ZeContext, oneArray, oneAPIBackend
import oneAPI: driver, driver!, device, device!, context, context!
#import oneAPI: CUBLAS, CUSOLVER

using UUIDs

"Represents a single Intel GPU device."
struct oneArrayDeviceProc <: Dagger.Processor
    owner::Int
    device_id::Int
end
Dagger.get_parent(proc::oneArrayDeviceProc) = Dagger.OSProc(proc.owner)
Dagger.root_worker_id(proc::oneArrayDeviceProc) = proc.owner
Base.show(io::IO, proc::oneArrayDeviceProc) =
    print(io, "oneArrayDeviceProc(worker $(proc.owner), device $(proc.device_id))")
Dagger.short_name(proc::oneArrayDeviceProc) = "W: $(proc.owner), oneAPI: $(proc.device)"
Dagger.@gpuproc(oneArrayDeviceProc, oneArray)

"Represents the memory space of a single Intel GPU's VRAM."
struct IntelVRAMMemorySpace <: Dagger.MemorySpace
    owner::Int
    device_id::Int
end
Dagger.root_worker_id(space::IntelVRAMMemorySpace) = space.owner
function Dagger.memory_space(x::oneArray)
    dev = oneAPI.device(x)
    device_id = _device_id(dev)
    return IntelVRAMMemorySpace(myid(), device_id)
end
_device_id(dev::ZeDevice) = findfirst(other_dev->other_dev === dev, collect(oneAPI.devices()))
function Dagger.aliasing(x::oneArray{T}) where T
    space = Dagger.memory_space(x)
    S = typeof(space)
    gpu_ptr = pointer(x)
    rptr = Dagger.RemotePtr{Cvoid}(UInt64(gpu_ptr), space)
    return Dagger.ContiguousAliasing(Dagger.MemorySpan{S}(rptr, sizeof(T)*length(x)))
end

Dagger.memory_spaces(proc::oneArrayDeviceProc) = Set([IntelVRAMMemorySpace(proc.owner, proc.device_id)])
Dagger.processors(space::IntelVRAMMemorySpace) = Set([oneArrayDeviceProc(space.owner, space.device_id)])

function to_device(proc::oneArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return DEVICES[proc.device_id]
end

function with_context!(device_id::Integer)
    driver!(DRIVERS[device_id])
    device!(DEVICES[device_id])
    context!(CONTEXTS[device_id])
end
function with_context!(proc::oneArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device_id)
end
function with_context!(space::IntelVRAMMemorySpace)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device_id)
end
Dagger.with_context!(proc::oneArrayDeviceProc) = with_context!(proc)
Dagger.with_context!(space::IntelVRAMMemorySpace) = with_context!(space)
function with_context(f, x)
    old_drv = driver()
    old_dev = device()
    old_ctx = context()

    with_context!(x)
    try
        f()
    finally
        driver!(old_drv)
        device!(old_dev)
        context!(old_ctx)
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    with_context(x) do
        oneAPI.synchronize()
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
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(rand)) = oneAPI.rand
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(randn)) = oneAPI.randn
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(ones)) = oneAPI.ones
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(zeros)) = oneAPI.zeros
struct AllocateUndef{S} end
(::AllocateUndef{S})(T, dims::Dims{N}) where {S,N} = oneArray{S,N}(undef, dims)
Dagger.allocate_array_func(::oneArrayDeviceProc, ::Dagger.AllocateUndef{S}) where S = AllocateUndef{S}()

# In-place
# N.B. These methods assume that later operations will implicitly or
# explicitly synchronize with their associated stream
function Dagger.move!(to_space::Dagger.CPURAMMemorySpace, from_space::IntelVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if Dagger.root_worker_id(from_space) == myid()
        sync_with_context(from_space)
        with_context!(from_space)
    end
    copyto!(to, from)
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::IntelVRAMMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    copyto!(to, from)
    return
end
function Dagger.move!(to_space::IntelVRAMMemorySpace, from_space::IntelVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    sync_with_context(from_space)
    with_context!(to_space)
    copyto!(to, from)
    return
end

# Out-of-place HtoD
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x)
    with_context(to_proc) do
        arr = adapt(oneArray, x)
        oneAPI.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    cpu_data = remotecall_fetch(unwrap, from_w, x)
    with_context(to_proc) do
        arr = adapt(oneArray, cpu_data)
        oneAPI.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::oneArray)
    if oneAPI.device(x) == to_device(to_proc)
        return x
    end
    with_context(to_proc) do
        _x = similar(x)
        copyto!(_x, x)
        oneAPI.synchronize()
        return _x
    end
end

# Out-of-place DtoH
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        oneAPI.synchronize()
        _x = adapt(Array, x)
        oneAPI.synchronize()
        return _x
    end
end
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    remotecall_fetch(from_w, x) do x
        arr = unwrap(x)
        return Dagger.move(from_proc, to_proc, arr)
    end
end
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x::oneArray{T,N}) where {T,N}
    with_context(from_proc) do
        oneAPI.synchronize()
        _x = Array{T,N}(undef, size(x))
        copyto!(_x, x)
        oneAPI.synchronize()
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::oneArrayDeviceProc, x::Dagger.Chunk{T}) where T<:oneArray
    if from_proc == to_proc
        # Same process and GPU, no change
        arr = unwrap(x)
        with_context(oneAPI.synchronize, from_proc)
        return arr
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy
        from_arr = unwrap(x)
        with_context(oneAPI.synchronize, from_proc)
        return with_context(to_proc) do
            to_arr = similar(from_arr)
            copyto!(to_arr, from_arr)
            oneAPI.synchronize()
            return to_arr
        end
    else
        # Different node, use DtoH, serialization, HtoD
        return oneArray(remotecall_fetch(from_proc.owner, x) do x
            Array(unwrap(x))
        end)
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

#= FIXME: Adapt BLAS/LAPACK functions
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
                @eval Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, ::$(typeof(fn))) = $cufn
            end
        end
    end
end
=#

# Task execution
function Dagger.execute!(proc::oneArrayDeviceProc, f, args...; kwargs...)
    @nospecialize f args kwargs
    tls = Dagger.get_tls()
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        with_context!(proc)
        result = Base.@invokelatest f(args...; kwargs...)
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

oneArray(H::Dagger.HaloArray) = convert(oneArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:oneArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.edges),
                     C.(H.corners),
                     H.halo_width)
Adapt.adapt_structure(to::oneAPI.KernelAdaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.edges),
                     adapt.(Ref(to), H.corners),
                     H.halo_width)
function Dagger.inner_stencil_proc!(::oneArrayDeviceProc, f, output, read_vars)
    Dagger.Kernel(_inner_stencil!)(f, output, read_vars; ndrange=size(output))
    return
end
@kernel function _inner_stencil!(f, output, read_vars)
    idx = @index(Global, Cartesian)
    f(idx, output, read_vars)
end

Dagger.gpu_processor(::Val{:oneAPI}) = oneArrayDeviceProc
Dagger.gpu_can_compute(::Val{:oneAPI}) = oneAPI.functional()
Dagger.gpu_kernel_backend(::oneArrayDeviceProc) = oneAPIBackend()
Dagger.gpu_with_device(f, proc::oneArrayDeviceProc) =
    device!(f, proc.device_id)
function Dagger.gpu_synchronize(proc::oneArrayDeviceProc)
    with_context(proc) do
        oneAPI.synchronize()
    end
end
function Dagger.gpu_synchronize(::Val{:oneAPI})
    for dev in oneAPI.devices()
        idx = findfirst(other_dev->other_dev == dev, collect(oneAPI.devices()))
        _sync_with_context(oneArrayDeviceProc(myid(), idx))
    end
end

Dagger.to_scope(::Val{:intel_gpu}, sc::NamedTuple) =
    Dagger.to_scope(Val{:intel_gpus}(), merge(sc, (;intel_gpus=[sc.intel_gpu])))
Dagger.scope_key_precedence(::Val{:intel_gpu}) = 1
function Dagger.to_scope(::Val{:intel_gpus}, sc::NamedTuple)
    if haskey(sc, :worker)
        workers = Int[sc.worker]
    elseif haskey(sc, :workers) && sc.workers != Colon()
        workers = sc.workers
    else
        workers = map(gproc->gproc.pid, Dagger.procs(Dagger.Sch.eager_context()))
    end
    scopes = Dagger.ExactScope[]
    dev_ids = sc.intel_gpus
    for worker in workers
        procs = Dagger.get_processors(Dagger.OSProc(worker))
        for proc in procs
            proc isa oneArrayDeviceProc || continue
            if dev_ids == Colon() || proc.device_id in dev_ids
                scope = Dagger.ExactScope(proc)
                push!(scopes, scope)
            end
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:intel_gpus}) = 1

const DEVICES = Dict{Int, ZeDevice}()
const DRIVERS = Dict{Int, ZeDriver}()
const CONTEXTS = Dict{Int, ZeContext}()

function __init__()
    if oneAPI.functional()
        for (device_id, dev) in enumerate(oneAPI.devices())
            @debug "Registering Intel GPU processor with Dagger: $dev"
            Dagger.add_processor_callback!("zearray_device_$(device_id)") do
                proc = oneArrayDeviceProc(myid(), device_id)
                DEVICES[device_id] = dev
                driver!(dev.driver)
                DRIVERS[device_id] = dev.driver
                device!(dev)
                ctx = ZeContext(dev.driver)
                CONTEXTS[device_id] = ctx
                return proc
            end
        end
    end
end

end # module IntelExt
