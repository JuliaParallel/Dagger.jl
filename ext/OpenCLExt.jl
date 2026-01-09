module OpenCLExt

export CLArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap
import MemPool: DRef, poolget
import Distributed: myid, remotecall_fetch
import LinearAlgebra
using KernelAbstractions, Adapt

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

if isdefined(Base, :get_extension)
    import OpenCL
else
    import ..OpenCL
end
import OpenCL: CLArray, OpenCLBackend, cl
import .cl: Device, Context, CmdQueue

using UUIDs

"Represents a single OpenCL device."
struct CLArrayDeviceProc <: Dagger.Processor
    owner::Int
    device::Int
end
Dagger.get_parent(proc::CLArrayDeviceProc) = Dagger.OSProc(proc.owner)
Dagger.root_worker_id(proc::CLArrayDeviceProc) = proc.owner
Base.show(io::IO, proc::CLArrayDeviceProc) =
    print(io, "CLArrayDeviceProc(worker $(proc.owner), device $(proc.device))")
Dagger.short_name(proc::CLArrayDeviceProc) = "W: $(proc.owner), CL: $(proc.device)"
Dagger.@gpuproc(CLArrayDeviceProc, CLArray)

"Represents the memory space of a single OpenCL device's RAM."
struct CLMemorySpace <: Dagger.MemorySpace
    owner::Int
    device::Int
end
Dagger.root_worker_id(space::CLMemorySpace) = space.owner
function Dagger.memory_space(x::CLArray)
    queue = x.data[].queue
    idx = findfirst(==(queue), QUEUES)
    return CLMemorySpace(myid(), idx)
end
function Dagger.aliasing(x::CLArray{T}) where T
    space = Dagger.memory_space(x)
    S = typeof(space)
    gpu_ptr = pointer(x)
    rptr = Dagger.RemotePtr{Cvoid}(UInt64(gpu_ptr), space)
    return Dagger.ContiguousAliasing(Dagger.MemorySpan{S}(rptr, sizeof(T)*length(x)))
end

Dagger.memory_spaces(proc::CLArrayDeviceProc) = Set([CLMemorySpace(proc.owner, proc.device)])
Dagger.processors(space::CLMemorySpace) = Set([CLArrayDeviceProc(space.owner, space.device)])

function to_device(proc::CLArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return DEVICES[proc.device]
end
function to_context(proc::CLArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return CONTEXTS[proc.device]
end
to_context(handle::Integer) = CONTEXTS[handle]
to_context(dev::Device) = to_context(dev.handle)

function with_context!(handle::Integer)
    cl.context!(CONTEXTS[handle])
    cl.queue!(QUEUES[handle])
end
function with_context!(proc::CLArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device)
end
function with_context!(space::CLMemorySpace)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device)
end
Dagger.with_context!(proc::CLArrayDeviceProc) = with_context!(proc)
Dagger.with_context!(space::CLMemorySpace) = with_context!(space)
function with_context(f, x)
    old_ctx = cl.context()
    old_queue = cl.queue()

    with_context!(x)
    try
        f()
    finally
        cl.context!(old_ctx)
        cl.queue!(old_queue)
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    with_context(x) do
        cl.finish(cl.queue())
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
Dagger.allocate_array_func(::CLArrayDeviceProc, ::typeof(rand)) = OpenCL.rand
Dagger.allocate_array_func(::CLArrayDeviceProc, ::typeof(randn)) = OpenCL.randn
Dagger.allocate_array_func(::CLArrayDeviceProc, ::typeof(ones)) = OpenCL.ones
Dagger.allocate_array_func(::CLArrayDeviceProc, ::typeof(zeros)) = OpenCL.zeros
struct AllocateUndef{S} end
(::AllocateUndef{S})(T, dims::Dims{N}) where {S,N} = CLArray{S,N}(undef, dims)
Dagger.allocate_array_func(::CLArrayDeviceProc, ::Dagger.AllocateUndef{S}) where S = AllocateUndef{S}()

# In-place
# N.B. These methods assume that later operations will implicitly or
# explicitly synchronize with their associated stream
function Dagger.move!(to_space::Dagger.CPURAMMemorySpace, from_space::CLMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if Dagger.root_worker_id(from_space) == myid()
        _sync_with_context(from_space)
        with_context!(from_space)
    end
    copyto!(to, from)
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::CLMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    copyto!(to, from)
    return
end
function Dagger.move!(to_space::CLMemorySpace, from_space::CLMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    sync_with_context(from_space)
    with_context!(to_space)
    copyto!(to, from)
    return
end

# Out-of-place HtoD
function Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x)
    with_context(to_proc) do
        arr = adapt(CLArray, x)
        cl.finish(cl.queue())
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    cpu_data = remotecall_fetch(unwrap, from_w, x)
    with_context(to_proc) do
        arr = adapt(CLArray, cpu_data)
        cl.finish(cl.queue())
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x::CLArray)
    queue = x.data[].queue
    if queue == QUEUES[to_proc.device]
        return x
    end
    with_context(to_proc) do
        _x = similar(x)
        copyto!(_x, x)
        cl.finish(cl.queue())
        return _x
    end
end

# Out-of-place DtoH
function Dagger.move(from_proc::CLArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        cl.finish(cl.queue())
        _x = adapt(Array, x)
        cl.finish(cl.queue())
        return _x
    end
end
function Dagger.move(from_proc::CLArrayDeviceProc, to_proc::CPUProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    remotecall_fetch(from_w, x) do x
        arr = unwrap(x)
        return Dagger.move(from_proc, to_proc, arr)
    end
end
function Dagger.move(from_proc::CLArrayDeviceProc, to_proc::CPUProc, x::CLArray{T,N}) where {T,N}
    with_context(from_proc) do
        cl.finish(cl.queue())
        _x = Array{T,N}(undef, size(x))
        copyto!(_x, x)
        cl.finish(cl.queue())
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::CLArrayDeviceProc, to_proc::CLArrayDeviceProc, x::Dagger.Chunk{T}) where T<:CLArray
    if from_proc == to_proc
        # Same process and GPU, no change
        arr = unwrap(x)
        _sync_with_context(from_proc)
        return arr
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy
        from_arr = unwrap(x)
        _sync_with_context(from_proc)
        return with_context(to_proc) do
            to_arr = similar(from_arr)
            copyto!(to_arr, from_arr)
            cl.finish(cl.queue())
            return to_arr
        end
    else
        # Different node, use DtoH, serialization, HtoD
        return CLArray(remotecall_fetch(from_proc.owner, x) do x
            Array(unwrap(x))
        end)
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::CLArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

# Task execution
function Dagger.execute!(proc::CLArrayDeviceProc, f, args...; kwargs...)
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

CLArray(H::Dagger.HaloArray) = convert(CLArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:CLArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.edges),
                     C.(H.corners),
                     H.halo_width)
Adapt.adapt_structure(to::OpenCL.KernelAdaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.edges),
                     adapt.(Ref(to), H.corners),
                     H.halo_width)
function Dagger.inner_stencil_proc!(::CLArrayDeviceProc, f, output, read_vars)
    Dagger.Kernel(_inner_stencil!)(f, output, read_vars; ndrange=size(output))
    return
end
@kernel function _inner_stencil!(f, output, read_vars)
    idx = @index(Global, Cartesian)
    f(idx, output, read_vars)
end

Dagger.gpu_processor(::Val{:OpenCL}) = CLArrayDeviceProc
Dagger.gpu_can_compute(::Val{:OpenCL}) = length(cl.platforms()) > 0
Dagger.gpu_kernel_backend(::CLArrayDeviceProc) = OpenCLBackend()
Dagger.gpu_with_device(f, proc::CLArrayDeviceProc) =
    cl.device!(f, proc.device)
function Dagger.gpu_synchronize(proc::CLArrayDeviceProc)
    with_context(proc) do
        cl.finish(QUEUES[proc.device])
    end
end
function Dagger.gpu_synchronize(::Val{:OpenCL})
    for idx in keys(DEVICES)
        _sync_with_context(CLArrayDeviceProc(myid(), idx))
    end
end

Dagger.to_scope(::Val{:cl_device}, sc::NamedTuple) =
    Dagger.to_scope(Val{:cl_devices}(), merge(sc, (;cl_devices=[sc.cl_device])))
Dagger.scope_key_precedence(::Val{:cl_device}) = 1
function Dagger.to_scope(::Val{:cl_devices}, sc::NamedTuple)
    if haskey(sc, :worker)
        workers = Int[sc.worker]
    elseif haskey(sc, :workers) && sc.workers != Colon()
        workers = sc.workers
    else
        workers = map(gproc->gproc.pid, Dagger.procs(Dagger.Sch.eager_context()))
    end
    scopes = Dagger.ExactScope[]
    dev_ids = sc.cl_devices
    for worker in workers
        procs = Dagger.get_processors(Dagger.OSProc(worker))
        for proc in procs
            proc isa CLArrayDeviceProc || continue
            if dev_ids == Colon() || proc.device in dev_ids
                scope = Dagger.ExactScope(proc)
                push!(scopes, scope)
            end
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:cl_devices}) = 1

const DEVICES = Dict{Int, Device}()
const CONTEXTS = Dict{Int, Context}()
const QUEUES = Dict{Int, CmdQueue}()

function __init__()
    # FIXME: Support multiple platforms
    if length(cl.platforms()) > 0
        platform = cl.default_platform()
        for (idx, dev) in enumerate(cl.devices(platform))
            @debug "Registering OpenCL device processor with Dagger: $dev"
            Dagger.add_processor_callback!("clarray_device_$(idx)") do
                proc = CLArrayDeviceProc(myid(), idx)
                cl.device!(dev) do
                    DEVICES[idx] = dev
                    CONTEXTS[idx] = cl.context()
                    QUEUES[idx] = cl.queue()
                end
                return proc
            end
        end
    end
end

end # module OpenCLExt
