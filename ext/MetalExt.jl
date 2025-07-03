module MetalExt

export MtlArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap
import MemPool: DRef, poolget
import Distributed: myid, remotecall_fetch
import LinearAlgebra
using KernelAbstractions, Adapt

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

if isdefined(Base, :get_extension)
    import Metal
else
    import ..Metal
end
import Metal: MtlArray, MetalBackend
# FIXME: import Metal: MTLBLAS, MTLSOLVER
const MtlDevice = Metal.MTL.MTLDeviceInstance
const MtlStream = Metal.MTL.MTLCommandQueue

struct MtlArrayDeviceProc <: Dagger.Processor
    owner::Int
    device_id::UInt64
end

Dagger.get_parent(proc::MtlArrayDeviceProc) = Dagger.OSProc(proc.owner)
Dagger.root_worker_id(proc::MtlArrayDeviceProc) = proc.owner
Dagger.short_name(proc::MtlArrayDeviceProc) = "W: $(proc.owner), Metal: $(proc.device_id)"
Dagger.@gpuproc(MtlArrayDeviceProc, MtlArray)

"Represents the memory space of a single Metal GPU's VRAM."
struct MetalVRAMMemorySpace <: Dagger.MemorySpace
    owner::Int
    device_id::Int
end
Dagger.root_worker_id(space::MetalVRAMMemorySpace) = space.owner
function Dagger.memory_space(x::MtlArray)
    dev = Metal.device(x)
    device_id = _device_id(dev)
    return MetalVRAMMemorySpace(myid(), device_id)
end
_device_id(dev::MtlDevice) = findfirst(other_dev->other_dev === dev, Metal.devices())

Dagger.memory_spaces(proc::MtlArrayDeviceProc) = Set([MetalVRAMMemorySpace(proc.owner, proc.device_id)])
Dagger.processors(space::MetalVRAMMemorySpace) = Set([MtlArrayDeviceProc(space.owner, space.device_id)])

function to_device(proc::MtlArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return DEVICES[proc.device_id]
end
function to_context(proc::MtlArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return Metal.global_queue() #CONTEXTS[proc.device]
end
to_context(device_id::Integer) = Metal.global_queue(DEVICES[device_id])
to_context(dev::MtlDevice) = to_context(_device_id(dev))

function with_context!(device_id::Integer)
end
function with_context!(proc::MtlArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
end
function with_context!(space::MetalVRAMMemorySpace)
    @assert Dagger.root_worker_id(space) == myid()
end
function with_context(f, x)
    with_context!(x)
    return f()
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    with_context(x) do
        Metal.synchronize()
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
Dagger.allocate_array_func(::MtlArrayDeviceProc, ::typeof(rand)) = Metal.rand
Dagger.allocate_array_func(::MtlArrayDeviceProc, ::typeof(randn)) = Metal.randn
Dagger.allocate_array_func(::MtlArrayDeviceProc, ::typeof(ones)) = Metal.ones
Dagger.allocate_array_func(::MtlArrayDeviceProc, ::typeof(zeros)) = Metal.zeros
struct AllocateUndef{S} end
(::AllocateUndef{S})(T, dims::Dims{N}) where {S,N} = MtlArray{S,N}(undef, dims)
Dagger.allocate_array_func(::MtlArrayDeviceProc, ::Dagger.AllocateUndef{S}) where S = AllocateUndef{S}()

# In-place
# N.B. These methods assume that later operations will implicitly or
# explicitly synchronize with their associated stream
function Dagger.move!(to_space::Dagger.CPURAMMemorySpace, from_space::MetalVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if Dagger.root_worker_id(from_space) == myid()
        sync_with_context(from_space)
        with_context!(from_space)
    end
    copyto!(to, from)
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::MetalVRAMMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    copyto!(to, from)
    return
end
function Dagger.move!(to_space::MetalVRAMMemorySpace, from_space::MetalVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    sync_with_context(from_space)
    with_context!(to_space)
    copyto!(to, from)
    return
end

# Out-of-place HtoD
function Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x)
    with_context(to_proc) do
        arr = adapt(MtlArray, x)
        Metal.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    cpu_data = remotecall_fetch(unwrap, from_w, x)
    with_context(to_proc) do
        arr = adapt(MtlArray, cpu_data)
        Metal.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x::MtlArray)
    if Metal.device(x) == to_device(to_proc)
        return x
    end
    with_context(to_proc) do
        _x = similar(x)
        copyto!(_x, x)
        Metal.synchronize()
        return _x
    end
end

# Out-of-place DtoH
function Dagger.move(from_proc::MtlArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        Metal.synchronize()
        _x = adapt(Array, x)
        Metal.synchronize()
        return _x
    end
end
function Dagger.move(from_proc::MtlArrayDeviceProc, to_proc::CPUProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    remotecall_fetch(from_w, x) do x
        arr = unwrap(x)
        return Dagger.move(from_proc, to_proc, arr)
    end
end
function Dagger.move(from_proc::MtlArrayDeviceProc, to_proc::CPUProc, x::MtlArray{T,N}) where {T,N}
    with_context(from_proc) do
        Metal.synchronize()
        _x = Array{T,N}(undef, size(x))
        copyto!(_x, x)
        Metal.synchronize()
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::MtlArrayDeviceProc, to_proc::MtlArrayDeviceProc, x::Dagger.Chunk{T}) where T<:MtlArray
    if from_proc == to_proc
        # Same process and GPU, no change
        arr = unwrap(x)
        with_context(Metal.synchronize, from_proc)
        return arr
    # FIXME: elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
    else
        # Different node, use DtoH, serialization, HtoD
        return MtlArray(remotecall_fetch(from_proc.owner, x) do x
            Array(unwrap(x))
        end)
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

#= FIXME: Adapt BLAS/LAPACK functions
import LinearAlgebra: BLAS, LAPACK
for lib in [BLAS, LAPACK]
    for name in names(lib; all=true)
        name == nameof(lib) && continue
        startswith(string(name), '#') && continue
        endswith(string(name), '!') || continue

        for culib in [MTLBLAS, MTLSOLVER]
            if name in names(culib; all=true)
                fn = getproperty(lib, name)
                cufn = getproperty(culib, name)
                @eval Dagger.move(from_proc::CPUProc, to_proc::MtlArrayDeviceProc, ::$(typeof(fn))) = $cufn
            end
        end
    end
end
=#

function Dagger.move_optimized(
    from_proc::CPUProc,
    to_proc::MtlArrayDeviceProc,
    x::Array
)
    # FIXME
    return nothing

    # If we have unified memory, we can try casting the `Array` to `MtlArray`.
    device = _get_metal_device(to_proc)

    if (device !== nothing) && device.hasUnifiedMemory
        marray = _cast_array_to_mtlarray(x, device)
        marray !== nothing && return marray
    end

    return nothing
end
function Dagger.move_optimized(
    from_proc::MtlArrayDeviceProc,
    to_proc::CPUProc,
    x::Array
)
    # FIXME
    return nothing

    # If we have unified memory, we can just cast the `MtlArray` to an `Array`.
    device = _get_metal_device(from_proc)

    if (device !== nothing) && device.hasUnifiedMemory
        return unsafe_wrap(Array{T}, x, size(x))
    end

    return nothing
end

# Task execution
function Dagger.execute!(proc::MtlArrayDeviceProc, f, args...; kwargs...)
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

MtlArray(H::Dagger.HaloArray) = convert(MtlArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:MtlArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.edges),
                     C.(H.corners),
                     H.halo_width)
function Dagger.inner_stencil_proc!(::MtlArrayDeviceProc, f, output, read_vars)
    Dagger.Kernel(_inner_stencil!)(f, output, read_vars; ndrange=size(output))
    return
end
@kernel function _inner_stencil!(f, output, read_vars)
    idx = @index(Global, Cartesian)
    f(idx, output, read_vars)
end

function Base.show(io::IO, proc::MtlArrayDeviceProc)
    print(io, "MtlArrayDeviceProc(worker $(proc.owner), device $(something(_get_metal_device(proc)).name))")
end

Dagger.gpu_processor(::Val{:Metal}) = MtlArrayDeviceProc
Dagger.gpu_can_compute(::Val{:Metal}) = Metal.functional()
Dagger.gpu_kernel_backend(proc::MtlArrayDeviceProc) = MetalBackend()
# TODO: Switch devices
Dagger.gpu_with_device(f, proc::MtlArrayDeviceProc) = f()

function Dagger.gpu_synchronize(proc::MtlArrayDeviceProc)
    with_context(proc) do
        Metal.synchronize()
    end
end
function Dagger.gpu_synchronize(::Val{:Metal})
    for dev in Metal.devices()
        _sync_with_context(MtlArrayDeviceProc(myid(), _device_id(dev)))
    end
end

Dagger.to_scope(::Val{:metal_gpu}, sc::NamedTuple) =
    Dagger.to_scope(Val{:metal_gpus}(), merge(sc, (;metal_gpus=[sc.metal_gpu])))
Dagger.scope_key_precedence(::Val{:metal_gpu}) = 1
function Dagger.to_scope(::Val{:metal_gpus}, sc::NamedTuple)
    if haskey(sc, :worker)
        workers = Int[sc.worker]
    elseif haskey(sc, :workers) && sc.workers != Colon()
        workers = sc.workers
    else
        workers = map(gproc->gproc.pid, Dagger.procs(Dagger.Sch.eager_context()))
    end
    scopes = Dagger.ExactScope[]
    dev_ids = sc.metal_gpus
    for worker in workers
        procs = Dagger.get_processors(Dagger.OSProc(worker))
        for proc in procs
            proc isa MtlArrayDeviceProc || continue
            if dev_ids == Colon() || proc.device_id in dev_ids
                scope = Dagger.ExactScope(proc)
                push!(scopes, scope)
            end
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:metal_gpus}) = 1

const DEVICES = Dict{Int, MtlDevice}()
#const CONTEXTS = Dict{Int, MtlContext}()
#const STREAMS = Dict{Int, MtlStream}()

function __init__()
    if Metal.functional()
        for (dev_id, dev) in enumerate(Metal.devices())
            @debug "Registering Metal GPU processor with Dagger: $dev"
            # FIXME: We only get a Ptr now
            Dagger.add_processor_callback!("metal_device_$(dev_id)") do
                proc = MtlArrayDeviceProc(myid(), dev_id)
                DEVICES[dev_id] = dev
                #=
                ctx = context(dev)
                CONTEXTS[dev.device_id] = ctx
                context!(ctx) do
                    STREAMS[dev.device_id] = stream()
                end
                =#
                return proc
            end
        end
    end
end


################################################################################
#                              Private functions
################################################################################

# Try casting the array `x` to an `MtlArray`. If the casting is not possible,
# return `nothing`.
function _cast_array_to_mtlarray(x::Array{T,N}, device::MtlDevice) where {T,N}
    # Try creating the buffer without copying.
    dims = size(x)
    nbytes_array = prod(dims) * sizeof(T)
    pagesize = ccall(:getpagesize, Cint, ())
    num_pages = div(nbytes_array, pagesize, RoundUp)
    nbytes = num_pages * pagesize

    pbuf = Metal.MTL.mtDeviceNewBufferWithBytesNoCopy(
        device,
        pointer(x),
        nbytes,
        Metal.Shared | Metal.MTL.DefaultTracking | Metal.MTL.DefaultCPUCache
    )

    if pbuf != C_NULL
        buf = MtlBuffer(pbuf)
        marray = MtlArray{T,N}(buf, dims)
        return marray
    end

    # If we reached here, the conversion was not possible.
    return nothing
end

# Return the Metal device handler given the ID recorded in `proc`.
function _get_metal_device(proc::MtlArrayDeviceProc)
    devices = Metal.devices()

    if devices === nothing
        return nothing
    else
        return devices[proc.device_id]
    end
end

end # module MetalExt
