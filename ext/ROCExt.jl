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
function with_context(f, x)
    old_ctx = context()
    old_device = AMDGPU.device()
    old_stream = stream()

    with_context!(x)
    try
        f()
    finally
        context!(old_ctx)
        AMDGPU.device!(old_device)
        stream!(old_stream)
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

# Indexing
Base.getindex(arr::ROCArray, d::Dagger.ArrayDomain) = arr[Dagger.indexes(d)...]

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
        # Same process and GPU, no change
        arr = unwrap(x)
        with_context(AMDGPU.synchronize, from_proc)
        return arr
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy
        from_arr = unwrap(x)
        dev = AMDGPU.device(from_arr)
        with_context(AMDGPU.synchronize, dev.device_id)
        return with_context(to_proc) do
            to_arr = similar(from_arr)
            copyto!(to_arr, from_arr)
            AMDGPU.synchronize()
            to_arr
        end
    else
        # Different node, use DtoH, serialization, HtoD
        return ROCArray(remotecall_fetch(from_proc.owner, x) do x
            Array(unwrap(x))
        end)
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

# Adapt BLAS/LAPACK functions
import LinearAlgebra: BLAS, LAPACK
for lib in [BLAS, LAPACK]
    for name in names(lib; all=true)
        name == nameof(lib) && continue
        startswith(string(name), '#') && continue
        endswith(string(name), '!') || continue

        for roclib in [rocBLAS, rocSOLVER]
            if name in names(roclib; all=true)
                fn = getproperty(lib, name)
                rocfn = getproperty(roclib, name)
                @eval Dagger.move(from_proc::CPUProc, to_proc::ROCArrayDeviceProc, ::$(typeof(fn))) = $rocfn
            end
        end
    end
end

# Task execution
function Dagger.execute!(proc::ROCArrayDeviceProc, f, args...; kwargs...)
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

ROCArray(H::Dagger.HaloArray) = convert(ROCArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:ROCArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.edges),
                     C.(H.corners),
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
                end
                return proc
            end
        end
    end
end

end # module ROCExt
