# Loaded after Dagger on the plain/Distributed and MPI workers.
const GPU_BACKENDS = Dict(
    "cuda" => (package=:CUDA, key=:cuda_gpu),
    "amdgpu" => (package=:AMDGPU, key=:rocm_gpu),
    "oneapi" => (package=:oneAPI, key=:intel_gpu),
    "metal" => (package=:Metal, key=:metal_gpu),
    "opencl" => (package=:OpenCL, key=:cl_device),
)

# Keep one backend per worker: fixtures, capability probes, warmup and every
# timed sample must all execute in the same GPU scope.
const benchmark_accel = isempty(accelerations) ? nothing : only(accelerations)
if benchmark_accel !== nothing
    all(bench.accels == [benchmark_accel] for list in values(benches) for bench in list) ||
        error("Use a separate benchmark worker for each backend; CPU and GPU methods cannot share a worker")
    cfg = get(GPU_BACKENDS, benchmark_accel, nothing)
    cfg === nothing && error("Unknown acceleration: $benchmark_accel")
    # Optional software ICD for local validation without GPU hardware.
    if benchmark_accel == "opencl" && get(ENV, "BENCHMARK_OPENCL_SOFTWARE", "false") == "true"
        @everywhere using pocl_jll
    end
    @eval @everywhere using $(cfg.package)
end
const benchmark_compute_scope = benchmark_accel === nothing ? nothing :
    Dagger.scope(; NamedTuple{(GPU_BACKENDS[benchmark_accel].key,)}((1,))...)

function with_benchmark_scope(f)
    benchmark_compute_scope === nothing && return f()
    return Dagger.with_options(f; scope=benchmark_compute_scope)
end
benchmark_eltype() = benchmark_accel === nothing ? Float64 : Float32
benchmark_assignment() = benchmark_accel === nothing && length(procs()) > 1 ? :cyclicrow : :arbitrary
