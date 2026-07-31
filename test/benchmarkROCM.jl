"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TUNING GUIDE  (ROCm / AMDGPU port of benchmarkCUDA.jl)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

GPU_SELECTION
  :all          → every detected GPU tested individually, then all together
  [1]           → only device 1 (1-based AMDGPU device_id — NOT 0-based)
  [1, 2]        → devices 1 and 2 individually + combined multi-GPU scope

MATRIX_SIZES    → list of (rows, cols) tuples
BLOCK_SIZES     → list of (blk_rows, blk_cols) Dagger tile tuples
                  smaller = more tasks + overhead; larger = better locality
BENCH_SAMPLES   → timed runs per configuration (warm-up not counted)
WARMUP_RUNS     → discarded runs before timing starts
MIN_FREE_VRAM_GIB → skip a config if any participating GPU has less free VRAM

RUN_MATMUL / RUN_TRANSPOSE / RUN_ELEMENTWISE / RUN_SATURATE / RUN_DAG → toggle ops
RUN_DAG_LINEAR / RUN_DAG_DIAMOND / RUN_DAG_CHAINLINK / RUN_DAG_TANGLED → toggle DAG shapes

RUN_SATURATE is not part of the size/block grid: it runs ONCE per scope at a fixed
shape (SATURATE_SIZE/SATURATE_BLOCK), with as many concurrent independent matmuls
as free VRAM allows — its only goal is to drive GPU utilization to 100%.

Every run also writes its full report to
test/benchresults/"benchmark ROCm results <n>.md".

N.B. AMDGPU has no NVML equivalent; utilization is read from the amdgpu driver's
     sysfs `gpu_busy_percent` instead (N/A if the node isn't readable).
     Device identity here is 1-based `proc.device_id` throughout (unlike the
     CUDA benchmark's 0-based `proc.device`), matching ROCExt's scope keys.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

# gfx1032 (RX 6600 / Navi 23) has no rocBLAS Tensile library; override to its
# binary-compatible RDNA2 sibling gfx1030. MUST be set before `using AMDGPU`,
# since the HSA runtime reads it at initialization.
ENV["HSA_OVERRIDE_GFX_VERSION"] = "10.3.0"

# ROCExt spawns a Julia task per Dagger task, and AMDGPU caches a rocBLAS handle
# per Julia task. Under concurrent (saturate) workloads hundreds of handles are
# live at once; with rocBLAS's default preallocated workspace that exhausts VRAM
# at handle-create time. 0 = on-demand allocation, so each handle reserves nothing.
ENV["ROCBLAS_DEVICE_MEMORY_SIZE"] = "0"

# ─── Dependencies ─────────────────────────────────────────────────────────────
using Pkg, LinearAlgebra
for dep in ["AMDGPU", "Dagger", "Statistics", "Printf", "Dates"]
    haskey(Pkg.project().dependencies, dep) || Pkg.add(dep)
end

using AMDGPU, Dagger, Statistics, Printf, Dates

# ═══════════════════════════════════════════════════════════════════════════════
#  PARAMETERS  ←  edit this block to configure everything
# ═══════════════════════════════════════════════════════════════════════════════

# Which GPU device IDs (1-based) to benchmark.
# :all  → auto-detect every GPU and also run a combined multi-GPU scope
# [1]   → only GPU 1
# [1,2] → GPU 1 and GPU 2 individually, plus a joint multi-GPU scope
const GPU_SELECTION = [1]

# Stream distribution strategy: :roundrobin, :random, or :sdq (shortest stream queue)
const STREAM_STRATEGY = :roundrobin

# Matrix shapes to test: (rows, cols)
const MATRIX_SIZES = [
    (4096, 4096),
    (8192, 8192),
]

# Dagger tile sizes: (blk_rows, blk_cols)
# Clamped to matrix dimensions automatically — safe to leave larger than matrix.
const BLOCK_SIZES = [
    #(512, 512),
    (1024, 1024),
    (2048, 2048),
]

const BENCH_SAMPLES     = 3     # timed runs per config
const WARMUP_RUNS       = 2     # discarded warm-up runs

const RUN_MATMUL        = true # A * B
const RUN_TRANSPOSE     = true  # A'
const RUN_ELEMENTWISE   = true  # A .* B
const RUN_SATURATE      = false   # N independent A*B launched concurrently to fill every stream
const RUN_DAG           = true   # DAG-shaped graphs of matmul nodes (see DAG_TOPOLOGIES)

# Which DAG topologies to run — toggle each shape like the RUN_* flags above.
const RUN_DAG_LINEAR    = true   # X → X → X → …           (DAG_CHAIN_LENGTH sequential nodes)
const RUN_DAG_DIAMOND   = true   # S ⇉ (B1 ∥ B2) → J       (fan-out, two parallel nodes, join)
const RUN_DAG_CHAINLINK = true   # DAG_CHAIN_LENGTH diamonds welded end to end
const RUN_DAG_TANGLED   = true   # 2 sources → crossed layer → crossed layer → 1 sink

const DAG_TOPOLOGIES = [t for (flag, t) in
    ((RUN_DAG_LINEAR, :linear), (RUN_DAG_DIAMOND, :diamond),
     (RUN_DAG_CHAINLINK, :chainlink), (RUN_DAG_TANGLED, :tangled)) if flag]

# Stages: sequential nodes in :linear, diamonds in :chainlink.
const DAG_CHAIN_LENGTH  = 3

# Saturation: ONE standalone run whose only goal is to peg the GPU at 100%.
# It ignores MATRIX_SIZES/BLOCK_SIZES entirely — shape is irrelevant here, only
# occupancy matters. Size/block are fixed below; the number of concurrent
# independent matmuls is derived from free VRAM at run time (saturate_count).
const SATURATE_SIZE     = 4096   # square side of each independent problem
const SATURATE_BLOCK    = 1024   # tile size — small tiles ⇒ many tasks ⇒ every stream fed
const SATURATE_MAX      = 32     # cap on concurrent problems

# Skip a config if any participating GPU has less than this much free VRAM.
const MIN_FREE_VRAM_GIB = 1.5

# ═══════════════════════════════════════════════════════════════════════════════
#  GPU discovery
#  Uses Dagger.gpu_processor(Val(:ROC)) so we never import ROCExt directly.
# ═══════════════════════════════════════════════════════════════════════════════

roc_device(device_id::Int) = AMDGPU.devices()[device_id]

function discover_gpu_procs()
    GpuProc = Dagger.gpu_processor(Val(:ROC))   # returns ROCArrayDeviceProc type
    procs_set = filter(p -> p isa GpuProc, Dagger.get_processors(Dagger.OSProc()))
    sorted_procs = sort(collect(procs_set); by = p -> p.device_id)
    return sorted_procs
end

function gpu_vram_info(device_id::Int)
    AMDGPU.device!(roc_device(device_id))
    free_b, total_b = AMDGPU.info()
    return (free_gib = free_b / 2^30, total_gib = total_b / 2^30)
end

# No NVML on ROCm, but the amdgpu kernel driver exports busy % in sysfs.
# ponytail: takes the max across every AMD card — exact for a single GPU;
# per-device mapping would need PCI-bus matching against AMDGPU.device_id.
const GPU_BUSY_NODES = filter(isfile,
    ["/sys/class/drm/card$i/device/gpu_busy_percent" for i in 0:15])

function _sample_util(device_ids::Vector{Int})
    best = NaN
    for node in GPU_BUSY_NODES
        try
            v = parse(Float64, strip(read(node, String)))
            best = isnan(best) ? v : max(best, v)
        catch
        end
    end
    return best
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Scope helpers
#  ROCExt matches `rocm_gpu(s)` scope keys against 1-based `proc.device_id`
#  directly (no offset), so scopes and selection use the same 1-based IDs.
# ═══════════════════════════════════════════════════════════════════════════════

single_gpu_scope(device_id::Int)          = Dagger.scope(; rocm_gpu  = device_id)
multi_gpu_scope(device_ids::Vector{Int})  = Dagger.scope(; rocm_gpus = device_ids)

# ═══════════════════════════════════════════════════════════════════════════════
#  Benchmark engine
# ═══════════════════════════════════════════════════════════════════════════════

function bench_function(f::Function, procs::Vector;
                        warmup  = WARMUP_RUNS,
                        samples = BENCH_SAMPLES)

    # Host-blocking barrier: sync every participating device (all its streams).
    function gpu_sync!()
        for p in procs
            AMDGPU.device!(roc_device(p.device_id))
            AMDGPU.device_synchronize()
        end
    end

    # Two-pass GC ensures DRef finalizers fire before pool reclaim
    reclaim!() = (GC.gc(false); GC.gc(true); AMDGPU.reclaim())

    # ── Warm-up ────────────────────────────────────────────────────────────
    for _ in 1:warmup
        r = f()
        r = nothing       # drop before reclaim so DRefs can be finalized
        gpu_sync!()
        reclaim!()
    end

    # ── GPU utilization sampler — background thread, 50 ms polling ────────
    device_ids   = [p.device_id for p in procs]
    util_samples = Float64[]
    util_stop    = Ref{Bool}(false)
    bg_util = Threads.@spawn begin
        try
            while !util_stop[]
                v = _sample_util(device_ids)
                isnan(v) || push!(util_samples, v)
                sleep(0.05)
            end
        catch
        end
    end

    # ── Timed samples ──────────────────────────────────────────────────────
    times_ns = Vector{Float64}(undef, samples)
    for i in 1:samples
        gpu_sync!()
        t0 = time_ns()
        r = f()
        gpu_sync!()
        times_ns[i] = Float64(time_ns() - t0)
        r = nothing       # drop before reclaim
        reclaim!()
    end

    util_stop[] = true
    wait(bg_util)

    times_s   = times_ns ./ 1e9
    util_peak = isempty(util_samples) ? NaN : maximum(util_samples)
    util_mean = isempty(util_samples) ? NaN : mean(util_samples)
    return (
        min       = minimum(times_s),
        mean      = mean(times_s),
        max       = maximum(times_s),
        std       = length(times_s) > 1 ? std(times_s) : 0.0,
        median    = median(times_s),
        util_peak = util_peak,
        util_mean = util_mean,
    )
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Kernel wrappers
# ═══════════════════════════════════════════════════════════════════════════════

function run_matmul(rows, inner, cols, blk_r, blk_c, scope)
    Dagger.with_options(; scope) do
        A = rand(Blocks(blk_r, blk_c), Float32, rows, inner)
        B = rand(Blocks(blk_c, blk_c), Float32, inner, cols)
        C = rand(Blocks(blk_c, blk_c), Float32, rows, cols)
        mul!(C, A, B)
        result = collect(C)
        free_darray!(A); free_darray!(B); free_darray!(C)
        result
    end
end

function run_transpose(rows, cols, blk_r, blk_c, scope)
    Dagger.with_options(; scope) do
        A = rand(Blocks(blk_r, blk_c), Float32, rows, cols)
        result = collect(A')
        free_darray!(A)
        result
    end
end

function run_elementwise(rows, cols, blk_r, blk_c, scope)
    Dagger.with_options(; scope) do
        A = rand(Blocks(blk_r, blk_c), Float32, rows, cols)
        B = rand(Blocks(blk_r, blk_c), Float32, rows, cols)
        C = A .* B
        result = collect(C)
        free_darray!(A); free_darray!(B); free_darray!(C)
        result
    end
end

# How many independent problems fit: 3 matrices each, 60% of the tightest GPU's
# free VRAM (the rest is rocBLAS workspace + tile copies).
function saturate_count(procs::Vector)
    free_gib = minimum(gpu_vram_info(p.device_id).free_gib for p in procs)
    per_gib  = 3 * SATURATE_SIZE^2 * sizeof(Float32) / 2^30
    return clamp(floor(Int, 0.6 * free_gib / per_gib), 2, SATURATE_MAX)
end

# Launch `count` independent matmuls concurrently. `mul!` blocks its calling
# task (its datadeps region waits), so each problem runs in its own Julia task;
# the regions touch disjoint memory and overlap, keeping every stream busy at
# once — this is what saturates the whole GPU. Nothing is collected back: a D2H
# copy would stall the streams and drop utilization, and the values are unused.
function run_saturate(count, scope)
    @sync for _ in 1:count
        Threads.@spawn Dagger.with_options(; scope) do
            A = rand(Blocks(SATURATE_BLOCK, SATURATE_BLOCK), Float32, SATURATE_SIZE, SATURATE_SIZE)
            B = rand(Blocks(SATURATE_BLOCK, SATURATE_BLOCK), Float32, SATURATE_SIZE, SATURATE_SIZE)
            C = rand(Blocks(SATURATE_BLOCK, SATURATE_BLOCK), Float32, SATURATE_SIZE, SATURATE_SIZE)
            mul!(C, A, B)
            free_darray!(A); free_darray!(B); free_darray!(C)
        end
    end
    return nothing
end

# DAG topologies. Every node is a matmul (C := A*B) spawned tile-by-tile into a
# SINGLE `spawn_datadeps` region. DAG edges are encoded by In/InOut on the shared
# chunks, so datadeps runs independent nodes concurrently (overlapping across
# streams) and serializes dependent ones — all from one submitting task. This is
# what keeps it deadlock-free: nested `spawn_datadeps` + `Threads.@spawn` would
# submit two datadeps regions from two Julia tasks at once, which wedges Dagger's
# eager scheduler on its state.lock/EAGER_ID_MAP locks.
# Uses square rows×rows matrices (cols is ignored — chained matmuls need square).
dag_matmuls(t) = t === :linear    ? DAG_CHAIN_LENGTH :
                 t === :diamond   ? 3 :
                 t === :chainlink ? 3 * DAG_CHAIN_LENGTH :
                 t === :tangled   ? 5 :
                 error("unknown DAG topology: $t")

# One DAG node: C := A*B, spawned as per-tile `BLAS.gemm!` tasks into the current
# datadeps region (dispatched to rocBLAS on GPU chunks). Mirrors Dagger's own
# `gemm_dagger!` inner loop but without opening its own region, so many nodes
# share one region and overlap.
function dag_gemm!(C::DArray, A::DArray, B::DArray)
    Ac, Bc, Cc = A.chunks, B.chunks, C.chunks
    Ant = size(Ac, 2)
    for n in axes(Cc, 2), m in axes(Cc, 1)
        for k in 1:Ant
            beta = k == 1 ? 0f0 : 1f0          # k==1 overwrites C, later k accumulate
            Dagger.@spawn BLAS.gemm!('N', 'N', 1f0,
                Dagger.In(Ac[m, k]), Dagger.In(Bc[k, n]),
                beta, Dagger.InOut(Cc[m, n]))
        end
    end
    return C
end

function run_dag(topology, sz, blk, scope)
    Dagger.with_options(; scope) do
        live = DArray[]                 # every allocation, freed together at the end
        alloc!() = (A = rand(Blocks(blk, blk), Float32, sz, sz); push!(live, A); A)

        # Pre-allocate every matrix (sources + one output per node) up front, then
        # run the whole DAG in one datadeps region. Output chunks are overwritten
        # (beta=0 on the first k), so their random init is irrelevant.
        # ponytail: every matrix stays live until the run ends; free-as-consumed
        # if large chainlinks hit the VRAM guard.
        out = if topology === :linear
            X = alloc!(); M = alloc!()
            outs = [alloc!() for _ in 1:DAG_CHAIN_LENGTH]
            Dagger.spawn_datadeps() do
                cur = X
                for o in outs
                    dag_gemm!(o, cur, M); cur = o
                end
            end
            outs[end]

        elseif topology === :diamond
            S = alloc!(); M1 = alloc!(); M2 = alloc!()
            B1 = alloc!(); B2 = alloc!(); J = alloc!()
            Dagger.spawn_datadeps() do
                dag_gemm!(B1, S, M1)        # ┐ independent branches:
                dag_gemm!(B2, S, M2)        # ┘ overlap across streams
                dag_gemm!(J, B1, B2)        # join
            end
            J

        elseif topology === :chainlink
            S = alloc!(); M1 = alloc!(); M2 = alloc!()
            stages = [(alloc!(), alloc!(), alloc!()) for _ in 1:DAG_CHAIN_LENGTH]
            Dagger.spawn_datadeps() do
                cur = S
                for (b1, b2, snext) in stages
                    dag_gemm!(b1, cur, M1)  # ┐ diamond branches overlap
                    dag_gemm!(b2, cur, M2)  # ┘
                    dag_gemm!(snext, b1, b2)  # weld to next diamond
                    cur = snext
                end
            end
            stages[end][3]

        elseif topology === :tangled
            S1 = alloc!(); S2 = alloc!()
            T1 = alloc!(); T2 = alloc!(); U1 = alloc!(); U2 = alloc!(); J = alloc!()
            Dagger.spawn_datadeps() do
                dag_gemm!(T1, S1, S2)       # ┐ layer 1 (independent)
                dag_gemm!(T2, S2, S1)       # ┘
                dag_gemm!(U1, T1, T2)       # ┐ layer 2 (independent, cross-consume)
                dag_gemm!(U2, T2, T1)       # ┘
                dag_gemm!(J, U1, U2)        # sink
            end
            J

        else
            error("unknown DAG topology: $topology")
        end

        result = collect(out)
        foreach(free_darray!, live)
        result
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Correctness check  (capped at 512 to avoid VRAM pressure during verify)
# ═══════════════════════════════════════════════════════════════════════════════

function verify_matmul(rows, inner, cols, blk_r, blk_c, scope)
    sz  = min(rows, inner, cols, 512)
    bsz = min(blk_r, blk_c, sz)
    Dagger.with_options(; scope) do
        A      = rand(Blocks(bsz, bsz), sz, sz)
        B      = rand(Blocks(bsz, bsz), sz, sz)
        C      = A * B
        result = collect(C)
        ref    = collect(A) * collect(B)
        free_darray!(A)
        free_darray!(B)
        free_darray!(C)
        result ≈ ref
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Throughput helpers
# ═══════════════════════════════════════════════════════════════════════════════

gflops_matmul(M, K, N, t)    = 2.0 * M * K * N / (t * 1e9)
gflops_transpose(M, N, t)    = 2.0 * M * N / (t * 1e9)
gflops_elementwise(M, N, t)  = 1.0 * M * N / (t * 1e9)

# ═══════════════════════════════════════════════════════════════════════════════
#  Result type
# ═══════════════════════════════════════════════════════════════════════════════

struct BenchResult
    op          ::String
    scope_label ::String
    mat_sz      ::Tuple{Int,Int}
    mat_sz2     ::Union{Nothing,Tuple{Int,Int}}
    blk_sz      ::Tuple{Int,Int}
    min_s       ::Float64
    mean_s      ::Float64
    max_s       ::Float64
    std_s       ::Float64
    median_s    ::Float64
    gflops      ::Float64
    correct     ::Union{Bool,Nothing}
    util_peak   ::Float64   # peak GPU compute utilization % (NaN — no NVML on ROCm)
    util_mean   ::Float64   # mean GPU compute utilization %
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Memory guard
# ═══════════════════════════════════════════════════════════════════════════════

"""Returns true if every proc in `procs` has at least `min_gib` GiB free VRAM."""
function enough_vram(procs::Vector, min_gib = MIN_FREE_VRAM_GIB)
    for p in procs
        gpu_vram_info(p.device_id).free_gib < min_gib && return false
    end
    return true
end

function force_reclaim!(procs::Vector)
    prev_free = AMDGPU.free()
    while true
        GC.gc(false)
        GC.gc(true)
        foreach(Dagger.gpu_synchronize, procs)   # sync every GPU in the scope
        AMDGPU.reclaim()
        curr_free = AMDGPU.free()
        delta = abs(Int64(curr_free) - Int64(prev_free))
        delta < 50 * 1024^2 && break
        prev_free = curr_free
    end
end

function free_darray!(A)
    # Unwrap if it's an Adjoint/Transpose from operations like A'
    core_array = A isa DArray ? A : parent(A)
    Dagger.unsafe_free!(core_array)
    return
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Report helpers
# ═══════════════════════════════════════════════════════════════════════════════

const LINE_WIDTH = 250

banner(c, n = LINE_WIDTH) = c^n

function thru_str(g)
    g >= 1000.0 ? @sprintf("%.3f TFLOPS", g / 1000.0) :
                  @sprintf("%.2f GFLOPS", g)
end

util_str(u) = isnan(u) ? "N/A" : @sprintf("%.1f%%", u)

function print_header()
    println(banner('═'))
    println("  DAGGER.jl MULTI-GPU BENCHMARK (ROCm)  ·  $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))")
    println("  Julia $(VERSION)  ·  AMDGPU.jl $(pkgversion(AMDGPU))  ·  Dagger.jl $(pkgversion(Dagger))")
    println(banner('═'))
end

function print_gpu_table(all_procs)
    println("\n  DETECTED GPUs")
    println(banner('─'))
    for p in all_procs
        info = gpu_vram_info(p.device_id)
        @printf("  [%d]  %-34s  %5.1f GiB total  %5.1f GiB free\n",
            p.device_id, AMDGPU.HIP.name(roc_device(p.device_id)),
            info.total_gib, info.free_gib)
    end
    println()
end

# Stream pool size for a GPU, straight from ROCExt's per-device STREAMS pool.
n_streams(p) = length(Base.get_extension(Dagger, :ROCExt).STREAMS[p.device_id])

function print_config(scope_entries)
    ops = filter(!isempty, [
        RUN_MATMUL      ? "MatMul"                    : "",
        RUN_TRANSPOSE   ? "Transpose"                 : "",
        RUN_ELEMENTWISE ? "Elementwise"               : "",
        RUN_SATURATE    ? "Saturate($(SATURATE_SIZE)², auto count)" : "",
        RUN_DAG         ? "DAG($(join(DAG_TOPOLOGIES, ",")))"   : "",
    ])
    labels = [e.label for e in scope_entries]
    println("  BENCHMARK CONFIGURATION")
    println(banner('─'))
    println("  Scopes          : $(join(labels, "  |  "))")
    bench_procs = unique(reduce(vcat, e.procs for e in scope_entries))
    println("  Streams/GPU     : $(join(["GPU $(p.device_id): $(n_streams(p))" for p in bench_procs], ", "))  ($STREAM_STRATEGY)")
    println("  Samples/Warm-up : $BENCH_SAMPLES / $WARMUP_RUNS")
    println("  Matrix sizes    : $(join(["$(r)×$(c)" for (r,c) in MATRIX_SIZES], ", "))")
    println("  Block sizes     : $(join(["$(r)×$(c)" for (r,c) in BLOCK_SIZES],  ", "))")
    println("  Operations      : $(join(ops, ", "))")
    println("  Min free VRAM   : $(MIN_FREE_VRAM_GIB) GiB  (configs below this are skipped)")
    println()
end

function print_results_table(results::Vector{BenchResult})
    println("  RESULTS")
    println(banner('─'))
    @printf("  %-14s  %-14s  %-20s  %-12s  %8s  %8s  %8s  %8s  %8s  %13s  %8s  %8s  %s\n",
        "Operation", "Scope", "Matrix shape", "Block size",
        "Min(ms)", "Mean(ms)", "Max(ms)", "Std(ms)", "Med(ms)",
        "Throughput", "UtilPk%", "UtilAvg%", "Correct?")
    println("  " * banner('─', LINE_WIDTH - 2))

    prev_op = ""
    for r in results
        # Visual separator between operation groups
        if r.op != prev_op && !isempty(prev_op)
            println("  " * banner('·', LINE_WIDTH - 2))
        end
        prev_op = r.op

        mat_str = isnothing(r.mat_sz2) ?
            "$(r.mat_sz[1])×$(r.mat_sz[2])" :
            "$(r.mat_sz[1])×$(r.mat_sz[2])·$(r.mat_sz2[1])×$(r.mat_sz2[2])"
        blk_str = "$(r.blk_sz[1])×$(r.blk_sz[2])"
        ok_str  = isnothing(r.correct) ? "—" : (r.correct ? "✓" : "✗ FAIL")

        @printf("  %-14s  %-14s  %-20s  %-12s  %8.2f  %8.2f  %8.2f  %8.3f  %8.2f  %13s  %8s  %8s  %s\n",
            r.op, r.scope_label, mat_str, blk_str,
            r.min_s * 1000, r.mean_s * 1000, r.max_s * 1000,
            r.std_s * 1000, r.median_s * 1000,
            thru_str(r.gflops), util_str(r.util_peak), util_str(r.util_mean), ok_str)
    end
    println()
end

function print_footer(results::Vector{BenchResult}, total_s::Float64 = 0.0, n_skipped::Int = 0)
    isempty(results) && return
    println(banner('═'))
    println("  BENCHMARK SUMMARY")
    println(banner('─'))

    # ── Coverage & correctness ─────────────────────────────────────────────────
    n_run     = length(results)
    n_checked = count(r -> !isnothing(r.correct), results)
    n_correct = count(r -> r.correct === true,    results)
    total_s > 0 && @printf("  Total runtime   : %.4f s  (%.1f min)\n", total_s, total_s / 60)
    @printf("  Configs run     : %d   \nskipped (low VRAM): %d\n", n_run, n_skipped)
    n_checked > 0 && @printf("  Correctness     : %d / %d passed\n", n_correct, n_checked)
    println()

    # ── Per-operation throughput ───────────────────────────────────────────────
    ops = unique(r.op for r in results)
    println("  PER-OPERATION THROUGHPUT")
    @printf("  %-14s  %13s  %13s  %13s  %8s  %8s  %6s\n",
        "Operation", "Avg", "Peak", "Worst", "AvgUtil%", "PkUtil%", "Runs")
    println("  " * banner('─', 80))
    for op in ops
        rs      = filter(r -> r.op == op, results)
        avg_g   = mean(r.gflops for r in rs)
        peak_g  = maximum(r.gflops for r in rs)
        worst_g = minimum(r.gflops for r in rs)
        valid_mean = filter(!isnan, [r.util_mean for r in rs])
        valid_peak = filter(!isnan, [r.util_peak for r in rs])
        avg_util   = isempty(valid_mean) ? NaN : mean(valid_mean)
        pk_util    = isempty(valid_peak) ? NaN : maximum(valid_peak)
        @printf("  %-14s  %13s  %13s  %13s  %8s  %8s  %6d\n",
            op, thru_str(avg_g), thru_str(peak_g), thru_str(worst_g),
            util_str(avg_util), util_str(pk_util), length(rs))
    end
    println()

    # ── Per-scope throughput ───────────────────────────────────────────────────
    scopes = unique(r.scope_label for r in results)
    if length(scopes) > 1
        println("  PER-SCOPE THROUGHPUT")
        @printf("  %-22s  %13s  %13s  %13s  %6s\n",
            "Scope", "Avg", "Peak", "Worst", "Runs")
        println("  " * banner('─', 72))
        for sc in scopes
            rs      = filter(r -> r.scope_label == sc, results)
            avg_g   = mean(r.gflops for r in rs)
            peak_g  = maximum(r.gflops for r in rs)
            worst_g = minimum(r.gflops for r in rs)
            @printf("  %-22s  %13s  %13s  %13s  %6d\n",
                sc, thru_str(avg_g), thru_str(peak_g), thru_str(worst_g), length(rs))
        end
        println()
    end

    # ── Per-matrix-size throughput ─────────────────────────────────────────────
    all_msizes = unique(r.mat_sz for r in results)
    if length(all_msizes) > 1
        println("  PER-MATRIX-SIZE THROUGHPUT")
        @printf("  %-14s  %-14s  %13s  %13s  %6s\n",
            "Operation", "Shape", "Avg", "Peak", "Runs")
        println("  " * banner('─', 64))
        for op in ops
            rs_op = filter(r -> r.op == op, results)
            szs   = sort(unique(r.mat_sz for r in rs_op))
            for sz in szs
                rs    = filter(r -> r.mat_sz == sz, rs_op)
                avg_g = mean(r.gflops for r in rs)
                pk_g  = maximum(r.gflops for r in rs)
                @printf("  %-14s  %-14s  %13s  %13s  %6d\n",
                    op, "$(sz[1])×$(sz[2])", thru_str(avg_g), thru_str(pk_g), length(rs))
            end
        end
        println()
    end

    # ── Highlights ────────────────────────────────────────────────────────────
    println(banner('─'))
    r_fast = results[argmin([r.mean_s for r in results])]
    println("  🏆 Fastest mean:    $(r_fast.op)  scope=$(r_fast.scope_label)  " *
            "$(r_fast.mat_sz[1])×$(r_fast.mat_sz[2])  block=$(r_fast.blk_sz[1])×$(r_fast.blk_sz[2])  " *
            @sprintf("→ %.2f ms", r_fast.mean_s * 1000))

    r_peak = results[argmax([r.gflops for r in results])]
    println("  ⚡ Best throughput: $(r_peak.op)  scope=$(r_peak.scope_label)  " *
            "$(r_peak.mat_sz[1])×$(r_peak.mat_sz[2])  block=$(r_peak.blk_sz[1])×$(r_peak.blk_sz[2])  " *
            "→ $(thru_str(r_peak.gflops))")

    avg_all = mean(r.gflops for r in results)
    @printf("  📊 Overall avg:     %s  across %d configs\n", thru_str(avg_all), n_run)

    println(banner('═'))
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Report file — same output as the console, next to this script,
#  numbered so successive runs never overwrite each other.
# ═══════════════════════════════════════════════════════════════════════════════

const REPORT_DIR    = joinpath(@__DIR__, "benchresults")
const REPORT_PREFIX = joinpath(REPORT_DIR, "benchmark ROCm results")

function write_report(results, scope_entries, total_s, n_skipped)
    mkpath(REPORT_DIR)
    i = 1
    while isfile("$REPORT_PREFIX $i.md"); i += 1; end
    path = "$REPORT_PREFIX $i.md"
    open(path, "w") do io
        redirect_stdout(io) do
            println("```")   # fixed-width report — fence it so markdown keeps the columns
            print_header()
            print_config(scope_entries)
            print_results_table(results)
            print_footer(results, total_s, n_skipped)
            println("```")
        end
    end
    return path
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Scope entry — bundles everything a run needs
# ═══════════════════════════════════════════════════════════════════════════════

struct ScopeEntry
    label ::String           # printed in the table
    scope ::Any              # Dagger scope
    procs ::Vector           # ROCArrayDeviceProcs used — needed for gpu_synchronize
end

function build_scope_entries(all_procs, selection)
    device_ids = selection === :all ? [p.device_id for p in all_procs] : selection

    available = Set(p.device_id for p in all_procs)
    for id in device_ids
        id in available || error("GPU device $id not found. Available: $(sort(collect(available)))")
    end

    entries = ScopeEntry[]

    if length(device_ids) == 1
        # Single device — one individual entry
        id   = only(device_ids)
        proc = only(filter(p -> p.device_id == id, all_procs))
        push!(entries, ScopeEntry("GPU $id", single_gpu_scope(id), [proc]))

    elseif selection === :all
        # :all → every GPU individually, then combined
        for id in device_ids
            proc = only(filter(p -> p.device_id == id, all_procs))
            push!(entries, ScopeEntry("GPU $id", single_gpu_scope(id), [proc]))
        end
        multi_procs = filter(p -> p.device_id in device_ids, all_procs)
        push!(entries, ScopeEntry(
            "Multi-GPU ($(join(device_ids, "+")))",
            multi_gpu_scope(device_ids),
            multi_procs,
        ))

    else
        # Explicit list of 2+ IDs → combined scope only, no individual tests
        multi_procs = filter(p -> p.device_id in device_ids, all_procs)
        push!(entries, ScopeEntry(
            "Multi-GPU ($(join(device_ids, "+")))",
            multi_gpu_scope(device_ids),
            multi_procs,
        ))
    end

    return entries
end

# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════

function main()
    AMDGPU.functional() || error("AMDGPU is not functional. Check your ROCm driver and installation.")

    all_procs = discover_gpu_procs()
    isempty(all_procs) &&
        error("No ROCm GPU processors found in Dagger. Is the ROCExt extension loaded?")

    rocext = Base.get_extension(Dagger, :ROCExt)
    rocext.stream_strategy!(STREAM_STRATEGY)
    println("  Stream strategy : $STREAM_STRATEGY")

    scope_entries = build_scope_entries(all_procs, GPU_SELECTION)

    print_header()
    print_gpu_table(all_procs)
    print_config(scope_entries)

    results   = BenchResult[]
    n_skipped = 0
    t_start   = time_ns()

    n_ops   = RUN_MATMUL + RUN_TRANSPOSE + RUN_ELEMENTWISE +
              (RUN_DAG ? length(DAG_TOPOLOGIES) : 0)
    total   = length(MATRIX_SIZES) * length(BLOCK_SIZES) * n_ops * length(scope_entries)
    idx     = 0

    for (rows, cols) in MATRIX_SIZES
        for (blk_r, blk_c) in BLOCK_SIZES

            eff_blk_r = min(blk_r, rows)
            eff_blk_c = min(blk_c, cols)

            for entry in scope_entries

                # ── MatMul ────────────────────────────────────────────────
                if RUN_MATMUL
                    idx  += 1
                    inner = cols   # A(rows×inner) * B(inner×cols)

                    @printf("\r  [%d/%d] MatMul %dx%d  blk=%dx%d  scope=%s …%-10s",
                        idx, total, rows, cols, eff_blk_r, eff_blk_c, entry.label, "")
                    flush(stdout)

                    force_reclaim!(entry.procs)

                    if !enough_vram(entry.procs)
                        @printf("\r  [%d/%d] SKIPPED (low VRAM)  scope=%s\n",
                            idx, total, entry.label)
                        n_skipped += 1
                    else
                        correct = verify_matmul(rows, inner, cols,
                                                eff_blk_r, eff_blk_c, entry.scope)

                        stats = bench_function(entry.procs) do
                            run_matmul(rows, inner, cols, eff_blk_r, eff_blk_c, entry.scope)
                        end

                        push!(results, BenchResult(
                            "MatMul", entry.label,
                            (rows, inner), (inner, cols),
                            (eff_blk_r, eff_blk_c),
                            stats.min, stats.mean, stats.max, stats.std, stats.median,
                            gflops_matmul(rows, inner, cols, stats.mean),
                            correct,
                            stats.util_peak, stats.util_mean,
                        ))
                    end
                end

                # ── Transpose ─────────────────────────────────────────────
                if RUN_TRANSPOSE
                    idx += 1

                    @printf("\r  [%d/%d] Transpose %dx%d  blk=%dx%d  scope=%s …%-10s",
                        idx, total, rows, cols, eff_blk_r, eff_blk_c, entry.label, "")
                    flush(stdout)

                    force_reclaim!(entry.procs)

                    if !enough_vram(entry.procs)
                        @printf("\r  [%d/%d] SKIPPED (low VRAM)  scope=%s\n",
                            idx, total, entry.label)
                        n_skipped += 1
                    else
                        stats = bench_function(entry.procs) do
                            run_transpose(rows, cols, eff_blk_r, eff_blk_c, entry.scope)
                        end

                        push!(results, BenchResult(
                            "Transpose", entry.label,
                            (rows, cols), nothing,
                            (eff_blk_r, eff_blk_c),
                            stats.min, stats.mean, stats.max, stats.std, stats.median,
                            gflops_transpose(rows, cols, stats.mean),
                            nothing,
                            stats.util_peak, stats.util_mean,
                        ))
                    end
                end

                # ── Elementwise ───────────────────────────────────────────
                if RUN_ELEMENTWISE
                    idx += 1

                    @printf("\r  [%d/%d] Elementwise %dx%d  blk=%dx%d  scope=%s …%-10s",
                        idx, total, rows, cols, eff_blk_r, eff_blk_c, entry.label, "")
                    flush(stdout)

                    force_reclaim!(entry.procs)

                    if !enough_vram(entry.procs)
                        @printf("\r  [%d/%d] SKIPPED (low VRAM)  scope=%s\n",
                            idx, total, entry.label)
                        n_skipped += 1
                    else
                        stats = bench_function(entry.procs) do
                            run_elementwise(rows, cols, eff_blk_r, eff_blk_c, entry.scope)
                        end

                        push!(results, BenchResult(
                            "Elementwise", entry.label,
                            (rows, cols), nothing,
                            (eff_blk_r, eff_blk_c),
                            stats.min, stats.mean, stats.max, stats.std, stats.median,
                            gflops_elementwise(rows, cols, stats.mean),
                            nothing,
                            stats.util_peak, stats.util_mean,
                        ))
                    end
                end

                # ── DAG topologies ────────────────────────────────────────
                if RUN_DAG
                    for topo in DAG_TOPOLOGIES
                        idx += 1

                        @printf("\r  [%d/%d] DAG:%s %dx%d  blk=%dx%d  scope=%s …%-10s",
                            idx, total, topo, rows, rows, eff_blk_r, eff_blk_r, entry.label, "")
                        flush(stdout)

                        force_reclaim!(entry.procs)

                        if !enough_vram(entry.procs)
                            @printf("\r  [%d/%d] SKIPPED (low VRAM)  scope=%s\n",
                                idx, total, entry.label)
                            n_skipped += 1
                        else
                            stats = bench_function(entry.procs) do
                                run_dag(topo, rows, eff_blk_r, entry.scope)
                            end

                            push!(results, BenchResult(
                                "DAG:$topo", entry.label,
                                (rows, rows), nothing,
                                (eff_blk_r, eff_blk_r),
                                stats.min, stats.mean, stats.max, stats.std, stats.median,
                                dag_matmuls(topo) * gflops_matmul(rows, rows, rows, stats.mean),
                                nothing,
                                stats.util_peak, stats.util_mean,
                            ))
                        end
                    end
                end

            end  # scope_entries
        end  # block sizes
    end  # matrix sizes

    # ── Saturate: one standalone run per scope, outside the config grid ────────
    # Runs once, at a fixed shape, with as many concurrent problems as VRAM takes.
    if RUN_SATURATE
        for entry in scope_entries
            force_reclaim!(entry.procs)
            count = saturate_count(entry.procs)

            @printf("\r  Saturate×%d  %dx%d  blk=%dx%d  scope=%s …%-10s",
                count, SATURATE_SIZE, SATURATE_SIZE,
                SATURATE_BLOCK, SATURATE_BLOCK, entry.label, "")
            flush(stdout)

            # One warm-up (compilation would swamp the numbers), one timed run.
            stats = bench_function(entry.procs; warmup = 1, samples = 1) do
                run_saturate(count, entry.scope)
            end

            push!(results, BenchResult(
                "Saturate×$count", entry.label,
                (SATURATE_SIZE, SATURATE_SIZE), nothing,
                (SATURATE_BLOCK, SATURATE_BLOCK),
                stats.min, stats.mean, stats.max, stats.std, stats.median,
                count * gflops_matmul(SATURATE_SIZE, SATURATE_SIZE, SATURATE_SIZE, stats.mean),
                nothing,
                stats.util_peak, stats.util_mean,
            ))
        end
    end

    total_s = (time_ns() - t_start) / 1e9

    # Clear the progress line before printing the table
    print("\r" * " "^LINE_WIDTH * "\r")
    print_results_table(results)
    print_footer(results, total_s, n_skipped)

    path = write_report(results, scope_entries, total_s, n_skipped)
    println("  Report written to: $path")

    return results
end

# ─── Entry point ──────────────────────────────────────────────────────────────
results = main()
