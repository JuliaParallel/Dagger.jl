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

RUN_MATMUL / RUN_TRANSPOSE / RUN_ELEMENTWISE / RUN_SATURATE → toggle operations

N.B. AMDGPU has no NVML equivalent, so the GPU-utilization columns report N/A.
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
const STREAM_STRATEGY = :random

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

const RUN_MATMUL        = false   # A * B
const RUN_TRANSPOSE     = false  # A'
const RUN_ELEMENTWISE   = false  # A .* B
const RUN_SATURATE      = true   # N independent A*B launched concurrently to fill every stream

# Saturation: number of independent matmuls launched at once. Each keeps a slice
# of the stream pool busy; enough of them saturate the whole GPU. Peak VRAM is
# ~SATURATE_COUNT × 3 matrices, so raise it only as far as VRAM allows.
const SATURATE_COUNT    = 4

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

# No NVML on ROCm — utilization is not sampled; report N/A.
_sample_util(device_ids::Vector{Int}) = NaN

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
        std       = std(times_s),
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

# Launch `count` independent matmuls concurrently. `mul!` blocks its calling
# task (its datadeps region waits), so each problem runs in its own Julia task;
# the regions touch disjoint memory and overlap, keeping every stream busy at
# once — this is what saturates the whole GPU.
function run_saturate(rows, inner, cols, blk_r, blk_c, scope; count = SATURATE_COUNT)
    results = Vector{Any}(undef, count)
    @sync for i in 1:count
        Threads.@spawn begin
            Dagger.with_options(; scope) do
                A = rand(Blocks(blk_r, blk_c), Float32, rows, inner)
                B = rand(Blocks(blk_c, blk_c), Float32, inner, cols)
                C = rand(Blocks(blk_c, blk_c), Float32, rows, cols)
                mul!(C, A, B)
                results[i] = collect(C)
                free_darray!(A); free_darray!(B); free_darray!(C)
            end
        end
    end
    return results
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

function print_config(scope_entries)
    ops = filter(!isempty, [
        RUN_MATMUL      ? "MatMul"                    : "",
        RUN_TRANSPOSE   ? "Transpose"                 : "",
        RUN_ELEMENTWISE ? "Elementwise"               : "",
        RUN_SATURATE    ? "Saturate×$SATURATE_COUNT"  : "",
    ])
    labels = [e.label for e in scope_entries]
    println("  BENCHMARK CONFIGURATION")
    println(banner('─'))
    println("  Scopes          : $(join(labels, "  |  "))")
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
    @printf("  Configs run     : %d   skipped (low VRAM): %d\n", n_run, n_skipped)
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

    n_ops   = RUN_MATMUL + RUN_TRANSPOSE + RUN_ELEMENTWISE + RUN_SATURATE
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

                # ── Saturate (concurrent independent matmuls) ─────────────
                if RUN_SATURATE
                    idx  += 1
                    inner = cols

                    @printf("\r  [%d/%d] Saturate×%d %dx%d  blk=%dx%d  scope=%s …%-10s",
                        idx, total, SATURATE_COUNT, rows, cols, eff_blk_r, eff_blk_c, entry.label, "")
                    flush(stdout)

                    force_reclaim!(entry.procs)

                    if !enough_vram(entry.procs)
                        @printf("\r  [%d/%d] SKIPPED (low VRAM)  scope=%s\n",
                            idx, total, entry.label)
                        n_skipped += 1
                    else
                        stats = bench_function(entry.procs) do
                            run_saturate(rows, inner, cols, eff_blk_r, eff_blk_c, entry.scope)
                        end

                        push!(results, BenchResult(
                            "Saturate×$SATURATE_COUNT", entry.label,
                            (rows, inner), (inner, cols),
                            (eff_blk_r, eff_blk_c),
                            stats.min, stats.mean, stats.max, stats.std, stats.median,
                            SATURATE_COUNT * gflops_matmul(rows, inner, cols, stats.mean),
                            nothing,
                            stats.util_peak, stats.util_mean,
                        ))
                    end
                end

            end  # scope_entries
        end  # block sizes
    end  # matrix sizes

    total_s = (time_ns() - t_start) / 1e9

    # Clear the progress line before printing the table
    print("\r" * " "^LINE_WIDTH * "\r")
    print_results_table(results)
    print_footer(results, total_s, n_skipped)

    return results
end

# ─── Entry point ──────────────────────────────────────────────────────────────
results = main()
