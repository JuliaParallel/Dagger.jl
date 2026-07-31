# ══════════════════════════════════════════════════════════════════════════════
#  Unified Dagger.jl multi-stream scheduling benchmark  (CUDA + ROCm, one file)
# ══════════════════════════════════════════════════════════════════════════════
#
#  Fixes the methodology problems of the old benchmarkCUDA/ROCM scripts:
#   1. Times ONLY the `spawn_datadeps` region — allocation and `collect` are OUT
#      of the timed block (they were confounding every number, esp. AMD `fake_rand`
#      which allocates on the CPU).
#   2. Measures GPU-busy directly: `gpu_work_ms / wall_ms`, where gpu_work_ms is a
#      measured single-tile-gemm kernel time × task count. Portable, no NVML.
#      >1 ⇒ streams overlap (device-bound);  <1 ⇒ GPU idle (host/launch-bound).
#   3. Stream-count sweep INCLUDES 1 (the single-stream / upstream baseline).
#   4. Tile-size sweep at fixed matrix ⇒ the host↔device "granularity crossover".
#   5. Reports each DAG shape's structure (nodes, critical path, task count).
#   6. Robust stats: median / min / IQR over many samples, with warmup.
#   7. Writes CSV (test/benchresults/unified_<backend>_<ts>.csv) for plotting.
#
#  Run:
#    julia --project=test test/benchmark.jl
#  Choose the backend via the `BACKEND` constant below, or the env var:
#    DAGGER_BENCH_BACKEND=CUDA julia --project=test test/benchmark.jl   # NVIDIA
#    DAGGER_BENCH_BACKEND=ROC  julia --project=test test/benchmark.jl   # AMD
#    DAGGER_BENCH_BACKEND=both julia --project=test test/benchmark.jl   # both, sequentially
#
#  Cross-stream EVENT counts (#5 in the review) need a counter in the extension.
#  If EXT defines `_EVENT_COUNT::Threads.Atomic{Int}`, it is read automatically;
#  otherwise that column is blank. (3-line add to CUDAExt/ROCExt `execute!`.)
# ══════════════════════════════════════════════════════════════════════════════

# ROCm on gfx1032 needs these BEFORE AMDGPU loads (harmless under CUDA). Override
# by exporting them yourself; delete if your AMD arch is natively supported.
get!(ENV, "HSA_OVERRIDE_GFX_VERSION", "10.3.0")
get!(ENV, "ROCBLAS_DEVICE_MEMORY_SIZE", "0")

# ═══ Backend selection ════════════════════════════════════════════════════════
# Edit the default here, or override with the DAGGER_BENCH_BACKEND env var:
#   :auto  → first functional GPU (CUDA preferred)
#   :CUDA  → NVIDIA only        :ROC → AMD only
#   :both  → run CUDA then ROC, each in its OWN process (clean timing, one CSV each)
const BACKEND = Symbol(get(ENV, "DAGGER_BENCH_BACKEND", "auto"))

# `:both` is just a driver: relaunch this script once per backend, then stop.
# Separate processes keep each run's launch-bound timings free of the other's
# host contention, and each backend loads only its own GPU stack. The env guard
# stops a spawned child (which always has DAGGER_BENCH_BACKEND set) from re-entering
# the driver — so even a hardcoded `:both` can't fork-bomb.
if BACKEND === :both && !(get(ENV, "DAGGER_BENCH_BACKEND", "") in ("CUDA", "ROC"))
    proj = dirname(Base.active_project())
    for b in ("CUDA", "ROC")
        println("\n" * "█"^78 * "\n██  launching $b backend\n" * "█"^78)
        p = run(ignorestatus(addenv(`$(Base.julia_cmd()) --project=$proj $(@__FILE__)`,
                                    "DAGGER_BENCH_BACKEND" => b)))
        success(p) || @warn "$b backend exited with failure (continuing to next)"
    end
    exit(0)
end

using Dagger, LinearAlgebra, Statistics, Printf, Dates
import LinearAlgebra: BLAS

# ─── Load & verify the chosen backend ─────────────────────────────────────────
const _BK = Ref{Symbol}(:none)
if BACKEND in (:auto, :CUDA)
    try; @eval using CUDA;   CUDA.functional()   && (_BK[] = :CUDA); catch e; BACKEND === :CUDA && rethrow(e); end
end
if _BK[] === :none && BACKEND in (:auto, :ROC)
    try; @eval using AMDGPU; AMDGPU.functional() && (_BK[] = :ROC);  catch e; BACKEND === :ROC && rethrow(e); end
end
_BK[] === :none && error("No functional GPU backend for BACKEND=:$BACKEND (use :auto, :CUDA, :ROC, or :both).")

const B   = _BK[]
const GPU = B === :CUDA ? CUDA : AMDGPU                                   # only the taken side is evaluated
const EXT = B === :CUDA ? Base.get_extension(Dagger, :CUDAExt) : Base.get_extension(Dagger, :ROCExt)

# Backend-specific bits. Function bodies resolve module names lazily, so the
# untaken branch is safe even though its module isn't loaded. Only the @elapsed
# MACRO must be built per-backend (macros expand eagerly) — hence @eval.
if B === :CUDA
    scope_for(d) = Dagger.scope(worker = 1, cuda_gpu = d + 1)   # CUDA scope keys are 1-based
    dev_id(p)    = p.device
    new_stream() = CUDA.CuStream()
else
    scope_for(d) = Dagger.scope(worker = 1, rocm_gpu = d)       # ROCm scope keys are raw device_id
    dev_id(p)    = p.device_id
    new_stream() = AMDGPU.HIPStream()
end
@eval gpu_elapsed(f) = $(B === :CUDA ? :(CUDA.@elapsed f()) : :(AMDGPU.@elapsed f()))  # seconds

const PROC  = first(sort(collect(filter(p -> p isa Dagger.gpu_processor(Val(B)),
                                        Dagger.get_processors(Dagger.OSProc()))); by = dev_id))
const DEVID = dev_id(PROC)
const SCOPE = scope_for(DEVID)
const GPU_LABEL = replace(Dagger.short_name(PROC), ',' => ';')   # keep commas out of the CSV

gpu_sync() = (Dagger.gpu_synchronize(PROC); GPU.synchronize())
set_strategy!(s) = EXT.stream_strategy!(s)
clear_syncdeps!() = EXT.clear_stream_syncdeps!()
read_events() = isdefined(EXT, :_EVENT_COUNT) ? getfield(EXT, :_EVENT_COUNT)[] : missing

# Rebuild the device's stream pool to `n` streams. Clears the producer→stream map
# first: stale (dev, stream_idx) entries could index past a now-shorter pool.
# No-op when already at `n` (avoids recreating streams every config in a sweep).
const _CUR_STREAMS = Ref(0)
function set_stream_count!(n::Int)
    n == _CUR_STREAMS[] && return n
    clear_syncdeps!()
    GPU.device!(EXT.DEVICES[DEVID])
    EXT.STREAMS[DEVID] = [new_stream() for _ in 1:n]
    # Per-stream in-flight completion-event queues (device occupancy for SDQ).
    EXT.STREAM_INFLIGHT[DEVID] = [B === :CUDA ? CUDA.CuEvent[] : AMDGPU.HIP.HIPEvent[] for _ in 1:n]
    EXT.STREAM_GEN[DEVID] = zeros(UInt64, n)
    EXT.LAST_WAITED[DEVID] = [Dict{Tuple{Int,Int}, UInt64}() for _ in 1:n]
    EXT.ROUNDROBIN[DEVID] = Threads.Atomic{Int}(1)
    _CUR_STREAMS[] = n
    return n
end

# ═══ Configuration ════════════════════════════════════════════════════════════
# Poster profile: full story (crossover, locality win, stream-count effect, N=1
# baseline, host-vs-device contrast) in ~45 min/backend. MAX_TASKS skips the
# tile-512 DAG monsters (100+ s/region — a noisy, low-info all-collapse regime);
# the extreme host-bound point is still shown by matmul-512. Raise MAX_TASKS to
# ~20_000 to include them (adds hours).
const MATRIX        = 4096                       # square side
const TILE_SIZES    = [512, 1024, 2048]          # granularity crossover (matmul spans all three)
const STREAM_COUNTS = [1, 2, 8, 32]              # 1 = single-stream (upstream) baseline
const STRATEGIES    = [:roundrobin, :random, :sdq, :locality]   # the comparison IS the point
const SHAPES        = [:matmul, :linear, :diamond, :chainlink, :tangled]
const SATURATE_K    = 4                           # :saturate = K independent matmuls (wide DAG)

const SAMPLES       = 7
const WARMUP        = 5                            # ≥5 tames the post-stream-rebuild median noise
const CHAIN_LEN     = 4                            # nodes in :linear, diamonds in :chainlink
const MAX_TASKS     = 1_200                        # skip tile-512 DAGs (≥1536 tasks); keeps chainlink-1024 (576)

const RUN_TILE_SWEEP   = true                      # tile × strategy   @ FIXED_STREAMS
const FIXED_STREAMS    = 8
const RUN_STREAM_SWEEP = true                      # streams × strategy @ FIXED_TILE (incl. saturate)
const FIXED_TILE       = 1024

# ═══ DAG shapes — per-tile gemm into the current datadeps region ═══════════════
function dag_gemm!(C, A, B)                         # C := A·B, tile by tile (→ CUBLAS/rocBLAS)
    Ac, Bc, Cc = A.chunks, B.chunks, C.chunks
    Ant = size(Ac, 2)
    for n in axes(Cc, 2), m in axes(Cc, 1), k in 1:Ant
        beta = k == 1 ? 0f0 : 1f0
        Dagger.@spawn BLAS.gemm!('N', 'N', 1f0,
            Dagger.In(Ac[m, k]), Dagger.In(Bc[k, n]), beta, Dagger.InOut(Cc[m, n]))
    end
end

# node topology per shape: (nodes, critical-path in nodes, max independent nodes)
function topo(shape)
    shape === :matmul    ? (1, 1, 1) :
    shape === :linear    ? (CHAIN_LEN, CHAIN_LEN, 1) :
    shape === :diamond   ? (3, 2, 2) :
    shape === :chainlink ? (3 * CHAIN_LEN, 2 * CHAIN_LEN, 2) :
    shape === :tangled   ? (5, 3, 2) :
    shape === :saturate  ? (SATURATE_K, 1, SATURATE_K) :
    error("unknown shape $shape")
end

# Pre-allocate everything OUTSIDE timing; return a zero-alloc `run!` closure.
function make_shape(shape, tile)
    mats = DArray[]
    A!() = (x = rand(Blocks(tile, tile), Float32, MATRIX, MATRIX); push!(mats, x); x)

    run! =
        if shape === :matmul
            C, X, Y = A!(), A!(), A!()
            () -> Dagger.spawn_datadeps() do; dag_gemm!(C, X, Y) end
        elseif shape === :linear
            X = A!(); M = A!(); outs = [A!() for _ in 1:CHAIN_LEN]
            () -> Dagger.spawn_datadeps() do
                cur = X; for o in outs; dag_gemm!(o, cur, M); cur = o end
            end
        elseif shape === :diamond
            S = A!(); M1 = A!(); M2 = A!(); B1 = A!(); B2 = A!(); J = A!()
            () -> Dagger.spawn_datadeps() do
                dag_gemm!(B1, S, M1); dag_gemm!(B2, S, M2); dag_gemm!(J, B1, B2)
            end
        elseif shape === :chainlink
            S = A!(); M1 = A!(); M2 = A!()
            st = [(A!(), A!(), A!()) for _ in 1:CHAIN_LEN]
            () -> Dagger.spawn_datadeps() do
                cur = S
                for (b1, b2, sn) in st
                    dag_gemm!(b1, cur, M1); dag_gemm!(b2, cur, M2); dag_gemm!(sn, b1, b2); cur = sn
                end
            end
        elseif shape === :tangled
            S1 = A!(); S2 = A!(); T1 = A!(); T2 = A!(); U1 = A!(); U2 = A!(); J = A!()
            () -> Dagger.spawn_datadeps() do
                dag_gemm!(T1, S1, S2); dag_gemm!(T2, S2, S1)
                dag_gemm!(U1, T1, T2); dag_gemm!(U2, T2, T1); dag_gemm!(J, U1, U2)
            end
        elseif shape === :saturate
            tri = [(A!(), A!(), A!()) for _ in 1:SATURATE_K]
            () -> Dagger.spawn_datadeps() do
                for (C, X, Y) in tri; dag_gemm!(C, X, Y) end
            end
        else
            error("unknown shape $shape")
        end

    free! = () -> foreach(Dagger.unsafe_free!, mats)
    nodes = topo(shape)[1]
    n_tasks = nodes * cld(MATRIX, tile)^3         # gemm tasks = nodes × (tiles/side)³
    return run!, free!, n_tasks
end

# ═══ Measurement ══════════════════════════════════════════════════════════════
const _TILE_MS = Dict{Int,Float64}()
function tile_kernel_ms(tile)                       # one standalone tile-gemm kernel, GPU time
    get!(_TILE_MS, tile) do
        GPU.device!(EXT.DEVICES[DEVID])
        a = GPU.ones(Float32, tile, tile); b = GPU.ones(Float32, tile, tile); c = GPU.ones(Float32, tile, tile)
        mul!(c, a, b); GPU.synchronize()            # compile
        t = minimum(gpu_elapsed(() -> (mul!(c, a, b); nothing)) for _ in 1:5) * 1000
        GPU.unsafe_free!(a); GPU.unsafe_free!(b); GPU.unsafe_free!(c)
        t
    end
end

iqr(x) = quantile(x, 0.75) - quantile(x, 0.25)

function time_region(run!)                          # times ONLY the datadeps region (+ device sync)
    for _ in 1:WARMUP
        run!(); gpu_sync()
        # Multistream CUDAExt currently retains tens of GiB/run until a full GC
        # (outside the CUDA.jl pool). Reclaim between iterations to avoid OOM.
        GC.gc(true)
        try; GPU.reclaim(); catch; end
    end
    ts = Float64[]
    for _ in 1:SAMPLES
        gpu_sync()
        t0 = time_ns(); run!(); gpu_sync()
        push!(ts, (time_ns() - t0) / 1e6)           # ms
        GC.gc(true)
        try; GPU.reclaim(); catch; end
    end
    (median(ts), minimum(ts), iqr(ts))
end

# ═══ One measured config ══════════════════════════════════════════════════════
mutable struct Row
    sweep::String; shape::Symbol; tile::Int; streams::Int; strategy::Symbol
    nodes::Int; crit::Int; n_tasks::Int
    wall_med::Float64; wall_min::Float64; wall_iqr::Float64
    tile_ms::Float64; gpu_work::Float64; busy::Float64; gflops::Float64; gflops_min::Float64
    events::Union{Int,Missing}
end

function measure(sweep, shape, tile, streams, strat)
    nodes, crit, _ = topo(shape)
    n_tasks = nodes * cld(MATRIX, tile)^3
    n_tasks > MAX_TASKS && return nothing           # skip pathological explosions (raise MAX_TASKS to include)

    set_stream_count!(streams); set_strategy!(strat); clear_syncdeps!()
    ev0 = read_events()

    med, mn, ir = Dagger.with_options(; scope = SCOPE) do
        run!, free!, _ = make_shape(shape, tile)
        try; time_region(run!) finally free!() end
    end
    # Drop device caches between configs so long sweeps don't trip OOM on shared nodes.
    GC.gc(true)
    try; GPU.reclaim(); catch; end
    gpu_sync()

    tms      = tile_kernel_ms(tile)
    gpu_work = tms * n_tasks
    busy     = gpu_work / med
    gflops     = n_tasks * 2.0 * tile^3 / (med / 1000) / 1e9   # achieved FLOP/s at the median wall
    gflops_min = n_tasks * 2.0 * tile^3 / (mn  / 1000) / 1e9   # peak FLOP/s at the best-case wall
    ev1      = read_events()
    # counter accrues over all WARMUP+SAMPLES region runs → normalize to per-region
    events   = (ev0 === missing || ev1 === missing) ? missing :
               round(Int, (ev1 - ev0) / (WARMUP + SAMPLES))

    Row(sweep, shape, tile, streams, strat, nodes, crit, n_tasks,
        med, mn, ir, tms, gpu_work, busy, gflops, gflops_min, events)
end

# ═══ Correctness (once) ═══════════════════════════════════════════════════════
function verify()
    set_stream_count!(FIXED_STREAMS); set_strategy!(:locality)
    ok = Dagger.with_options(; scope = SCOPE) do
        A = rand(Blocks(512, 512), Float32, 1024, 1024)
        Bm = rand(Blocks(512, 512), Float32, 1024, 1024)
        C = rand(Blocks(512, 512), Float32, 1024, 1024)
        Ah, Bh = collect(A), collect(Bm)
        Dagger.spawn_datadeps() do; dag_gemm!(C, A, Bm) end
        r = collect(C) ≈ Ah * Bh
        foreach(Dagger.unsafe_free!, (A, Bm, C)); r
    end
    println("  correctness (:locality tiled matmul vs CPU): ", ok ? "PASS ✓" : "FAIL ✗")
    ok
end

# ═══ Output ═══════════════════════════════════════════════════════════════════
const CSV_HEADER = "backend,gpu,sweep,shape,matrix,tile,tiles_per_side,nodes,crit_path,n_tasks," *
                   "streams,strategy,samples,wall_median_ms,wall_min_ms,wall_iqr_ms," *
                   "tile_kernel_ms,gpu_work_ms,busy_fraction,gflops,gflops_min,events\n"

csv_line(r::Row) = @sprintf("%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%s,%d,%.4f,%.4f,%.4f,%.5f,%.2f,%.4f,%.2f,%.2f,%s\n",
    string(B), GPU_LABEL, r.sweep, r.shape, MATRIX, r.tile, cld(MATRIX, r.tile),
    r.nodes, r.crit, r.n_tasks, r.streams, r.strategy, SAMPLES,
    r.wall_med, r.wall_min, r.wall_iqr, r.tile_ms, r.gpu_work, r.busy, r.gflops, r.gflops_min,
    r.events === missing ? "" : string(r.events))

function print_row(i, n, r::Row)
    @printf("  [%3d/%3d] %-8s %-10s t=%-4d str=%-2d nodes=%-2d %-11s  %8.1f ms (min %7.1f)  busy=%5.2f  %8.1f GF%s\n",
        i, n, r.sweep, r.shape, r.tile, r.streams, r.nodes, r.strategy,
        r.wall_med, r.wall_min, r.busy, r.gflops,
        r.events === missing ? "" : @sprintf("  ev=%d", r.events))
    flush(stdout)                                   # show progress live even when redirected to a file
end

# How many configs will actually run (after MAX_TASKS skips) — denominator for i/N.
function total_configs()
    fits(shape, tile) = topo(shape)[1] * cld(MATRIX, tile)^3 <= MAX_TASKS
    n = 0
    if RUN_TILE_SWEEP
        for shape in SHAPES, tile in TILE_SIZES, _ in STRATEGIES
            fits(shape, tile) && (n += 1)
        end
    end
    if RUN_STREAM_SWEEP
        for shape in vcat(SHAPES, :saturate), _ in STREAM_COUNTS, _ in STRATEGIES
            fits(shape, FIXED_TILE) && (n += 1)
        end
    end
    n
end

# ═══ Main ═════════════════════════════════════════════════════════════════════
function main()
    println("═"^90)
    println("  Dagger multi-stream benchmark  ·  backend=$B  ·  $(Dagger.short_name(PROC))")
    println("  Julia $(VERSION)  ·  Dagger $(pkgversion(Dagger))  ·  $(B == :CUDA ? "CUDA.jl" : "AMDGPU.jl") $(pkgversion(GPU))")
    println("  matrix=$(MATRIX)²  samples=$SAMPLES  warmup=$WARMUP  max_tasks=$MAX_TASKS")
    println("═"^90)
    verify()

    mkpath(joinpath(@__DIR__, "benchresults"))
    path = joinpath(@__DIR__, "benchresults",
                    "unified_$(B)_$(Dates.format(now(), "yyyymmdd_HHMMSS")).csv")
    io = open(path, "w"); write(io, CSV_HEADER)
    rows = Row[]
    total = total_configs()
    done = Ref(0)
    println("  running $total configs (any > $MAX_TASKS tasks are skipped)")
    emit(r) = (r === nothing && return; done[] += 1; push!(rows, r);
               write(io, csv_line(r)); flush(io); print_row(done[], total, r))

    if RUN_TILE_SWEEP
        println("\n── Tile-granularity sweep  (streams=$FIXED_STREAMS)  → host↔device crossover ──"); flush(stdout)
        for shape in SHAPES, tile in TILE_SIZES, strat in STRATEGIES
            emit(measure("tile", shape, tile, FIXED_STREAMS, strat))
        end
    end
    if RUN_STREAM_SWEEP
        println("\n── Stream-count sweep  (tile=$FIXED_TILE, incl. 1 = single-stream baseline) ──"); flush(stdout)
        for shape in vcat(SHAPES, :saturate), nstr in STREAM_COUNTS, strat in STRATEGIES
            emit(measure("streams", shape, FIXED_TILE, nstr, strat))
        end
    end

    close(io)
    println("\n", "═"^90)
    println("  $(length(rows)) configs measured.  CSV → $path")
    println("  Plot ideas: busy_fraction & gflops vs tile (crossover); gflops vs streams per strategy;")
    println("              gflops by strategy per shape.  busy>1 ⇒ overlap/device-bound, <1 ⇒ host-bound.")
    println("═"^90)
    return rows
end

main()
