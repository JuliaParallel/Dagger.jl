# Paper Table-1 benchmark grid (ROSS 2026). Two regimes (single-node CPU+1×H100,
# single-node CPU+2×H100), 2 workloads × 3 tile counts × 5 schedulers × 7 seeds.
# Reuses driver.jl building blocks (build_inputs, run_workload!, TimedScheduler,
# verify_workload, warm_*). Env: REGIME (a/b), OUTCSV, optional SMOKE=1.
using CUDA, Dagger, HiGHS, JuMP, LinearAlgebra, Statistics, Random, Printf
include(joinpath(@__DIR__, "driver.jl"))

# --- config ---------------------------------------------------------------
const WORKLOADS = [:cholesky, :matmul]
const NTS       = [2, 4, 8]
const BS_FOR_NT = Dict(2 => 4096, 4 => 4096, 8 => 2048)   # nt=8 smaller to fit VRAM
const SEEDS     = 1:7
const HEUR_TL   = 60.0   # PSZ fair-comparison cap for IG/SA (matches MILP)
const MILP_TL   = 60.0
const SCHED_NAMES = ["RoundRobinScheduler", "GreedyScheduler",
                     "IteratedGreedyScheduler", "SimulatedAnnealingScheduler",
                     "JuMPScheduler"]
const RESID_GATE = Dict(:cholesky => 1e-8, :matmul => 1e-10)

# SA's inner MUST be IG (paper §sa Algorithm 3); constructed explicitly.
function make_scheduler(name::String, seed::Int)
    if name == "RoundRobinScheduler"
        return Dagger.RoundRobinScheduler()
    elseif name == "GreedyScheduler"
        return Dagger.GreedyScheduler()
    elseif name == "IteratedGreedyScheduler"
        return Dagger.IteratedGreedyScheduler(; rng=MersenneTwister(seed),
                                              time_limit_sec=HEUR_TL)
    elseif name == "SimulatedAnnealingScheduler"
        return Dagger.SimulatedAnnealingScheduler(
            Dagger.IteratedGreedyScheduler(; rng=MersenneTwister(seed),
                                           time_limit_sec=HEUR_TL);
            rng=MersenneTwister(seed), time_limit_sec=HEUR_TL)
    elseif name == "JuMPScheduler"
        return Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=MILP_TL)
    end
    error("unknown scheduler $name")
end

milp_ext() = Base.get_extension(Dagger, :JuMPExt)
function reset_milp_info!()
    ext = milp_ext(); ext === nothing || (ext.LAST_MILP_SOLVE[] = ("NONE", NaN))
end
function read_milp_info()
    ext = milp_ext(); ext === nothing && return ("NA", NaN)
    return ext.LAST_MILP_SOLVE[]
end

# n_tasks (full-DAG K) captured once per (workload, nt, bs): deterministic in
# the workload topology. Uses a capturing scheduler that records the largest
# AOT DAG (the flat CPU+GPU DAG; hierarchical partitions would be smaller).
mutable struct NTCap <: Dagger.DataDepsScheduler
    inner::Dagger.GreedyScheduler
    maxn::Base.RefValue{Int}
end
function Dagger.datadeps_schedule_dag_aot!(s::NTCap, sc, dag, pr, sco)
    s.maxn[] = max(s.maxn[], Dagger.nv(dag.g))
    Dagger.datadeps_schedule_dag_aot!(s.inner, sc, dag, pr, sco)
end
Dagger.datadeps_schedule_task_jit!(s::NTCap, a...) = Dagger.datadeps_schedule_task_jit!(s.inner, a...)
Dagger.datadeps_schedule_cache(s::NTCap) = Dagger.datadeps_schedule_cache(s.inner)
Base.similar(s::NTCap) = NTCap(similar(s.inner), s.maxn)
function capture_ntasks(wl, nt, bs)
    cap = NTCap(Dagger.GreedyScheduler(), Ref(0))
    run_workload!(wl, build_inputs(wl, nt, bs), cap)
    empty!(Dagger.datadeps_schedule_cache(cap))   # don't leak into measured cells
    return cap.maxn[]
end

# One measured cell. Returns a NamedTuple row (or an :EXCEPTION row).
function run_cell(regime, wl, nt, bs, name, seed, ntk)
    # Metrics cache is warmed once per config (see `main`); measured cells then
    # keep enriching it. The global `metrics_cache_max_tasks!(5000)` bound set
    # by driver.jl trims on every write, so no per-cell trim is needed.
    inputs = build_inputs(wl, nt, bs)
    sched  = make_scheduler(name, seed)
    # Clear the per-type schedule cache so this (scheduler, seed) computes a
    # FRESH schedule -- otherwise a structurally-equivalent prior cell's cached
    # schedule would be reused, collapsing the 7-seed variance for IG/SA.
    empty!(Dagger.datadeps_schedule_cache(sched))
    timed  = TimedScheduler(sched)
    reset_milp_info!()

    t0 = time_ns()
    try
        run_workload!(wl, inputs, timed)
    catch e
        @warn "CELL EXCEPTION" regime wl nt bs name seed exception=(e, catch_backtrace())
        mstat, mobj = name == "JuMPScheduler" ? read_milp_info() : ("", NaN)
        return (; regime, workload=wl, nt, bs, scheduler=name, seed,
                wall_ms=NaN, aot_ms=NaN, residual=NaN, n_tasks=ntk,
                milp_status="EXCEPTION:$(typeof(e))", milp_obj=mobj, failed=true)
    end
    wall_ms = (time_ns() - t0) / 1e6
    aot_ms  = timed.last_aot_ns[] / 1e6
    _, resid = verify_workload(wl, inputs)
    mstat, mobj = name == "JuMPScheduler" ? read_milp_info() : ("", NaN)
    return (; regime, workload=wl, nt, bs, scheduler=name, seed,
            wall_ms, aot_ms, residual=resid, n_tasks=ntk,
            milp_status=mstat, milp_obj=mobj, failed=false)
end

csv_row(r) = join((r.regime, r.workload, r.nt, r.bs, r.scheduler, r.seed,
                   round(r.wall_ms, digits=3), round(r.aot_ms, digits=3),
                   r.residual, r.n_tasks, r.milp_status, r.milp_obj), ",")
const CSV_HDR = "regime,workload,nt,bs,scheduler,seed,wall_ms,aot_ms,residual,n_tasks,milp_status,milp_obj"

# --- time-cap verification (PSZ): IG/SA must honor time_limit_sec ----------
function verify_time_cap()
    println("SMOKE time-cap check: IG/SA at nt=8 with a 3s cap should show aot≈3s")
    wl, nt, bs = :matmul, 8, BS_FOR_NT[8]
    warm_gpu_metrics_for!(wl, nt, bs); warm_metrics_for!(wl, nt, bs)
    for (label, s) in (
        ("IG",  Dagger.IteratedGreedyScheduler(; rng=MersenneTwister(1), time_limit_sec=3.0)),
        ("SA",  Dagger.SimulatedAnnealingScheduler(
                    Dagger.IteratedGreedyScheduler(; rng=MersenneTwister(1), time_limit_sec=3.0);
                    rng=MersenneTwister(1), time_limit_sec=3.0)))
        timed = TimedScheduler(s)
        run_workload!(wl, build_inputs(wl, nt, bs), timed)
        aot = timed.last_aot_ns[] / 1e9
        @printf("SMOKE   %s aot=%.2fs  (cap=3s; respected if <~4s and not runaway)\n", label, aot)
    end
end

# --- main -----------------------------------------------------------------
function main()
    regime = get(ENV, "REGIME", "a")
    outcsv = get(ENV, "OUTCSV", "paper_grid_$(regime).csv")
    smoke  = get(ENV, "SMOKE", "0") != "0"
    ndev   = length(collect(CUDA.devices()))
    @printf("paper_grid regime=%s CUDA_devices=%d smoke=%s -> %s\n", regime, ndev, smoke, outcsv)

    workloads = smoke ? [:cholesky] : WORKLOADS
    nts       = smoke ? [parse(Int, get(ENV,"SMOKE_NT","4"))] : NTS
    seeds     = smoke ? (1:1)       : SEEDS

    open(outcsv, "w") do io
        println(io, CSV_HDR); flush(io)
        for wl in workloads, nt in nts
            bs = BS_FOR_NT[nt]
            ntk = capture_ntasks(wl, nt, bs)
            # Per-config warmup (once): populate the metrics cache with
            # ground-truth samples. Scheduler/seed-independent, so one pass
            # serves all cells of this config; measured cells enrich it further.
            try
                warm_gpu_metrics_for!(wl, nt, bs)
            catch e
                @warn "GPU warmup failed (continuing on CPU cost data)" wl nt exception=e
            end
            warm_metrics_for!(wl, nt, bs)
            @printf("[config] %s nt=%d bs=%d n_tasks=%d\n", wl, nt, bs, ntk); flush(stdout)
            # Seed-outer, scheduler-inner: every scheduler sees an equally-warm
            # cache at each seed (removes the "later scheduler sees richer cache"
            # bias). Order: seed=1[RR,Greedy,IG,SA,MILP], seed=2[...], ...
            for seed in seeds, name in SCHED_NAMES
                r = run_cell(regime, wl, nt, bs, name, seed, ntk)
                println(io, csv_row(r)); flush(io)
                gate = get(RESID_GATE, wl, 1e-8)
                flag = r.failed ? "  *** FAILED ***" :
                       (isfinite(r.residual) && r.residual > gate ? "  *** RESIDUAL GATE ***" : "")
                @printf("  %-28s seed=%d wall=%.1fms aot=%.1fms resid=%.1e%s\n",
                        name, seed, r.wall_ms, r.aot_ms, r.residual, flag); flush(stdout)
            end
        end
    end
    (smoke && get(ENV,"TIMECAP","1")!="0") && verify_time_cap()
    println("paper_grid DONE -> $outcsv")
end

main()
