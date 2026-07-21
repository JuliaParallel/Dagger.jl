#!/usr/bin/env julia
# =============================================================================
# tuolumne_oneshot.jl  --  one-shot MI300a APU benchmark for the ROSS 2026 paper
# =============================================================================
# ONE Julia session on ONE allocation covers three regimes:
#     M1  single node   (1 node's CPU cores + APU GPU(s))
#     M2  two nodes
#     M4  four nodes
# by scoping each regime to a subset of the workers Dagger can see. It does NOT
# spawn workers itself: launch Julia with however many workers your site's
# workload manager provides (Flux Framework, Slurm, ...); this script consumes
# whatever `Dagger.all_processors()` reports and groups them into nodes. See
# TUOLUMNE_README.md.
#
#   Validate first:   julia bench/datadeps_schedulers/tuolumne_oneshot.jl --smoke
#   Full run:         julia bench/datadeps_schedulers/tuolumne_oneshot.jl
#
# Outputs land in the invocation directory:
#   tuolumne_regime_m1_1node.csv / _m2_2node.csv / _m4_4node.csv
#   tuolumne_medians.csv   tuolumne_optgap.csv   tuolumne_run_summary.txt
# =============================================================================

# ---- 0. Bootstrap: activate a self-contained env, no --project needed -------
import Pkg
const SCRIPT_DIR = @__DIR__
const REPO_ROOT  = normpath(joinpath(SCRIPT_DIR, "..", ".."))
const ENV_DIR    = joinpath(SCRIPT_DIR, "tuolumne_env")
try
    Pkg.activate(ENV_DIR)
    # `develop` the in-tree Dagger so we run the paper's branch, not a registry
    # release. Idempotent; the README asks you to `instantiate` on a login node
    # (with network) beforehand, so this is a no-op re-check on the compute node.
    if Base.find_package("Dagger") === nothing || !haskey(Pkg.project().dependencies, "Dagger")
        Pkg.develop(path=REPO_ROOT)
    end
    Pkg.instantiate()
catch e
    println(stderr, "FATAL: environment setup failed. Run the prereq from ",
            "TUOLUMNE_README.md on a login node first:")
    println(stderr, "  julia --project=$(ENV_DIR) -e 'using Pkg; Pkg.develop(path=\"$(REPO_ROOT)\"); Pkg.instantiate()'")
    showerror(stderr, e); println(stderr)
    exit(2)
end

using Dagger, AMDGPU, HiGHS, JuMP
using LinearAlgebra, Statistics, Random, Printf, Dates
import Dagger: greedy_schedule!, iterated_greedy_schedule!, simulated_annealing_schedule!,
               cost_of_schedule, ScheduleState, _build_eft_cost_cache
import Dagger.MetricsTracker as MT
# Dagger schedules over DistributedNext and re-imports its worker API into its
# own namespace (see src/Dagger.jl), so `Dagger.myid` / `Dagger.remotecall_fetch`
# are the correct handles regardless of how the site's manager spawned workers.

# Building blocks (build_inputs, run_workload!/_run_workload_inner!, verify_workload,
# TimedScheduler, ...). driver.jl is backend-agnostic (no CUDA/AMDGPU import) and
# sets metrics_cache_max_tasks!(5000).
include(joinpath(SCRIPT_DIR, "driver.jl"))

const SMOKE = ("--smoke" in ARGS)

# ---- 1. Configuration -------------------------------------------------------
const WORKLOADS   = [:cholesky, :matmul]
const NTS         = [2, 4, 8]
const SCHED_NAMES = ["RoundRobinScheduler", "GreedyScheduler",
                     "IteratedGreedyScheduler", "SimulatedAnnealingScheduler",
                     "JuMPScheduler"]
const SEEDS       = 1:7
const HEUR_TL     = 60.0    # PSZ fair-comparison cap for IG/SA (matches MILP)
const MILP_TL     = 60.0
const RESID_GATE  = Dict(:cholesky => 1e-8, :matmul => 1e-10)
# bs: 4096 for nt in {2,4}; nt=8 tries 4096 first (MI300a's 128 GB unified HBM3
# may fit it) and falls back to 2048 if the warmup OOMs. Chosen bs is recorded
# per cell and logged.
bs_candidates(nt) = nt == 8 ? [4096, 2048] : [4096]
const REGIMES = [("m1", 1, "tuolumne_regime_m1_1node.csv"),
                 ("m2", 2, "tuolumne_regime_m2_2node.csv"),
                 ("m4", 4, "tuolumne_regime_m4_4node.csv")]
const FLUSH_INTERVAL_S = 300.0   # periodic partial-CSV flush (feature 3)

# The four scheduler-science commits that MUST be in HEAD (matched by subject so
# a rebase/re-hash doesn't break the check). Plus the MILP-objective
# instrumentation the optimality-gap number depends on.
const REQUIRED_SUBJECTS = [
    "stochastic reconstruction for IteratedGreedy",
    "Ruiz-Stützle acceptance",
    "Orsila SA tuning",
    "producer-finish term in greedy",
]

# ---- 2. Scheduler construction (exactly the five, SA-over-IG) ---------------
function make_scheduler(name::String, seed::Int)
    if name == "RoundRobinScheduler"
        return Dagger.RoundRobinScheduler()
    elseif name == "GreedyScheduler"
        return Dagger.GreedyScheduler()
    elseif name == "IteratedGreedyScheduler"
        return Dagger.IteratedGreedyScheduler(; rng=MersenneTwister(seed), time_limit_sec=HEUR_TL)
    elseif name == "SimulatedAnnealingScheduler"
        return Dagger.SimulatedAnnealingScheduler(
            Dagger.IteratedGreedyScheduler(; rng=MersenneTwister(seed), time_limit_sec=HEUR_TL);
            rng=MersenneTwister(seed), time_limit_sec=HEUR_TL)
    elseif name == "JuMPScheduler"
        return Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=MILP_TL)
    end
    error("unknown scheduler $name")
end

# ---- 3. MILP status/objective (from the committed JuMPExt instrumentation) ---
_milp_ext() = Base.get_extension(Dagger, :JuMPExt)
function milp_instrumentation_ok()
    ext = _milp_ext()
    return ext !== nothing && isdefined(ext, :LAST_MILP_SOLVE)
end
reset_milp_info!() = (ext = _milp_ext(); ext === nothing || (ext.LAST_MILP_SOLVE[] = ("NONE", NaN)); nothing)
read_milp_info()   = (ext = _milp_ext(); ext === nothing ? ("NA", NaN) : ext.LAST_MILP_SOLVE[])

# ---- 4. Node topology from the workers Dagger sees --------------------------
struct Topology
    hostnames::Vector{String}                 # sorted, one per node
    node_procs::Dict{String,Vector{Any}}      # hostname -> its Dagger procs
    n_nodes::Int
    total_procs::Int
    total_gpus::Int
end
function detect_topology()
    procs = collect(Dagger.all_processors())
    byworker = Dict{Int,Vector{Any}}()
    for p in procs
        push!(get!(byworker, Dagger.root_worker_id(p), Any[]), p)
    end
    wid_host = Dict{Int,String}()
    for wid in keys(byworker)
        wid_host[wid] = wid == Dagger.myid() ? gethostname() :
                        try Dagger.remotecall_fetch(gethostname, wid) catch; "worker$wid"; end
    end
    node_procs = Dict{String,Vector{Any}}()
    for (wid, host) in wid_host
        append!(get!(node_procs, host, Any[]), byworker[wid])
    end
    hosts = sort(collect(keys(node_procs)))
    ngpu = count(!_is_cpu_proc, procs)
    return Topology(hosts, node_procs, length(hosts), length(procs), ngpu)
end
# Procs / scope for the first `n` nodes of a topology (M1/M2/M4).
function regime_procs(topo::Topology, n::Int)
    sel = topo.hostnames[1:min(n, topo.n_nodes)]
    return reduce(vcat, (topo.node_procs[h] for h in sel); init=Any[])
end
regime_scope(procs) = Dagger.UnionScope([Dagger.ExactScope(p) for p in procs])

# ---- 5. Scoped run + warmup + DAG capture ----------------------------------
# Run a workload restricted to `scope` (the regime's procs).
scoped_run!(wl, inputs, sched, scope) =
    Dagger.with_options(; scope=scope) do
        _run_workload_inner!(wl, inputs, sched)
    end

# Per-config warmup (once): a GPU-scoped RR pass to force real APU samples, then
# an all-regime RR pass -- both restricted to this regime's procs. Metrics are
# ground-truth and accumulate across measured cells thereafter.
function warm_config!(wl, nt, bs, rprocs)
    gpu = filter(!_is_cpu_proc, rprocs)
    if !isempty(gpu)
        try
            scoped_run!(wl, build_inputs(wl, nt, bs), Dagger.RoundRobinScheduler(), regime_scope(gpu))
        catch e
            @warn "GPU warmup failed (continuing on CPU cost data)" wl nt exception=e
        end
    end
    scoped_run!(wl, build_inputs(wl, nt, bs), Dagger.RoundRobinScheduler(), regime_scope(rprocs))
    return
end

# Capture the largest AOT DAGSpec (+ procs + snapshot) the scheduler sees for a
# config, for n_tasks and the AOT-only optimality-gap pass.
mutable struct DagCap <: Dagger.DataDepsScheduler
    inner::Dagger.GreedyScheduler
    grab::Base.RefValue{Any}
end
function Dagger.datadeps_schedule_dag_aot!(s::DagCap, sc, dag, pr, sco)
    cur = s.grab[]
    if cur === nothing || Dagger.nv(dag.g) > Dagger.nv(cur[1].g)
        s.grab[] = (dag, pr)
    end
    Dagger.datadeps_schedule_dag_aot!(s.inner, sc, dag, pr, sco)
end
Dagger.datadeps_schedule_task_jit!(s::DagCap, a...) = Dagger.datadeps_schedule_task_jit!(s.inner, a...)
Dagger.datadeps_schedule_cache(s::DagCap) = Dagger.datadeps_schedule_cache(s.inner)
Base.similar(s::DagCap) = DagCap(similar(s.inner), s.grab)
function capture_dag(wl, nt, bs, scope)
    cap = DagCap(Dagger.GreedyScheduler(), Ref{Any}(nothing))
    scoped_run!(wl, build_inputs(wl, nt, bs), cap, scope)
    empty!(Dagger.datadeps_schedule_cache(cap))
    grabbed = cap.grab[]
    grabbed === nothing && return (nothing, nothing, 0)
    dag, procs = grabbed
    return (dag, procs, Dagger.nv(dag.g))
end

# ---- 6. One measured cell ---------------------------------------------------
function run_cell(regime, n_nodes, wl, nt, bs, name, seed, ntk, scope)
    inputs = build_inputs(wl, nt, bs)
    sched  = make_scheduler(name, seed)
    # Fresh schedule per (scheduler, seed): otherwise a structurally-equivalent
    # prior cell's cached schedule would be reused, collapsing the 7-seed spread.
    empty!(Dagger.datadeps_schedule_cache(sched))
    timed = TimedScheduler(sched)
    reset_milp_info!()
    t0 = time_ns()
    try
        scoped_run!(wl, inputs, timed, scope)
    catch e
        @warn "CELL EXCEPTION" regime wl nt bs name seed exception=(e, catch_backtrace())
        mstat, mobj = name == "JuMPScheduler" ? read_milp_info() : ("", NaN)
        return (; regime, n_nodes, workload=wl, nt, bs, scheduler=name, seed,
                wall_ms=NaN, aot_ms=NaN, residual=NaN, n_tasks=ntk,
                milp_status="EXCEPTION:$(nameof(typeof(e)))", milp_obj=mobj, failed=true)
    end
    wall_ms = (time_ns() - t0) / 1e6
    aot_ms  = timed.last_aot_ns[] / 1e6
    resid = try last(verify_workload(wl, inputs)) catch; NaN end
    mstat, mobj = name == "JuMPScheduler" ? read_milp_info() : ("", NaN)
    return (; regime, n_nodes, workload=wl, nt, bs, scheduler=name, seed,
            wall_ms, aot_ms, residual=resid, n_tasks=ntk,
            milp_status=mstat, milp_obj=mobj, failed=false)
end

# ---- 7. CSV plumbing --------------------------------------------------------
const MAIN_HDR = "regime,n_nodes,workload,nt,bs,scheduler,seed,wall_ms,aot_ms,residual,n_tasks,milp_status,milp_obj"
main_row(r) = join((r.regime, r.n_nodes, r.workload, r.nt, r.bs, r.scheduler, r.seed,
                    round(r.wall_ms, digits=3), round(r.aot_ms, digits=3),
                    r.residual, r.n_tasks, r.milp_status, r.milp_obj), ",")
const OPTGAP_HDR = "regime,n_nodes,workload,nt,bs,scheduler,seed,cost_of_schedule,milp_obj,ratio_to_milp"

# ---- 8. AOT-only optimality-gap (headline Greedy/MILP, IG/MILP, SA/MILP) -----
# For each config with at least one MILP=OPTIMAL cell, capture the DAG once and
# invoke Greedy/IG/SA in AOT-only mode (no execution) to get cost_of_schedule of
# the produced ScheduleState. Chains match the real schedulers: standalone
# Greedy uses producer_finish=true; IG/SA seed with plain greedy (as their
# actual construction does) then refine.
function aot_greedy(snap, dag, procs, cache)
    st = ScheduleState()
    greedy_schedule!(st, snap, dag, procs; cache=cache, producer_finish=true)
    return cost_of_schedule(st)
end
function aot_ig(snap, dag, procs, cache, seed)
    st = ScheduleState()
    greedy_schedule!(st, snap, dag, procs; cache=cache)                      # seed
    st = iterated_greedy_schedule!(st, snap, dag, procs; rng=MersenneTwister(seed),
                                   cache=cache, time_limit_sec=HEUR_TL)
    return cost_of_schedule(st)
end
function aot_sa(snap, dag, procs, cache, seed)
    st = ScheduleState()
    greedy_schedule!(st, snap, dag, procs; cache=cache)                      # greedy seed
    st = iterated_greedy_schedule!(st, snap, dag, procs; rng=MersenneTwister(seed),
                                   cache=cache, time_limit_sec=HEUR_TL)      # IG seed for SA
    simulated_annealing_schedule!(st, snap, dag, procs; rng=MersenneTwister(seed),
                                  cache=cache, time_limit_sec=HEUR_TL)
    return cost_of_schedule(st)
end
# `optimal_configs`: (regime,n_nodes,wl,nt,bs) => median milp_obj over OPTIMAL cells.
function optgap_pass!(io, optimal_configs, topo, log)
    for ((regime, n_nodes, wl, nt, bs), milp_obj) in optimal_configs
        rprocs = regime_procs(topo, n_nodes)
        scope  = regime_scope(rprocs)
        dag, procs, k = capture_dag(wl, nt, bs, scope)
        (dag === nothing || k == 0) && continue
        snap  = MT.snapshot(MT.global_metrics_cache())
        cache = _build_eft_cost_cache(snap, dag, procs)
        emit(sched, seed, cost) = begin
            ratio = (isfinite(milp_obj) && milp_obj > 0) ? cost / milp_obj : NaN
            println(io, join((regime, n_nodes, wl, nt, bs, sched, seed,
                              round(cost, digits=1), round(milp_obj, digits=1),
                              round(ratio, digits=4)), ","))
        end
        try; emit("GreedyScheduler", 0, aot_greedy(snap, dag, procs, cache)); catch e; log("optgap Greedy $wl nt=$nt failed: $e"); end
        for s in SEEDS
            try; emit("IteratedGreedyScheduler", s, aot_ig(snap, dag, procs, cache, s)); catch e; log("optgap IG $wl nt=$nt s=$s failed: $e"); end
            try; emit("SimulatedAnnealingScheduler", s, aot_sa(snap, dag, procs, cache, s)); catch e; log("optgap SA $wl nt=$nt s=$s failed: $e"); end
        end
        flush(io)
    end
end

# ---- 8b. Schedule-cache demonstration (Contribution 4) ----------------------
# For one config per regime (cholesky nt=4, seed=1, Greedy), invoke the SAME
# spawn_datadeps twice back-to-back. Call 1 populates the structural-equivalence
# cache (full AOT); call 2 hits it (equivalence lookup + return), so its aot
# should be ~0. If the two are similar, the cache isn't being hit on this Julia
# version. Fresh inputs each call -- identity differs, structure matches, which
# is exactly what the cache keys on.
function cache_demo!(io, topo, log)
    wl, nt, bs, seed = :cholesky, 4, 4096, 1
    for (rtag, nnodes, _) in REGIMES
        topo.n_nodes < nnodes && continue
        rprocs = regime_procs(topo, nnodes); scope = regime_scope(rprocs)
        try
            warm_config!(wl, nt, bs, rprocs)
            # Clean cache, then two back-to-back runs. GreedyScheduler shares one
            # per-type cache, so call 2 sees call 1's entry.
            empty!(Dagger.datadeps_schedule_cache(Dagger.GreedyScheduler()))
            t1 = TimedScheduler(Dagger.GreedyScheduler())
            scoped_run!(wl, build_inputs(wl, nt, bs), t1, scope)
            aot1 = t1.last_aot_ns[] / 1e6
            t2 = TimedScheduler(Dagger.GreedyScheduler())
            scoped_run!(wl, build_inputs(wl, nt, bs), t2, scope)
            aot2 = t2.last_aot_ns[] / 1e6
            println(io, join((rtag, wl, nt, bs, "GreedyScheduler", seed,
                              round(aot1, digits=3), round(aot2, digits=3)), ","))
            flush(io)
            log(@sprintf("  cache-demo %s: first=%.3fms hit=%.3fms (%.1fx)",
                         rtag, aot1, aot2, aot1 / max(aot2, 1e-6)))
        catch e
            @warn "cache-demo failed" rtag exception=e
            println(io, join((rtag, wl, nt, bs, "GreedyScheduler", seed, "NaN", "NaN"), ","))
            flush(io)
        end
    end
end

# ---- 9. Tree verification (feature 5) ---------------------------------------
function verify_tree(log)
    gitlog = try readchomp(`git -C $REPO_ROOT log --oneline -60`) catch; "" end
    missing = filter(s -> !occursin(s, gitlog), REQUIRED_SUBJECTS)
    sha = try readchomp(`git -C $REPO_ROOT rev-parse --short HEAD`) catch; "unknown" end
    if !isempty(missing)
        log("FATAL: HEAD ($sha) is missing required commits:")
        foreach(s -> log("    - \"$s\""), missing)
        log("Check out the paper branch at the pinned SHA (see TUOLUMNE_README.md).")
        return (false, sha)
    end
    if !milp_instrumentation_ok()
        log("FATAL: JuMPExt.LAST_MILP_SOLVE not found -- the MILP-objective")
        log("instrumentation commit is missing; optimality-gap numbers unavailable.")
        return (false, sha)
    end
    log("tree OK -- HEAD=$sha, all required commits + MILP instrumentation present")
    return (true, sha)
end

# ---- 10. Smoke: one tiny cell end-to-end, PASS/FAIL (feature 1) -------------
function run_smoke()
    println("=== TUOLUMNE SMOKE (validates env + branch + ROCM/AMDGPU stack) ===")
    ok, sha = verify_tree(println)
    ok || (println("SMOKE: FAIL (tree state)"); return false)
    topo = detect_topology()
    @printf("detected: %d node(s), %d procs, %d APU GPU(s) [%s]\n",
            topo.n_nodes, topo.total_procs, topo.total_gpus, join(topo.hostnames, ", "))
    if topo.total_gpus == 0
        println("SMOKE: WARNING -- no APU GPUs visible to Dagger. AMDGPU.jl/ROCm may not")
        println("       be initialised, or workers lack GPU access. Fix before the full run.")
    end
    rprocs = regime_procs(topo, 1); scope = regime_scope(rprocs)
    wl, nt, bs = :cholesky, 2, 4096
    try
        warm_config!(wl, nt, bs, rprocs)
        _, _, k = capture_dag(wl, nt, bs, scope)
        for name in ("RoundRobinScheduler", "GreedyScheduler")
            r = run_cell("smoke", 1, wl, nt, bs, name, 1, k, scope)
            gate = RESID_GATE[wl]
            status = r.failed ? "FAILED" : (r.residual > gate ? "RESID>gate" : "ok")
            @printf("  %-22s wall=%.1fms aot=%.1fms resid=%.2e -> %s\n",
                    name, r.wall_ms, r.aot_ms, r.residual, status)
            (r.failed || r.residual > gate) && (println("SMOKE: FAIL"); return false)
        end
    catch e
        println("SMOKE: FAIL (exception)"); showerror(stdout, e, catch_backtrace()); println()
        return false
    end
    println("SMOKE: PASS -- environment is ready for the full run.")
    return true
end

# ---- 11. Full grid ----------------------------------------------------------
function main()
    started = now()
    summary = String[]
    log(msg) = (s = "[$(Dates.format(now(), "HH:MM:SS"))] $msg"; println(s); push!(summary, s); flush(stdout))

    ok, sha = verify_tree(log)
    ok || (write_summary(summary, sha, started, 0); exit(1))
    topo = detect_topology()
    log(@sprintf("topology: %d node(s), %d procs, %d APU GPU(s): %s",
                 topo.n_nodes, topo.total_procs, topo.total_gpus, join(topo.hostnames, ", ")))

    optgap_io = open("tuolumne_optgap.csv", "w"); println(optgap_io, OPTGAP_HDR); flush(optgap_io)
    optimal_configs = Dict{Any,Float64}()
    all_rows = Vector{Any}()
    fail_count = 0
    last_flush = time()

    for (rtag, nnodes, outcsv) in REGIMES
        if topo.n_nodes < nnodes
            log("SKIP regime $rtag ($nnodes nodes) -- only $(topo.n_nodes) node(s) allocated")
            continue
        end
        rprocs = regime_procs(topo, nnodes); scope = regime_scope(rprocs)
        rgpu = count(!_is_cpu_proc, rprocs)
        log(@sprintf("REGIME %s: %d node(s), %d procs (%d APU GPU) -> %s",
                     uppercase(rtag), nnodes, length(rprocs), rgpu, outcsv))
        rstart = time()
        io = open(outcsv, "w"); println(io, MAIN_HDR); flush(io)
        for wl in WORKLOADS, nt in NTS
            # pick bs (nt=8 tries 4096 then 2048) via the warmup
            bs = 0
            for cand in bs_candidates(nt)
                try
                    warm_config!(wl, nt, cand, rprocs); bs = cand; break
                catch e
                    log("  $wl nt=$nt bs=$cand warmup failed ($(nameof(typeof(e)))); trying smaller")
                end
            end
            if bs == 0
                log("  $wl nt=$nt: no bs fit -- config skipped"); continue
            end
            _, _, ntk = capture_dag(wl, nt, bs, scope)
            log(@sprintf("  [config] %s nt=%d bs=%d n_tasks=%d", wl, nt, bs, ntk))
            optimal_objs = Float64[]
            for seed in SEEDS, name in SCHED_NAMES     # seed-major, scheduler-inner
                r = run_cell(rtag, nnodes, wl, nt, bs, name, seed, ntk, scope)
                println(io, main_row(r)); push!(all_rows, r)
                r.failed && (fail_count += 1)
                gate = RESID_GATE[wl]
                if !r.failed && isfinite(r.residual) && r.residual > gate
                    log(@sprintf("    *** RESIDUAL GATE %s %s nt=%d seed=%d resid=%.2e", rtag, name, nt, seed, r.residual))
                    fail_count += 1
                end
                if name == "JuMPScheduler" && r.milp_status == "OPTIMAL" && isfinite(r.milp_obj)
                    push!(optimal_objs, r.milp_obj)
                end
                if time() - last_flush > FLUSH_INTERVAL_S
                    flush(io); flush(optgap_io); last_flush = time()
                end
            end
            isempty(optimal_objs) || (optimal_configs[(rtag, nnodes, wl, nt, bs)] = median(optimal_objs))
        end
        close(io)
        log(@sprintf("REGIME %s done in %.1f min", uppercase(rtag), (time()-rstart)/60))
    end

    log("schedule-cache demonstration (Contribution 4)")
    open("tuolumne_cache.csv", "w") do cio
        println(cio, "regime,workload,nt,bs,scheduler,seed,aot_ms_first_call,aot_ms_cache_hit")
        cache_demo!(cio, topo, log)
    end

    log("optimality-gap AOT pass over $(length(optimal_configs)) OPTIMAL config(s)")
    optgap_pass!(optgap_io, optimal_configs, topo, log); close(optgap_io)

    write_medians(all_rows)
    write_summary(summary, sha, started, fail_count)
    log(@sprintf("ALL DONE. %d cells, %d flagged/failed. Elapsed %.1f min.",
                 length(all_rows), fail_count, (now()-started).value/60000))
end

# ---- 12. Aggregates ---------------------------------------------------------
function write_medians(rows)
    open("tuolumne_medians.csv", "w") do io
        println(io, "regime,n_nodes,workload,nt,bs,scheduler,wall_ms_median,wall_ms_stddev,",
                    "aot_ms_median,aot_ms_stddev,residual_max,n_tasks,milp_status,milp_obj")
        groups = Dict{Any,Vector{Any}}()
        for r in rows
            push!(get!(groups, (r.regime, r.n_nodes, r.workload, r.nt, r.bs, r.scheduler), Any[]), r)
        end
        med(xs) = (ys = filter(isfinite, xs); isempty(ys) ? NaN : median(ys))
        sdv(xs) = (ys = filter(isfinite, xs); length(ys) < 2 ? 0.0 : std(ys))
        for (key, rs) in sort(collect(groups), by = k -> string(k[1]))
            wl = [r.wall_ms for r in rs]; ao = [r.aot_ms for r in rs]
            resid = maximum(filter(isfinite, [r.residual for r in rs]); init=NaN)
            mstat = any(r -> r.milp_status == "OPTIMAL", rs) ? "OPTIMAL" :
                    (isempty(rs) ? "" : rs[1].milp_status)
            mobj = med([r.milp_obj for r in rs])
            println(io, join((key..., round(med(wl), digits=3), round(sdv(wl), digits=3),
                              round(med(ao), digits=3), round(sdv(ao), digits=3),
                              resid, rs[1].n_tasks, mstat, mobj), ","))
        end
    end
end

function write_summary(summary, sha, started, fail_count)
    open("tuolumne_run_summary.txt", "w") do io
        println(io, "Tuolumne one-shot benchmark -- run summary")
        println(io, "git SHA (HEAD): ", sha)
        println(io, "launch time:    ", started)
        println(io, "end time:       ", now())
        println(io, "flagged/failed cells: ", fail_count)
        println(io, "\n--- log ---")
        foreach(l -> println(io, l), summary)
    end
end

# ---- entry ------------------------------------------------------------------
if SMOKE
    exit(run_smoke() ? 0 : 1)
else
    main()
end
