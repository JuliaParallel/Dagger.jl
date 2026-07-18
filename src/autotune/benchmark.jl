# benchmark.jl
#
# `Autotune.benchmark(config)` - the offline training system.
#
# Architecture:
#
#   parent (this process)
#     ├─ ensure_project: create/reuse a temporary Julia project under the
#     │    autotune workdir, `Pkg.develop` Dagger into it, and `Pkg.add` every
#     │    package any registered algorithm/converter wants (each add is
#     │    individually fallible). A probe records which packages resolved
#     │    and whether GPUs are functional -> capabilities.toml.
#     ├─ expand_trials: cross operation feature axes × algorithms × FREE
#     │    config domains × process-level FIXED axes (Julia threads, worker
#     │    counts). Trials that can't run (missing package, over memory
#     │    budget) become pre-recorded skipped results, not spawned work.
#     ├─ for each process-group (nthreads, nprocs):
#     │    spawn `julia --project=<env> --threads=N [-p M] worker.jl ...`
#     │    The worker writes one TOML file per finished trial. A watchdog
#     │    kills the worker if no new result appears within the grace period;
#     │    the trial being executed is marked "timeout"/"crashed" and a fresh
#     │    worker resumes from the next trial. Explosions lose one trial.
#     └─ collect + merge into the on-disk database, print a report.
#
#   worker (fresh process per group, restarted after any death)
#     └─ run_worker: for each trial - load required packages, apply MUTABLE
#          configurables, synthesize inputs, convert to the target container
#          form, warm up, sample timings, validate the result (e.g. solve
#          residual vs requested accuracy), write trial_<id>.toml.

Base.@kwdef struct BenchmarkConfig
    ops::Vector{Symbol} = Symbol[:transfer, :gemm, :lu, :cholesky, :qr, :solve]
    # Per-op axis overrides, e.g. Dict(:lu => Dict("size" => [512, 8192],
    # "eltype" => ["Float64"])). Only the mentioned axes are replaced.
    axes::Dict{Symbol,Dict{String,Vector}} = Dict{Symbol,Dict{String,Vector}}()
    # Optional per-op allow-list of algorithm names. `nothing` (the default)
    # benchmarks every registered candidate; a front-end that reuses shared
    # runtime operations but only wants a subset (e.g. the regression suite
    # timing Dagger's implementation, not every BLAS/GPU baseline) passes e.g.
    # Dict(:lu => [:dagger_lu]). Ops absent from the dict keep all candidates.
    algorithms::Union{Nothing,Dict{Symbol,Vector{Symbol}}} = nothing
    # Process-level FIXED knobs (each combination = its own worker process).
    nthreads::Vector{Int} = default_thread_domain()
    nprocs::Vector{Int} = Int[0]
    # Extra top-level code spliced into every worker script *after* the process
    # topology is established but *before* trials run. Front-ends use it to load
    # Dagger on all workers (`@everywhere using Dagger`), pull in acceleration
    # packages, and register any front-end-specific operations/algorithms into
    # the worker's registry. Runs at the latest world age.
    worker_preamble::String = ""
    # Global replacement for every op's "array_type" axis (`nothing` = per-op
    # defaults, auto-extended with "CuArray"/"ROCArray"/"DArray" when the
    # probe finds them usable).
    array_types::Union{Nothing,Vector{String}} = nothing
    extra_packages::Vector{String} = String[]
    # Per-trial limits.
    trial_time_limit::Float64 = 120.0   # hard wall per trial (watchdog)
    sample_time::Float64 = 1.0          # accumulate at least this much timing
    max_samples::Int = 7
    gcsample::Bool = false              # run a full GC before every timed sample
    memory_fraction::Float64 = 0.5      # of Sys.total_memory(), per trial
    total_time_limit::Float64 = Inf     # overall budget; leftovers skipped
    # Environment.
    project::Union{Symbol,String} = :temp   # :temp | :current | "/path/to/env"
    fresh_project::Bool = false
    tie_blas_threads::Bool = true       # set blas_threads = nthreads per trial
    # Output.
    db_path::String = default_db_path()
    workdir::String = joinpath(default_autotune_dir(), "work")
    dry_run::Bool = false
    verbose::Bool = true
end

struct Trial
    id::Int
    op::Symbol
    algorithm::Symbol
    features::Dict{String,Any}
    config::Dict{String,Any}
    packages::Vector{String}
end

_vprintln(cfg::BenchmarkConfig, args...) = cfg.verbose && println(args...)

# ---------------------------------------------------------------------------
# Environment (temporary project) setup

_dagger_root() = begin
    pm = parentmodule(@__MODULE__)
    if (pm !== @__MODULE__) && pm !== Main
        root = try
            pkgdir(pm)
        catch
            nothing
        end
        root === nothing || return root
    end
    return normpath(joinpath(@__DIR__, "..", ".."))
end

function _candidate_packages(cfg::BenchmarkConfig)
    pkgs = String[]
    for op in cfg.ops, alg in algorithms_for(op)
        alg.package === nothing || push!(pkgs, alg.package)
    end
    for c in values(CONVERTERS)
        c.package === nothing || push!(pkgs, c.package)
    end
    append!(pkgs, cfg.extra_packages)
    return sort!(unique!(pkgs))
end

"""
    ensure_project(cfg) -> (project_path_or_nothing, capabilities)

Prepare the benchmark environment and return which packages/devices are
usable in it. `nothing` means "run workers under the current project".
"""
function ensure_project(cfg::BenchmarkConfig)
    if cfg.project === :current
        caps = _current_process_capabilities(cfg)
        active = Base.active_project()
        return (active === nothing ? nothing : dirname(active)), caps
    end
    proj = cfg.project isa AbstractString ? String(cfg.project) :
           joinpath(cfg.workdir, "env")
    mkpath(proj)
    capfile = joinpath(proj, "autotune_capabilities.toml")
    if cfg.fresh_project || !isfile(joinpath(proj, "Project.toml")) || !isfile(capfile)
        _vprintln(cfg, "Autotune: setting up benchmark project at $proj ...")
        _setup_project(cfg, proj, capfile)
    end
    caps = try
        TOML.parsefile(capfile)
    catch
        Dict{String,Any}()
    end
    return proj, caps
end

function _setup_project(cfg::BenchmarkConfig, proj::String, capfile::String)
    pkgs = _candidate_packages(cfg)
    root = _dagger_root()
    # The setup script tolerates every individual failure: a package that
    # can't be added simply reports unavailable, and its trials are skipped.
    code = """
    import Pkg, TOML
    Pkg.activate($(repr(proj)))
    caps = Dict{String,Any}("packages" => Dict{String,Any}())
    try
        Pkg.develop(path=$(repr(root)))
        caps["dagger_dev"] = true
    catch err
        @warn "Could not develop Dagger into the benchmark project" exception=err
        caps["dagger_dev"] = false
    end
    for pkg in $(repr(pkgs))
        ok = try
            Pkg.add(pkg); true
        catch err
            @warn "Could not add \$pkg" exception=err
            false
        end
        caps["packages"][pkg] = ok
    end
    try
        Pkg.instantiate()
        Pkg.precompile()
    catch err
        @warn "instantiate/precompile issues (continuing)" exception=err
    end
    for (pkg, key) in (("CUDA", "cuda_functional"), ("AMDGPU", "amdgpu_functional"))
        caps[key] = get(caps["packages"], pkg, false) && try
            mod = Base.require(Main, Symbol(pkg))
            Bool(mod.functional())
        catch
            false
        end
    end
    open($(repr(capfile)), "w") do io
        TOML.print(io, caps)
    end
    """
    log = joinpath(cfg.workdir, "setup.log")
    mkpath(cfg.workdir)
    cmd = Cmd(vcat(Base.julia_cmd().exec, ["--startup-file=no", "-e", code]))
    ok = success(pipeline(cmd; stdout=log, stderr=log))
    ok || @warn "Autotune: project setup reported errors; see $log"
    return nothing
end

function _current_process_capabilities(cfg::BenchmarkConfig)
    caps = Dict{String,Any}("packages" => Dict{String,Any}(), "dagger_dev" => true)
    for pkg in _candidate_packages(cfg)
        caps["packages"][pkg] = package_available(pkg)
    end
    for (pkg, key) in (("CUDA", "cuda_functional"), ("AMDGPU", "amdgpu_functional"))
        mod = loaded_module(pkg)
        check = get(FUNCTIONAL_CHECKS, pkg, nothing)
        caps[key] = mod !== nothing && (check === nothing || try
            check(mod)
        catch
            false
        end)
    end
    return caps
end

_pkg_usable(caps, name::Nothing) = true
function _pkg_usable(caps, name::AbstractString)
    pkgs = get(caps, "packages", Dict{String,Any}())
    get(pkgs, name, false) || return false
    name == "CUDA" && return get(caps, "cuda_functional", false)
    name == "AMDGPU" && return get(caps, "amdgpu_functional", false)
    name == "Dagger" && return get(caps, "dagger_dev", true)
    return true
end

# ---------------------------------------------------------------------------
# Trial expansion

_base_form(f::Dict{String,Any}) =
    get(f, "structure", "dense") == "sparse" ? :SparseMatrixCSC : :Array

function _cross_config(domains::Dict{String,Vector})
    isempty(domains) && return [Dict{String,Any}()]
    keys_ = sort!(collect(keys(domains)))
    out = [Dict{String,Any}()]
    for k in keys_
        nxt = Dict{String,Any}[]
        for base in out, v in domains[k]
            d = copy(base)
            d[k] = v
            push!(nxt, d)
        end
        out = nxt
    end
    return out
end

function _op_axes(cfg::BenchmarkConfig, opspec::OperationSpec, caps)
    axes = Dict{String,Vector}(k => copy(v) for (k, v) in opspec.default_axes)
    if haskey(axes, "array_type")
        if cfg.array_types !== nothing
            axes["array_type"] = Vector{Any}(cfg.array_types)
        else
            get(caps, "cuda_functional", false) && push!(axes["array_type"], "CuArray")
            get(caps, "amdgpu_functional", false) && push!(axes["array_type"], "ROCArray")
            _pkg_usable(caps, "Dagger") && opspec.name !== :transfer &&
                push!(axes["array_type"], "DArray")
            unique!(axes["array_type"])
        end
    end
    for (k, v) in get(cfg.axes, opspec.name, Dict{String,Vector}())
        axes[k] = copy(v)
    end
    return axes
end

"""
    expand_trials(cfg, caps) -> (trials, preresults)

Cross all axes into concrete trials. Configurations that provably can't run
(unusable package, over memory budget, missing converter for input staging)
are returned as pre-recorded `TrialResult`s instead of trials.
"""
function expand_trials(cfg::BenchmarkConfig, caps)
    trials = Trial[]
    preresults = TrialResult[]
    id = 0
    membudget = cfg.memory_fraction * Sys.total_memory()
    for op in cfg.ops
        opspec = operation(op)
        axes = _op_axes(cfg, opspec, caps)
        feats = opspec.enumerate_features(axes)
        allowed = cfg.algorithms === nothing ? nothing : get(cfg.algorithms, op, nothing)
        for f0 in feats, alg in algorithms_for(op)
            (allowed === nothing || alg.name in allowed) || continue
            alg.supports(f0) || continue
            f = copy(f0)
            fconfigs = _cross_config(alg.free_config)
            for nt in cfg.nthreads, np in cfg.nprocs, fc in fconfigs
                if f["array_type"] == "DArray"
                    f = copy(f0)
                    f["locality"] = np > 0 ? "remote" : "local"
                end
                config = copy(fc)
                config["nthreads"] = nt
                np > 0 && (config["nprocs"] = np)
                cfg.tie_blas_threads && get(f, "structure", "dense") == "dense" &&
                    (config["blas_threads"] = nt)

                pkgs = String[]
                alg.package === nothing || push!(pkgs, alg.package)
                base = _base_form(f)
                tgt = Symbol(f["array_type"])
                usable = _pkg_usable(caps, alg.package)
                # `benchmark_only` operations synthesize their inputs directly
                # in the target form (see `_timed_trial`), so they need no
                # container converter for input staging.
                if usable && !opspec.benchmark_only && tgt !== base
                    c = converter(base, tgt)
                    if c === nothing
                        usable = false
                    else
                        c.package === nothing || push!(pkgs, c.package)
                        usable = _pkg_usable(caps, c.package)
                    end
                end
                if !usable
                    push!(preresults, TrialResult(op, alg.name, f, config;
                        status="skipped_unavailable",
                        error="package/converter unavailable in benchmark environment"))
                    continue
                end
                if opspec.input_bytes(f) * 3 > membudget
                    push!(preresults, TrialResult(op, alg.name, f, config;
                        status="skipped_mem",
                        error="estimated footprint exceeds memory_fraction budget"))
                    continue
                end
                id += 1
                push!(trials, Trial(id, op, alg.name, f, config, unique!(pkgs)))
            end
        end
    end
    return trials, preresults
end

# ---------------------------------------------------------------------------
# Trial (de)serialization for the worker

function _trial_to_dict(t::Trial)
    return Dict{String,Any}(
        "id" => t.id, "op" => String(t.op), "algorithm" => String(t.algorithm),
        "features" => _toml_safe(t.features), "config" => _toml_safe(t.config),
        "packages" => t.packages)
end

_trial_path(outdir::String, id::Integer) =
    joinpath(outdir, "trial_" * lpad(string(id), 6, '0') * ".toml")

function _write_trials_file(path::String, trials::Vector{Trial}, cfg::BenchmarkConfig)
    doc = Dict{String,Any}(
        "limits" => Dict{String,Any}(
            "trial_time_limit" => cfg.trial_time_limit,
            "sample_time"      => cfg.sample_time,
            "max_samples"      => cfg.max_samples,
            "gcsample"         => cfg.gcsample,
            "memory_fraction"  => cfg.memory_fraction),
        "trials" => Any[_trial_to_dict(t) for t in trials])
    open(path, "w") do io
        TOML.print(io, doc)
    end
    return path
end

# ---------------------------------------------------------------------------
# Worker process management

# Fixed preamble that resolves the Autotune module before the front-end
# preamble (which typically loads Dagger on all workers and establishes any
# process topology) runs.
const _WORKER_SCRIPT_HEAD = raw"""
srcdir = get(ENV, "DAGGER_AUTOTUNE_SRC", @__DIR__)
autotune = try
    Base.require(Main, :Dagger).Autotune
catch
    include(joinpath(srcdir, "Autotune.jl"))
    Main.Autotune
end
import Distributed
"""

# Build the worker script for this run, splicing in the front-end preamble
# (e.g. `@everywhere using Dagger` and any suite-op registrations).
function _worker_script(cfg::BenchmarkConfig)
    return string(_WORKER_SCRIPT_HEAD, "\n",
                  cfg.worker_preamble, "\n",
                  "autotune.run_worker(ARGS[1], ARGS[2])\n")
end

function _worker_cmd(cfg::BenchmarkConfig, proj, script::String,
                     trialsfile::String, outdir::String, nt::Int, np::Int)
    args = ["--startup-file=no", "--threads=$nt"]
    np > 0 && append!(args, ["-p", string(np)])
    proj === nothing || push!(args, "--project=$proj")
    append!(args, [script, trialsfile, outdir])
    cmd = Cmd(vcat(Base.julia_cmd().exec, args))
    return addenv(cmd, "DAGGER_AUTOTUNE_SRC" => @__DIR__)
end

_count_done(outdir, trials) = count(t -> isfile(_trial_path(outdir, t.id)), trials)

# Keys that carry an operation's problem size; everything else identifies the
# "series" a trial belongs to (same algorithm/config/categorical-features,
# varying only in scale).
const _SIZE_FEATURE_KEYS = ("m", "n", "k", "bytes", "sparse_size", "size")

_nonsize_features(f::Dict{String,Any}) =
    Dict{String,Any}(k => v for (k, v) in f if !(k in _SIZE_FEATURE_KEYS))

# Identifies the monotonic-scale "series" of a trial: after a crash/OOM at some
# scale, every not-yet-run trial in the same series at a larger-or-equal scale
# is skipped (it would almost certainly OOM/crash too). Mirrors benchmark/'s
# "skip this and all larger scales of the same suite/method" behavior.
_series_key(t::Trial) = (t.op, t.algorithm, _canonical(t.config),
                         _canonical(_nonsize_features(t.features)))

function _trial_scale(t::Trial)
    try
        return Float64(operation(t.op).scale(t.features))
    catch
        return NaN
    end
end

# Compact, fixed-width descriptor for one trial's live progress line, e.g.
# "  [ 3/21] :linalg_lu/dagger  1000×1000 [DArray,rocm]". The trailing time is
# appended by `_finish_progress_line` once the trial completes.
function _progress_prefix(t::Trial, i::Int, n::Int)
    f = t.features
    tags = String[]
    at = get(f, "array_type", "")
    at == "" || push!(tags, String(at))
    b = get(f, "backend", "cpu")
    b == "cpu" || push!(tags, String(b))
    tag = isempty(tags) ? "" : " [" * join(tags, ",") * "]"
    desc = string(":", t.op, "/", t.algorithm, "  ", _size_label(f), tag)
    return string("  [", lpad(i, length(string(n))), "/", n, "] ", rpad(desc, 44))
end

_read_trial_status(path::String) =
    try
        String(get(TOML.parsefile(path), "status", "ok"))
    catch
        "ok"
    end

# Close an open progress line with the elapsed wall time (and status if not ok).
_finish_progress_line(io, t0::Float64, status::AbstractString) =
    @printf(io, " %8.2f s%s\n", time() - t0, status == "ok" ? "" : "  <$status>")

"""
    _run_group(cfg, proj, script, trials, outdir, logdir, deadline) -> Bool

Run one process-group's trials, respawning workers past failures. A watchdog
kills workers that make no progress within the grace period; the in-flight
trial is recorded as "timeout" (killed) or "crashed" (died on its own) and
execution resumes at the next trial. Returns `false` if the overall deadline
expired.
"""
function _run_group(cfg::BenchmarkConfig, proj, script::String, trials::Vector{Trial},
                    outdir::String, logdir::String, deadline::Float64)
    # Order by ascending scale (then id) so the smallest size of every series
    # runs first: if it OOMs/crashes, the larger sizes are skipped before they
    # ever spawn.
    order = sort(trials; by = t -> (isnan(_trial_scale(t)) ? Inf : _trial_scale(t), t.id))
    nt = order[1].config["nthreads"]
    np = get(order[1].config, "nprocs", 0)
    startup_grace = 600.0                       # package load + precompile
    trial_grace = 2 * cfg.trial_time_limit + 60.0
    attempt = 0
    # series key => smallest scale known to crash/timeout; that scale and any
    # larger one in the same series is skipped.
    vetoed = Dict{Any,Float64}()
    while true
        remaining = Trial[]
        for t in order
            isfile(_trial_path(outdir, t.id)) && continue
            thr = get(vetoed, _series_key(t), Inf)
            sc = _trial_scale(t)
            if isfinite(thr) && isfinite(sc) && sc >= thr
                _write_result(_trial_path(outdir, t.id),
                    TrialResult(t.op, t.algorithm, t.features, t.config;
                                status="skipped_larger",
                                error="a smaller scale of this series crashed/timed out"))
                continue
            end
            push!(remaining, t)
        end
        isempty(remaining) && return true
        if time() > deadline
            for t in remaining
                _write_result(_trial_path(outdir, t.id),
                    TrialResult(t.op, t.algorithm, t.features, t.config;
                                status="skipped_time", error="total_time_limit reached"))
            end
            return false
        end
        attempt += 1
        if attempt > length(order) + 5
            @warn "Autotune: group (nthreads=$nt, nprocs=$np) exceeded respawn budget; " *
                  "marking $(length(remaining)) trials crashed"
            for t in remaining
                _write_result(_trial_path(outdir, t.id),
                    TrialResult(t.op, t.algorithm, t.features, t.config;
                                status="crashed", error="respawn budget exceeded"))
            end
            return true
        end

        trialsfile = joinpath(dirname(outdir), "trials_nt$(nt)_np$(np)_a$(attempt).toml")
        _write_trials_file(trialsfile, remaining, cfg)
        log = joinpath(logdir, "worker_nt$(nt)_np$(np)_a$(attempt).log")
        cmd = _worker_cmd(cfg, proj, script, trialsfile, outdir, nt, np)
        _vprintln(cfg, "Autotune: worker (nthreads=$nt, nprocs=$np) starting: " *
                       "$(length(remaining)) trials, log at $log")
        proc = run(pipeline(cmd; stdout=log, stderr=log); wait=false)

        # Live progress: the worker runs `remaining` in order and writes one
        # result file per finished trial, so the count of result files tells us
        # which trial is in flight. We leave the current trial's descriptor on an
        # open line and close it with the elapsed wall time when its result
        # appears (so growth with scale, and any hang, is visible at a glance).
        # The first line's time also covers worker startup/compilation.
        n = length(remaining)
        killed = false
        next_idx = 1
        cur_start = time()
        line_open = false
        if cfg.verbose
            print(stdout, _progress_prefix(remaining[1], 1, n)); flush(stdout)
            line_open = true
        end
        last_done = 0
        watchdog = time() + startup_grace
        while process_running(proc)
            sleep(0.25)
            d = _count_done(outdir, remaining)
            while cfg.verbose && next_idx <= d && next_idx <= n
                st = _read_trial_status(_trial_path(outdir, remaining[next_idx].id))
                _finish_progress_line(stdout, cur_start, st)
                line_open = false
                next_idx += 1
                cur_start = time()
                if next_idx <= n
                    print(stdout, _progress_prefix(remaining[next_idx], next_idx, n))
                    flush(stdout)
                    line_open = true
                end
            end
            if d > last_done
                last_done = d
                watchdog = time() + trial_grace
            end
            if time() > watchdog || time() > deadline
                killed = true
                kill(proc)
                # Escalate if it ignores SIGTERM.
                for _ in 1:10
                    process_running(proc) || break
                    sleep(1.0)
                end
                process_running(proc) && kill(proc, 9)  # SIGKILL
                break
            end
        end
        wait(proc)
        # A trial still in flight when the worker died/was killed leaves an open
        # line; close it so the next message starts cleanly.
        line_open && _finish_progress_line(stdout, cur_start, killed ? "killed" : "died")

        still = [t for t in remaining if !isfile(_trial_path(outdir, t.id))]
        isempty(still) && return true
        if !killed && success(proc)
            # Worker claims success yet trials are missing - don't loop forever.
            @warn "Autotune: worker exited cleanly with $(length(still)) trials missing; marking failed"
            for t in still
                _write_result(_trial_path(outdir, t.id),
                    TrialResult(t.op, t.algorithm, t.features, t.config;
                                status="failed", error="worker exited without producing a result"))
            end
            return true
        end
        victim = first(still)
        status = killed ? "timeout" : "crashed"
        _vprintln(cfg, "Autotune:   worker $(status) on trial $(victim.id) " *
                       "(:$(victim.op)/$(victim.algorithm)); respawning")
        _write_result(_trial_path(outdir, victim.id),
            TrialResult(victim.op, victim.algorithm, victim.features, victim.config;
                        status=status, error="worker $(status); see $(log)"))
        # Skip this and all larger scales of the same series on subsequent passes.
        vsc = _trial_scale(victim)
        if isfinite(vsc)
            k = _series_key(victim)
            vetoed[k] = min(get(vetoed, k, Inf), vsc)
        end
    end
end

# ---------------------------------------------------------------------------
# Driver

"""
    benchmark(cfg::BenchmarkConfig=BenchmarkConfig())
    benchmark(; kwargs...)

Run the benchmark sweep and merge results into the on-disk database. See
`BenchmarkConfig` for every knob; a few common recipes:

    Autotune.benchmark()                                   # everything, defaults
    Autotune.benchmark(; ops=[:lu, :gemm], nthreads=[8])
    Autotune.benchmark(; axes=Dict(:solve => Dict(
        "sparse_size" => [10^5, 10^6, 10^7],
        "density"     => [1e-5, 1e-4, 1e-3],
        "accuracy"    => [1e-4, 1e-8, 1e-12])))
    Autotune.benchmark(; project=:current, dry_run=true)   # inspect the plan
"""
benchmark(; kwargs...) = benchmark(BenchmarkConfig(; kwargs...))

"""
    run_sweep(cfg::BenchmarkConfig) -> Union{Nothing,Vector{TrialResult}}

Execute the full benchmark sweep (project setup, trial expansion, subprocess
workers with OOM-robust respawn) and return every `TrialResult` (both the
pre-skipped configurations and the measured trials). Performs **no** database
I/O and prints no report, so front-ends that manage their own persistence and
reporting (e.g. the regression suite) can consume the raw results directly.
Returns `nothing` for a `dry_run`. `benchmark(cfg)` is `run_sweep` plus a merge
into the on-disk database.
"""
function run_sweep(cfg::BenchmarkConfig)
    t_start = time()
    mkpath(cfg.workdir)
    proj, caps = ensure_project(cfg)
    trials, preresults = expand_trials(cfg, caps)

    if cfg.dry_run
        _print_dry_run(cfg, trials, preresults)
        return nothing
    end

    rundir = joinpath(cfg.workdir, Dates.format(Dates.now(), "yyyymmdd-HHMMSS"))
    outdir = joinpath(rundir, "results")
    logdir = joinpath(rundir, "logs")
    mkpath(outdir); mkpath(logdir)
    script = joinpath(rundir, "worker.jl")
    write(script, _worker_script(cfg))

    groups = Dict{Tuple{Int,Int},Vector{Trial}}()
    for t in trials
        key = (t.config["nthreads"], get(t.config, "nprocs", 0))
        push!(get!(() -> Trial[], groups, key), t)
    end
    _vprintln(cfg, "Autotune: $(length(trials)) trials across $(length(groups)) " *
                   "process group(s); $(length(preresults)) configurations pre-skipped")

    deadline = t_start + cfg.total_time_limit
    for key in sort!(collect(keys(groups)))
        ok = _run_group(cfg, proj, script, groups[key], outdir, logdir, deadline)
        ok || break
    end

    results = copy(preresults)
    for t in trials
        p = _trial_path(outdir, t.id)
        isfile(p) || continue
        try
            push!(results, _result_from_dict(Dict{String,Any}(TOML.parsefile(p))))
        catch err
            @warn "Autotune: unreadable result file $p" exception=err
        end
    end
    return results
end

function benchmark(cfg::BenchmarkConfig)
    t_start = time()
    results = run_sweep(cfg)
    results === nothing && return nothing   # dry run

    db = load_db(cfg.db_path)
    merge_results!(db, results)
    db.meta["machine"] = machine_fingerprint()
    save_db(db, cfg.db_path)
    reset_db!()

    _vprintln(cfg, "\nAutotune: merged $(length(results)) results into $(cfg.db_path) " *
                   "($(round(time() - t_start; digits=1)) s total)\n")
    cfg.verbose && report(db)
    return db
end

function _print_dry_run(cfg::BenchmarkConfig, trials, preresults)
    println("Autotune dry run: $(length(trials)) trials, $(length(preresults)) pre-skipped")
    byop = Dict{Symbol,Int}()
    for t in trials
        byop[t.op] = get(byop, t.op, 0) + 1
    end
    for (op, n) in sort!(collect(byop); by=first)
        println("  :$op  $n trials")
    end
    shown = 0
    for t in trials
        shown >= 20 && (println("  ..."); break)
        println("  [$(t.id)] :$(t.op)/$(t.algorithm) features=$(t.features) config=$(t.config)")
        shown += 1
    end
    return nothing
end

# ---------------------------------------------------------------------------
# Worker side

function _write_result(path::String, r::TrialResult)
    tmp = path * ".tmp"
    open(tmp, "w") do io
        TOML.print(io, _result_to_dict(r))
    end
    mv(tmp, path; force=true)
    return nothing
end

function _write_worker_meta(outdir::String)
    meta = Dict{String,Any}("machine" => machine_fingerprint())
    cuda = loaded_module("CUDA")
    if cuda !== nothing
        meta["gpu"] = try
            Bool(cuda.functional()) ? string(cuda.name(cuda.device())) : "non-functional"
        catch
            "unknown"
        end
    end
    path = joinpath(outdir, "worker_meta.toml")
    isfile(path) && return
    open(path, "w") do io
        TOML.print(io, meta)
    end
    return nothing
end

"""
    run_worker(trialsfile, outdir)

Worker-process entrypoint (invoked by the generated worker script). Runs
each pending trial, writing one result file per trial so the parent can
resume precisely after a crash.
"""
function run_worker(trialsfile::AbstractString, outdir::AbstractString)
    doc = TOML.parsefile(String(trialsfile))
    limits = Dict{String,Any}(get(doc, "limits", Dict{String,Any}()))
    _write_worker_meta(String(outdir))
    for td in get(doc, "trials", Any[])
        td = Dict{String,Any}(td)
        path = _trial_path(String(outdir), Int(td["id"]))
        isfile(path) && continue
        r = _run_one_trial(td, limits)
        _write_result(path, r)
        # Reclaim between trials on every process (distributed benchmarks leave
        # garbage on the added workers too).
        if nprocs() > 1
            try
                @everywhere GC.gc()
            catch
                GC.gc()
            end
        else
            GC.gc()
        end
    end
    return nothing
end

function _run_one_trial(td::Dict{String,Any}, limits::Dict{String,Any})
    op = Symbol(td["op"])
    algname = Symbol(td["algorithm"])
    features = Dict{String,Any}(td["features"])
    config = Dict{String,Any}(td["config"])
    fail(status, msg) = TrialResult(op, algname, features, config;
                                    status=status, error=msg)
    opspec = try
        operation(op)
    catch err
        return fail("failed", sprint(showerror, err))
    end
    alg = algorithm(op, algname)
    alg === nothing && return fail("failed", "algorithm not registered in worker")

    # Load required packages (may legitimately fail: no GPU, etc.)
    for pkg in get(td, "packages", Any[])
        mod = try
            Base.require(Main, Symbol(pkg))
        catch err
            return fail("skipped_unavailable", "could not load $pkg: $(sprint(showerror, err))")
        end
        check = get(FUNCTIONAL_CHECKS, String(pkg), nothing)
        if check !== nothing
            functional = try
                check(mod)
            catch
                false
            end
            functional || return fail("skipped_unavailable", "$pkg loaded but not functional")
        end
    end
    startswith(String(algname), "dagger_") && !has_raw_impl(algname) &&
        return fail("skipped_unavailable",
                    "no raw impl for $algname (Dagger integration not loaded)")

    membudget = Float64(get(limits, "memory_fraction", 0.5)) * Sys.total_memory()
    est = try
        Float64(opspec.input_bytes(features))
    catch
        0.0
    end
    est * 3 > membudget && return fail("skipped_mem", "estimated footprint over budget")

    changes = Tuple{Symbol,Any}[]
    for (k, v) in config
        c = configurable(Symbol(k))
        c !== nothing && c.mutability === MUTABLE && push!(changes, (Symbol(k), v))
    end

    return try
        with_configurables(changes) do
            # `invokelatest`: the packages above were `Base.require`d *inside*
            # this call stack, so any algorithm that dispatches into a
            # freshly-loaded module (e.g. `ctx.mod.lu` for RecursiveFactorization,
            # `ctx.mod.klu`, Krylov, LinearSolve) would otherwise hit a
            # "method too new to be called from this world context" error. Running
            # the trial at the latest world age makes those methods visible.
            Base.invokelatest(_timed_trial, opspec, alg, op, algname,
                              features, config, limits)
        end
    catch err
        fail("failed", sprint(showerror, err))
    end
end

function _timed_trial(opspec::OperationSpec, alg::AlgorithmSpec, op, algname,
                      features::Dict{String,Any}, config::Dict{String,Any},
                      limits::Dict{String,Any})
    base_inputs = opspec.generate_inputs(features)
    inputs = Any[base_inputs...]
    # Decouple mutated positional args from `base_inputs` *before* any
    # conversion or invocation: `opspec.check` below validates against
    # `base_inputs`, and when an algorithm needs no container conversion
    # `inputs[idx]` would otherwise be the *same object* as `base_inputs[idx]`
    # - letting `alg.invoke` mutate it (e.g. `lu!`-ing it into factors) would
    # silently corrupt the very reference the check compares against.
    for idx in opspec.mutated_args
        (idx <= length(inputs) && inputs[idx] isa AbstractArray) || continue
        inputs[idx] = copy(inputs[idx])
    end
    # `benchmark_only` ops build inputs already in the target form.
    if !opspec.benchmark_only
        for (i, x) in enumerate(inputs)
            x isa AbstractArray || continue
            cur = array_form(x)
            tgt = target_form_for(alg, x)
            cur === tgt && continue
            c = converter(cur, tgt)
            c === nothing && return TrialResult(op, algname, features, config;
                status="failed", error="no converter $cur -> $tgt for input staging")
            inputs[i] = c.convert(x, config)
        end
    end
    acc = haskey(features, "accuracy") ? Float64(features["accuracy"]) : nothing
    ctx = InvokeContext(algorithm_module(alg), features, config, acc)
    prepared = alg.prepare(ctx, inputs...)
    form = alg.input_form

    # For in-place operations, `alg.invoke` mutates the (possibly
    # container-converted) inputs. Re-prime them from a pristine copy before
    # every timed call (including the warmup) so every sample measures the
    # same work - otherwise e.g. re-running `lu!` on an already-factored
    # matrix would measure garbage after the first call. Relies on `prepare`
    # returning the same array objects as `inputs` by default (true for
    # every registered `!` algorithm; only the :transfer op, which has no
    # mutated_args, overrides `prepare`).
    pristine = Dict{Int,Any}()
    for idx in opspec.mutated_args
        (idx <= length(inputs) && inputs[idx] isa AbstractArray) || continue
        pristine[idx] = copy(inputs[idx])
    end
    reprime!() = for (idx, p) in pristine
        copyto!(inputs[idx], p)
    end

    run1 = () -> begin
        reprime!()
        r = alg.invoke(ctx, prepared...)
        sync_form(form)
        r
    end

    tlimit = Float64(get(limits, "trial_time_limit", 120.0))
    tsample = Float64(get(limits, "sample_time", 1.0))
    smax = Int(get(limits, "max_samples", 7))
    gcsample = Bool(get(limits, "gcsample", false))
    finalize = opspec.finalize_sample

    t0 = time()
    result = run1()                       # warmup (includes compilation)
    finalize()
    twarm = time() - t0

    # `benchmark_only` ops skip validation: they carry no `check`, and
    # `collect`ing a large distributed result purely to validate it could OOM
    # the worker on a result the distributed op itself handled fine.
    if !opspec.benchmark_only
        err = opspec.check(features, base_inputs,
                           result isa AbstractArray ? collect(result) : result)
        err === nothing || return TrialResult(op, algname, features, config;
                                              status="check_failed", error=String(err))
    end
    result = nothing

    # Per-sample wall time, allocated bytes, and allocation count (via `@timed`,
    # so we avoid a BenchmarkTools dependency yet still capture what a front-end
    # needs to build a full `Trial`). `finalize()` runs *outside* the timed
    # region so per-sample GC/teardown never counts against the measurement.
    times = Float64[]
    mems = Int[]
    nallocs = Int[]
    budget_end = t0 + tlimit
    while (sum(times; init=0.0) < tsample || length(times) < 2) &&
          length(times) < smax && time() + twarm * 0.9 < budget_end
        gcsample && GC.gc()
        st = @timed run1()
        finalize()
        push!(times, st.time)
        push!(mems, Int(st.bytes))
        push!(nallocs, Int(Base.gc_alloc_count(st.gcstats)))
    end
    if isempty(times)                     # compile-polluted, but better than nothing
        push!(times, twarm); push!(mems, 0); push!(nallocs, 0)
    end

    imin = argmin(times)
    return TrialResult(op, algname, features, config;
                       status="ok", time_s=times[imin], times_s=times,
                       memory=mems[imin], allocs=nallocs[imin], samples=length(times))
end

# ---------------------------------------------------------------------------
# Human-readable report

_fmt_time(t::Float64) = !isfinite(t) ? "-" :
    t < 1e-3 ? @sprintf("%.3g µs", t * 1e6) :
    t < 1.0  ? @sprintf("%.3g ms", t * 1e3) :
               @sprintf("%.3g s", t)

_size_label(f::Dict{String,Any}) =
    haskey(f, "bytes") ? @sprintf("%.3g MB", Float64(f["bytes"]) / 1e6) :
    haskey(f, "k")     ? "$(f["m"])×$(f["k"])×$(f["n"])" :
    haskey(f, "m")     ? "$(f["m"])×$(f["n"])" : "?"

function _group_label(f::Dict{String,Any}, c::Dict{String,Any})
    parts = String[]
    for k in ("array_type", "eltype")
        haskey(f, k) && push!(parts, string(f[k]))
    end
    haskey(f, "density") && push!(parts, "density=$(f["density"])")
    haskey(f, "accuracy") && push!(parts, "acc=$(f["accuracy"])")
    get(f, "spd", false) == true && push!(parts, "spd")
    get(f, "locality", "local") == "remote" && push!(parts, "remote")
    haskey(c, "nthreads") && push!(parts, "$(c["nthreads"])T")
    haskey(c, "nprocs") && push!(parts, "$(c["nprocs"])P")
    return join(parts, ", ")
end

_alg_label(algname::Symbol, c::Dict{String,Any}) =
    haskey(c, "blocksize") ? "$(algname)(bs=$(c["blocksize"]))" : string(algname)

"""
    report(db::ResultDB=current_db(); io=stdout)

Human-readable summary: per operation and configuration group, the timing of
every benchmarked algorithm at every scale, with the winner marked `*`, plus
counts of failed/skipped configurations.
"""
function report(db::ResultDB=current_db(); io::IO=stdout)
    if isempty(db)
        println(io, "Autotune: benchmark database is empty. Run Dagger.benchmark().")
        return nothing
    end
    machine = get(db.meta, "machine", Dict{String,Any}())
    println(io, "Autotune benchmark report")
    println(io, "  machine: ", get(machine, "hostname", "?"), " — ",
            get(machine, "cpu", "?"), ", ",
            round(get(machine, "memory_bytes", 0) / 2^30; digits=1), " GiB, Julia ",
            get(machine, "julia", "?"))
    nbad = count(trial_veto, db.results)
    nskip = count(r -> startswith(r.status, "skipped"), db.results)
    println(io, "  results: ", length(db.results), " total (",
            count(trial_ok, db.results), " ok, ", nbad, " failed/vetoed, ",
            nskip, " skipped)")

    for op in sort!(unique!([r.op for r in db.results]))
        rs = results_for(db, op)
        println(io, "\n:", op)
        # group_label => size_label => alg_label => best time
        groups = Dict{String,Dict{String,Dict{String,Float64}}}()
        for r in rs
            trial_ok(r) || continue
            g = _group_label(r.features, r.config)
            s = _size_label(r.features)
            a = _alg_label(r.algorithm, r.config)
            d = get!(() -> Dict{String,Dict{String,Float64}}(), groups, g)
            dd = get!(() -> Dict{String,Float64}(), d, s)
            dd[a] = min(get(dd, a, Inf), r.time_s)
        end
        if isempty(groups)
            println(io, "  (no successful results)")
        end
        for g in sort!(collect(keys(groups)))
            println(io, "  [", g, "]")
            sizes = collect(keys(groups[g]))
            sort!(sizes; by = s -> begin
                idx = findfirst(r -> trial_ok(r) && _size_label(r.features) == s, rs)
                idx === nothing ? 0.0 : Float64(operation(op).scale(rs[idx].features))
            end)
            for s in sizes
                entries = sort!(collect(groups[g][s]); by = last)
                cells = [i == 1 ? "$(k): $(_fmt_time(v)) *" : "$(k): $(_fmt_time(v))"
                         for (i, (k, v)) in enumerate(entries)]
                @printf(io, "    %-16s %s\n", s, join(cells, "   "))
            end
        end
        nfail = count(trial_veto, rs)
        nfail > 0 && println(io, "  (", nfail,
            " failed/timed-out configurations recorded; those algorithm+scale ",
            "combinations are vetoed at runtime)")
    end
    return nothing
end
