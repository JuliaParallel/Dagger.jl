# Profile mode for the linalg integration kernels.
#
#   LINALG_BENCH_PROFILE=1 julia --project=… -t 16 benchmark/suites/linalg_profile.jl
#   (or via run_linalg_integration.sh with LINALG_BENCH_PROFILE=1)
#
# Environment (in addition to linalg_integration.jl):
#   LINALG_BENCH_PROFILE        1 | cpu | alloc | logs | all   (default: all)
#   LINALG_BENCH_ONLY           comma-separated profile keys
#   LINALG_BENCH_PROFILE_REPS   extra Profile.@profile repeats for short kernels
#   LINALG_BENCH_PROFILE_ITMAX  Krylov itmax used inside this script (default 8)
#
# Does not run the published full-solve benches (those can be hours under
# Profile). Times a few representative applies, then attributes time with
# Profile, Profile.Allocs, Base.gc_num (AGENTS.md lesson 4), and Dagger logs.
#
# Never run this on the Dagger-linalg-ultra workstation. AWS only.

include(joinpath(@__DIR__, "linalg_integration.jl"))

using Profile

const PROFILE_KIND = lowercase(get(ENV, "LINALG_BENCH_PROFILE", "all"))
const WANT_CPU = PROFILE_KIND in ("1", "true", "cpu", "all")
const WANT_ALLOC = PROFILE_KIND in ("1", "true", "alloc", "all")
const WANT_LOGS = PROFILE_KIND in ("1", "true", "logs", "all")
const PROFILE_REPS = parse(Int, get(ENV, "LINALG_BENCH_PROFILE_REPS", "8"))
const PROFILE_ITMAX = parse(Int, get(ENV, "LINALG_BENCH_PROFILE_ITMAX", "8"))
const PROFILE_WARMUP = parse(Int, get(ENV, "LINALG_BENCH_PROFILE_WARMUP", "10"))
const PROFILE_SAMPLES = parse(Int, get(ENV, "LINALG_BENCH_PROFILE_SAMPLES", "5"))

const PROFILE_ROWS = Dict{String,Any}[]

function profile_wanted(key)
    ONLY === nothing && return true
    key in ONLY && return true
    for o in ONLY
        startswith(key, o * "_") && return true
        startswith(o, key * "_") && return true
    end
    return false
end

function gc_delta(f; warmup=PROFILE_WARMUP, samples=PROFILE_SAMPLES)
    for _ in 1:warmup
        f()
    end
    GC.gc()
    best_t = Inf
    best_allocs = typemax(Int)
    best_bytes = typemax(Int)
    last_t = NaN
    for _ in 1:samples
        before = Base.gc_num()
        t = @elapsed f()
        diff = Base.GC_Diff(Base.gc_num(), before)
        last_t = t
        best_t = min(best_t, t)
        best_allocs = min(best_allocs, Base.gc_alloc_count(diff))
        best_bytes = min(best_bytes, Int(diff.allocd))
    end
    return (; time_s=best_t, last_s=last_t, allocs=best_allocs, bytes=best_bytes)
end

function classify_frame(file::AbstractString, func::AbstractString)
    f = lowercase(file)
    fn = lowercase(string(func))
    if occursin("gcsweep", f) || occursin("/gc.", f) || occursin("gc64", f) ||
       occursin("gc.c", f) || fn == "gc_collect" || occursin("gc_pool", fn)
        return "gc"
    elseif occursin("/sch/", f) || occursin("sch.jl", f)
        return "scheduler"
    elseif occursin("datadeps", f)
        return "datadeps"
    elseif occursin("thunk.jl", f) || occursin("dtask.jl", f) ||
           occursin("task-tls", f) || occursin("options.jl", f)
        return "spawn_thunk"
    elseif occursin("/array/mul.jl", f) || occursin("/array/linalg.jl", f) ||
           occursin("/array/copy.jl", f) || occursin("/array/alloc.jl", f)
        return "linalg_wrapper"
    elseif occursin("sparsearrays", f) || occursin("sparsematrixcsc", f) ||
           occursin("cholmod", f) || occursin("umfpack", f)
        return "sparse_compute"
    elseif occursin("openblas", f) || occursin("blis", f) || occursin("mkl", f) ||
           occursin("gemm", fn) || occursin("gemv", fn)
        return "blas"
    elseif occursin("krylov", f)
        return "krylov"
    else
        return "other"
    end
end

function summarize_cpu_profile()
    data, lidict = Profile.retrieve()
    buckets = Dict{String,Int}()
    inclusive = Dict{String,Int}()
    samples = 0
    i = 1
    n = length(data)
    while i <= n
        data[i] == 0 && (i += 1; continue)
        samples += 1
        seen = Set{String}()
        while i <= n && data[i] != 0
            ip = data[i]
            if haskey(lidict, ip)
                for sf in lidict[ip]
                    file = string(sf.file)
                    func = string(sf.func)
                    key = string(func, " @ ", file, ":", sf.line)
                    inclusive[key] = get(inclusive, key, 0) + 1
                    push!(seen, classify_frame(file, func))
                end
            end
            i += 1
        end
        for b in seen
            buckets[b] = get(buckets, b, 0) + 1
        end
        i += 1
    end
    tops = sort(collect(inclusive); by=last, rev=true)
    tops = first(tops, min(24, length(tops)))
    pct = Dict{String,Float64}()
    if samples > 0
        for (k, v) in buckets
            pct[k] = 100 * v / samples
        end
    end
    return Dict{String,Any}(
        "samples" => samples,
        "bucket_samples" => buckets,
        "bucket_pct" => pct,
        "top_inclusive" => [Dict("frame"=>k, "count"=>v) for (k, v) in tops],
    )
end

function cpu_profile(f; reps=PROFILE_REPS)
    WANT_CPU || return nothing
    Profile.clear()
    # One extra call so the profiled body is the steady-state path.
    f()
    Profile.@profile begin
        for _ in 1:reps
            f()
        end
    end
    return summarize_cpu_profile()
end

function alloc_profile(f; sample_rate=0.05)
    WANT_ALLOC || return nothing
    try
        Profile.Allocs.clear()
        Profile.Allocs.@profile sample_rate=sample_rate f()
        raw = Profile.Allocs.fetch()
        bytype = Dict{String,NamedTuple{(:count,:bytes),Tuple{Int,Int}}}()
        bysite = Dict{String,Int}()
        nbytes = 0
        nalloc = 0
        for a in raw.allocs
            nalloc += 1
            nbytes += Int(a.size)
            t = string(a.type)
            prev = get(bytype, t, (count=0, bytes=0))
            bytype[t] = (count=prev.count + 1, bytes=prev.bytes + Int(a.size))
            if !isempty(a.stacktrace)
                sf = a.stacktrace[1]
                site = string(sf.func, " @ ", sf.file, ":", sf.line)
                bysite[site] = get(bysite, site, 0) + 1
            end
        end
        types = sort(collect(bytype); by=x -> x[2].bytes, rev=true)
        sites = sort(collect(bysite); by=last, rev=true)
        return Dict{String,Any}(
            "sample_rate" => sample_rate,
            "sampled_allocs" => nalloc,
            "sampled_bytes" => nbytes,
            "top_types" => [Dict("type"=>k, "count"=>v.count, "bytes"=>v.bytes)
                            for (k, v) in first(types, min(12, length(types)))],
            "top_sites" => [Dict("site"=>k, "count"=>v)
                            for (k, v) in first(sites, min(12, length(sites)))],
        )
    catch err
        return Dict{String,Any}("error" => sprint(showerror, err))
    end
end

function analyze_logs(logs)
    logs === nothing && return nothing
    isempty(logs) && return Dict{String,Any}("compute_tasks" => 0, "error" => "empty logs")
    cat_count = Dict{String,Int}()
    cat_time = Dict{String,Float64}()
    ncompute = 0
    Dagger.logs_event_pairs(logs) do w, start_idx, finish_idx
        core_s = logs[w][:core][start_idx]
        core_f = logs[w][:core][finish_idx]
        cat = string(core_s.category)
        dur = (Float64(core_f.timestamp) - Float64(core_s.timestamp)) / 1e9
        cat_count[cat] = get(cat_count, cat, 0) + 1
        cat_time[cat] = get(cat_time, cat, 0.0) + dur
        cat == "compute" && (ncompute += 1)
    end
    names = Dict{String,Int}()
    if haskey(first(values(logs)), :taskfuncnames)
        for w in keys(logs)
            for name in logs[w][:taskfuncnames]
                name === nothing && continue
                s = string(name)
                names[s] = get(names, s, 0) + 1
            end
        end
    elseif haskey(first(values(logs)), :tasknames)
        for w in keys(logs)
            for name in logs[w][:tasknames]
                name === nothing && continue
                s = string(name)
                names[s] = get(names, s, 0) + 1
            end
        end
    end
    topnames = sort(collect(names); by=last, rev=true)
    return Dict{String,Any}(
        "compute_tasks" => ncompute,
        "category_count" => cat_count,
        "category_time_s" => cat_time,
        "top_task_names" => [Dict("name"=>k, "count"=>v)
                             for (k, v) in first(topnames, min(16, length(topnames)))],
    )
end

function logged_run(f)
    WANT_LOGS || return nothing
    try
        Dagger.enable_logging!(; all_task_deps=false, tasknames=true,
                               taskfuncnames=true, metrics=false)
        Dagger.fetch_logs!()
        f()
        logs = Dagger.fetch_logs!()
        Dagger.disable_logging!()
        return analyze_logs(logs)
    catch err
        try
            Dagger.disable_logging!()
        catch
        end
        return Dict{String,Any}("error" => sprint(showerror, err))
    end
end

function nonempty_chunk_count(A::Dagger.DArray)
    n = 0
    for c in A.chunks
        tile = fetch(c)
        mat = tile isa Dagger.DSparseArray ? tile.mat : tile
        if mat isa AbstractSparseMatrix
            nnz(mat) > 0 && (n += 1)
        else
            n += 1
        end
    end
    return n
end

function push_profile!(; key, feature, problem, notes="",
                       time_s=nothing, host_s=nothing, allocs=nothing, bytes=nothing,
                       ntiles=nothing, nnz_tiles=nothing, extra=nothing,
                       cpu=nothing, allocprof=nothing, logs=nothing, error=nothing)
    row = Dict{String,Any}(
        "key" => key,
        "feature" => feature,
        "problem" => problem,
        "notes" => notes,
        "time_s" => time_s,
        "host_s" => host_s,
        "allocs" => allocs,
        "bytes" => bytes,
        "ntiles" => ntiles,
        "nnz_tiles" => nnz_tiles,
        "extra" => extra,
        "cpu" => cpu,
        "allocprof" => allocprof,
        "logs" => logs,
        "error" => error,
    )
    push!(PROFILE_ROWS, row)
    if is_root()
        ts = time_s isa Real ? string(round(time_s * 1e3; digits=3), " ms") : string(time_s)
        as = allocs === nothing ? "" : "  allocs=$(allocs) bytes=$(bytes)"
        hs = host_s isa Real ? string("  host=", round(host_s * 1e3; digits=3), " ms") : ""
        println("PROFILE ", key, "  D=", ts, hs, as,
                error === nothing ? "" : "  ERR=$(error)")
        flush(stdout)
        _flush_profile()
    end
    return row
end

function _flush_profile()
    is_root() || return
    tag = MODE == "mpi" ? "mpi" : "mt"
    json_path = joinpath(OUTDIR, "linalg_profile_$(tag).json")
    md_path = joinpath(OUTDIR, "linalg_profile_$(tag).md")
    payload = Dict(
        "meta" => merge(META, Dict(
            "profile_kind" => PROFILE_KIND,
            "profile_warmup" => PROFILE_WARMUP,
            "profile_samples" => PROFILE_SAMPLES,
            "profile_reps" => PROFILE_REPS,
            "profile_itmax" => PROFILE_ITMAX,
        )),
        "rows" => PROFILE_ROWS,
    )
    open(json_path, "w") do io
        _write_json(io, payload)
    end
    open(md_path, "w") do io
        write(io, profile_markdown(PROFILE_ROWS))
    end
end

function fmt_pct(cpu)
    cpu === nothing && return "—"
    cpu isa Dict && haskey(cpu, "error") && return "err"
    pct = cpu["bucket_pct"]
    parts = String[]
    for k in ("scheduler", "datadeps", "spawn_thunk", "linalg_wrapper",
              "sparse_compute", "blas", "krylov", "gc", "other")
        haskey(pct, k) || continue
        v = pct[k]
        v >= 1 || continue
        push!(parts, string(k, "=", round(v; digits=1), "%"))
    end
    return isempty(parts) ? "—" : join(parts, ", ")
end

function profile_markdown(rows)
    io = IOBuffer()
    println(io, "| Key | Problem | Dagger | Host | Allocs / bytes | Tasks | CPU buckets (sample %) | Notes |")
    println(io, "|---|---|---|---|---|---|---|---|")
    for r in rows
        logs = r["logs"]
        tasks = logs isa Dict && haskey(logs, "compute_tasks") ? logs["compute_tasks"] : "—"
        extra = r["error"] === nothing ? r["notes"] : "ERROR: $(r["error"])"
        println(io, "| ", r["key"], " | ", r["problem"], " | ",
                fmt_s(r["time_s"]), " | ", fmt_s(r["host_s"]), " | ",
                r["allocs"] === nothing ? "—" : string(r["allocs"], " / ", r["bytes"]),
                " | ", tasks, " | ", fmt_pct(r["cpu"]), " | ", extra, " |")
    end
    println(io)
    println(io, "### Top inclusive frames (per kernel)")
    for r in rows
        cpu = r["cpu"]
        cpu isa Dict && haskey(cpu, "top_inclusive") || continue
        println(io, "\n#### ", r["key"])
        println(io, "samples=", get(cpu, "samples", "—"))
        for fr in cpu["top_inclusive"]
            println(io, "- ", fr["count"], "  `", fr["frame"], "`")
        end
        logs = r["logs"]
        if logs isa Dict && haskey(logs, "category_time_s")
            println(io, "log category time (s): ", logs["category_time_s"])
            println(io, "log category count: ", logs["category_count"])
        end
        ap = r["allocprof"]
        if ap isa Dict && haskey(ap, "top_types")
            println(io, "sampled alloc types: ", ap["top_types"])
        end
    end
    return String(take!(io))
end

macro safe_profile(key, ex)
    quote
        if profile_wanted($(esc(key)))
            try
                $(esc(ex))
            catch err
                is_root() && @error "profile $($(esc(key))) failed" exception = (err, catch_backtrace())
                push_profile!(key=$(esc(key)), feature=$(esc(key)), problem="(failed)",
                              error=sprint(showerror, err))
            end
            GC.gc()
        end
    end
end

# --- kernels ---------------------------------------------------------------

function profile_blas1()
    n, b4 = 4096, 1024
    # Published Krylov layout: 4 tiles. Also 16 tiles (the 5× CG probe).
    for (tag, b) in (("4tile", b4), ("16tile", 256))
        xh = rand(n)
        yh = rand(n)
        x = distribute(xh, Blocks(b)); wait(x)
        y = distribute(yh, Blocks(b)); wait(y)
        ntiles = length(x.chunks)
        for (op, fd, fh) in (
            ("dot", () -> LinearAlgebra.dot(x, y), () -> LinearAlgebra.dot(xh, yh)),
            ("axpy", () -> (LinearAlgebra.axpy!(0.1, x, y); wait(y)), () -> LinearAlgebra.axpy!(0.1, xh, yh)),
            ("axpby", () -> (LinearAlgebra.axpby!(0.1, x, 0.9, y); wait(y)), () -> LinearAlgebra.axpby!(0.1, xh, 0.9, yh)),
            ("rmul", () -> (LinearAlgebra.rmul!(y, 0.99); wait(y)), () -> LinearAlgebra.rmul!(yh, 0.99)),
            ("norm", () -> LinearAlgebra.norm(y), () -> LinearAlgebra.norm(yh)),
            ("copyto", () -> (copyto!(y, x); wait(y)), () -> copyto!(yh, xh)),
            ("fill", () -> (fill!(y, 0.0); wait(y)), () -> fill!(yh, 0.0)),
        )
            key = "blas1_$(op)_$(tag)"
            profile_wanted(key) || continue
            with_blas(BLAS_DAGGER) do
                d = gc_delta(fd)
                hs = gc_delta(fh).time_s
                cpu = cpu_profile(fd; reps=max(PROFILE_REPS, 20))
                logs = logged_run(fd)
                ap = op in ("axpy", "dot") ? alloc_profile(fd) : nothing
                push_profile!(key=key, feature="BLAS-1 $(op)",
                              problem="n=$(n), tiles=$(ntiles), block=$(b)",
                              time_s=d.time_s, host_s=hs, allocs=d.allocs, bytes=d.bytes,
                              ntiles=ntiles, cpu=cpu, allocprof=ap, logs=logs,
                              notes="Krylov calls this every iteration")
            end
        end
    end
end

function profile_spmv()
    # Published 1-D Laplacian + the Krylov 2-D operator at 4 / 16 tiles.
    specs = (
        (key="sparse_spmv", n=S.spmv_n, b=S.spmv_b, builder=_ -> laplacian_1d(Float64, S.spmv_n)),
        (key="sparse_spmv_krylov4", n=S.krylov_grid^2, b=S.krylov_b, builder=_ -> laplacian_2d(Float64, S.krylov_grid)),
        (key="sparse_spmv_krylov16", n=S.krylov_grid^2, b=256, builder=_ -> laplacian_2d(Float64, S.krylov_grid)),
    )
    for spec in specs
        profile_wanted(spec.key) || continue
        n, b = spec.n, spec.b
        Ah = spec.builder(n)
        xh = rand(n)
        yh = zeros(n)
        with_blas(BLAS_DAGGER) do
            A = distribute(Ah, Blocks(b, b)); wait(A)
            x = distribute(xh, Blocks(b)); wait(x)
            y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
            ntiles = length(A.chunks)
            nnz_tiles = nonempty_chunk_count(A)
            fd = () -> (mul!(y, A, x); wait(y))
            fh = () -> mul!(yh, Ah, xh)
            d = gc_delta(fd)
            hs = gc_delta(fh).time_s
            cpu = cpu_profile(fd; reps=max(PROFILE_REPS, 12))
            logs = logged_run(fd)
            ap = spec.key == "sparse_spmv_krylov4" ? alloc_profile(fd) : nothing
            push_profile!(key=spec.key, feature="Sparse SpMV",
                          problem="n=$(n), nnz=$(nnz(Ah)), tile=$(b), chunks=$(ntiles), nnz_tiles=$(nnz_tiles)",
                          time_s=d.time_s, host_s=hs, allocs=d.allocs, bytes=d.bytes,
                          ntiles=ntiles, nnz_tiles=nnz_tiles, cpu=cpu, allocprof=ap, logs=logs,
                          notes="gemv_dagger! spawns every (row,col) tile pair, including structural zeros")
        end
    end
end

function profile_gemm()
    n, b = S.gemm_n, S.gemm_b
    with_blas(BLAS_DAGGER) do
        A = rand(Blocks(b, b), Float64, n, n); wait(A)
        B = rand(Blocks(b, b), Float64, n, n); wait(B)
        fd = () -> (C = A * B; wait(C))
        d = gc_delta(fd; warmup=3, samples=3)
        Ah = rand(n, n); Bh = rand(n, n)
        hs = with_blas(BLAS_BASELINE) do
            gc_delta(() -> Ah * Bh; warmup=3, samples=3).time_s
        end
        cpu = cpu_profile(fd; reps=2)
        logs = logged_run(fd)
        push_profile!(key="dense_gemm", feature="Dense GEMM",
                      problem="n=$(n), tile=$(b)×$(b), C←A*B",
                      time_s=d.time_s, host_s=hs, allocs=d.allocs, bytes=d.bytes,
                      ntiles=length(A.chunks), cpu=cpu, logs=logs,
                      notes="Dagger BLAS=1; host BLAS=$(BLAS_BASELINE); closest published row (0.80×)")
    end
end

function profile_krylov_decomp()
    grid, b = S.krylov_grid, S.krylov_b
    n = grid * grid
    Ah = laplacian_2d(Float64, grid)
    bh = rand(n)
    A = distribute(Ah, Blocks(b, b)); wait(A)
    x = distribute(rand(n), Blocks(b)); wait(x)
    y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
    p = distribute(rand(n), Blocks(b)); wait(p)
    r = distribute(rand(n), Blocks(b)); wait(r)

    # One CG-shaped iteration, timed as a bundle and as pieces.
    function one_iter()
        mul!(y, A, p); wait(y)
        γ = LinearAlgebra.dot(r, r)
        δ = LinearAlgebra.dot(p, y)
        α = γ / δ
        LinearAlgebra.axpy!(α, p, x); wait(x)
        LinearAlgebra.axpy!(-α, y, r); wait(r)
        γ2 = LinearAlgebra.dot(r, r)
        LinearAlgebra.axpby!(1.0, r, γ2 / γ, p); wait(p)
        return LinearAlgebra.norm(r)
    end
    d = gc_delta(one_iter)
    cpu = cpu_profile(one_iter; reps=max(PROFILE_REPS, 8))
    logs = logged_run(one_iter)
    push_profile!(key="krylov_cg_oneiter", feature="CG-shaped iteration (decomp)",
                  problem="2-D Laplacian $(grid)×$(grid), tile=$(b)×$(b)",
                  time_s=d.time_s, allocs=d.allocs, bytes=d.bytes,
                  ntiles=length(x.chunks), cpu=cpu, logs=logs,
                  notes="1 SpMV + 3 dots + 2 axpy + 1 axpby + 1 norm (not Krylov.jl itself)")

    # Short real Krylov.cg — published table is 196 iters / 3.37 s.
    Db = distribute(bh, Blocks(b)); wait(Db)
    fd = () -> begin
        xsol, _ = Krylov.cg(A, Db; atol=ATOL, rtol=RTOL, itmax=PROFILE_ITMAX)
        wait_d(xsol)
    end
    d = gc_delta(fd; warmup=2, samples=2)
    cpu = cpu_profile(fd; reps=1)
    logs = logged_run(fd)
    push_profile!(key="krylov_cg_short", feature="Krylov.cg short",
                  problem="2-D Laplacian $(grid)×$(grid), itmax=$(PROFILE_ITMAX), tile=$(b)×$(b)",
                  time_s=d.time_s, allocs=d.allocs, bytes=d.bytes,
                  ntiles=length(x.chunks), cpu=cpu, logs=logs,
                  extra=Dict("itmax"=>PROFILE_ITMAX),
                  notes="short itmax; published full solve is 196 iters / 3.37 s")

    # Short GMRES — published is 192 iters / 77 s (memory=50).
    fd = () -> begin
        xsol, _ = Krylov.gmres(A, Db; atol=ATOL, rtol=RTOL, itmax=PROFILE_ITMAX, memory=GMRES_MEM)
        wait_d(xsol)
    end
    d = gc_delta(fd; warmup=1, samples=1)
    cpu = cpu_profile(fd; reps=1)
    logs = logged_run(fd)
    push_profile!(key="krylov_gmres_short", feature="Krylov.gmres short",
                  problem="2-D Laplacian $(grid)×$(grid), itmax=$(PROFILE_ITMAX), memory=$(GMRES_MEM)",
                  time_s=d.time_s, allocs=d.allocs, bytes=d.bytes,
                  ntiles=length(x.chunks), cpu=cpu, logs=logs,
                  extra=Dict("itmax"=>PROFILE_ITMAX, "memory"=>GMRES_MEM),
                  notes="short itmax; published full solve is 192 iters / 77.4 s")
end

function profile_pc_apply()
    grid, b = S.krylov_grid, S.krylov_b
    n = grid * grid
    Ah = laplacian_2d(Float64, grid)
    A = distribute(Ah, Blocks(b, b)); wait(A)
    x = distribute(rand(n), Blocks(b)); wait(x)
    y = Dagger.zeros(Blocks(b), Float64, n); wait(y)

    Pj = Dagger.JacobiPreconditioner(A)
    fd = () -> (mul!(y, Pj, x); wait(y))
    d = gc_delta(fd)
    push_profile!(key="pc_jacobi_apply", feature="Jacobi apply",
                  problem="2-D Laplacian $(grid)×$(grid), tile=$(b)×$(b)",
                  time_s=d.time_s, allocs=d.allocs, bytes=d.bytes,
                  ntiles=length(x.chunks),
                  cpu=cpu_profile(fd; reps=max(PROFILE_REPS, 12)),
                  logs=logged_run(fd),
                  notes="setup excluded; one mul! of JacobiPreconditioner")

    setup_t = @elapsed (Pb = Dagger.BlockJacobiPreconditioner(A))
    fd = () -> (mul!(y, Pb, x); wait(y))
    d = gc_delta(fd)
    push_profile!(key="pc_blockjacobi_apply", feature="BlockJacobi apply",
                  problem="2-D Laplacian $(grid)×$(grid), tile=$(b)×$(b)",
                  time_s=d.time_s, allocs=d.allocs, bytes=d.bytes,
                  ntiles=length(x.chunks),
                  cpu=cpu_profile(fd; reps=max(PROFILE_REPS, 8)),
                  logs=logged_run(fd),
                  extra=Dict("setup_s"=>setup_t),
                  notes="setup=$(round(setup_t; digits=3))s excluded from apply")
end

function profile_assembly()
    grid, b = S.assembly_grid, S.assembly_b
    n = grid * grid
    Ah = laplacian_2d(Float64, grid)
    I, J, V = findnz(Ah)
    part = Blocks(b, b)
    fd = () -> (A = SparseArrays.sparse(I, J, V, n, n, part); wait(A))
    d = gc_delta(fd; warmup=3, samples=3)
    fh = () -> begin
        Sloc = SparseArrays.sparse(I, J, V, n, n)
        A = distribute(Sloc, part)
        wait(A)
    end
    hs = gc_delta(fh; warmup=3, samples=3).time_s
    push_profile!(key="assembly", feature="Incremental sparse(I,J,V, Blocks)",
                  problem="2-D Laplacian COO $(grid)×$(grid), nnz=$(length(V)), tile=$(b)×$(b)",
                  time_s=d.time_s, host_s=hs, allocs=d.allocs, bytes=d.bytes,
                  cpu=cpu_profile(fd; reps=2),
                  logs=logged_run(fd),
                  notes="host baseline is sparse then distribute")
end

function profile_sparse_direct()
    grid, b = S.direct_grid, S.direct_b
    n = grid * grid
    Ah = laplacian_2d(Float64, grid)
    bh = rand(n)
    A = distribute(Ah, Blocks(b, b)); wait(A)
    db = distribute(bh, Blocks(b)); wait(db)
    setup_t = @elapsed (F = cholesky(A))
    fd = () -> (x = F \ db; wait_d(x))
    d = gc_delta(fd; warmup=2, samples=3)
    Fh = cholesky(Ah)
    hs = gc_delta(() -> Fh \ bh; warmup=2, samples=3).time_s
    push_profile!(key="sparse_chol_apply", feature="Sparse cholesky apply",
                  problem="2-D Laplacian $(grid)×$(grid), tile=$(b)×$(b)",
                  time_s=d.time_s, host_s=hs, allocs=d.allocs, bytes=d.bytes,
                  extra=Dict("setup_s"=>setup_t),
                  cpu=cpu_profile(fd; reps=2),
                  logs=logged_run(fd),
                  notes="setup=$(round(setup_t; digits=3))s (gather+CHOLMOD) excluded from apply")
    # One combined factor+solve so setup shows up in the CPU profile.
    fd2 = () -> begin
        F2 = cholesky(A)
        x = F2 \ db
        wait_d(x)
    end
    d2 = gc_delta(fd2; warmup=1, samples=2)
    push_profile!(key="sparse_chol_factor_solve", feature="Sparse cholesky factor+\\",
                  problem="2-D Laplacian $(grid)×$(grid), tile=$(b)×$(b)",
                  time_s=d2.time_s, allocs=d2.allocs, bytes=d2.bytes,
                  cpu=cpu_profile(fd2; reps=1),
                  logs=logged_run(fd2),
                  notes="includes gather; published combined time 2.42 s")
end

function profile_projected()
    n, b = S.op_n, S.op_b
    Ah = laplacian_1d(Float64, n)
    A = distribute(Ah, Blocks(b, b)); wait(A)
    N = distribute(ones(n), Blocks(b)); wait(N)
    PA = Dagger.Projected(A, N)
    x = distribute(rand(n), Blocks(b)); wait(x)
    y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
    fd = () -> (mul!(y, PA, x); wait(y))
    d = gc_delta(fd)
    push_profile!(key="projected", feature="Projected mul!",
                  problem="1-D Laplacian n=$(n), tile=$(b)",
                  time_s=d.time_s, allocs=d.allocs, bytes=d.bytes,
                  ntiles=length(x.chunks),
                  cpu=cpu_profile(fd; reps=max(PROFILE_REPS, 8)),
                  logs=logged_run(fd),
                  notes="3 SpMV-class applies (P A P); correctness-adjacent")
end

function profile_dense_lu()
    n, b = S.lu_n, S.lu_b
    A = rand(Blocks(b, b), Float64, n, n); wait(A)
    bv = rand(Blocks(b), Float64, n); wait(bv)
    fd = () -> begin
        F = lu(A, RowMaximum())
        wait_factor(F)
        x = F \ bv
        wait_d(x)
    end
    d = gc_delta(fd; warmup=2, samples=2)
    Ah = rand(n, n); bh = rand(n)
    hs = with_blas(BLAS_BASELINE) do
        gc_delta(() -> (lu(Ah, RowMaximum()) \ bh); warmup=2, samples=2).time_s
    end
    push_profile!(key="dense_lu", feature="Dense LU + \\",
                  problem="n=$(n), tile=$(b)×$(b)",
                  time_s=d.time_s, host_s=hs, allocs=d.allocs, bytes=d.bytes,
                  ntiles=length(A.chunks),
                  cpu=cpu_profile(fd; reps=1),
                  logs=logged_run(fd),
                  notes="tiled getrf vs host OpenBLAS=$(BLAS_BASELINE)")
end

function profile_main()
    if is_root()
        println("linalg_profile  mode=", MODE, " scale=", SCALE,
                " kind=", PROFILE_KIND, " threads=", NTHREADS)
        println("julia ", VERSION, "  commit ", META["commit"],
                "  instance ", META["instance"])
        println("warmup=", PROFILE_WARMUP, " samples=", PROFILE_SAMPLES,
                " reps=", PROFILE_REPS, " itmax=", PROFILE_ITMAX,
                " out=", OUTDIR)
        flush(stdout)
    end
    BLAS.set_num_threads(BLAS_DAGGER)

    @safe_profile "blas1" profile_blas1()
    @safe_profile "sparse_spmv" profile_spmv()
    @safe_profile "dense_gemm" profile_gemm()
    @safe_profile "krylov_decomp" profile_krylov_decomp()
    @safe_profile "pc_apply" profile_pc_apply()
    @safe_profile "assembly" profile_assembly()
    @safe_profile "sparse_chol" profile_sparse_direct()
    @safe_profile "projected" profile_projected()
    @safe_profile "dense_lu" profile_dense_lu()

    if is_root()
        println()
        println(profile_markdown(PROFILE_ROWS))
        println("Wrote ", joinpath(OUTDIR, "linalg_profile_$(MODE == "mpi" ? "mpi" : "mt").json"))
    end
    return PROFILE_ROWS
end

if abspath(PROGRAM_FILE) == @__FILE__
    profile_main()
end
