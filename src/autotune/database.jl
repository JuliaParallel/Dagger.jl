# database.jl
#
# On-disk benchmark results. Stored as TOML (stdlib-only, human-inspectable,
# stable across Julia versions - unlike Serialization). One file per machine
# by default; `Autotune.benchmark` merges new results into any existing file,
# keyed on (op, algorithm, features, config), so repeated/incremental
# benchmark runs refine rather than clobber.

const DB_FORMAT_VERSION = 1

"""
    TrialResult

One benchmarked (or failed) configuration.

`status` is one of:
  "ok"                  - timed successfully (`time_s` = min over samples)
  "failed"              - the trial errored (message in `error`)
  "check_failed"        - ran, but the result failed validation (e.g. the
                          requested solve accuracy wasn't reached)
  "timeout"             - the parent killed the worker while running this trial
  "crashed"             - the worker process died on this trial (OOM, segfault)
  "skipped_mem"         - estimated footprint exceeded the memory budget
  "skipped_larger"      - not run because a smaller scale of the same series
                          (same op/algorithm/config, larger size) already
                          crashed/timed out
  "skipped_unavailable" - required package missing/non-functional in the
                          benchmark environment

Only "ok" entries feed predictions. "failed"/"check_failed"/"timeout"/
"crashed" entries actively veto the algorithm near and above that scale
(see model.jl). "skipped_*" entries are neutral.

Timing detail. `time_s` is the minimum wall time over samples (the statistic
the cost model uses). `times_s` retains every per-sample wall time so that
variance/median/mean can be recovered after the fact (empty for pre-Phase-1
databases and for non-"ok" results). `memory`/`allocs` record the allocated
bytes and allocation count of the fastest sample (BenchmarkTools-style
`minimum(trial)` semantics), so a benchmark front-end can reconstruct a full
picture without depending on BenchmarkTools.
"""
struct TrialResult
    op::Symbol
    algorithm::Symbol
    features::Dict{String,Any}
    config::Dict{String,Any}
    status::String
    time_s::Float64          # NaN unless status == "ok"; min over samples
    samples::Int
    times_s::Vector{Float64} # per-sample wall times (empty unless timed)
    memory::Int              # allocated bytes of the fastest sample
    allocs::Int              # allocation count of the fastest sample
    error::String
    timestamp::String
end

function TrialResult(op, algorithm, features, config; status="ok", time_s=NaN,
                     samples=0, times_s=Float64[], memory=0, allocs=0,
                     error="", timestamp=string(Dates.now()))
    times = Float64[Float64(t) for t in times_s]
    isnan(time_s) && !isempty(times) && (time_s = minimum(times))
    samples == 0 && !isempty(times) && (samples = length(times))
    return TrialResult(Symbol(op), Symbol(algorithm),
                       Dict{String,Any}(features), Dict{String,Any}(config),
                       String(status), Float64(time_s), Int(samples),
                       times, Int(memory), Int(allocs),
                       String(error), String(timestamp))
end

trial_ok(r::TrialResult) = r.status == "ok" && isfinite(r.time_s)
trial_veto(r::TrialResult) =
    r.status in ("failed", "check_failed", "timeout", "crashed")

# Identity key for merging: same trial re-run replaces the old entry.
trial_key(r::TrialResult) = (r.op, r.algorithm,
                             _canonical(r.features), _canonical(r.config))
_canonical(d::Dict{String,Any}) =
    Tuple(sort!([(k, repr(v)) for (k, v) in d]; by=first))

struct ResultDB
    meta::Dict{String,Any}
    results::Vector{TrialResult}
end

ResultDB() = ResultDB(Dict{String,Any}("format" => DB_FORMAT_VERSION,
                                       "machine" => machine_fingerprint()),
                      TrialResult[])

Base.isempty(db::ResultDB) = isempty(db.results)
Base.length(db::ResultDB) = length(db.results)

# ---------------------------------------------------------------------------
# Paths

default_autotune_dir() = get(ENV, "DAGGER_AUTOTUNE_DIR") do
    joinpath(first(Base.DEPOT_PATH), "dagger", "autotune")
end
default_db_path() = joinpath(default_autotune_dir(), "autotune_db.toml")

# ---------------------------------------------------------------------------
# TOML (de)serialization

_toml_safe(v::Bool) = v
_toml_safe(v::Symbol) = String(v)
_toml_safe(v::Integer) = Int(v)
_toml_safe(v::AbstractFloat) = Float64(v)
_toml_safe(v::AbstractString) = String(v)
_toml_safe(v::AbstractVector) = Any[_toml_safe(x) for x in v]
_toml_safe(d::AbstractDict) =
    Dict{String,Any}(String(k) => _toml_safe(v) for (k, v) in d if v !== nothing)
_toml_safe(v) = repr(v)

function _result_to_dict(r::TrialResult)
    d = Dict{String,Any}(
        "op"        => String(r.op),
        "algorithm" => String(r.algorithm),
        "features"  => _toml_safe(r.features),
        "config"    => _toml_safe(r.config),
        "status"    => r.status,
        "samples"   => r.samples,
        "timestamp" => r.timestamp,
    )
    isfinite(r.time_s) && (d["time_s"] = r.time_s)
    isempty(r.times_s) || (d["times_s"] = Any[Float64(t) for t in r.times_s])
    r.memory == 0 || (d["memory"] = r.memory)
    r.allocs == 0 || (d["allocs"] = r.allocs)
    isempty(r.error) || (d["error"] = r.error)
    return d
end

function _result_from_dict(d::Dict{String,Any})
    return TrialResult(Symbol(d["op"]), Symbol(d["algorithm"]),
                       Dict{String,Any}(get(d, "features", Dict{String,Any}())),
                       Dict{String,Any}(get(d, "config", Dict{String,Any}()));
                       status = get(d, "status", "ok"),
                       time_s = Float64(get(d, "time_s", NaN)),
                       samples = Int(get(d, "samples", 0)),
                       times_s = Float64[Float64(t) for t in get(d, "times_s", Any[])],
                       memory = Int(get(d, "memory", 0)),
                       allocs = Int(get(d, "allocs", 0)),
                       error = get(d, "error", ""),
                       timestamp = get(d, "timestamp", ""))
end

"""
    save_db(db::ResultDB, path=default_db_path())

Atomically write the database (temp file + rename).
"""
function save_db(db::ResultDB, path::AbstractString=default_db_path())
    mkpath(dirname(path))
    doc = Dict{String,Any}(
        "meta"    => _toml_safe(db.meta),
        "results" => Any[_result_to_dict(r) for r in db.results],
    )
    tmp = path * ".tmp"
    open(tmp, "w") do io
        TOML.print(io, doc)
    end
    mv(tmp, path; force=true)
    return path
end

"""
    load_db(path=default_db_path()) -> ResultDB

Load a database, returning an empty one (with a warning suppressed to once)
if the file doesn't exist. Warns if the machine fingerprint doesn't match
the current machine.
"""
function load_db(path::AbstractString=default_db_path())
    isfile(path) || return ResultDB()
    doc = try
        TOML.parsefile(path)
    catch err
        @warn "Autotune: failed to parse benchmark database at $path; ignoring" exception=err
        return ResultDB()
    end
    meta = Dict{String,Any}(get(doc, "meta", Dict{String,Any}()))
    results = TrialResult[_result_from_dict(Dict{String,Any}(d))
                          for d in get(doc, "results", Any[])]
    db = ResultDB(meta, results)
    _check_fingerprint(db)
    return db
end

function _check_fingerprint(db::ResultDB)
    recorded = get(db.meta, "machine", nothing)
    recorded isa AbstractDict || return
    here = machine_fingerprint()
    for key in ("hostname", "cpu", "cpu_threads")
        rv = get(recorded, key, nothing)
        rv === nothing && continue
        if rv != here[key]
            @warn "Autotune: benchmark database was generated on a different machine " *
                  "($key: $(rv) vs $(here[key])); predictions may be inaccurate. " *
                  "Re-run Dagger.benchmark() on this machine." maxlog=1
            return
        end
    end
end

"""
    merge_results!(db::ResultDB, new::Vector{TrialResult}) -> db

Insert results, replacing existing entries with identical
(op, algorithm, features, config) keys.
"""
function merge_results!(db::ResultDB, new::Vector{TrialResult})
    # `trial_key` bakes the (variable-length) canonicalized feature/config
    # tuples into the key, so keys for different ops/algorithms have different
    # concrete tuple types. Force an abstract key type; a concretely-typed
    # `Dict` would try (and fail) to `convert` a longer/shorter key tuple to
    # the type inferred from the first entry.
    index = Dict{Any,Int}(trial_key(r) => i for (i, r) in enumerate(db.results))
    for r in new
        k = trial_key(r)
        if haskey(index, k)
            db.results[index[k]] = r
        else
            push!(db.results, r)
            index[k] = length(db.results)
        end
    end
    db.meta["updated"] = string(Dates.now())
    return db
end

# ---------------------------------------------------------------------------
# Runtime cache

const _DB_LOCK = ReentrantLock()
const _DB_CACHE = Ref{Union{Nothing,ResultDB}}(nothing)
const _DB_PATH = Ref{String}("")

"""
    current_db() -> ResultDB

The cached runtime database, loading it from `default_db_path()` on first
use. `load_db!(path)` to point at a different file; `reset_db!()` to force a
reload.
"""
function current_db()
    lock(_DB_LOCK) do
        if _DB_CACHE[] === nothing
            path = isempty(_DB_PATH[]) ? default_db_path() : _DB_PATH[]
            _DB_PATH[] = path
            _DB_CACHE[] = load_db(path)
        end
        return _DB_CACHE[]::ResultDB
    end
end

function load_db!(path::AbstractString=default_db_path())
    lock(_DB_LOCK) do
        _DB_PATH[] = String(path)
        _DB_CACHE[] = load_db(path)
        return _DB_CACHE[]::ResultDB
    end
end

reset_db!() = lock(_DB_LOCK) do
    _DB_CACHE[] = nothing
    nothing
end

# ---------------------------------------------------------------------------
# Queries

results_for(db::ResultDB, op::Symbol) = filter(r -> r.op === op, db.results)
results_for(db::ResultDB, op::Symbol, alg::Symbol) =
    filter(r -> r.op === op && r.algorithm === alg, db.results)
