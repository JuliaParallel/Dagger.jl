# model.jl
#
# Runtime algorithm selection. Given an operation and real inputs:
#
#   1. Profile the inputs (spec.jl) into a feature dict.
#   2. For every registered algorithm: check availability (owning package
#      loaded, raw impl hooked up), feature support, and converter existence
#      for each input that isn't already in the algorithm's container form.
#   3. Match benchmark database entries on hard-categorical features
#      (eltype, container form, structure, spd, locality), then group the
#      remaining entries by their configuration (nthreads, density, accuracy,
#      free knobs like block size). Each group's timings are interpolated
#      piecewise-linearly in log(work)-log(time) space to the current problem
#      scale; extrapolation uses the nearest segment's slope, clamped.
#   4. Add measured data-movement costs (from :transfer benchmarks, with
#      bandwidth fallbacks), MUTABLE-configurable transition costs, and
#      mismatch penalties (e.g. benchmark ran at a different Julia thread
#      count than the current FIXED value).
#   5. Groups containing failures at or below (~) the current scale veto the
#      candidate - "this algorithm is known to explode here".
#   6. Pick the cheapest available candidate; if the cheapest *overall*
#      candidate is meaningfully faster but its package isn't loaded, emit a
#      one-time suggestion to load it.
#   7. Execute: convert inputs, apply/restore MUTABLE configurables, invoke,
#      optionally convert an array result back to the caller's form.

# ---------------------------------------------------------------------------
# Interpolation

const MIN_TIME = 1.0e-9
const SLOPE_CLAMP = (0.25, 4.0)

"""
    predict_time(points, scale) -> Float64

Piecewise log-log interpolation of `(scale, seconds)` points at `scale`.
Single-point groups extrapolate linearly in scale (slope 1.0 in log-log),
which is principled because operation `scale` functions are work estimates.
"""
function predict_time(points::Vector{Tuple{Float64,Float64}}, scale::Float64)
    isempty(points) && return Inf
    pts = sort(points; by=first)
    n = length(pts)
    lx = log(max(scale, 1.0))
    xs = [log(max(p[1], 1.0)) for p in pts]
    ys = [log(max(p[2], MIN_TIME)) for p in pts]
    if n == 1
        return exp(ys[1] + 1.0 * (lx - xs[1]))
    end
    _slope(i) = begin
        dx = xs[i+1] - xs[i]
        s = dx <= 0 ? 1.0 : (ys[i+1] - ys[i]) / dx
        clamp(s, SLOPE_CLAMP[1], SLOPE_CLAMP[2])
    end
    if lx <= xs[1]
        return exp(ys[1] + _slope(1) * (lx - xs[1]))
    elseif lx >= xs[n]
        return exp(ys[n] + _slope(n - 1) * (lx - xs[n]))
    end
    i = searchsortedlast(xs, lx)
    i = clamp(i, 1, n - 1)
    dx = xs[i+1] - xs[i]
    t = dx <= 0 ? 0.0 : (lx - xs[i]) / dx
    return exp(ys[i] + t * (ys[i+1] - ys[i]))
end

# ---------------------------------------------------------------------------
# Movement / transition costs

const CHANNEL_LATENCY = 5.0e-5  # per-conversion fixed overhead floor, seconds

"""
    movement_cost(db, channel, bytes) -> Float64

Predicted seconds to move `bytes` over `channel`, using :transfer benchmark
results when present, else fallback bandwidths.
"""
function movement_cost(db::ResultDB, channel::Symbol, bytes::Float64)
    bytes <= 0 && return 0.0
    pts = Tuple{Float64,Float64}[]
    for r in results_for(db, :transfer, channel)
        trial_ok(r) || continue
        b = Float64(get(r.features, "bytes", 0))
        b > 0 && push!(pts, (b, r.time_s))
    end
    if !isempty(pts)
        return max(predict_time(pts, bytes), CHANNEL_LATENCY)
    end
    bw = get(FALLBACK_BANDWIDTH, channel, 5.0e9)
    return bytes / bw + CHANNEL_LATENCY
end

# Penalty for using benchmark data gathered at a different value of a FIXED
# knob (Julia thread count): heuristic +50% predicted time per octave of
# mismatch. Exact matches always win when they exist.
_nthreads_penalty(bench::Int, current::Int) =
    1.0 + 0.5 * abs(log2(max(bench, 1) / max(current, 1)))

# ---------------------------------------------------------------------------
# Candidate matching

# Features that must match exactly between runtime and benchmark entries.
const HARD_MATCH_KEYS = ("eltype", "structure", "spd", "locality")

function _hard_match(rf::Dict{String,Any}, bf::Dict{String,Any}, target_form::Symbol)
    get(bf, "array_type", "") == String(target_form) || return false
    for k in HARD_MATCH_KEYS
        haskey(rf, k) && haskey(bf, k) || continue
        rf[k] == bf[k] || return false
    end
    return true
end

# Group key: the benchmark entry's config plus continuous-ish features that
# distinguish setups (density, accuracy).
function _group_key(r::TrialResult)
    parts = Tuple{String,String}[]
    for (k, v) in r.config
        push!(parts, (k, repr(v)))
    end
    for k in ("density", "accuracy")
        haskey(r.features, k) && push!(parts, ("feat_" * k, repr(r.features[k])))
    end
    return Tuple(sort!(parts; by=first))
end

struct MatchedGroup
    config::Dict{String,Any}
    density::Union{Nothing,Float64}
    accuracy::Union{Nothing,Float64}
    ok_points::Vector{Tuple{Float64,Float64}}   # (scale, seconds)
    veto_scales::Vector{Float64}
end

function matched_groups(db::ResultDB, opspec::OperationSpec, alg::AlgorithmSpec,
                        rf::Dict{String,Any})
    groups = Dict{Any,MatchedGroup}()
    for r in results_for(db, opspec.name, alg.name)
        _hard_match(rf, r.features, alg.input_form) || continue
        key = _group_key(r)
        g = get!(groups, key) do
            MatchedGroup(copy(r.config),
                         haskey(r.features, "density") ? Float64(r.features["density"]) : nothing,
                         haskey(r.features, "accuracy") ? Float64(r.features["accuracy"]) : nothing,
                         Tuple{Float64,Float64}[], Float64[])
        end
        s = try
            opspec.scale(r.features)
        catch
            continue
        end
        if trial_ok(r)
            push!(g.ok_points, (Float64(s), r.time_s))
        elseif trial_veto(r)
            push!(g.veto_scales, Float64(s))
        end
    end
    return collect(values(groups))
end

# A failure at scale s vetoes the group for problem scales >= 0.9*s.
group_vetoed(g::MatchedGroup, scale::Float64) =
    any(v -> scale >= 0.9 * v, g.veto_scales)

# Accuracy admissibility: a group benchmarked at tolerance <= requested (i.e.
# tighter or equal) is a safe surrogate. Groups are otherwise inadmissible.
function _accuracy_admissible(g::MatchedGroup, requested::Union{Nothing,Float64})
    requested === nothing && return true
    g.accuracy === nothing && return true
    return g.accuracy <= requested * 1.0001
end

# Density proximity multiplier: sparse timings are matched to the nearest
# benchmarked density on a log scale, with a mild penalty per decade off.
function _density_penalty(g::MatchedGroup, rf::Dict{String,Any})
    haskey(rf, "density") || return 1.0
    g.density === nothing && return 1.0
    d0 = max(Float64(rf["density"]), 1.0e-12)
    d1 = max(g.density, 1.0e-12)
    return 1.0 + 0.25 * abs(log10(d0 / d1))
end

# ---------------------------------------------------------------------------
# Plans

struct ConversionStep
    index::Int              # positional input index
    conv::Converter
    bytes::Float64
end

"""
    Plan

A fully-resolved execution decision: which algorithm, with which per-input
conversions, which FREE/MUTABLE configuration values, and the predicted cost
breakdown (`"algorithm"`, `"movement"`, `"transition"`, `"total"` seconds).
"""
struct Plan
    op::Symbol
    alg::AlgorithmSpec
    features::Dict{String,Any}
    config::Dict{String,Any}
    conversions::Vector{ConversionStep}
    convert_back::Union{Nothing,Converter}
    config_changes::Vector{Tuple{Symbol,Any}}
    predicted::Dict{String,Float64}
    available::Bool
    from_benchmark::Bool     # false for fallback plans (no data)
end

# Which container form does positional input `x` need for algorithm `alg`?
# Matrix arguments take the algorithm's declared form; vector companions
# (e.g. `b` in solve) take the associated dense form on the same device.
const COMPANION_FORM = Dict{Symbol,Symbol}(
    :SparseMatrixCSC   => :Array,
    :CuSparseMatrixCSR => :CuArray,
)
function target_form_for(alg::AlgorithmSpec, x)
    x isa AbstractMatrix && return alg.input_form
    return get(COMPANION_FORM, alg.input_form, alg.input_form)
end

function _plan_conversions(alg::AlgorithmSpec, inputs::Tuple)
    steps = ConversionStep[]
    for (i, x) in enumerate(inputs)
        x isa AbstractArray || continue
        cur = array_form(x)
        tgt = target_form_for(alg, x)
        cur === tgt && continue
        c = converter(cur, tgt)
        (c === nothing || !converter_available(c)) && return nothing
        push!(steps, ConversionStep(i, c, c.bytes(x)))
    end
    return steps
end

# ---------------------------------------------------------------------------
# Selection

# Rewrite runtime features as they would look after converting inputs to the
# algorithm's container form. Structure changes (dense<->sparse) are not a
# conversion we perform, so those candidates are rejected here. Converted
# data is local by construction.
function _as_converted(rf::Dict{String,Any}, alg::AlgorithmSpec)
    cur = Symbol(get(rf, "array_type", "Array"))
    tgt = alg.input_form
    structure_of(tgt) == get(rf, "structure", "dense") || return nothing
    af = copy(rf)
    if cur !== tgt
        af["array_type"] = String(tgt)
        haskey(af, "locality") && (af["locality"] = "local")
    end
    return af
end

const _WARN_LOCK = ReentrantLock()
const _WARNED = Set{Any}()
function _warn_once(key, msg)
    lock(_WARN_LOCK) do
        key in _WARNED && return
        push!(_WARNED, key)
        @warn msg
    end
    return nothing
end

"""
    select_plan(op, inputs...; accuracy=nothing, db=current_db())
        -> Union{Plan,Nothing}

Choose the best available algorithm for `op` on `inputs`, or `nothing` if no
registered algorithm can handle them at all. Emits one-time warnings when
(a) no benchmark data exists, or (b) a faster algorithm exists but its
package isn't loaded.
"""
function select_plan(op::Symbol, inputs...; accuracy=nothing, db::ResultDB=current_db())
    opspec = operation(op)
    rf = opspec.extract_features(inputs...; accuracy=accuracy)
    scale = Float64(opspec.scale(rf))
    cur_nthreads = Threads.nthreads()

    best_avail = nothing
    best_avail_cost = Inf
    best_any_cost = Inf
    best_any_name = nothing
    best_any_pkg = nothing

    for alg in algorithms_for(op)
        # Evaluate candidacy on the features *as-if converted* to the
        # algorithm's container form (that's what its benchmark entries and
        # supports predicate are written against).
        af = _as_converted(rf, alg)
        af === nothing && continue
        alg.supports(af) || continue
        steps = _plan_conversions(alg, inputs)
        steps === nothing && continue
        avail = algorithm_available(alg) &&
                all(s -> converter_available(s.conv), steps)

        move = sum(s -> movement_cost(db, s.conv.channel, s.bytes), steps; init=0.0)
        back = nothing
        back_cost = 0.0
        if opspec.array_output
            prim = findfirst(x -> x isa AbstractArray, inputs)
            if prim !== nothing
                orig = array_form(inputs[prim])
                produced = opspec.output_ndims >= 2 ? alg.input_form :
                           get(COMPANION_FORM, alg.input_form, alg.input_form)
                if orig !== produced
                    c = converter(produced, orig)
                    if c !== nothing && converter_available(c)
                        back = c
                        back_cost = movement_cost(db, c.channel,
                                                  Float64(opspec.output_bytes(rf)))
                    end
                    # If no back-converter exists we return the native result
                    # (documented behavior), so no candidate is excluded here.
                end
            end
        end

        for g in matched_groups(db, opspec, alg, af)
            isempty(g.ok_points) && continue
            group_vetoed(g, scale) && continue
            _accuracy_admissible(g, accuracy === nothing ? nothing : Float64(accuracy)) || continue

            t_alg = predict_time(g.ok_points, scale)
            bench_nt = Int(get(g.config, "nthreads", cur_nthreads))
            t_alg *= _nthreads_penalty(bench_nt, cur_nthreads)
            t_alg *= _density_penalty(g, rf)

            changes = Tuple{Symbol,Any}[]
            trans = 0.0
            skip = false
            for (k, v) in g.config
                name = Symbol(k)
                c = configurable(name)
                c === nothing && continue
                if c.mutability === MUTABLE
                    cur = c.getter()
                    if cur != v
                        tc = transition_cost(name, cur, v)
                        isinf(tc) && (skip = true; break)
                        trans += 2 * tc
                        push!(changes, (name, v))
                    end
                end
                # FIXED knobs were handled by penalty above; FREE knobs flow
                # into the invoke config untouched.
            end
            skip && continue

            total = t_alg + move + back_cost + trans
            if total < best_any_cost
                best_any_cost = total
                best_any_name = alg.name
                best_any_pkg = alg.package
            end
            if avail && total < best_avail_cost
                best_avail_cost = total
                best_avail = Plan(op, alg, rf, copy(g.config), steps, back, changes,
                                  Dict("algorithm" => t_alg, "movement" => move + back_cost,
                                       "transition" => trans, "total" => total),
                                  true, true)
            end
        end
    end

    if best_avail === nothing
        # No benchmark data matched any available algorithm: fall back to the
        # zero-conversion path the user would have gotten anyway.
        isempty(db) && _warn_once((:nodb, op),
            "Autotune: no benchmark database found at $(_DB_PATH[] == "" ? default_db_path() : _DB_PATH[]); " *
            "falling back to default algorithms. Run Dagger.benchmark() to enable tuned selection.")
        best_avail = fallback_plan(op, opspec, rf, inputs)
    elseif best_any_name !== nothing && best_any_name !== best_avail.alg.name &&
           best_any_cost < 0.9 * best_avail_cost && best_any_pkg !== nothing &&
           !package_available(best_any_pkg)
        _warn_once((:pkg, op, best_any_name),
            "Autotune: $(best_any_name) (from $(best_any_pkg).jl) is predicted " *
            "$(round(best_avail_cost / best_any_cost; digits=2))x faster than the selected " *
            "$(best_avail.alg.name) for this :$op call, but $(best_any_pkg).jl is not loaded. " *
            "Load it (`using $(best_any_pkg)`) to enable it.")
    end
    return best_avail
end

"""
    fallback_plan(op, opspec, rf, inputs) -> Union{Plan,Nothing}

The plan used when no benchmark data applies: prefer an available algorithm
that needs no conversions, then any available algorithm reachable through
converters. Returns `nothing` if nothing at all can run.
"""
function fallback_plan(op::Symbol, opspec::OperationSpec, rf::Dict{String,Any}, inputs)
    zero_conv = nothing
    with_conv = nothing
    for alg in algorithms_for(op)
        af = _as_converted(rf, alg)
        af === nothing && continue
        (alg.supports(af) && algorithm_available(alg)) || continue
        steps = _plan_conversions(alg, inputs)
        steps === nothing && continue
        all(s -> converter_available(s.conv), steps) || continue
        plan = Plan(op, alg, rf, Dict{String,Any}(), steps, nothing,
                    Tuple{Symbol,Any}[], Dict("total" => NaN), true, false)
        if isempty(steps)
            zero_conv === nothing && (zero_conv = plan)
        else
            with_conv === nothing && (with_conv = plan)
        end
    end
    return zero_conv !== nothing ? zero_conv : with_conv
end

# ---------------------------------------------------------------------------
# Execution

"""
    execute_plan(plan::Plan, inputs...; kwargs...)

Apply the plan: convert inputs, set (and afterwards restore) MUTABLE
configurables, invoke the algorithm, and convert an array result back to the
caller's container form when possible. Extra `kwargs` are forwarded to the
algorithm's invoke function.
"""
function execute_plan(plan::Plan, inputs...; kwargs...)
    converted = Any[inputs...]
    for step in plan.conversions
        converted[step.index] = step.conv.convert(converted[step.index], plan.config)
    end
    ctx = InvokeContext(algorithm_module(plan.alg), plan.features, plan.config,
                        haskey(plan.features, "accuracy") ?
                            Float64(plan.features["accuracy"]) : nothing)
    result = with_configurables(plan.config_changes) do
        if isempty(kwargs)
            plan.alg.invoke(ctx, converted...)
        else
            plan.alg.invoke(ctx, converted...; kwargs...)
        end
    end
    if plan.convert_back !== nothing && result isa AbstractArray &&
       array_form(result) === plan.convert_back.from
        result = plan.convert_back.convert(result, plan.config)
    end
    # In-place operations (`OperationSpec.mutated_args`) must honor their `!`
    # contract regardless of which container form ended up fastest: if the
    # winning algorithm needed a converted copy of a mutated argument, copy
    # the (now-mutated) data back into the caller's original array. No-op
    # when that argument needed no conversion (already mutated in place).
    opspec = operation(plan.op)
    for idx in opspec.mutated_args
        idx > length(inputs) && continue
        orig = inputs[idx]
        conv = converted[idx]
        conv === orig && continue
        if view_parent(orig) === conv
            # `conv` is `orig`'s own dense backing store, handed over by the
            # zero-copy `DArray`-view unwrap: the in-place algorithm already
            # mutated it, so copying back would be a redundant (and aliasing)
            # self-copy. Just remap the returned reference.
            result === conv && (result = orig)
            continue
        end
        orig isa AbstractArray && conv isa AbstractArray && copyto!(orig, conv)
        # Base's `!` convention returns the same container that was mutated
        # (e.g. `mul!(C,A,B)` returns `C`, `ldiv!(A,b)` returns `b`): when the
        # algorithm's result *is* the converted array by reference, hand back
        # the caller's original instead. Factorization objects (`LU`,
        # `Cholesky`, ...) wrap rather than equal the converted array, so
        # this is a no-op for them; they're returned wrapping the converted
        # form (documented above).
        result === conv && (result = orig)
    end
    return result
end

"""
    invoke_best(op, inputs...; accuracy=nothing, kwargs...)

The single runtime entrypoint: profile, select, and execute the best
algorithm for `op` on `inputs`. This is what Dagger's public entrypoints
call when `Autotune.enabled()`.
"""
function invoke_best(op::Symbol, inputs...; accuracy=nothing, kwargs...)
    plan = select_plan(op, inputs...; accuracy=accuracy)
    plan === nothing &&
        error("Autotune: no registered algorithm can execute :$op for inputs of form " *
              "$(map(array_form, inputs)); register one with Autotune.register_algorithm!")
    return execute_plan(plan, inputs...; kwargs...)
end

# ---------------------------------------------------------------------------
# Introspection

"""
    explain(op, inputs...; accuracy=nothing, io=stdout)

Print the selected plan and its predicted cost breakdown - handy for
understanding (and debugging) why Autotune picked what it picked.
"""
function explain(op::Symbol, inputs...; accuracy=nothing, io::IO=stdout)
    plan = select_plan(op, inputs...; accuracy=accuracy)
    if plan === nothing
        println(io, "No executable algorithm for :$op with these inputs.")
        return nothing
    end
    println(io, "Operation :$op -> algorithm :$(plan.alg.name)" *
                (plan.from_benchmark ? "" : "  (fallback: no benchmark data)"))
    isempty(plan.config) || println(io, "  config: ", plan.config)
    for s in plan.conversions
        @printf(io, "  convert input %d: %s -> %s via %s (~%.3g MB)\n",
                s.index, s.conv.from, s.conv.to, s.conv.channel, s.bytes / 1e6)
    end
    plan.convert_back === nothing ||
        println(io, "  convert result: $(plan.convert_back.from) -> $(plan.convert_back.to)")
    for (name, v) in plan.config_changes
        println(io, "  set $name = $v (restored afterwards)")
    end
    if plan.from_benchmark
        p = plan.predicted
        @printf(io, "  predicted: %.3g s total (alg %.3g + movement %.3g + transitions %.3g)\n",
                p["total"], p["algorithm"], p["movement"], p["transition"])
    end
    return plan
end
