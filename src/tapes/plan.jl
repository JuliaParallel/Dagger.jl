# ===========================================================================
# Candidate generation and layout planning.
#
# Given a predicted operation chain and a set of candidate layouts, choosing
# the layout sequence that minimises total cost (including repartitioning on
# the edges) is exactly the classical automatic-data-layout problem of Kennedy
# & Kremer (TOPLAS 1998). They needed 0-1 integer programming because their
# phase graph was general. Ours is a *linear chain*, so it is a shortest path
# through a layered DAG and an exact O(n * |L|^2) dynamic program suffices.
# ===========================================================================

const ASSIGNMENTS_ALL = (:arbitrary, :blockrow, :blockcol, :cyclicrow, :cycliccol)

"""
    fallback_layout(::Type{T}, dims) -> LayoutChoice

The blind default: whatever `Dagger.auto_blocks` would pick, with `:arbitrary`
assignment (which spreads by compute capacity). Not good for much in
particular, but acceptable for most things, which is exactly what a fallback
should be. Every speculative plan is measured against this.
"""
function fallback_layout(::Type{T}, dims::Tuple) where {T}
    ab = try
        auto_blocks(map(Int, dims)::Dims)
    catch
        Blocks(ntuple(i -> max(1, Int(dims[i])), length(dims)))
    end
    return LayoutChoice(ab.blocksize, :arbitrary, :auto)
end

"""
    candidate_layouts(::Type{T}, dims, m) -> Vector{LayoutChoice}

Enumerate plausible layouts for an array of element type `T` and shape `dims`.

The set is deliberately small and structured rather than a dense sweep: the
planner is quadratic in candidate count, and the interesting decisions are
between *families* (square tiles vs row blocks vs column blocks) and between a
few block sizes within a family, not between 511 and 512.

Block sizes are drawn from powers of two bounded below by "big enough that a
task amortises `task_overhead`" and above by "small enough that there are at
least a few tiles per processor".

TODO(rank>2): the N-d cases here are a reasonable guess (split leading dim,
split trailing dim, near-cubic) but untested against real workloads. Tensor
contractions in particular want layouts derived from the contraction indices,
which the tape does not currently record.
"""
function candidate_layouts(::Type{T}, dims::Tuple, m::MachineModel = current_machine())
    N = length(dims)
    N == 0 && return LayoutChoice[]
    esz = max(1, _elsize(T))
    np = max(1, m.nprocs)
    out = LayoutChoice[]

    # Smallest tile worth scheduling, and largest that leaves >= 2 tiles/proc.
    min_tile_bytes = m.task_overhead * m.bandwidth / np
    min_edge = max(32, ceil(Int, (min_tile_bytes / esz)^(1 / N)))
    total_bytes = Float64(prod(dims; init = 1)) * esz
    max_tile_bytes = max(min_tile_bytes, total_bytes / (2 * np))
    max_edge = max(min_edge, floor(Int, (max_tile_bytes / esz)^(1 / N)))

    function push_unique!(l::LayoutChoice)
        l in out || push!(out, l)
        return nothing
    end

    # --- Family 1: near-cubic tiles at a few sizes -------------------------
    edges = Int[]
    e = 64
    while e <= 8192
        (e >= min_edge ÷ 2 && e <= max_edge * 2) && push!(edges, e)
        e *= 2
    end
    # Also the edge that yields exactly one tile per processor per dimension.
    push!(edges, max(1, cld(maximum(dims), max(1, round(Int, np^(1 / N))))))
    for edge in unique!(sort!(edges))
        bs = ntuple(i -> clamp(edge, 1, max(1, Int(dims[i]))), N)
        for a in (N >= 2 ? (:cyclicrow, :cycliccol, :arbitrary) : (:arbitrary,))
            push_unique!(LayoutChoice(bs, a, :square))
        end
    end

    # --- Family 2: block rows (whole trailing dims resident) ---------------
    for div in (np, 2np, 4np)
        bs = ntuple(i -> i == 1 ? max(1, cld(Int(dims[1]), div)) : Int(dims[i]), N)
        push_unique!(LayoutChoice(bs, :blockrow, :rowblock))
        push_unique!(LayoutChoice(bs, :arbitrary, :rowblock))
    end

    # --- Family 3: block columns (whole leading dims resident) -------------
    if N >= 2
        for div in (np, 2np, 4np)
            bs = ntuple(i -> i == N ? max(1, cld(Int(dims[N]), div)) : Int(dims[i]), N)
            push_unique!(LayoutChoice(bs, :blockcol, :colblock))
            push_unique!(LayoutChoice(bs, :arbitrary, :colblock))
        end
    end

    # --- Family 4: whatever the blind default would have been --------------
    fb = fallback_layout(T, dims)
    push_unique!(fb)

    # Trim to budget, spreading across families. The fallback is seeded first
    # so it always survives: the planner needs it present as the baseline.
    if length(out) > CONFIG.max_candidates
        keep = LayoutChoice[fb]
        per_family = max(1, cld(CONFIG.max_candidates - 1, 3))
        for fam in (:square, :rowblock, :colblock)
            fams = filter(l -> l.label === fam, out)
            isempty(fams) && continue
            stride = max(1, cld(length(fams), per_family))
            for l in fams[1:stride:end]
                l in keep || push!(keep, l)
            end
        end
        out = length(keep) > CONFIG.max_candidates ? keep[1:CONFIG.max_candidates] : keep
    end
    return out
end

# ---------------------------------------------------------------------------
# Turning a predicted operation into cost-model input
# ---------------------------------------------------------------------------

"""
    views_for(pred::PredictedOp, self::ArgSpec, layout) -> Vector{ArgView}

Build the [`ArgView`](@ref) list a cost model expects for one predicted
operation: the tracked array at position `pred.key.pos` takes the candidate
`layout`; the other arguments take whatever layout they were last observed
with.

TODO(co-argument layouts): using the *last observed* layout for co-arguments is
the weakest link in the whole chain. It means the planner evaluates "what if I
change only this array" rather than solving for a jointly consistent
assignment, so it systematically undervalues layouts that are only good when
all operands agree — precisely the layouts that matter for GEMM and TRSM. Fix
this together with the joint-planning TODO in `record_op!`: once arrays are
grouped into connected components, `views_for` should take a component-wide
assignment rather than a single layout.

TODO(distributional specs): co-argument specs are the last observation, not a
distribution. If the same site alternates between a 1-column and 512-column
right-hand side, we silently plan for whichever ran last.
"""
function views_for(pred::PredictedOp, self::ArgSpec, layout::LayoutChoice)
    pos = Int(pred.key.pos)
    arity = Int(pred.key.arity)
    specs = pred.argspecs
    n = max(arity, length(specs), pos)
    views = Vector{ArgView}(undef, n)
    @inbounds for i in 1:n
        spec = i <= length(specs) ? specs[i] : self
        if i == pos
            views[i] = ArgView(self, layout)
        else
            lay = LayoutChoice(unpad_dims(spec.blocksize, spec.ndims),
                               spec.assignment, :observed)
            views[i] = ArgView(spec, lay)
        end
    end
    return views
end

"""
    step_cost(pred, self, layout, m) -> Float64

Expected cost contribution of one predicted operation under `layout`.

Weighted two ways:
- with probability `pred.prob` the predicted operation happens, and costs what
  its model says;
- with the residual probability `1 - pred.prob` *something else* happens, which
  we charge at the generic rate. That residual term is what makes the objective
  risk-aware: a layout that is superb for the predicted chain but absurd in
  general (one giant tile, or a million tiny ones) pays for that here.
"""
function step_cost(pred::PredictedOp, self::ArgSpec, layout::LayoutChoice, m::MachineModel)
    views = views_for(pred, self, layout)
    c_hit = cost_of(pred.key.op, views, m)
    p = clamp(pred.prob, 0.0, 1.0)
    p >= 1.0 && return c_hit
    c_miss = generic_op_cost(ArgView[ArgView(self, layout)], m)
    return p * c_hit + (1 - p) * c_miss
end

# ---------------------------------------------------------------------------
# The planner
# ---------------------------------------------------------------------------

"""
    LayoutPlan

Result of planning. `steps[i]` is the layout the planner wants for predicted
operation `i`; `steps[1]` is the only one actually committed under the
receding-horizon policy (see [`plan_chain`](@ref)).
"""
struct LayoutPlan
    steps::Vector{LayoutChoice}
    cost::Float64
    fallback_cost::Float64
    "Worst per-operation ratio of chosen layout to best-possible layout."
    max_regret::Float64
    accepted::Bool
    reason::Symbol
end

"""
    plan_chain(pred, cands, self, m; start=nothing) -> LayoutPlan

Exact dynamic program over the predicted chain.

`D[i, l]` is the minimum expected cost of executing predicted operations
`1..i` with operation `i` running under candidate `l`, including any
repartitioning charged on the edges. Backtracking the argmin over the final
layer gives the optimal layout sequence.

If `start` is given it is the array's *current* layout, and the first step is
charged the cost of moving from it (used when re-planning mid-chain).

# Policy note

The plan covers the whole horizon, but callers commit only `steps[1]` and
re-plan at each subsequent observed operation. That is deliberate: a
speculative repartition is O(data) network traffic, so plan-and-commit exposes
the whole chain's worth of data movement to a single misprediction, whereas
receding-horizon control keeps the multi-operation lookahead benefit while
bounding exposure to one decision at a time.
"""
function plan_chain(pred::Vector{PredictedOp}, cands::Vector{LayoutChoice},
                    self::ArgSpec, m::MachineModel;
                    start::Union{Nothing,LayoutChoice} = nothing)
    n = length(pred)
    L = length(cands)
    fb = fallback_layout(self.eltype, size(self))

    (n == 0 || L == 0) &&
        return LayoutPlan([fb], Inf, Inf, 1.0, false, :no_prediction)

    D = fill(Inf, n, L)
    P = zeros(Int, n, L)

    # Per-operation cost of every candidate, reused for the regret bound.
    C = Matrix{Float64}(undef, n, L)
    @inbounds for i in 1:n, l in 1:L
        C[i, l] = step_cost(pred[i], self, cands[l], m)
    end

    @inbounds for l in 1:L
        entry = start === nothing ? 0.0 : redistribution_cost(start, cands[l], self, m)
        D[1, l] = C[1, l] + entry
    end
    @inbounds for i in 2:n
        for l in 1:L
            best = Inf; bi = 0
            for lp in 1:L
                c = D[i-1, lp] + redistribution_cost(cands[lp], cands[l], self, m)
                if c < best
                    best = c; bi = lp
                end
            end
            D[i, l] = best + C[i, l]
            P[i, l] = bi
        end
    end

    # Backtrack.
    bestl = 1; bestc = Inf
    @inbounds for l in 1:L
        D[n, l] < bestc && (bestc = D[n, l]; bestl = l)
    end
    steps = Vector{LayoutChoice}(undef, n)
    l = bestl
    for i in n:-1:1
        steps[i] = cands[l]
        i > 1 && (l = P[i, l])
    end

    # Same DP, pinned to the fallback layout throughout: the baseline to beat.
    fb_idx = findfirst(==(fb), cands)
    fb_cost = 0.0
    if fb_idx === nothing
        fbviews_cost = 0.0
        for i in 1:n
            fbviews_cost += step_cost(pred[i], self, fb, m)
        end
        fb_cost = fbviews_cost + (start === nothing ? 0.0 :
                                  redistribution_cost(start, fb, self, m))
    else
        for i in 1:n
            fb_cost += C[i, fb_idx]
        end
        fb_cost += start === nothing ? 0.0 : redistribution_cost(start, cands[fb_idx], self, m)
    end

    # Regret bound: how much worse is the committed layout than the best
    # possible layout, for the single operation where it does worst?
    committed = steps[1]
    ci = findfirst(==(committed), cands)
    max_regret = 1.0
    if ci !== nothing
        @inbounds for i in 1:n
            best_i = minimum(@view C[i, :])
            best_i > 0 && isfinite(best_i) &&
                (max_regret = max(max_regret, C[i, ci] / best_i))
        end
    end

    conf = confidence(pred)
    accepted = true
    reason = :ok
    if !isfinite(bestc)
        accepted = false; reason = :nonfinite_cost
    elseif bestc > CONFIG.gate_margin * fb_cost
        # Not enough predicted win to justify departing from the known-adequate default.
        accepted = false; reason = :insufficient_margin
    elseif conf < CONFIG.regret_confidence_threshold && max_regret > CONFIG.max_regret_ratio
        # Low confidence: refuse layouts that are catastrophic for any single
        # predicted operation, even if they win on the weighted sum.
        accepted = false; reason = :regret_too_high
    end

    return LayoutPlan(steps, bestc, fb_cost, max_regret, accepted, reason)
end

# ---------------------------------------------------------------------------
# Receding-horizon replanning
# ---------------------------------------------------------------------------

"""
    maybe_repartition!(A::DArray, trace::LiveTrace)

Hook called after each recorded operation when `CONFIG.allow_repartition` is
set: re-plan from the array's current position in the trie and, if the plan
calls for a different layout and the predicted win exceeds the redistribution
cost, physically repartition `A`.

**Currently a no-op stub.** The planning half is real (`replan` below); the
acting half is not, and deliberately so — see the TODO.

TODO(repartition-mechanism): performing this safely requires a real
redistribution primitive that (a) is expressed as Datadeps tasks so it
interleaves with in-flight work rather than serialising behind it, (b) mutates
the `DArray` in place — `domain`, `subdomains`, `chunks` and `partitioning` all
have to change atomically from the perspective of concurrent readers — and
(c) is safe against the array being captured by tasks already submitted but not
yet scheduled. (c) is the hard part: Dagger's eager submission means there may
be queued thunks holding `Chunk` references into the old block structure. The
plausible approach is to route repartitioning through the Datadeps queue as a
normal `InOut` task on the whole array, which makes the dependency explicit and
lets the existing scheduler serialise it correctly, at the cost of a
synchronisation point.

Until that exists, the planner's multi-step output is informational: it shows
up in `explain` and can guide manual `pin!` decisions, but only `steps[1]` is
ever acted on.
"""
function maybe_repartition!(A::DArray, trace::LiveTrace)
    CONFIG.allow_repartition || return nothing
    plan = replan(trace)
    plan === nothing && return nothing
    isempty(plan.steps) && return nothing
    want = plan.steps[1]
    want == trace.layout && return nothing
    plan.accepted || return nothing
    vlog("would repartition from $(trace.layout) to $want (not implemented)")
    return nothing
end

"""
    replan(trace::LiveTrace) -> Union{Nothing,LayoutPlan}

Re-run the planner from the array's current position in the trie, taking its
present layout as the starting state. This is the receding-horizon step: the
prediction is now conditioned on the prefix actually observed, so a chain that
diverged from the forecast is re-forecast against the branch it really took.
"""
function replan(trace::LiveTrace)
    pred = predict(trace.node)
    isempty(pred) && return nothing
    self = trace.self
    m = current_machine()
    cands = candidate_layouts(self.eltype, size(self), m)
    return plan_chain(pred, cands, self, m; start = trace.layout)
end
