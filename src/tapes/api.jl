# ===========================================================================
# Public API: allocation hooks, instrumentation macros, and introspection.
# ===========================================================================

# ---------------------------------------------------------------------------
# Ahead-of-time declaration
# ---------------------------------------------------------------------------

"""
    Declaration

An active [`@expect_ops`](@ref) region. `ops === nothing` means the region only
establishes a lexical scope (so allocations inside it are keyed cheaply and
consistently); a non-`nothing` list is a user assertion about what will happen,
adopted with `prob = declared_prob`.
"""
struct Declaration
    token::UInt64
    ops::Union{Nothing,Vector{Symbol}}
    declared_prob::Float64
end

const ACTIVE_DECLARATION = ScopedValue{Union{Nothing,Declaration}}(nothing)

"""
    @expect_ops begin ... end
    @expect_ops [:mul!, :cholesky!, :trsm!] begin ... end
    @expect_ops [:cholesky!] prob=0.9 begin ... end

Declare an operation chain ahead of time for allocations made inside the
region.

Two distinct uses, both feeding the same planner:

1. **With an operation list.** You are asserting what will happen to arrays
   allocated in this region. The list is adopted as a forecast with confidence
   `prob` (default `1.0`) and the planner optimises against it *immediately* —
   no warm-up run required, which is the main reason to reach for this.

2. **Without a list.** The region establishes a stable lexical site key and
   mixes into the calling-context hash. Allocations inside are keyed by the
   region rather than by an unwound stack, which makes
   `CONFIG.site_id = :lexical` (free) or `:context` (nearly free) as precise as
   `:backtrace` for the code you care about. Recording and prediction then
   proceed normally from observation.

The declared operations are matched positionally against argument position 1
with arity 1. That is a simplification — it cannot express "this array is the
`B` of a `trsm!`" — but it covers the common case where the declared array is
the primary operand.

TODO(richer-declarations): accept `:trsm! => 2` or a small tuple syntax to
declare argument position and arity, and accept per-array declarations rather
than region-wide ones (currently *every* allocation in the region adopts the
same chain, which is wrong when a region allocates both a matrix and its
right-hand side).

# Example

```julia
Dagger.@expect_ops [:mul!, :cholesky!, :trsm!] begin
    A = rand(AutoBlocks(), Float64, n, n)
    B = A * A' + n*I
    cholesky!(B)
    ldiv!(B, rhs)
end
```
"""
macro expect_ops(args...)
    isempty(args) && error("@expect_ops requires a body")
    body = args[end]
    rest = args[1:end-1]

    ops = nothing
    prob = 1.0
    for a in rest
        if @capture(a, prob = p_)
            prob = p
        elseif a isa Expr && (a.head === :vect || a.head === :tuple)
            ops = a
        else
            error("@expect_ops: unexpected argument `$a`; expected an op list and/or `prob=...`")
        end
    end

    token = lexical_token(__module__, __source__)
    opsexpr = ops === nothing ? :(nothing) : :(Symbol[$(map(esc, ops.args)...)])

    quote
        if $is_enabled()
            local __parent__ = $(ACTIVE_DECLARATION)[]
            local __tok__ = $mix_context(__parent__ === nothing ? $(CONTEXT_HASH)[] :
                                         __parent__.token, $token)
            local __decl__ = $Declaration(__tok__, $opsexpr, Float64($(esc(prob))))
            $(ScopedValues.with)($(ACTIVE_DECLARATION) => __decl__,
                                 $(CONTEXT_HASH) => __tok__) do
                $(esc(body))
            end
        else
            $(esc(body))
        end
    end
end

"Build a synthetic forecast from a user declaration."
function declared_prediction(d::Declaration, self::ArgSpec)
    d.ops === nothing && return PredictedOp[]
    p = clamp(d.declared_prob, 0.0, 1.0)
    return [PredictedOp(OpKey(op, 1, 1), p, ArgSpec[self]) for op in d.ops]
end

"""
    site_for(token::UInt64) -> UInt64

The site identifier for the current context, accounting for an enclosing
[`@expect_ops`](@ref) region.

**Every entry point that keys into the tape store must go through this.**
`plan_allocation`, `pin!` and `explain` each independently derive a site key,
and if they disagree then `pin!` silently pins a site nobody allocates from and
`explain` reports on a site that does not exist — both failures being invisible
rather than loud.
"""
@inline function site_for(token::UInt64)
    decl = ACTIVE_DECLARATION[]
    return decl === nothing ? current_site(token) : mix_context(decl.token, token)
end

"Build the full store key for an allocation of `T` and shape `dims` at `token`."
@inline site_key(::Type{T}, dims::Tuple, token::UInt64) where {T} =
    SiteKey(site_for(token), T, Int32(length(dims)), size_bucket(dims))

# ---------------------------------------------------------------------------
# Operation instrumentation
# ---------------------------------------------------------------------------

"""
    @record_op :opname A B C

Record that operation `:opname` is about to be applied to the given arrays.
Place this immediately before the operation's task submission.

Expands to a single `is_enabled()` branch plus a call, so it costs essentially
nothing when the subsystem is off. The macro-expansion site is baked in as a
literal and used both as the lexical site token and as the mixing constant for
the calling-context hash.

**Pass only `DArray` arguments**, in a stable order, and keep that order in
sync with the matching [`@cost_model`](@ref): argument position is part of an
operation's recorded identity, because layout preference depends on role.

```julia
function LinearAlgebra.mul!(C::DMatrix, A::DMatrix, B::DMatrix)
    Dagger.@record_op :mul! C A B
    ...
end
```
"""
macro record_op(op, args...)
    token = lexical_token(__module__, __source__)
    quote
        if $is_enabled()
            $_record_op!($(esc(op)), $token, ($(map(esc, args)...),))
        end
        nothing
    end
end

# ---------------------------------------------------------------------------
# Allocation hooks
# ---------------------------------------------------------------------------

"""
    AllocationPlan

What [`plan_allocation`](@ref) decided. `partitioning` and `assignment` are
what the caller should actually allocate with; the rest is bookkeeping handed
back to [`track!`](@ref).
"""
struct AllocationPlan
    partitioning::Union{Blocks,AutoBlocks}
    assignment::Any
    root::Union{Nothing,TapeRoot}
    self::Union{Nothing,ArgSpec}
    layout::Union{Nothing,LayoutChoice}
    predicted::Vector{PredictedOp}
    plan::Union{Nothing,LayoutPlan}
end

"A plan that changes nothing — returned whenever the subsystem declines to act."
passthrough(part, assignment) =
    AllocationPlan(part, assignment, nothing, nothing, nothing, PredictedOp[], nothing)

"""
    plan_allocation(::Type{T}, dims; requested=AutoBlocks(), assignment=:arbitrary,
                    token=UInt64(0)) -> AllocationPlan

Decide the partitioning for an array about to be allocated.

Declines to act — returning `requested` unchanged — when:

- the subsystem is disabled;
- `requested` is a concrete `Blocks` and `CONFIG.override_explicit_blocks` is
  false (explicit user intent wins; we only fill in what `AutoBlocks` would
  otherwise have guessed);
- the site has been seen fewer than `CONFIG.min_observations` times;
- the forecast's confidence is below `CONFIG.min_confidence`;
- the planner's gate rejects the plan (insufficient margin over the fallback,
  or too much regret at low confidence).

Always registers the site, so a declined allocation still *learns*.

Pair with [`track!`](@ref):

```julia
p = Tapes.plan_allocation(T, dims; requested=dist, assignment=assignment)
A = _allocate(T, dims, p.partitioning, p.assignment)
return Tapes.track!(A, p)
```
"""
function plan_allocation(::Type{T}, dims::Tuple;
                         requested = AutoBlocks(),
                         assignment = :arbitrary,
                         token::UInt64 = UInt64(0)) where {T}
    is_enabled() || return passthrough(requested, assignment)
    (requested isa Blocks && !CONFIG.override_explicit_blocks) &&
        return passthrough(requested, assignment)
    isempty(dims) && return passthrough(requested, assignment)

    decl = ACTIVE_DECLARATION[]
    key = site_key(T, dims, token)
    root = get_root!(key)

    fb = fallback_layout(T, dims)
    self = ArgSpec(T, dims, blocksize(fb), :arbitrary)

    # Manual override short-circuits everything.
    pinned = root.pinned
    if pinned !== nothing
        return AllocationPlan(to_blocks(pinned), pinned.assignment,
                              root, ArgSpec(T, dims, blocksize(pinned), pinned.assignment),
                              pinned, PredictedOp[], nothing)
    end

    # Forecast: a user declaration if present, otherwise the learned trie.
    pred = PredictedOp[]
    if decl !== nothing && decl.ops !== nothing
        pred = declared_prediction(decl, self)
    elseif root.nobservations >= CONFIG.min_observations
        pred = predict(root.root)
    end

    if isempty(pred) || confidence(pred) < CONFIG.min_confidence
        vlog("no usable forecast for $(key.eltype)$(dims); using fallback $fb")
        return AllocationPlan(requested, assignment, root, self, fb, pred, nothing)
    end

    m = current_machine()
    cands = candidate_layouts(T, dims, m)
    lp = plan_chain(pred, cands, self, m)

    if !lp.accepted
        vlog("plan rejected ($(lp.reason)); using fallback $fb")
        return AllocationPlan(requested, assignment, root, self, fb, pred, lp)
    end

    chosen = lp.steps[1]
    vlog("chose $chosen for $(key.eltype)$(dims) ",
         "(cost $(round(lp.cost; sigdigits=3))s vs fallback ",
         "$(round(lp.fallback_cost; sigdigits=3))s, regret $(round(lp.max_regret; digits=2)))")
    return AllocationPlan(to_blocks(chosen), chosen.assignment, root,
                          ArgSpec(T, dims, blocksize(chosen), chosen.assignment),
                          chosen, pred, lp)
end

"""
    track!(A::DArray, p::AllocationPlan) -> A

Attach the recording state produced by [`plan_allocation`](@ref) to the
freshly-allocated array, and return the array so this can be used in tail
position.

The trace is held in a `WeakKeyDict`, so it never extends `A`'s lifetime and
disappears with it.
"""
function track!(A::DArray, p::AllocationPlan)
    root = p.root
    (root === nothing || !is_enabled()) && return A
    self = p.self
    layout = p.layout
    self === nothing && return A
    layout === nothing && return A

    steps = p.plan === nothing ? LayoutChoice[layout] : p.plan.steps
    trace = LiveTrace(root, self, layout, p.predicted, steps)
    TRACES[A] = trace
    lock(STORE_LOCK) do
        root.nobservations += 1
        root.root.count += 1
    end
    return A
end

"""
    suggest_partitioning(::Type{T}, dims; kwargs...) -> (partitioning, assignment)

One-shot convenience wrapper for call sites that cannot conveniently thread an
[`AllocationPlan`](@ref) through to a [`track!`](@ref) call. The array is not
tracked, so it contributes nothing to future predictions — prefer
`plan_allocation` + `track!` wherever the allocation site can be edited
properly.
"""
function suggest_partitioning(::Type{T}, dims::Tuple; kwargs...) where {T}
    p = plan_allocation(T, dims; kwargs...)
    return (p.partitioning, p.assignment)
end

# ---------------------------------------------------------------------------
# Manual override
# ---------------------------------------------------------------------------

"""
    pin!(::Type{T}, dims, blocks::Blocks, assignment=:arbitrary; token=UInt64(0))

Force a layout for a site, bypassing prediction entirely. Intended for
debugging and for benchmark reproducibility: with a pin in place, the
subsystem's behaviour at that site is deterministic and independent of warm-up
history.

Note that pinning still requires the *site* to be identified, so in
`:backtrace` mode you must call `pin!` from the same context the allocation
happens in. In `:lexical` mode, wrap the allocation in [`@expect_ops`](@ref)
and pin against that region's token.
"""
function pin!(::Type{T}, dims::Tuple, blocks::Blocks, assignment::Symbol = :arbitrary;
              token::UInt64 = UInt64(0)) where {T}
    root = get_root!(site_key(T, dims, token))
    lock(STORE_LOCK) do
        root.pinned = LayoutChoice(blocks.blocksize, assignment, :pinned)
    end
    return root
end

"Remove all pins."
function unpin!()
    lock(STORE_LOCK) do
        for (_, r) in STORE
            r.pinned = nothing
        end
    end
    return nothing
end

"""
    clear!()

Discard all learned tapes. Does not change [`CONFIG`](@ref).

Essential for benchmarking: with tapes active, performance becomes
warm-up-dependent and bimodal, so any harness that reports a distribution
across repetitions needs to control explicitly whether each repetition starts
cold or warm.
"""
function clear!()
    lock(STORE_LOCK) do
        empty!(STORE)
        TOTAL_NODES[] = 0
    end
    empty!(TRACES)
    reset_machine!()
    return nothing
end

# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

"""
    stats() -> NamedTuple

Aggregate counters: sites tracked, trie nodes, total observations, and
forecast hit/miss counts.
"""
function stats()
    lock(STORE_LOCK) do
        nobs = 0; hits = 0; misses = 0; nodes = 0
        for (_, r) in STORE
            nobs += r.nobservations; hits += r.hits; misses += r.misses; nodes += r.nnodes
        end
        return (sites = length(STORE), nodes = nodes, observations = nobs,
                hits = hits, misses = misses,
                hit_rate = (hits + misses) > 0 ? hits / (hits + misses) : NaN,
                live_traces = length(TRACES))
    end
end

"""
    report([io]; limit=20)

Human-readable overview of what has been learned: one line per site with its
observation count and modal predicted chain.

(Named `report` rather than `summary` to avoid shadowing `Base.summary` inside
this module.)
"""
function report(io::IO = stdout; limit::Int = 20)
    s = stats()
    println(io, "Dagger.Tapes: ", CONFIG.enabled ? "enabled" : "disabled",
            " (site_id=:", CONFIG.site_id, ")")
    println(io, "  sites=", s.sites, " nodes=", s.nodes,
            " observations=", s.observations, " live=", s.live_traces)
    roots = lock(STORE_LOCK) do
        sort!(collect(values(STORE)); by = r -> -r.nobservations)
    end
    for (i, r) in enumerate(roots)
        i > limit && (println(io, "  ... ", length(roots) - limit, " more"); break)
        pred = predict(r.root)
        chain = isempty(pred) ? "(no prediction)" :
                join([string(p.key.op, "@", round(p.prob; digits = 2)) for p in pred], " -> ")
        println(io, "  [", r.nobservations, "x] ", r.key.eltype, "^", r.key.ndims,
                " site=", string(r.key.site; base = 16)[1:min(end, 8)],
                r.pinned === nothing ? "" : " PINNED=$(r.pinned)")
        println(io, "        ", chain)
    end
    return nothing
end

"""
    dump_tapes([io])

Print the full trie for every site, with per-branch counts. Verbose; intended
for debugging prediction quality when `report` shows something surprising.
"""
function dump_tapes(io::IO = stdout)
    roots = lock(STORE_LOCK) do
        sort!(collect(values(STORE)); by = r -> -r.nobservations)
    end
    for r in roots
        println(io, "site ", string(r.key.site; base = 16), " ", r.key.eltype,
                " ndims=", r.key.ndims, " obs=", r.nobservations)
        _dump_node(io, r.root, 1)
    end
    return nothing
end

function _dump_node(io::IO, n::TapeNode, indent::Int)
    kids = n.children
    kids === nothing && return nothing
    for (k, c) in sort!(collect(kids); by = kv -> -kv[2].count)
        stops = max(0, c.count - child_total(c))
        println(io, " "^(2 * indent), k, "  x", c.count,
                stops > 0 ? " (chain ended here x$stops)" : "")
        _dump_node(io, c, indent + 1)
    end
    return nothing
end

"""
    explain([io], ::Type{T}, dims; requested=AutoBlocks(), assignment=:arbitrary)

Show, for a hypothetical allocation of `T` and shape `dims` at the *current*
call site, what the subsystem predicts, which candidates it considered, what
each costs, and why it accepted or rejected the resulting plan.

This is the first thing to reach for when a layout choice looks wrong. Call it
from the same context as the real allocation so the site key matches.

```julia
Dagger.Tapes.explain(Float64, (4096, 4096))
```
"""
function explain(io::IO, ::Type{T}, dims::Tuple;
                 requested = AutoBlocks(), assignment = :arbitrary) where {T}
    if !is_enabled()
        println(io, "Tapes is disabled; every allocation uses the caller's request.")
        return nothing
    end
    decl = ACTIVE_DECLARATION[]
    key = site_key(T, dims, UInt64(0))
    site = key.site
    root = get_root(key)

    println(io, "site   = 0x", string(site; base = 16))
    println(io, "key    = ", T, " ndims=", length(dims), " bucket=",
            unpad_dims(size_bucket(dims), length(dims)))
    if root === nothing
        println(io, "status = unseen; would use fallback ", fallback_layout(T, dims))
        return nothing
    end
    println(io, "obs    = ", root.nobservations,
            " (min required ", CONFIG.min_observations, ")")

    pred = decl !== nothing && decl.ops !== nothing ?
           declared_prediction(decl, ArgSpec(T, dims, blocksize(fallback_layout(T, dims)), :arbitrary)) :
           predict(root.root)
    if isempty(pred)
        println(io, "forecast = none; would use fallback ", fallback_layout(T, dims))
        return nothing
    end
    println(io, "forecast =")
    for (i, p) in enumerate(pred)
        println(io, "  ", i, ". ", p.key, "  p=", round(p.prob; digits = 3),
                has_cost_model(p.key.op) ? "  [modeled]" : "  [generic]")
    end

    m = current_machine()
    self = ArgSpec(T, dims, blocksize(fallback_layout(T, dims)), :arbitrary)
    cands = candidate_layouts(T, dims, m)
    println(io, "candidates (per-step expected cost, seconds):")
    for c in cands
        costs = [step_cost(p, self, c, m) for p in pred]
        println(io, "  ", rpad(string(c), 44), " total=",
                round(sum(costs); sigdigits = 4), "  ",
                join([string(round(x; sigdigits = 3)) for x in costs], " "))
    end

    lp = plan_chain(pred, cands, self, m)
    println(io, "plan   = ", isempty(lp.steps) ? "(none)" : string(lp.steps[1]),
            "  (committed; receding horizon)")
    length(lp.steps) > 1 && println(io, "  full: ", join(string.(lp.steps), " | "))
    println(io, "cost   = ", round(lp.cost; sigdigits = 4),
            "  fallback = ", round(lp.fallback_cost; sigdigits = 4),
            "  ratio = ", round(lp.cost / lp.fallback_cost; digits = 3),
            "  (must be <= ", CONFIG.gate_margin, ")")
    println(io, "regret = ", round(lp.max_regret; digits = 3),
            "  (limit ", CONFIG.max_regret_ratio, " below confidence ",
            CONFIG.regret_confidence_threshold, ")")
    println(io, "verdict= ", lp.accepted ? "ACCEPTED" : "REJECTED ($(lp.reason))")
    return nothing
end

explain(::Type{T}, dims::Tuple; kwargs...) where {T} = explain(stdout, T, dims; kwargs...)
explain(::Type{T}, dims::Integer...; kwargs...) where {T} = explain(stdout, T, dims; kwargs...)
