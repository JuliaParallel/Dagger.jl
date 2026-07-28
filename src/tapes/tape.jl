# ===========================================================================
# Core types, allocation-site identification, and the operation-sequence trie.
# ===========================================================================

"""
Maximum array rank tracked in fixed-width tuples. Ranks above this are still
handled, but their trailing dimensions collapse into the last slot, which only
costs precision in the site key (never correctness).
"""
const MAX_TRACKED_DIMS = 8

const DimTuple = NTuple{MAX_TRACKED_DIMS,Int}

@inline function pad_dims(t::Tuple)::DimTuple
    n = length(t)
    ntuple(i -> i <= n ? Int(t[i]) : 0, MAX_TRACKED_DIMS)
end

@inline unpad_dims(t::DimTuple, n::Integer) = ntuple(i -> t[i], Int(n))

# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------

"""
    SiteKey

Identifies an allocation *site* together with the coarse shape of what it
allocates. Two allocations share a key only if they came from the same program
context, have the same element type and rank, and fall into the same size
bucket — the last of these matters because the best block size for a 1024^2
matrix is not the best block size for a 100000^2 matrix even at an identical
call site.
"""
struct SiteKey
    site::UInt64
    eltype::DataType
    ndims::Int32
    szbucket::DimTuple
end

Base.hash(k::SiteKey, h::UInt) =
    hash(k.szbucket, hash(k.ndims, hash(k.eltype, hash(k.site, h))))
Base.:(==)(a::SiteKey, b::SiteKey) =
    a.site == b.site && a.eltype === b.eltype && a.ndims == b.ndims && a.szbucket == b.szbucket

"""
    size_bucket(dims) -> DimTuple

Quantise `dims` logarithmically so that nearby problem sizes share a tape.
`CONFIG.size_buckets_per_octave == 0` disables bucketing (exact sizes).
"""
function size_bucket(dims::Tuple)::DimTuple
    bpo = CONFIG.size_buckets_per_octave
    if bpo <= 0
        return pad_dims(dims)
    end
    n = length(dims)
    ntuple(MAX_TRACKED_DIMS) do i
        i > n && return 0
        d = Int(dims[i])
        d <= 0 ? 0 : round(Int, log2(d) * bpo)
    end
end

"""
    OpKey

An operation as seen *from the perspective of one of its arguments*. The
argument position is part of the identity because layout preference depends on
role: being the `A` of `mul!(C, A, B)` implies a different preference from
being the `C`.
"""
struct OpKey
    op::Symbol
    pos::Int16
    arity::Int16
end
OpKey(op::Symbol, pos::Integer, arity::Integer) = OpKey(op, Int16(pos), Int16(arity))

Base.hash(k::OpKey, h::UInt) = hash(k.arity, hash(k.pos, hash(k.op, h)))
Base.:(==)(a::OpKey, b::OpKey) = a.op === b.op && a.pos == b.pos && a.arity == b.arity
Base.show(io::IO, k::OpKey) = print(io, k.op, "[", k.pos, "/", k.arity, "]")

const ROOT_OPKEY = OpKey(Symbol("#root"), 0, 0)

# ---------------------------------------------------------------------------
# Argument and layout descriptions
# ---------------------------------------------------------------------------

"""
    ArgSpec

Metadata-only snapshot of a `DArray` argument. Deliberately holds no reference
to the array itself so that recorded tapes never keep data alive.
"""
struct ArgSpec
    eltype::DataType
    ndims::Int32
    size::DimTuple
    blocksize::DimTuple
    assignment::Symbol
end

function ArgSpec(A::DArray{T,N}) where {T,N}
    bs = A.partitioning isa Blocks ? A.partitioning.blocksize : size(A)
    # N.B. `DArray` does not persist the assignment used to build it, so we
    # cannot recover it here.
    # TODO(assignment-provenance): add an `assignment` (or `procgrid`) field to
    # `DArray` so recorded specs and cost models can reason about the actual
    # processor mapping rather than assuming `:arbitrary`. Without it the
    # planner can pick a good *block shape* but has to guess at the mapping
    # quality of the arrays it did not itself allocate.
    ArgSpec(T, Int32(N), pad_dims(size(A)), pad_dims(bs), :arbitrary)
end

ArgSpec(::Type{T}, dims::Tuple, bs::Tuple, assignment::Symbol) where {T} =
    ArgSpec(T, Int32(length(dims)), pad_dims(dims), pad_dims(bs), assignment)

Base.eltype(s::ArgSpec) = s.eltype
Base.ndims(s::ArgSpec) = Int(s.ndims)
Base.size(s::ArgSpec) = unpad_dims(s.size, s.ndims)
Base.size(s::ArgSpec, d::Integer) = d <= s.ndims ? s.size[d] : 1

"""
    LayoutChoice

A candidate partitioning: a block shape plus a processor assignment strategy.
`label` records the family it came from, purely for explanation output.
"""
struct LayoutChoice
    blocksize::DimTuple
    ndims::Int32
    assignment::Symbol
    label::Symbol
end

LayoutChoice(bs::Tuple, assignment::Symbol, label::Symbol=:custom) =
    LayoutChoice(pad_dims(bs), Int32(length(bs)), assignment, label)

blocksize(l::LayoutChoice) = unpad_dims(l.blocksize, l.ndims)
blocksize(l::LayoutChoice, d::Integer) = d <= l.ndims ? l.blocksize[d] : 1

"Convert to a Dagger `Blocks{N}` suitable for passing to an allocator."
to_blocks(l::LayoutChoice) = Blocks(blocksize(l))

Base.:(==)(a::LayoutChoice, b::LayoutChoice) =
    a.blocksize == b.blocksize && a.ndims == b.ndims && a.assignment === b.assignment
Base.hash(l::LayoutChoice, h::UInt) = hash(l.assignment, hash(l.ndims, hash(l.blocksize, h)))

function Base.show(io::IO, l::LayoutChoice)
    print(io, "Blocks", blocksize(l), " / :", l.assignment)
    l.label === :custom || print(io, " (", l.label, ")")
end

"Number of blocks along each dimension for an array of `dims` under `l`."
nblocks(l::LayoutChoice, dims::Tuple) =
    ntuple(i -> max(1, cld(Int(dims[i]), max(1, blocksize(l, i)))), length(dims))

# ---------------------------------------------------------------------------
# Allocation-site identification
# ---------------------------------------------------------------------------

"""
    lexical_token(mod, source) -> UInt64

A compile-time-stable identifier for a macro expansion site. Computed at
macro-expansion time and baked into the expansion as a literal, so it costs
nothing at runtime.
"""
function lexical_token(mod::Module, source::LineNumberNode)
    h = hash(nameof(mod), UInt64(0x9e3779b97f4a7c15))
    h = hash(something(source.file, :unknown), h)
    h = hash(source.line, h)
    return h % UInt64
end

"""
    CONTEXT_HASH

Probabilistic calling context, in the sense of Bond & McKinley (OOPSLA 2007).
Maintained incrementally as `V := 3V + site` at instrumentation boundaries
rather than by unwinding, giving context sensitivity for the price of a
multiply-add. Propagates into child tasks automatically because
`ScopedValue`s do.
"""
const CONTEXT_HASH = ScopedValue{UInt64}(UInt64(0))

@inline mix_context(v::UInt64, token::UInt64) = 3 * v + token

"""
    backtrace_hash() -> UInt64

Hash the raw instruction pointers of the current stack, skipping
`CONFIG.backtrace_skip` frames and taking at most `CONFIG.backtrace_depth`.

Raw pointers are hashed rather than symbolicated frames because symbolication
dominates the cost by orders of magnitude. The consequence is that keys are
only meaningful within a single session and can shift if a frame is
recompiled at a different specialisation.

TODO(persistence): to persist tapes across sessions — which is what would make
the very first run of a program fast rather than the second — we need a stable
key. Options, in increasing order of cost: (a) symbolicate lazily only when
serialising and re-resolve on load; (b) hash `(file, line)` pairs obtained from
`StackTraces.lookup`, cached per instruction pointer in an `IdDict` so each
unique frame is symbolicated once; (c) require `:lexical`/`:context` mode for
persistable tapes. (b) is probably the right default — the cache makes steady-
state cost comparable to raw pointer hashing.

TODO(cost): even unsymbolicated, `backtrace()` is the single most expensive
thing this subsystem does. Consider (i) caching the hash in a task-local slot
keyed by a cheap discriminator so repeated allocations in a loop body unwind
once, or (ii) `jl_unw_stepn`-style bounded unwinding via a small C shim rather
than the full `backtrace()` allocation of a `Vector{Ptr}`.
"""
function backtrace_hash()
    bt = backtrace()
    n = length(bt)
    lo = min(CONFIG.backtrace_skip + 1, n + 1)
    hi = min(lo + CONFIG.backtrace_depth - 1, n)
    h = UInt64(0xcbf29ce484222325)
    @inbounds for i in lo:hi
        # Julia 1.12+ backtraces can mix `Ptr` frames with non-isbits
        # `Base.InterpreterIP` entries; only pointers can be reinterpreted.
        frame = bt[i]
        ip = frame isa Ptr ? (UInt(frame) % UInt64) : (hash(frame) % UInt64)
        h = (h ⊻ ip) * UInt64(0x100000001b3)
    end
    return h
end

"""
    current_site(token::UInt64) -> UInt64

Resolve the site identifier for the active `CONFIG.site_id` strategy. `token`
is the lexical token of the calling macro expansion.
"""
@inline function current_site(token::UInt64)
    mode = CONFIG.site_id
    if mode === :lexical
        return token
    elseif mode === :context
        return mix_context(CONTEXT_HASH[], token)
    else # :backtrace
        # Fold in the ambient context too: a backtrace cannot see through a
        # `Threads.@spawn` or `Dagger.@spawn` boundary, but the scoped context
        # hash can, and `@expect_ops` regions set it deliberately.
        return mix_context(CONTEXT_HASH[], backtrace_hash())
    end
end

# ---------------------------------------------------------------------------
# The tape: a prefix trie over operation sequences
# ---------------------------------------------------------------------------

"""
    TapeNode

One node of a per-site prefix trie. The path from the root to a node is an
observed operation sequence; `count` is how many times that exact prefix was
observed.

A trie rather than a flat list of recorded sequences because it gives, for
free: incremental commit (no need to wait for an array to die before learning
from it), prefix-conditional prediction (re-planning mid-chain naturally
restricts to the branch actually taken), and per-step branch probabilities
that feed straight into the expected-cost objective.
"""
mutable struct TapeNode
    const op::OpKey
    const depth::Int
    """
    Number of times this exact prefix was observed. For the root this is the
    number of arrays allocated at the site (bumped by `track!`), which makes it
    the correct denominator for `P(next op = k) = child.count / node.count`.
    The residual `node.count - sum(children counts)` is the probability that
    the chain *stopped* here — an array that is allocated and never used must
    reduce our confidence, or a site that is 90% dead weight would look
    perfectly predictable.
    """
    count::Int
    children::Union{Nothing,Dict{OpKey,TapeNode}}
    "Most recently observed specs of *all* arguments of this operation."
    argspecs::Vector{ArgSpec}
end

TapeNode(op::OpKey, depth::Int) = TapeNode(op, depth, 0, nothing, ArgSpec[])

"Sum of child counts; `count - child_total` is the number of chains that stopped here."
function child_total(n::TapeNode)
    kids = n.children
    kids === nothing && return 0
    t = 0
    for (_, c) in kids
        t += c.count
    end
    return t
end

@inline function children!(n::TapeNode)
    c = n.children
    c === nothing || return c
    d = Dict{OpKey,TapeNode}()
    n.children = d
    return d
end

nchildren(n::TapeNode) = n.children === nothing ? 0 : length(n.children)

"""
    TapeRoot

All history for one [`SiteKey`](@ref).
"""
mutable struct TapeRoot
    const key::SiteKey
    const root::TapeNode
    nobservations::Int
    nnodes::Int
    hits::Int
    misses::Int
    last_used::Float64
    "Manual override installed by [`pin!`](@ref); bypasses prediction entirely."
    pinned::Union{Nothing,LayoutChoice}
end

TapeRoot(key::SiteKey) =
    TapeRoot(key, TapeNode(ROOT_OPKEY, 0), 0, 1, 0, 0, time(), nothing)

const STORE = Dict{SiteKey,TapeRoot}()
const STORE_LOCK = ReentrantLock()
const TOTAL_NODES = Ref(0)

function get_root!(key::SiteKey)
    lock(STORE_LOCK) do
        r = get(STORE, key, nothing)
        if r === nothing
            maybe_evict!()
            r = TapeRoot(key)
            STORE[key] = r
            TOTAL_NODES[] += 1
        end
        r.last_used = time()
        return r
    end
end

get_root(key::SiteKey) = lock(STORE_LOCK) do
    get(STORE, key, nothing)
end

"""
Evict least-recently-used sites when over budget. Called with `STORE_LOCK` held.

TODO(eviction): LRU on sites is crude — a site observed 10000 times and not
touched for a minute is more valuable than one observed twice a second ago.
Weight by `nobservations` and by realised benefit (`hits`), i.e. evict by
`last_used - w*log(1+hits)`.
"""
function maybe_evict!()
    (length(STORE) < CONFIG.max_sites && TOTAL_NODES[] < CONFIG.max_nodes) && return nothing
    victims = sort!(collect(STORE); by = kv -> kv[2].last_used)
    ndrop = max(1, length(victims) ÷ 4)
    for i in 1:ndrop
        k, r = victims[i]
        TOTAL_NODES[] -= r.nnodes
        delete!(STORE, k)
    end
    vlog("evicted $ndrop site(s); $(length(STORE)) remain")
    return nothing
end

# ---------------------------------------------------------------------------
# Live traces: the association between an allocated array and its tape
# ---------------------------------------------------------------------------

"""
    PredictedOp

One step of a forecast: the operation, the cumulative probability of reaching
it, and a representative snapshot of the arguments it was last seen with.
"""
struct PredictedOp
    key::OpKey
    prob::Float64
    argspecs::Vector{ArgSpec}
end

"""
    LiveTrace

Per-array recording state. Attached to a `DArray` in a `WeakKeyDict`, so it
disappears with the array and never extends its lifetime.
"""
mutable struct LiveTrace
    const root::TapeRoot
    node::TapeNode
    nops::Int
    truncated::Bool
    """
    Spec of the array itself. Held separately from `root.key` because the key
    stores a *bucketed* size, which is right for tape lookup but wrong for
    costing — a plan computed against `2^round(log2(n))` would be off by up to
    41% in each dimension.
    """
    const self::ArgSpec
    "Layout actually chosen at allocation time."
    const layout::LayoutChoice
    "Forecast made at allocation time (for hit/miss accounting and re-planning)."
    predicted::Vector{PredictedOp}
    "Per-step layouts the planner would like; `plan[1]` is what was committed."
    plan::Vector{LayoutChoice}
    "How many recorded operations matched the forecast."
    matched::Int
    diverged::Bool
end

LiveTrace(root::TapeRoot, self::ArgSpec, layout::LayoutChoice,
          predicted::Vector{PredictedOp}, plan::Vector{LayoutChoice}) =
    LiveTrace(root, root.root, 0, false, self, layout, predicted, plan, 0, false)

const TRACES = WeakKeyDict{DArray,LiveTrace}()

get_trace(A::DArray) = get(TRACES, A, nothing)
get_trace(@nospecialize(_)) = nothing

# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------

"""
    advance!(trace, opkey, specs)

Extend `trace` by one observed operation, committing it into the trie
immediately.
"""
function advance!(trace::LiveTrace, opkey::OpKey, specs::Vector{ArgSpec})
    if trace.nops >= CONFIG.max_tape_length
        trace.truncated = true
        return nothing
    end

    # Divergence accounting against the forecast made at allocation time.
    idx = trace.nops + 1
    if idx <= length(trace.predicted)
        # Unlocked on purpose: these are diagnostic counters on the hot path,
        # and a torn increment costs us an inaccurate hit-rate readout, not
        # correctness. Everything the planner actually reads is either
        # trace-local or guarded by STORE_LOCK.
        if trace.predicted[idx].key == opkey
            trace.matched += 1
            trace.root.hits += 1
        else
            trace.diverged = true
            trace.root.misses += 1
        end
    end

    node = trace.node
    kids = children!(node)
    child = get(kids, opkey, nothing)
    if child === nothing
        child = TapeNode(opkey, node.depth + 1)
        kids[opkey] = child
        lock(STORE_LOCK) do
            trace.root.nnodes += 1
            TOTAL_NODES[] += 1
        end
    end
    child.count += 1
    child.argspecs = specs
    trace.node = child
    trace.nops += 1
    return nothing
end

"""
    record_op!(op::Symbol, token::UInt64, args::Tuple)

Record that operation `op` is about to be applied to `args`. Called by
[`@record_op`](@ref); the macro supplies `token`.

Every `DArray` in `args` that carries a live trace has the operation appended,
tagged with its own argument position. Arrays with no trace are ignored.

TODO(joint-planning): this treats each argument's tape independently, but
layout decisions are *joint* — `mul!(C, A, B)` needs the three layouts to be
mutually compatible, not merely individually good. That is the alignment half
of the classical automatic-data-layout problem (Kennedy & Kremer, TOPLAS 1998)
and it is the harder half. The right structure is to record the co-participating
arrays' identities here, union-find them into connected components, and solve
layout over the component rather than per array. Dagger's Datadeps aliasing
machinery (`src/datadeps/aliasing.jl`) already computes most of the required
overlap information.

TODO(adoption): when a traced array meets an untraced one (e.g. a user-supplied
`DArray` from `distribute`), we currently learn nothing about the untraced one.
It should be adopted into the traced one's component so the planner at least
knows a redistribution may be needed.
"""
function _record_op!(op::Symbol, token::UInt64, args::Tuple)
    is_enabled() || return nothing
    arity = length(args)

    # Fast path: if nothing here is traced, we only pay for the scan.
    any_traced = false
    @inbounds for a in args
        if a isa DArray && haskey(TRACES, a)
            any_traced = true
            break
        end
    end
    any_traced || return nothing

    specs = build_specs(args)
    @inbounds for i in 1:arity
        a = args[i]
        a isa DArray || continue
        trace = get_trace(a)
        trace === nothing && continue
        advance!(trace, OpKey(op, i, arity), specs)
        CONFIG.allow_repartition && maybe_repartition!(a, trace)
    end
    return nothing
end

"""
    record_op!(op::Symbol, args...)

Function form of [`@record_op`](@ref), for callers that build their argument
list dynamically. Carries no lexical token, so in `:context` site-identification
mode it contributes nothing to the context hash; prefer the macro where the
call site is statically known.
"""
record_op!(op::Symbol, args...) = _record_op!(op, UInt64(0), args)

"""
    build_specs(args) -> Vector{ArgSpec}

Snapshot every `DArray` argument. Non-`DArray` arguments get a degenerate spec
so that positions line up with the caller's argument list.
"""
function build_specs(args::Tuple)
    specs = Vector{ArgSpec}(undef, length(args))
    @inbounds for i in eachindex(args)
        a = args[i]
        specs[i] = a isa DArray ? ArgSpec(a) : ArgSpec(Nothing, (), (), :none)
    end
    return specs
end

# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------

"""
    predict(node::TapeNode; horizon, min_prob) -> Vector{PredictedOp}

Walk forward from `node`, greedily following the most-taken branch, and stop
when the branch probability or the cumulative probability falls below
threshold, when the chain is more likely than not to have ended, or when
`horizon` steps have been produced.

This is a first-order Markov / prediction-by-partial-match walk over the trie.
Each returned step carries the *cumulative* probability of reaching it, which
the planner uses directly as the weight on that operation's cost — an operation
we are 40% sure about should contribute 40% of its cost to the objective.
"""
function predict(node::TapeNode;
                 horizon::Int = CONFIG.horizon,
                 min_prob::Float64 = CONFIG.min_branch_prob)
    out = PredictedOp[]
    cur = node
    cum = 1.0
    for _ in 1:horizon
        kids = cur.children
        (kids === nothing || isempty(kids)) && break

        # Denominator is how many times we were *here*, not how many times we
        # left: chains that stopped at this node count against the forecast.
        total = max(cur.count, child_total(cur))
        total <= 0 && break

        best = nothing
        bestcount = 0
        for (_, c) in kids
            if c.count > bestcount
                bestcount = c.count
                best = c
            end
        end
        best === nothing && break

        p = bestcount / total
        p < min_prob && break
        cum *= p
        cum < min_prob && break

        push!(out, PredictedOp(best.op, cum, best.argspecs))
        cur = best
    end
    return out
end

"""
    confidence(pred) -> Float64

Cumulative probability of the first predicted step, i.e. how much we believe
anything at all is about to happen.
"""
confidence(pred::Vector{PredictedOp}) = isempty(pred) ? 0.0 : pred[1].prob
