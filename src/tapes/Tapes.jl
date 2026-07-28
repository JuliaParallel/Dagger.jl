"""
    Dagger.Tapes

Speculative, history-driven selection of `DArray` partitionings.

# Rationale

Choosing a partitioning at allocation time is a decision made with no knowledge
of what the array will be *used for*. Any fixed default (currently
`auto_blocks`, which splits the last dimension by processor count) is
necessarily wrong for large classes of operations: a Cholesky factorization
wants square tiles laid out 2D-cyclically, a row-wise `mapreduce` wants fat row
blocks, a triangular solve against a vector wants something else again.

This module exploits the *principle of persistence*: in real applications,
allocations happen at a small number of program points, and the sequence of
operations applied to the resulting array is highly repeatable across
executions of that program point. So:

1. At each allocation, identify the *site* (call-stack context + element type +
   coarse size bucket).
2. Record the ordered sequence of operations subsequently applied to the array
   from that site, into a per-site prefix trie (`TapeRoot`).
3. On a later allocation from the same site, walk the trie to predict the
   operation sequence that is about to follow, with a probability attached to
   each step.
4. Feed that prediction, plus per-operation cost models, into a dynamic program
   that picks the layout minimising expected total cost across the whole
   predicted chain (including the cost of any mid-chain repartitioning).

A misprediction costs a suboptimal layout, never a wrong answer.

# Status / safety

The whole subsystem is **opt-in** and disabled by default; when disabled every
hook is a single predictable branch on a `const` struct field, which the
compiler hoists out of hot paths. When enabled, it never overrides an
*explicit* `Blocks(...)` supplied by the user unless
`CONFIG.override_explicit_blocks` is set — it only fills in the choice that
`AutoBlocks()` would otherwise have made blindly.

# Quick start

```julia
Dagger.Tapes.enable!()                      # :backtrace site identification

# Run your workload once to populate tapes, then again to benefit:
for i in 1:10
    A = rand(AutoBlocks(), Float64, 4096, 4096)
    A = A * A' + 4096I
    cholesky!(A)
end

Dagger.Tapes.report()                      # what was learned
Dagger.Tapes.explain(Float64, (4096, 4096)) # why a layout was chosen
```

Cheaper site identification, and ahead-of-time declaration, are available via
[`@expect_ops`](@ref):

```julia
Dagger.@expect_ops [:mul!, :cholesky!, :trsm!] begin
    A = rand(AutoBlocks(), Float64, 4096, 4096)
    ...
end
```

# Public API

- [`enable!`](@ref), [`disable!`](@ref), [`CONFIG`](@ref)
- [`@record_op`](@ref) / [`record_op!`](@ref) — instrument an operation
- [`@cost_model`](@ref) — declare a cost model for an operation
- [`@expect_ops`](@ref) — declare an operation chain ahead of time
- [`plan_allocation`](@ref) / [`track!`](@ref) — allocation-site hooks
- [`report`](@ref), [`explain`](@ref), [`dump_tapes`](@ref), [`clear!`](@ref),
  [`pin!`](@ref), [`unpin!`](@ref) — introspection and manual override
"""
module Tapes

using ..Dagger
import ..Dagger: DArray, Blocks, AutoBlocks, AbstractBlocks, AssignmentType
import ..Dagger: auto_blocks, num_processors

using ScopedValues
using MacroTools: @capture

export @record_op, @cost_model, @expect_ops

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

"""
    TapeConfig

Tunables for the tape subsystem. The live instance is [`CONFIG`](@ref); mutate
its fields directly, or use [`enable!`](@ref) which takes the same names as
keyword arguments.
"""
Base.@kwdef mutable struct TapeConfig
    "Master switch. When `false` every hook is a single branch and returns immediately."
    enabled::Bool = false

    """
    How allocation sites are identified. One of:
    - `:backtrace` — hash the raw instruction pointers of `backtrace()`. Most
      precise, costs roughly 100-500us per allocation depending on stack depth.
      Keys are only valid within a session (see TODO on symbolication).
    - `:context`   — probabilistic calling context (Bond & McKinley, OOPSLA'07):
      an incrementally-maintained hash mixed at `@record_op` / `@expect_ops`
      boundaries. O(1), but only as precise as the instrumentation density.
    - `:lexical`   — the macro-expansion site of the allocation call only.
      Effectively free, but cannot distinguish two callers of the same wrapper.
    """
    site_id::Symbol = :backtrace

    "Number of stack frames hashed in `:backtrace` mode (nearest-first, after `backtrace_skip`)."
    backtrace_depth::Int = 24
    "Frames to discard from the top of the stack (Dagger's own plumbing)."
    backtrace_skip::Int = 2

    "Powers-of-two subdivisions per octave when bucketing sizes into a site key. `0` = exact sizes."
    size_buckets_per_octave::Int = 2

    "Maximum number of operations recorded per array (trie depth cap)."
    max_tape_length::Int = 24
    "How far ahead the planner looks when predicting."
    horizon::Int = 12
    "A trie branch is only followed during prediction if it holds at least this share of its parent's count."
    min_branch_prob::Float64 = 0.15
    "Minimum cumulative probability for the first predicted op before we will speculate at all."
    min_confidence::Float64 = 0.5
    "Minimum times a site must have been observed before its predictions are used."
    min_observations::Int = 2

    """
    The planned layout must beat the fallback layout by at least this factor to
    be adopted (`0.9` = must be at least 10% cheaper). Guards against churn from
    cost-model noise.
    """
    gate_margin::Float64 = 0.9

    """
    Reject a plan if, for any *individual* predicted operation, it is more than
    this many times worse than the best layout for that operation alone. This is
    the minimax-regret safety valve: early on, prefer a layout that is decent for
    everything over one that is optimal for the modal chain and catastrophic
    otherwise.
    """
    max_regret_ratio::Float64 = 3.0
    "Below this cumulative confidence, `max_regret_ratio` is enforced strictly."
    regret_confidence_threshold::Float64 = 0.85

    "Upper bound on candidate layouts considered per allocation (planner is O(n*L^2))."
    max_candidates::Int = 16

    "Override a partitioning the user gave explicitly. Off by default: explicit user intent wins."
    override_explicit_blocks::Bool = false

    """
    Permit mid-chain repartitioning when the plan calls for it. Currently a
    no-op stub; see `maybe_repartition!`.
    """
    allow_repartition::Bool = false

    "LRU cap on the number of distinct sites retained."
    max_sites::Int = 4096
    "LRU cap on trie nodes across all sites."
    max_nodes::Int = 200_000

    "Emit `@debug`-style commentary about every decision."
    verbose::Bool = false
end

"""
    CONFIG::TapeConfig

The live configuration. See [`TapeConfig`](@ref) for field documentation.
"""
const CONFIG = TapeConfig()

"""
    is_enabled() -> Bool

Master predicate guarding every hook. Deliberately trivial so that the branch
is cheap and predictable in the disabled case.
"""
@inline is_enabled() = CONFIG.enabled

"""
    enable!(; kwargs...)

Turn the tape subsystem on. Keyword arguments set the matching
[`TapeConfig`](@ref) fields.

```julia
Dagger.Tapes.enable!(site_id=:lexical, horizon=8, verbose=true)
```
"""
function enable!(; kwargs...)
    for (k, v) in kwargs
        hasfield(TapeConfig, k) || throw(ArgumentError("unknown Tapes config option: $k"))
        setfield!(CONFIG, k, convert(fieldtype(TapeConfig, k), v))
    end
    CONFIG.site_id in (:backtrace, :context, :lexical) ||
        throw(ArgumentError("site_id must be :backtrace, :context or :lexical, got $(CONFIG.site_id)"))
    CONFIG.enabled = true
    return CONFIG
end

"""
    disable!()

Turn the tape subsystem off. Learned tapes are retained; use [`clear!`](@ref)
to discard them.
"""
function disable!()
    CONFIG.enabled = false
    return CONFIG
end

@inline function vlog(args...)
    CONFIG.verbose && @info string("[Tapes] ", args...)
    nothing
end

include("tape.jl")
include("cost.jl")
include("plan.jl")
include("api.jl")
include("integration.jl")

end # module Tapes
