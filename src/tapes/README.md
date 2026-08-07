# Dagger.Tapes — history-driven partitioning selection

Speculative selection of `DArray` partitionings from recorded operation
history. Opt-in, disabled by default, and inert when disabled.

## Files

```
tapes/Tapes.jl        module entry, TapeConfig, enable!/disable!
tapes/tape.jl         keys, site identification, the trie, recording, prediction
tapes/cost.jl         MachineModel, ArgView, @cost_model, default models
tapes/plan.jl         candidate generation, DP planner, regret gating
tapes/api.jl          plan_allocation/track!, @record_op, @expect_ops, introspection
tapes/integration.jl  resolve_partitioning, @tracked_alloc, patch points
tapes_tests.jl        test suite (rename to test/tapes.jl)
```

Copy `tapes/` to `src/tapes/`, add to `src/Dagger.jl` immediately after the
`include("array/darray.jl")` line:

```julia
include("tapes/Tapes.jl"); using .Tapes
```

That alone is inert. The allocation and instrumentation patch points are
written out in the `PATCH POINTS` comment block at the bottom of
`tapes/integration.jl`.

## How it works

1. **Identify.** Each allocation gets a `SiteKey`: calling context, element
   type, rank, and a log-bucketed size. Three context strategies —
   `:backtrace` (unwind and hash raw IPs), `:context` (incremental
   probabilistic calling context, O(1)), `:lexical` (macro-expansion site
   only, free).

2. **Record.** `@record_op :cholesky! A` appends `(op, argument position,
   arity)` to the array's tape, committed into a per-site prefix trie
   immediately. Argument position is part of the identity: being the `A` of
   `mul!(C, A, B)` implies a different layout preference from being the `C`.

3. **Predict.** On a later allocation from the same site, walk the trie
   greedily and emit `PredictedOp`s carrying the cumulative probability of
   reaching each step.

4. **Plan.** Cost each candidate layout against each predicted step, then
   dynamic-program the chain including redistribution cost on the edges.
   Linear chain, so exact in O(n·|L|²).

5. **Gate.** Adopt the plan only if it beats the fallback by
   `gate_margin`, and — below `regret_confidence_threshold` — only if no
   single predicted operation is more than `max_regret_ratio` worse than its
   own best layout.

Only `steps[1]` is committed; the planner re-runs at each observed operation
against the branch actually taken.

## Usage

```julia
Dagger.Tapes.enable!()

for i in 1:10
    A = rand(AutoBlocks(), Float64, 4096, 4096)
    B = A * A' + 4096I
    cholesky!(B)
end

Dagger.Tapes.report()                        # what was learned
Dagger.Tapes.explain(Float64, (4096, 4096))  # why a layout was chosen
```

Ahead-of-time, no warm-up required:

```julia
Dagger.@expect_ops [:mul!, :cholesky!, :trsm!] begin
    A = rand(AutoBlocks(), Float64, n, n)
    ...
end
```

Without an operation list, `@expect_ops` just establishes a stable lexical
site key — which makes `site_id = :lexical` as precise as `:backtrace` for the
code you care about, at zero cost.

Declaring a cost model:

```julia
Dagger.Tapes.@cost_model my_solve(A, B) = begin
    nt = nblocks(A, 1)
    serial_time(nt * blocksize(A, 1)^2) +
    flops_time(size(A, 1)^2 * size(B, 2)) * imbalance(nt) +
    task_time(nt^2 / 2)
end
```

Argument order must match the `@record_op` call for that operation. Pass only
`DArray`s.

## Benchmarking

Performance becomes warm-up-dependent and bimodal once tapes are active, so
`dagger_bench.py` and anything else reporting a distribution across
repetitions needs to control cold vs warm explicitly:

- `Tapes.clear!()` between repetitions for cold measurements
- `Tapes.pin!(T, dims, Blocks(...), :cyclicrow)` to make a site deterministic
  and history-independent
- `Tapes.disable!()` for a true baseline
- `Tapes.stats().hit_rate` to confirm predictions are actually landing

## Verify in this order

1. **`@cost_model` expansion.** The one construct I could not test — no Julia
   in the build container. It names the method via
   `Expr(:., TAPES_MODULE, QuoteNode(:op_cost))`. If it misbehaves, the two
   alternatives are a bare unescaped `op_cost` (relies on hygiene resolving a
   *definition*) or `GlobalRef(TAPES_MODULE, :op_cost)`.
2. `Tapes.enable!(); Tapes.explain(Float64, (4096,4096))` — exercises site
   identification, candidate generation, the planner and all default models in
   one call.
3. The test suite, which runs without a cluster except for the last two
   groups.
4. `@record_op` overhead with `site_id = :backtrace` against your 100µs task
   floor.

## TODOs in the code

Every one carries its rationale inline. The load-bearing ones:

| where | what |
|:--|:--|
| `record_op!` | **Joint planning.** Tapes are per-array but layout decisions are joint — `mul!(C,A,B)` needs mutually compatible layouts. Currently undervalues layouts that only pay off when all operands agree. Wants union-find over co-participating arrays using the Datadeps aliasing analysis. |
| `integration.jl` | **Residency tracker.** Precondition for read-only replication, precise `redistribution_cost`, heterogeneous cost models and OOC. Highest-leverage missing piece. |
| `integration.jl` | **Datadeps lookahead.** Inside `spawn_datadeps` the full dependency structure is already known — exact lookahead, no prediction. Strictly better where it applies; build before broadening `@record_op` coverage. |
| `maybe_repartition!` | **Repartition mechanism.** Planner emits per-step layouts; nothing acts on them. Hard part is arrays captured by already-submitted-but-unscheduled thunks. |
| `current_machine` | **Calibration.** FLOP rate and bandwidth are guesses; should be measured at `enable!` and refined from `TimespanLogging` task durations. Natural autotuner seam. |
| `backtrace_hash` | **Persistence.** Raw IPs are session-local. Symbolicating `(file, line)` with a per-IP cache would make tapes survive restarts — which is what makes the *first* run fast rather than the second. |
| `ArgSpec` | **Assignment provenance.** `DArray` does not store the assignment it was built with, so recorded specs assume `:arbitrary`. |
| `OP_AFFINITY` | **Learn it.** Hand-maintained table that will rot; the tape already produces the triples needed to fit it. |
