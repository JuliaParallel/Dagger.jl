# Type Stability in Dagger's Task Pipeline

Investigation of dynamic dispatch and type instability from `Dagger.@spawn` /
`Dagger.spawn` through scheduling, execution, and `fetch`. Assumes
`TASK_TYPED[]=true` (the current default), which already stabilizes the
*submission-side* argument packaging via `TypedArgument` / `typed_spawn`.

Severity ranks combine: (1) how often the site runs per task, (2) whether it
allocates or only does type-domain work, and (3) how much Julia can specialize
once the site is fixed. Sites that are necessarily dynamic (user function
bodies, Distributed serialization) are called out as acceptable.

---

## Pipeline map

```text
@spawn / spawn / typed_spawn
  → Options merge + scoped options
  → DTaskSpec{true,FA} + eager_spawn (DTask + return-type metadata)
  → enqueue! → eager_launch!
      ★ TypedArgument tuple → Vector{Argument}   ← type info lost here
  → PayloadOne / PayloadMulti
  → eager_submit! → eager_submit_internal!
  → Thunk (inputs::Vector{Argument}, options::Options)
  → schedule_ready! → schedule_one!
      ★ scope constrain + compatible_processors + cost estimate
  → fire_tasks! → TaskSpec → do_tasks / do_task
      ★ move(@invokelatest) + execute!(@invokelatest)
  → store_result! → fill_registered_futures!
  → fetch(DTask) → ThunkFuture → move(OSProc, value)
```

---

## Severity legend

| Rank | Meaning |
|------|---------|
| **P0** | Hot path every task; large allocation / dispatch tax; blocks further specialization |
| **P1** | Hot path every task; moderate cost or amplifies P0 |
| **P2** | Hot path but cheaper, or cold/rare path with high per-call cost |
| **OK** | Intentionally / necessarily unstable; leave alone or only micro-optimize |

---

## P0 — Critical

### 1. Typed submission is erased at the launch boundary

**Where:** `eager_launch!` in `src/submission.jl` (`Argument[map(Argument, ...)]`),
then `PayloadOne.fargs::Vector{Argument}`, `Thunk.inputs::Vector{Argument}`,
`TaskSpec.data::Vector{Argument}`, `Argument.value` untyped.

**Why it hurts:** With `TASK_TYPED=true`, spawn builds a type-stable
`Tuple` of `TypedArgument{T}` values. Immediately before scheduler entry those
are converted to untyped `Argument`s. From that point on, every
`value`/`chunktype`/`istask`/`move` call sees `Any`. The typed path only helps
metadata inference and datadeps rewriting; the scheduler and executor never see
concrete argument types.

**Cost:** High. Per-task: boxing into `Vector{Argument}`, plus dynamic dispatch
on every subsequent argument walk (syncdeps wiring, `collect_task_inputs!`,
`signature`, `estimate_task_costs!`, `do_task` move/execute prep).

**Suggestions:**
- Carry a parallel typed payload as far as possible: e.g.
  `PayloadOne{FA}` / `Thunk` with `inputs::FA` where `FA<:Tuple`, or a
  compact `CallSig{Tf,Targs...}` stored on the thunk and reused for
  signature/defaults/compatibility.
- If full typed `Thunk` is too invasive, at least keep
  `signature` / `Tf` / per-arg `chunktype` as a concrete `Signature` or
  `NTuple{N,DataType}` computed once at launch (from the typed args) and
  never re-derived from `Argument.value`.
- Keep `Vector{Argument}` only as a serialization / remote-fire boundary
  representation; rebuild typed views on the worker when `Tf` and arg types
  are known.

### 2. Scope lattice is abstract and allocation-heavy (`UnionScope`, `TaintScope`)

**Where:** `src/scopes.jl`, used from `schedule_one!`, `can_use_proc`,
`compatible_processors`, datadeps scope construction, `Chunk.scope`.

**Problems:**
- `UnionScope.scopes::Tuple` — element type erased; construction builds a
  `Set{AbstractScope}` then re-tuples.
- `TaintScope` stores `scope::AbstractScope` and `taints::Set{AbstractScopeTaint}`.
- `DefaultScope()` allocates a fresh `Set` of taints every call.
- `constrain` / `isless` are multi-method lattices over `AbstractScope`;
  return type is effectively `AbstractScope`.
- `Options` holds `Union{AbstractScope,Nothing}` for
  `scope` / `compute_scope` / `result_scope` / `exec_scope`.
- `ScheduleTaskSpec.scope` / `TaskSpec.scope` are `AbstractScope`.

**Cost:** Very high on schedule. Each ready task typically:
1. `constrain(compute, result)`
2. `constrain` against every `Chunk` argument scope
3. `compatible_processors(scope, procs)` → more `constrain` + `proc_in_scope`
4. Per-candidate `can_use_proc` → `proc_in_scope` again

`UnionScope` × `UnionScope` is quadratic and allocates
`Vector{AbstractScope}` intermediates. Interaction with heterogeneous
`Context.procs` (below) multiplies the damage.

**Suggestions:**
- Specialize common scopes as concrete structs with type-stable ops:
  - `ExactScope{P}` (processor type in the type domain)
  - `ProcessScope` / `NodeScope` (already nearly concrete)
  - `UnionScope{S}` where `S` is a typed tuple, e.g.
    `UnionScope{Tuple{ExactScope{ThreadProc},ExactScope{ThreadProc}}}`
  - Represent `DefaultScope` / `ProcessorTypeScope` as singleton / parametric
    taints without heap `Set`s (`NTuple` of taint types, or a bitset of known
    taint kinds).
- Cache `exec_scope` aggressively once computed (partially exists via
  `options.exec_scope`); ensure datadeps and common spawn paths always set it
  so the scheduler skips constrain.
- Add a fast path: if scope is `ExactScope` / `ProcessScope`, skip
  `compatible_processors` set-building and index directly into a per-worker
  processor table.
- Replace `proc_in_scope` default (which calls `constrain`) with direct
  methods for every concrete scope pair (many already exist; close gaps so
  the default is never hit on the hot path).
- Consider a compact “scope key” (e.g. worker mask + thread mask + processor
  type tag) for the common ThreadProc-only case, with full `AbstractScope`
  only for exotic processors.

### 3. Processor collections are fully abstract

**Where:** `Context.procs::Vector{Processor}` (`src/context.jl`);
`get_processors` → `Set{Processor}`; `compatible_processors` → `Set{Processor}`;
reusable vectors in `schedule_one!` typed as `Processor`;
`OSPROC_PROCESSOR_CACHE` values are `Set{Processor}`.

**Why it hurts:** Almost every scheduling decision iterates processors and
calls `get_parent`, `root_worker_id`, `proc_in_scope`, `iscompatible_*`,
`estimate_task_costs!`. With `Vector{Processor}`, each iteration is dynamic
dispatch. Scopes that themselves dispatch on processor type (taints,
`ExactScope.processor::Processor`) compound this.

**Cost:** High per schedule attempt, scales with cluster size × candidates.

**Suggestions:**
- Partition processors by concrete type in `Context` / scheduler state, e.g.
  `Dict{Type,Vector{P}}` or dedicated `Vector{ThreadProc}` plus a side table
  for GPUs/others. Schedule ThreadProc tasks against the typed vector.
- Cache `compatible_processors(scope)` results keyed by
  `(scope_identity, procs_generation)` — scopes and the proc list change
  rarely relative to task rate.
- Store `ExactScope` with a typed processor field (`ExactScope{P}`).
- For `OSProc` children, keep a stable `Vector{ThreadProc}` (and similar) in
  the cache instead of `Set{Processor}`.

---

## P1 — High

### 4. `Options` is a bag of `Union`s and untyped callables

**Where:** `src/options.jl`; merged in `_spawn`, `populate_defaults!`,
`schedule_one!`, `get_propagated_options`, `do_task`.

**Problems:**
- Scope fields: `Union{AbstractScope,Nothing}` (see P0.2).
- `processor::Union{Processor,Nothing}`, util dicts with abstract keys/values
  (`Dict{Type,Any}`, `Dict{Type,Real}`).
- `proclist`, `checkpoint`, `restore`, `storage_root_tag` untyped.
- `options_merge!` / scoped `NamedTuple` merging uses runtime field access
  (generated merge helps, but source values are still `Any` from
  `NamedTuple`).
- `SIGNATURE_DEFAULT_CACHE` values are `Any`.
- `get_propagated_options` incrementally `merge`s a `NamedTuple` in a loop
  (allocations + type instability) before `with_options` on the worker.

**Cost:** High. Every scheduled task runs `populate_defaults!` (cache helps
after warmup) and repeatedly reads option fields through unions. Propagation
rebuilds a heterogeneous NT per execution.

**Suggestions:**
- Split “hot options” (scopes, occupancy, meta, get_result, syncdeps) into a
  compact concrete struct; keep rare hooks (`checkpoint`/`restore`/`proclist`)
  in a side object.
- Replace `Union{T,Nothing}` with sentinels or `Some` only where needed;
  for scopes, a single resolved `exec_scope::S` once known.
- Type util maps as `Dict{Type,Float64}` / `Dict{Type,UInt64}` (drop
  `MaxUtilization` from the hot dict or use a separate flag).
- Make `get_propagated_options` return a concrete `NamedTuple` type based on
  the `propagates` list when it is a constant tuple (common with
  `with_options`), or pass an `Options` subset instead of rebuilding NT.
- Cache fully populated `Options` per `Signature` (not just individual
  defaults) when task options are empty/default.

### 5. Execution path: `@invokelatest` + `Vector{Any}` arguments

**Where:** `do_task` / `ThreadProc.execute!` in `src/sch/Sch.jl`,
`src/threadproc.jl`.

**Problems:**
- Argument moves: `@invokelatest move(to_proc, value)` per argument.
- Args collected into `Vector{Any}` / kwargs `Vector{Pair{Symbol,Any}}`.
- `execute!(::ThreadProc, ...)` is `@nospecialize` and calls
  `@invokelatest f(args...; kwargs...)` into `Ref{Any}`.

**Cost:** High on every task execution. Move dispatch cannot specialize on
chunk/processor types; invoke always boxes.

**What is OK:** Invoking *arbitrary user `f`* cannot be fully type-stable in
the scheduler. Some dynamic call is required.

**Suggestions (keep user-call dynamic, stabilize the edges):**
- Specialize `move` without `@invokelatest` when `typeof(to_proc)` and
  `typeof(value)` are known — use a generated/triple-dispatch helper, or
  split Chunk vs non-Chunk paths before invoke.
- Pass arguments as a typed tuple into `execute!` when `TaskSpec` carries
  concrete types; keep `@invokelatest` only around `f(...)`.
- For `ThreadProc`, consider `Ref{T}` when return type was inferred
  (`DTaskMetadata.return_type` / signature) — best-effort, fall back to `Any`.
- Same-process Chunk handles: skip general `move` and use a concrete
  `poolget` + identity path.

### 6. `signature` rebuilds `Vector{Any}` on every schedule

**Where:** `src/sch/util.jl` `signature`, `src/utils/signature.jl`.

**Problems:** Allocates `Vector{Any}`, pushes kwarg name/type vectors, wraps
in `Signature` with hash. Done in `schedule_one!` before defaults and costing.
Argument types come from untyped `Argument`s (P0.1), so this cannot constant-fold.

**Cost:** Medium-high allocations per schedule; also keys several locked dicts.

**Suggestions:**
- Build `Signature` once at submit from typed args; store on `Thunk`.
- Represent as `NTuple{N,DataType}` (or two tuples: pos / kw) instead of
  `Vector{Any}`.
- Reuse the same object for `populate_defaults!`, cost caches, and
  `iscompatible_*`.

---

## P2 — Moderate / situational

### 7. Result storage and `fetch` path

**Where:** `store_result!` (`Some{Any}`), `TaskResult.result::Any`,
`Thunk.cache_ref::Any`, `ThunkFuture` → `Future`, `fetch` → `move(proc, value)`.

**Cost:** Medium. Completions and fetches are less frequent than schedule
attempts in wide DAGs, but still on the critical latency path for
`fetch`.

**Suggestions:**
- Store `Chunk{T,H,P,S}` without `Some{Any}` when successful (error path can
  stay boxed).
- `fetch` fast path: if value is already a local `Chunk` with concrete `T`,
  specialize `move`/`collect`.
- Accept remaining instability for remote `Future` payloads (Distributed).

### 8. `Chunk.domain` and weak wrappers

**Where:** `Chunk.domain` untyped; `WeakChunk` / `unwrap_weak_checked`;
`DepNode.thunk::Any` (intentional cycle break).

**Cost:** Low-medium. `domain` rarely used on hot schedule path;
weak unwrap is type-unstable but usually followed by `isa` checks.

**Suggestions:**
- Parameterize `domain` or use `UnitDomain` concretely by default
  (`Chunk{T,H,P,S,D}`).
- Leave `DepNode.thunk::Any` as-is (cycle break); local `::Thunk` asserts
  after unwrap are enough.

### 9. Return-type metadata and `promote_op`

**Where:** `DTaskMetadata.return_type::Type`, `cached_return_type` in
`submission.jl`.

**Cost:** Low after cache warmup; first shape still pays inference.
`Type` fields remain abstract.

**Suggestions:**
- Keep the cache (already done). Optionally store `return_type` as a
  generated function / `Val` only when used for specialization (execute
  `Ref{T}`).
- Accept instability when feeding `chunktype(::DTask)` into later signatures
  until P0.1 is fixed.

### 10. Reuse / cleanup infrastructure

**Where:** `ReuseCleanup.f::Function` (`src/utils/reuse.jl`); many
`@reusable_vector` pools typed with abstract eltypes (`Processor`,
`Union{Chunk,Nothing}`, `Any`).

**Cost:** Low-medium. Pools reduce allocs but force abstract containers.

**Suggestions:**
- Prefer concrete eltypes in reusable vectors where the common case is
  `ThreadProc` / `Chunk`.
- Store cleanup as inlined `finally` or a small union of known cleanup
  functors instead of `Function`.

### 11. Datadeps amplification

**Where:** `src/datadeps/queue.jl` `distribute_task!` — builds
`UnionScope(map(ExactScope, ...))`, writes `Options.scope` /
`exec_scope`, may preserve typed fargs longer than the default queue.

**Cost:** High *within* datadeps workloads (extra tasks for copies), but
datadeps already tries harder to keep typed specs. Still hits P0 scope/proc
issues when enqueueing to the upper queue.

**Suggestions:**
- Always set concrete `exec_scope` (already done) and prefer `ExactScope`
  over multi-exact `UnionScope` when there is a single target proc.
- Ensure upper-queue launch does not immediately erase types (depends on
  P0.1).

### 12. Logging / timespan / name annotation

**Where:** `@maybelog`, `logs_annotate!`, timeline NamedTuples with `Any`
payloads.

**Cost:** Negligible when log sink is `NoOpLog`; high when logging enabled.

**Suggestions:** Keep behind `@maybelog`; treat as OK for non-logging runs.

---

## OK — Acceptable type instability

| Site | Why acceptable |
|------|----------------|
| User `f` invocation via `@invokelatest` | Arbitrary code; must be dynamic |
| `remotecall_*` / serialization of `TaskSpec` | Distributed boundary |
| `PROCESSOR_CALLBACKS` + `invokelatest(cb)` | Rare init path |
| `checkpoint` / `restore` hooks | Rare, user-provided |
| `proclist` as `Function` | Deprecated; avoid in new code |
| Error paths (`CapturedException`, failure propagation) | Cold |
| `wait` / `fetch` blocking in `thunk_yield` | Latency dominated by wait, not dispatch |
| `NamedTuple` scoped options at `with_options` entry | User API flexibility; cost is once per scope region |

When recommending “leave unstable,” the bar used here is: either the
operation is rare, or stabilizing it would require encoding user-level
heterogeneity into the type system with little payoff versus type-domain
checks (`isa`, `chunktype`, signature hashes).

---

## Recommended attack order

1. **Stop erasing typed args at `eager_launch!`** (P0.1) — unlocks signature,
   defaults, and eventually execute specialization.
2. **Concrete / cached scopes + `exec_scope` fast path** (P0.2) — largest
   scheduler win for default and datadeps workloads.
3. **Typed processor tables + compatible-proc cache** (P0.3) — removes
   dispatch from the candidate loop; pairs with scope work.
4. **Slim hot `Options` + cached populated options** (P1.4).
5. **Stabilize `move` / arg packaging; keep only `f` dynamic** (P1.5).
6. **Precomputed concrete `Signature`** (P1.6) — partly falls out of (1).
7. Result/`Chunk` concreteness and reuse cleanup (P2).

---

## Suggested measurement plan

For each fix, compare before/after on a tight spawn loop and a small DAG:

```julia
# Submission microbench
@time for _ in 1:10^5
    t = Dagger.@spawn identity(1)
end

# Schedule+execute microbench
@time fetch.([Dagger.@spawn identity(i) for i in 1:10^4])

# Allocation focus
using AllocCheck # or Profile.Allocs / @time
```

Also `@code_warntype` on:
- `Dagger.typed_spawn(identity, 1)`
- `Dagger.Sch.schedule_one!` with a ready thunk (via a test harness)
- `Dagger.Sch.do_task` with a local `ThreadProc` `TaskSpec`

Watch for remaining `Any` / `AbstractScope` / `Processor` in inferred
locals on those paths.

---

## Summary

`TASK_TYPED=true` fixed the front door, but the hallway still widens to
`Any`: typed arguments become `Vector{Argument}`, scopes and processors are
abstract lattices/collections, and options/signature/execution re-derive
types from those erased values. The highest leverage work is preserving
type information from spawn through schedule (and caching scope×processor
queries), while leaving the final user-function invoke and Distributed
boundaries dynamic by design.
