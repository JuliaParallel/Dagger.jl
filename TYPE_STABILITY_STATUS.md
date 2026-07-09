# Type Stability Pipeline — Implementation Status

Companion to [`TYPE_STABILITY_PIPELINE.md`](TYPE_STABILITY_PIPELINE.md).
Commits on `jps/type-stable` (cherry-pickable individually).

## Commits landed

| Commit | Finding | Perf justification |
|--------|---------|-------------------|
| `scopes: Cache DefaultScope singleton; specialize TaintScope proc_in_scope` | P0.2 | `DefaultScope()` 0 allocs (was Set+TaintScope each call); `proc_in_scope` skips constrain lattice |
| `options/sch: Narrow scope fields to KnownScope concrete union` | P1.4 / P0.2 | Finite union for Options/TaskSpec scope fields → better specialization |
| `context: Store procs as Vector{OSProc}; specialize compatible_processors` | P0.3 | Concrete OSProc iteration in procs()/compatible_processors |
| `scopes: Store TaintScope taints as Tuple; fix UnionScope equality` | P0.2 | Tuple taints + DefaultScope identity fast path → 0 allocs in `proc_in_scope(DefaultScope)`; single-element UnionScope fast path; fixed buggy `UnionScope ==` |
| `signature: Use Vector{DataType}; build propagated options in one shot` | P1.6 / P1.4 | Concrete signature vector; one-shot NamedTuple for propagates |
| `sch: Cache Tf on Thunk; specialize CPU move without invokelatest` | P0.1 partial / P1.5 | Avoid re-deriving `chunktype(f)` each schedule; CPU `move` without `@invokelatest` (GPU keeps it) |
| `sch/scopes: Cache Signature on Thunk; cache compatible_processors` | P1.6 / P0.3 | Reuse Signature after first schedule; cache Set{Processor} rebuilds |
| `submission: Seed Thunk.sig from typed spawn args; cache by call Type` | P0.1 partial / P1.6 | Build `Signature` from typed fargs / `DTask` return types *before* `Argument` erasure; carry via `Payload.spawn_sig` → `ThunkSpec` → `Thunk.sig`; `SPAWN_SIG_CACHE` reuses one Signature per `(f, arg-types)` shape |
| `sch: Sync CPU moves in do_task` | P1.5 | `ThreadProc`/`OSProc` move args inline instead of per-arg `Threads.@spawn`; GPU/other procs keep async overlap |

## Intentionally deferred (with rationale)

| Finding | Why deferred |
|---------|--------------|
| **P0.1 full typed `FA<:Tuple` through Thunk** | Values still travel as `Vector{Argument}` (needed for mutation + Distributed). Spawn-time `Signature` seeding recovers schedule-path types for leaf/`DTask` graphs. Full dual-rep or parametric `Thunk{FA}` needs a dedicated design pass. |
| **P1.5 full execute! specialization** | User `f` must stay dynamic. CPU move specialized; GPU correctly keeps `@invokelatest`. |
| **P2 result `Some{Any}` / TLS / ReuseCleanup** | Lower ROI; TLS circular-include issue; result boxing needed for error union. |
| **Parametric ExactScope{P}** | Touches all GPU extensions; risk of method ambiguities. KnownScope union covers Options storage. |

## Validation

- `thunk`, `scheduler`, `scopes`, `options`: pass after spawn-sig seeding
- Datadeps: core aliasing/ChunkView/DArray paths pass; remaining Raw Data / DummyErrorScheduler failures are pre-existing (`TimespanLogging.PROFILE_TASKS` missing; hierarchical wraps `DummySchedulerError` in `CompositeException`) — unrelated to spawn-sig
- Thunk `@spawn` `EAGER_CONTEXT[] === nothing` fails only when run after datadeps in same process (pre-existing test-order coupling)
- GPU: not run in this environment; `move_arg!` keeps `@invokelatest` for non-CPU processors so CUDA/ROC/Metal/OpenCL/Intel extensions remain correct

## Suggested cherry-pick / bench order

1. DefaultScope singleton (safest, clear alloc win)
2. TaintScope Tuple + UnionScope ==
3. Context Vector{OSProc}
4. KnownScope Options
5. Signature DataType + propagates
6. Tf + move_arg!
7. Signature/compatible_processors caches
8. Spawn-time Signature seeding + `SPAWN_SIG_CACHE`
9. Sync CPU moves in `do_task` (CPU only; GPU stays async)

Benchmark against Datadeps linalg (cholesky/lu/matmul) and stencil suites; high-granularity (many small tiles) vs hierarchical-only for task-overhead signal.

## Latest A/B (spawn-sig + sync CPU moves vs prior HEAD)

`benchmark_results/opt2_ab/` — with = both commits; without = stashed back to prior HEAD.

| benchmark | without | with | ratio | allocs |
|-----------|---------|------|-------|--------|
| spawn_chain_64 | 12.67ms | 13.12ms | 1.036 | ~flat |
| spawn_fanout_128 | 13.11ms | 12.24ms | 0.934 | ~flat |
| gemm_128_8 | 4776ms | 4626ms | 0.968 | 4.6M→3.9M |
| gemm_256_16 | 8012ms | 5797ms | 0.724 | 6.1M→3.7M |
| cholesky_128_8 | 1264ms | 996ms | 0.788 | ~flat |
| cholesky_256_16 | 1822ms | 1081ms | 0.594 | ~flat |

Geometric mean ratio **0.826** (<1 = with faster). Datadeps wins dominate; spawn micro ~flat (cache offsets spawn-sig submit cost).

Vs hierarchical-only is noisy under low free RAM (~1–3 GiB): spawn micros favor type-stable (~3–7% / fewer allocs), but GEMM/Cholesky medians swing with run order. Prefer the within-tree A/B above for attributing these edits.
