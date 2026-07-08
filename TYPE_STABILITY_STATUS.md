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

## Intentionally deferred (with rationale)

| Finding | Why deferred |
|---------|--------------|
| **P0.1 full typed args through Thunk** | Large API surface (`PayloadOne`, `Thunk.inputs`, serialization). Partial win via `Tf` + cached `Signature`. Full typed `Vector{Argument}` erasure fix needs a dedicated design pass. |
| **P1.5 full execute! specialization** | User `f` must stay dynamic. CPU move specialized; GPU correctly keeps `@invokelatest`. |
| **P2 result `Some{Any}` / TLS / ReuseCleanup** | Lower ROI; TLS circular-include issue; result boxing needed for error union. |
| **Parametric ExactScope{P}** | Touches all GPU extensions; risk of method ambiguities. KnownScope union covers Options storage. |

## Validation

- `scopes`, `scheduler`, `processors`, `options`, `thunk` (alone), `datadeps`: pass
- Thunk `@spawn` `EAGER_CONTEXT[] === nothing` fails only when run after datadeps in same process (pre-existing test-order coupling, not a regression from these commits)
- GPU: not run in this environment; `move_arg!` keeps `@invokelatest` for non-CPU processors so CUDA/ROC/Metal/OpenCL/Intel extensions remain correct

## Suggested cherry-pick / bench order

1. DefaultScope singleton (safest, clear alloc win)
2. TaintScope Tuple + UnionScope ==
3. Context Vector{OSProc}
4. KnownScope Options
5. Signature DataType + propagates
6. Tf + move_arg!
7. Signature/compatible_processors caches

Benchmark against Datadeps linalg (cholesky/lu/matmul) and stencil suites.
