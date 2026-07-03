# Dagger.Autotune

Benchmark-driven algorithm selection for Dagger's linear algebra (and other)
entrypoints. Two halves:

- **`Autotune.benchmark(...)`** — an offline sweep that measures every
  registered algorithm (BLAS/LAPACK, UMFPACK, KLU, CHOLMOD, Krylov,
  LinearSolve, CUDA, Dagger's own tiled implementations, ...) across problem
  sizes, element types, sparsity, container types, and machine
  configurations, in crash-isolated subprocesses, and persists the results
  to a TOML database under the Julia depot.

- **`Autotune.invoke_best(op, inputs...)`** — the runtime half. Profiles the
  actual inputs, predicts the fastest algorithm from the database (including
  data-movement costs for `Array ↔ CuArray ↔ DArray` conversions and the
  cost of flipping configuration knobs), converts, configures, executes, and
  restores.

Dagger is deliberately *not* privileged: if plain `LinearAlgebra.lu!` or
KLU wins on your inputs on your machine, that's what runs.

Everything lives in this directory and depends only on stdlibs
(`LinearAlgebra`, `SparseArrays`, `Distributed`, `Random`, `Printf`,
`Dates`, `TOML`). External packages (CUDA, KLU, Krylov, ...) are looked up
lazily in `Base.loaded_modules` and used only when the user has loaded them.

## Files

| file | contents |
|---|---|
| `Autotune.jl` | module wrapper, enable/disable switch, `__init__` |
| `configurables.jl` | FIXED / MUTABLE / FREE configuration knobs |
| `spec.jl` | input profiling (feature extraction), machine fingerprint |
| `registry.jl` | operations, algorithms, converters + all default registrations |
| `database.jl` | TOML result database, merging, runtime cache |
| `model.jl` | cost interpolation, plan selection, `invoke_best`, `explain` |
| `benchmark.jl` | `BenchmarkConfig`, trial expansion, subprocess orchestration, `report` |
| `worker.jl` | standalone worker entrypoint (driver generates its own copy) |

## The three configuration classes

Every tunable knob is registered as a `Configurable` with one of:

- **`FIXED`** — cannot change inside a running process (e.g. `:nthreads`,
  Julia's thread count). Still swept at benchmark time: each value gets its
  own worker process. At runtime, database entries from a mismatched FIXED
  value are usable but penalized (+50% predicted time per octave of
  mismatch); exact matches always win when present.
- **`MUTABLE`** — changeable in-process with a modeled transition cost
  (e.g. `:blas_threads` via `BLAS.set_num_threads`). `invoke_best` charges
  2× the transition cost (set + restore) and restores the previous value in
  a `finally`.
- **`FREE`** — chosen per invocation with no transition cost (e.g. a Dagger
  algorithm's `"blocksize"`, declared as `free_config` on the algorithm).
  Every value in the domain is benchmarked; the best is picked at runtime
  and passed to the algorithm through its invoke context.

## Integrating into Dagger

1. `include("autotune/Autotune.jl")` inside `module Dagger` (after the
   DArray / linalg code), then `using .Autotune`.

2. In `Dagger.__init__`, hook up the raw implementations so Autotune can
   call Dagger's algorithms *without* re-entering the autotuned entrypoints
   (this is the recursion firewall — `RAW_IMPLS` must point at the direct
   implementations):

   ```julia
   Autotune.set_raw_impl!(:dagger_lu,       A -> LinearAlgebra.lu!(A))   # DArray method
   Autotune.set_raw_impl!(:dagger_cholesky, A -> LinearAlgebra.cholesky!(A))
   Autotune.set_raw_impl!(:dagger_qr,       A -> LinearAlgebra.qr!(A))
   Autotune.set_raw_impl!(:dagger_gemm,     (A, B) -> A * B)
   Autotune.set_raw_impl!(:dagger_solve,    (A, b) -> A \ b)
   Autotune.install_darray_locality_hook!()
   ```

   (Each hook receives already-`distribute`d `DArray`s; adjust the bodies to
   whatever the direct non-dispatching calls are.)

3. Wire the public entrypoints:

   ```julia
   function LinearAlgebra.lu(A::DArray, ...)
       Autotune.enabled() && return Autotune.invoke_best(:lu, A)
       ... # existing implementation
   end
   ```

   and export the trainer: `const benchmark = Autotune.benchmark`.

4. Optional: register `:stencil` (or anything else) as a new operation —
   see *Extending* below. Nothing in the selection machinery is
   linear-algebra specific; an operation is just feature extraction + a work
   scale + input generation + a correctness check.

## Usage

```julia
Dagger.benchmark()                       # full default sweep
Dagger.benchmark(; ops=[:lu, :gemm], nthreads=[16])
Dagger.benchmark(; dry_run=true)         # print the trial plan, run nothing
Dagger.benchmark(; project=:current)     # no registry access needed
Dagger.benchmark(; axes=Dict(:solve => Dict(
    "sparse_size" => [10^5, 10^6, 10^7],
    "density"     => [1e-5, 1e-4],
    "accuracy"    => [1e-4, 1e-8, 1e-12])))
```

Results land in `~/.julia/dagger/autotune/autotune_db.toml` (override the
directory with `ENV["DAGGER_AUTOTUNE_DIR"]`). Re-running merges: trials keyed
by (operation, algorithm, features, config) are replaced, everything else is
kept. A human-readable summary prints at the end (`Autotune.report()`
reprints it anytime).

At runtime:

```julia
x = A \ b                                # entrypoints call invoke_best
Autotune.explain(:solve, A, b; accuracy=1e-8)   # why did it pick that?
Autotune.disable!()                      # bypass selection entirely
Autotune.with_autotune(false) do ... end
```

Accuracy is a first-class feature for `:solve`: benchmark entries at a
*tighter-or-equal* tolerance than requested are admissible surrogates, so a
`1e-4` request may route to GMRES while `1e-12` routes to a direct solver —
based on measurements, not folklore.

### Availability semantics

Only algorithms whose owning package is currently loaded (and functional,
for GPUs) are eligible at runtime. If the database says an *unavailable*
algorithm would beat the selected one by >10%, a one-time `@warn` suggests
loading the package. If no database exists, selection falls back to the
zero-conversion default path with a one-time nudge to run
`Dagger.benchmark()`.

## Crash robustness

Benchmarks execute in worker subprocesses grouped by FIXED configuration
(`julia --threads=N [-p M] worker.jl trials.toml outdir`). Each finished
trial is written to its own file (atomic rename), so the parent's watchdog
can kill a hung worker, record the in-flight trial as `timeout`/`crashed`,
and respawn from the next trial. Failures are *data*: a recorded failure at
scale `s` vetoes that algorithm+configuration at runtime for scales
`≥ 0.9s` (an OOM at n=16384 doesn't slander the algorithm at n=512).

## Cost model

`OperationSpec.scale(features)` is a flops-like work estimate
(`n³/3` for Cholesky, `2·nnz·n` heuristic for sparse LU, ...) used as the
interpolation coordinate: timings are interpolated piecewise-linearly in
log(scale)–log(time); extrapolation reuses the nearest segment's slope,
clamped to [0.25, 4]. Movement costs come from the `:transfer`
pseudo-operation, which benchmarks each channel (`:memcpy`, `:h2d`, `:d2h`,
`:distribute`, `:collect`) across payload sizes, with hardcoded bandwidth
fallbacks when unmeasured. A selected plan's prediction is
`t_alg + Σ movement + Σ 2·transition`, viewable via `explain`.

## Extending

```julia
Autotune.register_operation!(Autotune.OperationSpec(
    name = :stencil, nargs = 1,
    scale = f -> Float64(f["m"]) * f["n"] * get(f, "iters", 1),
    input_bytes = ..., extract_features = ..., generate_inputs = ...,
    enumerate_features = ..., default_axes = ..., check = ...))

Autotune.register_algorithm!(Autotune.AlgorithmSpec(
    name = :my_gpu_stencil, op = :stencil, package = "CUDA",
    input_form = :CuArray,
    invoke = (ctx, A) -> ...,
    free_config = Dict("tile" => [8, 16, 32])))

Autotune.register_converter!(Autotune.Converter(
    :Array, :MyDeviceArray, "MyPkg", :h2d,
    (x, cfg) -> ..., x -> Float64(sizeof(x))))

Autotune.register_configurable!(Autotune.Configurable(
    :my_knob, Autotune.MUTABLE, get_it, set_it!, 1e-4, [1, 2, 4]))
```

New registrations are picked up by both `benchmark` and `invoke_best`
automatically.

## Known limitations / TODO

- **Untested as written.** This subsystem was authored in an environment
  without a Julia toolchain, so it has never been parsed or executed. The
  design is careful, but expect a shakedown pass: API-drift typos, the
  Dagger-facing seams (`_distribute_array` uses
  `Dagger.distribute(A, Dagger.Blocks(bs, bs))`; the DArray locality hook
  guesses `chunk.handle.owner` — replace with
  `install_darray_locality_hook!` on the real accessor), and the CUDA/KLU/
  CHOLMOD call spellings deserve first scrutiny.
- Conversion planning is single-hop (`Array → CuArray`, not
  `DArray → Array → CuArray`). Multi-hop is a small Dijkstra away if wanted.
- Dense↔sparse conversions are intentionally not registered as converters
  (silent densification of a 10⁶×10⁶ matrix is a footgun); algorithms are
  only matched within their structure class. Register one explicitly if you
  want that behavior.
- `nprocs > 0` sweeps spawn workers with `-p M` and mark DArray trials
  `locality="remote"`, but the input generator places data with the default
  `distribute`; a placement-controlling generator would sharpen the
  remote-locality numbers.
- The runtime penalty heuristics (FIXED-mismatch +50%/octave, density
  ±25%/decade) are educated guesses; both are isolated in `model.jl` and
  trivially replaceable by anything smarter.
- `LinearSolve.jl` is itself a meta-selector; `linearsolve_default` is
  registered so it competes as a unit, which is fair but means Autotune
  can't see inside its choice.
