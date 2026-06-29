# Automatic Differentiation

Dagger can differentiate through task graphs built from `DArray` operations
using reverse-mode automatic differentiation (AD), powered by
[Enzyme.jl](https://github.com/EnzymeAD/Enzyme.jl). This works for both
*functional* code (e.g. `sum`, `map`, broadcasting, reductions) and *mutating*
code that runs inside [Datadeps](@ref) regions (e.g. tiled linear algebra such
as `lu`), without requiring you to write any AD-specific kernels: ordinary
Dagger code is differentiated as-is.

AD support lives in a package extension that is loaded automatically once both
`Enzyme` and [DifferentiationInterface.jl](https://github.com/JuliaDiff/DifferentiationInterface.jl)
(DI) are present in your environment:

```julia
using Dagger
using Enzyme                    # triggers the AD extension
using DifferentiationInterface  # provides the `value_and_gradient` entry point
```

Both packages are *weak* dependencies of Dagger, so they are only required if
you use AD.

## Quick start

The recommended entry point is DifferentiationInterface's
`value_and_gradient`, using the `AutoEnzyme()` backend. Pass a function `f` and
a `DArray` `x`; you get back the primal value of `f(x)` and the gradient as a
`DArray` with the same blocking as `x`:

```julia
using Dagger, Enzyme, DifferentiationInterface

x = distribute(rand(64, 64), Blocks(16, 16))

# Scalar-valued function: gradient of the scalar.
val, grad = value_and_gradient(A -> sum(abs2, A), AutoEnzyme(), x)

@assert val ≈ sum(abs2, collect(x))
@assert collect(grad) ≈ 2 .* collect(x)   # d/dA sum(A.^2) = 2A
```

`grad` is itself a `DArray`, so it stays distributed and you only `collect` it
if/when you need the dense result.

### Scalar vs. array/factorization outputs

The shape of the gradient depends on what `f` returns:

- **Scalar output** (a reduction such as `sum`, `prod`, `mapreduce`): the
  gradient is of that scalar.
- **`DArray` or factorization output** (e.g. `A * B`, `lu(A, NoPivot())`): the
  outputs are seeded with ones, so the gradient is taken of the *sum* over
  those outputs. This is equivalent to seeding the output adjoint with a
  one-like value.

```julia
B = distribute(rand(64, 64), Blocks(16, 16))

# Array-valued function: gradient of sum(A * B).
val, grad = value_and_gradient(A -> A * B, AutoEnzyme(), x)
```

## What works

The following operations are differentiable today and are covered by the test
suite (`test/enzyme.jl`):

- Allocation and copies (`copy`, `map`-based scaling).
- Reductions: `sum`, `prod`, and user-specified `mapreduce` operators.
- User-specified `map` functions (e.g. `map(sin, x)`, custom polynomials).
- Broadcasting (element-wise `.*`, `.^`, etc.).
- `GEMM` (`A * B`, `B * A`) and `GEMV` (`A * v`).
- Unpivoted LU factorization (`lu(A, NoPivot())`).

### Known gaps

A few operations do not yet produce correct gradients and are tracked as broken
tests:

- **Cholesky** (`cholesky`): the per-block `potrf!` kernel has a custom Enzyme
  reverse rule and the surrounding BLAS updates differentiate natively, but a
  functional task in the factorization graph currently raises an Enzyme
  activity error.
- **QR** (`qr`): the Householder-reflector kernels run but differentiate to an
  incorrect adjoint.
- **Linear solve** (`\`): the triangular-solve path hits an Enzyme
  `No augmented forward pass` error.
- **Stencils** (`@stencil`): pending verification.

Contributions of dedicated reverse rules for these kernels are welcome.

## How it works

Differentiation is implemented as a task-queue layer rather than as
AD-instrumented algorithms. When you call `value_and_gradient(f, AutoEnzyme(),
x)`, Dagger:

1. Installs an `EnzymeTaskQueue` beneath your code. As `f` runs, the queue
   *records* every spawned task onto an AD tape and immediately forwards it
   downstream for normal (eager) execution. Because the region is not buffered,
   code that fetches intermediate results mid-region (e.g. a reduction ending in
   `collect`) works normally.
2. Reads each task's `In`/`Out`/`InOut`/`Deps` data-access metadata from the
   task's [`Options`](@ref) (the `arg_accesses` and `arg_aliases` fields), which
   Dagger records at spawn time. This is what lets AD compose with Datadeps
   regions no matter where in the task-queue stack the recording happens.
3. Walks the tape in reverse, running Enzyme's reverse-mode VJP on each kernel
   and accumulating adjoints, keyed by a canonical buffer identity so that
   adjoints chain correctly across copies and across Datadeps regions.

This design means the *same* user code is differentiable whether or not it uses
Datadeps, and no separate "AD-enabled" algorithm variants are needed.

### Lower-level entry point

For finer control you can use `Dagger.spawn_enzyme` directly. It runs a closure
as a differentiated region and returns the primal value alongside the raw
adjoint map (an `IdDict` from each taped value to its adjoint):

```julia
value, adjoints = Dagger.spawn_enzyme(() -> sum(abs2, x); seed=nothing)
```

`value_and_gradient` is a thin wrapper over `spawn_enzyme` that assembles the
adjoints into a gradient `DArray` matching the input's blocking. Most users
should prefer the `value_and_gradient` interface.

## Other DifferentiationInterface backends

Because the entry point is DifferentiationInterface, you can in principle swap
in other AD backends (e.g. `AutoForwardDiff()`, `AutoReverseDiff()`,
`AutoFiniteDiff()`, `AutoFiniteDifferences()`). These are *not* yet supported
through the distributed task-graph machinery and many will fail or fall back to
dense evaluation; they are exercised in a separate, non-blocking CI job to track
progress. `AutoEnzyme()` is the supported backend today.
```
