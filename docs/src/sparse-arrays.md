# Sparse Distributed Arrays

Dagger's [`DArray`](@ref) can hold **sparse** tiles, giving you a distributed,
tiled sparse matrix (or vector) that participates in the same scheduling,
[Datadeps](@ref), and linear-algebra machinery as dense `DArray`s. This is the
foundation for distributed sparse matrix multiplication and the
[matrix-free iterative solvers](@ref "Iterative Solvers").

Sparse support is provided through package extensions, so you opt in by loading a
sparse backend:

- **`SparseArrays`** (the standard library) — tiles are `SparseMatrixCSC` /
  `SparseVector`. This is the default, well-supported backend.
- **`Finch`** — tiles are `Finch.Tensor`s, enabling a wider range of sparse
  formats. This backend is more experimental.

```julia
using Distributed
addprocs(4)
using Dagger, SparseArrays
```

!!! note "Load order with workers"
    As with all Dagger usage, add your workers *before* `using Dagger` and the
    backend package, so the packages load on every worker. See the note at the
    top of the [home page](@ref "Dagger: A framework for out-of-core and parallel execution").

## Creating a sparse `DArray`

### From an existing sparse array

`distribute` accepts a sparse matrix or vector and partitions it into sparse
tiles according to a `Blocks` specification:

```julia
using SparseArrays
A = sprand(1000, 1000, 0.01)        # a SparseMatrixCSC
DA = distribute(A, Blocks(250, 250)) # a 4×4 grid of sparse tiles
```

Each tile is a sparse matrix in its own right, stored on one of the workers.

### Allocating directly

You can also allocate a sparse `DArray` without first building a local sparse
array, using the `Blocks`-aware methods of `spzeros` and `sprand`:

```julia
using SparseArrays

# All-zeros sparse DArray, Float64, 1000×1000 in 250×250 tiles
Z = spzeros(Blocks(250, 250), Float64, 1000, 1000)

# Random sparse DArray with ~1% nonzeros per tile
R = sprand(Blocks(250, 250), Float64, (1000, 1000), 0.01)
```

These run the per-tile allocation on the owning worker, so no large sparse array
is ever materialized on a single process.

### Converting back to a dense array

`collect` gathers the tiles and returns a **dense** `Array`:

```julia
M = collect(DA)   # dense Matrix{Float64}
```

To keep data sparse and distributed, operate on the `DArray` directly rather
than collecting.

## How it works

### The `DSparseArray` wrapper

Internally, each sparse tile is wrapped in a [`Dagger.DSparseArray`](@ref) — a
small mutable container holding the actual sparse storage (`mat`):

```julia
mutable struct DSparseArray{T,N} <: AbstractArray{T,N}
    mat   # e.g. a SparseMatrixCSC, SparseVector, or Finch.Tensor
end
```

`DSparseVector{T}` and `DSparseMatrix{T}` are the 1- and 2-dimensional aliases.

The wrapper exists because **sparse storage is reallocated on writes**. Many
sparse operations (e.g. `A*B`, or anything that changes the sparsity pattern)
cannot update their result in place — they produce a brand-new sparse array of a
different size. Datadeps, however, tracks data dependencies by the *identity* of
the objects it manages, and it does not support objects that grow or shrink. The
`DSparseArray` wrapper solves this: its identity is stable, and a write simply
swaps the inner `mat` for the new storage:

```julia
# Conceptually, how an in-place sparse update is modeled:
tile.mat = tile.mat * other     # identity of `tile` is unchanged
```

### Aliasing as a whole

Because the inner storage may move, it is never safe to alias *part* of a sparse
tile (e.g. via a `view` or a strided sub-region). Dagger therefore treats a
`DSparseArray` as an **indivisible aliasing unit**: any access — including
through `view`, `transpose`, `adjoint`, or `reshape` — resolves to the
container's stable whole-object aliasing. This is what keeps Datadeps correct
when sparse writes reallocate storage. (For the curious: the type opts in via
`Dagger.aliases_as_whole`, and Datadeps' `aliasing_root` unwraps any wrapper of a
`DSparseArray` before computing aliasing. Calling `pointer` on a `DSparseArray`
intentionally errors, to catch any code path that tries to treat it as raw
strided memory.)

The practical upshot: you can pass sparse tiles, or views of sparse `DArray`s,
into `Dagger.spawn_datadeps` regions and trust that read/write ordering is
tracked correctly.

## Operations

Sparse `DArray`s support the array operations that have distributed
implementations, including:

- **Matrix–matrix multiply** (`A * B`, `mul!`), sparse × sparse, producing a
  sparse result.
- **Sparse matrix–vector multiply** (SpMV: `A * x`, `mul!(y, A, x)`) with a
  sparse matrix and dense vectors — the workhorse of iterative solvers.
- **Transpose/adjoint**, **`collect`**, and elementwise/`norm` operations.

```julia
using SparseArrays, LinearAlgebra
A = distribute(sprand(1000, 1000, 0.01), Blocks(250, 250))
x = distribute(rand(1000), Blocks(250))

y = A * x            # distributed SpMV -> dense DVector
C = A * A            # distributed sparse-sparse matmul -> sparse DArray
```

### Partitioning guidance

- Choose tile sizes so each tile comfortably fits on a worker, and so the number
  of tiles is at least the number of workers (for parallelism).
- For **square operators used with the iterative solvers**, use **square tiles**
  (`Blocks(k, k)`); see [Iterative Solvers](@ref) for why.
- Operands with mismatched partitionings are aligned automatically (by buffered
  copy) where needed, but matching partitionings avoid that overhead.

## Backends

### `SparseArrays` (recommended)

Tiles are `SparseMatrixCSC` (matrices) or `SparseVector` (vectors). This backend
provides efficient SpMV (including transposed/adjoint operands) and uses
`SparseArrays`' own `*` for sparse–sparse products.

### `Finch` (experimental)

Loading `Finch` makes tiles `Finch.Tensor`s, supporting a broader set of sparse
and structured formats. Finch support is newer and exercised by a dedicated test
suite; prefer `SparseArrays` unless you specifically need a Finch format.

## Limitations

- `collect` densifies; there is no sparse-preserving global gather.
- A sparse tile is aliased as a whole — Datadeps cannot track independent writes
  to disjoint sub-regions of a single sparse tile (use finer tiling instead).
- Not every dense `DArray` operation has a sparse counterpart yet; sparse support
  focuses on multiplication and the building blocks needed for iterative solving.

## API

```@docs
Dagger.DSparseArray
```
