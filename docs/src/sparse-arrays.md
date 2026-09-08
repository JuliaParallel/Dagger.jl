# Sparse Distributed Arrays

Dagger's [`DArray`](@ref) can hold **sparse** tiles, giving you a distributed,
tiled sparse matrix (or vector) that participates in the same scheduling,
[Datadeps](@ref "Datadeps (Data Dependencies)"), and linear-algebra machinery as
dense `DArray`s. This is the foundation for distributed sparse matrix
multiplication and the
[matrix-free iterative solvers](@ref "Iterative Solvers").

Sparse support is provided through package extensions, so you opt in by loading a
sparse backend:

- **`SparseArrays`** (the standard library) — tiles are `SparseMatrixCSC` /
  `SparseVector` on the CPU. This is the default, well-supported backend.
- **`SparseMatricesCSR`** — tiles are `SparseMatrixCSR`. CSC is a poor host
  SpMV / GPU format; CSR keeps the same `mul!` / SpMV / SpGEMM paths without
  densifying. GPU backends may still store vendor CSC.
- **GPU + `SparseArrays`** — under a GPU compute scope (`cuda_gpu`, `rocm_gpu`,
  `cl_device`, `metal_gpu`, `intel_gpu`), tiles use the vendor sparse type when
  available (CUDA cuSPARSE / AMDGPU rocSPARSE) or else
  `Dagger.DeviceSparseMatrixCSC` (OpenCL / Metal / oneAPI) with host SpGEMM/SpMV
  fallbacks. Load the GPU package together with `SparseArrays`.
- **`Finch`** — tiles are `Finch.Tensor`s, enabling a wider range of sparse
  formats. This backend is more experimental (CPU only for now).

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

`distribute` also accepts a `SparseMatrixCSR` (from SparseMatricesCSR.jl) and
keeps the tiles in CSR — do not go through a dense `Matrix` slice:

```julia
using SparseMatricesCSR
A = sparsecsr(sprand(1000, 1000, 0.01))
DA = distribute(A, Blocks(250, 250))          # CSR tiles
DR = sparsecsr(distribute(sprand(1000, 1000, 0.01), Blocks(250, 250)),
               Blocks(250, 250))              # convert existing CSC tiles
S  = sparsecsr(DA)                            # gather to one SparseMatrixCSR
```

`sparsecsr(A::DMatrix)` gathers, like `sparse(A)` → `SparseMatrixCSC`.
`sparsecsr(A, Blocks(...))` is the tile-preserving convert. COO assembly
`sparsecsr(I, J, V, m, n, Blocks(...))` and
`spzeros(SparseMatrixCSR, Blocks(...), T, m, n)` match the SparseArrays
`sparse` / `spzeros` spellings. There is no `Dagger.to_csr`. SparseMatricesCSR
0.6 has no `spzeroscsr`; empty CSR tiles go through `SparseMatrixCSR(spzeros(...))`.

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

### Assembling from triplets

FEM/FVM codes produce local COO contributions `(I, J, V)` in global 1-based
indices. Pass those to `sparse` / `sparse!` with a `Blocks` tiling — the same
generics as `SparseArrays`, not a Dagger-specific assemble API. Triplets are
bucketed onto the owning tile; overlap (a contribution produced off the tile
that owns `(i,j)`) is sent there. Duplicates combine with `+` by default, as
with `sparse(I, J, V)`.

```julia
using SparseArrays

n = 1000
part = Blocks(250, 250)

# Local (or already-distributed) COO. Never builds a global SparseMatrixCSC.
I, J, V = Int[], Int[], Float64[]
# ... push element / face contributions ...
A = sparse(I, J, V, n, n, part)

# Incremental: allocate empty tiles, then add owner contributions (PETSc
# MatSetValues / HYPRE IJMatrixAddToValues). Do not use A[i,j] += v.
A = spzeros(part, Float64, n, n)
sparse!(A, I1, J1, V1)
sparse!(A, I2, J2, V2)   # second owner; shared (i,j) add

# I, J, V may themselves be DArrays (one chunk per owner). Their partitioning
# need not match `part` — that is the overlap-send case.
A = sparse(DI, DJ, DV, n, n, part)
```

`distribute(sparse(I, J, V), Blocks(...))` is unchanged: it still assembles a
host CSC and slices it. Prefer `sparse(I, J, V, m, n, Blocks(...))` when the
global CSC would not fit on one process.

### Converting back to a dense array

`collect` gathers the tiles and returns a **dense** `Array`. To gather without
densifying, use `sparse` on the `DArray`:

```julia
M = collect(DA)   # dense Matrix{Float64}
S = sparse(DA)    # SparseMatrixCSC, assembled from tile nonzeros
```

To keep data sparse and distributed, operate on the `DArray` directly rather
than collecting.

## How it works

### The `DSparseArray` wrapper

Internally, each sparse tile is wrapped in a [`Dagger.DSparseArray`](@ref) — a
small mutable container holding the actual sparse storage (`mat`):

```julia
mutable struct DSparseArray{T,N} <: AbstractArray{T,N}
    mat   # e.g. a SparseMatrixCSC, SparseMatrixCSR, SparseVector, or Finch.Tensor
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

### Bare sparse arguments

You can also hand a plain `SparseMatrixCSC` (or `SparseMatrixCSR`,
`SparseVector`, Finch tensor, or GPU CSC) straight to a Datadeps task. Since such a container has no identity
Datadeps can track, it is **adopted into a `DSparseArray`** — holding a private
copy — for the duration of the region, and the task receives that wrapper:

```julia
S = sprand(1000, 1000, 0.01)
Dagger.spawn_datadeps() do
    Dagger.@spawn count_nonzeros(In(S))   # receives a DSparseArray
end
```

Adoption is only possible for **read-only** (`In`) arguments. Requesting write
access (`Out`/`InOut`) throws, because the wrapper owns a copy and, more
fundamentally, `SparseMatrixCSC` and `Finch.Tensor` are immutable structs whose
storage is reallocated when the sparsity pattern changes — there is nothing to
update in place. To write, wrap it yourself and read the result back out:

```julia
S = Dagger.DSparseArray(A)
Dagger.spawn_datadeps() do
    Dagger.@spawn f!(InOut(S))
end
A = S.mat
```

A sparse `DArray` already has wrapped tiles, so it can be written to directly.

## Operations

Sparse `DArray`s support the array operations that have distributed
implementations, including:

- **Matrix–matrix multiply** (`A * B`, `mul!`), sparse × sparse, producing a
  sparse result.
- **Sparse matrix–vector multiply** (SpMV: `A * x`, `mul!(y, A, x)`) with a
  sparse matrix and dense vectors — the workhorse of iterative solvers and of
  `eigen` / `eigvals` (LOBPCG; a few extreme pairs, never a densified geev).
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
- For **square operators used with the iterative solvers**, prefer **square
  tiles** (`Blocks(k, k)`); see [Iterative Solvers](@ref) for why.
- Operands with mismatched partitionings are aligned automatically (by buffered
  copy) where needed, so nothing errors; matching partitionings avoid the
  overhead. The buffer keeps sparse tiles sparse, so aligning a sparse operand
  never densifies it.
- **`Blocks` is geometric** (contiguous index ranges). On an unstructured mesh
  that numbering is arbitrary, so geometric tiles cut many edges. Pass a graph
  partitioner to [`repartition`](@ref) / `distribute` — METIS after
  `using Metis`, or any `f(A, nparts) -> Vector{Int}`:

```julia
using Metis
# nparts = cld(n, k). Result is still `Blocks(k, k)`, of `A[p, p]`.
A2 = Dagger.repartition(A, Blocks(k, k); partitioner=Metis)

# Same permutation on the RHS (compute `perm` once; do not ask METIS twice):
perm = Dagger.partition_perm(Dagger.partition_graph(Metis, A, cld(n, k)))
A2 = Dagger.repartition(A, Blocks(k, k); perm)
b2 = Dagger.repartition(b, Blocks(k); perm)
# solve A2 * x2 = b2, then x = x2[invperm(perm)]
```

  The partitioner gathers the adjacency (`A + Aᵀ`); METIS is serial. The
  Schur-complement `splu` path still builds its own separator via the same
  k-way helper — do not route that through `repartition`.

## Backends

### `SparseArrays` (recommended)

Tiles are `SparseMatrixCSC` (matrices) or `SparseVector` (vectors). This backend
provides efficient SpMV (including transposed/adjoint operands) and uses
`SparseArrays`' own `*` for sparse–sparse products.

### `SparseMatricesCSR` (host CSR)

Loading `SparseMatricesCSR` lets tiles be `SparseMatrixCSR`. Forward SpMV uses
the package's row-wise `mul!`; SpGEMM and transposed SpMV convert the *tile* to
CSC (not a dense `Matrix`) and write the result back in the destination tile's
format. `similar` / `repartition` still allocate empty CSC tiles (the
`DArray` type does not record the inner format); `copyto!` of a whole CSR tile
restores CSR, and `sparsecsr(A, part)` converts after a re-tile. Empty CSR
`DMatrix`s are `spzeros(SparseMatrixCSR, Blocks(...), T, m, n)`.

Block-sparse (BSR) is not implemented here: there is no host ecosystem BSR type
to dispatch on.

### `Finch` (experimental)

Loading `Finch` makes tiles `Finch.Tensor`s, supporting a broader set of sparse
and structured formats. Finch support is newer and exercised by a dedicated test
suite; prefer `SparseArrays` unless you specifically need a Finch format.

## Limitations

- `collect` densifies; use `sparse(DA)` to gather tiles into one `SparseMatrixCSC`.
- A sparse tile is aliased as a whole — Datadeps cannot track independent writes
  to disjoint sub-regions of a single sparse tile (use finer tiling instead).
- Not every dense `DArray` operation has a sparse counterpart yet; sparse support
  focuses on multiplication and the building blocks needed for iterative solving.

## API

```@docs
Dagger.DSparseArray
Dagger.repartition
Dagger.partition_graph
Dagger.partition_perm
```
