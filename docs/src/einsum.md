# Einsum (`@einsum`)

`Dagger.@einsum` is a Dagger-owned Einstein-summation macro for `DArray`s,
in the same family as [`@stencil`](@ref). It generates one Datadeps task per
output tile (and per contracted tile), so communication follows the existing
chunk layout. It is **not** a wrapper around TensorOperations, OMEinsum, or
Tullio — those packages still need their own `DArray` tensor backend.

## Syntax

```julia
using Dagger
import Dagger: @einsum

A = rand(Blocks(4, 4), 8, 8)
B = rand(Blocks(4, 4), 8, 8)
C = zeros(Blocks(4, 4), 8, 8)

@einsum C[i,j] = A[i,k] * B[k,j]   # in-place; same values as mul!(C, A, B)
D = @einsum A[i,k] * B[k,j]        # allocate
@einsum D[i,j] := A[i,k] * B[k,j]  # allocate into D
s = @einsum A[i,j] * B[i,j]        # Frobenius inner product (scalar)
```

Repeated indices are contracted. A left-hand side names the free indices. A
naked product (no assignment) uses the Einstein convention: indices that
appear once are free, indices that appear more than once are summed.

`+=` accumulates. A scalar coefficient is allowed (`@einsum C[i,j] = α * A[i,k] * B[k,j]`).
A `begin ... end` block runs statements sequentially, like `@stencil`.

## LinearAlgebra still owns 2-tensor products

`mul!`, `*`, and `dot` remain the Base / LinearAlgebra entries for GEMM, GEMV,
and inner products. `@einsum` is the notation for those and for products that
have no single LinearAlgebra method (Hadamard-then-reduce, outer products,
three-factor chains). Prefer `mul!(C, A, B)` when that is what you mean.

## Partitioning

Operands that share an index name must have the same length **and** the same
`Blocks` size along that dimension. Repartition first if they do not; the
macro will not silently copy. Host `Array` operands are wrapped as a single
tile.

At most three tensor factors are supported in one product. Split a longer
chain into successive `@einsum` statements.
