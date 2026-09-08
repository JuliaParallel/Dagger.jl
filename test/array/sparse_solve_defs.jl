# Shared sparse-solve dispatch bodies.
#
# `A \ b` / `lu` / `factorize` / `ldiv!` on a sparse-backed DMatrix must never
# take the tiled dense LU path. The same checks have to hold across the
# acceleration × backend cross product (see `sparse_defs.jl`).
#
#   Distributed x CPU  -> test/array/linalg/solve.jl (Krylov fallback)
#                         and test/array/linalg/sparsedirect.jl (klu/splu)
#   Distributed x GPU  -> test/gpu.jl
#   MPI x CPU          -> test/mpi.jl
#   MPI x GPU          -> test/mpi_gpu_suite.jl

using SparseArrays

function _sparse_solve_with_scope(f, scope)
    scope === nothing && return f()
    return Dagger.with_options(f; scope)
end

# Nonsymmetric, diagonally dominant tridiagonal (same family as sparsedirect.jl).
sparse_solve_nonsym(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(3), n),
     1 => fill(T(-7) / 10, n - 1),
)

"""
    test_sparse_solve_dispatch(; scope, check_tile, expect_direct, T)

Assert that LinearAlgebra solve entry points on a sparse-backed `DMatrix`
stay off the dense `LU{<:DMatrix}` path.

- `expect_direct=true` when PureUMFPACK / PureKLU is loaded (`lu` → `DaggerSparseLU`)
- `expect_direct=false` when only Krylov is loaded (`lu` → `SparseIterativeFactorization`)
  If a direct extension is already loaded in this process (e.g. `sparsedirect.jl`
  ran earlier in `--test array/linalg`), the direct factor is still accepted.
"""
function test_sparse_solve_dispatch(; scope=nothing, check_tile=nothing,
                                    expect_direct::Bool=false, T=Float64)
    n, k = 32, 8
    direct_loaded = expect_direct ||
        Base.get_extension(Dagger, :PureUMFPACKExt) !== nothing ||
        Base.get_extension(Dagger, :PureKLUExt) !== nothing
    cmp_rtol = T <: AbstractFloat && sizeof(T) == 4 ? 1e-3 :
               (direct_loaded ? 1e-10 : 1e-6)

    Random.seed!(1234)
    Asp = sparse_solve_nonsym(T, n)
    b = rand(T, n)
    xref = Matrix(Asp) \ b
    B = rand(T, n, 3)
    Xref = Matrix(Asp) \ B

    _sparse_solve_with_scope(scope) do
        DA = distribute(Asp, Blocks(k, k))
        Db = distribute(b, Blocks(k))
        @test Dagger.is_sparse_backed(DA)
        @test !Dagger.is_sparse_backed(distribute(Matrix(Asp), Blocks(k, k)))
        if check_tile !== nothing
            @test check_tile(fetch(DA.chunks[1]; raw=true))
        end

        # `A \ b` — the path packages write.
        x = DA \ Db
        @test x isa Dagger.DVector
        @test collect(x) ≈ xref rtol=cmp_rtol

        # `lu` / `factorize` must not return a dense tiled LU.
        Flu = lu(DA)
        @test !(Flu isa LinearAlgebra.LU)
        Ff = factorize(DA)
        @test !(Ff isa LinearAlgebra.LU)
        if direct_loaded
            @test Flu isa Dagger.DaggerSparseLU
            @test Ff isa Dagger.DaggerSparseLU
        else
            @test Flu isa Dagger.SparseIterativeFactorization
            @test Ff isa Dagger.SparseIterativeFactorization
        end
        @test collect(Flu \ Db) ≈ xref rtol=cmp_rtol
        @test collect(Ff \ Db) ≈ xref rtol=cmp_rtol

        # Explicit pivot forms used to skip the `lu(A)` wrapper and hit dense LU.
        @test !(lu(DA, LinearAlgebra.RowMaximum()) isa LinearAlgebra.LU)
        @test !(lu!(DA, LinearAlgebra.NoPivot()) isa LinearAlgebra.LU)

        # In-place `ldiv!` (2-arg and 3-arg).
        y = similar(Db)
        copyto!(y, Db)
        LinearAlgebra.ldiv!(DA, y)
        @test collect(y) ≈ xref rtol=cmp_rtol

        z = similar(Db)
        LinearAlgebra.ldiv!(z, DA, Db)
        @test collect(z) ≈ xref rtol=cmp_rtol

        z2 = similar(Db)
        LinearAlgebra.ldiv!(z2, Flu, Db)
        @test collect(z2) ≈ xref rtol=cmp_rtol

        # Vector RHS (Dagger stages it as a DVector, same as dense `ldiv`).
        xv = DA \ b
        @test xv isa Dagger.DVector
        @test collect(xv) ≈ xref rtol=cmp_rtol

        # Multi-RHS stays off the dense factor path. The iterative fallback
        # is block-GMRES (`mul!(W, A, P)`), not a gather-and-loop of `A \ b`.
        DB = distribute(B, Blocks(k, k))
        X = DA \ DB
        @test X isa Dagger.DMatrix
        @test collect(X) ≈ Xref rtol=cmp_rtol

        # Operator tiles are still sparse (solve must not densify A).
        @test fetch(DA.chunks[1, 1]) isa Dagger.DSparseArray
        if check_tile !== nothing
            @test check_tile(fetch(DA.chunks[1]; raw=true))
        end
    end
end
