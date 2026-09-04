# Distributed iterative (Krylov) linear-solver tests.
#
# Exercises the matrix-free Krylov integration (`Dagger.cg`/`minres`/`gmres`/
# `bicgstab` + the generic `krylov_solve`) over both dense and sparse-backed
# `DMatrix` operators, plus the Jacobi preconditioner. Reference solutions come
# from a dense direct solve.
#
#     julia test/runtests.jl --test array/linalg/iterativesolvers

using Krylov
using AlgebraicMultigrid
using IncompleteLU

# Strongly diagonally-dominant tridiagonal SPD matrix. The large diagonal keeps
# the condition number small so the Krylov methods converge in a handful of
# iterations (keeping the distributed test fast). `inv(diag) == 1/4`.
const SPD_DIAG = 4.0

laplacian_1d(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(SPD_DIAG), n),
     1 => fill(-one(T), n - 1),
)

# Add a first-order advection term -> nonsymmetric, still well-conditioned.
function advection_diffusion_1d(T, n)
    return laplacian_1d(T, n) + SparseArrays.spdiagm(
        -1 => fill(T(-3) / 10, n - 1),
         1 => fill(T(3) / 10, n - 1),
    )
end

# Per-tile factories for the `Dagger.BlockPreconditioner` tests below. They run
# wherever the tile lives, so they (and `InvDiagOp`) must exist on every worker.
#
# `InvDiagOp` deliberately provides *only* `ldiv!` and does not subtype
# `Factorization`, which is the shape of most third-party preconditioners; the
# `lu` factory covers the `\` convention.
@everywhere import LinearAlgebra
@everywhere struct InvDiagOp{V}
    dinv::V
end
@everywhere LinearAlgebra.ldiv!(y, op::InvDiagOp, x) = (y .= op.dinv .* x; y)
@everywhere inv_diag_factory(tile) =
    InvDiagOp(1 ./ Vector(LinearAlgebra.diag(Dagger._tile_matrix(tile))))
@everywhere tile_lu_factory(tile) = LinearAlgebra.lu(Matrix(Dagger._tile_matrix(tile)))

@testset "Iterative solvers (Krylov)" begin
    n = 64
    k = 16
    Db_part = Blocks(k)
    A_part = Blocks(k, k)

    @testset "SPD operator ($(backend))" for backend in (:dense, :sparse)
        Asp = laplacian_1d(Float64, n)
        Adense = Matrix(Asp)
        b = rand(n)
        xref = Adense \ b

        DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
        Db = distribute(b, Db_part)

        @testset "$(nameof(solver))" for solver in (Dagger.cg, Dagger.minres, Dagger.gmres, Dagger.bicgstab)
            x, stats = solver(DA, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test x isa Dagger.DVector
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # Generic entry point.
        x, stats = Dagger.krylov_solve(:cg, DA, Db; atol = 1e-12, rtol = 1e-10)
        @test stats.solved
        @test collect(x) ≈ xref rtol = 1e-6
    end

    @testset "nonsymmetric operator ($(backend))" for backend in (:dense, :sparse)
        Asp = advection_diffusion_1d(Float64, n)
        b = rand(n)
        xref = Matrix(Asp) \ b

        DA = backend === :dense ? distribute(Matrix(Asp), A_part) : distribute(Asp, A_part)
        Db = distribute(b, Db_part)

        @testset "$(nameof(solver))" for solver in (Dagger.gmres, Dagger.bicgstab)
            x, stats = solver(DA, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    @testset "complex SPD (Hermitian) operator" begin
        # Real SPD tridiagonal is Hermitian as a complex matrix.
        Asp = SparseArrays.spdiagm(
            -1 => fill(ComplexF64(-1), n - 1),
             0 => fill(ComplexF64(SPD_DIAG), n),
             1 => fill(ComplexF64(-1), n - 1),
        )
        b = rand(ComplexF64, n)
        xref = Matrix(Asp) \ b
        DA = distribute(Asp, A_part)
        Db = distribute(b, Db_part)
        x, stats = Dagger.cg(DA, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test stats.solved
        @test collect(x) ≈ xref rtol = 1e-6
    end

    @testset "Jacobi preconditioner" begin
        Asp = laplacian_1d(Float64, n)
        b = rand(n)
        xref = Matrix(Asp) \ b

        @testset "build + apply ($(backend))" for backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Matrix(Asp), A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            P = Dagger.JacobiPreconditioner(DA)
            @test collect(P.dinv) ≈ fill(1 / SPD_DIAG, n)   # 1/diag

            # Apply: y = M⁻¹ x = dinv .* x.
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ (1 / SPD_DIAG) .* b

            x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # A non-square block grid is re-tiled, not rejected: the diagonal is the
        # same either way, so the result must be identical to the square case.
        DA_ragged = distribute(Matrix(Asp), Blocks(k, k ÷ 2))
        P_ragged = Dagger.JacobiPreconditioner(DA_ragged)
        @test collect(P_ragged.dinv) ≈ fill(1 / SPD_DIAG, n)
    end

    @testset "block-Jacobi preconditioner" begin
        Asp = laplacian_1d(Float64, n)
        Adense = Matrix(Asp)
        b = rand(n)
        xref = Adense \ b

        # Reference: apply the exact block-diagonal inverse.
        yref = similar(b)
        for s in 1:k:n
            r = s:min(s + k - 1, n)
            yref[r] = Adense[r, r] \ b[r]
        end

        @testset "build + apply ($(backend))" for backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            P = Dagger.BlockJacobiPreconditioner(DA)
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ yref

            x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # A single tile makes block-Jacobi an *exact* solve, so PCG converges
        # essentially immediately.
        DA1 = distribute(Adense, Blocks(n, n))
        Db1 = distribute(b, Blocks(n))
        P1 = Dagger.BlockJacobiPreconditioner(DA1)
        x1, s1 = Dagger.cg(DA1, Db1; M = P1, atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test s1.solved
        @test s1.niter <= 2
        @test collect(x1) ≈ xref rtol = 1e-8

        # A non-square block grid is re-tiled to `Blocks(k÷2, k÷2)` rather than
        # rejected, so the blocks are the *finer* ones -- check against those.
        yref_fine = similar(b)
        for s in 1:(k ÷ 2):n
            r = s:min(s + (k ÷ 2) - 1, n)
            yref_fine[r] = Adense[r, r] \ b[r]
        end
        DA_ragged = distribute(Adense, Blocks(k, k ÷ 2))
        P_ragged = Dagger.BlockJacobiPreconditioner(DA_ragged)
        Db_ragged = distribute(b, Blocks(k ÷ 2))
        y_ragged = similar(Db_ragged)
        mul!(y_ragged, P_ragged, Db_ragged)
        @test collect(y_ragged) ≈ yref_fine

        x_ragged, s_ragged = Dagger.cg(DA_ragged, Db_ragged; M = P_ragged,
                                       atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test s_ragged.solved
        @test collect(x_ragged) ≈ xref rtol = 1e-6
    end

    # `BlockPreconditioner` is the public form of the machinery every bundled
    # block preconditioner uses: hand it a per-tile factory and nothing else.
    # Both of Dagger's apply conventions are covered -- `tile_lu_factory`
    # returns a `Factorization` (applied with `\`) and `inv_diag_factory` an
    # `ldiv!`-only object, which is the shape most third-party preconditioners
    # (KrylovPreconditioners included) have.
    @testset "BlockPreconditioner (user-supplied per-tile factory)" begin
        Asp = laplacian_1d(Float64, n)
        Adense = Matrix(Asp)
        b = rand(n)
        xref = Adense \ b

        yref_lu = similar(b)
        for s in 1:k:n
            r = s:min(s + k - 1, n)
            yref_lu[r] = Adense[r, r] \ b[r]
        end

        factories = (
            ("lu (Factorization, `\\`)", tile_lu_factory, yref_lu),
            ("inv-diag (`ldiv!` only)", inv_diag_factory, (1 / SPD_DIAG) .* b),
        )

        @testset "$(name) ($(backend))" for (name, build, yref) in factories,
                                            backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            P = Dagger.BlockPreconditioner(DA, build)
            @test P isa Dagger.AbstractBlockPreconditioner
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ yref

            x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    @testset "AMG preconditioner ($(method))" for method in (:ruge_stuben, :smoothed_aggregation)
        Asp = laplacian_1d(Float64, n)
        Adense = Matrix(Asp)
        b = rand(n)
        xref = Adense \ b

        @testset "$(backend)" for backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            P = Dagger.AMGPreconditioner(DA; method = method)
            # Apply is a V-cycle approximating M⁻¹ x; just check it runs + is finite
            # (and repeatable, exercising the cached, pinned hierarchy).
            y1 = similar(Db); mul!(y1, P, Db)
            y2 = similar(Db); mul!(y2, P, Db)
            @test all(isfinite, collect(y1))
            @test collect(y1) ≈ collect(y2)

            x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    @testset "block-ILU preconditioner" begin
        Asp = laplacian_1d(Float64, n)
        Adense = Matrix(Asp)
        b = rand(n)
        xref = Adense \ b

        @testset "build + apply ($(backend))" for backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            P = Dagger.BlockILUPreconditioner(DA; τ = 0.01)
            y1 = similar(Db); mul!(y1, P, Db)
            y2 = similar(Db); mul!(y2, P, Db)
            @test all(isfinite, collect(y1))
            @test collect(y1) ≈ collect(y2)

            x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # A non-square block grid is re-tiled rather than rejected.
        DA_ragged = distribute(Adense, Blocks(k, k ÷ 2))
        P_ragged = Dagger.BlockILUPreconditioner(DA_ragged; τ = 0.01)
        Db_ragged = distribute(b, Blocks(k ÷ 2))
        x_ragged, s_ragged = Dagger.cg(DA_ragged, Db_ragged; M = P_ragged,
                                       atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test s_ragged.solved
        @test collect(x_ragged) ≈ xref rtol = 1e-6
    end
end

# Krylov.jl's own entry points, called directly on Dagger arrays.
#
# This is the interop that matters for portability: an application already
# written against Krylov.jl should run on Dagger by passing Dagger arrays in,
# with no `A isa DArray ? Dagger.cg(...) : Krylov.cg(...)` branch. Krylov's
# methods are generic over the vector type; the only piece that is not is
# workspace allocation, which `ext/KrylovExt.jl` routes through a
# `KrylovConstructor` so every internal vector is `similar(b)`.
@testset "Direct Krylov.jl entry points" begin
    n = 64
    k = 16
    A_part = Blocks(k, k)
    b_part = Blocks(k)

    Asp = laplacian_1d(Float64, n)
    Adense = Matrix(Asp)
    b = rand(n)
    xref = Adense \ b

    # Every square method Krylov exposes for a general/SPD operator. They differ
    # in what they assume about `A`, but the Laplacian is SPD so all apply.
    SQUARE_METHODS = (:cg, :cr, :car, :minres, :minares, :minres_qlp, :symmlq,
                      :cg_lanczos, :gmres, :fgmres, :fom, :diom, :dqgmres,
                      :bicgstab, :cgs, :bilq, :qmr)

    @testset "$(backend) operator" for backend in (:dense, :sparse)
        DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
        Db = distribute(b, b_part)

        @testset "Krylov.$(method)" for method in SQUARE_METHODS
            x, stats = getfield(Krylov, method)(DA, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test x isa Dagger.DVector
            # Workspace vectors are `similar(b)`, so the solution comes back
            # with `b`'s partitioning rather than a repartitioned copy.
            @test x.partitioning == Db.partitioning
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    DA = distribute(Asp, A_part)
    Db = distribute(b, b_part)

    @testset "generic entry points" begin
        for solve in (() -> Krylov.krylov_solve(Val(:cg), DA, Db),
                      () -> Krylov.krylov_solve(:gmres, DA, Db))
            x, stats = solve()
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # Pre-allocated workspace reused across solves, the shape a real
        # application uses to avoid re-allocating every step.
        ws = Krylov.krylov_workspace(Val(:cg), DA, Db)
        Krylov.cg!(ws, DA, Db; atol = 1e-12, rtol = 1e-10)
        @test Krylov.statistics(ws).solved
        @test collect(Krylov.solution(ws)) ≈ xref rtol = 1e-6
        Krylov.cg!(ws, DA, Db; atol = 1e-12, rtol = 1e-10)
        @test collect(Krylov.solution(ws)) ≈ xref rtol = 1e-6

        # Warm start from an existing DVector.
        x0 = similar(Db); fill!(x0, 0)
        x, stats = Krylov.cg(DA, Db, x0; atol = 1e-12, rtol = 1e-10)
        @test collect(x) ≈ xref rtol = 1e-6
    end

    @testset "Dagger preconditioners via the M keyword" begin
        @testset "$(nameof(typeof(P)))" for P in (Dagger.JacobiPreconditioner(DA),
                                                  Dagger.BlockJacobiPreconditioner(DA))
            x, stats = Krylov.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6

            x, stats = Krylov.gmres(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    # The least-squares/least-norm methods need a second workspace prototype of
    # length `size(A, 2)`, partitioned like `A`'s columns.
    @testset "rectangular (least-squares) methods" begin
        m = 96
        Ah = rand(m, n) + [i == j ? 5.0 : 0.0 for i in 1:m, j in 1:n]
        ch = rand(m)
        lsref = Ah \ ch

        DR = distribute(Ah, A_part)
        Dc = distribute(ch, b_part)

        @testset "Krylov.$(method)" for method in (:lsqr, :lsmr, :lslq, :cgls, :crls)
            x, stats = getfield(Krylov, method)(DR, Dc)
            @test collect(x) ≈ lsref rtol = 1e-5
        end

        # An adjoint operator exposes its column partitioning through the parent.
        x, _ = Krylov.lsqr(distribute(permutedims(Ah), Blocks(k, k))', Dc)
        @test collect(x) ≈ lsref rtol = 1e-5
    end
end

# Matrix-free operators
#
# The solvers never form `A`; they only need `mul!(y, A, x)` over `DVector`s.
# These two operators cover the shapes `docs/src/iterative-solving.md` promises:
# one built from distributed Dagger tasks over `x`'s tiles (the
# `MyStencilOperator` case), and one built purely from distributed BLAS-1
# vector operations.

# 1D Laplacian applied tile-by-tile, with a halo exchange between neighboring
# tiles. No matrix exists anywhere, and each tile's update is a Dagger task
# whose neighbor reads are ordered by Datadeps.
struct TiledLaplacian
    n::Int
    diag::Float64
end
Base.size(A::TiledLaplacian) = (A.n, A.n)
Base.size(A::TiledLaplacian, d::Integer) = d <= 2 ? A.n : 1
Base.eltype(::TiledLaplacian) = Float64

# The per-tile kernel runs wherever the scheduler places it, so it must exist on
# every worker.
@everywhere function laplacian_tile!(y, x, left, right, diag)
    nx = length(x)
    @inbounds for i in 1:nx
        lo = i == 1 ? (left === nothing ? 0.0 : left[end]) : x[i-1]
        hi = i == nx ? (right === nothing ? 0.0 : right[begin]) : x[i+1]
        y[i] = diag * x[i] - lo - hi
    end
    return
end

function LinearAlgebra.mul!(y::Dagger.DVector, A::TiledLaplacian, x::Dagger.DVector)
    xc, yc = x.chunks, y.chunks
    @assert length(xc) == length(yc) "operator requires matching partitionings"
    nt = length(xc)
    Dagger.spawn_datadeps() do
        for i in 1:nt
            left = i > 1 ? In(xc[i-1]) : nothing
            right = i < nt ? In(xc[i+1]) : nothing
            Dagger.@spawn laplacian_tile!(Out(yc[i]), In(xc[i]), left, right, A.diag)
        end
    end
    return y
end

# Diagonal plus a symmetric rank-one term, expressed only through distributed
# vector primitives. SPD for positive `d`, and indifferent to how `x` and `y`
# are partitioned.
struct DiagPlusRankOne{V}
    d::V
    u::V
end
Base.size(A::DiagPlusRankOne) = (length(A.d), length(A.d))
Base.size(A::DiagPlusRankOne, i::Integer) = i <= 2 ? length(A.d) : 1
Base.eltype(A::DiagPlusRankOne) = eltype(A.d)

function LinearAlgebra.mul!(y::Dagger.DVector, A::DiagPlusRankOne, x::Dagger.DVector)
    y .= A.d .* x
    axpy!(dot(A.u, x), A.u, y)
    return y
end

@testset "Matrix-free operators" begin
    n = 64
    k = 16
    b = rand(n)

    @testset "tiled stencil (Dagger tasks + datadeps)" begin
        A = TiledLaplacian(n, SPD_DIAG)
        Aref = Matrix(laplacian_1d(Float64, n))
        xref = Aref \ b

        @testset "blocks of $bs" for bs in (k, n, 7)
            Db = distribute(b, Blocks(bs))

            # `mul!` itself matches the dense reference.
            y = similar(Db)
            mul!(y, A, Db)
            @test collect(y) ≈ Aref * b

            @testset "Krylov.$(method)" for method in (:cg, :minres, :gmres, :bicgstab)
                x, stats = getfield(Krylov, method)(A, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
                @test stats.solved
                @test collect(x) ≈ xref rtol = 1e-6
            end

            # And through Dagger's own wrappers.
            x, stats = Dagger.cg(A, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    @testset "diagonal + rank-one (vector primitives)" begin
        d = rand(n) .+ 2
        u = rand(n) ./ 8
        Aref = Diagonal(d) + u * u'
        xref = Aref \ b

        A = DiagPlusRankOne(distribute(d, Blocks(k)), distribute(u, Blocks(k)))
        Db = distribute(b, Blocks(k))

        y = similar(Db)
        mul!(y, A, Db)
        @test collect(y) ≈ Aref * b

        @testset "Krylov.$(method)" for method in (:cg, :minres, :cr)
            x, stats = getfield(Krylov, method)(A, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    # A matrix-free *rectangular* operator cannot expose a column partitioning,
    # so the least-squares workspaces cannot be built automatically. That must
    # fail with an actionable message rather than a `MethodError`.
    @testset "rectangular matrix-free is rejected with guidance" begin
        A = TiledLaplacian(n, SPD_DIAG)
        Db = distribute(b, Blocks(k))
        @test_throws ArgumentError Krylov.lsqr(A, Db)
    end
end
