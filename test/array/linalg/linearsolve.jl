# LinearSolve.jl integration: `solve(LinearProblem(A, b), alg)` on Dagger arrays.
#
#     julia test/runtests.jl --test array/linalg/linearsolve

using LinearSolve
using Krylov
using PureKLU
using PureUMFPACK

const LSExt = Base.get_extension(Dagger, :LinearSolveExt)

function laplacian_1d(T, n)
    return SparseArrays.spdiagm(
        -1 => fill(-one(T), n - 1),
         0 => fill(T(4), n),
         1 => fill(-one(T), n - 1),
    )
end

function advection_diffusion_1d(T, n)
    return laplacian_1d(T, n) + SparseArrays.spdiagm(
        -1 => fill(T(-3) / 10, n - 1),
         1 => fill(T(3) / 10, n - 1),
    )
end

@testset "LinearSolve extension loaded" begin
    @test LSExt !== nothing
end

@testset "defaultalg dispatch" begin
    n = 32
    k = 16
    A_part = Blocks(k, k)
    b_part = Blocks(k)
    b = rand(n)
    Db = distribute(b, b_part)
    assump = LinearSolve.OperatorAssumptions(true)

    DA_dense = distribute(rand(n, n) + n * I, A_part)
    alg_dense = LinearSolve.defaultalg(DA_dense, Db, assump)
    @test alg_dense isa LinearSolve.KrylovJL
    @test LinearSolve.needs_concrete_A(alg_dense) == false

    Asp = laplacian_1d(Float64, n)
    DA_sparse = distribute(Asp, A_part)
    alg_sparse = LinearSolve.defaultalg(DA_sparse, Db, assump)
    @test alg_sparse isa Union{LinearSolve.PureKLUFactorization,
                               LinearSolve.PureUMFPACKFactorization}
    @test LSExt._fits_sparse_direct(DA_sparse)
    @test !LSExt._fits_sparse_direct(DA_dense)

    # Matrix-free + DVector: Krylov, not DefaultLinearSolver.
    struct _SizeOnly
        n::Int
    end
    Base.size(A::_SizeOnly) = (A.n, A.n)
    Base.size(A::_SizeOnly, d::Integer) = d <= 2 ? A.n : 1
    alg_mf = LinearSolve.defaultalg(_SizeOnly(n), Db, assump)
    @test alg_mf isa LinearSolve.KrylovJL
    @test !(alg_mf isa LinearSolve.DefaultLinearSolver)
end

@testset "dense DMatrix via LinearSolve" begin
    n = 32
    k = 16
    Asp = laplacian_1d(Float64, n)
    Adense = Matrix(Asp)
    b = rand(n)
    xref = Adense \ b
    DA = distribute(Adense, Blocks(k, k))
    Db = distribute(b, Blocks(k))

    # Default: Krylov on dense tiles.
    sol = LinearSolve.solve(LinearProblem(DA, Db); abstol = 1e-12, reltol = 1e-10)
    @test sol.retcode == ReturnCode.Success
    @test sol.u isa Dagger.DVector
    @test collect(sol.u) ≈ xref rtol = 1e-6

    sol_g = LinearSolve.solve(LinearProblem(DA, Db), KrylovJL_GMRES();
        abstol = 1e-12, reltol = 1e-10)
    @test collect(sol_g.u) ≈ xref rtol = 1e-6

    sol_cg = LinearSolve.solve(LinearProblem(DA, Db), KrylovJL_CG();
        abstol = 1e-12, reltol = 1e-10)
    @test collect(sol_cg.u) ≈ xref rtol = 1e-6
end

@testset "sparse DMatrix via LinearSolve" begin
    n = 32
    k = 16
    Asp = advection_diffusion_1d(Float64, n)
    b = rand(n)
    xref = Matrix(Asp) \ b
    DA = distribute(Asp, Blocks(k, k))
    Db = distribute(b, Blocks(k))

    # Default: gathered sparse direct (n << SPARSE_DIRECT_MAX_N).
    sol = LinearSolve.solve(LinearProblem(DA, Db))
    @test sol.retcode == ReturnCode.Success
    @test sol.u isa Dagger.DVector
    @test collect(sol.u) ≈ xref rtol = 1e-8

    sol_klu = LinearSolve.solve(LinearProblem(DA, Db), PureKLUFactorization())
    @test collect(sol_klu.u) ≈ xref rtol = 1e-8

    sol_umf = LinearSolve.solve(LinearProblem(DA, Db), PureUMFPACKFactorization())
    @test collect(sol_umf.u) ≈ xref rtol = 1e-8

    # Explicit Krylov still works on the sparse tiles.
    sol_k = LinearSolve.solve(LinearProblem(DA, Db), KrylovJL_GMRES();
        abstol = 1e-12, reltol = 1e-10)
    @test collect(sol_k.u) ≈ xref rtol = 1e-6
end

@testset "multi-RHS DMatrix via LinearSolve" begin
    n = 32
    k = 16
    p = 3
    Asp = advection_diffusion_1d(Float64, n)
    B = rand(n, p)
    Xref = Matrix(Asp) \ B
    DA = distribute(Asp, Blocks(k, k))
    DB = distribute(B, Blocks(k, k))
    assump = LinearSolve.OperatorAssumptions(true)

    # Sparse + multi-RHS default is still the gathered direct factor.
    alg = LinearSolve.defaultalg(DA, DB, assump)
    @test alg isa Union{LinearSolve.PureKLUFactorization,
                        LinearSolve.PureUMFPACKFactorization}
    sol = LinearSolve.solve(LinearProblem(DA, DB))
    @test sol.retcode == ReturnCode.Success
    @test sol.u isa Dagger.DMatrix
    @test collect(sol.u) ≈ Xref rtol = 1e-8

    sol_k = LinearSolve.solve(LinearProblem(DA, DB), KrylovJL_GMRES();
        abstol = 1e-12, reltol = 1e-10)
    @test sol_k.u isa Dagger.DMatrix
    @test collect(sol_k.u) ≈ Xref rtol = 1e-6

    DA_dense = distribute(Matrix(Asp), Blocks(k, k))
    alg_d = LinearSolve.defaultalg(DA_dense, DB, assump)
    @test alg_d isa LinearSolve.KrylovJL
    sol_d = LinearSolve.solve(LinearProblem(DA_dense, DB), KrylovJL_GMRES();
        abstol = 1e-12, reltol = 1e-10)
    @test collect(sol_d.u) ≈ Xref rtol = 1e-6

    S = Matrix(laplacian_1d(Float64, n))
    Bspd = rand(n, p)
    DS = distribute(S, Blocks(k, k))
    DBspd = distribute(Bspd, Blocks(k, k))
    sol_m = LinearSolve.solve(LinearProblem(DS, DBspd), KrylovJL_MINRES();
        abstol = 1e-12, reltol = 1e-10)
    @test collect(sol_m.u) ≈ S \ Bspd rtol = 1e-6
end

@testset "cache reuse and Dagger preconditioner as Pl" begin
    n = 32
    k = 16
    Asp = laplacian_1d(Float64, n)
    Adense = Matrix(Asp)
    b = rand(n)
    DA = distribute(Adense, Blocks(k, k))
    Db = distribute(b, Blocks(k))

    cache = LinearSolve.init(LinearProblem(DA, Db), KrylovJL_CG();
        abstol = 1e-12, reltol = 1e-10)
    sol1 = LinearSolve.solve!(cache)
    @test collect(sol1.u) ≈ Adense \ b rtol = 1e-6

    b2 = rand(n)
    cache.b = distribute(b2, Blocks(k))
    sol2 = LinearSolve.solve!(cache)
    @test collect(sol2.u) ≈ Adense \ b2 rtol = 1e-6

    P = Dagger.JacobiPreconditioner(DA)
    sol_p = LinearSolve.solve(LinearProblem(DA, Db), KrylovJL_CG();
        Pl = P, abstol = 1e-12, reltol = 1e-10)
    @test collect(sol_p.u) ≈ Adense \ b rtol = 1e-6
end

# Matrix-free: `LinearProblem` only accepts `AbstractMatrix` / SciML operators
# as `A` (anything else is treated as an ODE-style `f`). Subtype `AbstractMatrix`
# so SciML stores the operator, then implement only `mul!` — no entries.
struct DiagOp{T} <: AbstractMatrix{T}
    d::Dagger.DVector{T}
end
Base.size(A::DiagOp) = (length(A.d), length(A.d))
function LinearAlgebra.mul!(y::Dagger.DVector, A::DiagOp, x::Dagger.DVector)
    y .= A.d .* x
    return y
end

@testset "matrix-free operator via LinearSolve" begin
    n = 32
    k = 16
    d = rand(n) .+ 2
    b = rand(n)
    xref = d .\ b
    A = DiagOp(distribute(d, Blocks(k)))
    Db = distribute(b, Blocks(k))

    sol = LinearSolve.solve(LinearProblem(A, Db); abstol = 1e-12, reltol = 1e-10)
    @test sol.retcode == ReturnCode.Success
    @test collect(sol.u) ≈ xref rtol = 1e-6

    sol_e = LinearSolve.solve(LinearProblem(A, Db), KrylovJL_CG();
        abstol = 1e-12, reltol = 1e-10)
    @test collect(sol_e.u) ≈ xref rtol = 1e-6
end
