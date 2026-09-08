# Geometric multigrid tests.
#
# `GeometricMultigrid` is the structured-grid counterpart of `GlobalAMG`:
# injection / full-weighting `R`, linear / bilinear `P`, distributed Galerkin
# `Ac = R A P`, V-cycle via `mul!`. `@stencil` is not used — see AGENTS.md
# lesson 35. `AMGPreconditioner` / `GlobalAMG` semantics are not touched.
#
#     julia test/runtests.jl --test array/linalg/gmg

using Krylov
using AlgebraicMultigrid

# True 1-D / 2-D Poisson (diag 2). Same fixtures as `global_amg.jl`.
poisson_1d(n) = SparseArrays.spdiagm(
    -1 => fill(-1.0, n - 1),
     0 => fill(2.0, n),
     1 => fill(-1.0, n - 1),
)

function poisson_2d(nx, ny)
    Tx = poisson_1d(nx)
    Ty = poisson_1d(ny)
    Ix = SparseArrays.spdiagm(0 => ones(nx))
    Iy = SparseArrays.spdiagm(0 => ones(ny))
    return SparseArrays.kron(Iy, Tx) + SparseArrays.kron(Ty, Ix)
end

function true_relres(A, x, b)
    r = similar(b)
    mul!(r, A, x)
    axpy!(-one(eltype(b)), b, r)
    return LinearAlgebra.norm2(r) / LinearAlgebra.norm2(b)
end

function solve_gmres(DA, Db, M; itmax=400, rtol=1e-10)
    n = length(Db)
    x, stats = Krylov.gmres(DA, Db; M=M, atol=1e-14, rtol=rtol, itmax=itmax,
                            memory=min(n, 80))
    return x, stats, true_relres(DA, x, Db)
end

function jacobi_only_relres(A, b, relax, nsweeps)
    dinv = 1.0 ./ diag(A)
    u = zeros(length(b))
    ω = Float64(relax)
    for _ in 1:nsweeps
        u .+= ω .* dinv .* (b .- A * u)
    end
    return LinearAlgebra.norm2(A * u - b) / LinearAlgebra.norm2(b)
end

@testset "Geometric multigrid" begin
    @testset "1-D Poisson n=$n tiles=$nt" for n in (64, 128), nt in (2, 4, 8)
        k = n ÷ nt
        A = poisson_1d(n)
        b = rand(n)
        DA = distribute(A, Blocks(k, k))
        Db = distribute(b, Blocks(k))

        M = Dagger.GeometricMultigrid(DA; max_levels=3, max_coarse=16)
        @test M isa Dagger.GeometricMultigrid
        @test M.restriction === :full_weighting
        @test M.prolongation === :linear
        @test M.grid == (n,)
        @test !isempty(M.levels)

        L = M.levels[end]
        Ah = Dagger._collect_sparse_dmatrix(L.A)
        Rh = Dagger._collect_sparse_dmatrix(L.R)
        Ph = Dagger._collect_sparse_dmatrix(L.P)
        Ach = Dagger._collect_sparse_dmatrix(M.coarse_A)
        @test size(Rh, 2) == size(Ah, 1)
        @test size(Ph, 1) == size(Ah, 1)
        @test size(Ach) == (size(Rh, 1), size(Ph, 2))
        @test Ach ≈ Rh * Ah * Ph rtol = 1e-8 atol = 1e-9
        if length(M.levels) >= 2
            A2 = Dagger._collect_sparse_dmatrix(M.levels[2].A)
            R1 = Dagger._collect_sparse_dmatrix(M.levels[1].R)
            P1 = Dagger._collect_sparse_dmatrix(M.levels[1].P)
            A1 = Dagger._collect_sparse_dmatrix(M.levels[1].A)
            @test A2 ≈ R1 * A1 * P1 rtol = 1e-8 atol = 1e-9
        end

        # Host COO matches the assembled finest transfer.
        I, J, V, nc, nR = Dagger._gmg_restriction_coo(Float64, (n,), :full_weighting)
        Rref = SparseArrays.sparse(I, J, V, nc, nR)
        @test Dagger._collect_sparse_dmatrix(M.levels[1].R) ≈ Rref rtol = 1e-12

        y = similar(Db)
        mul!(y, M, Db)
        @test all(isfinite, collect(y))
        vrel = true_relres(DA, y, Db)
        jrel = jacobi_only_relres(A, b, M.relax, M.presweeps + M.postsweeps)
        @test vrel < 0.95 * jrel
        @test vrel < 0.5

        x, stats, rel = solve_gmres(DA, Db, M)
        @test rel < 1e-6
        @test collect(x) ≈ Matrix(A) \ b rtol = 1e-5

        Pj = Dagger.JacobiPreconditioner(DA)
        _, st_j, rel_j = solve_gmres(DA, Db, Pj; itmax=400)
        _, st_g, rel_g = solve_gmres(DA, Db, M; itmax=400)
        @test rel_g < 1e-6
        @test st_g.niter < st_j.niter
    end

    @testset "2-D 5-point Poisson $(nx)×$(ny) tiles=$nt" for (nx, ny, nt) in (
            (8, 8, 2), (8, 8, 4), (16, 16, 4), (16, 16, 8))
        n = nx * ny
        k = n ÷ nt
        k * nt == n || continue
        A = poisson_2d(nx, ny)
        b = rand(n)
        DA = distribute(A, Blocks(k, k))
        Db = distribute(b, Blocks(k))

        M = Dagger.GeometricMultigrid(DA; grid=(nx, ny), max_levels=3, max_coarse=16)
        @test M.grid == (nx, ny)
        @test M.prolongation === :bilinear
        @test !isempty(M.levels)

        L = M.levels[1]
        Ah = Dagger._collect_sparse_dmatrix(L.A)
        Rh = Dagger._collect_sparse_dmatrix(L.R)
        Ph = Dagger._collect_sparse_dmatrix(L.P)
        A2 = length(M.levels) >= 2 ? Dagger._collect_sparse_dmatrix(M.levels[2].A) :
             Dagger._collect_sparse_dmatrix(M.coarse_A)
        @test A2 ≈ Rh * Ah * Ph rtol = 1e-8 atol = 1e-9

        y = similar(Db)
        mul!(y, M, Db)
        vrel = true_relres(DA, y, Db)
        jrel = jacobi_only_relres(A, b, M.relax, M.presweeps + M.postsweeps)
        @test vrel < 0.95 * jrel

        x, stats, rel = solve_gmres(DA, Db, M)
        @test rel < 1e-6
        @test collect(x) ≈ Matrix(A) \ b rtol = 1e-5

        Pj = Dagger.JacobiPreconditioner(DA)
        _, st_j, _ = solve_gmres(DA, Db, Pj; itmax=400)
        _, st_g, rel_g = solve_gmres(DA, Db, M; itmax=400)
        @test rel_g < 1e-6
        @test st_g.niter < st_j.niter
    end

    @testset "injection restriction still coarsens" begin
        n, k = 64, 16
        A = poisson_1d(n)
        DA = distribute(A, Blocks(k, k))
        Db = distribute(rand(n), Blocks(k))
        M = Dagger.GeometricMultigrid(DA; restriction=:injection, prolongation=:linear,
                                      max_levels=3, max_coarse=16)
        @test M.restriction === :injection
        @test !isempty(M.levels)
        _, _, rel = solve_gmres(DA, Db, M)
        @test rel < 1e-6
    end

    @testset "user-supplied R and P" begin
        n, k = 64, 16
        A = poisson_1d(n)
        DA = distribute(A, Blocks(k, k))
        Db = distribute(rand(n), Blocks(k))
        I, J, V, nc, nR = Dagger._gmg_restriction_coo(Float64, (n,), :full_weighting)
        Ip, Jp, Vp, nP, ncP = Dagger._gmg_prolongation_coo(Float64, (n,), :linear)
        R = SparseArrays.sparse(I, J, V, nc, nR)
        P = SparseArrays.sparse(Ip, Jp, Vp, nP, ncP)
        M = Dagger.GeometricMultigrid(DA, R, P; max_levels=3, max_coarse=16)
        @test M.restriction === :user
        @test M.prolongation === :user
        @test !isempty(M.levels)
        @test Dagger._collect_sparse_dmatrix(M.levels[1].R) ≈ R
        @test Dagger._collect_sparse_dmatrix(M.levels[1].P) ≈ P
        _, _, rel = solve_gmres(DA, Db, M)
        @test rel < 1e-6
    end

    @testset "AMGPreconditioner and GlobalAMG are unchanged" begin
        n, k = 64, 16
        DA = distribute(poisson_1d(n), Blocks(k, k))
        P = Dagger.AMGPreconditioner(DA)
        @test P isa Dagger.AMGPreconditioner
        @test P isa Dagger.AbstractBlockPreconditioner
        @test length(P.ops) == n ÷ k
        G = Dagger.GlobalAMG(DA; max_levels=2, max_coarse=32)
        @test G isa Dagger.GlobalAMG
        @test !(G isa Dagger.GeometricMultigrid)
    end
end
