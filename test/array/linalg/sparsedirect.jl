# Sparse direct-solver tests (KLU via PureKLU, UMFPACK-style via PureUMFPACK).
#
# Exercises the whole-matrix direct solves (`Dagger.klu` / `Dagger.splu`) and the
# per-tile block direct preconditioners (`BlockKLUPreconditioner` /
# `BlockUMFPACKPreconditioner`). Reference solutions come from a dense direct
# solve.
#
#     julia test/runtests.jl --test array/linalg/sparsedirect

using PureKLU
using PureUMFPACK
using Metis
using Krylov

# A nonsymmetric, well-conditioned sparse system (advection-diffusion-like).
function nonsym_1d(T, n)
    return SparseArrays.spdiagm(
        -1 => fill(-one(T), n - 1),
         0 => fill(T(3), n),
         1 => fill(T(-7) / 10, n - 1),
    )
end

# SPD, diagonally dominant tridiagonal (stable under pivoting).
function spd_1d(T, n)
    return SparseArrays.spdiagm(
        -1 => fill(-one(T), n - 1),
         0 => fill(T(4), n),
         1 => fill(-one(T), n - 1),
    )
end

# 2D 5-point Laplacian on an N×N grid (SPD; clean METIS separators).
function lap2d(T, N)
    n = N * N
    Is = Int[]; Js = Int[]; Vs = T[]
    idx(i, j) = (j - 1) * N + i
    for j in 1:N, i in 1:N
        k = idx(i, j)
        push!(Is, k); push!(Js, k); push!(Vs, T(4))
        if i > 1; push!(Is, k); push!(Js, idx(i - 1, j)); push!(Vs, -one(T)); end
        if i < N; push!(Is, k); push!(Js, idx(i + 1, j)); push!(Vs, -one(T)); end
        if j > 1; push!(Is, k); push!(Js, idx(i, j - 1)); push!(Vs, -one(T)); end
        if j < N; push!(Is, k); push!(Js, idx(i, j + 1)); push!(Vs, -one(T)); end
    end
    return SparseArrays.sparse(Is, Js, Vs, n, n)
end

# Nonsymmetric, diagonally dominant 2D advection-diffusion stencil.
function advdiff2d(T, N)
    n = N * N
    Is = Int[]; Js = Int[]; Vs = T[]
    idx(i, j) = (j - 1) * N + i
    for j in 1:N, i in 1:N
        k = idx(i, j)
        push!(Is, k); push!(Js, k); push!(Vs, T(5))
        if i > 1; push!(Is, k); push!(Js, idx(i - 1, j)); push!(Vs, T(-12) / 10); end
        if i < N; push!(Is, k); push!(Js, idx(i + 1, j)); push!(Vs, T(-8) / 10); end
        if j > 1; push!(Is, k); push!(Js, idx(i, j - 1)); push!(Vs, T(-11) / 10); end
        if j < N; push!(Is, k); push!(Js, idx(i, j + 1)); push!(Vs, T(-9) / 10); end
    end
    return SparseArrays.sparse(Is, Js, Vs, n, n)
end

@testset "Sparse direct solvers" begin
    n = 64
    k = 16
    Db_part = Blocks(k)
    A_part = Blocks(k, k)

    Asp = nonsym_1d(Float64, n)
    Adense = Matrix(Asp)
    b = rand(n)
    xref = Adense \ b

    @testset "whole-matrix direct ($(solver)) — $(backend)" for solver in (:klu, :splu),
                                                                backend in (:dense, :sparse, :singletile)
        DA = if backend === :dense
            distribute(Adense, A_part)
        elseif backend === :sparse
            distribute(Asp, A_part)
        else
            distribute(Asp, Blocks(n, n))   # single tile
        end
        Db = distribute(b, Db_part)

        F = solver === :klu ? Dagger.klu(DA) : Dagger.splu(DA)
        @test size(F) == (n, n)

        # `F \ b` returns a DVector partitioned like `b`.
        x = F \ Db
        @test x isa Dagger.DVector
        @test collect(x) ≈ xref

        # In-place `ldiv!` writes into a preallocated DVector.
        y = similar(Db)
        LinearAlgebra.ldiv!(y, F, Db)
        @test collect(y) ≈ xref

        # Dimension mismatch is caught.
        @test_throws DimensionMismatch F \ distribute(rand(n ÷ 2), Blocks(k))
    end

    @testset "block direct preconditioner ($(name))" for (name, build) in (
        ("KLU", Dagger.BlockKLUPreconditioner),
        ("UMFPACK", Dagger.BlockUMFPACKPreconditioner),
    )
        @testset "build + apply ($(backend))" for backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            # Reference: apply the exact block-diagonal inverse.
            yref = similar(b)
            for s in 1:k:n
                r = s:min(s + k - 1, n)
                yref[r] = Adense[r, r] \ b[r]
            end

            P = build(DA)
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ yref
            # Repeatable (exercises the cached, pinned factorization).
            y2 = similar(Db)
            mul!(y2, P, Db)
            @test collect(y2) ≈ yref

            x, stats = Dagger.bicgstab(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # A single tile makes the block solve an *exact* whole-matrix solve.
        DA1 = distribute(Asp, Blocks(n, n))
        Db1 = distribute(b, Blocks(n))
        P1 = build(DA1)
        y1 = similar(Db1)
        mul!(y1, P1, Db1)
        @test collect(y1) ≈ xref

        # Non-square block grid must be rejected with a helpful error.
        @test_throws ArgumentError build(distribute(Adense, Blocks(k, k ÷ 2)))
    end

    @testset "distributed triangular solve (splu)" begin
        systems = (
            ("nonsym", nonsym_1d(Float64, n)),
            ("spd", spd_1d(Float64, n)),
        )
        # Include a case where A's tiling differs from the solve blocksize.
        configs = (
            (Blocks(k, k), nothing),          # default: match A's blocks
            (Blocks(k, k), 8),                # finer solve tiles than A
            (Blocks(32, 16), 16),             # non-square A tiling → square solve
            (Blocks(n, n), 16),               # single-tile A, multi-tile solve
        )
        for (name, Asp_sys) in systems, (A_part_cfg, solve_bs) in configs
            @testset "$name part=$(A_part_cfg.blocksize) bs=$(solve_bs)" begin
                Adense_sys = Matrix(Asp_sys)
                DA = distribute(Asp_sys, A_part_cfg)
                Db = distribute(b, Db_part)

                F4a = Dagger.splu(DA)
                F4b = if solve_bs === nothing
                    Dagger.splu(DA; distributed=true)
                else
                    Dagger.splu(DA; distributed=true, blocksize=solve_bs)
                end
                @test F4b isa Dagger.DistributedSparseLU
                @test size(F4b) == (n, n)
                @test F4b.L isa Dagger.DMatrix
                @test F4b.U isa Dagger.DMatrix
                # Factors stay sparse (no densification).
                @test fetch(F4b.L.chunks[1, 1]) isa Dagger.DSparseArray
                @test fetch(F4b.U.chunks[1, 1]) isa Dagger.DSparseArray

                x4a = collect(F4a \ Db)
                x = F4b \ Db
                @test x isa Dagger.DVector
                @test x.partitioning == Db.partitioning
                xc = collect(x)
                @test xc ≈ x4a rtol=1e-12 atol=1e-14
                @test norm(Adense_sys * xc - b) / norm(b) < 1e-10

                # Multi-RHS / repeated solves against one factor.
                for _ in 1:3
                    bi = rand(n)
                    Dbi = distribute(bi, Db_part)
                    xi = collect(F4b \ Dbi)
                    @test xi ≈ collect(F4a \ Dbi) rtol=1e-12 atol=1e-14
                    @test norm(Adense_sys * xi - bi) / norm(bi) < 1e-10
                end

                y = similar(Db)
                LinearAlgebra.ldiv!(y, F4b, Db)
                @test collect(y) ≈ x4a rtol=1e-12 atol=1e-14

                @test_throws DimensionMismatch F4b \ distribute(rand(n ÷ 2), Blocks(k))
            end
        end
    end

    @testset "Schur distributed LU (4c)" begin
        systems = (
            ("lap2d", N -> lap2d(Float64, N)),
            ("advdiff2d", N -> advdiff2d(Float64, N)),
        )
        for (name, buildA) in systems, N in (10, 20), nparts in (2, 4)
            @testset "$name N=$N nparts=$nparts" begin
                Asp_sys = buildA(N)
                nsys = size(Asp_sys, 1)
                bs = max(8, nsys ÷ 4)
                DA = distribute(Asp_sys, Blocks(bs, bs))
                bsys = rand(nsys)
                Db = distribute(bsys, Blocks(bs))

                F4a = Dagger.splu(DA)
                F4c = Dagger.splu(DA; distributed=true, method=:schur, nparts=nparts)
                @test F4c isa Dagger.DistributedSchurLU
                @test size(F4c) == (nsys, nsys)

                x = F4c \ Db
                @test x isa Dagger.DVector
                @test x.partitioning == Db.partitioning
                xc = collect(x)
                x4a = collect(F4a \ Db)
                @test xc ≈ x4a rtol=1e-8
                @test norm(Asp_sys * xc - bsys) / norm(bsys) < 1e-9

                y = similar(Db)
                LinearAlgebra.ldiv!(y, F4c, Db)
                @test collect(y) ≈ x4a rtol=1e-8
                @test norm(Asp_sys * collect(y) - bsys) / norm(bsys) < 1e-9

                # Factor-once / solve-many.
                for _ in 1:3
                    bi = rand(nsys)
                    Dbi = distribute(bi, Blocks(bs))
                    xi = collect(F4c \ Dbi)
                    @test xi ≈ collect(F4a \ Dbi) rtol=1e-8
                    @test norm(Asp_sys * xi - bi) / norm(bi) < 1e-9
                end

                @test_throws DimensionMismatch F4c \ distribute(rand(nsys ÷ 2), Blocks(bs))
            end
        end
    end
end

