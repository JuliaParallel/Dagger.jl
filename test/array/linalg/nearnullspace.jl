# Near-nullspace / rigid-body modes on GlobalAMG (smoothed aggregation).
#
# Scalar SA uses B = ones. Systems whose low-energy modes are not that
# constant (elasticity, a 2-component beam) need those modes as candidates.
# `AMGPreconditioner` is unchanged (per-tile). Check ‖Ax−b‖, not stats.solved
# (AGENTS.md lessons 19 / 32 / 39).
#
#     julia test/runtests.jl --test array/linalg/nearnullspace

using Krylov
using AlgebraicMultigrid

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

function amg_richardson(DA, Db, M; ncycles=15)
    x = similar(Db)
    fill!(x, zero(eltype(Db)))
    r = similar(Db)
    z = similar(Db)
    nb = LinearAlgebra.norm2(Db)
    rels = Float64[]
    for _ in 1:ncycles
        mul!(r, DA, x)
        axpy!(-one(eltype(Db)), Db, r)
        rmul!(r, -one(eltype(r)))
        mul!(z, M, r)
        axpy!(one(eltype(x)), z, x)
        mul!(r, DA, x)
        axpy!(-one(eltype(Db)), Db, r)
        push!(rels, LinearAlgebra.norm2(r) / nb)
    end
    return x, rels
end

function poisson_1d(n)
    return SparseArrays.spdiagm(
        -1 => fill(-1.0, n - 1),
         0 => fill(2.0, n),
         1 => fill(-1.0, n - 1),
    )
end

# Interleaved ncomp-component 1-D Dirichlet Laplacian: kron(L, I).
function vector_laplacian_1d(nnode, ncomp=2)
    L = poisson_1d(nnode)
    return SparseArrays.kron(L, SparseArrays.sparse(LinearAlgebra.I, ncomp, ncomp))
end

function component_modes(nnode, ncomp=2)
    B = zeros(nnode * ncomp, ncomp)
    for i in 1:nnode, c in 1:ncomp
        B[ncomp * (i - 1) + c, c] = 1
    end
    return B
end

# 1-D Timoshenko cantilever (w, θ) per node — a 2-component system whose
# near-nullspace is translation + rotation, not the scalar ones vector.
function timoshenko_Ke(EI, GAs, le)
    φ = 12 * EI / (GAs * le^2)
    a = EI / (le^3 * (1 + φ))
    return a * [
        12        6*le         -12        6*le
        6*le      le^2*(4+φ)   -6*le      le^2*(2-φ)
        -12       -6*le         12       -6*le
        6*le      le^2*(2-φ)   -6*le      le^2*(4+φ)
    ]
end

function timoshenko_cantilever(n_elem; EI=1.0, GAs=50.0, le=1.0)
    n_nodes = n_elem + 1
    ndof = 2 * n_nodes
    I, J, V = Int[], Int[], Float64[]
    Ke = timoshenko_Ke(EI, GAs, le)
    for e in 1:n_elem
        edof = (2e - 1):(2e + 2)
        for a in 1:4, b in 1:4
            push!(I, edof[a]); push!(J, edof[b]); push!(V, Ke[a, b])
        end
    end
    A = SparseArrays.sparse(I, J, V, ndof, ndof)
    B = zeros(ndof, 2)
    for i in 1:n_nodes
        x = (i - 1) * le
        B[2i - 1, 1] = 1
        B[2i,     1] = 0
        B[2i - 1, 2] = x
        B[2i,     2] = 1
    end
    b = zeros(ndof)
    b[2 * n_nodes - 1] = -1
    free = 3:ndof
    return A[free, free], b[free], B[free, :]
end

# Q1 plane-strain elasticity on a rectangular grid; left face clamped.
function q1_stiffness(hx, hy, λ, μ)
    D = [λ + 2μ  λ      0.0
         λ       λ + 2μ 0.0
         0.0     0.0    μ]
    g = 1 / sqrt(3)
    Ke = zeros(8, 8)
    detJ = hx * hy / 4
    for ξ in (-g, g), η in (-g, g)
        dNdξ = [-(1 - η) / 4, (1 - η) / 4, (1 + η) / 4, -(1 + η) / 4]
        dNdη = [-(1 - ξ) / 4, -(1 + ξ) / 4, (1 + ξ) / 4, (1 - ξ) / 4]
        dNdx = (2 / hx) .* dNdξ
        dNdy = (2 / hy) .* dNdη
        Bm = zeros(3, 8)
        for a in 1:4
            Bm[1, 2a - 1] = dNdx[a]
            Bm[2, 2a]     = dNdy[a]
            Bm[3, 2a - 1] = dNdy[a]
            Bm[3, 2a]     = dNdx[a]
        end
        Ke .+= (Bm' * D * Bm) .* detJ
    end
    return Ke
end

function q1_elasticity_cantilever(nelx, nely; λ=1.0, μ=1.0, Lx=Float64(nelx), Ly=Float64(nely))
    hx, hy = Lx / nelx, Ly / nely
    ngx, ngy = nelx + 1, nely + 1
    nnode = ngx * ngy
    ndof = 2 * nnode
    I, J, V = Int[], Int[], Float64[]
    Ke = q1_stiffness(hx, hy, λ, μ)
    for ey in 1:nely, ex in 1:nelx
        n1 = ex + (ey - 1) * ngx
        n2 = n1 + 1
        n3 = n2 + ngx
        n4 = n1 + ngx
        nodes = (n1, n2, n3, n4)
        edof = Vector{Int}(undef, 8)
        for a in 1:4
            edof[2a - 1] = 2 * nodes[a] - 1
            edof[2a]     = 2 * nodes[a]
        end
        for a in 1:8, b in 1:8
            push!(I, edof[a]); push!(J, edof[b]); push!(V, Ke[a, b])
        end
    end
    A = SparseArrays.sparse(I, J, V, ndof, ndof)
    B = zeros(ndof, 3)
    b = zeros(ndof)
    fixed = Int[]
    for j in 1:ngy, i in 1:ngx
        n = i + (j - 1) * ngx
        x = (i - 1) * hx
        y = (j - 1) * hy
        B[2n - 1, 1] = 1
        B[2n,     2] = 1
        B[2n - 1, 3] = -y
        B[2n,     3] = x
        if i == 1
            push!(fixed, 2n - 1, 2n)
        end
        if i == ngx
            b[2n] -= 1 / ngy
        end
    end
    free = setdiff(1:ndof, fixed)
    return A[free, free], b[free], B[free, :]
end

function distribute_system(A, b, N, k)
    DA = distribute(A, Blocks(k, k))
    Db = distribute(b, Blocks(k))
    DN = distribute(N, Blocks(k, size(N, 2)))
    return DA, Db, DN
end

@testset "Near-nullspace AMG" begin
    @testset "Q1 element has a 3-D rigid kernel" begin
        Ke = q1_stiffness(1.0, 1.0, 1.0, 1.0)
        @test issymmetric(Ke)
        sv = LinearAlgebra.svdvals(Ke)
        @test count(<(1e-8), sv) == 3
    end

    @testset "Timoshenko free beam has a 2-D rigid kernel" begin
        A, _, _ = timoshenko_cantilever(2)
        Afree = let
            n_elem = 2
            n_nodes = n_elem + 1
            ndof = 2 * n_nodes
            I, J, V = Int[], Int[], Float64[]
            Ke = timoshenko_Ke(1.0, 50.0, 1.0)
            for e in 1:n_elem
                edof = (2e - 1):(2e + 2)
                for a in 1:4, b in 1:4
                    push!(I, edof[a]); push!(J, edof[b]); push!(V, Ke[a, b])
                end
            end
            SparseArrays.sparse(I, J, V, ndof, ndof)
        end
        sv = LinearAlgebra.svdvals(Matrix(Afree))
        @test count(<(1e-8), sv) == 2
        @test size(A, 1) == 4
    end

    @testset "constructor API (Poisson still scalar-SA)" begin
        n, k = 64, 16
        A = poisson_1d(n)
        b = rand(n)
        DA = distribute(A, Blocks(k, k))
        Db = distribute(b, Blocks(k))

        M0 = Dagger.SmoothedAggregationPreconditioner(DA; max_levels=3, max_coarse=16)
        @test M0 isa Dagger.GlobalAMG
        @test M0.nmodes == 1
        @test occursin("nullspace=1", sprint(show, M0))

        N1 = distribute(ones(n), Blocks(k))
        M1 = Dagger.SmoothedAggregationPreconditioner(DA; nullspace=N1, max_levels=3, max_coarse=16)
        @test M1.nmodes == 1
        _, _, rel1 = solve_gmres(DA, Db, M1)
        @test rel1 < 1e-6

        Nmat = distribute(ones(n, 1), Blocks(k, 1))
        M1m = Dagger.SmoothedAggregationPreconditioner(DA; B=Nmat, max_levels=3, max_coarse=16)
        @test M1m.nmodes == 1

        Nbad = distribute(ones(n + 1, 2), Blocks(k, 2))
        @test_throws DimensionMismatch Dagger.SmoothedAggregationPreconditioner(
            DA; nullspace=Nbad, max_levels=2, max_coarse=16)

        @test_throws ArgumentError Dagger.RugeStubenPreconditioner(
            DA; nullspace=N1, max_levels=2, max_coarse=16)
        @test_throws ArgumentError Dagger.SmoothedAggregationPreconditioner(
            DA; nullspace=N1, B=Nmat, max_levels=2, max_coarse=16)

        Ptile = Dagger.AMGPreconditioner(DA)
        @test Ptile isa Dagger.AMGPreconditioner
        @test Ptile isa Dagger.AbstractBlockPreconditioner
        @test length(Ptile.ops) == n ÷ k
    end

    @testset "2-component 1-D vector Laplacian" begin
        nnode, ncomp, nt = 48, 2, 4
        n = nnode * ncomp
        k = n ÷ nt
        A = vector_laplacian_1d(nnode, ncomp)
        N = component_modes(nnode, ncomp)
        b = rand(n)
        DA, Db, DN = distribute_system(A, b, N, k)

        Ms = Dagger.SmoothedAggregationPreconditioner(DA; max_levels=3, max_coarse=16)
        Mn = Dagger.SmoothedAggregationPreconditioner(DA; nullspace=DN, max_levels=3, max_coarse=16)
        @test Ms.nmodes == 1
        @test Mn.nmodes == 2
        @test !isempty(Ms.levels) && !isempty(Mn.levels)
        @test size(Mn.levels[1].P, 2) > size(Ms.levels[1].P, 2)

        x, stats, rel = solve_gmres(DA, Db, Mn)
        @test rel < 1e-6
        @test collect(x) ≈ Matrix(A) \ b rtol = 1e-5

        Mp = Dagger.SmoothedAggregationPreconditioner(
            Dagger.Projected(DA, DN); max_levels=3, max_coarse=16)
        @test Mp.nmodes == 2
        _, _, relp = solve_gmres(DA, Db, Mp)
        @test relp < 1e-6
    end

    @testset "Timoshenko cantilever (2-component NNS)" begin
        n_elem = 48
        A, b, N = timoshenko_cantilever(n_elem)
        @test issymmetric(A)
        @test all(>(0), diag(A))
        n = size(A, 1)
        k = 16
        DA, Db, DN = distribute_system(A, b, N, k)

        Ms = Dagger.SmoothedAggregationPreconditioner(DA; max_levels=3, max_coarse=16)
        Mn = Dagger.SmoothedAggregationPreconditioner(DA; nullspace=DN, max_levels=3, max_coarse=16)
        @test Mn.nmodes == 2
        @test !isempty(Mn.levels)
        @test size(Mn.levels[1].P, 2) > size(Ms.levels[1].P, 2)

        _, rels_s = amg_richardson(DA, Db, Ms; ncycles=16)
        _, rels_n = amg_richardson(DA, Db, Mn; ncycles=16)
        xn, stn, reln = solve_gmres(DA, Db, Mn)
        xs, sts, rels = solve_gmres(DA, Db, Ms)
        # Timoshenko is a 2-component API check (P is wider). Jacobi-smoothed
        # Richardson can grow the residual here (lesson 32); Q1 elasticity
        # below is the stall + 1e-6 residual test.
        @test reln < 1e-4
        @test collect(xn) ≈ Matrix(A) \ b rtol = 1e-3
        @test last(rels_n) < 0.95 * last(rels_s) || stn.niter < sts.niter || rels > 10 * reln
    end

    @testset "2-D Q1 elasticity cantilever: scalar SA stalls vs NNS" begin
        A, b, N = q1_elasticity_cantilever(12, 4)
        @test issymmetric(A)
        n = size(A, 1)
        k = 16
        DA, Db, DN = distribute_system(A, b, N, k)

        Ms = Dagger.SmoothedAggregationPreconditioner(DA; max_levels=3, max_coarse=20)
        Mn = Dagger.SmoothedAggregationPreconditioner(DA; nullspace=DN, max_levels=3, max_coarse=20)
        @test Mn.nmodes == 3
        @test !isempty(Mn.levels)
        @test size(Mn.levels[1].P, 2) > size(Ms.levels[1].P, 2)

        _, rels_s = amg_richardson(DA, Db, Ms; ncycles=16)
        _, rels_n = amg_richardson(DA, Db, Mn; ncycles=16)
        xn, stn, reln = solve_gmres(DA, Db, Mn)
        xs, sts, rels = solve_gmres(DA, Db, Ms)
        @test reln < 1e-6
        @test last(rels_n) < 0.5
        @test collect(xn) ≈ Matrix(A) \ b rtol = 1e-4
        @test last(rels_n) < 0.95 * last(rels_s) || stn.niter < sts.niter || rels > 10 * reln
    end
end
