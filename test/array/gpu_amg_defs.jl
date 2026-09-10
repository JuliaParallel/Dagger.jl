# Shared GPU-resident GlobalAMG bodies (lesson 16 / 52).
#
# AlgebraicMultigrid.jl is host: setup host-stages *tiles* inside GPU-scoped
# tasks. The V-cycle keeps Krylov / workspace *vectors* in device memory
# (Jacobi / ℓ1-Jacobi / Chebyshev / SpMV). Hybrid GS Adapts a temporary
# inside the GPU-scoped tile kernel. Coarsest LU is still gathered
# (`_solve_pinned_dvector`). Per-tile `AMGPreconditioner` is Schwarz — see
# `gpu_pc_defs.jl` — not a coarse grid (lesson 19).
#
# Entry points:
#   Distributed x GPU  -> test/gpu.jl
#   MPI x GPU          -> test/mpi_gpu_suite.jl

function _gpu_amg_laplacian(T, n)
    return SparseArrays.spdiagm(
        -1 => fill(-one(T), n - 1),
         0 => fill(T(2), n),
         1 => fill(-one(T), n - 1),
    )
end

function _gpu_amg_relres(A, x, b)
    r = similar(b)
    mul!(r, A, x)
    axpy!(-one(eltype(b)), b, r)
    return LinearAlgebra.norm2(r) / LinearAlgebra.norm2(b)
end

function _gpu_amg_jacobi_only(A, b, relax, nsweeps)
    dinv = inv.(Vector(diag(A)))
    u = zeros(eltype(b), length(b))
    ω = eltype(b)(relax)
    for _ in 1:nsweeps
        u .+= ω .* dinv .* (b .- A * u)
    end
    return LinearAlgebra.norm2(A * u - b) / LinearAlgebra.norm2(b)
end

"""
    test_gpu_global_amg(; scope, check_vec, T)

Small GPU Laplacian: GlobalAMG V-cycle + GMRES, asserting `‖Ax−b‖` and that
vector chunks stay device-resident after apply / solve.
"""
function test_gpu_global_amg(; scope=nothing, check_vec=nothing, T=Float32)
    Base.get_extension(Dagger, :AlgebraicMultigridExt) === nothing && return
    n, k = 32, 8
    cmp_rtol = T <: AbstractFloat && sizeof(T) == 4 ? 1e-3 : 1e-8
    rel_tol = T <: AbstractFloat && sizeof(T) == 4 ? T(1e-4) : T(1e-6)

    Random.seed!(1234)
    Ah = _gpu_amg_laplacian(T, n)
    bh = rand(T, n)

    _gpu_pc_with_scope(scope) do
        DA = distribute(Ah, Blocks(k, k))
        Db = distribute(bh, Blocks(k))

        @testset "GlobalAMG V-cycle (jacobi) stays on-device" begin
            M = Dagger.GlobalAMG(DA; max_levels=3, max_coarse=16, smoother=:jacobi)
            @test M isa Dagger.GlobalAMG
            @test !isempty(M.levels)
            y = similar(Db)
            mul!(y, M, Db)
            @test all(isfinite, collect(y))
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(y.chunks[1]))
            end
            vrel = _gpu_amg_relres(DA, y, Db)
            jrel = _gpu_amg_jacobi_only(Ah, bh, M.relax, M.presweeps + M.postsweeps)
            @test vrel < jrel
        end

        @testset "GlobalAMG GMRES residual (jacobi)" begin
            M = Dagger.GlobalAMG(DA; max_levels=3, max_coarse=16, smoother=:jacobi)
            x, stats = Krylov.gmres(DA, Db; M=M, atol=T(1e-10), rtol=T(1e-7),
                                    itmax=200, memory=min(n, 40))
            rel = _gpu_amg_relres(DA, x, Db)
            @test rel < rel_tol
            @test collect(x) ≈ Matrix(Ah) \ bh rtol=cmp_rtol
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(x.chunks[1]))
            end
        end

        @testset "GlobalAMG ℓ1-Jacobi apply stays on-device" begin
            M = Dagger.GlobalAMG(DA; max_levels=2, max_coarse=16, smoother=:l1jacobi)
            y = similar(Db)
            mul!(y, M, Db)
            @test all(isfinite, collect(y))
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(y.chunks[1]))
            end
        end

        @testset "GlobalAMG hybrid-GS Adapt keeps chunk on-device" begin
            M = Dagger.GlobalAMG(DA; max_levels=2, max_coarse=16, smoother=:hybrid_gs)
            y = similar(Db)
            mul!(y, M, Db)
            @test all(isfinite, collect(y))
            if check_vec !== nothing
                @test check_vec(_gpu_pc_chunk(y.chunks[1]))
            end
        end
    end
end
