# Shared sparse-DArray test bodies.
#
# Sparse support has to hold across the cross product of acceleration
# (Distributed / MPI) and compute backend (CPU / GPU), and each combination has
# its own entry point. Keeping the bodies here means one definition covers all of
# them:
#
#   Distributed x CPU  -> test/datadeps.jl (`test_sparse_bare_args` only; the
#                          tile/SpGEMM/solver coverage on this axis is the much
#                          broader test/array/linalg/matmul_sparse.jl and
#                          array/linalg/iterativesolvers.jl)
#   Distributed x GPU  -> test/gpu.jl
#   MPI x CPU          -> test/mpi.jl
#   MPI x GPU          -> test/mpi_gpu_suite.jl (test/mpi_opencl.jl, mpi_cuda.jl, ...)
#
# `scope` selects the compute backend (`nothing` for the ambient one) and
# `check_tile` optionally asserts that a distributed tile is device-resident.
#
# N.B. Every array is built from a seeded RNG. Under MPI these bodies run SPMD
# on every rank, so each rank must generate bit-identical inputs.

using SparseArrays

function _sparse_defs_with_scope(f, scope)
    scope === nothing && return f()
    return Dagger.with_options(f; scope)
end

sparse_defs_laplacian(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(4), n),
     1 => fill(-one(T), n - 1),
)
# Add a first-order advection term -> nonsymmetric, still well-conditioned.
sparse_defs_advection(T, n) = sparse_defs_laplacian(T, n) + SparseArrays.spdiagm(
    -1 => fill(T(-3) / 10, n - 1),
     1 => fill(T(3) / 10, n - 1),
)

# Sparse tile construction, SpGEMM and SpMV.
function test_sparse_darray(; scope=nothing, check_tile=nothing, T=Float32)
    Random.seed!(1234)
    SA = sprand(T, 8, 8, 0.35)
    SB = sprand(T, 8, 8, 0.35)
    x = rand(T, 8)

    _sparse_defs_with_scope(scope) do
        DSA = distribute(SA, Blocks(4, 4))
        DSB = distribute(SB, Blocks(4, 4))
        if check_tile !== nothing
            for chunk in DSA.chunks
                @test check_tile(fetch(chunk; raw=true))
            end
        end

        @test collect(DSA * DSB) ≈ SA * SB
        @test collect(DSA * DSB') ≈ SA * SB'
        @test collect(DSA' * DSB) ≈ SA' * SB

        DSC = similar(DSA)
        mul!(DSC, DSA, DSB)
        @test collect(DSC) ≈ SA * SB

        Dx = distribute(x, Blocks(4))
        @test collect(DSA * Dx) ≈ SA * x

        Z = SparseArrays.spzeros(Blocks(4, 4), T, 8, 8)
        # `collect` densifies; check emptiness on device tiles / dense gather.
        @test iszero(sum(abs, collect(Z)))
        if check_tile !== nothing
            for chunk in Z.chunks
                @test check_tile(fetch(chunk; raw=true))
            end
        end
    end
end

# Krylov solvers over sparse tiles, plus the Jacobi preconditioner.
#
# Block-Jacobi/ILU/AMG build host factorizations that Datadeps cannot place
# under a GPU-only compute scope, so they are covered on CPU only, in
# `array/linalg/iterativesolvers.jl`.
function test_sparse_solvers(; scope=nothing, check_tile=nothing, T=Float32)
    n, k = 32, 8
    A_part, b_part = Blocks(k, k), Blocks(k)
    atol, rtol, cmp_rtol = T(1e-6), T(1e-5), 1e-3

    Random.seed!(1234)
    Asp = sparse_defs_laplacian(T, n)
    b = rand(T, n)
    xref = Matrix(Asp) \ b
    Anonsym = sparse_defs_advection(T, n)
    bn = rand(T, n)
    xrefn = Matrix(Anonsym) \ bn

    _sparse_defs_with_scope(scope) do
        DA = distribute(Asp, A_part)
        Db = distribute(b, b_part)
        if check_tile !== nothing
            @test check_tile(fetch(DA.chunks[1]; raw=true))
        end

        @testset "$(nameof(solver))" for solver in (Dagger.cg, Dagger.minres, Dagger.gmres, Dagger.bicgstab)
            x, stats = solver(DA, Db; atol, rtol, itmax = 500)
            @test stats.solved
            @test x isa Dagger.DVector
            @test collect(x) ≈ xref rtol = cmp_rtol
        end

        x, stats = Dagger.krylov_solve(:cg, DA, Db; atol, rtol)
        @test stats.solved
        @test collect(x) ≈ xref rtol = cmp_rtol

        # Krylov's own entry point, with no Dagger-specific call site.
        x, stats = Krylov.cg(DA, Db; atol, rtol, itmax = 500)
        @test stats.solved
        @test collect(x) ≈ xref rtol = cmp_rtol

        P = Dagger.JacobiPreconditioner(DA)
        @test collect(P.dinv) ≈ fill(T(1) / T(4), n)
        y = similar(Db)
        mul!(y, P, Db)
        @test collect(y) ≈ (T(1) / T(4)) .* b
        x, stats = Dagger.cg(DA, Db; M = P, atol, rtol, itmax = 500)
        @test stats.solved
        @test collect(x) ≈ xref rtol = cmp_rtol

        DAn = distribute(Anonsym, A_part)
        Dbn = distribute(bn, b_part)
        @testset "$(nameof(solver)) nonsym" for solver in (Dagger.gmres, Dagger.bicgstab)
            x, stats = solver(DAn, Dbn; atol, rtol, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xrefn rtol = cmp_rtol
        end
    end
end

# Bare sparse containers handed straight to a Datadeps task: read-only access is
# adopted into a `DSparseArray`, write access is rejected. See
# `adopt_sparse_arg!`.
# N.B. These reach through to `.mat.nzval` rather than using `sum`/broadcast on
# the wrapper: generic `AbstractArray` fallbacks index element-wise, which is
# scalar indexing once the tile has been moved to a device.
@everywhere sparse_defs_nzsum(X) = sum(X.mat.nzval)
@everywhere sparse_defs_type(X) = string(typeof(X))
@everywhere sparse_defs_scale!(X, a) = (X.mat.nzval .*= a; nothing)

#
# `writeback_visible` must be false on the MPI ranks that do not own the origin:
# a bare Julia object is replicated per-rank under SPMD, so Datadeps writes back
# into rank 0's copy and every other rank keeps its own untouched replica. This
# is the same reason test/mpi.jl guards its in-place assertions with `rank == 0`.
function test_sparse_bare_args(; scope=nothing, T=Float64, writeback_visible=true)
    Random.seed!(1234)
    S = sprand(T, 16, 16, 0.3)
    Sref = copy(S)

    _sparse_defs_with_scope(scope) do
        seen, total = nothing, nothing
        Dagger.spawn_datadeps() do
            seen = Dagger.@spawn sparse_defs_type(In(S))
            total = Dagger.@spawn sparse_defs_nzsum(In(S))
        end
        @test fetch(seen) == string(Dagger.DSparseArray{T,2})
        @test fetch(total) ≈ sum(Sref)

        for dep in (InOut, Out)
            @test_throws ArgumentError Dagger.spawn_datadeps() do
                Dagger.@spawn sparse_defs_scale!(dep(S), T(2))
            end
        end

        # Wrapping it yourself is the documented way to get write access.
        W = Dagger.DSparseArray(copy(S))
        total = nothing
        Dagger.spawn_datadeps() do
            Dagger.@spawn sparse_defs_scale!(InOut(W), T(2))
            total = Dagger.@spawn sparse_defs_nzsum(In(W))
        end
        # Fetching a task result is collective, so the RAW through the wrapper is
        # checked on every rank; the write-back into `W` itself is not.
        @test fetch(total) ≈ 2 * sum(Sref)
        if writeback_visible
            @test collect(W) ≈ 2 .* Sref
        end
    end

    # Adoption copies, so the caller's matrix is never touched.
    @test S == Sref
end
