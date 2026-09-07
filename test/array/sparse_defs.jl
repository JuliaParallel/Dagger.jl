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

        # Re-tiling must keep the tiles sparse: allocating dense tiles here
        # would silently densify the operator (an out-of-memory multiplier for a
        # real problem, not just a slowdown). This is the allocation every
        # partitioning-mismatched `mul!` and every block preconditioner over a
        # non-square-tiled operator goes through.
        DSA_fine = Dagger.repartition(DSA, Blocks(2, 2))
        @test DSA_fine.partitioning == Blocks(2, 2)
        @test collect(DSA_fine) ≈ SA
        for chunk in DSA_fine.chunks
            tile = fetch(chunk; raw=true)
            @test Dagger.chunktype(tile) <: Dagger.DSparseArray
            check_tile === nothing || @test check_tile(tile)
        end

        # A partitioning mismatch between operator and vector: `mul!` aligns
        # them itself rather than requiring the caller to match up front.
        Dx_coarse = distribute(x, Blocks(8))
        @test collect(DSA_fine * Dx_coarse) ≈ SA * x
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
# N.B. These reach the storage vector rather than using `sum`/broadcast on the
# wrapper: generic `AbstractArray` fallbacks index element-wise, which is
# scalar indexing once the tile has been moved to a device. CUSPARSE/rocSPARSE
# name that vector `nzVal`; SparseArrays and `DeviceSparseMatrixCSC` use `nzval`.
@everywhere sparse_defs_nzvals(A) =
    hasfield(typeof(A), :nzval) ? getfield(A, :nzval) : getfield(A, :nzVal)
@everywhere sparse_defs_nzsum(X) = sum(sparse_defs_nzvals(X.mat))
@everywhere sparse_defs_type(X) = string(typeof(X))
@everywhere sparse_defs_scale!(X, a) = (sparse_defs_nzvals(X.mat) .*= a; nothing)

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

# 1D Laplacian COO (global 1-based indices), optionally restricted to a row range
# so multiple "owners" can each contribute their rows — including off-diagonals
# that land on a neighboring column tile.
function sparse_defs_laplacian_coo(T, n, rows=1:n)
    I = Int[]; J = Int[]; V = T[]
    for i in rows
        if i > 1
            push!(I, i); push!(J, i - 1); push!(V, -one(T))
        end
        push!(I, i); push!(J, i); push!(V, T(2))
        if i < n
            push!(I, i); push!(J, i + 1); push!(V, -one(T))
        end
    end
    return I, J, V
end

sparse_defs_laplacian_1d(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(2), n),
     1 => fill(-one(T), n - 1),
)

# Assemble a 1D Laplacian from per-row COO on multiple owners; compare to
# `distribute(spdiagm(...), Blocks(...))`. Also exercises duplicate (I,J)
# combine and DArray-valued I,J,V (overlap send to the owning tile).
function test_sparse_assembly(; scope=nothing, check_tile=nothing, T=Float64)
    n, k = 16, 4
    part = Blocks(k, k)
    Aref = sparse_defs_laplacian_1d(T, n)
    I, J, V = sparse_defs_laplacian_coo(T, n)

    _sparse_defs_with_scope(scope) do
        Dref = distribute(Aref, part)

        # Host COO + Blocks (and the Blocks-first spelling, matching spzeros).
        DA = SparseArrays.sparse(I, J, V, n, n, part)
        @test DA.partitioning == part
        @test collect(DA) ≈ collect(Dref)
        @test collect(DA) ≈ Matrix(Aref)
        DA2 = SparseArrays.sparse(part, I, J, V, n, n)
        @test collect(DA2) ≈ Matrix(Aref)
        for chunk in DA.chunks
            tile = fetch(chunk; raw=true)
            @test Dagger.chunktype(tile) <: Dagger.DSparseArray
            check_tile === nothing || @test check_tile(tile)
        end

        # Existing `distribute(sparse(...), Blocks)` path is unchanged.
        S = SparseArrays.sparse(I, J, V, n, n)
        @test S isa SparseArrays.SparseMatrixCSC
        @test collect(distribute(S, part)) ≈ S

        # Per-owner incremental assembly: each owner adds only its rows.
        Z = SparseArrays.spzeros(part, T, n, n)
        nowners = 4
        for o in 1:nowners
            r1 = (o - 1) * k + 1
            r2 = o * k
            Io, Jo, Vo = sparse_defs_laplacian_coo(T, n, r1:r2)
            SparseArrays.sparse!(Z, Io, Jo, Vo)
        end
        @test collect(Z) ≈ Matrix(Aref)

        # Duplicate (I,J) combine (`+` by default; `max` as an alternate).
        Z2 = SparseArrays.spzeros(part, T, n, n)
        SparseArrays.sparse!(Z2, I, J, V)
        SparseArrays.sparse!(Z2, I, J, V)
        @test collect(Z2) ≈ 2 .* Matrix(Aref)
        I2 = vcat(I, I); J2 = vcat(J, J); V2 = vcat(V, V)
        @test collect(SparseArrays.sparse(I2, J2, V2, n, n, part)) ≈ 2 .* Matrix(Aref)
        @test collect(SparseArrays.sparse(I2, J2, V2, n, n, max, part)) ≈ Matrix(Aref)

        # Distributed COO whose chunking is independent of the matrix tiles, so
        # some triplets are produced off the owning tile (overlap send).
        ntrips = length(I)
        coo_part = Blocks(cld(ntrips, 3))
        DI = distribute(I, coo_part)
        DJ = distribute(J, coo_part)
        DV = distribute(V, coo_part)
        DD = SparseArrays.sparse(DI, DJ, DV, n, n, part)
        @test collect(DD) ≈ Matrix(Aref)
        Z3 = SparseArrays.spzeros(part, T, n, n)
        SparseArrays.sparse!(Z3, DI, DJ, DV)
        @test collect(Z3) ≈ Matrix(Aref)

        # Non-square tiles.
        part_rect = Blocks(4, 8)
        @test collect(SparseArrays.sparse(I, J, V, n, n, part_rect)) ≈ Matrix(Aref)
    end
end
