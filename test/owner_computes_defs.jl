# Shared body of the owner-computes tests: runs in one process (test/owner_computes.jl) and under MPI (test/mpi/owner_computes.jl, 2 and 4 ranks).
# Every rank builds the same problem from the same seed, runs the tile algorithm, gathers the result on rank 0 and compares it with LinearAlgebra.
using Dagger, LinearAlgebra, Random, Test

tf_id(nt, i, j) = (j - 1) * nt + i
tf_cyclic(nt, np) = begin
    pr = floor(Int, sqrt(np)); while np % pr != 0; pr -= 1; end; pc = np ÷ pr
    id -> (i = (id - 1) % nt + 1; j = (id - 1) ÷ nt + 1; mod(i - 1, pr) * pc + mod(j - 1, pc))
end
tf_block(M, nb, i, j) = M[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb]

function tf_cholesky!(sp, nt)
    for k in 1:nt
        Dagger.tile_spawn!(sp, A -> LAPACK.potrf!('L', A), Dagger.TileRW(tf_id(nt, k, k)))
        for m in k+1:nt
            Dagger.tile_spawn!(sp, (L, A) -> BLAS.trsm!('R', 'L', 'T', 'N', 1.0, L, A), Dagger.TileIn(tf_id(nt, k, k)), Dagger.TileRW(tf_id(nt, m, k)))
        end
        for n in k+1:nt
            Dagger.tile_spawn!(sp, (A, C) -> BLAS.syrk!('L', 'N', -1.0, A, 1.0, C), Dagger.TileIn(tf_id(nt, n, k)), Dagger.TileRW(tf_id(nt, n, n)))
            for m in n+1:nt
                Dagger.tile_spawn!(sp, (A, B, C) -> BLAS.gemm!('N', 'T', -1.0, A, B, 1.0, C),
                                   Dagger.TileIn(tf_id(nt, m, k)), Dagger.TileIn(tf_id(nt, n, k)), Dagger.TileRW(tf_id(nt, m, n)))
            end
        end
    end
end
function tf_lu!(sp, nt)      # no pivoting: diagonally dominant input
    for k in 1:nt
        Dagger.tile_spawn!(sp, A -> (lu!(A, NoPivot()); nothing), Dagger.TileRW(tf_id(nt, k, k)))
        for j in k+1:nt
            Dagger.tile_spawn!(sp, (D, A) -> BLAS.trsm!('L', 'L', 'N', 'U', 1.0, D, A), Dagger.TileIn(tf_id(nt, k, k)), Dagger.TileRW(tf_id(nt, k, j)))
        end
        for i in k+1:nt
            Dagger.tile_spawn!(sp, (D, A) -> BLAS.trsm!('R', 'U', 'N', 'N', 1.0, D, A), Dagger.TileIn(tf_id(nt, k, k)), Dagger.TileRW(tf_id(nt, i, k)))
        end
        for i in k+1:nt, j in k+1:nt
            Dagger.tile_spawn!(sp, (L, U, C) -> BLAS.gemm!('N', 'N', -1.0, L, U, 1.0, C),
                               Dagger.TileIn(tf_id(nt, i, k)), Dagger.TileIn(tf_id(nt, k, j)), Dagger.TileRW(tf_id(nt, i, j)))
        end
    end
end
function tf_gemm!(sp, nt)    # C = A*B; ids: A 1.., B nt^2+1.., C 2nt^2+1..
    n2 = nt * nt
    for i in 1:nt, j in 1:nt, k in 1:nt
        Dagger.tile_spawn!(sp, (A, B, C) -> BLAS.gemm!('N', 'N', 1.0, A, B, 1.0, C),
                           Dagger.TileIn(tf_id(nt, i, k)), Dagger.TileIn(n2 + tf_id(nt, k, j)), Dagger.TileRW(2n2 + tf_id(nt, i, j)))
    end
end
# a chain of tasks on ONE tile that hops between the ranks (every step is a transfer)
tf_pingpong!(sp, steps, np) = for s in 1:steps; Dagger.tile_spawn!(sp, A -> (A[1] += 1.0; nothing), Dagger.TileRW(1); at = s % np); end

# bring tiles `ids` to rank 0 and return them there (nothing elsewhere)
function tf_gather(sp, ids, me)
    for id in ids; Dagger.tile_spawn!(sp, A -> nothing, Dagger.TileIn(id); at = 0); end
    Dagger.tile_sync!(sp)
    me == 0 || return nothing
    return Dict(id => copy(Dagger.tile_data(sp, id)) for id in ids)
end
function tf_assemble(d, nt, nb, off)
    M = zeros(nt * nb, nt * nb)
    for i in 1:nt, j in 1:nt; M[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] = d[off + tf_id(nt, i, j)]; end
    return M
end

function owner_computes_checks(me::Int, np::Int)
    nt, nb = 6, 24; n = nt * nb
    ctile(M) = id -> tf_block(M, nb, (id - 1) % nt + 1, (id - 1) ÷ nt + 1)
    @testset "Cholesky" begin
        Random.seed!(42); X = rand(n, n); M = (X + X') / 2 + n * I
        sp = Dagger.TileSpace(nt^2, tf_cyclic(nt, np), id -> (nb, nb), ctile(M))
        tf_cholesky!(sp, nt)
        d = tf_gather(sp, collect(1:nt^2), me)
        if me == 0
            @test norm(tril(tf_assemble(d, nt, nb, 0)) - cholesky(Symmetric(M, :L)).L) / norm(M) < 1e-12
        end
        st = Dagger.tile_stats(sp); @test st.tasks == 56 + nt^2     # the gather tasks count too
        np > 1 && @test st.transfers > 0
    end
    @testset "LU (no pivoting)" begin
        Random.seed!(43); M = rand(n, n) + n * I
        sp = Dagger.TileSpace(nt^2, tf_cyclic(nt, np), id -> (nb, nb), ctile(M))
        tf_lu!(sp, nt)
        d = tf_gather(sp, collect(1:nt^2), me)
        if me == 0
            F = lu(M, NoPivot())
            @test norm(tf_assemble(d, nt, nb, 0) - (F.L + F.U - I)) / norm(M) < 1e-12
        end
    end
    @testset "GEMM" begin
        Random.seed!(44); A = rand(n, n); B = rand(n, n)
        init = id -> begin
            blk = (id - 1) ÷ nt^2; t = (id - 1) % nt^2 + 1; i = (t - 1) % nt + 1; j = (t - 1) ÷ nt + 1
            blk == 0 ? tf_block(A, nb, i, j) : blk == 1 ? tf_block(B, nb, i, j) : zeros(nb, nb)
        end
        own = id -> tf_cyclic(nt, np)((id - 1) % nt^2 + 1)
        sp = Dagger.TileSpace(3nt^2, own, id -> (nb, nb), init)
        tf_gemm!(sp, nt)
        d = tf_gather(sp, collect(2nt^2+1:3nt^2), me)
        me == 0 && @test norm(tf_assemble(d, nt, nb, 2nt^2) - A * B) / norm(A * B) < 1e-12
    end
    @testset "ping-pong chain (one transfer per step, write-after-read, pool reuse)" begin
        steps = 40
        sp = Dagger.TileSpace(1, id -> 0, id -> (8, 8), id -> zeros(8, 8))
        tf_pingpong!(sp, steps, np)
        d = tf_gather(sp, [1], me)
        me == 0 && @test d[1][1] == steps
    end
    @testset "a failing kernel surfaces at tile_sync! on every rank and nobody hangs" begin
        # tile 1 is written on rank 0 by a kernel that fails; tile 2 (rank np-1) then READS tile 1: its receive must still be satisfied
        sp = Dagger.TileSpace(2, id -> id == 1 ? 0 : np - 1, id -> (4, 4), id -> zeros(4, 4))
        Dagger.tile_spawn!(sp, A -> error("tile kernel boom"), Dagger.TileRW(1))
        Dagger.tile_spawn!(sp, (A, B) -> (B .+= 1; nothing), Dagger.TileIn(1), Dagger.TileRW(2))
        @test_throws Exception Dagger.tile_sync!(sp)          # rank 0: its own error; the others: "a task failed on another rank"
        # the space is still usable for a fresh run afterwards
        sp2 = Dagger.TileSpace(1, id -> 0, id -> (2, 2), id -> zeros(2, 2))
        Dagger.tile_spawn!(sp2, A -> (A[1] = 5.0; nothing), Dagger.TileRW(1))
        @test Dagger.tile_sync!(sp2) === nothing
    end
    @testset "restore: a tile written on another rank ends up in its original array" begin
        # tile 1 starts on rank 0, tile 2 on the last rank; one task writes BOTH (it runs on rank 0, so tile 2 travels there)
        A1 = fill(1.0, 3, 3); A2 = fill(2.0, 3, 3)
        mine = Dict(1 => A1, 2 => A2)
        sp = Dagger.TileSpace(2, id -> id == 1 ? 0 : np - 1, id -> (3, 3), id -> mine[id])
        Dagger.tile_spawn!(sp, (X, Y) -> (X .+= 10; Y .+= 100; nothing), Dagger.TileRW(1), Dagger.TileRW(2))
        Dagger.tile_restore!(sp)
        me == 0 && @test all(==(11.0), A1)
        me == np - 1 && @test all(==(102.0), A2)       # the result went back to where tile 2 came from
    end
    @testset "a small array every rank has its own copy of (replicated tile) is the same everywhere afterwards" begin
        info = zeros(Int, 1)
        sp = Dagger.TileSpace(1, id -> 0, id -> (1,), id -> info; types = id -> Int,
                              initial = id -> (UInt64(1) << np) - 1, homes = id -> (UInt64(1) << np) - 1)
        Dagger.tile_spawn!(sp, a -> (a[1] = 7; nothing), Dagger.TileRW(1); at = np - 1)      # written on one rank only
        Dagger.tile_restore!(sp)
        @test info[1] == 7                                                                      # every rank sees it
    end
    @testset "element types: Float32 and ComplexF64 tiles" begin
        for T in (Float32, ComplexF64)
            sp = Dagger.TileSpace(2, id -> id == 1 ? 0 : np - 1, id -> (3, 3), id -> fill(T(id), 3, 3); T)
            Dagger.tile_spawn!(sp, (A, B) -> (B .+= A; nothing), Dagger.TileIn(1), Dagger.TileRW(2))
            d = tf_gather(sp, [2], me)
            me == 0 && @test all(==(T(3)), d[2])
        end
    end
end
