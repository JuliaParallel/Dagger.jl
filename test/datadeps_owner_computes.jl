# spawn_datadeps regions executed on the owner-computes path: the library code is unchanged, results must equal the
# Datadeps path, and the counter proves the layer really ran (the fallback gives the same numbers).
using Dagger, LinearAlgebra, Random, Test

# Only in-place (`!`) functions are lowered (`Dagger._region_inplace`): any other task may produce a block or value that the rest of Dagger needs as a Chunk.
tf_sum!(x) = sum(x)
tf_boom!(x) = error("boom")
tf_noop!(x, y) = nothing

# The bridge handles the tiles of ONE process. With workers added (the Distributed suite runs with -p 3) a tile may live on another process and the region
# then falls back to the ordinary planner, depending on where the arrays happened to be placed: the counter is asserted only when there is one process.
tf_single_process() = length(Dagger.procs()) == 1
tf_lowered(runs) = tf_single_process() ? (@test runs >= 1) : nothing

function tf_region_compare(f)
    old = Dagger.OWNER_COMPUTES_REGIONS[]
    try
        Dagger.owner_computes_regions!(false)
        ref = f()
        Dagger.owner_computes_regions!(true)
        n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        got = f()
        return ref, got, Dagger.OWNER_COMPUTES_REGION_RUNS[] - n0
    finally
        Dagger.owner_computes_regions!(old)
    end
end

@testset "spawn_datadeps regions on the owner-computes path" begin
    n, nb = 96, 16
    @testset "Cholesky ($T, $W)" for T in (Float64, ComplexF64), W in (LowerTriangular, UpperTriangular)
        X = rand(T, n, n); A = X * X' + n * I; A = (A + A') / 2
        chol = () -> begin
            D = distribute(copy(A), Blocks(nb, nb))
            r, info = LinearAlgebra._chol!(D, W)
            (collect(r.data), info)
        end
        ref, got, runs = tf_region_compare(chol)
        F = W === LowerTriangular ? tril : triu
        tf_lowered(runs)
        @test got[2] == 0 && ref[2] == 0
        @test norm(F(got[1]) - F(ref[1])) / norm(A) < 1e-10
    end
    @testset "Cholesky of a matrix that is not positive definite reports the same info" begin
        A = rand(64, 64); A = A + A'
        nonpd = () -> (D = distribute(copy(A), Blocks(16, 16)); LinearAlgebra._chol!(D, LowerTriangular)[2])
        ref, got, runs = tf_region_compare(nonpd)
        tf_lowered(runs)
        @test ref > 0 && got > 0
    end
    @testset "LU without pivoting ($T)" for T in (Float64, ComplexF64)
        A = rand(T, n, n) + n * I
        lunp = () -> begin
            D = distribute(copy(A), Blocks(nb, nb))
            F = lu(D, NoPivot())
            (collect(F.L), collect(F.U))
        end
        ref, got, runs = tf_region_compare(lunp)
        tf_lowered(runs)
        @test norm(got[1] * got[2] - A) / norm(A) < 1e-9
        @test norm(got[1] - ref[1]) / norm(A) < 1e-9
    end
    @testset "matrix product ($T)" for T in (Float64, Float32)
        A = rand(T, 64, 48); B = rand(T, 48, 80)
        ref, got, runs = tf_region_compare(() -> collect(distribute(A, Blocks(16, 16)) * distribute(B, Blocks(16, 16))))
        tf_lowered(runs)
        @test norm(got - A * B) / norm(A * B) < (T === Float32 ? 1e-5 : 1e-12)
    end
    @testset "the decision does not depend on timing (freshly created arrays, used at once)" begin
        # A DArray's chunks are created asynchronously; a region must be lowered whether or not they are finished (ranks could otherwise disagree)
        old = Dagger.OWNER_COMPUTES_REGIONS[]
        try
            Dagger.owner_computes_regions!(true)
            n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
            for _ in 1:10
                X = rand(64, 64); A = X * X' + 64I
                D = distribute(copy(A), Blocks(16, 16))
                _, info = LinearAlgebra._chol!(D, LowerTriangular)
                @test info == 0
            end
            tf_single_process() && @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0 + 10
        finally
            Dagger.owner_computes_regions!(old)
        end
    end
    @testset "non-finite input still fails like Datadeps" begin
        A = rand(48, 48) + 48I; A[3, 3] = NaN
        old = Dagger.OWNER_COMPUTES_REGIONS[]
        try
            for on in (false, true)
                Dagger.owner_computes_regions!(on)
                @test_throws Dagger.DTaskFailedException lu(distribute(copy(A), Blocks(16, 16)), NoPivot())
            end
        finally
            Dagger.owner_computes_regions!(old)
        end
    end
    @testset "task values, use of results, failures, fallback" begin
        old = Dagger.OWNER_COMPUTES_REGIONS[]
        try
            Dagger.owner_computes_regions!(true)
            D = distribute(fill(2.0, 32, 32), Blocks(16, 16)); Ac = D.chunks
            n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
            t = nothing
            Dagger.spawn_datadeps() do
                t = Dagger.@spawn tf_sum!(Dagger.In(Ac[1, 1]))
            end
            tf_single_process() && @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0 + 1
            @test fetch(t) == 2.0 * 16 * 16                                     # fetch gives the value
            # a task of a lowered region is an ordinary argument of a later task: its value is passed
            @test fetch(Dagger.@spawn (x -> x + 1)(t)) == 2.0 * 16 * 16 + 1
            # a failing task raises DTaskFailedException from the region
            @test_throws Dagger.DTaskFailedException Dagger.spawn_datadeps() do
                Dagger.@spawn tf_boom!(Dagger.InOut(Ac[1, 1]))
            end
            # an unsupported argument (a Ref) sends the whole region to the ordinary planner
            n1 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
            r = Ref(0)
            Dagger.spawn_datadeps() do
                Dagger.@spawn tf_noop!(Dagger.In(Ac[1, 1]), r)
            end
            @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n1
            # a task that is not an in-place function may produce a block or a value for others: it is never lowered, and its result stays an ordinary Chunk
            n2 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
            Dagger.spawn_datadeps() do
                Dagger.@spawn sum(Dagger.In(Ac[1, 1]))
            end
            @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n2
            E = distribute(fill(1.0, 32, 32), Blocks(16, 16))
            @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n2
            @test all(c -> fetch(c; raw=true) isa Dagger.Chunk, E.chunks)
        finally
            Dagger.owner_computes_regions!(old)
        end
    end
end

tf_fill!(x, v) = (x .= v; nothing)
tf_boom_if!(x, bad) = (bad && error("boom"); x .= 1.0; nothing)

# Every wait in these tests is bounded: a hang in the code under test must show up as a failed test, not as a stuck run.
function tf_bounded(f; seconds=60)
    t = Threads.@spawn try f() catch e; e end
    timedwait(() -> istaskdone(t), seconds) === :ok || return :timeout
    return fetch(t)
end

@testset "regions on the owner-computes path: default off, fallbacks, nesting, failures" begin
    old = Dagger.OWNER_COMPUTES_REGIONS[]
    try
        @testset "off by default and while the switch is off nothing is lowered" begin
            @test !old
            Dagger.owner_computes_regions!(false)
            D = distribute(fill(2.0, 32, 32), Blocks(16, 16)); Ac = D.chunks
            n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
            t = nothing
            Dagger.spawn_datadeps() do
                t = Dagger.@spawn tf_sum!(Dagger.In(Ac[1, 1]))
            end
            @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0
            @test t.stub === nothing                                 # an ordinary task: has a thunk, no stub
            @test fetch(t) == 2.0 * 16 * 16
            Dagger.owner_computes_regions!(true)
            u = nothing
            Dagger.spawn_datadeps() do
                u = Dagger.@spawn tf_sum!(Dagger.In(Ac[1, 1]))
            end
            tf_single_process() && @test u.stub isa Dagger.TileStub
            @test fetch(u) == fetch(t)                               # same value with the switch on and off
        end
        @testset "views and sub-ranges fall back and compute the right thing" begin
            Dagger.owner_computes_regions!(true)
            D = distribute(zeros(32, 32), Blocks(16, 16)); Ac = D.chunks
            n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
            Dagger.spawn_datadeps() do
                Dagger.@spawn tf_fill!(Dagger.InOut(view(Ac[1, 1], 1:4, 1:4)), 5.0)
            end
            @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0          # not lowered
            M = collect(D)
            @test M[1:4, 1:4] == fill(5.0, 4, 4) && sum(M) == 80.0   # and only the view was written
        end
        @testset "a region inside a region" begin
            Dagger.owner_computes_regions!(true)
            D = distribute(zeros(32, 32), Blocks(16, 16)); Ac = D.chunks
            n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
            r = tf_bounded() do
                Dagger.spawn_datadeps() do
                    Dagger.spawn_datadeps() do
                        Dagger.@spawn tf_fill!(Dagger.InOut(Ac[2, 2]), 1.0)
                    end
                    Dagger.@spawn tf_fill!(Dagger.InOut(Ac[1, 2]), 2.0)
                end
                sum(collect(D))
            end
            @test r == 16 * 16 * 1.0 + 16 * 16 * 2.0
            tf_single_process() && @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0 + 2
        end
        @testset "cancel! of a finished lowered task, then ordinary work" begin
            Dagger.owner_computes_regions!(true)
            D = distribute(fill(1.0, 16, 16), Blocks(16, 16)); Ac = D.chunks
            t = nothing
            Dagger.spawn_datadeps() do
                t = Dagger.@spawn tf_sum!(Dagger.In(Ac[1, 1]))
            end
            @test tf_bounded(() -> (Dagger.cancel!(t); fetch(t))) == 256.0
            @test fetch(Dagger.@spawn (x -> x + 1)(1)) == 2
        end
        @testset "one failing task fails the region, the others do not hide it" begin
            Dagger.owner_computes_regions!(true)
            D = distribute(zeros(48, 48), Blocks(16, 16)); Ac = D.chunks
            r = tf_bounded() do
                try
                    Dagger.spawn_datadeps() do
                        for j in 1:3, i in 1:3
                            Dagger.@spawn tf_boom_if!(Dagger.InOut(Ac[i, j]), i == 2 && j == 2)
                        end
                    end
                    nothing
                catch e
                    e
                end
            end
            @test r isa Dagger.DTaskFailedException
            # and the next region runs normally
            Dagger.spawn_datadeps() do
                Dagger.@spawn tf_fill!(Dagger.InOut(Ac[1, 1]), 7.0)
            end
            @test collect(D)[1:16, 1:16] == fill(7.0, 16, 16)
        end
        @testset "the switches are plain booleans in one process" begin
            for on in (true, false)
                @test Dagger.owner_computes_regions!(on) == on && Dagger.OWNER_COMPUTES_REGIONS[] == on
                @test Dagger.owner_computes_linalg!(on) == on && Dagger.OWNER_COMPUTES_LINALG[] == on
            end
            Dagger.owner_computes_linalg!(false)
        end
    finally
        Dagger.owner_computes_regions!(old)
    end
end
