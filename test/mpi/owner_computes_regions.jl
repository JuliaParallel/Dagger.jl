# spawn_datadeps regions on the owner-computes layer over MPI (included by cpu.jl, which has set up MPI acceleration and `comm`): unchanged library code,
# results equal the Datadeps path, the region counter proves the layer ran, failures are agreed on every rank, and the contract holds.
# Only in-place (`!`) functions are lowered (`Dagger._region_inplace`)
mpi_region_sum!(x) = sum(x)
mpi_region_boom!(x) = (x[1] = 1.0; error("boom"))
mpi_region_copy!(dst, src) = (copyto!(dst, src); nothing)

@testset "Owner-computes regions (MPI)" begin
    old = Dagger.OWNER_COMPUTES_REGIONS[]
    me = MPI.Comm_rank(comm)
    @test !Dagger.OWNER_COMPUTES_REGIONS[]                      # off by default
    let n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        D = zeros(Blocks(16, 16), Float64, 32, 32)
        Dagger.spawn_datadeps() do
            Dagger.@spawn mpi_region_sum!(Dagger.In(D.chunks[1, 1]))
        end
        @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0         # nothing is lowered unless asked for
    end
    function cmp(f)
        Dagger.owner_computes_regions!(false); ref = f()
        Dagger.owner_computes_regions!(true); n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        got = f(); runs = Dagger.OWNER_COMPUTES_REGION_RUNS[] - n0
        Dagger.owner_computes_regions!(false)
        return ref, got, runs
    end
    try
        n, nb = 96, 16
        Random.seed!(11)
        for T in (Float64, ComplexF64), W in (LowerTriangular, UpperTriangular)
            X = rand(T, n, n); A = X * X' + n * I; A = (A + A') / 2
            chol = () -> begin
                D = zeros(Blocks(nb, nb), T, n, n); copyto!(D, A)
                r, info = LinearAlgebra._chol!(D, W)
                (collect(r.data), info)
            end
            ref, got, runs = cmp(chol)
            F = W === LowerTriangular ? tril : triu
            @test runs >= 1
            @test got[2] == 0 && ref[2] == 0
            @test norm(F(got[1]) - F(ref[1])) / norm(A) < 1e-10
        end
        # not positive definite: the same nonzero info on every rank, nobody hangs
        A = rand(64, 64); A = A + A'
        nonpd = () -> (D = zeros(Blocks(16, 16), Float64, 64, 64); copyto!(D, A); LinearAlgebra._chol!(D, LowerTriangular)[2])
        # N.B. the Datadeps path itself is not uniform here
        ref, got, runs = cmp(nonpd)
        @test runs >= 1
        @test got > 0
        infos = MPI.Allgather(Int(got), comm)
        @test all(==(infos[1]), infos)
        # LU without pivoting and a matrix product
        A = rand(n, n) + n * I
        lunp = () -> (D = zeros(Blocks(nb, nb), Float64, n, n); copyto!(D, A); F = lu(D, NoPivot()); (collect(F.L), collect(F.U)))
        ref, got, runs = cmp(lunp)
        @test runs >= 1
        @test norm(got[1] * got[2] - A) / norm(A) < 1e-9
        Am = rand(64, 48); Bm = rand(48, 80)
        mm = () -> (Da = zeros(Blocks(16, 16), Float64, 64, 48); copyto!(Da, Am); Db = zeros(Blocks(16, 16), Float64, 48, 80); copyto!(Db, Bm); collect(Da * Db))
        ref, got, runs = cmp(mm)
        @test runs >= 1
        @test norm(got - Am * Bm) / norm(Am * Bm) < 1e-12
        # the value of a task of a lowered region is the same on EVERY rank (fetch is collective), and it can be passed on to a later task
        Dagger.owner_computes_regions!(true)
        D2 = zeros(Blocks(16, 16), Float64, 48, 48); copyto!(D2, fill(2.0, 48, 48))
        Ac = D2.chunks
        ts = Any[]
        n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        Dagger.spawn_datadeps() do
            for j in 1:3, i in 1:3
                push!(ts, Dagger.@spawn mpi_region_sum!(Dagger.In(Ac[i, j])))
            end
        end
        @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0 + 1
        vals = [fetch(t) for t in ts]
        @test all(==(2.0 * 16 * 16), vals)                                  # the same value on every rank, whichever rank ran the task
        @test fetch(Dagger.@spawn (x -> x + 1)(ts[end])) == 2.0 * 16 * 16 + 1
        Dagger.owner_computes_regions!(false)
        # a failing kernel inside a lowered region reaches the caller on EVERY rank, and nobody hangs
        Dagger.owner_computes_regions!(true)
        Db = zeros(Blocks(16, 16), Float64, 48, 48)
        n1 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        @test_throws Dagger.DTaskFailedException Dagger.spawn_datadeps() do
            for j in 1:3, i in 1:3
                Dagger.@spawn mpi_region_boom!(Dagger.InOut(Db.chunks[i, j]))
            end
        end
        @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n1 + 1
        # a task whose two tile arguments live on different ranks (the chunks are placed round robin)
        Dagger.owner_computes_regions!(true)
        Dc = zeros(Blocks(16, 16), Float64, 48, 48); copyto!(Dc, fill(3.0, 48, 48))
        Dd = zeros(Blocks(16, 16), Float64, 48, 48)
        owners = [fetch(Dc.chunks[i, j]; raw=true).handle.rank for i in 1:3, j in 1:3]
        @test length(unique(owners)) > 1
        Dagger.spawn_datadeps() do
            for j in 1:3, i in 1:3
                Dagger.@spawn mpi_region_copy!(Dagger.Out(Dd.chunks[i, 3-j+1]), Dagger.In(Dc.chunks[i, j]))
            end
        end
        @test collect(Dd) == fill(3.0, 48, 48)
        Dagger.owner_computes_regions!(false)
        # a non-finite input fails with DTaskFailedException on every rank
        Anan = rand(48, 48) + 48I; Anan[3, 3] = NaN
        Dagger.owner_computes_regions!(true)
        D = zeros(Blocks(16, 16), Float64, 48, 48); copyto!(D, Anan)
        @test_throws Dagger.DTaskFailedException lu(D, NoPivot())
        Dagger.owner_computes_regions!(false)
    finally
        Dagger.owner_computes_regions!(old)
    end
end

mpi_region_fill!(x, v) = (x .= v; nothing)
mpi_region_boom_if!(x, bad) = (bad && error("boom"); x .= 1.0; nothing)

# Every section is bounded: the work runs on the calling task (chunks must be made there, and the acceleration is task-local), and a watchdog on the
# interactive thread ends the process with a failure if it has not finished in time, so a hang in the code under test cannot go unnoticed.
function mpi_bounded(f; seconds=120, label="section")
    done = Threads.Atomic{Bool}(false)
    wd = Threads.@spawn :interactive begin
        t0 = time()
        while !done[] && time() - t0 < seconds; sleep(0.2); end
        if !done[]
            println(stderr, "WATCHDOG: owner-computes regions test section '", label, "' did not finish in ", seconds, " s on rank ", MPI.Comm_rank(MPI.COMM_WORLD))
            exit(124)
        end
    end
    try
        return try f() catch e; e end
    finally
        done[] = true
        wait(wd)
    end
end

@testset "Owner-computes regions (MPI): switch agreement, off path, fallbacks, one failing tile, communicator check" begin
    old = Dagger.OWNER_COMPUTES_REGIONS[]
    oldl = Dagger.OWNER_COMPUTES_LINALG[]
    try
        Dagger.owner_computes_regions!(false); Dagger.owner_computes_linalg!(false)
        # A switch set differently on different ranks is an error on EVERY rank (the setter is a collective), not a hang later. The value is unchanged.
        for setter in (Dagger.owner_computes_regions!, Dagger.owner_computes_linalg!)
            r = mpi_bounded(() -> setter(rank == 0))
            if nranks > 1
                @test r isa ArgumentError
                @test !Dagger.OWNER_COMPUTES_REGIONS[] && !Dagger.OWNER_COMPUTES_LINALG[]
            end
            @test mpi_bounded(() -> setter(false)) == false
        end
        # with the switch off the region goes through the planner on every rank: no stub, the counter stays, the value is right
        D = zeros(Blocks(16, 16), Float64, 48, 48); copyto!(D, fill(2.0, 48, 48)); Ac = D.chunks
        n0 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        t = nothing
        Dagger.spawn_datadeps() do
            t = Dagger.@spawn mpi_region_sum!(Dagger.In(Ac[1, 1]))
        end
        @test t.stub === nothing && Dagger.OWNER_COMPUTES_REGION_RUNS[] == n0
        @test fetch(t) == 2.0 * 256
        # views fall back on every rank (the same decision everywhere) and write only the view
        Dagger.owner_computes_regions!(true)
        n1 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        Dv = zeros(Blocks(16, 16), Float64, 48, 48); Av = Dv.chunks
        r = mpi_bounded() do
            Dagger.spawn_datadeps() do
                Dagger.@spawn mpi_region_fill!(Dagger.InOut(view(Av[1, 1], 1:4, 1:4)), 5.0)
            end
            collect(Dv)
        end
        @test r isa Matrix && sum(r) == 80.0 && r[1:4, 1:4] == fill(5.0, 4, 4)
        @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n1
        # exactly one tile's kernel fails (so only its owner sees the exception): every rank still gets DTaskFailedException, nobody hangs,
        # and the next region works
        Df = zeros(Blocks(16, 16), Float64, 48, 48); Af = Df.chunks
        n2 = Dagger.OWNER_COMPUTES_REGION_RUNS[]
        r = mpi_bounded() do
            try
                Dagger.spawn_datadeps() do
                    for j in 1:3, i in 1:3
                        Dagger.@spawn mpi_region_boom_if!(Dagger.InOut(Af[i, j]), i == 2 && j == 3)
                    end
                end
                nothing
            catch e
                e
            end
        end
        @test r isa Dagger.DTaskFailedException
        @test Dagger.OWNER_COMPUTES_REGION_RUNS[] == n2 + 1
        r = mpi_bounded() do
            Dagger.spawn_datadeps() do
                Dagger.@spawn mpi_region_fill!(Dagger.InOut(Af[1, 1]), 7.0)
            end
            collect(Df)[1:16, 1:16]
        end
        @test r == fill(7.0, 16, 16)
        # A chunk belongs to one communicator, and a region is lowered only on the communicator of its tiles. (Running a whole region on another
        # communicator needs the planner to support one, which this base does not, so the decision is checked directly.)
        dup = MPI.Comm_dup(comm)
        wchunk = fetch(Af[1, 1]; raw=true)
        @test Dagger.tile_chunk_transport_ok(MPIExt.MPITileTransport(comm), wchunk)
        @test !Dagger.tile_chunk_transport_ok(MPIExt.MPITileTransport(dup), wchunk)
        MPI.Barrier(comm)
    finally
        Dagger.owner_computes_regions!(old); Dagger.owner_computes_linalg!(oldl)
    end
end
