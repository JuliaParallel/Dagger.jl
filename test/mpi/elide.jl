# Owner elision (`Dagger.elide_foreign_tasks!`): a rank that neither runs a task nor
# owns its data builds no thunk for it. The program must still be indistinguishable:
# same task ids on every rank, same results, same behaviour of later SPMD work.
elide_inc(x::Int) = x + 1                 # concrete, cannot throw: elidable
elide_add(a::Int, b::Int) = a + b
elide_throws(x::Int) = x > 0 ? error("elide_throws") : x   # may throw, concrete Int
elide_rank() = MPI.Comm_rank(MPI.COMM_WORLD)
elide_where() = (MPI.Comm_rank(MPI.COMM_WORLD), Threads.threadid())   # concrete, cannot be proven not to throw

@testset "Owner elision" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    pinned(r) = Dagger.ExactScope(last(filter(p -> p.rank == r, cpus)))
    # Uniformity checks are collective over all ranks and so disable elision
    Dagger.check_uniformity!(false)
    Dagger.elide_foreign_tasks!(true)
    try
        @testset "pinned tasks: results, stubs, uniform ids" begin
            for target in 0:nranks-1
                t = Dagger.@spawn scope=pinned(target) elide_inc(41)
                wait(t)
                @test (t.stub !== nothing) == (rank != target)
                @test t.owner == target
                @test fetch(t) == 42
                @test allequal(MPI.Allgather(Int(t.uid), comm))
                # the placeholder and the real chunk agree on type and space
                chunk = fetch(t; raw=true)
                @test chunk.chunktype === Int
                @test allequal(MPI.Allgather(hash((chunk.handle.rank, chunk.handle.id, chunk.chunktype)), comm))
            end
        end

        @testset "rank-level scope: the scheduler picks the thread" begin
            # `scope(mpi_rank=r)` names a rank, not a thread; the rank is all a spectator needs
            for target in 0:nranks-1
                t = Dagger.@spawn scope=Dagger.scope(mpi_rank=target) elide_inc(1)
                wait(t)
                @test (t.stub !== nothing) == (rank != target)
                @test t.owner == target
                @test fetch(t) == 2
                @test allequal(MPI.Allgather(Int(t.uid), comm))
                chunk = fetch(t; raw=true)
                @test chunk.handle.rank == target
                @test allequal(MPI.Allgather(hash((chunk.handle.rank, chunk.handle.id, chunk.chunktype)), comm))
            end
            # one scope object shared by many tasks (a `with_options` block): the owner spreads them over its threads
            target = nranks - 1
            tasks = Dagger.with_options(; scope=Dagger.scope(mpi_rank=target)) do
                [Dagger.@spawn(elide_where()) for _ in 1:16]
            end
            where = fetch.(tasks)
            @test all(w -> w[1] == target, where)
            @test length(unique(last.(where))) >= min(2, Threads.nthreads())
            @test count(t -> t.stub !== nothing, tasks) == (rank == target ? 0 : 16)
            # a chain across two rank-level scopes, and a failure
            a = Dagger.@spawn scope=Dagger.scope(mpi_rank=0) elide_inc(1)
            b = Dagger.@spawn scope=Dagger.scope(mpi_rank=nranks-1) elide_add(a, 10)
            @test fetch(b) == 12
            bad = Dagger.@spawn scope=Dagger.scope(mpi_rank=0) elide_throws(1)
            @test (bad.stub !== nothing) == (rank != 0)
            @test_throws Exception fetch(bad)
            dep = Dagger.@spawn scope=Dagger.scope(mpi_rank=nranks-1) elide_inc(bad)
            @test_throws Exception fetch(dep)
            # several ranks allowed: the rank is the scheduler's choice, nothing is elided
            multi = Dagger.@spawn scope=Dagger.scope(mpi_ranks=:) elide_inc(1)
            @test multi.stub === nothing
            @test fetch(multi) == 2
        end

        @testset "dependent chains" begin
            # a chain on one rank, then a consumer on another
            a = Dagger.@spawn scope=pinned(0) elide_inc(1)
            b = Dagger.@spawn scope=pinned(0) elide_inc(a)
            c = Dagger.@spawn scope=pinned(nranks-1) elide_add(b, a)
            @test fetch(c) == 3 + 2
            @test (a.stub !== nothing) == (rank != 0)
            @test (c.stub !== nothing) == (rank != nranks - 1 && rank != 0)  # rank 0 owns a and b: it sends them
            # many tasks across all ranks
            tasks = [Dagger.@spawn(scope=pinned(i % nranks), elide_inc(i)) for i in 1:4nranks]
            @test fetch.(tasks) == [i + 1 for i in 1:4nranks]
            @test count(t -> t.stub !== nothing, tasks) == 4nranks - 4
        end

        @testset "tasks that must not be elided" begin
            # a rank that owns an argument takes part
            x = Dagger.tochunk(10, proc_for_rank(0), space_for_rank(0))
            t = Dagger.@spawn scope=pinned(nranks-1) elide_inc(x)
            @test (t.stub !== nothing) == (rank != 0 && rank != nranks - 1)
            @test fetch(t) == 11
            # unknown owner: a task without a pinned scope is placed by the scheduler
            u = Dagger.@spawn elide_inc(1)
            @test u.stub === nothing
            w = Dagger.@spawn scope=pinned(nranks-1) elide_inc(u)
            @test w.stub === nothing
            @test fetch(w) == 3
            # a tagged task keeps the status broadcast
            v = Dagger.@spawn scope=pinned(0) tag=UInt32(12345) elide_inc(1)
            @test v.stub === nothing
            @test fetch(v) == 2
        end

        @testset "tasks that may throw" begin
            # success: elided like any other, nobody waits for a status
            ok = Dagger.@spawn scope=pinned(0) elide_throws(0)
            @test (ok.stub !== nothing) == (rank != 0)
            @test fetch(ok) == 0
            # failure: every rank sees an error, none hangs
            bad = Dagger.@spawn scope=pinned(0) elide_throws(1)
            @test (bad.stub !== nothing) == (rank != 0)
            @test_throws Exception fetch(bad)
            # ... and so does a task that consumes the failed task's result, wherever it runs
            for target in 0:nranks-1
                dep = Dagger.@spawn scope=pinned(target) elide_inc(bad)
                @test_throws Exception fetch(dep)
            end
            # a failure far from the data: the consumer's own rank learns of it on the move
            far = Dagger.@spawn scope=pinned(nranks-1) elide_add(bad, 1)
            @test_throws Exception fetch(far)
        end

        @testset "later SPMD work is unaffected" begin
            y = Dagger.@spawn scope=Dagger.scope(mpi_rank=nranks-1) fill(2.0, 8)
            s = Dagger.@spawn scope=Dagger.scope(mpi_rank=0) sum(y)
            @test fetch(s) == 16.0
            @test allequal(MPI.Allgather(Int(s.uid), comm))
        end
    finally
        Dagger.elide_foreign_tasks!(false)
        Dagger.check_uniformity!(true)
    end
    MPI.Barrier(comm)
end
