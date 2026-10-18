# Owner elision (`Dagger.elide_foreign_tasks!`): a rank that neither runs a task nor
# owns its data builds no thunk for it. The program must still be indistinguishable:
# same task ids on every rank, same results, same behaviour of later SPMD work.
elide_inc(x::Int) = x + 1                 # concrete, cannot throw: elidable
elide_add(a::Int, b::Int) = a + b
elide_throws(x::Int) = x > 0 ? error("elide_throws") : x   # may throw, concrete Int
elide_len(x::Vector{Int}) = length(x)    # concrete, cannot throw
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

        @testset "tasks without a scope: placed at spawn" begin
            # The owner follows the deterministic placement rule and is decided when the
            # task is created, so every other rank can elide it. The scheduler must
            # then run it exactly where the rule said.
            n = nranks * Threads.nthreads()
            tasks = [Dagger.@spawn(elide_inc(i)) for i in 1:2n]
            @test fetch.(tasks) == [i + 1 for i in 1:2n]
            @test all(t -> t.owner >= 0, tasks)
            @test all(t -> fetch(t; raw=true).handle.rank == t.owner, tasks)
            @test all(t -> allequal(MPI.Allgather(Int(t.uid), comm)), tasks[1:4])
            @test count(t -> t.stub !== nothing, tasks) == count(t -> t.owner != rank, tasks)
            @test all(r -> any(t -> t.owner == r, tasks), 0:nranks-1)   # round robin reaches every rank
            # data affinity: a task that reads an MPI chunk runs on the chunk's rank
            x = Dagger.tochunk(10, proc_for_rank(0), space_for_rank(0))
            y = Dagger.@spawn elide_inc(x)
            @test y.owner == 0
            @test fetch(y) == 11
            @test (y.stub !== nothing) == (rank != 0)
            # a chain without scopes stays on one rank, and a scoped producer is followed
            u1 = Dagger.@spawn scope=pinned(nranks-1) elide_inc(1)
            u2 = Dagger.@spawn elide_inc(u1)
            u3 = Dagger.@spawn elide_add(u2, u1)
            @test u2.owner == nranks - 1 && u3.owner == nranks - 1
            @test fetch(u3) == 3 + 2
            # an argument whose owner is unknown at spawn time: left to the scheduler
            lazy = Dagger.@spawn scope=Dagger.scope(mpi_ranks=:) elide_inc(1)
            z = Dagger.@spawn elide_inc(lazy)
            @test lazy.stub === nothing && z.stub === nothing
            @test fetch(z) == 3
            # a failing task without a scope: every rank sees an error and none hangs
            bad = Dagger.@spawn elide_throws(1)
            @test_throws Exception fetch(bad)
            dep = Dagger.@spawn elide_inc(bad)
            @test_throws Exception fetch(dep)
        end

        @testset "chunk arguments with a scope restriction (as a DArray block has)" begin
            # A chunk owned by `target` whose scope names that rank: a rank that neither runs the task nor
            # owns the chunk elides the task. The restriction only matters if it contradicts the task's scope.
            # N.B. `tochunk(x, proc, scope)` would place the chunk on rank 0 (the owner of plain values), so the memory
            # space is given as well.
            target = nranks - 1
            data = fill(5, 4)
            restricted = Dagger.tochunk(data, proc_for_rank(target), space_for_rank(target), Dagger.scope(mpi_rank=target))
            t = Dagger.@spawn scope=Dagger.scope(mpi_rank=target) elide_len(restricted)
            wait(t)
            @test (t.stub !== nothing) == (rank != target)
            @test fetch(t) == 4
            @test allequal(MPI.Allgather(Int(t.uid), comm))
            # ... and when the chunk is also an argument of a task pinned to one processor of that rank
            t2 = Dagger.@spawn scope=pinned(target) elide_len(restricted)
            wait(t2)
            @test (t2.stub !== nothing) == (rank != target)
            @test fetch(t2) == 4
            if nranks > 1
                # A scope that contradicts the task's is not elided: the scheduler of the owner fails the task
                other = (target + 1) % nranks
                clash = Dagger.tochunk(data, proc_for_rank(other), space_for_rank(other), Dagger.scope(mpi_rank=other))
                t3 = Dagger.@spawn scope=Dagger.scope(mpi_rank=target) elide_len(clash)
                @test t3.stub === nothing
                @test_throws Exception fetch(t3)
                @test allequal(MPI.Allgather(Int(t3.uid), comm))
            end
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
