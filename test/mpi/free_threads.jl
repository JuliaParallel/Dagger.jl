# An MPI task whose thread nobody asked for runs on any free
# thread of its rank instead of waiting behind a busy one.
hog_busy(seconds::Float64) = (t0 = time(); while time() - t0 < seconds; end; 1)   # does not yield
free_quick(x::Int) = x + 1
free_where() = Threads.threadid()

@testset "Free MPI threads" begin
    if Threads.nthreads() >= 3
        cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
        rank0 = sort(filter(p -> p.rank == 0, cpus); by=p -> p.innerProc.tid)
        # Warm up first: the first fetch of a task compiles for a second on slow nodes, which would be measured otherwise
        foreach(fetch, Dagger.with_options(; scope=Dagger.scope(mpi_rank=0)) do
            [Dagger.@spawn(free_quick(i)) for i in 1:16]
        end)
        foreach(fetch, [Dagger.@spawn(scope=Dagger.ExactScope(p), hog_busy(0.01)) for p in rank0])
        MPI.Barrier(comm)
        # Occupy every thread of rank 0 but the first with a pinned (so bound) task that never yields
        hogs = [Dagger.@spawn(scope=Dagger.ExactScope(p), hog_busy(2.5)) for p in rank0[2:end]]
        sleep(0.3)
        MPI.Barrier(comm)
        t0 = time()
        # Scope = the whole rank, 16 tasks: the scheduler names every thread in turn, busy ones included
        tasks = Dagger.with_options(; scope=Dagger.scope(mpi_rank=0)) do
            [Dagger.@spawn(free_quick(i)) for i in 1:16]
        end
        vals = fetch.(tasks)
        elapsed = time() - t0
        @test vals == [i + 1 for i in 1:16]
        # Before tasks ran on free threads, the tasks the scheduler places on a busy thread wait for the hogs (measured: 3.3 s of 4 s)
        rank == 0 && @test elapsed < 1.5       # the hogs still have a second to run
        # An explicit thread is still a promise
        where = fetch(Dagger.@spawn scope=Dagger.ExactScope(last(rank0)) free_where())
        @test where == last(rank0).innerProc.tid
        foreach(wait, hogs)
    end
    MPI.Barrier(comm)
end

# A scope that names some threads of a rank is a request for one of them, not only an
# `ExactScope`: the task must stay on a thread of the scope even while all of them are busy.
@testset "Free MPI threads respect a scope that names threads" begin
    if Threads.nthreads() >= 3
        cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
        rank0 = sort(filter(p -> p.rank == 0, cpus); by=p -> p.innerProc.tid)
        subset = rank0[2:3]
        subset_tids = [p.innerProc.tid for p in subset]
        subset_scope = Dagger.UnionScope(map(Dagger.ExactScope, subset))
        # Warm up the task shapes first
        foreach(fetch, [Dagger.@spawn(scope=subset_scope, free_where()) for _ in 1:4])
        foreach(fetch, [Dagger.@spawn(scope=Dagger.ExactScope(p), hog_busy(0.01)) for p in subset])
        MPI.Barrier(comm)
        # Both threads of the scope are busy for a while. A task that did not care would run on
        # another thread at once; these must wait for one of the two.
        hogs = [Dagger.@spawn(scope=Dagger.ExactScope(p), hog_busy(1.0)) for p in subset]
        sleep(0.3)
        MPI.Barrier(comm)
        tasks = [Dagger.@spawn(scope=subset_scope, free_where()) for _ in 1:6]
        tids = fetch.(tasks)
        @test all(in(subset_tids), tids)
        # The same through `compute_scope`, and with a scope that names every thread of the rank
        tasks = [Dagger.@spawn(compute_scope=subset_scope, free_where()) for _ in 1:6]
        @test all(in(subset_tids), fetch.(tasks))
        foreach(wait, hogs)
        all_threads = Dagger.UnionScope(map(Dagger.ExactScope, rank0))
        MPI.Barrier(comm)
        hogs = [Dagger.@spawn(scope=Dagger.ExactScope(p), hog_busy(2.5)) for p in rank0[2:end]]
        sleep(0.3)
        MPI.Barrier(comm)
        t0 = time()
        vals = fetch.([Dagger.@spawn(scope=all_threads, free_quick(i)) for i in 1:8])
        @test vals == [i + 1 for i in 1:8]
        rank == 0 && @test time() - t0 < 1.5    # the hogs still have a second to run
        foreach(wait, hogs)
    end
    MPI.Barrier(comm)
end

# The task of an MPI processor starts pinned to the thread of its inner processor, so
# `execute!(::ThreadProc)` runs the function inline instead of creating a second pinned task
# and waiting for it (measured: 160-190 us per task). A task that asked for no particular
# thread starts migratable.
@testset "MPI task starts pinned to its processor's thread" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    mine = first(sort(filter(p -> p.rank == rank, cpus); by=p -> p.innerProc.tid))
    others = filter(p -> p.rank != rank, cpus)
    tid_for = Dagger.Sch.task_tid_for_processor
    spec(scope) = (; scope = scope)    # the hook only reads `task.scope`
    # (a rank with one thread has none to choose from: its exact scope leaves nothing out)
    nthr = count(p -> p.rank == rank, cpus)
    @test tid_for(mine, spec(Dagger.ExactScope(mine))) == (nthr > 1 ? mine.innerProc.tid : nothing)
    # A task that did not ask for a thread is not pinned
    @test tid_for(mine, spec(nothing)) === nothing
    @test tid_for(mine, spec(Dagger.DefaultScope())) === nothing
    @test tid_for(mine, spec(Dagger.AnyScope())) === nothing
    # A processor of another rank has no thread here
    isempty(others) || @test tid_for(first(others), spec(Dagger.ExactScope(first(others)))) === nothing
    # And it runs where it was pinned
    if rank == 0
        @test fetch(Dagger.@spawn(scope=Dagger.ExactScope(mine), free_where())) == mine.innerProc.tid
    else
        fetch(Dagger.@spawn(scope=Dagger.ExactScope(first(filter(p -> p.rank == 0, cpus))), free_where()))
    end
    MPI.Barrier(comm)
end

# In a job of one rank every task is on a communicator of one rank: nobody to tell about its
# status or result type, so none is sent. The result must still be right, and a failure rethrown.
if nranks == 1
    @testset "Task on a one-rank communicator" begin
        @test fetch(Dagger.@spawn(free_quick(41))) == 42
        @test fetch(Dagger.@spawn(identity(nothing))) === nothing
        t = Dagger.@spawn(error("one-rank failure"))
        @test_throws Exception fetch(t)
        @test fetch(Dagger.@spawn(free_quick(1))) == 2   # still working afterwards
    end
end

# The acceleration keeps freed tile memory in the heap (glibc mallopt); it is a no-op elsewhere
@testset "tune_malloc!" begin
    ok = Dagger.tune_malloc!()
    @test ok isa Bool
    # Idempotent
    @test Dagger.tune_malloc!() == ok
    # Allocating and freeing a tile-sized libc buffer still works with the settings in place
    A = Dagger.alloc_libc_array(Float64, (256, 256))
    A .= 1.0
    @test sum(A) == 256 * 256
end
