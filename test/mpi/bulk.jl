# `Dagger.dynamic_for` under MPI: the workers run on one rank (their shared counter lives there),
# every index runs exactly once, and a failure in `f` reaches every rank.
const BULK_N = 4000
const bulk_hits = zeros(Int, BULK_N)
bulk_mark(i) = (bulk_hits[i] += 1; nothing)
bulk_gate(n) = n
bulk_mark_with(i, n) = (i <= n && (bulk_hits[i] += 1); nothing)
bulk_report() = (count(==(1), bulk_hits), count(>(1), bulk_hits), count(==(0), bulk_hits))
bulk_item(i) = [i]

@testset "Dynamic loop under MPI" begin
    last_rank = MPI.Comm_size(comm) - 1
    scope = Dagger.scope(mpi_rank=last_rank)
    fill!(bulk_hits, 0)
    MPI.Barrier(comm)
    Dagger.dynamic_for(bulk_mark, BULK_N; scope)
    ones_, dups, misses = fetch(Dagger.@spawn(scope=scope, bulk_report()))
    @test (ones_, dups, misses) == (BULK_N, 0, 0)
    @test_throws Exception Dagger.dynamic_for(i -> i == 3 ? error("boom") : nothing, 100; scope)

    # task arguments: the loop starts once its DTask argument exists, on the rank of the scope
    fill!(bulk_hits, 0)
    MPI.Barrier(comm)
    gate = Dagger.@spawn scope=scope bulk_gate(BULK_N)
    Dagger.dynamic_for(bulk_mark_with, BULK_N, gate; scope)
    @test fetch(Dagger.@spawn(scope=scope, bulk_report())) == (BULK_N, 0, 0)
end
MPI.Barrier(comm)

# The default scope of a loop is "this rank", which is ambiguous in a graph shared by several ranks (every rank plans the same
# tasks, so a per-rank default would make them disagree about where a worker runs): an error there, not a silent default.
# `Dagger.scope(worker=myid())`, the Distributed default, matches no MPI processor.
@testset "Dynamic loop: default scope under MPI" begin
    MPI.Barrier(comm)
    if MPI.Comm_size(comm) > 1
        @test_throws ArgumentError Dagger.dynamic_for(i -> nothing, 60)
        @test_throws ArgumentError Dagger.dynamic_reduce(bulk_item, vcat, 30; init=Int[], chunk=1)
    else
        @test sum(Dagger.dynamic_for(i -> nothing, 60)) == 60
        @test Dagger.dynamic_reduce(bulk_item, vcat, 30; init=Int[], chunk=1) == collect(1:30)
        @test Dagger.local_scope() isa Dagger.UnionScope
    end
end
MPI.Barrier(comm)

# One scope per worker: a scope naming ONE processor binds its worker to that thread, so a vector of exact scopes pins each
# worker to its own thread, e.g. to keep the loop off the driver's thread 1.
const bulk_seen = zeros(Int, Threads.maxthreadid())
bulk_seen_work(i) = (bulk_seen[Threads.threadid()] += 1; t0 = time_ns(); while time_ns() - t0 < 5e6; end; nothing)
bulk_seen_report() = copy(bulk_seen)
@testset "Dynamic loop: one scope per worker pins the workers" begin
    owner = MPI.Comm_size(comm) - 1
    procs = sort(collect(Dagger.get_processors(MPIExt.MPIOSProc(comm, owner))); by=p -> p.innerProc.tid)
    if length(procs) >= 3
        scopes = Dagger.AbstractScope[Dagger.ExactScope(p) for p in procs[2:end]]
        fill!(bulk_seen, 0)
        MPI.Barrier(comm)
        Dagger.dynamic_for(bulk_seen_work, 60; workers=length(scopes), chunk=1, scope=scopes)
        seen = fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=owner), bulk_seen_report()))
        @test seen[1] == 0                                  # never on thread 1
        @test count(>(0), seen) == length(scopes)           # every worker on its own thread
        @test sum(seen) == 60
    end
end
MPI.Barrier(comm)

# A loop over a scope that covers several threads of one rank is pinned by Dagger itself (one worker per thread), and the driver
# thread is left alone when the rank has three or more: no convoy behind tasks that are bound to thread 1.
@testset "Dynamic loop over a rank scope pins one worker per thread" begin
    owner = MPI.Comm_size(comm) - 1
    nth = length(Dagger.get_processors(MPIExt.MPIOSProc(comm, owner)))
    if nth >= 2
        ws = nth >= 3 ? nth - 1 : nth
        fill!(bulk_seen, 0)
        MPI.Barrier(comm)
        Dagger.dynamic_for(bulk_seen_work, 60; workers=ws, chunk=1, scope=Dagger.scope(mpi_rank=owner))
        seen = fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=owner), bulk_seen_report()))
        nth >= 3 && @test seen[1] == 0
        @test count(>(0), seen) == ws
        @test sum(seen) == 60
        r = Dagger.dynamic_reduce(i -> (bulk_seen_work(i); [i]), vcat, 40; init=Int[], chunk=1, workers=ws, scope=Dagger.scope(mpi_rank=owner))
        @test r == collect(1:40)
    end
end
MPI.Barrier(comm)
