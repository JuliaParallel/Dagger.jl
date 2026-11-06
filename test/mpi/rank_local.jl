# Tasks spawned with `acceleration=DistributedAcceleration()` run only on the
# rank that spawns them, outside SPMD execution. Ranks may spawn different
# numbers of them; they must neither land on MPI processors nor shift the task
# IDs (and so the MPI tags) of the uniform tasks that follow.
mpi_local_location(i) = (MPI.Comm_rank(MPI.COMM_WORLD), i)

@testset "Rank-local tasks" begin
    local_accel = Dagger.DistributedAcceleration()
    for round in 1:2
        # A different number of local tasks on every rank
        n = 2 + 3rank
        tasks = [Dagger.@spawn(acceleration=local_accel, mpi_local_location(i)) for i in 1:n]
        for (i, task) in enumerate(tasks)
            @test fetch(task) == (rank, i)
            proc = fetch(task; raw=true).processor
            @test proc isa Dagger.ThreadProc
        end
        tasks = Dagger.with_options(; acceleration=local_accel) do
            [Dagger.@spawn(mpi_local_location(i)) for i in 1:n]
        end
        @test map(fetch, tasks) == [(rank, i) for i in 1:n]

        # Uniform work afterwards still agrees across ranks
        x = Dagger.@spawn scope=Dagger.scope(mpi_rank=nranks-1) fill(Float64(round), 100)
        @test fetch(Dagger.@spawn scope=Dagger.scope(mpi_rank=0) sum(x)) == 100round
        @test sum(fetch.([Dagger.@spawn(mpi_local_location(i)) for i in 1:4]) .|> last) == 10

        # A local task can use a uniform result owned by its own rank
        mine = [Dagger.@spawn(scope=Dagger.scope(mpi_rank=r), fill(Float64(r + 1), 10)) for r in 0:nranks-1]
        @test fetch(Dagger.@spawn(acceleration=local_accel, sum(mine[rank+1]))) == 10(rank + 1)
        MPI.Barrier(comm)
    end
end
