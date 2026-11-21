# Plain eager tasks need a uniform placement decision even when ranks make
# progress at different speeds. Check actual execution, scopes, and locality.
mpi_spawn_location(x=nothing) = (MPI.Comm_rank(MPI.COMM_WORLD), Threads.threadid(), x)
mpi_spawn_value() = (sleep(0.05); 42)
mpi_spawn_value_fast(i) = 42 + i - i
mpi_spawn_checked(i::Int) = i > 0 ? i : error("negative")

function check_spawn_placements(tasks, allowed, value=nothing)
    placements = Dagger.Processor[]
    for task in tasks
        proc = fetch(task; raw=true).processor
        push!(placements, proc)
        @test proc in allowed
        @test Dagger.check_uniform(proc)
        @test fetch(task) == (proc.rank, proc.innerProc.tid, value)
    end
    @test [count(==(p), placements) for p in allowed] == fill(4, length(allowed))
end

@testset "Plain spawn placement" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    target = nranks - 1
    rank_cpus = filter(p -> p.rank == target, cpus)
    last_cpus = [last(filter(p -> p.rank == r, cpus)) for r in 0:nranks-1]
    thread_scope = Dagger.UnionScope(map(Dagger.ExactScope, last_cpus))
    cases = (
        ("default", (;), cpus),
        ("all ranks", (; scope=Dagger.scope(mpi_ranks=:)), cpus),
        ("rank scope", (; scope=Dagger.scope(mpi_rank=target)), rank_cpus),
        ("compute scope", (; compute_scope=thread_scope), last_cpus),
        ("exact scope", (; scope=Dagger.ExactScope(last(cpus))), [last(cpus)]),
    )
    @testset "Canonical processor listing" begin
        procs = Dagger.Processor[reverse(cpus)...]
        @test @inferred(Dagger.select_processors_uniform!(procs, accel)) === procs
        @test procs == cpus
        task = Dagger.Thunk(mpi_spawn_location; id=10)
        @test @inferred(Dagger.select_processors_uniform!(procs, accel, task, nothing)) === procs
        empty!(procs)
        @test Dagger.select_processors_uniform!(procs, accel, task, nothing) === procs
    end
    # Start the scheduler before checking complete rounds of consecutive IDs.
    fetch(Dagger.@spawn mpi_spawn_location())
    @testset "uniformity checks=$checks" for checks in (true, false)
        try
            @testset "$label" for (label, options, allowed) in cases
                Dagger.check_uniformity!(checks)
                tasks = Dagger.with_options(; options...) do
                    [Dagger.@spawn(mpi_spawn_location()) for _ in 1:4length(allowed)]
                end
                foreach(wait, tasks)
                Dagger.check_uniformity!(true)
                check_spawn_placements(tasks, allowed)
            end

            @testset "Chunk owner preference" begin
                chunk = Dagger.tochunk(42, proc_for_rank(target), space_for_rank(target))
                Dagger.check_uniformity!(checks)
                tasks = [Dagger.@spawn(mpi_spawn_location(chunk)) for _ in 1:4length(rank_cpus)]
                foreach(wait, tasks)
                Dagger.check_uniformity!(true)
                check_spawn_placements(tasks, rank_cpus, 42)

                # A scope can exclude the input's owner. Fall back to the
                # permitted rank, spreading across all its threads.
                allowed = filter(p -> p.rank == 0, cpus)
                Dagger.check_uniformity!(checks)
                tasks = Dagger.with_options(; scope=Dagger.scope(mpi_rank=0)) do
                    [Dagger.@spawn(mpi_spawn_location(chunk)) for _ in 1:4length(allowed)]
                end
                foreach(wait, tasks)
                Dagger.check_uniformity!(true)
                check_spawn_placements(tasks, allowed, 42)
            end

            @testset "DTask owner preference" begin
                Dagger.check_uniformity!(checks)
                producer = Dagger.@spawn scope=Dagger.ExactScope(last(rank_cpus)) mpi_spawn_value()
                tasks = [Dagger.@spawn(mpi_spawn_location(producer)) for _ in 1:4length(rank_cpus)]
                foreach(wait, tasks)
                Dagger.check_uniformity!(true)
                check_spawn_placements(tasks, rank_cpus, 42)
            end
        finally
            Dagger.check_uniformity!(true)
        end
    end
end

# The ref-id bookkeeping of a task that is not part of a Datadeps region is dropped when the
# task is done; it used to grow by one entry per task for the whole session
reftid_size() = lock(d -> length(d), MPIExt._MPIREF_TID)
@testset "Ref-id table does not grow with plain tasks" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    foreach(fetch, [Dagger.@spawn(mpi_spawn_value_fast(i)) for i in 1:8])   # warm up
    MPI.Barrier(comm)
    before = reftid_size()
    # unscoped, pinned to every rank in turn, and a body that may throw
    foreach(wait, [Dagger.@spawn(mpi_spawn_value_fast(i)) for i in 1:100])
    foreach(wait, [Dagger.@spawn(scope=Dagger.ExactScope(cpus[mod1(i, length(cpus))]), mpi_spawn_value_fast(i)) for i in 1:100])
    foreach(wait, [Dagger.@spawn(mpi_spawn_checked(i)) for i in 1:100])
    MPI.Barrier(comm)
    # Nothing is left behind by 300 tasks (a few entries may belong to tasks still finishing)
    @test reftid_size() - before < 20
    # and ids are still uniform afterwards
    t = Dagger.@spawn mpi_spawn_value_fast(1)
    @test fetch(t) == 42
    @test allequal(MPI.Allgather(Int(t.uid), comm))
end
