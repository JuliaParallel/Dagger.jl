# Check actual execution as well as scheduler metadata: a rank-wide scope
# used to discard Datadeps' thread choice, sending every task to the first
# thread. MPI disables work stealing, so that choice was never corrected.
mpi_thread_location() = (MPI.Comm_rank(MPI.COMM_WORLD), Threads.threadid())

@testset "Datadeps thread placement" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    rank_cpus = filter(p -> p.rank == nranks - 1, cpus)
    # Select the last available thread on each rank without assuming thread
    # IDs start at 1 (Julia can also have an interactive thread pool).
    last_cpus = [last(filter(p -> p.rank == r, cpus)) for r in 0:nranks-1]
    thread_scope = Dagger.UnionScope(map(Dagger.ExactScope, last_cpus))
    cases = (
        ("default", (;), cpus),
        ("rank scope", (; scope=Dagger.scope(mpi_rank=nranks-1)), rank_cpus),
        ("compute scope", (; compute_scope=thread_scope), last_cpus),
        ("exact scope", (; scope=Dagger.ExactScope(last(cpus))), [last(cpus)]),
    )
    @testset "hierarchical=$hierarchical" for hierarchical in (false, true)
        @testset "$label" for (label, options, allowed) in cases
            # Several full rounds must use every permitted processor evenly.
            tasks = Dagger.spawn_datadeps(; hierarchical) do
                Dagger.with_options(; options...) do
                    [Dagger.@spawn(mpi_thread_location()) for _ in 1:4length(allowed)]
                end
            end
            placements = Dagger.Processor[]
            for task in tasks
                chunk = fetch(task; raw=true)
                proc = chunk.processor
                push!(placements, proc)
                @test proc in allowed
                @test Dagger.check_uniform(proc)
                location = fetch(task)
                if rank == proc.rank
                    @test location == (rank, proc.innerProc.tid)
                end
            end
            @test [count(==(p), placements) for p in allowed] == fill(4, length(allowed))
        end
    end
end
