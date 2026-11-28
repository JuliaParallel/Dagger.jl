# Check actual execution as well as scheduler metadata: a rank-wide scope
# used to discard Datadeps' thread choice, sending every task to the first
# thread. MPI disables work stealing, so that choice was never corrected.
mpi_thread_location() = (MPI.Comm_rank(MPI.COMM_WORLD), Threads.threadid())
include("deadline.jl")

@testset "Datadeps thread placement" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    rank_cpus = filter(p -> p.rank == nranks - 1, cpus)
    # Select the last available thread on each rank without assuming thread
    # IDs start at 1 (Julia can also have an interactive thread pool).
    last_cpus = [last(filter(p -> p.rank == r, cpus)) for r in 0:nranks-1]
    thread_scope = Dagger.UnionScope(map(Dagger.ExactScope, last_cpus))
    # Unscoped tasks skip the thread that plans the region (thread 1 of each rank) when other
    # threads exist; tasks that name a processor keep it.
    off_driver = filter(p -> !Dagger.is_driver_thread_proc(p), cpus)
    isempty(off_driver) && (off_driver = cpus)  # one thread per rank: nothing to skip
    @test all(p -> Dagger.is_driver_thread_proc(p) == (p.innerProc.tid == 1), cpus)
    # A task that names the driver thread of every rank (or the one of rank 0) gets it: the skip is for
    # tasks that name nothing
    drivers = filter(Dagger.is_driver_thread_proc, cpus)
    driver_scope = Dagger.UnionScope(map(Dagger.ExactScope, drivers))
    cases = (
        ("default", (;), off_driver),
        ("driver threads named", (; compute_scope=driver_scope), drivers),
        ("one driver thread named", (; scope=Dagger.ExactScope(first(drivers))), [first(drivers)]),
        ("rank scope", (; scope=Dagger.scope(mpi_rank=nranks-1)), rank_cpus),
        ("compute scope", (; compute_scope=thread_scope), last_cpus),
        ("exact scope", (; scope=Dagger.ExactScope(last(cpus))), [last(cpus)]),
    )
    @testset "hierarchical=$hierarchical" for hierarchical in (false, true)
        @testset "$label" for (label, options, allowed) in cases
            # Several full rounds must use every permitted processor evenly.
            tasks = within(120, "thread placement: $label") do
                Dagger.spawn_datadeps(; hierarchical) do
                    Dagger.with_options(; options...) do
                        [Dagger.@spawn(mpi_thread_location()) for _ in 1:4length(allowed)]
                    end
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
