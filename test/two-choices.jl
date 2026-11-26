using Dagger, Test, Distributed
import Dagger: ThreadProc, Processor, LockedObject

@testset "Two-choices placement" begin
    pid = myid()
    nproc = 16
    procs = Processor[ThreadProc(pid, t) for t in 1:nproc]
    busy = procs[5]
    pressure = Dict{Processor,Threads.Atomic{UInt64}}(p => Threads.Atomic{UInt64}(UInt64(p === busy ? 10^9 : 0)) for p in procs)
    state = (; worker_time_pressure=LockedObject(Dict{Int,Dict{Processor,Threads.Atomic{UInt64}}}(pid => pressure)))
    sorted = Vector{Processor}(undef, nproc)
    firsts = Set{Processor}()
    for _ in 1:3000
        Dagger.Sch.place_two_choices!(sorted, state, procs, nproc, pid)
        # a permutation of the candidates
        @test length(unique(sorted)) == nproc
        @test Set(sorted) == Set(procs)
        # the loaded processor never wins a comparison, so it is never first
        @test sorted[1] !== busy
        push!(firsts, sorted[1])
    end
    # every other processor is a possible first choice (the choice is random, not fixed)
    @test length(firsts) == nproc - 1

    # a processor with less pressure than its competitor is placed ahead of it
    for p in procs; pressure[p][] = UInt64(0); end
    pressure[procs[1]][] = UInt64(7)
    for _ in 1:500
        Dagger.Sch.place_two_choices!(sorted, state, procs, nproc, pid)
        @test findfirst(==(procs[1]), sorted) >= 2   # worse than every other processor it is compared with
    end

    # no table for this worker yet: still a valid order
    empty_state = (; worker_time_pressure=LockedObject(Dict{Int,Dict{Processor,Threads.Atomic{UInt64}}}()))
    Dagger.Sch.place_two_choices!(sorted, empty_state, procs, nproc, pid)
    @test Set(sorted) == Set(procs)

    # candidates of two workers use the full ranking path
    @test Dagger.Sch.single_worker_pid(procs, Processor) == pid
    if nprocs() > 1
        mixed = Processor[ThreadProc(pid, 1), ThreadProc(first(workers()), 1)]
        @test Dagger.Sch.single_worker_pid(mixed, Processor) == -1
    end
end
