# Cooperative collectives: barrier, bcast, gather, allgather, scatter, alltoall, allreduce, reduce_to.
# They wait with MPI.Test and yield, so no OS thread is parked inside MPI.

const CollSerialization = Base.require(Base.PkgId(Base.UUID("9e88b42a-f829-5b0c-bbe9-9e923198166b"), "Serialization"))
struct CollBad end
CollSerialization.serialize(::CollSerialization.AbstractSerializer, ::CollBad) = error("CollBad does not serialize")

struct CollPoint
    x::Float64
    tag::String
end

coll_payload(r::Int) = r == 1 ? Float64[] : fill(Float64(r), 1000 * r)         # rank 0 contributes an empty array
coll_part(i::Int) = 1 / (1 + i)^1.3 * (iseven(i) ? -1.0 : 1.0)
coll_last_thread(r) = Dagger.ExactScope(last(sort(filter(p -> p.rank == r && p.innerProc isa Dagger.ThreadProc, mpi_procs());
                                                  by=p -> p.innerProc.tid)))
coll_roots() = unique((0, nranks - 1, min(1, nranks - 1)))
# what rank `src` sends to rank `dst` in the scatter / alltoall tests: sizes differ, some are empty, some are not bits values
coll_item(src::Int, dst::Int) = (src + dst) % 3 == 0 ? Float64[] : (src + dst) % 3 == 1 ? fill(Float64(10src + dst), 300 * (dst + 1)) : CollPoint(Float64(src), "to$dst")

coll_two(r, i, c) = (Dagger.allgather(r * 10 + i; comm=c), Dagger.bcast(i == 1 ? r : nothing; root=0, comm=c))

# A collective that goes wrong must show up as a failed assertion in bounded time, not as a hung job: if the whole group of
# tests below has not finished after this long, report it and abort every rank.
const coll_watchdog = Timer(300) do _
    println(stderr, "Test Failed: collectives did not finish in 300 s on rank $rank (a collective hangs)")
    flush(stderr)
    MPI.Abort(MPI.COMM_WORLD, 1)
end

@testset "Cooperative collectives" begin
    @testset "barrier" begin
        # rank 0 arrives late, nobody leaves before it. The other ranks measure from their own start, so the delay
        # must be much longer than the skew between the ranks' starts (a loaded machine easily makes it 100 ms)
        rank == 0 && sleep(1.0)
        t0 = time()
        Dagger.barrier()
        @test rank == 0 || time() - t0 > 0.2
        Dagger.barrier(; comm=MPI.COMM_WORLD)
    end

    @testset "allgather" begin
        # sizes differ between ranks, one contribution is empty, the others are large
        got = Dagger.allgather(coll_payload(rank + 1))
        @test length(got) == nranks
        @test all(r -> got[r + 1] == coll_payload(r + 1), 0:nranks-1)
        # values that are not bits types, of different types on different ranks
        vals = Dagger.allgather(rank == 0 ? "text" : CollPoint(Float64(rank), "r$rank"))
        @test vals[1] == "text"
        @test all(r -> vals[r + 1] == CollPoint(Float64(r), "r$r"), 1:nranks-1)
        @test Dagger.allgather(nothing) == fill(nothing, nranks)
        @test Dagger.allgather((rank, [rank^2])) == [(r, [r^2]) for r in 0:nranks-1]
        @test Dagger.allgather(rank; comm=MPI.COMM_WORLD) == collect(0:nranks-1)
        big = Dagger.allgather(fill(Float64(rank), 1_000_000))
        @test all(r -> length(big[r + 1]) == 1_000_000 && big[r + 1][end] == Float64(r), 0:nranks-1)
    end

    # the two implementations and every fanout of the hand-written one give the same answer
    @testset "allgather, native = $native, fanout $k" for (native, k) in ((false, 1), (false, 2), (false, 4), (true, 4))
        MPIExt.with(MPIExt.MPI_ALLGATHER_NATIVE => native, MPIExt.MPI_ALLGATHER_FANOUT => k) do
            got = Dagger.allgather(coll_payload(rank + 1))
            @test all(r -> got[r + 1] == coll_payload(r + 1), 0:nranks-1)
            @test Dagger.allgather((rank, k)) == [(r, k) for r in 0:nranks-1]
            @test Dagger.allgather(Float64[]) == fill(Float64[], nranks)
        end
    end

    @testset "the native allgather does not touch the messages of the rounds" begin
        # a message with the rounds' tag waits at rank 1 while the native allgather runs; only the rounds would consume it
        tag = MPIExt.allgather_rounds_tag()
        rank == 0 && MPIExt.send_yield("stray", comm, 1, tag)
        MPIExt.with(MPIExt.MPI_ALLGATHER_NATIVE => true) do
            @test Dagger.allgather(rank) == collect(0:nranks-1)
        end
        rank == 1 && @test MPIExt.recv_yield(comm, 0, tag) == "stray"
        @test Dagger.allgather(rank) == collect(0:nranks-1)
    end

    @testset "bcast, root $root" for root in coll_roots()
        @test Dagger.bcast(rank == root ? "from $root" : nothing; root) == "from $root"
        @test Dagger.bcast(rank == root ? coll_payload(5) : Float64[]; root) == coll_payload(5)
        @test Dagger.bcast(rank == root ? Float64[] : [1.0]; root) == Float64[]              # empty payload
        @test Dagger.bcast(rank == root ? CollPoint(2.5, "p") : 0; root) == CollPoint(2.5, "p")
    end

    @testset "gather, root $root" for root in coll_roots()
        got = Dagger.gather(coll_payload(rank + 1); root)
        if rank == root
            @test length(got) == nranks
            @test all(r -> got[r + 1] == coll_payload(r + 1), 0:nranks-1)
        else
            @test got === nothing
        end
        got = Dagger.gather(rank == 0 ? "text" : CollPoint(Float64(rank), "r$rank"); root)
        rank == root && @test got[1] == "text" && all(r -> got[r + 1] == CollPoint(Float64(r), "r$r"), 1:nranks-1)
    end

    @testset "scatter, root $root" for root in coll_roots()
        xs = rank == root ? [coll_item(root, d) for d in 0:nranks-1] : nothing
        @test Dagger.scatter(xs; root) == coll_item(root, rank)
        # an empty element for every rank
        @test Dagger.scatter(rank == root ? [Float64[] for _ in 1:nranks] : nothing; root) == Float64[]
    end

    @testset "alltoall" begin
        got = Dagger.alltoall([coll_item(rank, d) for d in 0:nranks-1])
        @test length(got) == nranks
        @test all(s -> got[s + 1] == coll_item(s, rank), 0:nranks-1)
        @test Dagger.alltoall([Float64[] for _ in 1:nranks]) == fill(Float64[], nranks)
        big = Dagger.alltoall([fill(Float64(rank), 200_000 + d) for d in 0:nranks-1])
        @test all(s -> length(big[s + 1]) == 200_000 + rank && big[s + 1][1] == Float64(s), 0:nranks-1)
    end

    @testset "allreduce and reduce_to" begin
        x = coll_part(rank + 1)
        expected = foldl(+, coll_part.(1:nranks))
        total = Dagger.allreduce(+, x)
        @test total === expected                                        # folded in rank order
        @test allequal(Dagger.allgather(reinterpret(UInt64, [total])[1]))   # the same bits everywhere
        @test Dagger.allreduce(+, x; init=10.0) === foldl(+, coll_part.(1:nranks); init=10.0)
        @test Dagger.allreduce(max, rank) == nranks - 1
        # a non-commutative op sees the ranks in order
        @test Dagger.allreduce(*, string(rank)) == join(string.(0:nranks-1))
        @test Dagger.allreduce(vcat, [rank]) == collect(0:nranks-1)
        for root in coll_roots()
            r = Dagger.reduce_to(+, x; root)
            rank == root ? (@test r === expected) : (@test r === nothing)
            r = Dagger.reduce_to(*, string(rank); root, init="")
            rank == root && @test r == join(string.(0:nranks-1))
        end
    end

    @testset "a value that cannot be sent reaches every rank" begin
        # the failing rank must not leave the others waiting in the collective
        bad = nranks - 1
        @test_throws ErrorException Dagger.allgather(rank == bad ? CollBad() : 1)
        @test_throws ErrorException Dagger.bcast(rank == 0 ? CollBad() : 1)
        @test_throws ErrorException Dagger.alltoall([rank == bad && d == nranks - 1 ? CollBad() : 1 for d in 0:nranks-1])   # bad for one destination only: all must throw
        @test_throws ErrorException Dagger.alltoall([1])                   # one element per rank is required, on every rank
        @test_throws ErrorException Dagger.scatter(rank == 0 ? [CollBad() for _ in 1:nranks] : nothing)
        @test_throws ErrorException Dagger.scatter(rank == 0 ? collect(1:nranks+1) : nothing)   # wrong length on the root
        # a gather reports it to the root and to the contributor only; the others have nothing to learn
        res = try Dagger.gather(rank == bad ? CollBad() : 1) catch err; err end
        @test (rank == 0 || rank == bad) ? res isa ErrorException : res === nothing
        # a bad root argument is rejected everywhere, and the next collective works
        @test_throws ArgumentError Dagger.bcast(1; root=nranks)
        @test_throws ArgumentError Dagger.gather(1; root=-1)
        @test_throws ArgumentError Dagger.scatter([1]; root=nranks)
        @test Dagger.allgather(rank) == collect(0:nranks-1)
        Dagger.barrier()
    end

    # the task form: the results of tasks that live on different ranks, gathered without moving them first
    coll_task_value(r::Int) = r == 1 ? Float64[] : fill(Float64(r), 100 * (r + 1))     # sizes differ, rank 1's is empty
    coll_task_fail(r::Int) = error("task of rank $r failed")
    coll_task_maybe_fail(r::Int) = r == nranks - 1 ? coll_task_fail(r) : r
    coll_task_list(f) = [Dagger.@spawn(scope=coll_last_thread(r), return_type=Any, f(r)) for r in 0:nranks-1]

    @testset "allgather of tasks, native = $native" for native in (false, true)
        MPIExt.with(MPIExt.MPI_ALLGATHER_NATIVE => native) do
            ts = coll_task_list(coll_task_value)
            got = Dagger.allgather(ts, Any)
            @test got isa Vector{Any} && length(got) == nranks
            @test all(r -> got[r + 1] == coll_task_value(r), 0:nranks-1)
            @test Dagger.allgather_tasks(ts) == [coll_task_value(r) for r in 0:nranks-1]
            # the typed form converts the result
            ts = [Dagger.@spawn(scope=coll_last_thread(r), return_type=Int, r * 7) for r in 0:nranks-1]
            typed = Dagger.allgather(ts, Int)
            @test typed isa Vector{Int} && typed == [7r for r in 0:nranks-1]
            # several tasks per rank, in an order that interleaves the owners
            ts = [Dagger.@spawn(scope=coll_last_thread((i + 1) % nranks), return_type=Int, identity(i)) for i in 0:2nranks-1]
            @test Dagger.allgather(ts, Int) == collect(0:2nranks-1)
            # an empty list
            @test Dagger.allgather_tasks(Dagger.DTask[]) == []
        end
    end

    @testset "allgather of tasks: a failing task reaches every rank" begin
        ts = [Dagger.@spawn(scope=coll_last_thread(r), return_type=Any, coll_task_maybe_fail(r)) for r in 0:nranks-1]
        res = try Dagger.allgather_tasks(ts); nothing catch err; err end
        @test res !== nothing                  # every rank throws, none is left waiting
        @test occursin("task of rank $(nranks - 1) failed", sprint(showerror, res))
        # the next collective, and the task form, still work
        @test Dagger.allgather(rank) == collect(0:nranks-1)
        @test Dagger.allgather(coll_task_list(r -> r), Int) == collect(0:nranks-1)
    end

    @testset "allgather of tasks: the value form is unchanged" begin
        @test Dagger.allgather(rank) == collect(0:nranks-1)
        @test Dagger.allgather([rank]) == [[r] for r in 0:nranks-1]   # a vector is a value here, not a task list
    end

    @testset "called from a task" begin
        # one task per rank, each on its own rank: they run the collectives at the same time
        ts = [Dagger.@spawn(scope=coll_last_thread(r), return_type=Vector{Any}, Dagger.allgather(r)) for r in 0:nranks-1]
        for t in ts
            @test fetch(t) == collect(0:nranks-1)
        end
        ts = [Dagger.@spawn(scope=coll_last_thread(r), return_type=Int, Dagger.scatter(r == 0 ? collect(100:100+nranks-1) : nothing)) for r in 0:nranks-1]
        @test [fetch(t) for t in ts] == collect(100:100+nranks-1)
        Dagger.barrier()
    end

    @testset "several collectives in flight, one communicator each" begin
        # two tasks per rank communicate at the same time; each uses its own communicator, so their order does not matter
        comms = [MPI.Comm_dup(comm), MPI.Comm_dup(comm)]
        ts = [Dagger.@spawn(scope=coll_last_thread(r), return_type=Any, coll_two(r, i, comms[i])) for r in 0:nranks-1 for i in 1:2]
        res = [fetch(t) for t in ts]
        for r in 0:nranks-1, i in 1:2
            @test res[2 * r + i] == (Any[q * 10 + i for q in 0:nranks-1], i == 1 ? 0 : nothing)
        end
        Dagger.barrier()
    end
    @testset "a sub-communicator of another size" begin
        # `comm=` selects the communicator: a split gives groups of other sizes than the world
        sub = MPI.Comm_split(comm, rank % 2, rank)
        members = collect(rank % 2:2:nranks-1)
        srank = MPI.Comm_rank(sub)
        @test srank == length(filter(<(rank), members))
        @test Dagger.allgather(rank; comm=sub) == members
        MPIExt.with(MPIExt.MPI_ALLGATHER_NATIVE => true) do
            @test Dagger.allgather(rank; comm=sub) == members
        end
        for root in unique((0, length(members) - 1))
            @test Dagger.bcast(srank == root ? "from $(members[root + 1])" : nothing; root, comm=sub) == "from $(members[root + 1])"
            g = Dagger.gather(rank; root, comm=sub)
            srank == root ? (@test g == members) : (@test g === nothing)
            @test Dagger.scatter(srank == root ? [10m for m in members] : nothing; root, comm=sub) == 10rank
        end
        @test Dagger.alltoall([(rank, m) for m in members]; comm=sub) == [(m, rank) for m in members]
        @test Dagger.allreduce(+, rank; comm=sub) == sum(members)
        @test Dagger.reduce_to(vcat, [rank]; root=0, comm=sub) == (srank == 0 ? members : nothing)
        @test_throws ArgumentError Dagger.bcast(1; root=length(members), comm=sub)
        @test_throws ArgumentError Dagger.reduce_to(+, 1; root=length(members), comm=sub)
        Dagger.barrier(; comm=sub)
        MPI.free(sub)
        Dagger.barrier()
    end

    @testset "contribution size limit" begin
        # a contribution that does not fit one Cint count together with those of the other ranks is an error contribution
        @test MPIExt.coll_limit(comm) == typemax(Cint) ÷ nranks
        ok = MPIExt.coll_encode(fill(1.0, 100), 10_000)
        @test ok[1] == MPIExt.COLL_OK
        big = MPIExt.coll_encode(fill(1.0, 100), 100)
        @test big[1] == MPIExt.COLL_ERR
        @test_throws ErrorException MPIExt.coll_decode(big, 3)
        @test occursin("limit", try MPIExt.coll_decode(big, 3) catch err; sprint(showerror, err) end)
        @test occursin("rank 3", try MPIExt.coll_decode(big, 3) catch err; sprint(showerror, err) end)
    end

    @testset "tags do not collide with uniformity checks" begin
        @test MPIExt.allgather_rounds_tag() != MPIExt.compare_all_mpi_tag()
        @test MPIExt.allgather_rounds_tag() < UInt32(MPI.tag_ub())
        # a task runs rounds-allgathers on the world communicator while the driver keeps doing uniformity checks on it
        ts = [Dagger.@spawn(scope=coll_last_thread(r), return_type=Int,
                            sum(length(Dagger.allgather(fill(q, 50 + q))) for q in 1:40)) for r in 0:nranks-1]
        for i in 1:300
            @test MPIExt.compare_all(i, comm)
        end
        @test all(t -> fetch(t) == 40 * nranks, ts)
        Dagger.barrier()
    end
end
close(coll_watchdog)
