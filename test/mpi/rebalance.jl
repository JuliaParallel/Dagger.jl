# Dagger.rebalance under MPI: chunks change owner, the data arrives, tasks follow. (The plan itself is tested in test/rebalance.jl.)
@testset "rebalance: chunks change owner, data intact, tasks follow" begin
    nr = MPI.Comm_size(comm)
    if nr >= 2
        P = 2 * nr
        procs_by_rank = Dict(r => first(filter(p -> p.rank == r, mpi_procs())) for r in 0:nr-1)
        # every chunk starts on rank 0, with its index as data
        chunks = [Dagger.tochunk(fill(Float64(i), 1000), procs_by_rank[0], Dagger.memory_space(procs_by_rank[0])) for i in 1:P]
        @test all(c -> Dagger.owner_rank(c) == 0, chunks)
        costs = fill(1.0, P)
        new, moves = Dagger.rebalance(chunks, costs)
        owners = Int[Dagger.owner_rank(c) for c in new]
        load = [count(==(r), owners) for r in 0:nr-1]
        @test maximum(load) - minimum(load) <= 1
        @test length(moves) == count(!=(0), owners)
        # the data arrived and the tasks bound to the new owner run there
        sums = [fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=Dagger.owner_rank(c)), sum(c))) for c in new]
        @test sums == [1000.0 * i for i in 1:P]
        # a second call with the new owners and equal costs moves nothing
        new2, moves2 = Dagger.rebalance(new, costs)
        @test isempty(moves2)
        @test all(i -> new2[i] === new[i], 1:P)
    end
end
MPI.Barrier(comm)

@testset "rebalance: DTask elements are accepted, other elements refused" begin
    nr = MPI.Comm_size(comm)
    if nr >= 2
        P = 2 * nr
        # DTask elements (as in DArray.chunks)
        dts = Any[Dagger.@spawn(scope=Dagger.scope(mpi_rank=0), identity(fill(Float64(i), 100))) for i in 1:P]
        new, moves = Dagger.rebalance(dts, fill(1.0, P))
        @test all(c -> c isa Dagger.Chunk, new)
        @test maximum([count(c -> Dagger.owner_rank(c) == r, new) for r in 0:nr-1]) - minimum([count(c -> Dagger.owner_rank(c) == r, new) for r in 0:nr-1]) <= 1
        @test [fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=Dagger.owner_rank(c)), sum(c))) for c in new] == [100.0i for i in 1:P]
        # something that is not a chunk is refused
        @test_throws ArgumentError Dagger.rebalance([1.0, 2.0], [1.0, 1.0])
    end
end
MPI.Barrier(comm)

# Every rank must derive the same plan (the moves are tasks that every rank plans), the costs decide what moves, the arguments
# `tolerance` and `max_moves` reach the plan, and non-uniform costs or a graph of a subset of the ranks are refused.
@testset "rebalance: same plan on every rank, costs honoured, tolerance and max_moves, refusals" begin
    nr = MPI.Comm_size(comm)
    if nr >= 2
        P = 2 * nr
        r0 = first(filter(p -> p.rank == 0, mpi_procs()))
        fresh() = [Dagger.tochunk(fill(Float64(i), 10), r0, Dagger.memory_space(r0)) for i in 1:P]
        costs = [3.0; ones(P - 1)]
        chunks = fresh()
        new, moves = Dagger.rebalance(chunks, costs)
        @test moves == Dagger.rebalance_plan(zeros(Int, P), costs, nr)         # the plan of the pure function, from these costs
        @test moves != Dagger.rebalance_plan(zeros(Int, P), ones(P), nr)       # and not the one of equal costs
        owners = Int[Dagger.owner_rank(c) for c in new]
        @test all(i -> owners[i] == get(Dict(moves), i, 0), 1:P)
        # the same on every rank
        all_owners = MPI.Allgather(owners, comm)
        @test all(r -> all_owners[(r * P + 1):((r + 1) * P)] == owners, 0:(nr - 1))
        # within the tolerance nothing moves, and the entries are the very same objects
        same, nomoves = Dagger.rebalance(chunks, costs; tolerance=100.0)
        @test isempty(nomoves) && all(i -> same[i] === chunks[i], 1:P)
        # `max_moves` bounds the number of chunks that move
        _, one = Dagger.rebalance(chunks, ones(P); max_moves=1)
        @test length(one) == 1
        none, zero_moves = Dagger.rebalance(chunks, ones(P); max_moves=0)
        @test isempty(zero_moves) && all(i -> none[i] === chunks[i], 1:P)
        # costs that differ between ranks are refused on every rank, and the next call still works
        # (this check does not depend on `check_uniformity!`)
        for uniformity in (true, false)
            Dagger.check_uniformity!(uniformity)
            @test_throws ArgumentError Dagger.rebalance(chunks, fill(1.0 + MPI.Comm_rank(comm), P))
            MPI.Barrier(comm)
        end
        Dagger.check_uniformity!(true)
        again, moves3 = Dagger.rebalance(chunks, ones(P))
        @test length(moves3) == length(Dagger.rebalance_plan(zeros(Int, P), ones(P), nr))
        # a graph of a subset of the ranks (a duplicated communicator stands in for one) is refused
        dup = MPI.Comm_dup(comm)
        @test_throws ArgumentError Dagger.rebalance(MPIExt.MPIAcceleration(dup), chunks, ones(P))
        MPI.free(dup)
    end
end
MPI.Barrier(comm)
