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

# Groups of chunks that must stay together: one cost, one owner, moved as a unit, every member on the same new rank.
@testset "rebalance: groups move together" begin
    nr = MPI.Comm_size(comm)
    if nr >= 2
        P = 2 * nr
        r0 = first(filter(p -> p.rank == 0, mpi_procs()))
        mk(i, j) = Dagger.tochunk(fill(Float64(10 * i + j), 50), r0, Dagger.memory_space(r0))
        # a group of 5 chunks per partition, all on rank 0
        groups = [ntuple(j -> mk(i, j), 5) for i in 1:P]
        new, moves = Dagger.rebalance(groups, fill(1.0, P))
        @test length(new) == P && all(g -> g isa AbstractVector && length(g) == 5, new)
        owners = [Dagger.owner_rank(first(g)) for g in new]
        @test all(i -> all(c -> Dagger.owner_rank(c) == owners[i], new[i]), 1:P)      # members of a group share the rank
        @test !isempty(moves) && all(p -> owners[first(p)] == last(p), moves)
        load = [count(==(r), owners) for r in 0:nr-1]
        @test maximum(load) - minimum(load) <= 1
        @test owners == [get(Dict(moves), i, 0) for i in 1:P]
        # the moves are those of the plan of whole groups
        @test moves == Dagger.rebalance_plan(zeros(Int, P), ones(P), nr)
        # every member arrived with its data, in order, and tasks bound to the new owner see it
        for i in 1:P
            sums = [fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=Dagger.owner_rank(c)), sum(c))) for c in new[i]]
            @test sums == [50.0 * (10 * i + j) for j in 1:5]
        end
        # the same on every rank
        all_owners = MPI.Allgather(owners, comm)
        @test all(r -> all_owners[(r * P + 1):((r + 1) * P)] == owners, 0:(nr - 1))
        # a second call with the new placement and equal costs moves nothing
        again, nomoves = Dagger.rebalance(new, fill(1.0, P))
        @test isempty(nomoves)
        # groups given as vectors, DTasks as members, single chunks next to groups: a single stays a Chunk
        mixed = Any[Dagger.@spawn(scope=Dagger.scope(mpi_rank=0), identity(fill(1.0, 5))) for _ in 1:P]
        mixed = Any[i % 2 == 0 ? [mixed[i], mk(i, 1)] : mk(i, 2) for i in 1:P]
        out, mmoves = Dagger.rebalance(mixed, fill(1.0, P))
        @test all(i -> i % 2 == 0 ? (out[i] isa AbstractVector && length(out[i]) == 2 &&
                                     Dagger.owner_rank(out[i][1]) == Dagger.owner_rank(out[i][2])) : out[i] isa Dagger.Chunk, 1:P)
        # members with different owners, and an empty group, are refused
        r1 = first(filter(p -> p.rank == 1, mpi_procs()))
        split = Any[(mk(1, 1), Dagger.tochunk(fill(1.0, 5), r1, Dagger.memory_space(r1)))]
        @test_throws ArgumentError Dagger.rebalance(split, [1.0])
        @test_throws ArgumentError Dagger.rebalance(Any[()], [1.0])
    end
end
MPI.Barrier(comm)

# Ranks of different speed: costs are work, the slow rank ends with about its share of the work.
@testset "rebalance: speeds" begin
    nr = MPI.Comm_size(comm)
    if nr >= 2
        P = 6 * nr
        r0 = first(filter(p -> p.rank == 0, mpi_procs()))
        chunks = [Dagger.tochunk(fill(Float64(i), 10), r0, Dagger.memory_space(r0)) for i in 1:P]
        spd = [0.5; ones(nr - 1)]
        new, moves = Dagger.rebalance(chunks, ones(P); speeds=spd)
        owners = Int[Dagger.owner_rank(c) for c in new]
        work = [count(==(r), owners) for r in 0:nr-1]
        @test moves == Dagger.rebalance_plan(zeros(Int, P), ones(P), nr; speeds=spd)
        @test all(r -> work[1] < work[r], 2:nr)                                  # the slow rank has the least
        @test work[1] <= 0.5 * minimum(work[2:end]) + 1.5                         # about half of the others'
        @test maximum(Dagger.rebalance_loads(zeros(Int, P), ones(P), nr; speeds=spd, plan=moves)) <= P / sum(spd) + 1 / minimum(spd) + 1e-9   # within one unit chunk of the ideal time
        # without speeds the same input is split evenly
        even, _ = Dagger.rebalance(chunks, ones(P))
        ev = [count(c -> Dagger.owner_rank(c) == r, even) for r in 0:nr-1]
        @test maximum(ev) - minimum(ev) <= 1
        # data intact
        @test [fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=Dagger.owner_rank(c)), sum(c))) for c in new] == [10.0i for i in 1:P]
        # arguments are checked on every rank
        @test_throws ArgumentError Dagger.rebalance(chunks, ones(P); speeds=ones(nr + 1))
        @test_throws ArgumentError Dagger.rebalance(chunks, ones(P); speeds=[0.0; ones(nr - 1)])
        @test_throws ArgumentError Dagger.rebalance(chunks, ones(P); speeds=[-1.0; ones(nr - 1)])
        # speeds, move costs or horizons that differ between ranks are refused on every rank
        @test_throws ArgumentError Dagger.rebalance(chunks, ones(P); speeds=fill(1.0 + MPI.Comm_rank(comm), nr))
        MPI.Barrier(comm)
        @test_throws ArgumentError Dagger.rebalance(chunks, ones(P); move_cost=1.0 + MPI.Comm_rank(comm))
        MPI.Barrier(comm)
        @test_throws ArgumentError Dagger.rebalance(chunks, ones(P); horizon=1.0 + MPI.Comm_rank(comm))
        MPI.Barrier(comm)
        again, moves2 = Dagger.rebalance(chunks, ones(P); speeds=spd)      # still works afterwards
        @test moves2 == moves
    end
end
MPI.Barrier(comm)

# A move is only made if it saves more than it costs over the horizon.
@testset "rebalance: move_cost and horizon" begin
    nr = MPI.Comm_size(comm)
    if nr >= 2
        P = 2 * nr
        r0 = first(filter(p -> p.rank == 0, mpi_procs()))
        chunks = [Dagger.tochunk(fill(Float64(i), 10), r0, Dagger.memory_space(r0)) for i in 1:P]
        # P unit chunks on rank 0: the first move saves 1 (P -> P - 1) at least
        none, nomoves = Dagger.rebalance(chunks, ones(P); move_cost=100.0)
        @test isempty(nomoves) && all(i -> none[i] === chunks[i], 1:P)
        some, somemoves = Dagger.rebalance(chunks, ones(P); move_cost=100.0, horizon=1000)
        @test somemoves == Dagger.rebalance_plan(zeros(Int, P), ones(P), nr)
        @test all(i -> Dagger.owner_rank(some[i]) == get(Dict(somemoves), i, 0), 1:P)
        cheap, cheapmoves = Dagger.rebalance(chunks, ones(P); move_cost=0.5)
        @test cheapmoves == somemoves
        vec, vecmoves = Dagger.rebalance(chunks, ones(P); move_cost=fill(100.0, P))
        @test isempty(vecmoves)
    end
end
MPI.Barrier(comm)

# Every rank derives the same plan, however often it is asked, and the loads it predicts match the placement it produced.
@testset "rebalance: determinism on every rank, predicted loads" begin
    nr = MPI.Comm_size(comm)
    P = 3 * nr
    spd = [1.0 + 0.5 * r for r in 0:nr-1]
    cost = [1.0 + (i % 4) for i in 1:P]
    first_plan = Dagger.rebalance_plan(zeros(Int, P), cost, nr; speeds=spd)
    @test all(_ -> Dagger.rebalance_plan(zeros(Int, P), cost, nr; speeds=spd) == first_plan, 1:100)
    h = hash(first_plan)
    @test all(==(h), MPI.Allgather([h], comm))
    if nr >= 2
        r0 = first(filter(p -> p.rank == 0, mpi_procs()))
        chunks = [Dagger.tochunk(fill(Float64(i), 10), r0, Dagger.memory_space(r0)) for i in 1:P]
        new, moves = Dagger.rebalance(chunks, cost; speeds=spd)
        @test moves == first_plan
        owners = Int[Dagger.owner_rank(c) for c in new]
        @test Dagger.rebalance_loads(zeros(Int, P), cost, nr; speeds=spd, plan=moves) ≈ Dagger.rebalance_loads(owners, cost, nr; speeds=spd)
        mh = hash(moves)
        @test all(==(mh), MPI.Allgather([mh], comm))
    end
end
MPI.Barrier(comm)
