# Rank-local task graphs (`Dagger.with_rank_local`, `Dagger.per_rank`): a graph on a communicator of
# size one. Only the owning rank plans and runs these tasks; the world's ids, tags and planning
# must stay in agreement however many local tasks each rank spawns.
local_double(x::Int) = 2x
local_add(a::Int, b::Int) = a + b
local_fill(n::Int, v::Float64) = fill(v, n)
local_sum(x::Vector{Float64}) = sum(x)
local_sum2(a::Vector{Float64}, b::Vector{Float64}) = sum(a) + sum(b)
local_inc(x::Int) = x + 1

@testset "Rank-local task graphs" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    pinned(r) = Dagger.ExactScope(last(filter(p -> p.rank == r, cpus)))

    @testset "a different number of local tasks on every rank" begin
        n = 3 + 4rank
        vals = Dagger.with_rank_local() do
            tasks = [Dagger.@spawn(local_double(i)) for i in 1:n]
            fetch.(tasks)
        end
        @test vals == [2i for i in 1:n]
        # chain and fan-in inside the local graph
        total = Dagger.with_rank_local() do
            leaves = [Dagger.@spawn(local_inc(i)) for i in 1:n]
            level = leaves
            while length(level) > 1
                nxt = Any[]
                for j in 1:2:length(level)-1
                    push!(nxt, Dagger.@spawn(local_add(level[j], level[j+1])))
                end
                isodd(length(level)) && push!(nxt, level[end])
                level = nxt
            end
            fetch(level[1])
        end
        @test total == sum(i + 1 for i in 1:n)
        # tasks of the world afterwards: ids, tags and planning agree on every rank
        t = Dagger.@spawn scope=pinned(nranks-1) local_inc(1)
        @test fetch(t) == 2
        @test allequal(MPI.Allgather(Int(t.uid), comm))
        # a world id is still below the tag range, a local one above it
        @test Int(t.uid) < MPI.tag_ub()
        lt = Dagger.with_rank_local() do
            Dagger.@spawn(local_inc(1))
        end
        @test Int(lt.uid) >= (1 << 23)
        @test fetch(lt) == 2
    end

    @testset "local tasks run on this rank only" begin
        where = Dagger.with_rank_local() do
            fetch(Dagger.@spawn(MPI.Comm_rank(MPI.COMM_WORLD)))
        end
        @test where == rank
    end

    @testset "per_rank: local graph to shared tasks" begin
        chunks = Dagger.per_rank(Vector{Float64}) do
            Dagger.@spawn local_fill(10, Float64(rank + 1))
        end
        @test length(chunks) == nranks
        @test all(c -> c.handle isa MPIExt.MPIRef && c.handle.comm == MPI.COMM_WORLD, chunks)
        @test [c.handle.rank for c in chunks] == collect(0:nranks-1)
        @test allequal(MPI.Allgather(hash(map(c -> c.handle.id, chunks)), comm))
        # a shared task on each rank reads its rank's chunk; then the shared work continues in agreement
        sums = [fetch(Dagger.@spawn(scope=pinned(r), local_sum(chunks[r+1]))) for r in 0:nranks-1]
        @test sums == [10.0 * (r + 1) for r in 0:nranks-1]
        t = Dagger.@spawn scope=pinned(0) local_inc(1)
        @test allequal(MPI.Allgather(Int(t.uid), comm))
        # a shared task that combines chunks of different ranks (data moves between ranks)
        both = fetch(Dagger.@spawn(scope=pinned(0), local_sum2(chunks[nranks], chunks[1])))
        @test both == 10.0 * nranks + 10.0
    end

    @testset "shared to local: a local task reads a world chunk this rank owns" begin
        mine = Dagger.tochunk(fill(3.0, 5), proc_for_rank(rank), space_for_rank(rank))
        s = Dagger.with_rank_local() do
            fetch(Dagger.@spawn(local_sum(mine)))
        end
        @test s == 15.0
    end

    @testset "a failing local task fails only locally" begin
        bad(x::Int) = x > 0 ? error("local failure") : x
        f = Dagger.with_rank_local() do
            Dagger.@spawn(bad(1))
        end
        if rank == 0
            @test_throws Exception fetch(f)
        end
        # the world is unaffected and still agrees
        t = Dagger.@spawn scope=pinned(nranks-1) local_inc(5)
        @test fetch(t) == 6
        @test allequal(MPI.Allgather(Int(t.uid), comm))
    end
    MPI.Barrier(comm)
end

# Datadeps regions inside a rank-local graph: the planner of a graph on a one-rank communicator
# must not depend on what the other ranks do
local_scale!(X, c) = (X .*= c; nothing)
local_plus!(X, c) = (X .+= c; nothing)
@testset "Datadeps inside a rank-local graph" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    pinned(r) = Dagger.ExactScope(last(filter(p -> p.rank == r, cpus)))
    @testset "a different number of regions and tasks on every rank" begin
        n = 2 + 3rank
        A = Dagger.with_rank_local() do
            A = fill(1.0, 6)
            for _ in 1:(1 + rank)
                Dagger.spawn_datadeps() do
                    for _ in 1:n
                        Dagger.@spawn local_plus!(InOut(A), 1.0)
                    end
                end
            end
            A
        end
        @test A == fill(1.0 + n * (1 + rank), 6)
    end
    @testset "views of one array order correctly" begin
        B = Dagger.with_rank_local() do
            B = fill(1.0, 8)
            Dagger.spawn_datadeps() do
                Dagger.@spawn local_plus!(InOut(view(B, 1:4)), 1.0)
                Dagger.@spawn local_scale!(InOut(view(B, 3:6)), 2.0)
                Dagger.@spawn local_plus!(InOut(view(B, 5:8)), 3.0)
            end
            B
        end
        @test B == [2.0, 2.0, 4.0, 4.0, 5.0, 5.0, 4.0, 4.0]
    end
    @testset "the world still agrees afterwards" begin
        t = Dagger.@spawn scope=pinned(nranks-1) local_inc(7)
        @test fetch(t) == 8
        @test allequal(MPI.Allgather(Int(t.uid), comm))
        # and Datadeps of the world still works
        W = fill(1.0, 4)
        Dagger.spawn_datadeps() do
            Dagger.@spawn scope=pinned(nranks-1) local_plus!(InOut(W), 1.0)
        end
        rank == 0 && @test W == fill(2.0, 4)
    end
    MPI.Barrier(comm)
end
