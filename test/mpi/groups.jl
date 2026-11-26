# Group graphs (`Dagger.with_group`, `Dagger.per_group`): a task graph on a communicator of k ranks that only
# its members plan and run, with its own task ids. The world's ids, tags and planning must stay in agreement
# however many tasks each group spawns.
group_inc(x::Int) = x + 1
group_add(a::Int, b::Int) = a + b
group_fill(n::Int, v::Float64) = fill(v, n)
group_sum(x::Vector{Float64}) = sum(x)
group_sum2(a::Vector{Float64}, b::Vector{Float64}) = sum(a) + sum(b)
group_pass(x::Vector{Float64}) = x
group_bad(x::Int) = x > 0 ? error("group failure") : x

@testset "Group graphs" begin
    # Two stages of overlapping groups: pairs {0,1} {2,3} ..., then by parity {0,2,..} {1,3,..}
    stage1 = MPI.Comm_split(comm, rank ÷ 2, rank)
    stage2 = MPI.Comm_split(comm, rank % 2, rank)
    g1rank, g1size = MPI.Comm_rank(stage1), MPI.Comm_size(stage1)
    g2rank, g2size = MPI.Comm_rank(stage2), MPI.Comm_size(stage2)
    # a processor of the member with rank `r` in the group `c` (the last thread of it)
    function gproc(c, r)
        cpus = filter(p -> p.rank == r && p.innerProc isa Dagger.ThreadProc,
                      collect(Dagger.get_processors(MPIExt.MPIClusterProc(c))))
        return last(sort(cpus; by=p -> p.innerProc.tid))
    end
    gscope(c, r) = Dagger.ExactScope(gproc(c, r))
    pinned(r) = Dagger.ExactScope(last(filter(p -> p.rank == r && p.innerProc isa Dagger.ThreadProc, mpi_procs())))

    @testset "a different number of tasks in every group; the world still agrees" begin
        n = 3 + 4 * (rank ÷ 2)
        vals = Dagger.with_group(stage1) do
            fetch.([Dagger.@spawn(scope=gscope(stage1, mod(i, g1size)), group_inc(i)) for i in 1:n])
        end
        @test vals == [i + 1 for i in 1:n]
        t = Dagger.@spawn scope=pinned(nranks-1) group_inc(1)
        @test fetch(t) == 2
        @test allequal(MPI.Allgather(Int(t.uid), comm))
        @test Int(t.uid) < MPI.tag_ub()
        # the task ids of the group are above the tag range and distinct from the other group's
        gt = Dagger.with_group(stage1) do
            Dagger.@spawn scope=gscope(stage1, 0) group_inc(1)
        end
        @test Int(gt.uid) >= MPIExt.MPI_GROUP_ID_BASE
        @test fetch(gt) == 2
        # members of one group agree on the ids; two groups never use the same id in one process
        @test allequal(MPI.Allgather(Int(gt.uid), stage1))
        gt2 = Dagger.with_group(stage2) do
            Dagger.@spawn scope=gscope(stage2, 0) group_inc(1)
        end
        @test fetch(gt2) == 2
        @test Int(gt2.uid) != Int(gt.uid)
    end

    @testset "data moves between the members of a group" begin
        if g1size >= 2
            r = Dagger.with_group(stage1) do
                a = Dagger.@spawn scope=gscope(stage1, 0) group_inc(1)
                b = Dagger.@spawn scope=gscope(stage1, g1size - 1) group_add(a, 10)
                fetch(b)
            end
            @test r == 12
        end
    end

    @testset "per_group: hand-over between two stages of groups" begin
        # stage 1: every member builds a piece, the group sums them on its first member
        pieces = Dagger.per_group(stage1, Vector{Float64}) do
            Dagger.@spawn group_fill(10, Float64(rank + 1))
        end
        @test length(pieces) == g1size
        @test [c.handle.rank for c in pieces] == collect(0:g1size-1)
        total1 = Dagger.with_group(stage1) do
            res = Dagger.@spawn scope=gscope(stage1, 0) group_sum2(pieces[1], pieces[end])
            fetch(res)
        end
        members = (2 * (rank ÷ 2)):min(2 * (rank ÷ 2) + 1, nranks - 1)
        @test total1 == 10.0 * (first(members) + 1) + 10.0 * (last(members) + 1)

        # stage 2: the first member of every stage-1 group (the even ranks) hands its group's total to the
        # parity group, in place; the other members hand over zeros
        function s1total(g)
            ms = (2g):min(2g + 1, nranks - 1)
            return 10.0 * (first(ms) + 1) + 10.0 * (last(ms) + 1)
        end
        chunks2 = Dagger.per_group(stage2, Vector{Float64}) do
            if g1rank == 0
                Dagger.@spawn group_fill(1, total1)         # a local task of the rank-local graph
            else
                Dagger.@spawn group_fill(1, 0.0)
            end
        end
        total2 = Dagger.with_group(stage2) do
            fetch(Dagger.@spawn(scope=gscope(stage2, 0), group_sum2(chunks2[1], chunks2[end])))
        end
        parity_members = [r for r in 0:nranks-1 if r % 2 == rank % 2]
        expect = rank % 2 == 0 ? s1total(first(parity_members) ÷ 2) + (length(parity_members) > 1 ? s1total(last(parity_members) ÷ 2) : s1total(first(parity_members) ÷ 2)) : 0.0
        @test total2 == expect
    end

    @testset "a failing task of a group fails only in that group" begin
        f = Dagger.with_group(stage1) do
            Dagger.@spawn scope=gscope(stage1, 0) group_bad(1)
        end
        if rank ÷ 2 == 0
            @test_throws Exception fetch(f)
        end
        t = Dagger.@spawn scope=pinned(nranks-1) group_inc(5)
        @test fetch(t) == 6
        @test allequal(MPI.Allgather(Int(t.uid), comm))
    end

    Dagger.free_group!(stage1)
    Dagger.free_group!(stage2)
    MPI.Barrier(comm)
end
