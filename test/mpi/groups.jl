# Group graphs (`Dagger.with_group`, `Dagger.per_group`): a task graph on a communicator of k ranks that only
# its members plan and run, with its own task ids. The world's ids, tags and planning must stay in agreement
# however many tasks each group spawns.
group_inc(x::Int) = x + 1
group_add(a::Int, b::Int) = a + b
group_fill(n::Int, v::Float64) = fill(v, n)
group_sum(x::Vector{Float64}) = sum(x)
group_sum2(a::Vector{Float64}, b::Vector{Float64}) = sum(a) + sum(b)
group_pass(x::Vector{Float64}) = x
group_plus!(X, c) = (X .+= c; nothing)
group_bad(x::Int) = x > 0 ? error("group failure") : x
include("deadline.jl")
group_slow_seven() = (sleep(0.05); 7)

within(300, "group graphs") do
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
        # ... also in the part of the id that names the group: a process that belongs to both groups must
        # not give them the same index (the counters alone would keep the ids apart only by luck)
        group_index(uid) = (Int(uid) - MPIExt.MPI_GROUP_ID_BASE) >> 24
        @test group_index(gt.uid) != group_index(gt2.uid)
        # an id of a group never becomes a tag larger than the implementation allows
        @test 0 <= MPIExt.mpi_tag_of(Int(gt.uid)) <= MPI.tag_ub()
        @test MPIExt.mpi_tag_of(Int(gt.uid)) < (1 << 22)
        @test MPIExt.to_tag(MPIExt.MPIRefID(0, true, UInt64(gt.uid))) < (1 << 22)
    end

    @testset "the exchange that creates a group is an allgather in rank order" begin
        ex = MPIExt.mpi_group_exchange(stage1, UInt128[UInt128(rank), UInt128(7)])
        members = [r for r in 0:nranks-1 if r ÷ 2 == rank ÷ 2]
        @test length(ex) == g1size
        @test first.(ex) == UInt128.(members)
        @test all(e -> e[2] == 7, ex)
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

    @testset "a group over a subset of the ranks completes while the others are idle; freeing it" begin
        # with two ranks the group is rank 0 alone, otherwise ranks 0 and 1; the other ranks spawn nothing
        # and only wait for the members
        if nranks >= 2
            members = nranks >= 3 ? Cint[0, 1] : Cint[0]
            sub = MPI.Group_incl(MPI.Comm_group(comm), members)
            ismember = rank in members
            subcomm = ismember ? MPI.Comm_create_group(comm, sub, 0) : nothing
            gsub(c, r) = Dagger.ExactScope(last(sort(filter(p -> p.rank == r && p.innerProc isa Dagger.ThreadProc,
                                                            collect(Dagger.get_processors(MPIExt.MPIClusterProc(c)))); by=p -> p.innerProc.tid)))
            pieces = nothing
            if ismember
                last_member = length(members) - 1
                within(60, "a group over a subset of the ranks, others idle") do
                    vals = Dagger.with_group(subcomm) do
                        a = Dagger.@spawn scope=gsub(subcomm, 0) group_inc(1)
                        b = Dagger.@spawn scope=gsub(subcomm, last_member) group_add(a, 10)
                        [fetch(a), fetch(b)]
                    end
                    @test vals == [2, 12]
                    @test MPIExt.mpi_is_group(subcomm)
                    pieces = Dagger.per_group(subcomm, Vector{Float64}) do
                        Dagger.@spawn group_fill(3, Float64(rank + 1))
                    end
                    @test fetch(pieces[1]) == fill(1.0, 3)
                end
            end
            # the idle ranks wait here, cooperatively, until the members are done
            req = MPI.Ibarrier(comm)
            within(60, "barrier after the group's work") do
                while !MPI.Test(req)
                    sleep(0.01)
                end
            end
            if ismember
                st = lock(MPIExt.BCAST_STATES) do states
                    get(states, subcomm, nothing)
                end
                @test st !== nothing
                # the state the group registered, to be gone after `free_group!`
                @test haskey(MPIExt.MPIClusterProcChildren, subcomm)
                @test lock(MPIExt.MPI_NODE_UUIDS) do maps; haskey(maps, subcomm) end
                @test lock(MPIExt.MPI_INITIALIZED_COMMS) do comms; subcomm in comms end
                @test any(p -> p isa MPIExt.MPIOSProc && p.comm == subcomm, Dagger.Sch.eager_context().procs)
                MPIExt.uniform_mpi_processors(MPIExt.mpi_registered_group_acceleration(subcomm))
                @test any(k -> k[1] == subcomm, keys(MPIExt.MPIUniformProcsCache))
                within(60, "free_group!") do
                    Dagger.free_group!(subcomm)
                end
                # a freed group leaves no relay polling behind and no registered state
                @test st.running[] == false
                @test istaskdone(st.relay)
                @test !MPIExt.mpi_is_group(subcomm)
                @test isempty(filter(p -> p isa MPIExt.MPIOSProc && p.comm == subcomm, Dagger.Sch.eager_context().procs))
                @test lock(MPIExt.BCAST_STATES) do states
                    !haskey(states, subcomm)
                end
                # nothing keyed by the communicator outlives it (a new communicator may get the same handle)
                @test !haskey(MPIExt.MPIClusterProcChildren, subcomm)
                @test !any(k -> k[1] == subcomm, keys(MPIExt.MPIUniformProcsCache))
                @test lock(MPIExt.MPI_NODE_UUIDS) do maps; !haskey(maps, subcomm) end
                @test lock(MPIExt.MPI_INITIALIZED_COMMS) do comms; !(subcomm in comms) end
                # the communicator the relay duplicated is freed too (a leaked one counts against the MPI
                # implementation's limit of live communicators)
                @test st.bcast_comm == MPI.COMM_NULL
                @test_throws ErrorException MPIExt.mpi_registered_group_acceleration(subcomm)
                # a chunk of a freed group cannot be fetched, and the error says why
                msg = try
                    fetch(pieces[1])
                    "no error"
                catch e
                    sprint(showerror, e)
                end
                @test occursin("free_group!", msg)
                # new work in the freed group's communicator re-registers it (collectively), it does not
                # silently reuse the old state
                # the application frees its own communicator; a new group over the same ranks may get the same
                # handle and must not see anything of the old one
                MPI.free(subcomm)
                sub2 = MPI.Comm_create_group(comm, sub, 1)
                within(60, "a new group over the same ranks") do
                    v2 = Dagger.with_group(sub2) do
                        a = Dagger.@spawn scope=gsub(sub2, 0) group_inc(1)
                        fetch(Dagger.@spawn scope=gsub(sub2, length(members) - 1) group_add(a, 10))
                    end
                    @test v2 == 12
                end
                # a group does not run out of ids silently: spawning past the limit is an error
                g = lock(MPIExt.MPI_GROUPS) do groups
                    groups[sub2]
                end
                g.counter[] = MPIExt.MPI_GROUP_COUNTER_LIMIT
                @test_throws ErrorException Dagger.eager_next_id(g.accel)
                msg = try
                    Dagger.with_group(sub2) do
                        Dagger.@spawn scope=gsub(sub2, 0) group_inc(1)
                    end
                    "no error"
                catch e
                    sprint(showerror, e)
                end
                @test occursin("used up its task ids", msg)
                within(60, "free_group! of the second group") do
                    Dagger.free_group!(sub2)
                end
                MPI.free(sub2)
            end
            # the world's task ids still agree on every rank: the fetch of the group's chunk took its id from
            # the group's counter, not from the world's (the other ranks took none)
            @test allequal(MPI.Allgather(Int(Dagger.eager_next_id()), comm))
            MPI.Barrier(comm)
        end
    end

    @testset "first use of a group while a peer waits for a result of this rank" begin
        # A rank that enters a group for the first time waits for the others cooperatively. Rank 1 first
        # waits for a task of rank 0 and only then joins the group, so rank 0's first use of the group has
        # to let that task run (and its result be sent) while it waits for rank 1. A blocking
        # communicator duplication would park rank 0's thread, and with one thread per rank nothing
        # could run the task: the job would never finish.
        if nranks >= 2
            gall = MPI.Comm_split(comm, 0, rank)
            within(60, "first use of a group while a peer waits for this rank") do
                t = Dagger.@spawn scope=pinned(0) group_slow_seven()
                rank == 1 && wait(t)
                v = Dagger.with_group(gall) do
                    fetch(Dagger.@spawn(group_inc(1)))
                end
                @test v == 2
                @test fetch(t) == 7
            end
            # a fetch of a chunk of the world inside a group block takes its id from the world's counter
            wc = Dagger.tochunk(fill(1.0, 2), proc_for_rank(0), space_for_rank(0))
            gst = lock(MPIExt.MPI_GROUPS) do groups
                groups[gall]
            end
            within(60, "fetch of a world chunk inside a group block") do
                Dagger.with_group(gall) do
                    c0 = gst.counter[]
                    @test fetch(wc) == fill(1.0, 2)
                    @test gst.counter[] == c0
                end
            end
            Dagger.free_group!(gall)
            MPI.free(gall)
            MPI.Barrier(comm)
        end
    end

    @testset "Datadeps inside a group: a copy between members uses a valid tag" begin
        if g1size >= 2
            B = fill(1.0, 4)
            Dagger.with_group(stage1) do
                Dagger.spawn_datadeps() do
                    # the array is owned by member 0 and the task runs on the last member: the argument
                    # travels between the members with a tag derived from a group task id (above 2^40)
                    Dagger.@spawn scope=gscope(stage1, g1size - 1) group_plus!(InOut(B), 2.0)
                    Dagger.@spawn scope=gscope(stage1, 0) group_plus!(InOut(B), 3.0)
                end
            end
            @test B == (g1rank == 0 ? fill(6.0, 4) : fill(1.0, 4))
        end
    end

    @testset "Datadeps inside a group: the first member owns a plain array" begin
        A = fill(1.0, 4)
        Dagger.with_group(stage1) do
            Dagger.spawn_datadeps() do
                Dagger.@spawn group_plus!(InOut(A), 1.0)
            end
        end
        @test A == (g1rank == 0 ? fill(2.0, 4) : fill(1.0, 4))
        t = Dagger.@spawn scope=pinned(nranks-1) group_inc(5)
        @test fetch(t) == 6
        @test allequal(MPI.Allgather(Int(t.uid), comm))
    end

    Dagger.free_group!(stage1)
    Dagger.free_group!(stage2)
    MPI.Barrier(comm)
end
end
