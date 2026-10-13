# Shared by the Distributed and MPI suites. Capture tasks without launching
# them so ordering checks do not depend on execution speed or thread count.
function capture_launch_graph(f)
    queue = Dagger.DataDepsTaskQueue(Dagger.DefaultTaskQueue();
                                    scheduler=Dagger.RoundRobinScheduler())
    Dagger.with_options(f; task_queue=queue)
    metas, args = Dagger.collect_aliased_args(queue.seen_tasks)
    _, overlaps, ainfos = Dagger.build_aliasing_parallel(args)
    dag = Dagger.build_dependency_dag(metas, ainfos, overlaps)
    return metas, dag
end

@testset "Datadeps launch ordering" begin
    @testset "Independent leaves precede value consumers" begin
        metas, dag = capture_launch_graph() do
            p = [Dagger.@spawn identity(i) for i in 1:16]
            Dagger.treereduce((x,y) -> Dagger.@spawn(+(x,y)), p)
        end
        order = @inferred Dagger.datadeps_launch_order(dag, true)
        @test order[1:16] == collect(1:16)
        positions = invperm(order)
        @test all(positions[v] < positions[s] for v in Dagger.vertices(dag)
                  for s in Dagger.outneighbors(dag, v))
        @test Dagger.datadeps_launch_order(dag, false) ==
              Dagger.topological_sort_by_dfs(dag)
    end

    @testset "Recursive gather exposes every independent bottom concatenation" begin
        chunks = [Dagger.datadeps_arg_wrap([i]) for i in 1:16]
        metas, dag = capture_launch_graph() do
            Dagger.treereduce((x,y) -> Dagger.@spawn(vcat(Dagger.In(x), Dagger.In(y))), chunks)
        end
        roots = [v for v in Dagger.vertices(dag) if isempty(Dagger.inneighbors(dag, v))]
        @test length(roots) == 8
        order = Dagger.datadeps_launch_order(dag, true)
        @test order[1:8] == roots
    end

    @testset "Unresolved aliases preserve RAW, WAR and WAW" begin
        for modifier in (Dagger.In, Dagger.Out, Dagger.InOut)
            A = ones(4)
            metas, dag = capture_launch_graph() do
                t = Dagger.@spawn identity(Dagger.In(A))
                Dagger.@spawn sum(modifier(t))
                Dagger.@spawn fill!(Dagger.Out(A), 0)
            end
            @test 3 in Dagger.outneighbors(dag, 2)
            @test Dagger.datadeps_launch_order(dag, true) == [1, 2, 3]
        end
        A = ones(4)
        metas, dag = capture_launch_graph() do
            t = Dagger.@spawn identity(Dagger.In(A))
            Dagger.@spawn fill!(Dagger.Out(A), 2)
            Dagger.@spawn sum(Dagger.In(t))
        end
        @test 3 in Dagger.outneighbors(dag, 2)

        metas, dag = capture_launch_graph() do
            t = Dagger.@spawn return_type=Any identity(Dagger.In(A))
            Dagger.@spawn sum(Dagger.In(t))
            Dagger.@spawn fill!(Dagger.Out(A), 0)
        end
        @test metas[2].unresolved_read
        @test 3 in Dagger.outneighbors(dag, 2)

        # Array-typed results are unknown even when their producers look pure.
        # An unknown writer must wait for earlier readers, and later readers
        # and writers must wait for it. Consecutive unknown writes start new
        # epochs without dropping any dependency.
        metas, dag = capture_launch_graph() do
            t = Dagger.@spawn identity(Dagger.In(A))
            Dagger.@spawn sum(Dagger.In(A))
            Dagger.@spawn fill!(Dagger.Out(t), 2)
            Dagger.@spawn sum(Dagger.In(A))
            Dagger.@spawn fill!(Dagger.Out(t), 3)
            Dagger.@spawn sum(Dagger.In(A))
        end
        @test 3 in Dagger.outneighbors(dag, 2)
        @test 4 in Dagger.outneighbors(dag, 3)
        @test 5 in Dagger.outneighbors(dag, 4)
        @test 6 in Dagger.outneighbors(dag, 5)
    end

    @testset "Returned arrays and views retain program order" begin
        for hierarchical in (false, true), producer in (identity, x -> view(x, 2:3))
            A = collect(1:4)
            chunk = Dagger.datadeps_arg_wrap(A)
            scope = Dagger.memory_space_scope(Dagger.memory_space(chunk))
            t = Dagger.with_options(; scope) do
                Dagger.spawn_datadeps(; hierarchical) do
                    a = Dagger.@spawn producer(Dagger.In(A))
                    r = Dagger.@spawn sum(Dagger.In(a))
                    Dagger.@spawn fill!(Dagger.Out(A), 0)
                    r
                end
            end
            @test fetch(t) == (producer === identity ? 10 : 5)
        end
        A = zeros(4)
        # Returning a moved array can return a replica. Keep the producer
        # on the original array's owner so this checks actual result aliasing.
        chunk = Dagger.datadeps_arg_wrap(A)
        scope = Dagger.memory_space_scope(Dagger.memory_space(chunk))
        tasks = Dagger.with_options(; scope) do
            Dagger.spawn_datadeps() do
                a = Dagger.@spawn identity(Dagger.In(A))
                Dagger.@spawn fill!(Dagger.Out(a), 7)
                r = Dagger.@spawn sum(Dagger.In(A))
                Dagger.@spawn fill!(Dagger.Out(a), 8)
                s = Dagger.@spawn sum(Dagger.In(A))
                (r, s)
            end
        end
        @test fetch.(tasks) == (28, 32)
    end

    @testset "Explicit synchronization constrains launch order" begin
        metas, dag = capture_launch_graph() do
            t = Dagger.@spawn identity(1)
            s = Dagger.@spawn identity(t)
            Dagger.@spawn syncdeps=Set([Dagger.ThunkSyncdep(s)]) identity(3)
        end
        @test 3 in Dagger.outneighbors(dag, 2)
        @test Dagger.datadeps_launch_order(dag, true) == [1, 2, 3]
        t = Dagger.spawn_datadeps() do
            p = Dagger.@spawn identity(1)
            s = Dagger.@spawn identity(p)
            Dagger.@spawn syncdeps=Set([Dagger.ThunkSyncdep(s)]) identity(3)
        end
        @test fetch(t) == 3
    end

    @testset "Cycles are reported" begin
        dag = Dagger.SimpleDiGraph(2)
        Dagger.add_edge!(dag, 1, 2)
        Dagger.add_edge!(dag, 2, 1)
        @test_throws ArgumentError Dagger.datadeps_launch_order(dag, true)
    end
end
