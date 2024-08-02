@testset "Chunk Scopes" begin
    wid1, wid2 = addprocs(2, exeflags=["-t 2"])
    @everywhere [wid1,wid2] using Dagger
    Dagger.addprocs!(Dagger.Sch.eager_context(), [wid1,wid2])
    fetch(Dagger.@spawn 1+1) # Force scheduler to pick up new workers

    # Tests run locally
    @test Dagger.system_uuid(myid()) == Dagger.system_uuid(wid2)
    @test Dagger.system_uuid(wid1) == Dagger.system_uuid(wid2)

    # Test different nodes by creating fake UUIDs
    wid1_uuid, wid2_uuid = uuid4(), uuid4()
    @everywhere Dagger.SYSTEM_UUIDS[$wid1] = $wid1_uuid
    @everywhere Dagger.SYSTEM_UUIDS[$wid2] = $wid2_uuid

    # Emulate running "remotely"
    @test Dagger.system_uuid(wid1) == wid1_uuid
    @test Dagger.system_uuid(wid2) == wid2_uuid
    @test Dagger.system_uuid(myid()) != Dagger.system_uuid(wid2)
    @test Dagger.system_uuid(wid1) != Dagger.system_uuid(wid2)

    ns1, ns2 = NodeScope(wid1_uuid), NodeScope(wid2_uuid)
    ns1_ch = Dagger.tochunk(nothing, OSProc(), ns1)
    ns2_ch = Dagger.tochunk(nothing, OSProc(), ns2)

    ps1, ps2 = ProcessScope(wid1), ProcessScope(wid2)
    ps1_ch = Dagger.tochunk(nothing, OSProc(), ps1)
    ps2_ch = Dagger.tochunk(nothing, OSProc(), ps2)

    es1, es2 = ExactScope(Dagger.ThreadProc(wid1, 1)), ExactScope(Dagger.ThreadProc(wid2, 2))
    es1_ch = Dagger.tochunk(nothing, OSProc(), es1)
    es2_ch = Dagger.tochunk(nothing, OSProc(), es2)

    os1 = ExactScope(OSProc(1))

    @testset "Default Scope" begin
        ds = DefaultScope()
        for (s1, s2) in ((ds, es1), (es1, ds))
            @test Dagger.constrain(s1, s2) == es1
        end
        for (s1, s2) in ((ds, os1), (os1, ds))
            @test Dagger.constrain(s1, s2) isa Dagger.InvalidScope
        end
    end
    @testset "Node Scope" begin
        @everywhere node_scope_test(ch...) = Dagger.system_uuid()

        # One node
        ts = fetch.([Dagger.@spawn node_scope_test(isodd(i) ? ns1_ch : ns2_ch) for i in 1:20])
        @test all(x->x==wid1_uuid, ts[1:2:20])
        @test all(x->x==wid2_uuid, ts[2:2:20])

        # Same node
        t = fetch(Dagger.@spawn node_scope_test(ns1_ch, ns1_ch))
        @test t == wid1_uuid

        # Different nodes
        for (ch1, ch2) in [(ns1_ch, ns2_ch), (ns2_ch, ns1_ch)]
            @test_throws_unwrap Dagger.DTaskFailedException ex.reason<"Scopes are not compatible:" fetch(Dagger.@spawn ch1 + ch2)
        end
    end
    @testset "Process Scope" begin
        @everywhere process_scope_test(ch...) = myid()
        @test ps1.parent.uuid == Dagger.system_uuid(wid1)
        @test ps2.parent.uuid == Dagger.system_uuid(wid2)

        # One process
        ts = fetch.([Dagger.@spawn process_scope_test(isodd(i) ? ps1_ch : ps2_ch) for i in 1:20])
        @test all(x->x==wid1, ts[1:2:20])
        @test all(x->x==wid2, ts[2:2:20])

        # Same process
        t = fetch(Dagger.@spawn process_scope_test(ps1_ch, ps1_ch))
        @test t == wid1

        # Different process
        for (ch1, ch2) in [(ps1_ch, ps2_ch), (ps2_ch, ps1_ch)]
            @test_throws_unwrap Dagger.DTaskFailedException ex.reason<"Scopes are not compatible:" fetch(Dagger.@spawn ch1 + ch2)
        end

        # Same process and node
        @test fetch(Dagger.@spawn process_scope_test(ps1_ch, ns1_ch)) == wid1

        # Different process and node
        for (ch1, ch2) in [(ps1_ch, ns2_ch), (ns2_ch, ps1_ch)]
            @test_throws_unwrap Dagger.DTaskFailedException ex.reason<"Scopes are not compatible:" fetch(Dagger.@spawn ch1 + ch2)
        end
    end
    @testset "Exact Scope" begin
        @everywhere exact_scope_test(ch...) = Dagger.task_processor()
        @test es1.parent.wid == wid1
        @test es1.parent.parent.uuid == Dagger.system_uuid(wid1)
        @test es2.parent.wid == wid2
        @test es2.parent.parent.uuid == Dagger.system_uuid(wid2)

        # One process
        ts = fetch.([Dagger.@spawn exact_scope_test(isodd(i) ? es1_ch : es2_ch) for i in 1:20])
        @test all(x->x.owner==wid1, ts[1:2:20])
        @test all(x->x.owner==wid2&&x.tid==2, ts[2:2:20])

        # Same process
        t = fetch(Dagger.@spawn exact_scope_test(es1_ch, es1_ch))
        @test t.owner == wid1 && t.tid == 1

        # Different process, different processor
        for (ch1, ch2) in [(es1_ch, es2_ch), (es2_ch, es1_ch)]
            @test_throws_unwrap Dagger.DTaskFailedException ex.reason<"Scopes are not compatible:" fetch(Dagger.@spawn ch1 + ch2)
        end

        # Same process, different processor
        es1_2 = ExactScope(Dagger.ThreadProc(wid1, 2))
        es1_2_ch = Dagger.tochunk(nothing, OSProc(), es1_2)
        for (ch1, ch2) in [(es1_ch, es1_2_ch), (es1_2_ch, es1_ch)]
            @test_throws_unwrap Dagger.DTaskFailedException ex.reason<"Scopes are not compatible:" fetch(Dagger.@spawn ch1 + ch2)
        end
    end
    @testset "Union Scope" begin
        # One inner scope
        us_es1_ch = Dagger.tochunk(nothing, OSProc(), UnionScope(es1))
        @test fetch(Dagger.@spawn exact_scope_test(us_es1_ch)) == es1.processor

        # Multiple redundant inner scopes
        us_es1_multi_ch = Dagger.tochunk(nothing, OSProc(), UnionScope(es1, es1))
        @test fetch(Dagger.@spawn exact_scope_test(us_es1_multi_ch)) == es1.processor

        # No inner scopes
        @test UnionScope() isa UnionScope

        # Same inner scope
        @test fetch(Dagger.@spawn exact_scope_test(us_es1_ch, us_es1_ch)) == es1.processor

        # Extra unmatched inner scope
        us_es1_es2_ch = Dagger.tochunk(nothing, OSProc(), UnionScope(es1, es2))
        for (ch1, ch2) in [(us_es1_ch, us_es1_es2_ch), (us_es1_es2_ch, us_es1_ch)]
            @test fetch(Dagger.@spawn exact_scope_test(ch1, ch2)) == es1.processor
        end
        us_res = Dagger.constrain(UnionScope(es1, es2), UnionScope(es1))
        @test us_res isa UnionScope
        @test es1 in us_res.scopes
        @test !(es2 in us_res.scopes)
    end
    @testset "Processor Type Scope" begin
        pts_th = ProcessorTypeScope(Dagger.ThreadProc)
        pts_os = ProcessorTypeScope(Dagger.OSProc)

        @test Dagger.constrain(pts_th, es1) == es1
        @test Dagger.constrain(pts_th, os1) isa Dagger.InvalidScope

        @test Dagger.constrain(pts_os, es1) isa Dagger.InvalidScope
        @test Dagger.constrain(pts_os, os1) == os1

        # Duplicate
        pts_th_dup = Dagger.constrain(pts_th, pts_th)
        @test Dagger.constrain(pts_th_dup, es1) == es1
        @test Dagger.constrain(pts_th_dup, os1) isa Dagger.InvalidScope

        # Empty intersection
        pts_all = Dagger.constrain(pts_th, pts_os)
        @test Dagger.constrain(pts_all, es1) isa Dagger.InvalidScope
        @test Dagger.constrain(pts_all, os1) isa Dagger.InvalidScope
    end
    # TODO: Test scope propagation

    @testset "scope helper" begin
        @test Dagger.scope(:any) isa AnyScope
        @test Dagger.scope(:default) == DefaultScope()
        @test_throws ArgumentError Dagger.scope(:blah)
        @test Dagger.scope(()) == UnionScope()

        @test Dagger.scope(worker=wid1) ==
              Dagger.scope(workers=[wid1]) ==
              ProcessScope(wid1)
        @test Dagger.scope(workers=[wid1,wid2]) == UnionScope([ProcessScope(wid1),
                                                               ProcessScope(wid2)])
        @test Dagger.scope(workers=[]) == UnionScope()

        @test Dagger.scope(thread=1) ==
              Dagger.scope(threads=[1]) ==
              UnionScope([ExactScope(Dagger.ThreadProc(w,1)) for w in procs()])
        @test Dagger.scope(threads=[1,2]) == UnionScope([ExactScope(Dagger.ThreadProc(w,t)) for t in [1,2] for w in procs()])
        @test Dagger.scope(threads=[]) == UnionScope()

        @test Dagger.scope(worker=wid1,thread=1) ==
              Dagger.scope(thread=1,worker=wid1) ==
              Dagger.scope(workers=[wid1],thread=1) ==
              Dagger.scope(worker=wid1,threads=[1]) ==
              Dagger.scope(workers=[wid1],threads=[1]) ==
              ExactScope(Dagger.ThreadProc(wid1,1))

        @test_throws ArgumentError Dagger.scope(blah=1)
        @test_throws ArgumentError Dagger.scope(thread=1, blah=1)

        @test Dagger.scope(worker=1,thread=1) ==
              Dagger.scope((worker=1,thread=1)) ==
              Dagger.scope(((worker=1,thread=1),))
        @test Dagger.scope((worker=1,thread=1),(worker=wid1,thread=2)) ==
              Dagger.scope(((worker=1,thread=1),(worker=wid1,thread=2),)) ==
              Dagger.scope(((worker=1,thread=1),), ((worker=wid1,thread=2),)) ==
              UnionScope([ExactScope(Dagger.ThreadProc(1, 1)),
                          ExactScope(Dagger.ThreadProc(wid1, 2))])
        @test_throws ArgumentError Dagger.scope((;blah=1))
        @test_throws ArgumentError Dagger.scope((thread=1, blah=1))

        @testset "custom handler" begin
            @eval begin
                Dagger.scope_key_precedence(::Val{:gpu}) = 1
                Dagger.scope_key_precedence(::Val{:rocm}) = 2
                Dagger.scope_key_precedence(::Val{:cuda}) = 2

                # Some fake scopes to use as sentinels
                Dagger.to_scope(::Val{:gpu}, sc::NamedTuple) = ExactScope(Dagger.ThreadProc(1, sc.device))
                Dagger.to_scope(::Val{:rocm}, sc::NamedTuple) = ExactScope(Dagger.ThreadProc($wid1, sc.gpu))
                Dagger.to_scope(::Val{:cuda}, sc::NamedTuple) = ExactScope(Dagger.ThreadProc($wid2, sc.gpu))
            end

            @test Dagger.scope(gpu=1,device=2) ==
                  Dagger.scope(device=2,gpu=1) ==
                  Dagger.scope(gpu=1,device=2,blah=3) ==
                  Dagger.scope((gpu=1,device=2,blah=3)) ==
                  ExactScope(Dagger.ThreadProc(1, 2))
            @test Dagger.scope((gpu=1,device=2),(worker=1,thread=1)) ==
                  Dagger.scope((worker=1,thread=1),(device=2,gpu=1)) ==
                  UnionScope([ExactScope(Dagger.ThreadProc(1, 2)),
                              ExactScope(Dagger.ThreadProc(1, 1))])
            @test Dagger.scope((gpu=1,device=2),(device=3,gpu=1)) ==
                  UnionScope([ExactScope(Dagger.ThreadProc(1, 2)),
                              ExactScope(Dagger.ThreadProc(1, 3))])

            @test Dagger.scope(rocm=1,gpu=2) ==
                  Dagger.scope(gpu=2,rocm=1) ==
                  ExactScope(Dagger.ThreadProc(wid1, 2))
            @test Dagger.scope(cuda=1,gpu=2) ==
                  Dagger.scope(gpu=2,cuda=1) ==
                  ExactScope(Dagger.ThreadProc(wid2, 2))
            @test_throws ArgumentError Dagger.scope(rocm=1,cuda=1,gpu=2)
            @test_throws ArgumentError Dagger.scope(gpu=2,rocm=1,cuda=1)
            @test_throws ArgumentError Dagger.scope((rocm=1,cuda=1,gpu=2))
        end
    end

    @testset "compatible_processors" begin
        scope = Dagger.scope(workers=[])
        comp_procs = Dagger.compatible_processors(scope)
        @test Dagger.num_processors(scope) == length(comp_procs)
        @test !any(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid1)))
        @test !any(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid2)))

        scope = Dagger.scope(worker=wid1)
        comp_procs = Dagger.compatible_processors(scope)
        @test Dagger.num_processors(scope) == length(comp_procs)
        @test all(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid1)))
        @test !any(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid2)))

        scope = Dagger.scope(worker=wid1, thread=2)
        comp_procs = Dagger.compatible_processors(scope)
        @test Dagger.num_processors(scope) == length(comp_procs)
        @test length(comp_procs) == 1
        @test !all(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid1)))
        @test !all(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid2)))
        @test Dagger.ThreadProc(wid1, 2) in comp_procs

        scope = Dagger.scope(workers=[wid1, wid2])
        comp_procs = Dagger.compatible_processors(scope)
        @test Dagger.num_processors(scope) == length(comp_procs)
        @test all(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid1)))
        @test all(proc->proc in comp_procs, Dagger.get_processors(OSProc(wid2)))

        comp_procs = Dagger.compatible_processors()
        @test Dagger.num_processors() == length(comp_procs)
    end

    rmprocs([wid1, wid2])
end
