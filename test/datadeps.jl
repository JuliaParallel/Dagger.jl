using LinearAlgebra, Graphs

@testset "Memory Aliasing" begin
    A = rand(4)
    a = Dagger.aliasing(A)
    @test a isa Dagger.ContiguousAliasing
    @test a.span.ptr.addr == UInt(pointer(A))
    @test a.span.len == sizeof(Float64) * length(A)

    r = Ref(3)
    a = Dagger.aliasing(r)
    @test a isa Dagger.CombinedAliasing
    @test length(a.sub_ainfos) == 1
    s = only(a.sub_ainfos)
    @test s isa Dagger.ObjectAliasing
    @test s.ptr == pointer_from_objref(r)
    @test s.sz == sizeof(3)
end

function with_logs(f)
    Dagger.enable_logging!(;taskdeps=true, taskargs=true)
    try
        f()
        return Dagger.fetch_logs!()
    finally
        Dagger.disable_logging!()
    end
end
task_id(t::Dagger.DTask) = lock(Dagger.Sch.EAGER_ID_MAP) do id_map
    id_map[t.uid]
end
function taskdeps_for_task(logs::Dict{Int,<:Dict}, tid::Int)
    for w in keys(logs)
        _logs = logs[w]
        for idx in 1:length(_logs[:core])
            core_log = _logs[:core][idx]
            if core_log.category == :add_thunk && core_log.kind == :start
                taskdeps = _logs[:taskdeps][idx]::Pair{Int,Vector{Int}}
                if taskdeps[1] == tid
                    return taskdeps[2]
                end
            end
        end
    end
    error("Task $tid not found in logs")
end
function test_task_dominators(logs::Dict, tid::Int, doms::Vector; all_tids::Vector=[], nondom_check::Bool=true)
    g = SimpleDiGraph()
    tid_to_v = Dict{Int,Int}()
    seen = Set{Int}()
    to_visit = copy(all_tids)
    while !isempty(to_visit)
        this_tid = popfirst!(to_visit)
        this_tid in seen && continue
        push!(seen, this_tid)
        if !(this_tid in keys(tid_to_v))
            add_vertex!(g); tid_to_v[this_tid] = nv(g)
        end

        # Add syncdeps
        deps = taskdeps_for_task(logs, this_tid)
        for dep in deps
            if !(dep in keys(tid_to_v))
                add_vertex!(g); tid_to_v[dep] = nv(g)
            end
            add_edge!(g, tid_to_v[this_tid], tid_to_v[dep])
            push!(to_visit, dep)
        end
    end
    state = dijkstra_shortest_paths(g, tid_to_v[tid])
    any_failed = false
    @test !has_edge(g, tid_to_v[tid], tid_to_v[tid])
    any_failed |= has_edge(g, tid_to_v[tid], tid_to_v[tid])
    for dom in doms
        @test state.pathcounts[tid_to_v[dom]] > 0
        if state.pathcounts[tid_to_v[dom]] == 0
            println("Expected dominance for $dom of $tid")
            any_failed = true
        end
    end
    if nondom_check
        for nondom in all_tids
            nondom == tid && continue
            nondom in doms && continue
            @test state.pathcounts[tid_to_v[nondom]] == 0
            if state.pathcounts[tid_to_v[nondom]] > 0
                println("Expected non-dominance for $nondom of $tid")
                any_failed = true
            end
        end
    end

    # For debugging purposes
    if any_failed
        println("Failure detected!")
        println("Root: $tid")
        println("Exp. doms: $doms")
        println("All: $all_tids")
        e_vs = collect(edges(g))
        e_tids = map(e->Edge(only(filter(tv->tv[2]==src(e), tid_to_v))[1],
                             only(filter(tv->tv[2]==dst(e), tid_to_v))[1]),
                     e_vs)
        sort!(e_tids)
        for e in e_tids
            s_tid, d_tid = src(e), dst(e)
            println("Edge: $s_tid -(up)> $d_tid")
        end
    end
end

@everywhere do_nothing(Xs...) = nothing
function test_datadeps(;args_chunks::Bool,
                        args_thunks::Bool,
                        args_loc::Int,
                        aliasing::Bool)
    # Returns last value
    @test Dagger.spawn_datadeps(;aliasing) do
        42
    end == 42

    # Tasks are started and finished as spawn_datadeps returns
    ts = []
    Dagger.spawn_datadeps(;aliasing) do
        for i in 1:5
            t = Dagger.@spawn sleep(0.1)
            @test !istaskstarted(t)
        end
    end
    @test all(istaskdone, ts)

    # Rethrows any task exceptions
    @test_throws Exception Dagger.spawn_datadeps(;aliasing) do
        Dagger.@spawn error("Test")
    end

    A = rand(1)
    if args_chunks
        A = remotecall_fetch(Dagger.tochunk, args_loc, A)
    elseif args_thunks
        A = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)
    end

    # Task return values can be tracked
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps(;aliasing) do
            t1 = Dagger.@spawn fill(42, 1)
            push!(ts, t1)
            push!(ts, Dagger.@spawn copyto!(Out(A), In(t1)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    @test fetch(A)[1] == 42.0
    test_task_dominators(logs, tid_1, []; all_tids=[tid_1, tid_2])
    # FIXME: We don't record the task as a syncdep, but instead internally `fetch` the chunk
    test_task_dominators(logs, tid_2, [#=tid_1=#]; all_tids=[tid_1, tid_2])

    # R->R Non-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps(;aliasing) do
            push!(ts, Dagger.@spawn do_nothing(In(A)))
            push!(ts, Dagger.@spawn do_nothing(In(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    test_task_dominators(logs, tid_1, []; all_tids=[tid_1, tid_2])
    test_task_dominators(logs, tid_2, []; all_tids=[tid_1, tid_2])

    # R->W Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps(;aliasing) do
            push!(ts, Dagger.@spawn do_nothing(In(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    test_task_dominators(logs, tid_1, []; all_tids=[tid_1, tid_2])
    test_task_dominators(logs, tid_2, [tid_1]; all_tids=[tid_1, tid_2])

    # W->W Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps(;aliasing) do
            push!(ts, Dagger.@spawn do_nothing(Out(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    test_task_dominators(logs, tid_1, []; all_tids=[tid_1, tid_2])
    test_task_dominators(logs, tid_2, [tid_1]; all_tids=[tid_1, tid_2])

    # R->R Non-Self-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps(;aliasing) do
            push!(ts, Dagger.@spawn do_nothing(In(A), In(A)))
            push!(ts, Dagger.@spawn do_nothing(In(A), In(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    test_task_dominators(logs, tid_1, []; all_tids=[tid_1, tid_2])
    test_task_dominators(logs, tid_2, []; all_tids=[tid_1, tid_2])

    # R->W Self-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps(;aliasing) do
            push!(ts, Dagger.@spawn do_nothing(In(A), In(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A), Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    test_task_dominators(logs, tid_1, []; all_tids=[tid_1, tid_2])
    test_task_dominators(logs, tid_2, [tid_1]; all_tids=[tid_1, tid_2])

    # W->W Self-Aliasing
    ts = []
    logs = with_logs() do
        Dagger.spawn_datadeps(;aliasing) do
            push!(ts, Dagger.@spawn do_nothing(Out(A), Out(A)))
            push!(ts, Dagger.@spawn do_nothing(Out(A), Out(A)))
        end
    end
    tid_1, tid_2 = task_id.(ts)
    test_task_dominators(logs, tid_1, []; all_tids=[tid_1, tid_2])
    test_task_dominators(logs, tid_2, [tid_1]; all_tids=[tid_1, tid_2])

    if aliasing
        function wrap_chunk_thunk(f, args...)
            if args_thunks || args_chunks
                result = Dagger.@spawn scope=Dagger.scope(worker=args_loc) f(args...)
                if args_thunks
                    return result
                elseif args_chunks
                    return fetch(result; raw=true)
                end
            else
                # N.B. We don't allocate remotely for raw data
                return f(args...)
            end
        end
        B = wrap_chunk_thunk(rand, 4, 4)

        # Views
        B_ul = wrap_chunk_thunk(view, B, 1:2, 1:2)
        B_ur = wrap_chunk_thunk(view, B, 1:2, 3:4)
        B_ll = wrap_chunk_thunk(view, B, 3:4, 1:2)
        B_lr = wrap_chunk_thunk(view, B, 3:4, 3:4)
        B_mid = wrap_chunk_thunk(view, B, 2:3, 2:3)
        for (B_name, B_view) in (
                                 (:B_ul, B_ul),
                                 (:B_ur, B_ur),
                                 (:B_ll, B_ll),
                                 (:B_lr, B_lr),
                                 (:B_mid, B_mid))
            @test Dagger.will_alias(Dagger.aliasing(B), Dagger.aliasing(B_view))
            B_view === B_mid && continue
            @test Dagger.will_alias(Dagger.aliasing(B_mid), Dagger.aliasing(B_view))
        end
        local t_A, t_B, t_ul, t_ur, t_ll, t_lr, t_mid
        local t_ul2, t_ur2, t_ll2, t_lr2
        logs = with_logs() do
            Dagger.spawn_datadeps(;aliasing) do
                t_A = Dagger.@spawn do_nothing(InOut(A))
                t_B = Dagger.@spawn do_nothing(InOut(B))
                t_ul = Dagger.@spawn do_nothing(InOut(B_ul))
                t_ur = Dagger.@spawn do_nothing(InOut(B_ur))
                t_ll = Dagger.@spawn do_nothing(InOut(B_ll))
                t_lr = Dagger.@spawn do_nothing(InOut(B_lr))
                t_mid = Dagger.@spawn do_nothing(InOut(B_mid))
                t_ul2 = Dagger.@spawn do_nothing(InOut(B_ul))
                t_ur2 = Dagger.@spawn do_nothing(InOut(B_ur))
                t_ll2 = Dagger.@spawn do_nothing(InOut(B_ll))
                t_lr2 = Dagger.@spawn do_nothing(InOut(B_lr))
            end
        end
        tid_A, tid_B, tid_ul, tid_ur, tid_ll, tid_lr, tid_mid =
            task_id.([t_A, t_B, t_ul, t_ur, t_ll, t_lr, t_mid])
        tid_ul2, tid_ur2, tid_ll2, tid_lr2 =
            task_id.([t_ul2, t_ur2, t_ll2, t_lr2])
        tids_all = [tid_A, tid_B, tid_ul, tid_ur, tid_ll, tid_lr, tid_mid,
                    tid_ul2, tid_ur2, tid_ll2, tid_lr2]
        test_task_dominators(logs, tid_A, []; all_tids=tids_all)
        test_task_dominators(logs, tid_B, []; all_tids=tids_all)
        test_task_dominators(logs, tid_ul, [tid_B]; all_tids=tids_all)
        test_task_dominators(logs, tid_ur, [tid_B]; all_tids=tids_all)
        test_task_dominators(logs, tid_ll, [tid_B]; all_tids=tids_all)
        test_task_dominators(logs, tid_lr, [tid_B]; all_tids=tids_all)
        test_task_dominators(logs, tid_mid, [tid_B, tid_ul, tid_ur, tid_ll, tid_lr]; all_tids=tids_all)
        test_task_dominators(logs, tid_ul2, [tid_B, tid_mid, tid_ul]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_ur2, [tid_B, tid_mid, tid_ur]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_ll2, [tid_B, tid_mid, tid_ll]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_lr2, [tid_B, tid_mid, tid_lr]; all_tids=tids_all, nondom_check=false)

        # (Unit)Upper/LowerTriangular and Diagonal
        B_upper = wrap_chunk_thunk(UpperTriangular, B)
        B_unitupper = wrap_chunk_thunk(UnitUpperTriangular, B)
        B_lower = wrap_chunk_thunk(LowerTriangular, B)
        B_unitlower = wrap_chunk_thunk(UnitLowerTriangular, B)
        for (B_name, B_view) in (
                                 (:B_upper, B_upper),
                                 (:B_unitupper, B_unitupper),
                                 (:B_lower, B_lower),
                                 (:B_unitlower, B_unitlower))
            @test Dagger.will_alias(Dagger.aliasing(B), Dagger.aliasing(B_view))
        end
        @test Dagger.will_alias(Dagger.aliasing(B_upper), Dagger.aliasing(B_lower))
        @test !Dagger.will_alias(Dagger.aliasing(B_unitupper), Dagger.aliasing(B_unitlower))
        @test Dagger.will_alias(Dagger.aliasing(B_upper), Dagger.aliasing(B_unitupper))
        @test Dagger.will_alias(Dagger.aliasing(B_lower), Dagger.aliasing(B_unitlower))

        @test Dagger.will_alias(Dagger.aliasing(B_upper), Dagger.aliasing(B, Diagonal))
        @test Dagger.will_alias(Dagger.aliasing(B_lower), Dagger.aliasing(B, Diagonal))
        @test !Dagger.will_alias(Dagger.aliasing(B_unitupper), Dagger.aliasing(B, Diagonal))
        @test !Dagger.will_alias(Dagger.aliasing(B_unitlower), Dagger.aliasing(B, Diagonal))

        local t_A, t_B, t_upper, t_unitupper, t_lower, t_unitlower, t_diag
        local t_upper2, t_unitupper2, t_lower2, t_unitlower2
        logs = with_logs() do
            Dagger.spawn_datadeps(;aliasing) do
                t_A = Dagger.@spawn do_nothing(InOut(A))
                t_B = Dagger.@spawn do_nothing(InOut(B))
                t_upper = Dagger.@spawn do_nothing(InOut(B_upper))
                t_unitupper = Dagger.@spawn do_nothing(InOut(B_unitupper))
                t_lower = Dagger.@spawn do_nothing(InOut(B_lower))
                t_unitlower = Dagger.@spawn do_nothing(InOut(B_unitlower))
                t_diag = Dagger.@spawn do_nothing(Deps(B, InOut(Diagonal)))
                t_unitlower2 = Dagger.@spawn do_nothing(InOut(B_unitlower))
                t_lower2 = Dagger.@spawn do_nothing(InOut(B_lower))
                t_unitupper2 = Dagger.@spawn do_nothing(InOut(B_unitupper))
                t_upper2 = Dagger.@spawn do_nothing(InOut(B_upper))
            end
        end
        tid_A, tid_B, tid_upper, tid_unitupper, tid_lower, tid_unitlower, tid_diag =
            task_id.([t_A, t_B, t_upper, t_unitupper, t_lower, t_unitlower, t_diag])
        tid_upper2, tid_unitupper2, tid_lower2, tid_unitlower2 =
            task_id.([t_upper2, t_unitupper2, t_lower2, t_unitlower2])
        tids_all = [tid_A, tid_B, tid_upper, tid_unitupper, tid_lower, tid_unitlower, tid_diag,
                    tid_upper2, tid_unitupper2, tid_lower2, tid_unitlower2]
        test_task_dominators(logs, tid_A, []; all_tids=tids_all)
        test_task_dominators(logs, tid_B, []; all_tids=tids_all)
        # FIXME: Proper non-dominance checks
        test_task_dominators(logs, tid_upper, [tid_B]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_unitupper, [tid_B, tid_upper]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_lower, [tid_B, tid_upper]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_unitlower, [tid_B, tid_lower]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_diag, [tid_B, tid_upper, tid_lower]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_unitlower2, [tid_B, tid_lower, tid_unitlower]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_lower2, [tid_B, tid_lower, tid_unitlower, tid_diag, tid_unitlower2]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_unitupper2, [tid_B, tid_upper, tid_unitupper]; all_tids=tids_all, nondom_check=false)
        test_task_dominators(logs, tid_upper2, [tid_B, tid_upper, tid_unitupper, tid_diag, tid_unitupper2]; all_tids=tids_all, nondom_check=false)

        # Additional aliasing tests
        views_overlap(x, y) = Dagger.will_alias(Dagger.aliasing(x), Dagger.aliasing(y))

        A = wrap_chunk_thunk(identity, B)

        A_r1 = wrap_chunk_thunk(view, A, 1:1, 1:4)
        A_r2 = wrap_chunk_thunk(view, A, 2:2, 1:4)
        B_r1 = wrap_chunk_thunk(view, B, 1:1, 1:4)
        B_r2 = wrap_chunk_thunk(view, B, 2:2, 1:4)

        A_c1 = wrap_chunk_thunk(view, A, 1:4, 1:1)
        A_c2 = wrap_chunk_thunk(view, A, 1:4, 2:2)
        B_c1 = wrap_chunk_thunk(view, B, 1:4, 1:1)
        B_c2 = wrap_chunk_thunk(view, B, 1:4, 2:2)

        A_mid = wrap_chunk_thunk(view, A, 2:3, 2:3)
        B_mid = wrap_chunk_thunk(view, B, 2:3, 2:3)

        @test views_overlap(A_r1, A_r1)
        @test views_overlap(B_r1, B_r1)
        @test views_overlap(A_c1, A_c1)
        @test views_overlap(B_c1, B_c1)

        @test views_overlap(A_r1, B_r1)
        @test views_overlap(A_r2, B_r2)
        @test views_overlap(A_c1, B_c1)
        @test views_overlap(A_c2, B_c2)

        @test !views_overlap(A_r1, A_r2)
        @test !views_overlap(B_r1, B_r2)
        @test !views_overlap(A_c1, A_c2)
        @test !views_overlap(B_c1, B_c2)

        @test views_overlap(A_r1, A_c1)
        @test views_overlap(A_r1, B_c1)
        @test views_overlap(A_r2, A_c2)
        @test views_overlap(A_r2, B_c2)

        for (name, mid) in ((:A_mid, A_mid), (:B_mid, B_mid))
            @test !views_overlap(A_r1, mid)
            @test !views_overlap(B_r1, mid)
            @test !views_overlap(A_c1, mid)
            @test !views_overlap(B_c1, mid)

            @test views_overlap(A_r2, mid)
            @test views_overlap(B_r2, mid)
            @test views_overlap(A_c2, mid)
            @test views_overlap(B_c2, mid)
        end

        @test views_overlap(A_mid, A_mid)
        @test views_overlap(A_mid, B_mid)
    end

    # FIXME: Deps

    # Scope
    exec_procs = fetch.(Dagger.spawn_datadeps(;aliasing) do
        [Dagger.@spawn Dagger.task_processor() for i in 1:10]
    end)
    unique!(exec_procs)
    scope = Dagger.get_options(:scope)
    all_procs = vcat([collect(Dagger.get_processors(OSProc(w))) for w in procs()]...)
    scope_procs = filter(proc->!isa(Dagger.constrain(scope, ExactScope(proc)), Dagger.InvalidScope), all_procs)
    for proc in exec_procs
        @test proc in scope_procs
    end
    for proc in scope_procs
        proc == Dagger.ThreadProc(1, 1) && continue
        @test proc in exec_procs
    end

    # Add-to-copy
    A = rand(1000)
    B = rand(1000)
    C = rand(1000)
    D = zeros(1000)
    add!(X, Y) = (X .+= Y;)
    expected = (A .+ B) .+ (A .+ C)
    if args_chunks
        A = remotecall_fetch(Dagger.tochunk, args_loc, A)
        B = remotecall_fetch(Dagger.tochunk, args_loc, B)
        C = remotecall_fetch(Dagger.tochunk, args_loc, C)
        D = remotecall_fetch(Dagger.tochunk, args_loc, D)
    elseif args_thunks
        A = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)
        B = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(B)
        C = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(C)
        D = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(D)
    end
    Dagger.spawn_datadeps(;aliasing) do
        Dagger.@spawn add!(InOut(B), In(A))
        Dagger.@spawn add!(InOut(C), In(A))
        Dagger.@spawn add!(InOut(C), In(B))
        Dagger.@spawn copyto!(Out(D), In(C))
    end
    @test isapprox(fetch(C), expected)
    @test isapprox(fetch(D), expected)

    # Tree reduce
    As = [rand(1000) for _ in 1:1000]
    expected = reduce((x,y)->x .+ y, As)
    if args_chunks
        As = map(A->remotecall_fetch(Dagger.tochunk, args_loc, A), As)
    elseif args_thunks
        As = map(A->(Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)), As)
    end
    Dagger.spawn_datadeps(;aliasing) do
        to_reduce = Vector[]
        push!(to_reduce, As)
        while !isempty(to_reduce)
            As = pop!(to_reduce)
            n = length(As)
            if n == 2
                Dagger.@spawn Base.mapreducedim!(identity, +, InOut(As[1]), In(As[2]))
            elseif n > 2
                push!(to_reduce, [As[1], As[div(n,2)+1]])
                push!(to_reduce, As[1:div(n,2)])
                push!(to_reduce, As[div(n,2)+1:end])
            end
        end
    end
    @test isapprox(fetch(As[1]), expected)

    # Cholesky
    m, n = 1000, 1000
    nb = 100
    mt, nt = fld(m+nb-1, nb), fld(n+nb-1, nb)
    M_dense = rand(m, n)
    # Make M positive definite
    M_dense = M_dense * M_dense'
    expected = copy(M_dense); LAPACK.potrf!('L', expected)
    M = [M_dense[i:(i+nb-1), j:(j+nb-1)] for i in 1:nb:m, j in 1:nb:n]
    if args_chunks
        M = map(m->remotecall_fetch(Dagger.tochunk, args_loc, m), M)
    elseif args_thunks
        M = map(m->(Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(m)), M)
    end
    Dagger.spawn_datadeps(;aliasing) do
        for k in range(1, mt)
            Dagger.@spawn LAPACK.potrf!('L', InOut(M[k, k]))
            for _m in range(k+1, mt)
                Dagger.@spawn BLAS.trsm!('R', 'L', 'T', 'N', 1.0, In(M[k, k]), InOut(M[_m, k]))
            end
            for _n in range(k+1, nt)
                Dagger.@spawn BLAS.syrk!('L', 'N', -1.0, In(M[_n, k]), 1.0, InOut(M[_n, _n]))
                for _m in range(_n+1, mt)
                    Dagger.@spawn BLAS.gemm!('N', 'T', -1.0, In(M[_m, k]), In(M[_n, k]), 1.0, InOut(M[_m, _n]))
                end
            end
        end
    end
    for i in 1:nb:m, j in 1:nb:n
        M_dense[i:(i+nb-1), j:(j+nb-1)] .= fetch(M[div(i,nb)+1, div(j,nb)+1])
    end
    @test isapprox(M_dense, expected)
end

@testset "$(aliasing ? "With" : "Without") Aliasing Support" for aliasing in (true, false)
    @testset "$args_mode Data" for args_mode in (:Raw, :Chunk, :Thunk)
        args_chunks = args_mode == :Chunk
        args_thunks = args_mode == :Thunk
        for nw in (1, 2)
            args_loc = nw == 2 ? 2 : 1
            for nt in (1, 2)
                if nprocs() >= nw && Threads.nthreads() >= nt
                    @testset "$nw Workers, $nt Threads" begin
                        Dagger.with_options(;scope=Dagger.scope(workers=1:nw, threads=1:nt)) do
                            test_datadeps(;args_chunks, args_thunks, args_loc, aliasing)
                        end
                    end
                end
            end
        end
    end
end
