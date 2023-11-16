using LinearAlgebra

function test_datadeps(;args_chunks::Bool,
                        args_thunks::Bool,
                        args_loc::Int=1,
                        static::Bool,
                        traversal::Symbol)
    # Returns last value
    @test Dagger.spawn_datadeps(()->42; static, traversal) == 42

    # Throws any task exceptions
    @test_throws Exception Dagger.spawn_datadeps(;static, traversal) do
        Dagger.@spawn error("Test")
    end

    # R+R Aliasing
    A = rand(1000)
    B = rand(1000)
    expected = B .+ (10 .* (A .+ A))
    if args_chunks
        A = remotecall_fetch(Dagger.tochunk, args_loc, A)
        B = remotecall_fetch(Dagger.tochunk, args_loc, B)
    elseif args_thunks
        A = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)
        B = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(B)
    end
    add_read_alias! = (Z, X, Y) -> Z .+= X .+ Y
    Dagger.spawn_datadeps(;static, traversal) do
        for i in 1:10
            Dagger.@spawn add_read_alias!(InOut(B), In(A), In(A))
        end
    end
    @test isapprox(fetch(B), expected)

    # R+W Aliasing
    A = rand(1000)
    expected = 8 .* A
    if args_chunks
        A = remotecall_fetch(Dagger.tochunk, args_loc, A)
    elseif args_thunks
        A = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)
    end
    add! = (X, Y) -> X .+= Y
    Dagger.spawn_datadeps(;static, traversal) do
        for i in 1:3
            Dagger.@spawn add!(InOut(A), In(A))
        end
    end
    @test isapprox(fetch(A), expected)

    # W+W Aliasing
    A = rand(1000)
    expected = 27 .* A
    if args_chunks
        A = remotecall_fetch(Dagger.tochunk, args_loc, A)
    elseif args_thunks
        A = Dagger.@spawn scope=Dagger.scope(worker=args_loc) copy(A)
    end
    add_write_alias! = (X, Y) -> begin
        Z = copy(Y)
        X .+= Z
        Y .+= Z
    end
    Dagger.spawn_datadeps(;static, traversal) do
        for i in 1:3
            Dagger.@spawn add_write_alias!(InOut(A), InOut(A))
        end
    end
    @test isapprox(fetch(A), expected)

    # Scope
    exec_procs = fetch.(Dagger.spawn_datadeps(;static, traversal) do
        [Dagger.@spawn Dagger.thunk_processor() for i in 1:10]
    end)
    unique!(exec_procs)
    scope = Dagger.get_options(:scope)
    all_procs = vcat([collect(Dagger.get_processors(OSProc(w))) for w in procs()]...)
    scope_procs = filter(proc->!isa(Dagger.constrain(scope, ExactScope(proc)), Dagger.InvalidScope), all_procs)
    for proc in exec_procs
        @test proc in scope_procs
    end
    for proc in scope_procs
        if !static && proc == Dagger.ThreadProc(1,1)
            # Sch doesn't schedule tasks on the scheduler thread
            continue
        end
        @test proc in exec_procs
    end

    # Add-to-copy
    A = rand(1000)
    B = rand(1000)
    C = rand(1000)
    D = zeros(1000)
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
    Dagger.spawn_datadeps(;static, traversal) do
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
    Dagger.spawn_datadeps(;static, traversal) do
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
    Dagger.spawn_datadeps(;static, traversal) do
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
@testset "Datadeps" begin
    @testset "$(static ? "Static" : "Dynamic") Scheduling" for static in (true, false)
        if static
            @testset "$args_mode Data" for args_mode in (:Raw, :Chunk, :Thunk)
                args_chunks = args_mode == :Chunk
                args_thunks = args_mode == :Thunk
                # TODO: BFS, DFS
                @testset "$((;inorder="In-Order", bfs="BFS", dfs="DFS")[traversal]) Traversal" for traversal in (:inorder,)# :bfs, :dfs)
                    for nw in (1, 2)
                        args_loc = nw == 2 ? 2 : 1
                        for nt in (1, 2)
                            if nprocs() >= nw && Threads.nthreads() >= nt
                                @testset "$nw Workers, $nt Threads" begin
                                    Dagger.with_options(;scope=Dagger.scope(workers=1:nw, threads=1:nt)) do
                                        test_datadeps(;args_chunks, args_thunks, static, traversal, args_loc)
                                    end
                                end
                            end
                        end
                    end
                end
            end
        else
            @testset "$(args_chunks ? "Chunk" : "Raw") Data" for args_chunks in (false, true)
                for nt in (1, 2)
                    if Threads.nthreads() >= nt
                        @testset "$nt Threads" begin
                            Dagger.with_options(;scope=Dagger.scope(worker=1, threads=1:nt)) do
                                test_datadeps(;args_chunks, args_thunks=false, static, traversal=:inorder)
                            end
                        end
                    end
                end
            end
        end
    end
end
