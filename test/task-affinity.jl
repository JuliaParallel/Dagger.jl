@testset "Task affinity" begin
<<<<<<< HEAD
    fetch_or_invalidscope(x::DTask) = try
        fetch(x; raw=true)
        nothing
    catch err
        @assert Dagger.Sch.unwrap_nested_exception(err) isa Dagger.Sch.SchedulingException
        return Dagger.InvalidScope
    end
    get_compute_scope(x::DTask) = Dagger.Sch._find_thunk(x).compute_scope

    get_result_scope(x::DTask) = Dagger.Sch._find_thunk(x).result_scope

    get_final_result_scope(x::DTask) = @something(fetch_or_invalidscope(x), fetch(x; raw=true).scope)

    function get_execution_scope(x::DTask)
        res = fetch_or_invalidscope(x)
        if res !== nothing
            return res
        end
        thunk = Dagger.Sch._find_thunk(x)
        compute_scope = thunk.compute_scope
        result_scope = thunk.result_scope
        f_scope = thunk.f isa Dagger.Chunk ? thunk.f.scope : Dagger.AnyScope()
        inputs_scopes = Dagger.AbstractScope[]
        for input in thunk.inputs
            if input isa Dagger.Chunk
                push!(inputs_scopes, input.scope)
            else
                push!(inputs_scopes, Dagger.AnyScope())
            end
        end
        return Dagger.constrain(compute_scope, result_scope, f_scope, inputs_scopes...)
=======

    get_compute_scope(x::DTask) = try
        Dagger.Sch._find_thunk(x).compute_scope
    catch
        Dagger.InvalidScope
    end

    get_result_scope(x::DTask) = try
        fetch(x; raw=true).scope
    catch
        Dagger.InvalidScope
    end

    get_execution_scope(x::DTask) = try
        chunk = fetch(x; raw=true)
        Dagger.ExactScope(chunk.processor)
    catch
        Dagger.InvalidScope
    end

    function intersect_scopes(scope1::Dagger.AbstractScope, scopes::Dagger.AbstractScope...)
        for s in scopes
            scope1 = Dagger.constrain(scope1, s)
            scope1 isa Dagger.InvalidScope && return (scope1,)
        end
        return scope1.scopes
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
    end

    availprocs  = collect(Dagger.all_processors())
    availscopes = shuffle!(Dagger.ExactScope.(availprocs))
    numscopes   = length(availscopes)

    master_proc  = Dagger.ThreadProc(1, 1)
    master_scope = Dagger.ExactScope(master_proc)

<<<<<<< HEAD
    @testset "scope, compute_scope and result_scope" begin
=======
    @testset "Function: scope, compute_scope and result_scope" begin

>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        @everywhere f(x) = x + 1

        @testset "scope" begin
            scope_only = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))

            task1 = Dagger.@spawn scope=scope_only f(10); fetch(task1)
            @test get_compute_scope(task1) == scope_only
            @test get_result_scope(task1) == Dagger.AnyScope()
<<<<<<< HEAD
            @test get_final_result_scope(task1) == Dagger.AnyScope()
            @test issubset(get_execution_scope(task1), scope_only)
=======

            execution_scope1 = get_execution_scope(task1)
            @test execution_scope1 in intersect_scopes(execution_scope1,scope_only)
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        end

        @testset "compute_scope" begin
            compute_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
            scope              = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))

            task1 = Dagger.@spawn compute_scope=compute_scope_only f(10);             fetch(task1)
            task2 = Dagger.@spawn scope=scope compute_scope=compute_scope_only f(20); fetch(task2) 

            @test get_compute_scope(task1) == get_compute_scope(task2) == compute_scope_only
            @test get_result_scope(task1)  == get_result_scope(task2)  == Dagger.AnyScope()
<<<<<<< HEAD
            @test get_final_result_scope(task1) == get_final_result_scope(task2) == Dagger.AnyScope()
            @test issubset(get_execution_scope(task1), compute_scope_only) &&
                  issubset(get_execution_scope(task2), compute_scope_only)
=======

            execution_scope1 = get_execution_scope(task1)
            execution_scope2 = get_execution_scope(task2)
            @test execution_scope1 in intersect_scopes(execution_scope1, compute_scope_only)  &&
                execution_scope2 in intersect_scopes(execution_scope2, compute_scope_only)
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        end

        @testset "result_scope" begin
            result_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))

            task1 = Dagger.@spawn result_scope=result_scope_only f(10); fetch(task1)

            @test get_compute_scope(task1) == Dagger.DefaultScope()
            @test get_result_scope(task1)  == result_scope_only
<<<<<<< HEAD
            @test get_final_result_scope(task1) == result_scope_only
            @test issubset(get_execution_scope(task1), result_scope_only)
=======
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        end

        @testset "compute_scope and result_scope with intersection" begin
            if numscopes >= 3
                n = cld(numscopes, 3)

                scope_a = availscopes[1:n]
                scope_b = availscopes[n+1:2n]
                scope_c = availscopes[2n+1:end]

                compute_scope_intersect  = Dagger.UnionScope(scope_a..., scope_b...)
                scope_intersect          = compute_scope_intersect
                scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_intersect   = Dagger.UnionScope(scope_b..., scope_c...)
<<<<<<< HEAD
                all_scope_intersect      = Dagger.constrain(compute_scope_intersect, result_scope_intersect)
=======
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669

                task1 = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect f(10);                  fetch(task1)
                task2 = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect f(20);                                  fetch(task2)
                task3 = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect f(30); fetch(task3)

                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == result_scope_intersect
<<<<<<< HEAD
                @test get_final_result_scope(task1) == get_final_result_scope(task2) == get_final_result_scope(task3) == all_scope_intersect
                @test issubset(get_execution_scope(task1), all_scope_intersect) &&
                      issubset(get_execution_scope(task2), all_scope_intersect) &&
                      issubset(get_execution_scope(task3), all_scope_intersect)
=======

                execution_scope1   = get_execution_scope(task1)
                execution_scope2   = get_execution_scope(task2)
                execution_scope3   = get_execution_scope(task3)
                @test execution_scope1 in intersect_scopes(execution_scope1, compute_scope_intersect, result_scope_intersect) &&
                    execution_scope2 in intersect_scopes(execution_scope2, compute_scope_intersect, result_scope_intersect) &&
                    execution_scope3 in intersect_scopes(execution_scope3, compute_scope_intersect, result_scope_intersect) 
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
            end
        end

        @testset "compute_scope and result_scope without intersection" begin
            if length(availscopes) >= 2
                n = cld(numscopes, 2)

                scope_a = availscopes[1:n]
                scope_b = availscopes[n+1:end]

                compute_scope_no_intersect = Dagger.UnionScope(scope_a...)
                scope_no_intersect         = Dagger.UnionScope(scope_a...)
                scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_no_intersect  = Dagger.UnionScope(scope_b...)

                task1 = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect f(10);                  wait(task1)
                task2 = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect f(20);                                  wait(task2)
                task3 = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect f(30); wait(task3)

                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_no_intersect
<<<<<<< HEAD
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == result_scope_no_intersect
                @test get_final_result_scope(task1) == get_final_result_scope(task2) == get_final_result_scope(task3) == Dagger.InvalidScope
                @test get_execution_scope(task1) == get_execution_scope(task2) == get_execution_scope(task3) == Dagger.InvalidScope
            end
        end
    end

    @testset "Chunk function, scope, compute_scope and result_scope" begin 
        @everywhere g(x, y) = x * 2 + y * 3

        n = cld(numscopes, 3)

        shuffle!(availscopes)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:2n]
        scope_c = availscopes[2n+1:end]
        @testset "scope" begin
            scope_only  = Dagger.UnionScope(scope_a..., scope_b...)
            chunk_proc = rand(availprocs)
            chunk_scope = Dagger.UnionScope(scope_b..., scope_c...)
            all_scope = Dagger.constrain(scope_only, chunk_scope)

            g_chunk = Dagger.tochunk(g, chunk_proc, chunk_scope)
            task1  = Dagger.@spawn scope=scope_only g_chunk(10, 11); fetch(task1)

            @test get_compute_scope(task1) == scope_only
            @test get_result_scope(task1)  == Dagger.AnyScope()
            @test get_final_result_scope(task1) == Dagger.AnyScope()
            @test issetequal(get_execution_scope(task1), all_scope)
        end

        shuffle!(availscopes)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:2n]
        scope_c = availscopes[2n+1:end]
        @testset "compute_scope" begin
            compute_scope_only = Dagger.UnionScope(scope_a..., scope_b...)
            scope              = Dagger.UnionScope(scope_c...)
            chunk_proc         = rand(availprocs)
            chunk_scope        = Dagger.UnionScope(scope_b..., scope_c...)
            all_scope          = Dagger.constrain(compute_scope_only, chunk_scope)

            g_chunk = Dagger.tochunk(g, chunk_proc, chunk_scope)
            task1  = Dagger.@spawn compute_scope=compute_scope_only g_chunk(10, 11);             fetch(task1)
            task2  = Dagger.@spawn scope=scope compute_scope=compute_scope_only g_chunk(20, 21); fetch(task2)

            @test get_compute_scope(task1) == get_compute_scope(task2) == compute_scope_only
            @test get_result_scope(task1)  == get_result_scope(task2)  == Dagger.AnyScope()
            @test get_final_result_scope(task1) == get_final_result_scope(task2) == Dagger.AnyScope()
            @test issetequal(get_execution_scope(task1),
                                 get_execution_scope(task2),
                                 all_scope)
        end

        shuffle!(availscopes)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:2n]
        scope_c = availscopes[2n+1:end]
        @testset "result_scope" begin
            result_scope_only = Dagger.UnionScope(scope_a..., scope_b...)
            chunk_proc         = rand(availprocs)
            chunk_scope        = Dagger.UnionScope(scope_b..., scope_c...)
            all_scope = Dagger.constrain(result_scope_only, chunk_scope)

            g_chunk = Dagger.tochunk(g, chunk_proc, chunk_scope)
            task1  = Dagger.@spawn result_scope=result_scope_only g_chunk(10, 11); fetch(task1)

            @test get_compute_scope(task1) == Dagger.DefaultScope()
            @test get_result_scope(task1)  == result_scope_only
            @test get_final_result_scope(task1) == result_scope_only
            @test issetequal(get_execution_scope(task1), all_scope)
        end

        shuffle!(availscopes)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:2n]
        scope_c = availscopes[2n+1:end]
        @testset "compute_scope and result_scope with intersection" begin
            if length(availscopes) >= 3
=======
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == Dagger.InvalidScope
                
                @test get_execution_scope(task1) == get_execution_scope(task2) == get_execution_scope(task3) == Dagger.InvalidScope
            end
        end

    end

    @testset "Chunk function: scope, compute_scope and result_scope" begin 

        @everywhere g(x, y) = x * 2 + y * 3
        
        availscopes = shuffle!(Dagger.ExactScope.(collect(Dagger.all_processors())))
        n = cld(numscopes, 2)

        chunk_scope = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
        chunk_proc = rand(availprocs)

        @testset "scope" begin
            scope_only  = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))

            task1  = Dagger.@spawn scope=scope_only Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope); fetch(task1)

            @test get_compute_scope(task1) == scope_only
            @test get_result_scope(task1)  == chunk_scope

            execution_scope1     = get_execution_scope(task1)

            @test execution_scope1 == Dagger.ExactScope(chunk_proc)
        end

        @testset "compute_scope" begin
            compute_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            scope              = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
            task1  = Dagger.@spawn compute_scope=compute_scope_only Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope);             fetch(task1)
            task2  = Dagger.@spawn scope=scope compute_scope=compute_scope_only Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope); fetch(task2)

            @test get_compute_scope(task1) == get_compute_scope(task2) == compute_scope_only
            @test get_result_scope(task1)  == get_result_scope(task2)  == chunk_scope

            execution_scope1  = get_execution_scope(task1)
            execution_scope2  = get_execution_scope(task2)  
            @test execution_scope1 == execution_scope2  == Dagger.ExactScope(chunk_proc)
        end

        @testset "result_scope" begin
            result_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
            task1  = Dagger.@spawn result_scope=result_scope_only Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope); fetch(task1)

            @test get_compute_scope(task1) == Dagger.DefaultScope()
            @test get_result_scope(task1)  == chunk_scope 

            execution_scope1  = get_execution_scope(task1)
            @test execution_scope1  == Dagger.ExactScope(chunk_proc)
        end

        @testset "compute_scope and result_scope with intersection" begin
            if length(availscopes) >= 3
                n = cld(numscopes, 3)

                shuffle!(availscopes)
                scope_a = availscopes[1:n]
                scope_b = availscopes[n+1:2n]
                scope_c = availscopes[2n+1:end]

>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
                compute_scope_intersect  = Dagger.UnionScope(scope_a..., scope_b...)
                scope_intersect          = compute_scope_intersect
                scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_intersect   = Dagger.UnionScope(scope_b..., scope_c...)
<<<<<<< HEAD
                chunk_proc               = rand(availprocs)
                chunk_scope              = Dagger.UnionScope(scope_b..., scope_c...)
                all_scope = Dagger.constrain(compute_scope_intersect, result_scope_intersect, chunk_scope)

                g_chunk = Dagger.tochunk(g, chunk_proc, chunk_scope)
                task1  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect g_chunk(10, 11);                  fetch(task1)
                task2  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect g_chunk(20, 21);                                  fetch(task2)
                task3  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect g_chunk(30, 31); fetch(task3)

                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == result_scope_intersect
                @test get_final_result_scope(task1) == get_final_result_scope(task2) == get_final_result_scope(task3) == result_scope_intersect
                @test issetequal(get_execution_scope(task1),
                                     get_execution_scope(task2),
                                     get_execution_scope(task3),
                                     all_scope)
            end
        end

        shuffle!(availscopes)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:end]
=======

                task1  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope);                  fetch(task1 )
                task2  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope);                                  fetch(task2 )
                task3  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect Dagger.tochunk(g(30, 31), chunk_proc, chunk_scope); fetch(task3 )
                
                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == chunk_scope 

                execution_scope1  = get_execution_scope(task1)
                execution_scope2  = get_execution_scope(task2)
                execution_scope3  = get_execution_scope(task3)  
                @test execution_scope1 == execution_scope2 == execution_scope3 == Dagger.ExactScope(chunk_proc)             
            end
        end

>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        @testset "compute_scope and result_scope without intersection" begin
            if length(availscopes) >= 2
                n = cld(length(availscopes), 2)

<<<<<<< HEAD
=======
                shuffle!(availscopes)
                scope_a = availscopes[1:n]
                scope_b = availscopes[n+1:end]

>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
                compute_scope_no_intersect = Dagger.UnionScope(scope_a...)
                scope_no_intersect         = Dagger.UnionScope(scope_a...)
                scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_no_intersect  = Dagger.UnionScope(scope_b...)
<<<<<<< HEAD
                chunk_proc                 = rand(availprocs)
                chunk_scope                = Dagger.UnionScope(scope_b..., scope_c...)

                g_chunk = Dagger.tochunk(g, chunk_proc, chunk_scope)
                task1  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect g_chunk(10, 11);                  wait(task1)
                task2  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect g_chunk(20, 21);                                  wait(task2)
                task3  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect g_chunk(30, 31); wait(task3)

                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_no_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == result_scope_no_intersect
                @test get_final_result_scope(task1) == get_final_result_scope(task2) == get_final_result_scope(task3) == Dagger.InvalidScope
=======

                task1  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope);                  wait(task1 )
                task2  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope);                                  wait(task2 )
                task3  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect Dagger.tochunk(g(30, 31), chunk_proc, chunk_scope); wait(task3 )

                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_no_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == Dagger.InvalidScope 

                execution_scope1  = get_execution_scope(task1)
                execution_scope2  = get_execution_scope(task2)
                execution_scope3  = get_execution_scope(task3)  
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
                @test get_execution_scope(task1) == get_execution_scope(task2) == get_execution_scope(task3) == Dagger.InvalidScope
            end
        end

    end 

<<<<<<< HEAD
    @testset "Chunk arguments, scope, compute_scope and result_scope with non-intersection of chunk arg and scope" begin 
        @everywhere g(x, y) = x * 2 + y * 3

=======
    @testset "Chunk arguments: scope, compute_scope and result_scope with non-intersection of chunk arg and scope" begin 

        @everywhere g(x, y) = x * 2 + y * 3
        
        availscopes = shuffle!(Dagger.ExactScope.(collect(Dagger.all_processors())))
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        n = cld(numscopes, 2)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:end]

        arg_scope = Dagger.UnionScope(scope_a...)
        arg_proc = rand(availprocs)
        arg    = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)

        @testset "scope" begin
            scope_only  = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))

            task11 = Dagger.@spawn scope=scope_only g(arg, 11); wait(task11)

            @test get_compute_scope(task11) == scope_only
<<<<<<< HEAD
            @test get_result_scope(task11)  == Dagger.AnyScope()
            @test get_final_result_scope(task11) == Dagger.InvalidScope
=======
            @test get_result_scope(task11)  == Dagger.InvalidScope

>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
            execution_scope11     = get_execution_scope(task11)

            @test execution_scope11 == Dagger.InvalidScope
        end

        @testset "compute_scope" begin
            compute_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))
            scope              = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))
<<<<<<< HEAD

            task11 = Dagger.@spawn compute_scope=compute_scope_only  g(arg, 11);            wait(task11)
            task21 = Dagger.@spawn scope=scope compute_scope=compute_scope_only g(arg, 21); wait(task21)

            @test get_compute_scope(task11) == get_compute_scope(task21) == compute_scope_only
            @test get_result_scope(task11)  == get_result_scope(task21)  == Dagger.AnyScope()
            @test get_final_result_scope(task11) == get_final_result_scope(task21) == Dagger.InvalidScope
            @test get_execution_scope(task11) == get_execution_scope(task21) == Dagger.InvalidScope
=======
            
            task11 = Dagger.@spawn compute_scope=compute_scope_only  g(arg, 11);            wait(task11)
            task21 = Dagger.@spawn scope=scope compute_scope=compute_scope_only g(arg, 21); wait(task21)

           @test get_compute_scope(task11) == get_compute_scope(task21) == compute_scope_only
            @test get_result_scope(task11)  == get_result_scope(task21)  == Dagger.InvalidScope

            execution_scope11  = get_execution_scope(task11)
            execution_scope21  = get_execution_scope(task21)  
            @test execution_scope11 == execution_scope21  == Dagger.InvalidScope
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        end

        @testset "result_scope" begin
            result_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))
<<<<<<< HEAD

            task11  = Dagger.@spawn result_scope=result_scope_only g(arg, 11); wait(task11)

            @test get_compute_scope(task11) == Dagger.DefaultScope()
            @test get_result_scope(task11)  == result_scope_only
            @test get_final_result_scope(task11) == Dagger.InvalidScope
            @test get_execution_scope(task11) == Dagger.InvalidScope
=======
            
            task11  = Dagger.@spawn result_scope=result_scope_only g(arg, 11); wait(task11)

            @test get_compute_scope(task11) == Dagger.DefaultScope()
            @test get_result_scope(task11)  == Dagger.InvalidScope

            execution_scope11  = get_execution_scope(task11)
            @test execution_scope11  == Dagger.InvalidScope
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        end

        @testset "compute_scope and result_scope with intersection" begin
            if length(scope_b) >= 3
                n = cld(length(scope_b), 3)

                scope_ba = scope_b[1:n]
                scope_bb = scope_b[n+1:2n]
                scope_bc = scope_b[2n+1:end]

                compute_scope_intersect  = Dagger.UnionScope(scope_ba..., scope_bb...)
                scope_intersect          = compute_scope_intersect
                scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_intersect   = Dagger.UnionScope(scope_bb..., scope_bc...)

<<<<<<< HEAD
                task11  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect g(arg, 11);                          wait(task11)
                task21  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect g(arg, 21);                                          wait(task21)
                task31  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect g(arg, 31);         wait(task31)

                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == result_scope_intersect
                @test get_final_result_scope(task11) == get_final_result_scope(task21) == get_final_result_scope(task31) == Dagger.InvalidScope
                @test get_execution_scope(task11) == get_execution_scope(task21) == get_execution_scope(task31) == Dagger.InvalidScope
=======
                task11  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect g(arg, 11);                          wait(task11 )
                task21  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect g(arg, 21);                                          wait(task21 )
                task31  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect g(arg, 31);         wait(task31 )
                
                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == Dagger.InvalidScope

                execution_scope11  = get_execution_scope(task11)
                execution_scope21  = get_execution_scope(task21)
                execution_scope31  = get_execution_scope(task31)  
                @test execution_scope11 == execution_scope21 == execution_scope31 == Dagger.InvalidScope       
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
            end
        end

        @testset "compute_scope and result_scope without intersection" begin
            if length(scope_b) >= 2
                n = cld(length(scope_b), 2)

                scope_ba = scope_b[1:n]
                scope_bb = scope_b[n+1:end]

                compute_scope_no_intersect = Dagger.UnionScope(scope_ba...)
                scope_no_intersect         = Dagger.UnionScope(scope_ba...)
                scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_no_intersect  = Dagger.UnionScope(scope_bb...)

<<<<<<< HEAD
                task11  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect g(arg, 11);                  wait(task11)
                task21  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect g(arg, 21);                                  wait(task21)
                task31  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect g(arg, 31); wait(task31)

                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_no_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == result_scope_no_intersect
                @test get_final_result_scope(task11) == get_final_result_scope(task21) == get_final_result_scope(task31) == Dagger.InvalidScope
=======
                task11  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect g(arg, 11); ;                  wait(task11 )
                task21  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect g(arg, 21); ;                                  wait(task21 )
                task31  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect g(arg, 31);   wait(task31 )

                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_no_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == Dagger.InvalidScope 

                execution_scope11  = get_execution_scope(task11)
                execution_scope21  = get_execution_scope(task21)
                execution_scope31  = get_execution_scope(task31)  
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
                @test get_execution_scope(task11) == get_execution_scope(task21) == get_execution_scope(task31) == Dagger.InvalidScope
            end
        end

    end

<<<<<<< HEAD
    @testset "Chunk arguments, scope, compute_scope and result_scope with intersection of chunk arg and scope" begin 
        @everywhere g(x, y) = x * 2 + y * 3

        shuffle!(availscopes)
=======
    @testset "Chunk arguments: scope, compute_scope and result_scope with intersection of chunk arg and scope" begin 

        @everywhere g(x, y) = x * 2 + y * 3
        
        availscopes = shuffle!(Dagger.ExactScope.(collect(Dagger.all_processors())))
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        n = cld(numscopes, 3)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:2n]
        scope_c = availscopes[2n+1:end]

        arg_scope = Dagger.UnionScope(scope_a..., scope_b...)
        arg_proc = rand(availprocs)
        arg    = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)

        @testset "scope" begin
<<<<<<< HEAD
            scope_only  = Dagger.UnionScope(scope_b..., scope_c...)
            all_scope   = Dagger.constrain(scope_only, arg_scope)
=======
            scope_only  = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669

            task11  = Dagger.@spawn scope=scope_only g(arg, 11); fetch(task11)

            @test get_compute_scope(task11) == scope_only
            @test get_result_scope(task11)  == Dagger.AnyScope()
<<<<<<< HEAD
            @test get_final_result_scope(task11) == Dagger.AnyScope()
            @test issetequal(get_execution_scope(task11), all_scope)
        end

        @testset "compute_scope" begin
            compute_scope_only = Dagger.UnionScope(scope_b..., scope_c...)
            scope              = Dagger.UnionScope(scope_b..., scope_c...)
            all_scope          = Dagger.constrain(compute_scope_only, arg_scope)

=======

            execution_scope11     = get_execution_scope(task11)

            @test execution_scope11 in intersect_scopes(execution_scope11, scope_only, arg_scope)
        end

        @testset "compute_scope" begin
            compute_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)
            scope              = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)
            
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
            task11  = Dagger.@spawn compute_scope=compute_scope_only g(arg, 11);             fetch(task11)
            task21  = Dagger.@spawn scope=scope compute_scope=compute_scope_only g(arg, 21); fetch(task21)

            @test get_compute_scope(task11) == get_compute_scope(task21) == compute_scope_only
            @test get_result_scope(task11)  == get_result_scope(task21)  == Dagger.AnyScope()
<<<<<<< HEAD
            @test get_final_result_scope(task11) == get_final_result_scope(task21) == Dagger.AnyScope()
            @test issetequal(get_execution_scope(task11),
                                 get_execution_scope(task21),
                                 all_scope)
        end

        @testset "result_scope" begin
            result_scope_only  = Dagger.UnionScope(scope_b..., scope_c...)
            all_scope          = Dagger.constrain(result_scope_only, arg_scope)

=======

            execution_scope11  = get_execution_scope(task11)
            execution_scope21  = get_execution_scope(task21)  
            @test execution_scope11 in intersect_scopes(execution_scope11, compute_scope_only, arg_scope) &&
                  execution_scope11 in intersect_scopes(execution_scope11, compute_scope_only, arg_scope)
        end

        @testset "result_scope" begin
            result_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)
            
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
            task11  = Dagger.@spawn result_scope=result_scope_only g(arg, 11); fetch(task11)

            @test get_compute_scope(task11) == Dagger.DefaultScope()
            @test get_result_scope(task11)  == result_scope_only 
<<<<<<< HEAD
            @test get_final_result_scope(task11) == result_scope_only
            @test issetequal(get_execution_scope(task11), all_scope)
=======

            execution_scope11  = get_execution_scope(task11)
            @test execution_scope11 in intersect_scopes(execution_scope11, result_scope_only, arg_scope)
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
        end

        @testset "compute_scope and result_scope with intersection" begin
            scope_bc = [scope_b...,scope_c...]
            if length(scope_bc) >= 3
                n = cld(length(scope_bc), 3)

                scope_bca = scope_bc[1:n]
                scope_bcb = scope_bc[n+1:2n]
                scope_bcc = scope_bc[2n+1:end]

                compute_scope_intersect  = Dagger.UnionScope(scope_bca..., scope_bcb...)
                scope_intersect          = compute_scope_intersect
                scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_intersect   = Dagger.UnionScope(scope_bcb..., scope_bcc...)
<<<<<<< HEAD
                all_scope                = Dagger.constrain(compute_scope_intersect, result_scope_intersect, arg_scope)

                task11  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect g(arg, 11);                  fetch(task11)
                task21  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect g(arg, 21);                                  fetch(task21)
                task31  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect g(arg, 31); fetch(task31)

                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == result_scope_intersect
                @test get_final_result_scope(task11) == get_final_result_scope(task21) == get_final_result_scope(task31) == result_scope_intersect
                @test issetequal(get_execution_scope(task11),
                                     get_execution_scope(task21),
                                     get_execution_scope(task31),
                                     all_scope)
=======

                task11  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect g(arg, 11);                  fetch(task11 )
                task21  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect g(arg, 21);                                  fetch(task21 )
                task31  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect g(arg, 31); fetch(task31 )
                
                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == result_scope_intersect

                execution_scope11  = get_execution_scope(task11)
                execution_scope21  = get_execution_scope(task21)
                execution_scope31  = get_execution_scope(task31)  
                @test execution_scope11 in intersect_scopes(execution_scope11, compute_scope_intersect, result_scope_intersect, arg_scope) &&
                      execution_scope21 in intersect_scopes(execution_scope21, scope_intersect, result_scope_intersect, arg_scope) &&
                      execution_scope31 in intersect_scopes(execution_scope31, compute_scope_intersect, result_scope_intersect, arg_scope)             
>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
            end
        end

        @testset "compute_scope and result_scope without intersection" begin
            scope_bc = [scope_b...,scope_c...]
            if length(scope_bc) >= 2
                n = cld(length(scope_bc), 2)

                scope_bca = scope_bc[1:n]
                scope_bcb = scope_bc[n+1:end]

                compute_scope_no_intersect = Dagger.UnionScope(scope_bca...)
                scope_no_intersect         = Dagger.UnionScope(scope_bca...)
                scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_no_intersect  = Dagger.UnionScope(scope_bcb...)

<<<<<<< HEAD
                task11  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect g(arg, 11);                  wait(task11)
                task21  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect g(arg, 21);                                  wait(task21)
                task31  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect g(arg, 31); wait(task31)

                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_no_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == result_scope_no_intersect
                @test get_final_result_scope(task11) == get_final_result_scope(task21) == get_final_result_scope(task31) == Dagger.InvalidScope
                @test get_execution_scope(task11) == get_execution_scope(task21) == get_execution_scope(task31) == Dagger.InvalidScope
            end
        end
    end
=======
                task11  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect g(arg, 11);                  wait(task11 )
                task21  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect g(arg, 21);                                  wait(task21 )
                task31  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect g(arg, 31); wait(task31 )

                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_no_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == Dagger.InvalidScope 

                execution_scope11  = get_execution_scope(task11)
                execution_scope21  = get_execution_scope(task21)
                execution_scope31  = get_execution_scope(task31)  
                @test get_execution_scope(task11) == get_execution_scope(task21) == get_execution_scope(task31) == Dagger.InvalidScope
            end
        end

    end 

    # @testset "Chunk function with Chunk arguments: scope, compute_scope and result_scope with non-intersection of chunk arg and chunk scope" begin 

    #     @everywhere g(x, y) = x * 2 + y * 3
        
    #     availscopes = shuffle!(Dagger.ExactScope.(collect(Dagger.all_processors())))

    #     chunk_scope = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
    #     chunk_proc = rand(chunk_scope.scopes).processor
    #     arg_scope = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
    #     arg_proc = rand(arg_scope.scopes).processor
    #     arg    = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)

    #     @testset "scope" begin
    #         scope_only  = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))

    #         task11 = Dagger.@spawn scope=scope_only arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope); fetch(task11)

    #         @test get_compute_scope(task11) == scope_only
    #         @test get_result_scope(task11)  == chunk_scope

    #         execution_scope11     = get_execution_scope(task11)

    #         @test execution_scope11 == Dagger.ExactScope(chunk_proc)
    #     end

    #     @testset "compute_scope" begin
    #         compute_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
    #         scope              = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
    #         task11 = Dagger.@spawn compute_scope=compute_scope_only  arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope);            fetch(task11)
    #         task21 = Dagger.@spawn scope=scope compute_scope=compute_scope_only arg -> Dagger.tochunk(g(arg, 21), chunk_proc, chunk_scope); fetch(task21)

    #         @test get_compute_scope(task11) == get_compute_scope(task21) == compute_scope_only
    #         @test get_result_scope(task11)  == get_result_scope(task21)  == chunk_scope

    #         execution_scope11  = get_execution_scope(task11)
    #         execution_scope21  = get_execution_scope(task21)  
    #         @test execution_scope11 == execution_scope21  == Dagger.ExactScope(chunk_proc)
    #     end

    #     @testset "result_scope" begin
    #         result_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
    #         task11  = Dagger.@spawn result_scope=result_scope_only arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope); fetch(task11) 

    #         @test get_compute_scope(task11) == Dagger.DefaultScope()
    #         @test get_result_scope(task11)  == chunk_scope

    #         execution_scope11  = get_execution_scope(task11)
    #         @test execution_scope11  == Dagger.ExactScope(chunk_proc)
    #     end

    #     @testset "compute_scope and result_scope with intersection" begin
    #         if numscopes >= 3
    #             n = cld(numscopes, 3)

    #             shuffle!(availscopes)
    #             scope_a = availscopes[1:n]
    #             scope_b = availscopes[n+1:2n]
    #             scope_c = availscopes[2n+1:end]

    #             compute_scope_intersect  = Dagger.UnionScope(scope_a..., scope_b...)
    #             scope_intersect          = compute_scope_intersect
    #             scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
    #             result_scope_intersect   = Dagger.UnionScope(scope_b..., scope_c...)

    #             task11  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope);                          wait(task11 )
    #             task21  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect arg -> Dagger.tochunk(g(arg, 21), chunk_proc, chunk_scope);                                          wait(task21 )
    #             task31  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect arg -> Dagger.tochunk(g(arg, 31), chunk_proc, chunk_scope);         wait(task31 )
                
    #             @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_intersect
    #             @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == chunk_scope

    #             execution_scope11  = get_execution_scope(task11)
    #             execution_scope21  = get_execution_scope(task21)
    #             execution_scope31  = get_execution_scope(task31)  
    #             @test execution_scope11 == execution_scope21 == execution_scope31 == Dagger.ExactScope(chunk_proc)    
    #         end
    #     end

    #     @testset "compute_scope and result_scope without intersection" begin
    #         if numscopes >= 2
    #             n = cld(numscopes, 2)

    #             shuffle!(availscopes)
    #             scope_a = availscopes[1:n]
    #             scope_b = availscopes[n+1:end]

    #             compute_scope_no_intersect = Dagger.UnionScope(scope_a...)
    #             scope_no_intersect         = Dagger.UnionScope(scope_a...)
    #             scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
    #             result_scope_no_intersect  = Dagger.UnionScope(scope_b...)

    #             task11  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope);                  wait(task11 )
    #             task21  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect arg -> Dagger.tochunk(g(arg, 21), chunk_proc, chunk_scope);                                  wait(task21 )
    #             task31  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect arg -> Dagger.tochunk(g(arg, 31), chunk_proc, chunk_scope); wait(task31 )

    #             @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_no_intersect
    #             @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == Dagger.InvalidScope 

    #             execution_scope11  = get_execution_scope(task11)
    #             execution_scope21  = get_execution_scope(task21)
    #             execution_scope31  = get_execution_scope(task31)  
    #             @test get_execution_scope(task11) == get_execution_scope(task21) == get_execution_scope(task31) == Dagger.InvalidScope
    #         end
    #     end

    # end

>>>>>>> 0b15479548bd89fc051c7c388be1e7399d2ba669
end