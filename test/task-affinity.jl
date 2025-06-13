@testset "Dagger Task affinity Tests" begin

    function shuffle!(v::AbstractVector)
        for i in length(v):-1:2
            j = rand(1:i)
            v[i], v[j] = v[j], v[i]
        end
        return v
    end

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
    end

    availprocs  = [proc for i in Dagger.procs() for proc in Dagger.get_processors(Dagger.OSProc(i))]
    availscopes = shuffle!(Dagger.ExactScope.(availprocs))
    numscopes   = length(availscopes)

    master_proc  = Dagger.ThreadProc(1, 1)
    master_scope = Dagger.ExactScope(master_proc)

    @testset "Dagger Function: scope, compute_scope and result_scope Tests" begin

        @everywhere f(x) = x + 1

        @testset "scope Tests" begin
            scope_only = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))

            task1 = Dagger.@spawn scope=scope_only f(10); wait(task1); fetch(task1)
            @test get_compute_scope(task1) == scope_only
            @test get_result_scope(task1) == Dagger.AnyScope()

            execution_scope1 = get_execution_scope(task1)
            @test execution_scope1 in intersect_scopes(execution_scope1,scope_only)
        end

        @testset "compute_scope Tests" begin
            compute_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
            scope              = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))

            task1 = Dagger.@spawn compute_scope=compute_scope_only f(10);             wait(task1); fetch(task1)
            task2 = Dagger.@spawn scope=scope compute_scope=compute_scope_only f(20); wait(task2); fetch(task2) 

            @test get_compute_scope(task1) == get_compute_scope(task2) == compute_scope_only
            @test get_result_scope(task1)  == get_result_scope(task2)  == Dagger.AnyScope()

            execution_scope1 = get_execution_scope(task1)
            execution_scope2 = get_execution_scope(task2)
            @test execution_scope1 in intersect_scopes(execution_scope1, compute_scope_only)  &&
                execution_scope2 in intersect_scopes(execution_scope2, compute_scope_only)
        end

        @testset "result_scope Tests" begin
            result_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))

            task1 = Dagger.@spawn result_scope=result_scope_only f(10); wait(task1); fetch(task1)

            @test get_compute_scope(task1) == Dagger.DefaultScope()
            @test get_result_scope(task1)  == result_scope_only
        end

        @testset "compute_scope and result_scope Tests with Intersection" begin
            if numscopes >= 3
                n = cld(numscopes, 3)

                scope_a = availscopes[1:n]
                scope_b = availscopes[n+1:2n]
                scope_c = availscopes[2n+1:end]

                compute_scope_intersect  = Dagger.UnionScope(scope_a..., scope_b...)
                scope_intersect          = compute_scope_intersect
                scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_intersect   = Dagger.UnionScope(scope_b..., scope_c...)

                task1 = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect f(10);                  wait(task1); fetch(task1)
                task2 = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect f(20);                                  wait(task2); fetch(task2)
                task3 = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect f(30); wait(task3); fetch(task3)

                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == result_scope_intersect

                execution_scope1   = get_execution_scope(task1)
                execution_scope2   = get_execution_scope(task2)
                execution_scope3   = get_execution_scope(task3)
                @test execution_scope1 in intersect_scopes(execution_scope1, compute_scope_intersect, result_scope_intersect) &&
                    execution_scope2 in intersect_scopes(execution_scope2, compute_scope_intersect, result_scope_intersect) &&
                    execution_scope3 in intersect_scopes(execution_scope3, compute_scope_intersect, result_scope_intersect) 
            end
        end

        @testset "compute_scope and result_scope Tests without Intersection" begin
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
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == Dagger.InvalidScope
                
                @test get_execution_scope(task1) == get_execution_scope(task2) == get_execution_scope(task3) == Dagger.InvalidScope
            end
        end

    end

    @testset "Dagger Chunk in Task: scope, compute_scope and result_scope Tests" begin 

        @everywhere g(x, y) = x * 2 + y * 3
        
        availscopes = shuffle!(Dagger.ExactScope.([proc for i in Dagger.procs() for proc in Dagger.get_processors(Dagger.OSProc(i))]))
        n = cld(numscopes, 2)

        chunk_scope = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
        chunk_proc = rand(availprocs)

        @testset "scope Tests" begin
            scope_only  = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))

            task1  = Dagger.@spawn scope=scope_only Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope); wait(task1); fetch(task1)

            @test get_compute_scope(task1) == scope_only
            @test get_result_scope(task1)  == chunk_scope

            execution_scope1     = get_execution_scope(task1)

            @test execution_scope1 == Dagger.ExactScope(chunk_proc)
        end

        @testset "compute_scope Tests" begin
            compute_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            scope              = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
            task1  = Dagger.@spawn compute_scope=compute_scope_only Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope);             wait(task1 ); fetch(task1)
            task2  = Dagger.@spawn scope=scope compute_scope=compute_scope_only Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope); wait(task2 ); fetch(task2)

            @test get_compute_scope(task1) == get_compute_scope(task2) == compute_scope_only
            @test get_result_scope(task1)  == get_result_scope(task2)  == chunk_scope

            execution_scope1  = get_execution_scope(task1)
            execution_scope2  = get_execution_scope(task2)  
            @test execution_scope1 == execution_scope2  == Dagger.ExactScope(chunk_proc)
        end

        @testset "result_scope Tests" begin
            result_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
            task1  = Dagger.@spawn result_scope=result_scope_only Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope); wait(task1 ); fetch(task1)

            @test get_compute_scope(task1) == Dagger.DefaultScope()
            @test get_result_scope(task1)  == chunk_scope 

            execution_scope1  = get_execution_scope(task1)
            @test execution_scope1  == Dagger.ExactScope(chunk_proc)
        end

        @testset "compute_scope and result_scope Tests with Intersection" begin
            if length(availscopes) >= 3
                n = cld(numscopes, 3)

                shuffle!(availscopes)
                scope_a = availscopes[1:n]
                scope_b = availscopes[n+1:2n]
                scope_c = availscopes[2n+1:end]

                compute_scope_intersect  = Dagger.UnionScope(scope_a..., scope_b...)
                scope_intersect          = compute_scope_intersect
                scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_intersect   = Dagger.UnionScope(scope_b..., scope_c...)

                task1  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope);                          wait(task1 ); fetch(task1 )
                task2  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope);                                          wait(task2 ); fetch(task2 )
                task3  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect Dagger.tochunk(g(30, 31), chunk_proc, chunk_scope);         wait(task3 ); fetch(task3 )
                
                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == chunk_scope 

                execution_scope1  = get_execution_scope(task1)
                execution_scope2  = get_execution_scope(task2)
                execution_scope3  = get_execution_scope(task3)  
                @test execution_scope1 == execution_scope2 == execution_scope3 == Dagger.ExactScope(chunk_proc)             
            end
        end

        @testset "compute_scope and result_scope Tests without Intersection" begin
            if length(availscopes) >= 2
                n = cld(length(availscopes), 2)

                shuffle!(availscopes)
                scope_a = availscopes[1:n]
                scope_b = availscopes[n+1:end]

                compute_scope_no_intersect = Dagger.UnionScope(scope_a...)
                scope_no_intersect         = Dagger.UnionScope(scope_a...)
                scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_no_intersect  = Dagger.UnionScope(scope_b...)

                task1  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope);                  wait(task1 )
                task2  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope);                                  wait(task2 )
                task3  = Dagger.@spawn compute_scope=compute_scope_no_intersect scope=scope_rand result_scope=result_scope_no_intersect Dagger.tochunk(g(30, 31), chunk_proc, chunk_scope); wait(task3 )

                @test get_compute_scope(task1) == get_compute_scope(task2) == get_compute_scope(task3) == compute_scope_no_intersect
                @test get_result_scope(task1)  == get_result_scope(task2)  == get_result_scope(task3)  == Dagger.InvalidScope 

                execution_scope1  = get_execution_scope(task1)
                execution_scope2  = get_execution_scope(task2)
                execution_scope3  = get_execution_scope(task3)  
                @test get_execution_scope(task1) == get_execution_scope(task2) == get_execution_scope(task3) == Dagger.InvalidScope
            end
        end

    end 

    @testset "Dagger Chunk Arguments in Task: scope, compute_scope and result_scope Tests with non-Intersection of chunk arg and scope" begin 

        @everywhere g(x, y) = x * 2 + y * 3
        
        availscopes = shuffle!(Dagger.ExactScope.([proc for i in Dagger.procs() for proc in Dagger.get_processors(Dagger.OSProc(i))]))
        n = cld(numscopes, 2)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:end]

        arg_scope = Dagger.UnionScope(scope_a...)
        arg_proc = rand(availprocs)
        arg    = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)

        @testset "scope Tests" begin
            scope_only  = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))

            task11 = Dagger.@spawn scope=scope_only g(arg, 11); wait(task11)

            @test get_compute_scope(task11) == scope_only
            @test get_result_scope(task11)  == Dagger.InvalidScope

            execution_scope11     = get_execution_scope(task11)

            @test execution_scope11 == Dagger.InvalidScope
        end

        @testset "compute_scope Tests" begin
            compute_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))
            scope              = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))
            
            task11 = Dagger.@spawn compute_scope=compute_scope_only  g(arg, 11);            wait(task11)
            task21 = Dagger.@spawn scope=scope compute_scope=compute_scope_only g(arg, 21); wait(task21)

           @test get_compute_scope(task11) == get_compute_scope(task21) == compute_scope_only
            @test get_result_scope(task11)  == get_result_scope(task21)  == Dagger.InvalidScope

            execution_scope11  = get_execution_scope(task11)
            execution_scope21  = get_execution_scope(task21)  
            @test execution_scope11 == execution_scope21  == Dagger.InvalidScope
        end

        @testset "result_scope Tests" begin
            result_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b))))
            
            task11  = Dagger.@spawn result_scope=result_scope_only g(arg, 11); wait(task11)

            @test get_compute_scope(task11) == Dagger.DefaultScope()
            @test get_result_scope(task11)  == Dagger.InvalidScope

            execution_scope11  = get_execution_scope(task11)
            @test execution_scope11  == Dagger.InvalidScope
        end

        @testset "compute_scope and result_scope Tests with Intersection" begin
            if length(scope_b) >= 3
                n = cld(length(scope_b), 3)

                scope_ba = scope_b[1:n]
                scope_bb = scope_b[n+1:2n]
                scope_bc = scope_b[2n+1:end]

                compute_scope_intersect  = Dagger.UnionScope(scope_ba..., scope_bb...)
                scope_intersect          = compute_scope_intersect
                scope_rand               = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_intersect   = Dagger.UnionScope(scope_bb..., scope_bc...)

                task11  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect g(arg, 11);                          wait(task11 )
                task21  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect g(arg, 21);                                          wait(task21 )
                task31  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect g(arg, 31);         wait(task31 )
                
                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == Dagger.InvalidScope

                execution_scope11  = get_execution_scope(task11)
                execution_scope21  = get_execution_scope(task21)
                execution_scope31  = get_execution_scope(task31)  
                @test execution_scope11 == execution_scope21 == execution_scope31 == Dagger.InvalidScope       
            end
        end

        @testset "compute_scope and result_scope Tests without Intersection" begin
            if length(scope_b) >= 2
                n = cld(length(scope_b), 2)

                scope_ba = scope_b[1:n]
                scope_bb = scope_b[n+1:end]

                compute_scope_no_intersect = Dagger.UnionScope(scope_ba...)
                scope_no_intersect         = Dagger.UnionScope(scope_ba...)
                scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_no_intersect  = Dagger.UnionScope(scope_bb...)

                task11  = Dagger.@spawn compute_scope=compute_scope_no_intersect result_scope=result_scope_no_intersect g(arg, 11); ;                  wait(task11 )
                task21  = Dagger.@spawn scope=scope_no_intersect result_scope=result_scope_no_intersect g(arg, 21); ;                                  wait(task21 )
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

    @testset "Dagger Chunk Arguments in Task: scope, compute_scope and result_scope Tests with Intersection of chunk arg and scope" begin 

        @everywhere g(x, y) = x * 2 + y * 3
        
        availscopes = shuffle!(Dagger.ExactScope.([proc for i in Dagger.procs() for proc in Dagger.get_processors(Dagger.OSProc(i))]))
        n = cld(numscopes, 3)
        scope_a = availscopes[1:n]
        scope_b = availscopes[n+1:2n]
        scope_c = availscopes[2n+1:end]

        arg_scope = Dagger.UnionScope(scope_a..., scope_b...)
        arg_proc = rand(availprocs)
        arg    = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)

        @testset "scope Tests" begin
            scope_only  = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)

            task11  = Dagger.@spawn scope=scope_only g(arg, 11); wait(task11); fetch(task11)

            @test get_compute_scope(task11) == scope_only
            @test get_result_scope(task11)  == Dagger.AnyScope()

            execution_scope11     = get_execution_scope(task11)

            @test execution_scope11 in intersect_scopes(execution_scope11, scope_only, arg_scope)
        end

        @testset "compute_scope Tests" begin
            compute_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)
            scope              = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)
            
            task11  = Dagger.@spawn compute_scope=compute_scope_only g(arg, 11);             wait(task11 ); fetch(task11)
            task21  = Dagger.@spawn scope=scope compute_scope=compute_scope_only g(arg, 21); wait(task21 ); fetch(task21)

            @test get_compute_scope(task11) == get_compute_scope(task21) == compute_scope_only
            @test get_result_scope(task11)  == get_result_scope(task21)  == Dagger.AnyScope()

            execution_scope11  = get_execution_scope(task11)
            execution_scope21  = get_execution_scope(task21)  
            @test execution_scope11 in intersect_scopes(execution_scope11, compute_scope_only, arg_scope) &&
                  execution_scope11 in intersect_scopes(execution_scope11, compute_scope_only, arg_scope)
        end

        @testset "result_scope Tests" begin
            result_scope_only = Dagger.UnionScope(rand(scope_b, rand(1:length(scope_b)))..., rand(scope_c, rand(1:length(scope_c)))...)
            
            task11  = Dagger.@spawn result_scope=result_scope_only g(arg, 11); wait(task11 ); fetch(task11)

            @test get_compute_scope(task11) == Dagger.DefaultScope()
            @test get_result_scope(task11)  == result_scope_only 

            execution_scope11  = get_execution_scope(task11)
            @test execution_scope11 in intersect_scopes(execution_scope11, result_scope_only, arg_scope)
        end

        @testset "compute_scope and result_scope Tests with Intersection" begin
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

                task11  = Dagger.@spawn compute_scope=compute_scope_intersect result_scope=result_scope_intersect g(arg, 11);                  wait(task11 ); fetch(task11 )
                task21  = Dagger.@spawn scope=scope_intersect result_scope=result_scope_intersect g(arg, 21);                                  wait(task21 ); fetch(task21 )
                task31  = Dagger.@spawn compute_scope=compute_scope_intersect scope=scope_rand result_scope=result_scope_intersect g(arg, 31); wait(task31 ); fetch(task31 )
                
                @test get_compute_scope(task11) == get_compute_scope(task21) == get_compute_scope(task31) == compute_scope_intersect
                @test get_result_scope(task11)  == get_result_scope(task21)  == get_result_scope(task31)  == result_scope_intersect

                execution_scope11  = get_execution_scope(task11)
                execution_scope21  = get_execution_scope(task21)
                execution_scope31  = get_execution_scope(task31)  
                @test execution_scope11 in intersect_scopes(execution_scope11, compute_scope_intersect, result_scope_intersect, arg_scope) &&
                      execution_scope21 in intersect_scopes(execution_scope21, scope_intersect, result_scope_intersect, arg_scope) &&
                      execution_scope31 in intersect_scopes(execution_scope31, compute_scope_intersect, result_scope_intersect, arg_scope)             
            end
        end

        @testset "compute_scope and result_scope Tests without Intersection" begin
            scope_bc = [scope_b...,scope_c...]
            if length(scope_bc) >= 2
                n = cld(length(scope_bc), 2)

                scope_bca = scope_bc[1:n]
                scope_bcb = scope_bc[n+1:end]

                compute_scope_no_intersect = Dagger.UnionScope(scope_bca...)
                scope_no_intersect         = Dagger.UnionScope(scope_bca...)
                scope_rand                 = Dagger.UnionScope(rand(availscopes, rand(1:length(availscopes))))
                result_scope_no_intersect  = Dagger.UnionScope(scope_bcb...)

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

    # @testset "Dagger Chunk with Chunk Arguments in Task: scope, compute_scope and result_scope Tests with non-Intersection of chunk arg and chunk scope" begin 

    #     @everywhere g(x, y) = x * 2 + y * 3
        
    #     availscopes = shuffle!(Dagger.ExactScope.([proc for i in Dagger.procs() for proc in Dagger.get_processors(Dagger.OSProc(i))]))

    #     chunk_scope = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
    #     chunk_proc = rand(chunk_scope.scopes).processor
    #     arg_scope = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
    #     arg_proc = rand(arg_scope.scopes).processor
    #     arg    = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)

    #     @testset "scope Tests" begin
    #         scope_only  = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))

    #         task11 = Dagger.@spawn scope=scope_only arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope); wait(task11); fetch(task11)

    #         @test get_compute_scope(task11) == scope_only
    #         @test get_result_scope(task11)  == chunk_scope

    #         execution_scope11     = get_execution_scope(task11)

    #         @test execution_scope11 == Dagger.ExactScope(chunk_proc)
    #     end

    #     @testset "compute_scope Tests" begin
    #         compute_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
    #         scope              = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
    #         task11 = Dagger.@spawn compute_scope=compute_scope_only  arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope);            wait(task11); fetch(task11)
    #         task21 = Dagger.@spawn scope=scope compute_scope=compute_scope_only arg -> Dagger.tochunk(g(arg, 21), chunk_proc, chunk_scope); wait(task21); fetch(task21)

    #         @test get_compute_scope(task11) == get_compute_scope(task21) == compute_scope_only
    #         @test get_result_scope(task11)  == get_result_scope(task21)  == chunk_scope

    #         execution_scope11  = get_execution_scope(task11)
    #         execution_scope21  = get_execution_scope(task21)  
    #         @test execution_scope11 == execution_scope21  == Dagger.ExactScope(chunk_proc)
    #     end

    #     @testset "result_scope Tests" begin
    #         result_scope_only = Dagger.UnionScope(rand(availscopes, rand(1:numscopes)))
            
    #         task11  = Dagger.@spawn result_scope=result_scope_only arg -> Dagger.tochunk(g(arg, 11), chunk_proc, chunk_scope); wait(task11); fetch(task11) 

    #         @test get_compute_scope(task11) == Dagger.DefaultScope()
    #         @test get_result_scope(task11)  == chunk_scope

    #         execution_scope11  = get_execution_scope(task11)
    #         @test execution_scope11  == Dagger.ExactScope(chunk_proc)
    #     end

    #     @testset "compute_scope and result_scope Tests with Intersection" begin
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

    #     @testset "compute_scope and result_scope Tests without Intersection" begin
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

end