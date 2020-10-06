@testset "Fault tolerance" begin
    function setup_funcs(nofail)
        @everywhere begin
            $(Expr(:using, Expr(Symbol("."), :Dagger)))
            function kill_eager(x)
                _x = x+1
                sleep(1)

                _x == 2 && myid() != $nofail && exit(1)

                return _x
            end
            kill_eager(x...) = sum(x)

            kill_lazy(x) = x+1
            function kill_lazy(x...)
                _x = sum(x)
                sleep(1)

                _x == 6 && myid() != $nofail && exit(1)

                return _x
            end
        end
    end

    ## 2 workers will fail and exit while one (the last one) will complete the tasks
    setup_funcs(workers() |> last)
    for kill_func in (kill_eager, kill_lazy)
        @test workers() |> length == 3

        a = delayed(kill_func)(1)
        b = delayed(kill_func)(a)
        c = delayed(kill_func)(a)
        d = delayed(kill_func)(b, c)
        @test collect(d) == 6

        addprocs(2)
        using Dagger
        setup_funcs(workers() |> last)

        a = delayed(kill_func)(1)
        b = delayed(kill_func)(delayed(kill_func)(a))
        c = delayed(kill_func)(a, b)
        @test collect(c) == 6

        addprocs(2)
        using Dagger
        setup_funcs(workers() |> last)

        a1 = delayed(kill_func)(1)
        a2 = delayed(kill_func)(1)
        a3 = delayed(kill_func)(1)
        b1 = delayed(kill_func)(a1, a2)
        b2 = delayed(kill_func)(a2, a3)
        c = delayed(kill_func)(b1, b2)
        @test collect(c) == 8

        addprocs(2)
        using Dagger
        setup_funcs(workers() |> last)

        a = delayed(kill_func)(1)
        b = delayed(kill_func)(a)
        c = delayed(kill_func)(a)
        d = delayed(kill_func)(b, c)
        @test_throws AssertionError collect(d; options=Dagger.Sch.SchedulerOptions(single=first(workers())))

        addprocs(1)
        using Dagger
        setup_funcs(workers() |> last)
    end
end
