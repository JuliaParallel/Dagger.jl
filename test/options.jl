@everywhere begin
    check_option(option, value, default) = Dagger.get_options(option, default) == value
    function inc_by_myid(x)
        m = Dagger.@spawn myid()
        fetch(Dagger.@spawn x + m)
    end
    switch_option(option, value, default) = Dagger.with_options(NamedTuple([option=>value])) do
        fetch(Dagger.@spawn Dagger.get_options(option, default))
    end

    struct SpecialFunc
        x::Int
    end
    function (sf::SpecialFunc)(obj)
        if obj isa Dagger.Chunk
            return fetch(obj)+sf.x
        else
            return sf.x
        end
    end
    Dagger.move(from::Union{OSProc,Dagger.ThreadProc}, to::Dagger.ThreadProc, sf::SpecialFunc) =
        (to.owner == 1 && to.tid == 1) ? SpecialFunc(1) : sf
end

@testset "Scope propagation" begin
    first_wid = first(workers())
    last_wid = last(workers())
    for (option, default, value, value2) in [
        # Special handling
        (:scope, AnyScope(), ProcessScope(first_wid), ProcessScope(last_wid)),
        (:processor, OSProc(), Dagger.ThreadProc(first_wid, 1), Dagger.ThreadProc(last_wid, 1)),
        # ThunkOptions field
        (:single, 0, first_wid, last_wid),
        # Thunk field
        (:meta, false, true, false)
    ]
        # Test local and remote default values
        @test Dagger.get_options(option, default) == default
        @test fetch(Dagger.@spawn check_option(option, default, default))

        # Test local propagation
        Dagger.with_options(NamedTuple([option=>value])) do
            fetch(@async @test Dagger.get_options(option, default) == value)
            fetch(Threads.@spawn @test Dagger.get_options(option, default) == value)
        end

        # Test remote option switching
        Dagger.with_options(NamedTuple([option=>value])) do
            @test fetch(Dagger.@spawn switch_option(option, value2, default)) == value2
        end
    end

    # Test scope/single is applied
    for wid in workers()
        for (option, value) in [
            (:scope, ProcessScope(wid)),
            (:single, wid)
        ]
            Dagger.with_options(NamedTuple([option=>value])) do
                @test fetch(Dagger.@spawn inc_by_myid(1)) == 1+wid
                @test fetch(Dagger.@spawn inc_by_myid(2)) == 2+wid
            end
        end
    end

    # Test processor/meta is applied
    sf = SpecialFunc(0)
    obj = Dagger.tochunk(42)
    Dagger.with_options(;scope=Dagger.ExactScope(Dagger.ThreadProc(first_wid,1))) do
        @test fetch(Dagger.@spawn sf(obj)) == 0
        @test fetch(Dagger.@spawn sf(obj)) == 0
    end
    Dagger.with_options(;scope=Dagger.ExactScope(Dagger.ThreadProc(1,1)), processor=Dagger.ThreadProc(1,1), meta=true) do
        @test fetch(Dagger.@spawn sf(obj)) == 43
        @test fetch(Dagger.@spawn sf(obj)) == 43
    end
end
