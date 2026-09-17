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

option_default_target(x; ignored=nothing) = x
Dagger.@option :meta option_default_target(Integer) = true
Dagger.@option :get_result option_default_target(AbstractFloat) = true
Dagger.default_option(::Val{:name}, ::Type{typeof(option_default_target)},
                      ::Type{T}) where {T<:AbstractFloat} = string(T)

@testset "Signature option defaults" begin
    positional = Dagger.Signature(Any[typeof(option_default_target), Int])
    keyword = Dagger.Signature(Any[typeof(Core.kwcall), @NamedTuple{ignored::String},
                                   typeof(option_default_target), Int])
    other_keyword = Dagger.Signature(Any[typeof(Core.kwcall), @NamedTuple{other::Float64},
                                         typeof(option_default_target), Int])
    @test positional.hash_nokw == keyword.hash_nokw == other_keyword.hash_nokw
    @test positional.hash != keyword.hash

    # Exercise both the vector and kwarg-view cache-miss paths, independently
    # of the cache entries left by other tests in this task.
    cache = Dagger.SIGNATURE_DEFAULT_CACHE[]
    for sig in (positional, keyword, other_keyword)
        for option in (:meta, :get_result)
            key = (sig.hash_nokw, option)
            delete!(cache.cache, key)
            delete!(cache.freq, key)
        end
        opts = Dagger.populate_defaults!(Dagger.Options(), sig)
        @test opts.meta === true
        @test opts.get_result === nothing
        @test Dagger.populate_defaults!(Dagger.Options(; meta=false), sig).meta === false
    end

    floating = Dagger.Signature(Any[typeof(option_default_target), Float64])
    opts = Dagger.populate_defaults!(Dagger.Options(), floating)
    @test opts.meta === nothing
    @test opts.get_result === true
    @test opts.name == "Float64"
    @test Dagger.default_option(Val(:meta), typeof(option_default_target), String) === nothing
    @test_throws ArgumentError Dagger.default_option(Val(:meta))
end

@testset "Scope propagation" begin
    first_wid = first(workers())
    last_wid = last(workers())
    for (option, default, value, value2) in [
        # Special handling
        (:scope, AnyScope(), ProcessScope(first_wid), ProcessScope(last_wid)),
        # Options field
        (:single, 0, first_wid, last_wid),
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

    # Test previous option preservation
    Dagger.with_options(scope=Dagger.scope(worker=last_wid)) do
        Dagger.with_options(meta=true) do
            @test haskey(Dagger.get_options(), :meta)
            @test Dagger.get_options(:meta) == true
            @test haskey(Dagger.get_options(), :scope)
            @test Dagger.get_options(:scope) == Dagger.scope(worker=last_wid)
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
    if nprocs() > 1
        Dagger.with_options(;scope=Dagger.ExactScope(Dagger.ThreadProc(first_wid,1))) do
            @test fetch(Dagger.@spawn sf(obj)) == 0
            @test fetch(Dagger.@spawn sf(obj)) == 0
        end
    end
    Dagger.with_options(;scope=Dagger.ExactScope(Dagger.ThreadProc(1,1)), meta=true) do
        @test fetch(Dagger.@spawn sf(obj)) == 43
        @test fetch(Dagger.@spawn sf(obj)) == 43
    end
end

@testset "Propagation list ownership" begin
    # `propagates` vectors belong to the caller: spawning must not append to,
    # filter, or reorder them, however many tasks are spawned.
    scoped_props = Symbol[:scope]
    Dagger.with_options(; propagates=scoped_props,
                          scope=Dagger.ExactScope(Dagger.ThreadProc(1,1))) do
        @test fetch(Dagger.@spawn 1+1) == 2
        @test fetch(Dagger.@spawn 1+1) == 2
    end
    @test scoped_props == Symbol[:scope]

    # Likewise for a `propagates` passed directly to a task.
    task_props = Symbol[:meta]
    @test fetch(Dagger.@spawn propagates=task_props meta=false 1+1) == 2
    @test task_props == Symbol[:meta]
end
