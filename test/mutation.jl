@everywhere begin
    struct DynamicHistogram
        bins::Vector{Float64}
        start::Float64
        binwidth::Float64
        maxbins::Int
        lock::Threads.SpinLock
    end
    DynamicHistogram(start, binwidth, maxbins) =
        DynamicHistogram(zeros(1), start, binwidth, maxbins, Threads.SpinLock())
    function Base.:+(hist1::T, hist2::T) where {T<:DynamicHistogram}
        bins = zeros(max(length(hist1.bins), length(hist2.bins)))
        for i in 1:length(bins)
            bins[i] += length(hist1.bins) >= i ? hist1.bins[i] : 0.0
            bins[i] += length(hist2.bins) >= i ? hist2.bins[i] : 0.0
        end
        DynamicHistogram(bins, hist1.start, hist1.binwidth, hist1.maxbins, Threads.SpinLock())
    end
    function Base.push!(hist::DynamicHistogram, value::Float64)
        idx = 1
        binstart = hist.start
        lock(hist.lock) do
            while length(hist.bins) < hist.maxbins
                # Add bins if necessary
                if idx > length(hist.bins)
                    push!(hist.bins, 0.0)
                end
                if binstart <= value <= binstart+hist.binwidth
                    hist.bins[idx] += value
                    return
                end
                idx += 1
                binstart += hist.binwidth
            end
            throw(DomainError(value, "Exhausted allowed number of bins"))
        end
    end
end

@everywhere mutable_update!(x) = x[] = myid()

@testset "Mutation" begin

@testset "@mutable" begin
    w1 = first(workers())
    @assert w1 != 1 "Not enough workers to test mutability"
    for (w, wo) in [(1, w1), (w1, 1)]
        x = Dagger.@mutable worker=w Ref{Int}()
        @test fetch(Dagger.@spawn mutable_update!(x)) == w
        wo_scope = Dagger.ProcessScope(wo)
        @test_throws_unwrap Dagger.DTaskFailedException fetch(Dagger.@spawn scope=wo_scope mutable_update!(x))
    end
end # @testset "@mutable"

@testset "Shard" begin
    s = Dagger.@shard Threads.Atomic{Int}(0)
    ctxprocs = Dagger.Sch.eager_context().procs
    for p in keys(s.chunks)
        @test p isa OSProc
        @test p in ctxprocs

        c = s.chunks[p]
        @test c.processor == p
        @test c.scope isa Dagger.ProcessScope
        @test c.scope.wid == p.pid

        @test fetch(c) isa Threads.Atomic{Int}
    end

    @testset "procs kwarg" begin
        procs = [OSProc(first(workers()))]
        s = Dagger.@shard procs=procs Threads.Atomic{Int}(0)
        @test length(keys(s.chunks)) == 1
        p = first(keys(s.chunks))
        @test p isa Dagger.OSProc
        @test p.pid == first(workers())

        c = s.chunks[p]
        @test c.processor == p
        @test c.scope isa Dagger.ProcessScope
        @test c.scope.wid == p.pid
    end

    @testset "workers kwarg" begin
        s = Dagger.@shard workers=[first(workers())] Threads.Atomic{Int}(0)
        @test length(keys(s.chunks)) == 1
        p = first(keys(s.chunks))
        @test p isa Dagger.OSProc
        @test p.pid == first(workers())

        c = s.chunks[p]
        @test c.processor == p
        @test c.scope isa Dagger.ProcessScope
        @test c.scope.wid == p.pid
    end

    @testset "per_thread kwarg" begin
        s = Dagger.@shard per_thread=true Threads.Atomic{Int}(0)
        for p in keys(s.chunks)
            @test p isa Dagger.ThreadProc
            gp = Dagger.get_parent(p)
            @test gp in ctxprocs
            @test p in Dagger.get_processors(p)

            c = s.chunks[p]
            @test c.processor == p
            @test c.scope isa Dagger.ExactScope
            @test c.scope.processor == p
        end
    end

    # Can't mix procs and workers
    @test_throws ArgumentError Dagger.@shard procs=[OSProc] workers=[1] 1+1
    # Can't mix procs and per_thread=true
    @test_throws ArgumentError Dagger.@shard procs=[OSProc] per_thread=true 1+1

    # Need at least one processor
    @test_throws ArgumentError Dagger.@shard procs=[] 1+1
    @test_throws ArgumentError Dagger.@shard workers=[] 1+1

    @testset "Distributed Histogram" begin
        cs = Dagger.@shard DynamicHistogram(0.0, 1.0, 100)
        wait.([Dagger.@spawn push!(cs, 5.2) for i in 1:100])
        @test sum(sum(fetch.(map(identity, cs))).bins) ≈ 5.2*100
    end
end # @testset "Shard"

end # @testset "Mutation"
