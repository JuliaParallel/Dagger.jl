using MetricsTracker
using MetricsTracker: ContextKey, ContextStorage, MetricStorage, MetricsSnapshot,
                     pending_context!, get_or_create_storage!, write_metric_value!,
                     index_keys_by_value
using Test
using Serialization

struct DummyKey
    id::Int
end

struct StringMetric <: AbstractMetric end
MetricsTracker.metric_applies(::StringMetric, _) = true
MetricsTracker.metric_type(::Type{StringMetric}) = String
MetricsTracker.start_metric(::StringMetric) = nothing
MetricsTracker.stop_metric(::StringMetric, _) = "hello"

struct CounterMetric <: AbstractMetric end
MetricsTracker.metric_applies(::CounterMetric, _) = true
MetricsTracker.metric_type(::Type{CounterMetric}) = Int
MetricsTracker.start_metric(::CounterMetric) = nothing
const COUNTER = Ref(0)
MetricsTracker.stop_metric(::CounterMetric, _) = (COUNTER[] += 1; COUNTER[])

@testset "MetricsTracker" begin

@testset "Type Stability" begin
    @test metric_type(TimeMetric) === UInt64
    @test metric_type(TimeMetric()) === UInt64
    @test metric_type(AllocMetric) === Base.GC_Diff
    @test metric_type(StringMetric) === String
end

@testset "Basic Cache Operations" begin
    cache = MetricsCache()
    @test isempty(snapshot(cache).contexts)

    write_metric_value!(cache, Main, :test, 1, TimeMetric(), UInt64(100))
    snap = snapshot(cache)
    @test haskey(snap.contexts, (Main, :test))
    @test lookup_value(snap, Main, :test, TimeMetric(), 1) == UInt64(100)
    @test lookup_value(snap, Main, :test, TimeMetric(), 999) === nothing
    @test lookup_value(snap, Main, :missing, TimeMetric(), 1) === nothing
end

@testset "Type-Stable Lookups" begin
    cache = MetricsCache()
    write_metric_value!(cache, Main, :test, 1, TimeMetric(), UInt64(42))
    snap = snapshot(cache)
    result = lookup_value(snap, Main, :test, TimeMetric(), 1)
    @test result isa Union{UInt64, Nothing}
    inferred_type = Base.return_types(lookup_value, Tuple{MetricsSnapshot, Module, Symbol, TimeMetric, Int})[1]
    @test inferred_type === Union{UInt64, Nothing}
end

@testset "Snapshot Generations" begin
    cache = MetricsCache()
    snap1 = snapshot(cache)
    write_metric_value!(cache, Main, :test, 1, TimeMetric(), UInt64(1))
    snap2 = snapshot(cache)
    @test snap1 !== snap2
    @test snap2.generation > snap1.generation

    snap3 = snapshot(cache)
    @test snap3 === snap2

    write_metric_value!(cache, Main, :test, 2, TimeMetric(), UInt64(2))
    snap4 = snapshot(cache)
    @test snap4.generation > snap3.generation
    @test snap4 !== snap3
end

@testset "Snapshot Isolation" begin
    cache = MetricsCache()
    write_metric_value!(cache, Main, :test, 1, TimeMetric(), UInt64(1))
    snap = snapshot(cache)
    write_metric_value!(cache, Main, :test, 2, TimeMetric(), UInt64(2))

    @test lookup_value(snap, Main, :test, TimeMetric(), 1) == UInt64(1)
    @test lookup_value(snap, Main, :test, TimeMetric(), 2) === nothing

    new_snap = snapshot(cache)
    @test lookup_value(new_snap, Main, :test, TimeMetric(), 1) == UInt64(1)
    @test lookup_value(new_snap, Main, :test, TimeMetric(), 2) == UInt64(2)
end

@testset "with_metrics Block" begin
    cache = MetricsCache()
    spec = MetricsSpec(TimeMetric(), StringMetric())
    result = with_metrics(spec, Main, :compute, 42, SyncInto(cache)) do
        sleep(0.001)
        return 99
    end
    @test result == 99
    snap = snapshot(cache)
    elapsed = lookup_value(snap, Main, :compute, TimeMetric(), 42)
    @test elapsed isa UInt64
    @test elapsed > 0
    @test lookup_value(snap, Main, :compute, StringMetric(), 42) == "hello"
end

@testset "LookupExact" begin
    cache = MetricsCache()
    write_metric_value!(cache, Main, :test, 1, TimeMetric(), UInt64(100))
    write_metric_value!(cache, Main, :test, 1, StringMetric(), "alpha")
    write_metric_value!(cache, Main, :test, 2, TimeMetric(), UInt64(200))
    write_metric_value!(cache, Main, :test, 2, StringMetric(), "beta")
    write_metric_value!(cache, Main, :test, 3, TimeMetric(), UInt64(300))
    write_metric_value!(cache, Main, :test, 3, StringMetric(), "alpha")

    snap = snapshot(cache)
    matched_time = cache_lookup(snap, Main, :test, TimeMetric(), LookupExact(StringMetric(), "alpha"))
    @test matched_time in (UInt64(100), UInt64(300))

    keys = find_keys(snap, Main, :test, LookupExact(StringMetric(), "alpha"))
    @test sort(keys) == [1, 3]

    keys_none = find_keys(snap, Main, :test, LookupExact(StringMetric(), "gamma"))
    @test isempty(keys_none)
end

@testset "LookupCustom" begin
    cache = MetricsCache()
    write_metric_value!(cache, Main, :test, 1, TimeMetric(), UInt64(50))
    write_metric_value!(cache, Main, :test, 2, TimeMetric(), UInt64(150))
    write_metric_value!(cache, Main, :test, 3, TimeMetric(), UInt64(250))

    snap = snapshot(cache)
    big_keys = find_keys(snap, Main, :test, LookupCustom(TimeMetric(), v -> v > UInt64(100)))
    @test sort(big_keys) == [2, 3]
end

@testset "Multi-Lookup Intersection" begin
    cache = MetricsCache()
    for (k, time, name) in [(1, UInt64(50), "alpha"), (2, UInt64(150), "alpha"),
                             (3, UInt64(150), "beta"), (4, UInt64(250), "beta")]
        write_metric_value!(cache, Main, :test, k, TimeMetric(), time)
        write_metric_value!(cache, Main, :test, k, StringMetric(), name)
    end

    snap = snapshot(cache)
    keys = find_keys(snap, Main, :test, (LookupExact(StringMetric(), "beta"),
                                          LookupExact(TimeMetric(), UInt64(150))))
    @test keys == [3]

    keys_multi = find_keys(snap, Main, :test, (LookupExact(StringMetric(), "alpha"),
                                                LookupCustom(TimeMetric(), v -> v < UInt64(100))))
    @test keys_multi == [1]
end

@testset "values_for_metric" begin
    cache = MetricsCache()
    for k in 1:3
        write_metric_value!(cache, Main, :test, k, TimeMetric(), UInt64(k * 100))
    end
    snap = snapshot(cache)
    vals = values_for_metric(snap, Main, :test, TimeMetric())
    @test vals isa Dict{Any, UInt64}
    @test length(vals) == 3
    @test vals[1] == UInt64(100)
end

@testset "Index Caching" begin
    cache = MetricsCache()
    write_metric_value!(cache, Main, :test, 1, StringMetric(), "x")
    write_metric_value!(cache, Main, :test, 2, StringMetric(), "y")
    snap = snapshot(cache)
    result1 = index_keys_by_value(snap, Main, :test, StringMetric(), "x")
    result2 = index_keys_by_value(snap, Main, :test, StringMetric(), "x")
    @test result1 == [1]
    @test result2 == [1]
    @test result1 === result2
end

@testset "Concurrent Reads" begin
    cache = MetricsCache()
    for k in 1:100
        write_metric_value!(cache, Main, :test, k, TimeMetric(), UInt64(k))
    end

    nthreads = max(2, Threads.nthreads())
    results = Vector{Int}(undef, nthreads)
    Threads.@threads for tid in 1:nthreads
        snap = snapshot(cache)
        local_sum = 0
        for k in 1:100
            v = lookup_value(snap, Main, :test, TimeMetric(), k)
            if v !== nothing
                local_sum += Int(v)
            end
        end
        results[tid] = local_sum
    end
    expected = sum(1:100)
    @test all(==(expected), results)
end

@testset "Concurrent Reads with Writes" begin
    cache = MetricsCache()
    for k in 1:50
        write_metric_value!(cache, Main, :test, k, TimeMetric(), UInt64(k))
    end

    stop = Ref(false)
    writer = Threads.@spawn begin
        k = 51
        while !stop[]
            write_metric_value!(cache, Main, :test, k, TimeMetric(), UInt64(k))
            k += 1
            yield()
        end
    end

    successes = 0
    for _ in 1:200
        snap = snapshot(cache)
        v = lookup_value(snap, Main, :test, TimeMetric(), 25)
        if v == UInt64(25)
            successes += 1
        end
    end
    stop[] = true
    wait(writer)
    @test successes == 200
end

@testset "Merge Caches" begin
    src = MetricsCache()
    dst = MetricsCache()
    write_metric_value!(src, Main, :test, 1, TimeMetric(), UInt64(10))
    write_metric_value!(src, Main, :test, 2, TimeMetric(), UInt64(20))
    write_metric_value!(dst, Main, :test, 3, TimeMetric(), UInt64(30))

    merge_into!(dst, src)
    snap = snapshot(dst)
    @test lookup_value(snap, Main, :test, TimeMetric(), 1) == UInt64(10)
    @test lookup_value(snap, Main, :test, TimeMetric(), 2) == UInt64(20)
    @test lookup_value(snap, Main, :test, TimeMetric(), 3) == UInt64(30)
end

@testset "Persistence - Base Snapshot" begin
    tmpdir = mktempdir()
    try
        path = joinpath(tmpdir, "metrics.dat")
        cache1 = MetricsCache()
        write_metric_value!(cache1, Main, :test, 1, TimeMetric(), UInt64(100))
        write_metric_value!(cache1, Main, :test, 2, TimeMetric(), UInt64(200))
        save_metrics(cache1, path)
        @test isfile(path * ".base")

        cache2 = MetricsCache()
        MetricsTracker.load_metrics!(cache2, path)
        snap = snapshot(cache2)
        @test lookup_value(snap, Main, :test, TimeMetric(), 1) == UInt64(100)
        @test lookup_value(snap, Main, :test, TimeMetric(), 2) == UInt64(200)
    finally
        rm(tmpdir; recursive=true, force=true)
    end
end

@testset "Persistence - Journal Replay" begin
    tmpdir = mktempdir()
    try
        path = joinpath(tmpdir, "metrics.dat")
        cache1 = MetricsCache()
        save_metrics(cache1, path)
        attach_journal!(cache1, path)

        spec = MetricsSpec(TimeMetric())
        with_metrics(spec, Main, :compute, 1, SyncInto(cache1)) do
            sleep(0.001)
            return 0
        end
        with_metrics(spec, Main, :compute, 2, SyncInto(cache1)) do
            sleep(0.001)
            return 0
        end
        detach_journal!(cache1)

        cache2 = MetricsCache()
        MetricsTracker.load_metrics!(cache2, path)
        snap = snapshot(cache2)
        v1 = lookup_value(snap, Main, :compute, TimeMetric(), 1)
        v2 = lookup_value(snap, Main, :compute, TimeMetric(), 2)
        @test v1 isa UInt64 && v1 > 0
        @test v2 isa UInt64 && v2 > 0
    finally
        rm(tmpdir; recursive=true, force=true)
    end
end

@testset "Persistence - Compaction" begin
    tmpdir = mktempdir()
    try
        path = joinpath(tmpdir, "metrics.dat")
        cache = MetricsCache()
        save_metrics(cache, path)
        attach_journal!(cache, path)

        spec = MetricsSpec(TimeMetric())
        for k in 1:5
            with_metrics(spec, Main, :compute, k, SyncInto(cache)) do
                return 0
            end
        end

        save_metrics(cache, path)
        @test isfile(path * ".base")
        @test stat(path).size == 0 || !isfile(path)
        detach_journal!(cache)
    finally
        rm(tmpdir; recursive=true, force=true)
    end
end

@testset "Reset Global Cache" begin
    write_metric_value!(global_metrics_cache(), Main, :test, 42, TimeMetric(), UInt64(7))
    @test lookup_value(snapshot(global_metrics_cache()), Main, :test, TimeMetric(), 42) == UInt64(7)
    reset_global_cache!()
    @test lookup_value(snapshot(global_metrics_cache()), Main, :test, TimeMetric(), 42) === nothing
end

end
