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

    typed_vals = values_for_metric(snap, Main, :test, TimeMetric(), Int)
    @test typed_vals isa Dict{Int, UInt64}
    inferred_typed = Base.return_types(values_for_metric,
        Tuple{MetricsSnapshot, Module, Symbol, TimeMetric, Type{Int}})[1]
    @test inferred_typed === Dict{Int, UInt64}

    inferred_untyped = Base.return_types(values_for_metric,
        Tuple{MetricsSnapshot, Module, Symbol, TimeMetric})[1]
    @test inferred_untyped === Dict{Any, UInt64}
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
        save_metrics!(cache1, path)
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
        save_metrics!(cache1, path)
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
        save_metrics!(cache, path)
        attach_journal!(cache, path)

        spec = MetricsSpec(TimeMetric())
        for k in 1:5
            with_metrics(spec, Main, :compute, k, SyncInto(cache)) do
                return 0
            end
        end

        save_metrics!(cache, path)
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

@testset "with_metrics - SyncTask" begin
    spec = MetricsSpec(TimeMetric())
    with_metrics(spec, Main, :compute_task, 11, SyncTask()) do
        sleep(0.001)
        return 0
    end
    snap = snapshot(local_metrics_cache())
    v = lookup_value(snap, Main, :compute_task, TimeMetric(), 11)
    @test v isa UInt64
    @test v > 0
end

@testset "with_metrics - SyncGlobal" begin
    reset_global_cache!()
    spec = MetricsSpec(TimeMetric())
    with_metrics(spec, Main, :compute_global, 7, SyncGlobal()) do
        sleep(0.001)
        return 0
    end
    snap = snapshot(global_metrics_cache())
    v = lookup_value(snap, Main, :compute_global, TimeMetric(), 7)
    @test v isa UInt64 && v > 0
    reset_global_cache!()
end

@testset "with_metrics - is_result_metric branch" begin
    cache = MetricsCache()
    spec = MetricsSpec(ResultShapeMetric())
    result = with_metrics(spec, Main, :compute, 1, SyncInto(cache)) do
        return [1.0, 2.0, 3.0, 4.0]
    end
    @test result == [1.0, 2.0, 3.0, 4.0]
    snap = snapshot(cache)
    shape = lookup_value(snap, Main, :compute, ResultShapeMetric(), 1)
    @test shape == (4,)

    cache2 = MetricsCache()
    with_metrics(spec, Main, :compute, 2, SyncInto(cache2)) do
        return 42
    end
    snap2 = snapshot(cache2)
    @test lookup_value(snap2, Main, :compute, ResultShapeMetric(), 2) === nothing
end

struct InapplicableMetric <: AbstractMetric end
MetricsTracker.metric_applies(::InapplicableMetric, ::Val{:applicable_ctx}) = true
MetricsTracker.metric_type(::Type{InapplicableMetric}) = Int
MetricsTracker.start_metric(::InapplicableMetric) = 99
MetricsTracker.stop_metric(::InapplicableMetric, _) = 99

@testset "with_metrics - metric_applies returning false" begin
    cache = MetricsCache()
    spec = MetricsSpec(InapplicableMetric())
    with_metrics(spec, Main, :other_ctx, 1, SyncInto(cache)) do
        return 0
    end
    snap = snapshot(cache)
    @test lookup_value(snap, Main, :other_ctx, InapplicableMetric(), 1) === nothing

    cache2 = MetricsCache()
    with_metrics(spec, Main, :applicable_ctx, 1, SyncInto(cache2)) do
        return 0
    end
    snap2 = snapshot(cache2)
    @test lookup_value(snap2, Main, :applicable_ctx, InapplicableMetric(), 1) == 99
end

@testset "with_metrics - f() throws" begin
    cache = MetricsCache()
    spec = MetricsSpec(TimeMetric())
    err = nothing
    try
        with_metrics(spec, Main, :throwing, 1, SyncInto(cache)) do
            sleep(0.001)
            error("intentional")
        end
    catch e
        err = e
    end
    @test err isa ErrorException
    snap = snapshot(cache)
    v = lookup_value(snap, Main, :throwing, TimeMetric(), 1)
    @test v isa UInt64 && v > 0
end

@testset "with_metrics - nested collection forbidden" begin
    cache = MetricsCache()
    spec = MetricsSpec(TimeMetric())
    err = nothing
    try
        with_metrics(spec, Main, :outer, 1, SyncInto(cache)) do
            with_metrics(spec, Main, :inner, 1, SyncInto(cache)) do
                return 0
            end
        end
    catch e
        err = e
    end
    @test err isa AssertionError
end

@testset "with_metrics - macro forms" begin
    cache = MetricsCache()
    spec = MetricsSpec(TimeMetric())
    result = MetricsTracker.@with_metrics spec :macro_test 100 SyncInto(cache) begin
        sleep(0.001)
        99
    end
    @test result == 99
    snap = snapshot(cache)
    v = lookup_value(snap, Main, :macro_test, TimeMetric(), 100)
    @test v isa UInt64 && v > 0
end

@testset "Built-in Metrics" begin
    @testset "TimeMetric" begin
        @test metric_type(TimeMetric) === UInt64
        s = MetricsTracker.start_metric(TimeMetric())
        @test s isa UInt64
        sleep(0.001)
        e = MetricsTracker.stop_metric(TimeMetric(), s)
        @test e isa UInt64 && e > 0
    end

    @testset "ThreadTimeMetric" begin
        @test metric_type(ThreadTimeMetric) === UInt64
        s = MetricsTracker.start_metric(ThreadTimeMetric())
        @test s isa UInt64
        sum(1:10000)
        e = MetricsTracker.stop_metric(ThreadTimeMetric(), s)
        @test e isa UInt64
    end

    @testset "CompileTimeMetric" begin
        @test metric_type(CompileTimeMetric) === Tuple{UInt64, UInt64}
        s = MetricsTracker.start_metric(CompileTimeMetric())
        @test s isa Tuple{UInt64, UInt64}
        e = MetricsTracker.stop_metric(CompileTimeMetric(), s)
        @test e isa Tuple{UInt64, UInt64}
        @test e[1] >= 0
    end

    @testset "AllocMetric" begin
        @test metric_type(AllocMetric) === Base.GC_Diff
        s = MetricsTracker.start_metric(AllocMetric())
        @test s isa Base.GC_Num
        e = MetricsTracker.stop_metric(AllocMetric(), s)
        @test e isa Base.GC_Diff
    end

    @testset "ResultShapeMetric" begin
        @test metric_type(ResultShapeMetric) === Union{Dims, Nothing}
        @test MetricsTracker.is_result_metric(ResultShapeMetric())
        @test MetricsTracker.result_metric(ResultShapeMetric(), [1, 2, 3]) == (3,)
        @test MetricsTracker.result_metric(ResultShapeMetric(), [1 2; 3 4]) == (2, 2)
        @test MetricsTracker.result_metric(ResultShapeMetric(), 42) === nothing
        @test MetricsTracker.result_metric(ResultShapeMetric(), "string") === nothing
    end

    @testset "LoadAverageMetric" begin
        @test metric_type(LoadAverageMetric) === NTuple{3, Float64}
        s = MetricsTracker.start_metric(LoadAverageMetric())
        @test s === nothing
        e = MetricsTracker.stop_metric(LoadAverageMetric(), s)
        @test e isa NTuple{3, Float64}
        @test all(>=(0.0), e)
    end

    @testset "Transfer metrics" begin
        @test metric_type(TransferTimeMetric) === UInt64
        @test metric_type(TransferSizeMetric) === UInt64
        @test metric_type(TransferRateMetric) === UInt64
    end

    @testset "TimeMetric end-to-end through with_metrics" begin
        cache = MetricsCache()
        with_metrics(MetricsSpec(TimeMetric()), Main, :bi_time, 1, SyncInto(cache)) do
            sleep(0.002)
            return 0
        end
        snap = snapshot(cache)
        v = lookup_value(snap, Main, :bi_time, TimeMetric(), 1)
        @test v isa UInt64 && v >= UInt64(1_000_000)
    end

    @testset "AllocMetric end-to-end through with_metrics" begin
        cache = MetricsCache()
        with_metrics(MetricsSpec(AllocMetric()), Main, :bi_alloc, 1, SyncInto(cache)) do
            return [zeros(1024) for _ in 1:50]
        end
        snap = snapshot(cache)
        diff = lookup_value(snap, Main, :bi_alloc, AllocMetric(), 1)
        @test diff isa Base.GC_Diff
        @test diff.allocd > 0
    end

    @testset "ResultShapeMetric end-to-end through with_metrics" begin
        cache = MetricsCache()
        with_metrics(MetricsSpec(ResultShapeMetric()), Main, :bi_shape, 1, SyncInto(cache)) do
            return rand(7, 5)
        end
        snap = snapshot(cache)
        @test lookup_value(snap, Main, :bi_shape, ResultShapeMetric(), 1) == (7, 5)
    end

    @testset "LoadAverageMetric end-to-end through with_metrics" begin
        cache = MetricsCache()
        with_metrics(MetricsSpec(LoadAverageMetric()), Main, :bi_load, 1, SyncInto(cache)) do
            return 0
        end
        snap = snapshot(cache)
        load = lookup_value(snap, Main, :bi_load, LoadAverageMetric(), 1)
        @test load isa NTuple{3, Float64}
    end
end

@testset "trim!" begin
    cache = MetricsCache()
    for k in 1:10
        write_metric_value!(cache, Main, :trim_test, k, TimeMetric(), UInt64(k))
    end
    snap_before = snapshot(cache)
    @test length(values_for_metric(snap_before, Main, :trim_test, TimeMetric())) == 10

    trim!(cache; keep_per_metric=3)
    snap_after = snapshot(cache)
    vals = values_for_metric(snap_after, Main, :trim_test, TimeMetric())
    @test length(vals) == 3
    @test 8 in keys(vals) && 9 in keys(vals) && 10 in keys(vals)
    @test !(1 in keys(vals))
end

@testset "trim! - zero keep" begin
    cache = MetricsCache()
    for k in 1:5
        write_metric_value!(cache, Main, :trim_zero, k, TimeMetric(), UInt64(k))
    end
    trim!(cache; keep_per_metric=0)
    snap = snapshot(cache)
    @test isempty(values_for_metric(snap, Main, :trim_zero, TimeMetric()))
end

@testset "trim! - keep >= length" begin
    cache = MetricsCache()
    for k in 1:3
        write_metric_value!(cache, Main, :trim_big, k, TimeMetric(), UInt64(k))
    end
    trim!(cache; keep_per_metric=100)
    snap = snapshot(cache)
    @test length(values_for_metric(snap, Main, :trim_big, TimeMetric())) == 3
end

@testset "trim! - across multiple metrics" begin
    cache = MetricsCache()
    for k in 1:6
        write_metric_value!(cache, Main, :trim_multi, k, TimeMetric(), UInt64(k))
        write_metric_value!(cache, Main, :trim_multi, k, StringMetric(), "v$k")
    end
    trim!(cache; keep_per_metric=2)
    snap = snapshot(cache)
    @test length(values_for_metric(snap, Main, :trim_multi, TimeMetric())) == 2
    @test length(values_for_metric(snap, Main, :trim_multi, StringMetric())) == 2
end

@testset "Key Type Mismatch" begin
    cache = MetricsCache()
    write_metric_value!(cache, Main, :ktype, 1, TimeMetric(), UInt64(1))
    @test_throws ArgumentError write_metric_value!(cache, Main, :ktype, "two", TimeMetric(), UInt64(2))
end

@testset "sync_insertion_order!" begin
    cache = MetricsCache()
    for k in 1:5
        write_metric_value!(cache, Main, :sync_test, k, TimeMetric(), UInt64(k))
    end
    snap = snapshot(cache)
    storage = MetricsTracker.metric_storage(snap, Main, :sync_test, TimeMetric())
    @test storage !== nothing

    storage.data[99] = UInt64(999)
    MetricsTracker.sync_insertion_order!(storage)
    @test 99 in storage.insertion_order
    @test length(storage.insertion_order) == length(storage.data)
end

@testset "trim! survives storage tampering" begin
    cache = MetricsCache()
    for k in 1:5
        write_metric_value!(cache, Main, :tamper, k, TimeMetric(), UInt64(k))
    end
    MetricsTracker.bulk_update!(cache) do c
        ctx = MetricsTracker.pending_context!(c, Main, :tamper, Int)
        storage = MetricsTracker.get_or_create_storage!(ctx, TimeMetric())
        storage.data[100] = UInt64(100)
        storage.data[101] = UInt64(101)
    end
    trim!(cache; keep_per_metric=3)
    snap = snapshot(cache)
    vals = values_for_metric(snap, Main, :tamper, TimeMetric(), Int)
    @test length(vals) == 3
end

@testset "Journal cache eviction via WeakKeyDict" begin
    tmpdir = mktempdir()
    try
        path = joinpath(tmpdir, "weak.dat")
        cache_ref = Ref{Union{MetricsCache, Nothing}}(MetricsCache())
        attach_journal!(cache_ref[], path)
        @test length(lock(j -> length(j), MetricsTracker.JOURNALS)) >= 1 ||
              haskey(lock(identity, MetricsTracker.JOURNALS), cache_ref[])
        cache_ref[] = nothing
        GC.gc(); GC.gc(); GC.gc()
        nremaining = lock(MetricsTracker.JOURNALS) do dict
            count(c -> c !== nothing, keys(dict))
        end
        @test nremaining == 0
    finally
        rm(tmpdir; recursive=true, force=true)
    end
end

end
