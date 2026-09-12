using TimespanLogging
import TimespanLogging: NoOpLog, LocalEventLog, MultiEventLog, ActiveLog
import TimespanLogging: Event, EventRecord, LegacyEvent, LogCategory
import TimespanLogging: enable!, disable!, reset!, steal_typed, steal_legacy
import TimespanLogging: steal_all_old_events, CHUNK_CAPACITY, category_id
import TimespanLogging: Events
using Test

TimespanLogging.@logcategory BenchTick as=:bench_tick id=(n::Int,) data=Nothing
TimespanLogging.@logcategory BenchPay as=:bench_pay id=(n::Int,) data=Any
TimespanLogging.@logcategory PairCat as=:pair id=(key::UInt,) data=Any

struct NullContext
end

struct Ctx
    log_sink
    profile::Bool
end
TimespanLogging.log_sink(ctx::Ctx) = ctx.log_sink
TimespanLogging.profile(ctx::Ctx, xs...) = ctx.profile

function measure_allocs(f)
    f()  # warmup caller-side
    GC.gc()
    before = Base.gc_num()
    f()
    diff = Base.GC_Diff(Base.gc_num(), before)
    return (allocs=Base.gc_alloc_count(diff), bytes=Int(diff.allocd))
end

@testset verbose=true "TimespanLogging" begin
    reset!()

    @testset "NoOp sink" begin
        ctx = NullContext()
        @test TimespanLogging.log_sink(ctx) == NoOpLog()
        timespan_start(ctx, :compute, 1, 2)
        timespan_finish(ctx, :compute, 1, 2)
        @test TimespanLogging.get_logs!(ctx) === nothing
        @test isempty(steal_legacy())
    end

    @testset "Legacy Symbol API + LocalEventLog" begin
        reset!()
        ctx = Ctx(LocalEventLog(), false)
        timespan_start(ctx, :compute, 1, 2)
        timespan_finish(ctx, :compute, 1, 2)
        raw = TimespanLogging.get_logs!(ctx.log_sink; raw=true)
        @test length(raw[1]) == 2
        @test raw[1][1] isa Event{:start}
        @test raw[1][2] isa Event{:finish}
        @test raw[1][1].category === :compute
        @test raw[1][1].id == 1
        @test raw[1][2].timeline == 2
        # second fetch is empty
        raw2 = TimespanLogging.get_logs!(ctx.log_sink; raw=true)
        @test isempty(raw2[1])

        timespan_start(ctx, :compute, :a, nothing)
        timespan_finish(ctx, :compute, :a, :done)
        spans = TimespanLogging.get_logs!(ctx.log_sink)
        @test spans isa Vector{TimespanLogging.Timespan}
        @test length(spans) == 1
        @test spans[1].category === :compute
        @test spans[1].id === :a
    end

    @testset "MultiEventLog consumers run at collect" begin
        reset!()
        ml = MultiEventLog()
        ml[:core] = Events.CoreMetrics()
        ml[:id] = Events.IDMetrics()
        ctx = Ctx(ml, false)
        timespan_start(ctx, :move, (;thunk_id=3), (;data=1))
        timespan_finish(ctx, :move, (;thunk_id=3), (;data=2))
        logs = TimespanLogging.get_logs!(ml)
        @test haskey(logs, 1)
        @test length(logs[1][:core]) == 2
        @test logs[1][:core][1].kind === :start
        @test logs[1][:core][2].kind === :finish
        @test logs[1][:core][1].category === :move
        @test logs[1][:id][1].thunk_id == 3
        logs2 = TimespanLogging.get_logs!(ml)
        @test isempty(logs2[1][:core])
    end

    @testset "Typed categories" begin
        reset!()
        @test category_id(BenchTick) isa UInt8
        @test TimespanLogging.category_symbol(BenchTick) === :bench_tick
        @test isbitstype(EventRecord{BenchTick, BenchTickId, Nothing})

        enable!(categories=[BenchTick, BenchPay])
        @logstart BenchTick BenchTickId(1) nothing
        @logfinish BenchTick BenchTickId(1) nothing
        @logstart BenchPay BenchPayId(7) "hello"
        @logfinish BenchPay BenchPayId(7) "world"

        ticks = steal_typed(BenchTick)
        pays = steal_typed(BenchPay)
        @test length(ticks) == 2
        @test ticks[1].phase == 0x00
        @test ticks[2].phase == 0x01
        @test ticks[1].id.n == 1
        @test ticks[1].timestamp <= ticks[2].timestamp
        @test length(pays) == 2
        @test pays[1].data == "hello"
        @test pays[2].data == "world"

        # Disabled category does not record
        @logstart PairCat PairCatId(UInt(1)) :x
        @test isempty(steal_typed(PairCat))

        disable!()
        @logstart BenchTick BenchTickId(99) nothing
        @test isempty(steal_typed(BenchTick))
    end

    @testset "@logstart with ctx gates on NoOpLog" begin
        reset!()
        ctx_off = Ctx(NoOpLog(), false)
        ctx_on = Ctx(LocalEventLog(), false)
        @logstart ctx_off BenchTick BenchTickId(1) nothing
        @logstart ctx_on BenchTick BenchTickId(2) nothing
        ticks = steal_typed(BenchTick)
        @test length(ticks) == 1
        @test ticks[1].id.n == 2
    end

    @testset "Chunk overflow publishes full slabs" begin
        reset!()
        enable!(categories=[BenchTick])
        N = CHUNK_CAPACITY * 3 + 17
        for i in 1:N
            @logstart BenchTick BenchTickId(i) nothing
        end
        st = TimespanLogging.thread_state()
        buf = TimespanLogging.typed_buffer(BenchTick, st)
        # Peek without steal: open + published should cover N events
        nchunk = @lock buf.lock TimespanLogging.chunk_count(buf.open, buf.published)
        @test nchunk == 4  # 3 full + partial
        evs = steal_typed(BenchTick)
        @test length(evs) == N
        @test [e.id.n for e in evs] == 1:N
        @test all(e.phase == 0x00 for e in evs)
        @test isempty(steal_typed(BenchTick))
    end

    @testset "Start/finish pairing across threads" begin
        reset!()
        enable!(categories=[PairCat])
        nwriters = min(Threads.nthreads(), 4)
        nper = 64
        @sync for t in 1:nwriters
            Threads.@spawn begin
                for i in 1:nper
                    key = UInt(t) << 32 | UInt(i)
                    @logstart PairCat PairCatId(key) :start
                    @logfinish PairCat PairCatId(key) :finish
                end
            end
        end
        evs = steal_typed(PairCat)
        @test length(evs) == nwriters * nper * 2
        starts = Dict{UInt,Int}()
        finishes = Dict{UInt,Int}()
        for e in evs
            if e.phase == 0x00
                starts[e.id.key] = get(starts, e.id.key, 0) + 1
            else
                finishes[e.id.key] = get(finishes, e.id.key, 0) + 1
            end
        end
        @test length(starts) == nwriters * nper
        @test starts == finishes
        @test all(==(1), values(starts))
    end

    @testset "Concurrent steal does not drop or duplicate" begin
        reset!()
        enable!(categories=[BenchTick])
        nper = 128
        stolen = Threads.Atomic{Int}(0)
        @sync begin
            Threads.@spawn begin
                for i in 1:nper
                    @logstart BenchTick BenchTickId(i) nothing
                end
            end
            Threads.@spawn begin
                # Fixed iteration budget — never wait on a condition that
                # another sticky spinner might prevent from running.
                for _ in 1:16
                    evs = steal_typed(BenchTick)
                    Threads.atomic_add!(stolen, length(evs))
                    stolen[] >= nper && break
                    sleep(0.001)
                end
            end
        end
        Threads.atomic_add!(stolen, length(steal_typed(BenchTick)))
        @test stolen[] == nper
    end

    @testset "Collect-time consumers are type-stable per event" begin
        reset!()
        enable!(categories=[BenchPay];
                consumers=Dict{Symbol,Any}(:core => Events.CoreMetrics(),
                                           :id => Events.IDMetrics()))
        ctx = Ctx(ActiveLog(), false)
        @logstart ctx BenchPay BenchPayId(4) :payload
        @logfinish ctx BenchPay BenchPayId(4) :done
        logs = TimespanLogging.get_logs!(ActiveLog())
        @test length(logs[1][:core]) == 2
        @test logs[1][:core][1].category === :bench_pay
        @test logs[1][:id][1] isa NamedTuple
        @test logs[1][:id][1].n == 4
    end

    @testset "generated as_old_id and old_data" begin
        reset!()
        TimespanLogging.@logcategory LogWrap as=:wrap id=(n::Int,) old_data=:payload
        enable!(categories=[LogWrap])
        @logstart LogWrap LogWrapId(3) "x"
        evs = steal_all_old_events()
        @test length(evs) == 1
        @test evs[1].id == (;n=3)
        @test evs[1].timeline == (;payload="x")
        @logstart LogWrap LogWrapId(4) (;payload="kept")
        evs = steal_all_old_events()
        @test evs[1].timeline == (;payload="kept")
    end

    @testset "per-list max_chunks drops overflow" begin
        list = TimespanLogging.ChunkList{Int}(1)
        N = CHUNK_CAPACITY * 2 + 10
        for i in 1:N
            TimespanLogging.push_event!(list, i)
        end
        @test list.dropped == CHUNK_CAPACITY
        open, pub = TimespanLogging.steal!(list)
        @test TimespanLogging.event_count(open, pub) == CHUNK_CAPACITY + 10
    end

    @testset "as_old_event projection" begin
        reset!()
        enable!(categories=[BenchTick])
        @logstart BenchTick BenchTickId(5) nothing
        evs = steal_all_old_events()
        @test length(evs) == 1
        @test evs[1] isa Event{:start}
        @test evs[1].category === :bench_tick
        @test evs[1].id isa NamedTuple
        @test evs[1].id.n == 5
    end

    @testset "Disabled emit does not evaluate payloads" begin
        reset!()
        evaluated = Ref(false)
        payload() = (evaluated[] = true; BenchTickId(1))
        ctx = Ctx(NoOpLog(), false)
        @logstart ctx BenchTick payload() nothing
        @test !evaluated[]
        # The Symbol API is a function, so arguments are evaluated; Dagger
        # uses `@maybelog` (or `@logstart`) to skip construction.
    end

    @testset "Allocations: disabled and typed heartbeat" begin
        reset!()
        ctx = Ctx(NoOpLog(), false)
        # Disabled Symbol path
        function disabled_legacy()
            timespan_start(ctx, :compute, 1, nothing)
            timespan_finish(ctx, :compute, 1, nothing)
        end
        disabled_legacy()
        a = measure_allocs(disabled_legacy)
        @test a.allocs == 0

        enable!(categories=[BenchTick])
        # Warm TLS / first chunk
        for i in 1:32
            @logstart BenchTick BenchTickId(i) nothing
            @logfinish BenchTick BenchTickId(i) nothing
        end
        steal_typed(BenchTick)

        function typed_heartbeat()
            @logstart BenchTick BenchTickId(1) nothing
            @logfinish BenchTick BenchTickId(1) nothing
        end
        a = measure_allocs(typed_heartbeat)
        @test a.allocs == 0
        steal_typed(BenchTick)
        disable!()
    end

    @testset "enable! / disable! bitset filter" begin
        reset!()
        enable!(categories=[BenchTick])
        @test TimespanLogging.category_enabled(BenchTick)
        @test !TimespanLogging.category_enabled(BenchPay)
        @logstart BenchTick BenchTickId(1) nothing
        @logstart BenchPay BenchPayId(1) :x
        @test length(steal_typed(BenchTick)) == 1
        @test isempty(steal_typed(BenchPay))
        enable!(categories=[BenchTick, BenchPay])
        @logstart BenchPay BenchPayId(2) :y
        @test length(steal_typed(BenchPay)) == 1
    end

    include("compat_api.jl")
end
