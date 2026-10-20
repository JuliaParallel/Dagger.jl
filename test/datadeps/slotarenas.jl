# These tests need distinct memory spaces; -p 0 cannot exercise arena slots.
using Distributed, Test
using Dagger: In, InOut

@everywhere begin
    using LinearAlgebra
    arena_add!(A, n) = (A .+= n; nothing)
    arena_sum(A) = sum(A)
    arena_upper_sum(A) = sum(UpperTriangular(A))
    arena_fail!(A) = error("intentional arena task failure")
    const ARENA_TEST_GATES = Dict{UInt,Tuple{RemoteChannel,RemoteChannel}}()
    arena_store_gate!(id, gate, entered) = (ARENA_TEST_GATES[id] = (gate, entered); nothing)
    function arena_poison!(enabled)
        Dagger.SLOT_ARENA_POISON[] = enabled
        enabled || empty!(ARENA_TEST_GATES)
        return nothing
    end
    function arena_expired(h)
        try
            Dagger.unwrap(h)
            return false
        catch err
            return err isa ArgumentError
        end
    end
    function arena_gate_add!(A, id::UInt)
        gate, entered = ARENA_TEST_GATES[id]
        put!(entered, nothing)
        take!(gate)
        A .+= 1
        return nothing
    end
    arena_ids() = Dagger.@lock Dagger.SLOT_ARENAS_LOCK sort!(collect(keys(Dagger.SLOT_ARENAS)))
    arena_allocations() = Int(Dagger.SLOT_ARENA_NEXT_ID[])
end

arena_all_ids() = [w == myid() ? arena_ids() : remotecall_fetch(arena_ids, w) for w in procs()]
arena_all_empty() = all(isempty, arena_all_ids())
arena_total_allocations() = sum(w == myid() ? arena_allocations() :
                               remotecall_fetch(arena_allocations, w) for w in procs())
arena_chunk() = remotecall_fetch(() -> Dagger.tochunk(zeros(64,64)), workers()[1])
function arena_ready(channel)
    timedwait(() -> isready(channel), 60; pollint=0.001) === :ok || error("arena gate timed out")
    take!(channel)
    return nothing
end
arena_gate() = RemoteChannel(() -> Channel{Nothing}(1))
function arena_register_gate(gate, entered)
    id = rand(UInt)
    # Functions are Datadeps arguments too; carry only an isbits key through
    # the kernel and keep synchronization handles in explicit worker globals.
    for w in procs()
        if w == myid()
            arena_store_gate!(id, gate, entered)
        else
            remotecall_fetch(arena_store_gate!, w, id, gate, entered)
        end
    end
    return id
end
function arena_on_task(f)
    testset = Test.get_testset()
    return fetch(Threads.@spawn begin
        Test.push_testset(testset)
        try
            f()
        finally
            Test.pop_testset()
        end
    end)
end
function arena_cache_valid()
    lock(Dagger.SLOT_CACHE) do store
        all(entry -> !(entry.slot.handle isa Dagger.ArenaRef), values(store.entries))
    end
end

# Pause inside actual planning after the first slot has been prepared. A
# foreign drain must block here even if that first kernel has already finished.
struct ArenaGateScheduler <: Dagger.DataDepsScheduler
    inner::Dagger.RoundRobinScheduler
    entered::RemoteChannel
    gate::RemoteChannel
    calls::Base.RefValue{Int}
end

# The production single-owner policy currently always falls back to flat.
# Exercise the retained per-partition pipeline explicitly, without changing
# that policy globally or claiming the default call reached it.
struct ArenaPipelineScheduler <: Dagger.DataDepsScheduler end
Base.similar(::ArenaPipelineScheduler) = Dagger.RoundRobinScheduler()
function Dagger.distribute_tasks_hierarchical!(queue::Dagger.DataDepsTaskQueue{ArenaPipelineScheduler},
                                               ctx::Dagger.DataDepsContext)
    procs = Dagger.Processor[p for p in Dagger.get_processors(Dagger.OSProc(myid()))
                            if p isa Dagger.ThreadProc]
    return Dagger._distribute_tasks_hierarchical!(queue, queue.seen_tasks,
                                                  Dagger.current_acceleration(), procs, ctx)
end
function Dagger.datadeps_schedule_task(s::ArenaGateScheduler, state, procs, scope,
                                      task_scope, spec::Dagger.DTaskSpec, task::Dagger.DTask)
    s.calls[] += 1
    if s.calls[] == 2
        put!(s.entered, nothing)
        take!(s.gate)
    end
    return Dagger.datadeps_schedule_task(s.inner, state, procs, scope, task_scope, spec, task)
end

@testset "Context epoch slot arenas" begin
    if length(procs()) < 3
        @test_skip length(procs()) >= 3
    else
        target = Dagger.scope(worker=workers()[2])
        for w in procs()
            w == myid() ? arena_poison!(true) : remotecall_fetch(arena_poison!, w, true)
        end
        try
            @testset "Async carry-in, targeted drain, and free=false" begin
                arena_on_task() do
                    a, b = arena_chunk(), arena_chunk()
                    gate, entered = arena_gate(), arena_gate()
                    gate_id = arena_register_gate(gate, entered)
                    try
                        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                            Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                            Dagger.@spawn scope=target arena_gate_add!(InOut(b), gate_id)
                        end
                        arena_ready(entered)
                        ctx = Dagger.get_context!()
                        @test ctx.arenas !== nothing
                        ids = arena_all_ids()
                        @test any(!isempty, ids)
                        Dagger.synchronize(a)
                        @test all(==(1), fetch(a))
                        @test arena_all_ids() == ids # Partial drain must keep B's bytes.
                        Dagger.spawn_datadeps(; hierarchical=true, sync=false) do
                            Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                        end
                        @test arena_cache_valid() # No flat arena slot may enter SLOT_CACHE.
                        @test arena_all_ids() == ids
                        put!(gate, nothing)
                        Dagger.synchronize(; free=false)
                        @test all(==(2), fetch(a))
                        @test all(==(1), fetch(b))
                        @test ctx.state.slot_pool === nothing
                        @test arena_all_empty()
                    finally
                        isready(gate) || put!(gate, nothing)
                        Dagger.synchronize(; check_errors=false)
                    end
                end
            end

            @testset "Cold and reused hierarchical slots remain cacheable" begin
                arena_on_task() do
                    a = arena_chunk()
                    old = Dagger.HIER_TIMING[]
                    old_report = Dagger.HIER_TIMING_REPORT[]
                    try
                        Dagger.HIER_TIMING[] = true
                        Dagger.HIER_TIMING_REPORT[] = false
                        Dagger.TimespanLogging.steal_typed(Dagger.LogHierSlot)
                        for _ in 1:2
                            result = Dagger.spawn_datadeps(; hierarchical=true) do
                                Dagger.@spawn scope=target arena_sum(In(a))
                            end
                            @test fetch(result) == 0
                            @test arena_cache_valid()
                            @test arena_all_empty()
                        end
                        @test Dagger.LAST_HIER_STATS[].reused_count >= 1
                    finally
                        Dagger.HIER_TIMING[] = old
                        Dagger.HIER_TIMING_REPORT[] = old_report
                    end
                end
            end

            @testset "Deferred write-back still owns its arenas" begin
                arena_on_task() do
                    a = arena_chunk()
                    Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                        Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                    end
                    Dagger.synchronize(; write_back=false)
                    @test all(iszero, fetch(a))
                    @test !arena_all_empty()
                    @test !Dagger.issynchronized()
                    Dagger.synchronize()
                    @test all(==(1), fetch(a))
                    @test arena_all_empty()
                end
            end

            @testset "Byte cap falls back to ordinary DRefs" begin
                old = Dagger.DATADEPS_SLOT_ARENA_LIMIT[]
                try
                    Dagger.DATADEPS_SLOT_ARENA_LIMIT[] = 64 * 2^10
                    arena_on_task() do
                        inputs = [arena_chunk() for _ in 1:3]
                        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                            for a in inputs
                                Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                            end
                        end
                        ctx = Dagger.get_context!()
                        @test 0 < ctx.arenas.reserved_bytes <= Dagger.DATADEPS_SLOT_ARENA_LIMIT[]
                        slots = collect(values(ctx.state.remote_args[Dagger.CPURAMMemorySpace(workers()[2])]))
                        @test count(Dagger.freed_with_its_arena, slots) == 2
                        @test count(slot -> slot.handle isa Dagger.DRef, slots) == 1
                        Dagger.synchronize()
                        @test all(a -> all(==(1), fetch(a)), inputs)
                        @test arena_all_empty()
                    end
                finally
                    Dagger.DATADEPS_SLOT_ARENA_LIMIT[] = old
                end
            end

            @testset "Cached AOT prefill does not reserve the working set twice" begin
                arena_on_task() do
                    a = arena_chunk()
                    scheduler = Dagger.GreedyScheduler()
                    Dagger.spawn_datadeps(; hierarchical=false, sync=false, scheduler) do
                        Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                    end
                    ctx = Dagger.get_context!()
                    ids, bytes = arena_all_ids(), ctx.arenas.reserved_bytes
                    @test any(!isempty, ids)
                    Dagger.spawn_datadeps(; hierarchical=false, sync=false, scheduler) do
                        Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                    end
                    @test arena_all_ids() == ids
                    @test ctx.arenas.reserved_bytes == bytes
                    Dagger.synchronize()
                    @test all(==(2), fetch(a))
                    @test arena_all_empty()
                end
            end

            @testset "Tiled GEMM keeps every copy and output alive through drain" begin
                for hierarchical in (false, true)
                    arena_on_task() do
                        left, right = rand(256,256), rand(256,256)
                        A = Dagger.DArray(left, Dagger.Blocks(64,64))
                        B = Dagger.DArray(right, Dagger.Blocks(64,64))
                        C = zeros(Dagger.Blocks(64,64), 256,256)
                        wait(A); wait(B); wait(C)
                        before = arena_total_allocations()
                        Dagger.with(Dagger.DATADEPS_HIERARCHICAL => hierarchical) do
                            mul!(C, A, B)
                        end
                        @test collect(C) ≈ left * right
                        hierarchical || @test arena_total_allocations() > before
                        @test arena_all_empty()
                    end
                end
            end

            @testset "Pristine prefill is owned, aligned, and reset on drain" begin
                arena_on_task() do
                    ctx = Dagger.get_context!()
                    state = ctx.state = Dagger.DataDepsState()
                    Dagger.prefill_slot_pool!(ctx, state, Dagger.DTaskPair[], Dict{Dagger.DTask,Dagger.Processor}())
                    pool = state.slot_pool
                    space = Dagger.CPURAMMemorySpace(workers()[2])
                    Dagger.reserve_slot_arenas!(pool, Dict(space => 128))
                    slot = Dagger._arena_slot!(pool, Dagger.ThreadProc(workers()[2], 1), space, Vector{Float64}, (1,))
                    @test slot.handle.addr % Dagger.SLOT_ARENA_ALIGN == 0
                    @test (@inferred Dagger._arena_slot_bytes(Vector{Float64}, (1,))) == 8
                    @test (@inferred Dagger.datasize(slot.handle)) == 8
                    span = Dagger.aliasing(slot, identity)
                    @test Dagger._state_is_pristine(state)
                    @test !Dagger.context_drained(ctx)
                    Dagger.maybe_drop_context!()
                    @test Dagger.DATADEPS_CONTEXT[] === ctx
                    Dagger.synchronize()
                    @test ctx.state === state
                    @test state.slot_pool === nothing
                    @test pool.epoch.freed
                    @test Dagger._arena_slot!(pool, Dagger.ThreadProc(workers()[2], 1), space, Vector{Float64}, (1,)) === nothing
                    @test remotecall_fetch(arena_expired, workers()[2], slot.handle)
                    @test Dagger.aliasing(slot, identity) == span
                    @test arena_all_empty()
                end
            end

            @testset "Foreign all-context and task-exit drains reclaim arenas" begin
                for task_exit in (false, true)
                    a = arena_chunk()
                    ready, release = Channel{Nothing}(1), Channel{Nothing}(1)
                    owner = Threads.@spawn begin
                        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                            Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                        end
                        put!(ready, nothing)
                        take!(release)
                    end
                    take!(ready)
                    @test !arena_all_empty()
                    if task_exit
                        put!(release, nothing)
                        fetch(owner)
                        @test timedwait(arena_all_empty, 30; pollint=0.01) === :ok
                    else
                        try
                            Dagger.synchronize_all!()
                            @test arena_all_empty()
                        finally
                            put!(release, nothing)
                            fetch(owner)
                        end
                    end
                    @test all(==(1), fetch(a))
                end
            end

            @testset "State-pressure drain retires the old epoch" begin
                old = Dagger.DATADEPS_STATE_LIMIT[]
                try
                    Dagger.DATADEPS_STATE_LIMIT[] = 1
                    arena_on_task() do
                        a, b = arena_chunk(), arena_chunk()
                        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                            Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                            Dagger.@spawn scope=target arena_add!(InOut(b), 1)
                        end
                        ctx = Dagger.get_context!()
                        epoch = ctx.arenas
                        @test !arena_all_empty()
                        Dagger.spawn_datadeps(() -> nothing; hierarchical=false, sync=false)
                        @test epoch.freed
                        @test all(==(1), fetch(a))
                        @test all(==(1), fetch(b))
                        @test arena_all_empty()
                    end
                finally
                    Dagger.DATADEPS_STATE_LIMIT[] = old
                end
            end

            @testset "Per-partition pipeline drains its epoch before returning" begin
                if Threads.nthreads() < 4
                    @test_skip Threads.nthreads() >= 4
                else
                    arena_on_task() do
                        a = arena_chunk()
                        before = Dagger.SLOT_ARENA_NEXT_ID[]
                        results = Dagger.with_options(; scope=Dagger.scope(worker=myid())) do
                            Dagger.spawn_datadeps(; hierarchical=true, sync=false, scheduler=ArenaPipelineScheduler()) do
                                # Partial modifiers cannot use SLOT_CACHE. With
                                # one compute space and remote inputs, each
                                # parallel partition must really use an arena.
                                [Dagger.@spawn arena_upper_sum(Dagger.Deps(a, In(UpperTriangular)))
                                 for _ in 1:64]
                            end
                        end
                        @test all(t -> fetch(t) == 0, results)
                        @test Dagger.SLOT_ARENA_NEXT_ID[] > before
                        @test arena_all_empty()
                    end
                end
            end

            @testset "Concurrent pools share one epoch byte budget" begin
                old = Dagger.DATADEPS_SLOT_ARENA_LIMIT[]
                try
                    Dagger.DATADEPS_SLOT_ARENA_LIMIT[] = 64 * 2^10
                    arena_on_task() do
                        ctx = Dagger.get_context!()
                        ctx.arenas = Dagger.EpochSlotArenas()
                        epoch = ctx.arenas
                        pools = [Dagger.SlotPool(epoch) for _ in 1:2]
                        @sync for i in 1:2
                            Threads.@spawn Dagger.reserve_slot_arenas!(pools[i],
                                Dict(Dagger.CPURAMMemorySpace(workers()[i]) => 64 * 2^10))
                        end
                        @test length(epoch.arenas) == 1
                        @test epoch.reserved_bytes == 64 * 2^10
                        Dagger.synchronize()
                        @test arena_all_empty()
                    end
                finally
                    Dagger.DATADEPS_SLOT_ARENA_LIMIT[] = old
                end
            end

            @testset "Foreign drains wait for the complete planning publication" begin
                for targeted in (false, true)
                    a = arena_chunk()
                    entered, gate = arena_gate(), arena_gate()
                    owner_release = Channel{Nothing}(1)
                    context = Channel{Dagger.DataDepsContext}(1)
                    scheduler = ArenaGateScheduler(Dagger.RoundRobinScheduler(), entered, gate, Ref(0))
                    owner = Threads.@spawn begin
                        put!(context, Dagger.get_context!())
                        Dagger.spawn_datadeps(; hierarchical=false, sync=false, scheduler) do
                            Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                            Dagger.@spawn scope=target arena_add!(InOut(a), 1)
                        end
                        take!(owner_release)
                    end
                    ctx = take!(context)
                    try
                        arena_ready(entered)
                        ids = arena_all_ids()
                        @test any(!isempty, ids)
                        drainer = Threads.@spawn targeted ? Dagger.synchronize_task!(owner, a) :
                                                           Dagger.synchronize_task!(owner)
                        @test timedwait(() -> Dagger.@lock(ctx.lock, ctx.planning_done !== nothing),
                                        30; pollint=0.001) === :ok
                        @test !istaskdone(drainer)
                        @test arena_all_ids() == ids
                        put!(gate, nothing)
                        fetch(drainer)
                        @test all(==(2), fetch(a))
                        targeted && Dagger.synchronize_task!(owner)
                        @test arena_all_empty()
                    finally
                        isready(gate) || put!(gate, nothing)
                        put!(owner_release, nothing)
                        fetch(owner)
                    end
                end
            end

            @testset "Failed tasks do not reclaim a live sibling's slots" begin
                arena_on_task() do
                    a, b = arena_chunk(), arena_chunk()
                    gate, entered = arena_gate(), arena_gate()
                    gate_id = arena_register_gate(gate, entered)
                    try
                        Dagger.spawn_datadeps(; hierarchical=false, sync=false) do
                            Dagger.@spawn scope=target arena_fail!(InOut(a))
                            Dagger.@spawn scope=target arena_gate_add!(InOut(b), gate_id)
                        end
                        arena_ready(entered)
                        @test !arena_all_empty()
                        put!(gate, nothing)
                        @test_throws Dagger.DataDepsRegionError Dagger.synchronize()
                        @test all(==(1), fetch(b))
                        @test arena_all_empty()
                    finally
                        isready(gate) || put!(gate, nothing)
                        Dagger.synchronize(; check_errors=false)
                    end
                end
            end
        finally
            for w in procs()
                if w == myid()
                    arena_poison!(false)
                else
                    remotecall_fetch(arena_poison!, w, false)
                end
            end
        end
    end
end
