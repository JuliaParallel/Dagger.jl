function task_queue_update(r, op, x, y)
    Threads.atomic_add!(r, 1)
    return op(x, y)
end
function task_queue_wait_update(w, waiting, r, op, x, y)
    # Register that we are about to park *while holding the lock*, then wait.
    # `wait` atomically releases the lock and enqueues us on the condition, so
    # any notifier that subsequently acquires the lock is guaranteed to see us
    # parked. This makes the notify/wait handshake free of lost-wakeup races.
    @lock w begin
        Threads.atomic_add!(waiting, 1)
        wait(w)
    end
    Threads.atomic_add!(r, 1)
    return op(x, y)
end

# Spin (with a generous timeout) until `pred` holds. Used to replace fixed
# `sleep`-based timing, which is inherently flaky on busy/multithreaded CI.
function wait_for_condition(pred; timeout=60.0)
    tstart = time()
    while !pred()
        if time() - tstart > timeout
            error("task-queues test timed out waiting for a condition to hold")
        end
        sleep(0.005)
    end
    return nothing
end
# Block until at least `n` tasks are parked in `wait(w)`.
wait_until_parked(waiting, n) = wait_for_condition(() -> waiting[] >= n)
# Block until the shared counter `r` has reached at least `n`.
wait_until_count(r, n) = wait_for_condition(() -> r[] >= n)

@testset "DefaultTaskQueue" begin
    r = Threads.Atomic{Int}(0)
    R = Dagger.@mutable r
    d = begin
        b = Dagger.@spawn task_queue_update(R, *, 2, 3)
        wait(b); @test r[] == 1
        c = Dagger.@spawn task_queue_update(R, +, 3, 4)
        wait(c); @test r[] == 2
        Dagger.@spawn task_queue_update(R, /, c, b)
    end
    wait(d)
    @test r[] == 3
end

@testset "LazyTaskQueue" begin
    r = Threads.Atomic{Int}(0)
    R = Dagger.@mutable r
    d = Dagger.spawn_bulk() do
        b = Dagger.@spawn task_queue_update(R, *, 2, 3)
        @test_throws ConcurrencyViolationError wait(b); @test r[] == 0
        c = Dagger.@spawn task_queue_update(R, +, 3, 4)
        @test_throws ConcurrencyViolationError wait(c); @test r[] == 0
        Dagger.@spawn task_queue_update(R, /, c, b)
    end
    @test fetch(d) == (3+4) / (2*3)
    @test r[] == 3
end

@testset "InOrderTaskQueue" begin
    r = Threads.Atomic{Int}(0)
    R = Dagger.@mutable r
    w = Threads.Condition()
    waiting = Threads.Atomic{Int}(0)
    occ = Dict(Dagger.ThreadProc=>0)
    d = Dagger.spawn_sequential() do
        b = Dagger.@spawn occupancy=occ task_queue_wait_update(w, waiting, R, *, 2, 3)
        c = Dagger.@spawn occupancy=occ task_queue_wait_update(w, waiting, R, +, 3, 4)
        Dagger.@spawn task_queue_wait_update(w, waiting, R, /, c, b)
    end
    # Tasks run one-at-a-time; release each once it has actually parked.
    wait_until_parked(waiting, 1); @test r[] == 0
    @lock w notify(w); wait_until_count(r, 1); @test r[] == 1
    wait_until_parked(waiting, 2)
    @lock w notify(w); wait_until_count(r, 2); @test r[] == 2
    wait_until_parked(waiting, 3)
    @lock w notify(w); wait_until_count(r, 3); @test r[] == 3
    @test fetch(d) == (3+4) / (2*3)
end

@testset "LazyTaskQueue within InOrderTaskQueue" begin
    r = Threads.Atomic{Int}(0)
    R = Dagger.@mutable r
    w = Threads.Condition()
    waiting = Threads.Atomic{Int}(0)
    occ = Dict(Dagger.ThreadProc=>0)
    Dagger.spawn_sequential() do
        Dagger.spawn_bulk() do
            for i in 1:10
                Dagger.@spawn occupancy=occ task_queue_wait_update(w, waiting, R, *, 2, 3)
            end

            # Tasks not launched until end of block
            @test r[] == 0
        end
        # All 10 tasks launch together; release them once all are parked.
        wait_until_parked(waiting, 10)
        @lock w notify(w); wait_until_count(r, 10); @test r[] == 10

        Dagger.spawn_bulk() do
            for i in 1:5
                Dagger.@spawn occupancy=occ task_queue_wait_update(w, waiting, R, *, 2, 3)
            end
        end
        Dagger.spawn_bulk() do
            for i in 1:5
                Dagger.@spawn occupancy=occ task_queue_wait_update(w, waiting, R, *, 2, 3)
            end
        end
        # Second task group has a dependency on first task group
        wait_until_parked(waiting, 15)
        @lock w notify(w); wait_until_count(r, 15); @test r[] == 15
        wait_until_parked(waiting, 20)
        @lock w notify(w); wait_until_count(r, 20); @test r[] == 20
    end
end

@testset "InOrderTaskQueue within LazyTaskQueue" begin
    r = Threads.Atomic{Int}(0)
    R = Dagger.@mutable r
    w = Threads.Condition()
    waiting = Threads.Atomic{Int}(0)
    occ = Dict(Dagger.ThreadProc=>0)
    d = Dagger.spawn_bulk() do
        _d = Dagger.spawn_sequential() do
            b = Dagger.@spawn occupancy=occ task_queue_wait_update(w, waiting, R, *, 2, 3)
            c = Dagger.@spawn occupancy=occ task_queue_wait_update(w, waiting, R, +, 3, 4)
            Dagger.@spawn task_queue_wait_update(w, waiting, R, /, c, b)
        end
        # Tasks are not launched until the bulk block ends
        @test r[] == 0
        _d
    end
    wait_until_parked(waiting, 1); @test r[] == 0
    @lock w notify(w); wait_until_count(r, 1); @test r[] == 1
    wait_until_parked(waiting, 2)
    @lock w notify(w); wait_until_count(r, 2); @test r[] == 2
    wait_until_parked(waiting, 3)
    @lock w notify(w); wait_until_count(r, 3); @test r[] == 3
    @test fetch(d) == (3+4) / (2*3)
end

@everywhere function task_queue_propagated()
    return Dagger.get_options(:task_queue, nothing) !== nothing
end

@testset "Non-propagation" begin
    Dagger.spawn_sequential() do
        @test task_queue_propagated()
        @test !fetch(Dagger.@spawn task_queue_propagated())
    end
end
