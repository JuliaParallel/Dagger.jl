function task_queue_update(r, op, x, y)
    r[] += 1
    return op(x, y)
end
function task_queue_wait_update(w, r, op, x, y)
    wait(w)
    r[] += 1
    return op(x, y)
end

@testset "DefaultTaskQueue" begin
    r = Ref(0)
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
    r = Ref(0)
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
    r = Ref(0)
    R = Dagger.@mutable r
    w = Condition()
    occ = Dict(Dagger.ThreadProc=>0)
    d = Dagger.spawn_sequential() do
        b = Dagger.@spawn occupancy=occ task_queue_wait_update(w, R, *, 2, 3)
        c = Dagger.@spawn occupancy=occ task_queue_wait_update(w, R, +, 3, 4)
        Dagger.@spawn task_queue_wait_update(w, R, /, c, b)
    end
    sleep(1); @test r[] == 0
    notify(w); sleep(0.1); @test r[] == 1
    sleep(0.1); notify(w); sleep(0.1); @test r[] == 2
    sleep(0.1); notify(w); sleep(0.1); @test r[] == 3
    @test fetch(d) == (3+4) / (2*3)
end

@testset "LazyTaskQueue within InOrderTaskQueue" begin
    r = Ref(0)
    R = Dagger.@mutable r
    w = Condition()
    occ = Dict(Dagger.ThreadProc=>0)
    Dagger.spawn_sequential() do
        Dagger.spawn_bulk() do
            for i in 1:10
                Dagger.@spawn occupancy=occ task_queue_wait_update(w, R, *, 2, 3)
            end

            # Tasks not launched until end of block
            sleep(0.1); notify(w); sleep(0.1); @test r[] == 0
        end
        sleep(0.1); notify(w); sleep(0.1); @test r[] == 10

        Dagger.spawn_bulk() do
            for i in 1:5
                Dagger.@spawn occupancy=occ task_queue_wait_update(w, R, *, 2, 3)
            end
        end
        Dagger.spawn_bulk() do
            for i in 1:5
                Dagger.@spawn occupancy=occ task_queue_wait_update(w, R, *, 2, 3)
            end
        end
        # Second task group has a dependency on first task group
        sleep(0.1); notify(w); sleep(0.1); @test r[] == 15
        sleep(0.1); notify(w); sleep(0.1); @test r[] == 20
    end
end

@testset "InOrderTaskQueue within LazyTaskQueue" begin
    r = Ref(0)
    R = Dagger.@mutable r
    w = Condition()
    occ = Dict(Dagger.ThreadProc=>0)
    d = Dagger.spawn_bulk() do
        _d = Dagger.spawn_sequential() do
            b = Dagger.@spawn occupancy=occ task_queue_wait_update(w, R, *, 2, 3)
            c = Dagger.@spawn occupancy=occ task_queue_wait_update(w, R, +, 3, 4)
            Dagger.@spawn task_queue_wait_update(w, R, /, c, b)
        end
        sleep(1); notify(w); sleep(0.1); @test r[] == 0
        _d
    end
    sleep(0.1); @test r[] == 0
    notify(w); sleep(0.1); @test r[] == 1
    notify(w); sleep(0.1); @test r[] == 2
    notify(w); sleep(0.1); @test r[] == 3
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
