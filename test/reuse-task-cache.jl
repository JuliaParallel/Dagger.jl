using Test

@testset "Lazy reusable task slots" begin
    setup_calls = Ref(0)
    setup(task) = (setup_calls[] += 1; Dagger.set_task_migratable!(task))
    cache = Dagger.ReusableTaskCache(32)
    Dagger.reusable_task_cache_init!(setup, cache)
    @test setup_calls[] == 0
    @test all(i -> !isassigned(cache.tasks, i), 1:cache.N)
    @test all(i -> !isassigned(cache.chans, i), 1:cache.N)

    # First use allocates just one slot, and registration still precedes the
    # payload even if the new task has already started on another thread.
    registered = Ref(false)
    result = Channel{Tuple{Any,Bool}}(1)
    task = Dagger.with_options(; scope=Dagger.scope(worker=1, thread=1)) do
        cache(() -> put!(result, (Dagger.get_options(:scope, nothing), registered[])),
              "lazy task slot", task -> (registered[] = true))
    end
    @test take!(result) == (nothing, true)
    @test task === cache.tasks[1]
    @test setup_calls[] == 1
    @test count(i -> isassigned(cache.tasks, i), 1:cache.N) == 1
    @test count(i -> isassigned(cache.chans, i), 1:cache.N) == 1
    @test timedwait(() -> cache.ready[1][], 10.0; pollint=0.001) === :ok
    done = Base.Event()
    @test (@inferred cache(() -> notify(done), "reuse task slot")) === task
    wait(done)
    @test setup_calls[] == 1
    @test timedwait(() -> cache.ready[1][], 10.0; pollint=0.001) === :ok
    finalize(cache)
    @test timedwait(() -> istaskdone(task), 10.0; pollint=0.001) === :ok
    @test !istaskfailed(task)

    # Saturation still uses the original overflow path; capacity is unchanged.
    cache = Dagger.ReusableTaskCache(2)
    setup_calls[] = 0
    Dagger.reusable_task_cache_init!(setup, cache)
    entered = Channel{Task}(3)
    release = Base.Event()
    f() = (put!(entered, current_task()); wait(release))
    tasks = [cache(f, "saturated task slots") for _ in 1:3]
    @test Set(take!(entered) for _ in 1:3) == Set(tasks)
    @test tasks[1] === cache.tasks[1]
    @test tasks[2] === cache.tasks[2]
    @test tasks[3] ∉ cache.tasks
    @test setup_calls[] == 3
    notify(release)
    wait(tasks[3])
    @test timedwait(() -> all(getindex, cache.ready), 10.0; pollint=0.001) === :ok
    finalize(cache)
    @test timedwait(() -> all(istaskdone, tasks), 10.0; pollint=0.001) === :ok
    @test !any(istaskfailed, tasks)

    # No payload ever used this cache: finalization must skip its empty cells.
    empty_cache = Dagger.ReusableTaskCache(32)
    Dagger.reusable_task_cache_init!(setup, empty_cache)
    finalize(empty_cache)
    @test all(i -> !isassigned(empty_cache.chans, i), 1:empty_cache.N)
end
