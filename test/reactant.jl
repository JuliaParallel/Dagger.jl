# Reactant integration, as seen by a session which has *not* loaded Reactant.jl.
# The tests which actually exercise Reactant live in `test/reactantenv`, which has
# its own project, since Reactant is far too heavy to depend on here.

import Logging

@testset "Mode selection" begin
    @test Dagger.reactant_mode(:inner) === Dagger.ReactantInner()
    @test Dagger.reactant_mode(:full) === Dagger.ReactantFull()
    @test Dagger.reactant_mode(Dagger.ReactantInner()) === Dagger.ReactantInner()
    @test_throws ArgumentError Dagger.reactant_mode(:bogus)
    @test_throws ArgumentError Dagger.reactant_mode(42)

    # Requirements are carried by the mode, and are additive
    @test Dagger.reactant_mode(:inner; must_opt=true) === Dagger.ReactantInner(; must_opt=true)
    @test Dagger.reactant_mode(:full; must_load=true) === Dagger.ReactantFull(; must_load=true)
    @test Dagger.reactant_mode(Dagger.ReactantFull(; must_opt=true); must_load=true) ===
          Dagger.ReactantFull(; must_opt=true, must_load=true)

    @test_throws ArgumentError macroexpand(@__MODULE__, :(Dagger.@reactant bogus=true nothing))
    @test_throws ArgumentError macroexpand(@__MODULE__, :(Dagger.@reactant))
end

@testset "Running without Reactant" begin
    @test !Dagger.reactant_available()

    A = rand(8, 8)
    B = rand(8, 8)

    # The same application code must run with and without Reactant, so a missing
    # Reactant is a warning rather than an error, and is only reported once
    logs, C = Test.collect_test_logs(min_level=Logging.Warn) do
        Dagger.@reactant fetch(Dagger.@spawn A * B)
        Dagger.@reactant mode=:inner fetch(Dagger.@spawn A * B)
    end
    @test C ≈ A * B
    @test length(logs) == 1
    @test occursin(r"Reactant.jl is not loaded", logs[1].message)

    # `:full` falls back to Datadeps executing the region as usual
    C = zeros(8, 8)
    Dagger.@reactant mode=:full begin
        Dagger.spawn_datadeps() do
            Dagger.@spawn mul!(Out(C), In(A), In(B))
        end
    end
    @test C ≈ A * B

    @test Dagger.with_reactant(() -> Dagger.get_options(:reactant, nothing), :inner) === nothing
end

@testset "must_load" begin
    A = rand(8, 8)

    # Code which cannot afford to silently run without Reactant asks to be told
    @test_throws Dagger.ReactantUnavailableError begin
        Dagger.@reactant must_load=true fetch(Dagger.@spawn sum(A))
    end
    @test_throws Dagger.ReactantUnavailableError begin
        Dagger.@reactant mode=:full must_load=true begin
            Dagger.spawn_datadeps() do
                Dagger.@spawn sum(In(A))
            end
        end
    end

    err = Dagger.ReactantUnavailableError(2)
    @test occursin("worker 2", sprint(showerror, err))

    # `must_opt` has nothing to act on without Reactant, so it is `must_load`'s job
    # alone to notice that Reactant is missing
    @test (@test_logs (:warn, r"Reactant.jl is not loaded") match_mode=:any begin
        Dagger.@reactant must_opt=true fetch(Dagger.@spawn sum(A))
    end) ≈ sum(A)
end

@testset "Compilation cache" begin
    # Nothing can have been compiled without Reactant, but the cache is still
    # queryable, as application code may report on it
    @test Dagger.reactant_cache_size() == 0
    Dagger.reactant_cache_clear!()
    @test Dagger.reactant_cache_size() == 0
end
