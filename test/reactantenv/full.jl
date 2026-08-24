@testset "Full mode" begin
    @testset "A region becomes one compiled program" begin
        Dagger.reactant_cache_clear!()

        A = rand(8, 8)
        B = rand(8, 8)
        C = zeros(8, 8)
        Dagger.@reactant mode=:full begin
            Dagger.spawn_datadeps() do
                Dagger.@spawn mul!(Out(C), In(A), In(B))
            end
        end
        @test C ≈ A * B
        @test Dagger.reactant_cache_size() == 1

        # Running the same region again reuses the program compiled for it
        for _ in 1:2
            Dagger.@reactant mode=:full begin
                Dagger.spawn_datadeps() do
                    Dagger.@spawn mul!(Out(C), In(A), In(B))
                end
            end
        end
        @test C ≈ A * B
        @test Dagger.reactant_cache_size() == 1
    end

    @testset "Tasks are chained through their dependencies" begin
        A = rand(8, 8)
        B = zeros(8, 8)
        C = zeros(8, 8)
        increment!(X) = X .+= 1
        Dagger.@reactant mode=:full begin
            Dagger.spawn_datadeps() do
                Dagger.@spawn copyto!(Out(B), In(A))
                Dagger.@spawn increment!(InOut(B))
                Dagger.@spawn mul!(Out(C), In(B), In(A))
            end
        end
        @test B ≈ A .+ 1
        @test C ≈ (A .+ 1) * A
    end

    @testset "Task results are available once the region has run" begin
        A = rand(8, 8)
        task = Ref{Any}(nothing)
        Dagger.@reactant mode=:full begin
            Dagger.spawn_datadeps() do
                task[] = Dagger.@spawn sum(In(A))
            end
        end
        @test fetch(task[]) ≈ sum(A)
    end

    @testset "Regions which Reactant cannot run are run by Dagger" begin
        function scalar_fill!(A)
            for idx in eachindex(A)
                A[idx] = idx
            end
            return nothing
        end
        A = zeros(4, 4)
        Dagger.@reactant mode=:full begin
            Dagger.spawn_datadeps() do
                Dagger.@spawn scalar_fill!(InOut(A))
            end
        end
        @test A == reshape(collect(1.0:16.0), 4, 4)

        # A `Ref` used as a scalar output cannot be written to by a compiled
        # program, so the region must be handed back to Dagger
        total = Ref(0.0)
        B = rand(4, 4)
        accumulate!(total, X) = (total[] = sum(X); nothing)
        Dagger.@reactant mode=:full begin
            Dagger.spawn_datadeps() do
                Dagger.@spawn accumulate!(Out(total), In(B))
            end
        end
        @test total[] ≈ sum(B)
    end

    @testset "must_opt reports a region Reactant cannot run" begin
        function scalar_fill!(A)
            for idx in eachindex(A)
                A[idx] = idx
            end
            return nothing
        end
        A = zeros(4, 4)
        @test_throws Dagger.ReactantOptimizationError begin
            Dagger.@reactant mode=:full must_opt=true begin
                Dagger.spawn_datadeps() do
                    Dagger.@spawn scalar_fill!(InOut(A))
                end
            end
        end
        # The region is reported as un-runnable before any of it has run
        @test all(A .== 0)
    end

    @testset "cholesky($(bs)x$(bs) blocks)" for bs in (8, 16)
        Random.seed!(1234)
        X = rand(16, 16)
        X = X * X' + 16I
        DX = distribute(copy(X), Blocks(bs, bs))

        chol = Dagger.@reactant mode=:full cholesky(DX)
        ref = cholesky(X)
        @test collect(chol.U) ≈ collect(ref.U) rtol=1e-8
        @test collect(chol.L) * collect(chol.U) ≈ X rtol=1e-8
    end

    @testset "@stencil" begin
        A = zeros(Blocks(2, 2), Int, 4, 4)
        A[2, 2] = 1
        source = collect(A)
        B = zeros(Blocks(2, 2), Int, 4, 4)
        Dagger.@reactant mode=:full begin
            @stencil B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        end
        expected = [sum(source[mod1(i + di, 4), mod1(j + dj, 4)] for di in -1:1, dj in -1:1)
                    for i in 1:4, j in 1:4]
        @test collect(B) == expected
    end
end
