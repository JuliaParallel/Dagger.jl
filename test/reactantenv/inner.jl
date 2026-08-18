@testset "Inner mode" begin
    @testset "Task functions are compiled and run by Reactant" begin
        Dagger.reactant_cache_clear!()

        A = rand(8, 8)
        B = rand(8, 8)
        C = Dagger.@reactant fetch(Dagger.@spawn A * B)
        @test C isa Matrix{Float64}
        @test C ≈ A * B
        @test Dagger.reactant_cache_size() == 1

        # The same call with the same shapes reuses the compiled program
        C = Dagger.@reactant fetch(Dagger.@spawn A * B)
        @test C ≈ A * B
        @test Dagger.reactant_cache_size() == 1

        # Different shapes are a different program, as XLA compiles for fixed shapes
        A2 = rand(4, 4)
        @test Dagger.@reactant(fetch(Dagger.@spawn A2 * A2)) ≈ A2 * A2
        @test Dagger.reactant_cache_size() == 2

        Dagger.reactant_cache_clear!()
        @test Dagger.reactant_cache_size() == 0
    end

    @testset "Writes to arguments are visible to Dagger" begin
        A = rand(8, 8)
        B = rand(8, 8)
        C = zeros(8, 8)
        Dagger.@reactant begin
            Dagger.spawn_datadeps() do
                Dagger.@spawn mul!(Out(C), In(A), In(B))
            end
        end
        @test C ≈ A * B
    end

    @testset "Keyword arguments" begin
        f(A; scale) = A .* scale
        A = rand(8, 8)
        @test Dagger.@reactant(fetch(Dagger.@spawn f(A; scale=3))) ≈ A .* 3
    end

    @testset "Nested and wrapped array arguments" begin
        # Arguments are converted by Adapt, so arrays nested in containers and
        # behind array wrappers are converted (and written back) as well
        function scale_all!(pair, scale)
            pair.first .*= scale
            pair.second .*= scale
            return nothing
        end
        A = ones(4, 4)
        B = ones(4, 4)
        Dagger.@reactant fetch(Dagger.@spawn scale_all!((first=A, second=B), 3))
        @test all(A .== 3) && all(B .== 3)

        # A write through a `view` lands in the array it is a view of
        fill_with!(X, value) = (X .= value; nothing)
        C = zeros(4, 4)
        Dagger.@reactant fetch(Dagger.@spawn fill_with!(view(C, 1:2, :), 5))
        @test all(C[1:2, :] .== 5) && all(C[3:4, :] .== 0)

        # An array passed twice is one buffer, so both writes are kept
        increment_both!(X, Y) = (X .+= 1; Y .+= 1; nothing)
        D = zeros(4, 4)
        Dagger.@reactant fetch(Dagger.@spawn increment_both!(D, D))
        @test all(D .== 2)
    end

    @testset "Concurrent writes to views of one array" begin
        # Datadeps runs tasks which write to disjoint views of an array
        # concurrently, so each task's writes must be published back to its own
        # view rather than to the whole array
        fill_with!(X, value) = (X .= value; nothing)
        blocks = [idx:(idx + 15) for idx in 1:16:64]
        A = zeros(64, 64)
        Dagger.@reactant begin
            Dagger.spawn_datadeps() do
                for (bi, I) in enumerate(blocks), (bj, J) in enumerate(blocks)
                    Dagger.@spawn fill_with!(Out(view(A, I, J)), bi * 10 + bj)
                end
            end
        end
        @test A == [fld1(i, 16) * 10 + fld1(j, 16) for i in 1:64, j in 1:64]
    end

    @testset "Results are plain Julia values" begin
        # Reactant values must not escape into the caller's hands, including from
        # within the arrays that Dagger's reductions pass between their tasks
        DA = distribute(rand(16, 16), Blocks(8, 8))
        total = Dagger.@reactant sum(DA)
        @test total isa Float64
        @test total ≈ sum(collect(DA))

        column_sums = Dagger.@reactant collect(sum(DA; dims=1))
        @test column_sums isa Matrix{Float64}
        @test column_sums ≈ sum(collect(DA); dims=1)
    end

    @testset "Tasks which Reactant cannot compile are run without it" begin
        # A scalar-indexed kernel cannot be traced, and must not be a failure:
        # the task simply runs as it normally would
        function scalar_fill!(A)
            for idx in eachindex(A)
                A[idx] = idx
            end
            return A
        end
        A = zeros(4, 4)
        result = Dagger.@reactant fetch(Dagger.@spawn scalar_fill!(A))
        @test result == reshape(collect(1.0:16.0), 4, 4)

        # ... and is not attempted again
        A2 = zeros(4, 4)
        result = @test_logs min_level=Logging.Warn begin
            Dagger.@reactant fetch(Dagger.@spawn scalar_fill!(A2))
        end
        @test result == reshape(collect(1.0:16.0), 4, 4)

        # Unless the caller cannot afford to lose Reactant, in which case the
        # failure is reported rather than worked around
        A3 = zeros(4, 4)
        err = try
            Dagger.@reactant must_opt=true fetch(Dagger.@spawn scalar_fill!(A3))
            nothing
        catch caught
            caught
        end
        @test Dagger.Sch.unwrap_nested_exception(err) isa Dagger.ReactantOptimizationError
        @test all(A3 .== 0)
    end

    @testset "cholesky($T, $(bs)x$(bs) blocks)" for T in (Float32, Float64), bs in (4, 8)
        Random.seed!(1234)
        X = rand(T, 16, 16)
        X = X * X' + 16I
        DX = distribute(copy(X), Blocks(bs, bs))

        chol = Dagger.@reactant cholesky(DX)
        ref = cholesky(X)
        rtol = T == Float32 ? 1e-3 : 1e-8
        @test collect(chol.U) ≈ collect(ref.U) rtol=rtol
        @test collect(chol.L) * collect(chol.U) ≈ X rtol=rtol
    end

    @testset "@stencil" begin
        A = zeros(Blocks(2, 2), Int, 4, 4)
        B = zeros(Blocks(2, 2), Int, 4, 4)
        Dagger.@reactant begin
            @stencil begin
                A[idx] = 1
                B[idx] = A[idx] + 2
            end
        end
        @test all(collect(A) .== 1)
        @test all(collect(B) .== 3)

        # Neighborhood reads, which go through a halo and so are the least
        # Reactant-friendly thing `@stencil` generates
        C = zeros(Blocks(2, 2), Int, 4, 4)
        C[2, 2] = 1
        source = collect(C)
        D = zeros(Blocks(2, 2), Int, 4, 4)
        Dagger.@reactant begin
            @stencil D[idx] = sum(@neighbors(C[idx], 1, Wrap()))
        end
        expected = [sum(source[mod1(i + di, 4), mod1(j + dj, 4)] for di in -1:1, dj in -1:1)
                    for i in 1:4, j in 1:4]
        @test collect(D) == expected
    end
end
